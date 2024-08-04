use std::{collections::HashMap, fmt::Display, sync::{Arc, RwLock}};

use crate::btree::{btree::request::{DeleteRequest, InsertReqest}, db::{scanner::Scanner, INDEX_ADD, INDEX_DEL, TDEF_META, TDEF_TABLE}, kv::{node::BNode, nodeinterface::{BNodeOperationInterface, BNodeReadInterface, BNodeWriteInterface}}, parser::{delete::DeleteExpr, expr::Expr, insert::InsertExpr, lib::Parser, select::SelectExpr, statement::{ExprSQL, ExprSQLList, SQLExpr, ScanExpr}, update::UpdateExpr}, scan::comp::OP_CMP, table::{record::Record, table::TableDef, value::Value}, BTreeError, MODE_INSERT_ONLY, MODE_UPDATE_ONLY, MODE_UPSERT};
use super::{tx::{self, Tx}, txRecord::{DataRow, DataTable}, txScanner::{self, TxScanner}, txbiter::TxBIter, txinterface::{DBTxInterface, TxInterface, TxReadContext, TxReaderInterface, TxWriteContext}};

pub struct txwriter{
    pub context : Tx,
    pub tables: Arc<RwLock<HashMap<Vec<u8>,TableDef>>>,
}

impl DBTxInterface for txwriter{

    fn Scan(&self, cmp1: OP_CMP, cmp2: Option<OP_CMP>, key1:&Record, key2:Option<&Record>)->Result<TxScanner,BTreeError> {
        if let Ok(indexNo) = key1.findIndexes()
        {
            return self.SeekRecord(indexNo, cmp1, cmp2, key1, key2);
        }
        else {            
            return Err(BTreeError::IndexNotFoundError);
        }
    }
    fn DeleteRecord(&mut self, rec:&crate::btree::table::record::Record)->Result<bool,crate::btree::BTreeError> {
        let bCheck = rec.checkPrimaryKey();
        if (bCheck == false) {
            return Err(BTreeError::PrimaryKeyIsNotSet);
        }

        let mut key = Vec::new();
        rec.encodeKey(rec.def.Prefix, &mut key);

        let mut request = DeleteRequest::new(&key);
        let ret = self.Delete(&mut request);
        if ret == false 
        {
            return Ok(false);
        }

        if rec.def.Indexes.len() == 0  {
            return Ok(true);
        }

        let mut old = Record::new(&rec.def);
        old.decodeValues(&request.OldValue);
        old.deencodeKey(&key);
        self.indexOp(&mut old, INDEX_DEL);

        return Ok(true);
    }

    fn AddTable(&mut self, tdef:&mut crate::btree::table::table::TableDef)-> Result<(),crate::btree::BTreeError> {
        
        //check the existing table
        let mut rtable = Record::new(&TDEF_TABLE);
        rtable.Set( "name".as_bytes(), Value::BYTES(tdef.Name.clone()));

        let ret1 = self.dbGet(&mut rtable);
        if let Ok(rc) = ret1
        {
            if rc == true
            {
                return Err(BTreeError::TableAlreadyExist);
            }
        }

        assert!(0 == tdef.Prefix);
        let mut rMeta = Record::new(&TDEF_META);

        tdef.Prefix = crate::btree::TABLE_PREFIX_MIN;
        rMeta.Set("key".as_bytes(), Value::BYTES("next_prefix".as_bytes().to_vec()));

        let retSearchMeta = self.dbGet( &mut rMeta);
        if let Ok(v) = retSearchMeta {
            if(v == true)
            {
                let v =rMeta.Get("val".as_bytes());
                if let Some( Value::BYTES(str)) = v
                {
                        tdef.Prefix = u32::from_le_bytes(str.try_into().unwrap());
                }
            }
        }

        tdef.Prefix += 1;

        let nPrefix: u32 = tdef.Indexes.len() as u32 + tdef.Prefix as u32 + 1;
        rMeta.Set("val".as_bytes(), Value::BYTES(nPrefix.to_le_bytes().to_vec()));
        self.dbUpdate(&mut rMeta, 0);

        tdef.FixIndexes();
        // store the definition
        let str = tdef.Marshal();

        rtable.Set("def".as_bytes(), Value::BYTES(str.as_bytes().to_vec()));
        self.dbUpdate(&mut rtable, 0);

        Ok(())
    }

    fn UpdateRecord(&mut self, rec:&mut crate::btree::table::record::Record, mode: u16) -> Result<(),crate::btree::BTreeError> {

        let mut bCheck = rec.checkRecord();
        if bCheck == false {
            return Err(BTreeError::ColumnValueMissing);
        }

        bCheck = rec.checkPrimaryKey();
        if bCheck == false {
            return Err(BTreeError::PrimaryKeyIsNotSet);
        }

        bCheck = rec.checkIndexes();
        if bCheck == false {
            return Err(BTreeError::IndexesValueMissing);
        }

        let mut key:Vec<u8> = Vec::new();
        rec.encodeKey(rec.def.Prefix, &mut key);

        let mut v:Vec<u8> = Vec::new();
        rec.encodeValues(&mut v);

        let mut request = InsertReqest::new(&key,&v,mode);
        self.Set(&mut request);

        if (rec.def.Indexes.len() == 0) || (request.Updated == false) {
            return Ok(());
        }

        if (request.Updated == true && request.Added == false) {

            let mut old = Record::new(&rec.def);
            old.decodeValues(&request.OldValue);
            old.deencodeKey(&key);
            self.indexOp(&mut old, INDEX_DEL);
        }

        if request.Updated {
            let mut old = Record::new(&rec.def);
            // old.decodeValues(&key);
            // old.deencodeKey(&key);
            self.indexOp(rec, INDEX_ADD);
        }

        return Ok(());
    }
    
}

impl TxReaderInterface for txwriter{

    // get a single row by the primary key
    fn dbGet(&self,rec:&mut Record)->Result<bool,BTreeError> {
        let bCheck = rec.checkPrimaryKey();
        if bCheck == false {
            return Err(BTreeError::PrimaryKeyIsNotSet);
        }

        let mut list:Vec<u8> = Vec::new();
        rec.encodeKey(rec.def.Prefix,&mut list);

        let val = self.Get(&list);
        match &val {
            Some(v)=>{
                rec.decodeValues(&v);
                return Ok(true);
            },
            Other=>{
                return Ok(false);
            }
        }
    }
        
    fn Get(&self, key:&[u8])  -> Option<Vec<u8>> {
        let rootNode = self.context.get(self.context.get_root());
        match rootNode{
            Some(root) => return self.treeSearch(&root,key),
            None => return None
        }
    }

    fn Seek(&self, key:&[u8], cmp:crate::btree::scan::comp::OP_CMP) -> super::txbiter::TxBIter {
        let mut iter = self.SeekLE(key);
        if iter.Valid() {
            if let OP_CMP::CMP_LE = cmp  
            {
                return iter;
            }

            let cur = iter.Deref();
            if crate::btree::scan::comp::cmpOK(cur.0, key, &cmp) == false {
                //off by one
                if cmp.value() > 0 {
                    _ = iter.Next();
                } else {
                    _ = iter.Prev();
                }
                return iter;
            }
        }
        return iter;
    }
}


impl TxInterface for txwriter{
    fn Set(&mut self,req:&mut InsertReqest){
        self.InsertKV(req);
    }

    fn Delete(&mut self, req: &mut DeleteRequest) -> bool{
        self.DeleteKV(req)
    }
}

impl txwriter{

    pub fn ExecuteSQLStatments(&mut self,statements:String)->Result<Vec<DataTable>,BTreeError>
    {
        let mut list = Vec::new();
        let ret = ExprSQLList().parse(&statements);
        if let Ok((ret,sqlExprList)) = ret
        {
            for sql1 in sqlExprList
            {
                match &sql1 {
                    SQLExpr::Select(expr) => {
                        if let Ok(table) = self.ExecuteReader(&expr)
                        {
                            list.push(table);
                        }
                    },
                    expr@Other => {
                        if let Ok(affected) = self.ExecuteNoQuery(&expr)
                        {
                            println!("affected: {}",affected);
                        }
                    },
                }                 
            }
        }
        Ok(list)
    }

    pub fn ExecuteNoQuery(&mut self,expr:&SQLExpr)->Result<usize,BTreeError>
    {
        match &expr {
            //SQLExpr::Select(expr) => return self.executeSelect(expr),
            SQLExpr::Update(expr) => return self.executeUpdate(expr),
            SQLExpr::Delete(expr) => return self.executeDelete(expr),
            SQLExpr::Insert(expr) => return self.executeInsert(expr),
            SQLExpr::CreatTable(v) => return self.createTable(v),
            _Other => panic!("Not Supported")
        }
    }
    
    fn search<F>(&self,tdef:&TableDef,expr:&ScanExpr,mut fnProcess :F)->Result<(), BTreeError>
    where 
        F: FnMut(DataRow)
    {
        if let Ok((key1,key2,cmp1,cmp2)) = expr.createScan(&tdef)
        {
            let mut index:usize = 0;
            let mut count:usize = 0;
            let mut scanner = self.Scan( cmp1, cmp2, &key1, key2.as_ref());
            match &mut scanner {
                Ok(cursor) =>{

                    cursor.into_iter()
                    .filter(|x| 
                        {
                            if let Some(filter) = &expr.Filter
                            {
                                Self::evalFilterExpr(&filter, tdef,&x)
                            }
                            else {
                                true
                            }
                        })
                    .skip(expr.Offset)
                    .take(expr.Limit)
                    .for_each(|x| fnProcess(x));
                },
                Err(err) => { return Err(BTreeError::NextNotFound)}
            }
        }
        Ok(())

    }

    pub fn ExecuteReader(&mut self, cmd:&SelectExpr)->Result<DataTable,BTreeError>
    {
        let tdef = self.getTableDef(&cmd.Scan.Table.to_vec());
        if tdef.is_none()
        {
            return Err(BTreeError::TableNotFind);
        }

        let tdef = tdef.unwrap();
        let mut txTable = DataTable::new(&tdef);
        for i in 0..cmd.Name.len()
        {
            if cmd.Name[i].len() == 0
            {
                txTable.Cols.push(cmd.Ouput[i].to_string().as_bytes().to_vec());
            }
            else {
                txTable.Cols.push(cmd.Name[i].clone());
            }
        }

        let fnProcessRecord = |r:DataRow| {
            let mut rc: DataRow = DataRow::new();
            for i in 0..cmd.Ouput.len()
            {
                if let Ok(v) = cmd.Ouput[i].eval(&tdef,&r.Vals)
                {
                    rc.Vals.push(v);
                }
            }
            txTable.Rows.push(rc);
        };

        self.search(&tdef, &cmd.Scan, fnProcessRecord);
        if txTable.Rows.len() > 0
        {
            for v in &txTable.Rows.get(0).as_ref().unwrap().Vals
            {   
                txTable.Types.push(v.GetValueType());
            }
        }
        Ok(txTable)

    }

    fn evalFilterExpr(expr:&Expr,tdef:&TableDef,row:&DataRow)->bool
    {
        if let Ok(Value::BOOL(true)) = expr.eval(tdef,&row.Vals)
        {
            return true;
        }
        false
    }

    fn executeUpdate(&mut self, cmd:&UpdateExpr)->Result<usize,BTreeError>
    {
        let tdef = self.getTableDef(&cmd.Scan.Table.to_vec());
        if tdef.is_none()
        {
            return Err(BTreeError::TableNotFind);
        }
        
        let tdef = tdef.unwrap();
        let mut list = Vec::new();
        let mut count:usize = 0;

        let fnProcessRecord = |r:DataRow| {
            let mut rc = Record::new(&tdef);
            rc.Vals = r.Vals;
            list.push(rc);
        };

        self.search(&tdef, &cmd.Scan, fnProcessRecord);

        for mut r in list
        {
            for i in 0..cmd.Name.len()
            {
                if let Ok(v) = cmd.Values[i].eval(&tdef,&r.Vals)
                {
                    if let Err(ex) = r.Set(&cmd.Name[i], v)
                    {
                        return Err(BTreeError::EvalException);
                    }
                }
            }
            if let Ok(v) = self.UpdateRecord(&mut r,MODE_UPDATE_ONLY)
            {
                 count += 1;
            }
        }

        Ok(count)

    }

    fn executeDelete(&mut self, cmd:&DeleteExpr)->Result<usize,BTreeError>
    {
        let tdef = self.getTableDef(&cmd.Scan.Table.to_vec());
        if tdef.is_none()
        {
            return Err(BTreeError::TableNotFind);
        }

        let tdef = tdef.unwrap();
        let mut count:usize = 0;

        let mut list = Vec::new();
        let fnProcessRecord = |r:DataRow| {
            let mut rc = Record::new(&tdef);
            rc.Vals = r.Vals;
            list.push(rc);
        };

        self.search(&tdef, &cmd.Scan, fnProcessRecord);
        list.iter().for_each(
            |row| {
                if let Ok(true) = self.DeleteRecord(&row)
                {
                        count += 1;
                }
            }
        );

        Ok(count)
    }

    fn executeInsert(&mut self, cmd:&InsertExpr)->Result<usize,BTreeError>
    {
        let tdef = self.getTableDef(&cmd.TableName.to_vec());
        if tdef.is_none()
        {
            return Err(BTreeError::TableNotFind);
        }

        let mut recordes = cmd.createQuest(&tdef.as_ref().unwrap());
        let mut count:usize = 0;
        for row in &mut recordes.unwrap()
        {
            if let Err(err) = self.UpdateRecord(row, MODE_INSERT_ONLY)
            {
                return Err(err);                
            }
            count += 1;
        }

        Ok(count)
    }


    fn createTable(&mut self, tdef:&TableDef)->Result<usize,BTreeError>
    {
        let mut tdef = tdef.clone();
        if let Err(err) = self.AddTable(&mut tdef)
        {
            Err(err)
        }
        else {
            Ok(1)
        }
    }

    fn SeekRecord(&self,idxNumber:i16, cmp1: OP_CMP, cmp2: Option<OP_CMP>, key1:&Record, key2:Option<&Record>)->Result<TxScanner,BTreeError> {
        
        // sanity checks
        if cmp2.is_some()
        {
            if cmp1.value() > 0 && cmp2.unwrap().value() < 0 
            {} 
            else if cmp2.unwrap().value() > 0 && cmp1.value() < 0 
            {} 
            else {
                return Err(BTreeError::BadArrange);
            }
        }

        let mut keyStart: Vec<u8> = Vec::new();
        let mut keyEnd: Vec<u8> = Vec::new();

        if idxNumber == -1
        {
            let bCheck1 = key1.checkPrimaryKey();
            if  bCheck1 == false {
                return Err(BTreeError::KeyError);
            }

            if key2.is_some()
            {
                let bCheck2 = key2.unwrap().checkPrimaryKey();
                if  bCheck2 == false {
                    return Err(BTreeError::KeyError);
                }
            }
    
            key1.encodeKey(key1.def.Prefix, &mut keyStart);
            if key2.is_some()
            {
                key2.unwrap().encodeKey(key2.unwrap().def.Prefix, &mut keyEnd);
            }
        }
        else {
            key1.encodeKeyPartial(idxNumber as usize,&mut keyStart,&cmp1);
            if key2.is_some()
            {
                key2.unwrap().encodeKeyPartial(idxNumber as usize,&mut keyEnd,&cmp2.unwrap());
            }
            println!("KeyStart:{:?}  KeyEnd:{:?}",keyStart,keyEnd);
        }

        let iter = self.Seek(&keyStart, cmp1);
        if iter.Valid() == false
        {
            return Err(BTreeError::NextNotFound);
        }
        Ok(
            if key2.is_some()
            {
                TxScanner::new(self,key1.def.clone(),idxNumber,cmp1,cmp2,keyStart,Some(keyEnd),iter)
            }
            else {
                TxScanner::new(self,key1.def.clone(),idxNumber,cmp1,cmp2,keyStart,None,iter)
            }
        )

    }


    fn indexOp(& mut self, rec: &mut Record, op: u16) -> Result<(),BTreeError> {

        for i in 0..rec.def.Indexes.len(){

            let mut index = Vec::new();
            rec.encodeIndex(rec.def.IndexPrefixes[i], i, &mut index);
            //println!("Rec:{}",rec);
            //println!("Index :{}\n  Vals Result:{:?} ", i, index);
            if op == INDEX_ADD {
                let mut request = InsertReqest::new( &index ,&[0;1], MODE_UPSERT);
                self.Set(&mut request);
            } 
            else if op == INDEX_DEL 
            {
                let mut reqDelete = DeleteRequest::new(&index);
                self.Delete(&mut reqDelete);
            } 
            else {
                panic!("bad op value!");
            }
        }

        Ok(())
    }

    //get Table Define
    fn getTableDefFromDB(&self, name: &[u8])->Option<TableDef> {

        let mut rec = Record::new(&TDEF_TABLE);
        rec.Set("name".as_bytes(), Value::BYTES(name.to_vec()));
        let ret = self.dbGet(&mut rec);
        if let Err(er) = ret
        {
            return None;
        }

        if let Ok(r) = ret{
            if r == true
            {
                let r1 = rec.Get("def".as_bytes());
                if let Some(Value::BYTES(val)) = r1
                {
                    let def: TableDef = serde_json::from_str( &String::from_utf8(val.to_vec()).unwrap()) .unwrap();
                    return Some(def);
                }
            }
        }
        return None;
    }

    pub fn getTableDef(&mut self, name: &[u8]) -> Option<TableDef> {
        if let tbs = self.tables.clone().read().unwrap()
        {
            if let Some(def) = tbs.get(name)
            {
                return Some(def.clone());
            }
            drop(tbs);
        }

        let defParsed =  self.getTableDefFromDB(name);
        if let Some(def) = defParsed
        {
            if let mut tbs = self.tables.clone().write().unwrap()
            {
                tbs.insert(name.to_vec(), def.clone());
                drop(tbs);
            }
            return Some(def);
        }

        return None;
    }

    // add a row to the table
    fn dbUpdate(&mut self, rec:&mut Record, mode: u16) -> Result<(),BTreeError> {

        let mut bCheck = rec.checkRecord();
        if bCheck == false {
            return Err(BTreeError::ColumnValueMissing);
        }

        bCheck = rec.checkPrimaryKey();
        if bCheck == false {
            return Err(BTreeError::PrimaryKeyIsNotSet);
        }

        let mut key:Vec<u8> = Vec::new();
        rec.encodeKey(rec.def.Prefix, &mut key);

        let mut v:Vec<u8> = Vec::new();
        rec.encodeValues(&mut v);

        let mut request = InsertReqest::new(&key, &v, mode);
        self.Set(&mut request);
        return Ok(());
    }

    
    fn SeekLE(&self, key:&[u8]) -> TxBIter
    {
        let mut iter = TxBIter::new(&self.context);

        let mut ptr = self.context.get_root();
        let mut n = self.context.get(ptr).unwrap();
        let mut idx: usize = 0;
        while (ptr != 0) {
            n = self.context.get(ptr).unwrap();
            idx = n.nodeLookupLE(key) as usize;

            if n.btype() == crate::btree::BNODE_NODE {
                ptr = n.get_ptr(idx);
            } else {
                ptr = 0;
            }

            iter.path.push(n);
            iter.pos.push(idx);
        }
        iter.valid = true;
        return iter;
    }
    
     // Search a key from the tree
     fn treeSearch<T:BNodeReadInterface>(&self, treenode: &T, key: &[u8]) -> Option<Vec<u8>> {
        // where to find the key?
        let idx = treenode.nodeLookupLE(key);
        // act depending on the node type
        match  treenode.btype() {
            crate::btree::BNODE_LEAF => {
                // leaf, node.getKey(idx) <= key
                let key1 = treenode.get_key(idx);
                let comp = crate::btree::util::compare_arrays(key, key1);
                if  comp == 0 {
                    return Some(treenode.get_val(idx).to_vec());
                } else {
                    // not found
                    return None;
                }
            },
            crate::btree::BNODE_NODE => {
                let ptr = treenode.get_ptr(idx as usize);
                let subNode = self.context.get(ptr);
                match subNode{
                    Some(node) => {
                        return self.treeSearch(&node,key);
                    } 
                    None => return None
                }
            },
            other=> return None
        }
    }

    fn DeleteKV(&mut self, request: &mut DeleteRequest) -> bool
    {
        assert!(request.Key.len() != 0);
        assert!(request.Key.len() <= crate::btree::BTREE_MAX_KEY_SIZE);

        if (self.context.get_root() == 0) {
            return false;
        }

        //const n1 = try self.kv.get(self.kv.getRoot());
        let n1 = self.context.get(self.context.get_root());
        if(n1.is_none())
        {
            panic!("Root not found!");
        }        

        let updated = self.treeDelete(&n1.unwrap(), request);
        match updated{
            Some(n) => {
                _ = self.context.del(self.context.get_root());
                if n.btype() == crate::btree::BNODE_NODE && n.nkeys() == 1 {
                    // remove a level
                    self.context.set_root(n.get_ptr(0));
                } else {
                    let newroot = self.context.add(n);
                    self.context.set_root(newroot);
                }
                return true
            },
            None=> return false
        }
    }

     // delete a key from the tree
     fn treeDelete<T:BNodeReadInterface>(&mut self, treenode: &T, request: &mut DeleteRequest) -> Option<BNode> {
        // where to find the key?
        let idx = treenode.nodeLookupLE(request.Key);
        // act depending on the node type
        match treenode.btype() {
            crate::btree::BNODE_LEAF => {
                // leaf, node.getKey(idx) <= key
                let comp = crate::btree::util::compare_arrays(request.Key, treenode.get_key(idx));
                if comp == 0 {
                    // delete the key in the leaf
                    //std.debug.print("Node Delete! {d}", .{idx});
                    //treenode.print();
                    let mut node = BNode::new(crate::btree::BTREE_PAGE_SIZE);
                    node.leaf_delete(treenode, idx);

                    let v = treenode.get_val(idx);
                    request.OldValue.extend_from_slice(v);
                    //updatedNode.print();
                    return Some(node);
                } else {
                    // not found
                    return None;
                }
            },
            crate::btree::BNODE_NODE => {
                // internal node, insert it to a kid node.
                return self.nodeDelete(treenode, idx, request);
            },
            other => {
                panic!("Exception Insert Node!\n");
            },
        }
    }

    fn nodeDelete<T:BNodeReadInterface>(&mut self, treenode:&T, idx: u16, request: &mut DeleteRequest) -> Option<BNode> {
        // recurse into the kid
        let kptr = treenode.get_ptr(idx as usize);
        let realnode = self.context.get(kptr);
        if realnode.is_none()
        {
            panic!("Node is not found! idx:{:?} Key:{:?}", idx, request.Key);
        }

        let updated = self.treeDelete(&realnode.unwrap(), request);
        if  updated.is_none() {
            return None; // not found
        }
        _ = self.context.del(kptr);

        let mut nodeUpdated = updated.unwrap();
        let mut newNode = BNode::new(crate::btree::BTREE_PAGE_SIZE);
        // check for merging
        let (flagMerged,sibling) = self.shouldMerge(treenode, idx, &nodeUpdated);
        match flagMerged {
            0 => {
                assert!(nodeUpdated.nkeys() > 0);
                let ptr = self.context.add(nodeUpdated);

                let updatedNode = self.context.get(ptr).unwrap();
                let key = updatedNode.get_key(0);
                let nodes = vec![(ptr,key.to_vec())]; 
                newNode.nodeReplaceKidN(treenode, idx,nodes);
            },
            -1 => { //left
                //print!("Merge Left.\n");
                let mut merged = BNode::new(crate::btree::BTREE_PAGE_SIZE);
                let nodeMerged = self.context.get(sibling.unwrap());
                match nodeMerged
                {
                    Some(n) => {
                        merged.nodeMerge(&n, &nodeUpdated);
                        let prtMerged = self.context.add(merged);
                        _ = self.context.del(treenode.get_ptr(idx as usize - 1));

                        let nodeMerged = self.context.get(prtMerged).unwrap();
                        newNode.nodeReplace2Kid(treenode, idx - 1, prtMerged, nodeMerged.get_key(0));
                    },
                    None => panic!("Get Node Exception idx: {:?}", sibling)
                }
            },
            1 => { //right
                //std.debug.print("Merge Right.\n", .{});
                let mut merged = BNode::new(crate::btree::BTREE_PAGE_SIZE);
                let nodeMerged = self.context.get(sibling.unwrap());
                match nodeMerged
                {
                    Some(n) => {
                        merged.nodeMerge( &nodeUpdated,&n);
                        let prtMerged = self.context.add(merged);
                        _ = self.context.del(treenode.get_ptr(idx as usize + 1));

                        let nodeMerged = self.context.get(prtMerged).unwrap();
                        newNode.nodeReplace2Kid(treenode, idx, prtMerged, nodeMerged.get_key(0));
                    },
                    None => panic!("Get Node Exception idx: {:?}", sibling)
                }
            },
            other => {
                panic!("Exception Merge Flag!");
            },
        }

        return Some(newNode);
    }

    fn shouldMerge<T:BNodeReadInterface>(&self, treenode: &T, idx: u16, updated: &BNode)-> (i16,Option<u64>) {
        if updated.nbytes() > crate::btree::BTREE_PAGE_SIZE / 4 {
            return (0, None);
        }

        if  idx > 0 {
            let sibling = self.context.get(treenode.get_ptr(idx as usize - 1));
            match sibling{
                Some(n) => {
                    let merged:usize = n.nbytes() as usize + updated.nbytes() as usize - crate::btree::HEADER as usize;
                    if merged <= crate::btree::BTREE_PAGE_SIZE {
                        return (-1, Some(treenode.get_ptr(idx as usize - 1)));
                    }        
                },
                None => panic!("Get Node Exception idx: {:?}", idx - 1)
            }

        }
        if  idx + 1 < treenode.nkeys() {
            let sibling = self.context.get(treenode.get_ptr(idx as usize + 1));
            match sibling{
                Some(n) => {
                    let merged:usize = n.nbytes() as usize + updated.nbytes() as usize - crate::btree::HEADER as usize;
                    if merged <= crate::btree::BTREE_PAGE_SIZE {
                        return (1, Some(treenode.get_ptr(idx as usize + 1)));
                    }        
                },
                None => panic!("Get Node Exception idx: {:?}", idx - 1)
            }
        }

        return (0,None);
    }
    //Interface for Insert KV
    fn InsertKV(&mut self, request:&mut InsertReqest) {
        assert!(request.Key.len() != 0);
        assert!(request.Key.len() <= crate::btree::BTREE_MAX_KEY_SIZE);
        assert!(request.Val.len() <= crate::btree::BTREE_MAX_VALUE_SIZE);
    
        if self.context.get_root() == 0 {
            let mut root = BNode::new(crate::btree::BTREE_PAGE_SIZE);
            root.set_header(crate::btree::BNODE_LEAF, 2);
            root.node_append_kv(0, 0, &[0;1], &[0;1]);
            root.node_append_kv(1, 0, request.Key, request.Val);

            let newroot = self.context.add(root);
            self.context.set_root(newroot);

            request.Added = true;
            request.Updated = false;

            return;
        }
    
        let oldRootPtr = self.context.get_root();
        let nodeRoot = self.context.get(oldRootPtr).unwrap();
    
        let mut nodeTmp = self.treeInsert(&nodeRoot, request);
        match(nodeTmp)
        {
            Some(node) => {
            let (count,n1,n2,n3) = node.nodeSplit3();
            if count == 1
            {
                    let ptr1 = self.context.add(n1.unwrap());
                    self.context.set_root(ptr1);
                    _ = self.context.del(oldRootPtr);

                    return;
            }
                
            let mut root = BNode::new(crate::btree::BTREE_PAGE_SIZE);
            root.set_header(crate::btree::BNODE_NODE, count);

            let ptr1 = self.context.add(n1.unwrap());
            let node1 = self.context.get(ptr1).unwrap();
            let node1key = node1.get_key(0);
            root.node_append_kv(0, ptr1, node1key, &[0;1]);

            if n2.is_none() == false
            {
                let ptr = self.context.add(n2.unwrap());
                let node = self.context.get(ptr).unwrap();
                let nodekey = node.get_key(0);
                root.node_append_kv(1, ptr, nodekey, &[0;1]);
            }

            if n3.is_none() == false
            {
                let ptr = self.context.add(n3.unwrap());
                let node = self.context.get(ptr).unwrap();
                let nodekey = node.get_key(0);
                root.node_append_kv(2, ptr, nodekey, &[0;1]);    
            }
            let rootPtr = self.context.add(root);
            self.context.set_root(rootPtr);
            },
            None => {}
        }
        _ = self.context.del(oldRootPtr);
    }

    // insert a KV into a node, the result might be split into 2 nodes.
    // the caller is responsible for deallocating the input node
    // and splitting and allocating result nodes.
    fn treeInsert<T:BNodeReadInterface>(&mut self, oldNode:&T,request:&mut InsertReqest) -> Option<BNode> {
        // where to insert the key?
        let idx = oldNode.nodeLookupLE(request.Key);
        //println!("Find  Key:{:?} Index:{:?}", key, idx );
        // act depending on the node type
        let mut newNode = BNode::new(2 * crate::btree::BTREE_PAGE_SIZE);
        match oldNode.btype() {
            crate::btree::BNODE_LEAF => {
                // leaf, node.getKey(idx) <= key
                let comp = crate::btree::util::compare_arrays(request.Key, oldNode.get_key(idx));
                if  comp == 0 {
                    if request.Mode == crate::btree::MODE_INSERT_ONLY 
                    {
                        return None;
                    }
                    // found the key, update it.
                    newNode.leaf_update(oldNode, idx, request.Key, request.Val);
                    let v = oldNode.get_val(idx);
                    request.OldValue.extend_from_slice(v);
                    request.Added = false;
                    request.Updated = true;
                } 
                else {
                    if request.Mode == crate::btree::MODE_UPDATE_ONLY {
                        return None;
                    }
                    // insert it after the position.
                    newNode.leaf_insert(oldNode, idx + 1,  request.Key, request.Val);
                    request.Added = true;
                    request.Updated = true;
                }
            },
            crate::btree::BNODE_NODE => {
                // internal node, insert it to a kid node.
                self.nodeInsert(& mut newNode, oldNode, idx, request);
            },
            other => {
                panic!("Exception Insert Node!\n");
            },
        }
        return Some(newNode);
    }

     // part of the treeInsert(): KV insertion to an internal node
     fn nodeInsert<T:BNodeReadInterface>(&mut self, newNode: &mut BNode, oldNode: &T, idx:u16, request: &mut InsertReqest) {
        //get and deallocate the kid node
        let kptr = oldNode.get_ptr(idx as usize);
        let mut knode = self.context.get(kptr).unwrap();

        let insertNode = self.treeInsert(&knode, request);
        match insertNode{            
            Some(node) => {
                let mut nodes = Vec::new();
                let (_,n1,n2,n3) = node.nodeSplit3();
                match n1
                {
                    Some(subn) => {
                        let ptr = self.context.add(subn);
                        let subnode = self.context.get(ptr).unwrap();
                        let key = subnode.get_key(0);
                        nodes.push((ptr,key.to_vec()));
                    },
                    None => {}
                }
                match n2
                {
                    Some(subn) => {
                        let ptr = self.context.add(subn);
                        let subnode = self.context.get(ptr).unwrap();
                        let key = subnode.get_key(0);
                        nodes.push((ptr,key.to_vec()));
                    },
                    None => {}
                }
                match n3
                {
                    Some(subn) => {
                        let ptr = self.context.add(subn);
                        let subnode = self.context.get(ptr).unwrap();
                        let key = subnode.get_key(0);
                        nodes.push((ptr,key.to_vec()));
                    },
                    None => {}
                }
                newNode.nodeReplaceKidN(oldNode, idx, nodes);
            },
            None => {}
        }
        //_ = self.context.del(kptr);
        //std.debug.print("Split Count:{d}", .{subNodes.Count});
    }


    pub fn print(&self){
        let root = self.context.get_root();
        println!("BTree content: Root:{:?} \n", root);
        if root == 0
        {
            return;
        }

        let nodeRoot = self.context.get(root);
        match nodeRoot{
            Some(r) => self.printNode(&r),
            None => println!("Root is not set!")
        }
        println!();
    }

    fn printNode<T:BNodeReadInterface>(&self, treenode: &T) {
        if treenode.btype() == crate::btree::BNODE_LEAF {
            treenode.print();
        } else if treenode.btype() == crate::btree::BNODE_FREE_LIST {
            //treenode.print();
        } else {
            treenode.print();
            let nkeys = treenode.nkeys();
            println!("NKeys {:?}", nkeys);
            let mut idx: u16 = 0;
            while idx < nkeys {
                let prtNode = treenode.get_ptr(idx as usize);
                let subNode = self.context.get(prtNode);
                match subNode{
                    Some(r) => self.printNode(&r),
                    None => println!("Root is not set!")
                }
                idx = idx + 1;
            }
        }
    }

}


#[cfg(test)]
mod tests {

    use std::sync::{Arc, RwLock};
    use crate::btree::{table::value::ValueType, tx::{ shared::Shared, winmmap::Mmap}, BTREE_PAGE_SIZE};
    use super::*;

    #[test]
    fn test_database_byIndexes()
    {
        let tables = Arc::new(RwLock::new(HashMap::new()));
        tables.write().unwrap().insert("@meta".as_bytes().to_vec(),TDEF_META.clone());
        tables.write().unwrap().insert("@table".as_bytes().to_vec(),TDEF_TABLE.clone());

        let mut data: Vec<u8> = vec![0; BTREE_PAGE_SIZE*2];
        let mut tx = prepaircase_nonefreelist_noneNode(&mut data);
        let mut dbinstance = txwriter{
            context:tx,
            tables:tables.clone()
        };

        let mut table = TableDef{
            Prefix:0,
            Name: "person".as_bytes().to_vec(),
            Types : vec!["BYTES".into(), "BYTES".into(),"BYTES".into(), "INT16".into(), "BOOL".into() ] ,
            Cols : vec!["id".as_bytes().to_vec() , "name".as_bytes().to_vec(),"address".as_bytes().to_vec(),"age".as_bytes().to_vec(),"married".as_bytes().to_vec() ] ,
            PKeys : 0,
            Indexes : vec![vec!["address".as_bytes().to_vec() , "married".as_bytes().to_vec()],vec!["name".as_bytes().to_vec()]],
            IndexPrefixes : vec![],
        };
        //table.FixIndexes();

        let ret = dbinstance.AddTable(&mut table);
        if let Err(ret) = ret
        {
            println!("Error when add table:{}",ret);
        }

        let ret = dbinstance.getTableDef("person".as_bytes());
        if let Some(tdef) = ret
        {
            println!("Table define:{}",tdef);
            let mut r = Record::new(&tdef);

            for i in 0..100 {
                r.Set("id".as_bytes(), Value::BYTES(format!("{}", i).as_bytes().to_vec()));
                r.Set( "name".as_bytes(), Value::BYTES(format!("Bob{}", i).as_bytes().to_vec()));
                r.Set("address".as_bytes(), Value::BYTES("Montrel Canada H9T 1R5".as_bytes().to_vec()));
                r.Set("age".as_bytes(), Value::INT16(20));
                r.Set("married".as_bytes(), Value::BOOL(false));

                dbinstance.UpdateRecord(&mut r,crate::btree::MODE_UPSERT);
            }

            r.Set("id".as_bytes(), Value::BYTES(("21").as_bytes().to_vec()));
            r.Set( "name".as_bytes(), Value::BYTES(("Bob504").as_bytes().to_vec()));
            r.Set("address".as_bytes(), Value::BYTES("Montrel Canada H9T 1R5".as_bytes().to_vec()));
            r.Set("age".as_bytes(), Value::INT16(20));
            r.Set("married".as_bytes(), Value::BOOL(false));

            dbinstance.UpdateRecord(&mut r,crate::btree::MODE_UPSERT);


            r.Set("id".as_bytes(), Value::BYTES(("22").as_bytes().to_vec()));
            dbinstance.DeleteRecord(&mut r);

            let mut key1 = Record::new(&tdef);
            let mut key2 = Record::new(&tdef);
            key1.Set("name".as_bytes(), Value::BYTES("Bob1".as_bytes().to_vec()));
            key2.Set("name".as_bytes(), Value::BYTES("Bob5".as_bytes().to_vec()));
            //let mut scanner = dbinstance.Seek(1,OP_CMP::CMP_GT, OP_CMP::CMP_LE, &key1, &key2);
            let mut scanner = dbinstance.Scan(OP_CMP::CMP_GT, Some(OP_CMP::CMP_LE), &key1, Some(&key2));
    
            let mut r3 = Record::new(&tdef);
            match &mut scanner {
                Ok(cursor) =>{
                    cursor.into_iter().for_each(|v| println!("{}",v));
                },
                Err(err) => { println!("Error when add table:{}",err)}
                
            }    
        }
    }

    #[test]
    fn test_table()
    {
        let tables = Arc::new(RwLock::new(HashMap::new()));
        tables.write().unwrap().insert("@meta".as_bytes().to_vec(),TDEF_META.clone());
        tables.write().unwrap().insert("@table".as_bytes().to_vec(),TDEF_TABLE.clone());

        let mut data: Vec<u8> = vec![0; BTREE_PAGE_SIZE*2];
        let mut tx = prepaircase_nonefreelist_noneNode(&mut data);
        let mut txwriter = txwriter{
            context:tx,
            tables:tables.clone()
        };


        let mut table = TableDef{
            Prefix:0,
            Name: "person".as_bytes().to_vec(),
            Types : vec!["BYTES".into(), "BYTES".into(),"BYTES".into(), "INT16".into(), "BOOL".into() ] ,
            Cols : vec!["id".as_bytes().to_vec() , "name".as_bytes().to_vec(),"address".as_bytes().to_vec(),"age".as_bytes().to_vec(),"married".as_bytes().to_vec() ] ,
            PKeys : 0,
            Indexes : vec![vec!["address".as_bytes().to_vec() , "married".as_bytes().to_vec()],vec!["age".as_bytes().to_vec()]],
            IndexPrefixes : vec![],
        };
        //table.FixIndexes();

        let ret = txwriter.AddTable(&mut table);
        assert!(ret.is_ok());
        if let Err(ret) = ret
        {
            println!("Error when add table:{}",ret);
        }

        let ret = txwriter.getTableDef("person".as_bytes());
        if let Some(tdef1) = ret
        {
            //println!("Table define:{}",tdef);
            let mut r = Record::new(&tdef1);

            for i in 0..100 {
                r.Set("id".as_bytes(), Value::BYTES(format!("{}", i).as_bytes().to_vec()));
                r.Set( "name".as_bytes(), Value::BYTES(format!("Bob{}", i).as_bytes().to_vec()));
                r.Set("address".as_bytes(), Value::BYTES("Montrel Canada H9T 1R5".as_bytes().to_vec()));
                r.Set("age".as_bytes(), Value::INT16(20));
                r.Set("married".as_bytes(), Value::BOOL(false));

                txwriter.UpdateRecord(&mut r, MODE_UPSERT);
            }
    
            let mut key1 = Record::new(&tdef1);
            let mut key2 = Record::new(&tdef1);
            key1.Set("id".as_bytes(), Value::BYTES("2".as_bytes().to_vec()));
            key2.Set("id".as_bytes(), Value::BYTES("5".as_bytes().to_vec()));
            let mut scanner = txwriter.SeekRecord(-1,OP_CMP::CMP_GE, Some(OP_CMP::CMP_LE), &key1, Some(&key2));
    
            let mut r3 = Record::new(&tdef1);
            match &mut scanner {
                Ok(cursor) =>{
                    cursor.into_iter().for_each(|v| println!("{}",v));

                    // while cursor.Valid(){
                    //         cursor.Deref(&txwriter,&mut r3);
                    //         println!("{}", r3);
                    //         cursor.Next();
                    //     }                
                },
                Err(err) => { println!("Error Get Cursor:{}",err)}
                
            }
    
        }
    }

    #[test]
    fn test_seek()
    {
        let mut data: Vec<u8> = vec![0; BTREE_PAGE_SIZE*2];
        let mut tx = prepaircase_nonefreelist_noneNode(&mut data);
        let tables = Arc::new(RwLock::new(HashMap::new()));
        let mut txwriter = txwriter{
            context:tx,
            tables:tables.clone()
        };

        let mut request = InsertReqest::new("3".as_bytes(), "33333".as_bytes(), crate::btree::MODE_UPSERT);
        txwriter.Set(&mut request);
        let mut request = InsertReqest::new("1".as_bytes(), "11111".as_bytes(), crate::btree::MODE_UPSERT);
        txwriter.Set(&mut request);
        let mut request = InsertReqest::new("7".as_bytes(), "77777".as_bytes(), crate::btree::MODE_UPSERT);
        txwriter.Set(&mut request);
        let mut request = InsertReqest::new("5".as_bytes(), "55555".as_bytes(), crate::btree::MODE_UPSERT);
        txwriter.Set(&mut request);

        let it = txwriter.Seek("3".as_bytes(), OP_CMP::CMP_LT);
        let ret = it.Deref();
        println!("\nLess Then => Key:{} Value:{} \n", String::from_utf8(ret.0.to_vec()).unwrap(), String::from_utf8(ret.1.to_vec()).unwrap());
    
        let it2 = txwriter.Seek("3".as_bytes(), OP_CMP::CMP_LE);
        let ret2 = it2.Deref();
        println!("Less and Equal => Key:{} Value:{} \n", String::from_utf8(ret2.0.to_vec()).unwrap(), String::from_utf8(ret2.1.to_vec()).unwrap());

        let it3 = txwriter.Seek("3".as_bytes(), OP_CMP::CMP_GT);
        let ret3 = it3.Deref();
        println!("Large Than => Key:{} Value:{} \n", String::from_utf8(ret3.0.to_vec()).unwrap(), String::from_utf8(ret3.1.to_vec()).unwrap());

        let it4 = txwriter.Seek("3".as_bytes(), OP_CMP::CMP_GE);
        let ret4 = it4.Deref();
        println!("Large and Equal => Key:{} Value:{} \n", String::from_utf8(ret4.0.to_vec()).unwrap(), String::from_utf8(ret4.1.to_vec()).unwrap());


        //Test SeekLE
        let mut itLe = txwriter.SeekLE("3".as_bytes());

        let mut retLe = itLe.Deref();
        println!("Key:{} Value:{} \n", String::from_utf8(retLe.0.to_vec()).unwrap(), String::from_utf8(retLe.1.to_vec()).unwrap());

        if itLe.Prev() {
            retLe = itLe.Deref();
            println!("Key:{} Value:{} \n", String::from_utf8(retLe.0.to_vec()).unwrap(), String::from_utf8(retLe.1.to_vec()).unwrap());
        }
    
        if itLe.Prev() {
            retLe = itLe.Deref();
            println!("Key:{} Value:{} \n", String::from_utf8(retLe.0.to_vec()).unwrap(), String::from_utf8(retLe.1.to_vec()).unwrap());
        }

        if itLe.Prev() {
            retLe = itLe.Deref();
            println!("Key:{} Value:{} \n", String::from_utf8(retLe.0.to_vec()).unwrap(), String::from_utf8(retLe.1.to_vec()).unwrap());
        }

        if itLe.Next() {
            retLe = itLe.Deref();
            println!("Key:{} Value:{} \n", String::from_utf8(retLe.0.to_vec()).unwrap(), String::from_utf8(retLe.1.to_vec()).unwrap());
        }
        if itLe.Next() {
            retLe = itLe.Deref();
            println!("Key:{} Value:{} \n", String::from_utf8(retLe.0.to_vec()).unwrap(), String::from_utf8(retLe.1.to_vec()).unwrap());
        }
        if itLe.Next() {
            retLe = itLe.Deref();
            println!("Key:{} Value:{} \n", String::from_utf8(retLe.0.to_vec()).unwrap(), String::from_utf8(retLe.1.to_vec()).unwrap());
        }
        if itLe.Next() {
            retLe = itLe.Deref();
            println!("Key:{} Value:{} \n", String::from_utf8(retLe.0.to_vec()).unwrap(), String::from_utf8(retLe.1.to_vec()).unwrap());
        }
        if itLe.Next() {
            retLe = itLe.Deref();
            println!("Key:{} Value:{} \n", String::from_utf8(retLe.0.to_vec()).unwrap(), String::from_utf8(retLe.1.to_vec()).unwrap());
        }

    }


    #[test]
    fn test_txwriter()
    {
        let mut data: Vec<u8> = vec![0; BTREE_PAGE_SIZE*2];
        let mut tx = prepaircase_nonefreelist_noneNode(&mut data);
        let tables = Arc::new(RwLock::new(HashMap::new()));
        let mut txwriter = txwriter{
            context:tx,
            tables:tables.clone(),
        };
        
        let mut request = InsertReqest::new("1".as_bytes(), &[31;2500], crate::btree::MODE_UPSERT);
        txwriter.Set(&mut request);
        let mut request = InsertReqest::new("2".as_bytes(), &[32;2500], crate::btree::MODE_UPSERT);
        txwriter.Set(&mut request);
        let mut request = InsertReqest::new("hello".as_bytes(),  "rust".as_bytes(), crate::btree::MODE_UPSERT);
        txwriter.Set(&mut request);
        let mut request = InsertReqest::new("3".as_bytes(), &[33;2500], crate::btree::MODE_UPSERT);
        txwriter.Set(&mut request);
        let mut request = InsertReqest::new("4".as_bytes(), &[34;2500], crate::btree::MODE_UPSERT);
        txwriter.Set(&mut request);

        let v = txwriter.Get("hello".as_bytes());
        match(v)
        {
            Some(s) => println!("{0}",String::from_utf8(s).unwrap()),
            None=> {}
        }

        let mut request = DeleteRequest::new("2".as_bytes());
        let r1 = txwriter.Delete(&mut request);
        assert_eq!(true,r1);

        let mut request = DeleteRequest::new("2".as_bytes());
        let r2 = txwriter.Delete(&mut request);
        assert_eq!(false,r2);

        let r3 = txwriter.Get("2".as_bytes());
        assert_eq!(true,r3.is_none());

        //txwriter.print();
    }

    #[test]
    fn test_set_delete()
    {
        let mut data: Vec<u8> = vec![0; BTREE_PAGE_SIZE*2];
        let mut tx = prepaircase_nonefreelist_noneNode(&mut data);
        let tables = Arc::new(RwLock::new(HashMap::new()));

        let mut txwriter = txwriter{
            context:tx,
            tables:tables.clone(),
        };

        let mut request = InsertReqest::new("3".as_bytes(), "33333".as_bytes(), crate::btree::MODE_UPSERT);
        txwriter.Set(&mut request);
        let mut request = InsertReqest::new("1".as_bytes(), "11111".as_bytes(), crate::btree::MODE_UPSERT);
        txwriter.Set(&mut request);
        let mut request = InsertReqest::new("7".as_bytes(), "77777".as_bytes(), crate::btree::MODE_UPSERT);
        txwriter.Set(&mut request);
        let mut request = InsertReqest::new("5".as_bytes(), "55555".as_bytes(), crate::btree::MODE_UPSERT);
        txwriter.Set(&mut request);

        let mut reqUpdate = InsertReqest::new("1".as_bytes(),"rust".as_bytes(),crate::btree::MODE_UPSERT);
        txwriter.Set(&mut reqUpdate);
        assert_eq!("11111".as_bytes(),reqUpdate.OldValue);

        let mut reqDelete = DeleteRequest::new("3".as_bytes());
        txwriter.Delete(&mut reqDelete);
        assert_eq!("33333".as_bytes(),reqDelete.OldValue);
    }

    fn prepaircase_nonefreelist_noneNode(data:&mut Vec<u8>)->Tx
    {
        //master
        let mut master = BNode::new(BTREE_PAGE_SIZE);

        //root node
        let mut root = BNode::new(BTREE_PAGE_SIZE);
        root.set_header(crate::btree::BNODE_LEAF, 1);
        root.node_append_kv(0, 0, &[0;1], &[0;1]);

        data[0..BTREE_PAGE_SIZE].copy_from_slice(master.data());
        data[BTREE_PAGE_SIZE..2*BTREE_PAGE_SIZE].copy_from_slice(root.data());

        //println!("{:?}",data);
        let data_ptr: *mut u8 = data.as_mut_ptr();
        let mmap = Mmap { ptr: data_ptr, writer: Shared::new(())};
        let mmap =  Arc::new(RwLock::new(mmap));
        let tx = Tx::new(mmap,1,2,BTREE_PAGE_SIZE * 2, 
            0,1,1);

        tx
    }

}