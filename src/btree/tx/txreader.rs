use std::{collections::HashMap, sync::{Arc, RwLock, RwLockReadGuard}};

use crate::btree::{db::TDEF_TABLE, kv::{node::BNode, nodeinterface::{BNodeReadInterface, BNodeWriteInterface}}, parser::{expr::Expr, lib::Parser, select::SelectExpr, statement::{ExprSQL, ExprSQLList, SQLExpr}}, scan::{biter::BIter, comp::OP_CMP}, table::{record::Record, table::TableDef, value::Value}, BTreeError, BTREE_PAGE_SIZE};
use super::{txRecord::{DataRow, DataTable}, txScanner::TxScanner, txbiter::TxBIter, txinterface::{DBReadInterface, TxReadContext, TxReaderInterface}, winmmap::Mmap};

pub struct TxReader{
    data:Arc<RwLock<Mmap>>,
    pub tables: Arc<RwLock<HashMap<Vec<u8>,TableDef>>>,
    root: u64,
    pub version:u64,
    pub index:usize,
    len:usize
}

impl DBReadInterface for TxReader{
    fn Scan(&self, cmp1: OP_CMP, cmp2: Option<OP_CMP>, key1:&Record, key2:Option<&Record>)->Result<TxScanner,BTreeError> {
        if let Ok(indexNo) = key1.findIndexes()
        {
            return self.SeekRecord(indexNo, cmp1, cmp2, key1, key2);
        }
        else {            
            return Err(BTreeError::IndexNotFoundError);
        }
    }
}

impl TxReader{
    pub fn new(data:Arc<RwLock<Mmap>>,root:u64,len:usize,version:u64,index:usize,tables: Arc<RwLock<HashMap<Vec<u8>,TableDef>>>) -> TxReader{
        TxReader{
            data:data,
            len:len,
            root:root,
            version:version,
            index:index,
            tables:tables,
        }
    }

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
                       return Err(BTreeError::BadSQLStatement);
                    },
                }                 
            }
        }
        Ok(list)
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
        if let Ok((key1,key2,cmp1,cmp2)) = cmd.Scan.createScan(&tdef)
        {
            let mut index:usize = 0;
            let mut count:usize = 0;
            let mut scanner = self.Scan( cmp1, cmp2, &key1, key2.as_ref());
            match &mut scanner {
                Ok(cursor) =>{
                    cursor.into_iter()
                    .filter(|x| 
                        {
                                if let Some(filter) = &cmd.Scan.Filter
                                {
                                   Self::evalFilterExpr(&filter, &tdef,&x)
                                }
                                else {
                                    true
                                }
                        })
                    .skip(cmd.Scan.Offset)
                    .take(cmd.Scan.Limit)
                    .for_each(|x| txTable.Rows.push(x));
                },
                Err(err) => { return Err(BTreeError::NextNotFound)}
            }
        }

        if txTable.Rows.len() > 0
        {
            for v in &txTable.Rows.get(0).as_ref().unwrap().Vals
            {   
                txTable.Types.push(v.GetValueType());
            }
        }

        Ok(txTable)

    }

    fn evalFilterExpr(expr:&Expr,tdef:&TableDef,rc:&DataRow)->bool
    {
        //Todo
        let mut rc = Record::new(&tdef);
        rc.Vals = rc.Vals.clone();

        if let Ok(Value::BOOL(true)) = expr.eval(&rc)
        {
            return true;
        }
        false
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
            //println!("KeyStart:{:?}  KeyEnd:{:?}",keyStart,keyEnd);
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
    
    fn SeekLE(&self, key:&[u8]) -> TxBIter
    {
        let mut iter = TxBIter::new(self);

        let mut ptr = self.get_root();
        let mut n = self.get(ptr).unwrap();
        let mut idx: usize = 0;
        while (ptr != 0) {
            n = self.get(ptr).unwrap();
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

        if let tbs = self.tables.read().unwrap()
        {
            if let Some(def) = tbs.get(name)
            {
                return Some(def.clone());
            }
        }

        let defParsed =  self.getTableDefFromDB(name);
        if let Some(def) = defParsed
        {
            self.tables.write().unwrap().insert(name.to_vec(), def.clone());
            return Some(def);
        }

        return None;
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
                let subNode = self.get(ptr);
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
}

impl TxReaderInterface for TxReader{

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
        let rootNode = self.get(self.get_root());
        match rootNode{
            Some(root) => return self.treeSearch(&root,key),
            None => return None
        }
    }

    fn Seek(&self, key:&[u8], cmp:crate::btree::scan::comp::OP_CMP) -> TxBIter {
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

impl TxReadContext for TxReader{    
    fn get_root(&self)->u64{
        return self.root;
    }

    fn get(&self,key:u64) -> Option<BNode>{
        let offset = key as usize * BTREE_PAGE_SIZE;
        assert!(offset + BTREE_PAGE_SIZE <= self.len);
        
        if let Ok(mmap) = self.data.read(){

            let mut newNode = BNode::new(BTREE_PAGE_SIZE);
            //println!("index:{}",key);
            newNode.copy_Content(mmap.ptr, offset, BTREE_PAGE_SIZE);
            drop(mmap);
            //newNode.copy_Data(&self.data,offset,BTREE_PAGE_SIZE);
            //newNode.print();
            return Some(newNode);    

        }
        println!("Get Lock Error!");
        None
    }
}

