

use crate::btree::btree::btreeinterface::BTreeInterface;
use crate::btree::btree::request::DeleteRequest;
use crate::btree::btree::request::InsertReqest;
use crate::btree::scan::comp::OP_CMP;
use crate::btree::scan::scaninterface::ScanInterface;
use crate::btree::table::record::Record;
use crate::btree::table::table::TableDef;
use crate::btree::table::value::Value;
use crate::btree::table::value::ValueType;
use crate::btree::BTreeError;
use crate::btree::kv::contextinterface::KVContextInterface;
use crate::btree::btree::btreeinterface::BTreeKVInterface;
use crate::btree::btree::btree::BTree;
use crate::btree::MODE_UPSERT;
use std::collections::HashMap;

use super::scanner::Scanner;
use super::INDEX_ADD;
use super::INDEX_DEL;

lazy_static! {
    static ref TDEF_META: TableDef = TableDef{
        Prefix:1,
        Name: "@meta".as_bytes().to_vec(),
        Types : vec![ValueType::BYTES, ValueType::BYTES] ,
        Cols : vec!["key".as_bytes().to_vec() , "val".as_bytes().to_vec() ] ,
        PKeys : 0,
        Indexes : vec![],
        IndexPrefixes : vec![],
    };

    static ref TDEF_TABLE: TableDef = TableDef{
        Prefix:2,
        Name: "@table".as_bytes().to_vec(),
        Types : vec![ValueType::BYTES, ValueType::BYTES] ,
        Cols : vec!["name".as_bytes().to_vec() , "def".as_bytes().to_vec() ] ,
        PKeys : 0,
        Indexes : vec![],
        IndexPrefixes : vec![],
    };
}

pub struct DataBase<'a> {
    btree: BTree<'a>,
    tables: HashMap<Vec<u8>,TableDef>,
}

impl<'a> DataBase<'a>{

    pub fn new(context:&'a mut dyn KVContextInterface) ->Self{
        let mut tables = HashMap::new();

        tables.insert("@meta".as_bytes().to_vec(),TDEF_META.clone());
        tables.insert("@table".as_bytes().to_vec(),TDEF_TABLE.clone());

        DataBase{
            btree:BTree::new(context),
            tables :tables,
        }
    }

    pub fn print(&self) {
        self.btree.print();
    }

    // pub fn SetEx(&mut self, rec: &Record, mode: u16) {
    //     self.dbUpdateEx(rec.def, rec, mode);
    // }

    pub fn Insert(&mut self, rec:&mut Record)->Result<(),BTreeError>{
        return self.Set(rec, crate::btree::MODE_INSERT_ONLY);
    }

    pub fn Update(&mut self, rec: &mut Record) ->Result<(),BTreeError>{
        return self.Set(rec, crate::btree::MODE_UPDATE_ONLY);
    }

    pub fn Upsert(&mut self, rec: &mut Record)->Result<(),BTreeError>{
        return self.Set(rec, crate::btree::MODE_UPSERT);
    }

    pub fn Get(&self, rec:&mut Record)->Result<bool,BTreeError> {
        return self.dbGet(rec);
    }

    // delete a record by its primary key
    pub fn Delete(&mut self, rec:&Record)->Result<bool,BTreeError> {
        
        let bCheck = rec.checkPrimaryKey();
        if (bCheck == false) {
            return Err(BTreeError::PrimaryKeyIsNotSet);
        }

        let mut key = Vec::new();
        rec.encodeKey(rec.def.Prefix, &mut key);

        return Ok(self.btree.Delete(&key));
    }

    pub fn Scan(&self, cmp1: OP_CMP, cmp2: OP_CMP, key1:&Record, key2:&Record)->Result<Scanner,BTreeError> 
    {
        if let Ok(indexNo) = key1.findIndexes()
        {
            return self.Seek(indexNo, cmp1, cmp2, key1, key2);
        }
        else {            
            return Err(BTreeError::IndexNotFoundError);
        }
    }

    pub fn Seek(&self,idxNumber:i16, cmp1: OP_CMP, cmp2: OP_CMP, key1:&Record, key2:&Record)->Result<Scanner,BTreeError> {
        
        // sanity checks
        if cmp1.value() > 0 && cmp2.value() < 0 
        {} 
        else if cmp2.value() > 0 && cmp1.value() < 0 
        {} 
        else {
            return Err(BTreeError::BadArrange);
        }

        let mut keyStart: Vec<u8> = Vec::new();
        let mut keyEnd: Vec<u8> = Vec::new();

        if idxNumber == -1
        {
            let bCheck1 = key1.checkPrimaryKey();
            if  bCheck1 == false {
                return Err(BTreeError::KeyError);
            }
            let bCheck2 = key2.checkPrimaryKey();
            if  bCheck2 == false {
                return Err(BTreeError::KeyError);
            }
    
            key1.encodeKey(key1.def.Prefix, &mut keyStart);
            key2.encodeKey(key2.def.Prefix, &mut keyEnd);
        }
        else {
            key1.encodeKeyPartial(idxNumber as usize,&mut keyStart,);
            key2.encodeKeyPartial(idxNumber as usize,&mut keyEnd);
            println!("KeyStart:{:?}  KeyEnd:{:?}",keyStart,keyEnd);
        }

        let iter = self.btree.Seek(&keyStart, cmp1);
        if iter.Valid() == false
        {
            return Err(BTreeError::NextNotFound);
        }
        Ok(
            Scanner::new(idxNumber,cmp1,cmp2,keyStart,keyEnd,iter)
        )
    }



    //add a record
    fn Set(&mut self, rec:&mut Record, mode: u16)->Result<(),BTreeError> {
        return self.dbUpdate(rec, mode);
    }

    // get a single row by the primary key
    pub fn dbGet(&self,rec:&mut Record)->Result<bool,BTreeError> {
        let bCheck = rec.checkPrimaryKey();
        if bCheck == false {
            return Err(BTreeError::PrimaryKeyIsNotSet);
        }

        let mut list:Vec<u8> = Vec::new();
        rec.encodeKey(rec.def.Prefix,&mut list);

        let val = self.btree.Get(&list);
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

        self.btree.Set(&key, &v, mode);
        return Ok(());
    }

    pub fn UpdateEx(&mut self, rec:&mut Record, mode: u16) -> Result<(),BTreeError> {

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
        self.btree.SetEx(&mut request);

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

    pub fn DeleteEx(&mut self, rec:&Record)->Result<bool,BTreeError> {
        
        let bCheck = rec.checkPrimaryKey();
        if (bCheck == false) {
            return Err(BTreeError::PrimaryKeyIsNotSet);
        }

        let mut key = Vec::new();
        rec.encodeKey(rec.def.Prefix, &mut key);

        let mut request = DeleteRequest::new(&key);
        let ret = self.btree.DeleteEx(&mut request);
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

    pub fn indexOp(& mut self, rec: &mut Record, op: u16) -> Result<(),BTreeError> {

        for i in 0..rec.def.Indexes.len(){

            let mut index = Vec::new();
            rec.encodeIndex(rec.def.IndexPrefixes[i], i, &mut index);
            //println!("Rec:{}",rec);
            //println!("Index :{}\n  Vals Result:{:?} ", i, index);
            if op == INDEX_ADD {
                let mut request = InsertReqest::new( &index ,&[0;1], MODE_UPSERT);
                self.btree.SetEx(&mut request);
            } 
            else if op == INDEX_DEL 
            {
                let mut reqDelete = DeleteRequest::new(&index);
                self.btree.DeleteEx(&mut reqDelete);
            } 
            else {
                panic!("bad op value!");
            }
        }

        Ok(())
    }

    //add Table
    pub fn AddTable(&mut self, tdef:&mut TableDef)-> Result<(),BTreeError>{
        //tableDefCheck(tdef);

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

    //get Table Define
    pub fn getTableDefFromDB(&self, name: &[u8])->Option<TableDef> {

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

    // get the table definition by name
    pub fn getTableDef(&mut self, name: &[u8]) -> Option<TableDef> {
        let v = self.tables.get(name);
        if let Some(def) = v
        {
            return Some(def.clone());
        }

        let defParsed =  self.getTableDefFromDB(name);
        if let Some(def) = defParsed
        {
            self.tables.insert(name.to_vec(), def.clone());
            return Some(def);
        }

        return None;
    }
}

#[cfg(test)]
mod tests {
    use crate::btree::kv::windowsfilecontext::WindowsFileContext;

    use super::*;

    #[test]
    fn test_database()
    {
        let mut context = crate::btree::kv::memorycontext::MemoryContext::new();
        let mut dbinstance = DataBase::new(&mut context);

        let mut table = TableDef{
            Prefix:0,
            Name: "person".as_bytes().to_vec(),
            Types : vec![ValueType::BYTES, ValueType::BYTES,ValueType::BYTES, ValueType::INT16, ValueType::BOOL ] ,
            Cols : vec!["id".as_bytes().to_vec() , "name".as_bytes().to_vec(),"address".as_bytes().to_vec(),"age".as_bytes().to_vec(),"married".as_bytes().to_vec() ] ,
            PKeys : 0,
            Indexes : vec![vec!["address".as_bytes().to_vec() , "married".as_bytes().to_vec()],vec!["age".as_bytes().to_vec()]],
            IndexPrefixes : vec![],
        };
        //table.FixIndexes();

        let ret = dbinstance.AddTable(&mut table);
        if let Err(ret) = ret
        {
            println!("Error when add table:{}",ret);
        }

        let ret = dbinstance.getTableDef("person".as_bytes());
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

                dbinstance.Insert(&mut r);
            }
    
            let mut key1 = Record::new(&tdef1);
            let mut key2 = Record::new(&tdef1);
            key1.Set("id".as_bytes(), Value::BYTES("2".as_bytes().to_vec()));
            key2.Set("id".as_bytes(), Value::BYTES("5".as_bytes().to_vec()));
            let mut scanner = dbinstance.Seek(-1,OP_CMP::CMP_GE, OP_CMP::CMP_LE, &key1, &key2);
    
            let mut r3 = Record::new(&tdef1);
            match &mut scanner {
                Ok(cursor) =>{
                    while cursor.Valid(){
                            cursor.Deref(&dbinstance,&mut r3);
                            println!("{}", r3);
                            cursor.Next();
                        }                
                },
                Err(err) => { println!("Error Get Cursor:{}",err)}
                
            }
    
        }

        // let ret = dbinstance.AddTable(&mut table);
        // if let Err(ret) = ret
        // {
        //     println!("Error when add table:{}",ret);
        // }


    }
    
    #[test]
    fn test_database_byIndexes()
    {
        let mut context = crate::btree::kv::memorycontext::MemoryContext::new();
        let mut dbinstance = DataBase::new(&mut context);

        let mut table = TableDef{
            Prefix:0,
            Name: "person".as_bytes().to_vec(),
            Types : vec![ValueType::BYTES, ValueType::BYTES,ValueType::BYTES, ValueType::INT16, ValueType::BOOL ] ,
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

                dbinstance.UpdateEx(&mut r,crate::btree::MODE_UPSERT);
            }

            r.Set("id".as_bytes(), Value::BYTES(("21").as_bytes().to_vec()));
            r.Set( "name".as_bytes(), Value::BYTES(("Bob504").as_bytes().to_vec()));
            r.Set("address".as_bytes(), Value::BYTES("Montrel Canada H9T 1R5".as_bytes().to_vec()));
            r.Set("age".as_bytes(), Value::INT16(20));
            r.Set("married".as_bytes(), Value::BOOL(false));

            dbinstance.UpdateEx(&mut r,crate::btree::MODE_UPSERT);


            r.Set("id".as_bytes(), Value::BYTES(("22").as_bytes().to_vec()));
            dbinstance.DeleteEx(&mut r);

            let mut key1 = Record::new(&tdef);
            let mut key2 = Record::new(&tdef);
            key1.Set("name".as_bytes(), Value::BYTES("Bob1".as_bytes().to_vec()));
            key2.Set("name".as_bytes(), Value::BYTES("Bob5".as_bytes().to_vec()));
            //let mut scanner = dbinstance.Seek(1,OP_CMP::CMP_GT, OP_CMP::CMP_LE, &key1, &key2);
            let mut scanner = dbinstance.Scan(OP_CMP::CMP_GT, OP_CMP::CMP_LE, &key1, &key2);
    
            let mut r3 = Record::new(&tdef);
            match &mut scanner {
                Ok(cursor) =>{
                    while cursor.Valid(){
                            cursor.Deref(&dbinstance,&mut r3);
                            println!("{}", r3);
                            cursor.Next();
                        }                
                },
                Err(err) => { println!("Error when add table:{}",err)}
                
            }    
        }
    }

    #[test]
    fn test_windows_database()
    {
        let mut context = WindowsFileContext::new("c:/temp/rustdb.dat".as_bytes(),4096,1000);
        
        if let Ok(mut dbContext) = context
        {
            dbContext.open();
            let mut dbinstance = DataBase::new(&mut dbContext);

            let mut table = TableDef{
                Prefix:0,
                Name: "person".as_bytes().to_vec(),
                Types : vec![ValueType::BYTES, ValueType::BYTES,ValueType::BYTES, ValueType::INT16, ValueType::BOOL ] ,
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

                    dbinstance.UpdateEx(&mut r,crate::btree::MODE_UPSERT);
                }

                r.Set("id".as_bytes(), Value::BYTES(("21").as_bytes().to_vec()));
                r.Set( "name".as_bytes(), Value::BYTES(("Bob504").as_bytes().to_vec()));
                r.Set("address".as_bytes(), Value::BYTES("Montrel Canada H9T 1R5".as_bytes().to_vec()));
                r.Set("age".as_bytes(), Value::INT16(20));
                r.Set("married".as_bytes(), Value::BOOL(false));

                dbinstance.UpdateEx(&mut r,crate::btree::MODE_UPSERT);


                r.Set("id".as_bytes(), Value::BYTES(("22").as_bytes().to_vec()));
                dbinstance.DeleteEx(&mut r);

                let mut key1 = Record::new(&tdef);
                let mut key2 = Record::new(&tdef);
                key1.Set("name".as_bytes(), Value::BYTES("Bob1".as_bytes().to_vec()));
                key2.Set("name".as_bytes(), Value::BYTES("Bob5".as_bytes().to_vec()));
                //let mut scanner = dbinstance.Seek(1,OP_CMP::CMP_GT, OP_CMP::CMP_LE, &key1, &key2);
                let mut scanner = dbinstance.Scan(OP_CMP::CMP_GT, OP_CMP::CMP_LE, &key1, &key2);
        
                let mut r3 = Record::new(&tdef);
                match &mut scanner {
                    Ok(cursor) =>{
                        while cursor.Valid(){
                                cursor.Deref(&dbinstance,&mut r3);
                                println!("{}", r3);
                                cursor.Next();
                            }                
                    },
                    Err(err) => { println!("Error when add table:{}",err)}
                    
                }    
            }
        }
        else {
            println!("Open Database File Error");
        }
    }

}