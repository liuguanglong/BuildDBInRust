

use crate::btree::scan::comp::OP_CMP;
use crate::btree::table::record::Record;
use crate::btree::table::table::TableDef;
use crate::btree::table::value::Value;
use crate::btree::table::value::ValueType;
use crate::btree::BTreeError;
use crate::btree::kv::contextinterface::KVContextInterface;
use crate::btree::btree::btreeinterface::BTreeKVInterface;
use crate::btree::btree::btree::BTree;
use std::collections::HashMap;

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

    // pub fn Seek(&mut self, key1: &Record, cmp1: OP_CMP, key2: &Record, cmp2: OP_CMP)->Scanner {
    //     var scanner1 = try scanner.Scanner.createScanner(self.allocator, cmp1, cmp2, key1, key2);
    //     try scanner1.Seek(self.kv);
    //     return scanner1;
    // }

    //add a record
    fn Set(&mut self, rec:&mut Record, mode: u16)->Result<(),BTreeError> {
        return self.dbUpdate(rec, mode);
    }

    // get a single row by the primary key
    fn dbGet(&self,rec:&mut Record)->Result<bool,BTreeError> {
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
        if let Some(tdef) = ret
        {
            println!("Table define:{}",tdef);
        }

        let ret = dbinstance.AddTable(&mut table);
        if let Err(ret) = ret
        {
            println!("Error when add table:{}",ret);
        }



    }
    
}