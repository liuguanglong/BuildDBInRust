

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
        pub fn dbUpdate(&mut self,rec:&mut Record, mode: u16) -> Result<(),BTreeError> {

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
        if let Ok(_) = ret1
        {
            return Err(BTreeError::TableAlreadyExist);
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
                if let Some(val) = v
                {
                    if let Value::BYTES(str) = val
                    {
                        tdef.Prefix = u32::from_le_bytes(str.try_into().unwrap());
                    }
                }
            }
        }

        tdef.Prefix += 1;

        let nPrefix: u32 = tdef.Indexes.len() as u32 + tdef.Prefix as u32 + 1;
        rMeta.Set("val".as_bytes(), Value::BYTES(nPrefix.to_le_bytes().to_vec()));
        self.dbUpdate(&mut rMeta, 0);

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

