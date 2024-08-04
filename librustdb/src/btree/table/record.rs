use crate::btree::scan::comp::OP_CMP;
use crate::btree::table::table::TableDef;
use crate::btree::table::value::Value;
use crate::btree::table::value::ValueType;
use std::fmt;
use std::panic;
use crate::btree::BTreeError;

//Record Table Row
pub struct Record<'a>{
    pub Vals: Vec<Value>,
    pub def: &'a TableDef,
}

impl<'a> fmt::Display for Record<'a> {

    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f,"Record Content:\n|");
        for i in 0..self.def.Cols.len()
        {
            write!(f,"{}|", String::from_utf8(self.def.Cols[i].to_vec()).unwrap());
        } 
        write!(f,"\n");
        for i in 0..self.def.Cols.len()
        {
            write!(f,"{}|", self.Vals[i]);
        } 
        write!(f,"\n")
    }
}

impl<'a> Record<'a> {
    pub fn new(def:&'a TableDef) -> Self{
        let mut vals = Vec::with_capacity(def.Cols.len());
        for i in 0..def.Cols.len()
        {
            vals.push(Value::None);
        }
        Record{
            Vals : vals,
            def : def
        }
    }

    pub fn Set(&mut self, key: &[u8], val: Value) -> Result<(),BTreeError>{
        let idx = self.GetColumnIndex(key);
        if let Some(i) = idx
        {
            match (&self.def.Types[i],val)
            {
                (ValueType::BYTES, val@Value::BYTES(_)) => self.Vals[i] = val,
                (ValueType::BYTES, val@Value::None) =>  self.Vals[i] = val,
                (ValueType::INT64, val@Value::INT64(_)) =>  self.Vals[i] = val,
                (ValueType::INT64, Value::INT32(v)) => self.Vals[i] = Value::INT64(v as i64),
                (ValueType::INT64, Value::INT16(v)) => self.Vals[i] = Value::INT64(v as i64),
                (ValueType::INT64, Value::INT8(v)) => self.Vals[i] = Value::INT64(v as i64),
                (ValueType::INT64, val@Value::None) =>  self.Vals[i] = val,
                (ValueType::INT32, Value::INT64(v)) => self.Vals[i] = Value::INT32(i32::try_from(v).unwrap_or_default()),
                (ValueType::INT32, val@Value::INT32(_)) =>  self.Vals[i] = val,
                (ValueType::INT32, Value::INT16(v)) => self.Vals[i] = Value::INT32(i32::try_from(v).unwrap_or_default()),
                (ValueType::INT32, Value::INT8(v)) => self.Vals[i] = Value::INT32(i32::try_from(v).unwrap_or_default()),
                (ValueType::INT32, val@Value::None) =>  self.Vals[i] = val,
                (ValueType::INT16, Value::INT64(v)) => self.Vals[i] = Value::INT16(i16::try_from(v).unwrap_or_default()),
                (ValueType::INT16, Value::INT32(v)) => self.Vals[i] = Value::INT16(i16::try_from(v).unwrap_or_default()),
                (ValueType::INT16, val@Value::INT16(_)) =>  self.Vals[i] = val,
                (ValueType::INT16, Value::INT8(v)) => self.Vals[i] = Value::INT16(i16::try_from(v).unwrap_or_default()),
                (ValueType::INT16, val@Value::None) =>  self.Vals[i] = val,
                (ValueType::INT8, Value::INT64(v)) => self.Vals[i] = Value::INT8(i8::try_from(v).unwrap_or_default()),
                (ValueType::INT8, Value::INT32(v)) => self.Vals[i] = Value::INT8(i8::try_from(v).unwrap_or_default()),
                (ValueType::INT8, Value::INT16(v)) => self.Vals[i] = Value::INT8(i8::try_from(v).unwrap_or_default()),
                (ValueType::INT8, val@Value::INT8(_)) =>  self.Vals[i] = val,
                (ValueType::INT8, val@Value::None) =>  self.Vals[i] = val,
                (ValueType::ID, val@Value::ID(_)) =>  self.Vals[i] = val,
                (ValueType::ID, val@Value::None) =>  self.Vals[i] = val,
                (ValueType::BOOL, val@Value::BOOL(_)) =>  self.Vals[i] = val,
                (ValueType::BOOL, val@Value::None) =>  self.Vals[i] = val,
                _Other => return Err(BTreeError::ValueTypeWrong(std::str::from_utf8(key).unwrap().to_string()))     
            };
            Ok(())
        }
        else {
            Err(BTreeError::ColumnNotFound(std::str::from_utf8(key).unwrap().to_string()))
        }
    }


    pub fn Get(&self, key: &[u8])-> Option<Value> {
        let idx = self.GetColumnIndex(key);
        match idx 
        {
            Some(i) => {
                return Some(self.Vals[i].clone());
            },
            None=>{ return None;}
        }
    }

    pub fn findIndexes(&self) -> Result<i16,BTreeError>
    {
        self.def.findIndexWithRecord(&self.Vals)
    }

    // check primaykey
    pub fn checkPrimaryKey(&self) -> bool {

        for i in 0..self.def.PKeys as usize + 1
        {
            if let Value::None = self.Vals[i]
            {
                return false;
            }
        }
        return true;
    }

    // check record
    pub fn checkRecord(&self) -> bool {
        for i in self.def.PKeys as usize + 1..self.def.Cols.len()
        {
            if let Value::None = self.Vals[i]
            {
                return false;
            }
        }
        return true;
    }

    // check Indexes
    pub fn checkIndexes(&self)->bool {

        for i in 0..self.def.Indexes.len()
        {
            for j in 0..self.def.Indexes[i].len()
            {
                let idx = self.GetColumnIndex(&self.def.Indexes[i][j]);
                match  idx {
                    Some(col) => {
                        if let Value::None = self.Vals[col]
                        {
                            return false;
                        }
                        return true;
                    },
                    _Other=>{return false;}
                }
            }
        }
        return true;
    }

     // check record
     pub fn checkVals(&self) -> bool {

        for i in 0..self.def.PKeys as usize + 1
        {
            if let Value::None = self.Vals[i]
            {
                return false;
            }           
        }
        return true;
    }

    fn GetColumnIndex(&self, key: &[u8])-> Option<usize> {
        self.def.GetColumnIndex(key)
    }

    // order-preserving encoding
    pub fn encodeValues(&self, list:&mut Vec<u8>) {
        let mut idx: usize = self.def.PKeys as usize + 1;
        while idx < self.def.Cols.len() 
        {
            self.encodeVal(idx, list);
            idx += 1;
        }
    }

    pub fn decodeValues(&mut self, list:&Vec<u8>) {
        let mut pos: usize = 0;
        let mut idx: usize = self.def.PKeys as usize + 1;
        while idx < self.def.Cols.len() 
        {
            pos = self.decodeVal(list, idx, pos);
            idx += 1;
        }
    }

    // order-preserving encoding
    fn encodeKeys(&self, list:&mut Vec<u8>) {

        for i in 0..self.def.PKeys as usize +1
        {
            self.encodeVal(i,list);            
        }
    }

    // for primary keys
    pub fn deencodeKey(&mut self, val: &[u8]) {
        let prefix = self.def.Prefix.to_le_bytes();

        assert!(prefix[0..prefix.len()] == val[0..prefix.len()]);

        let mut pos: usize = prefix.len() as usize;
        let mut idx: usize = 0;
        while idx <= self.def.PKeys as usize {
            pos = self.decodeVal(val, idx, pos);
            idx += 1;
        }
    }

    // for primary keys
    pub fn encodeKey(&self, prefix: u32, list:&mut Vec<u8>){
        list.extend_from_slice(&prefix.to_le_bytes());
        self.encodeKeys(list);
    }

    // order-preserving encoding
    pub fn encodeIndex(&self, prefix: u32, index: usize, list: &mut Vec<u8>) {

        let pValue: i32 = prefix as i32;
        list.extend_from_slice(&pValue.to_le_bytes());

        for i in 0..self.def.Indexes[index].len()
        {
            let idx = self.GetColumnIndex(&self.def.Indexes[index][i]);
            self.encodeVal(idx.unwrap(), list);
        }
    }

    // The range key can be a prefix of the index key,
    // we may have to encode missing columns to make the comparison work.
    pub fn encodeKeyPartial(&self,idx:usize, list: &mut Vec<u8>,cmp:&OP_CMP) 
    {
        list.extend_from_slice(&self.def.IndexPrefixes[idx].to_le_bytes());
        for x in &self.def.Indexes[idx]
        {
            if let Some(i) = self.GetColumnIndex(&x)
            {
                if let Value::None = self.Vals[i]
                {
                    // Encode the missing columns as either minimum or maximum values,
                    // depending on the comparison operator.
                    // 1. The empty string is lower than all possible value encodings,
                    // thus we don't need to add anything for CMP_LT and CMP_GE.
                    // 2. The maximum encodings are all 0xff bytes.
                    if *cmp == OP_CMP::CMP_GT || *cmp == OP_CMP::CMP_LE
                    {
                        match &self.def.Types[i]
                        {
                            ValueType::BOOL => {list.extend(&[0;1])},
                            ValueType::INT8 => {list.extend(&[0xff])},
                            ValueType::INT16 => {list.extend(&[0xff;2])},
                            ValueType::INT32 => {list.extend(&[0xff;4])},
                            ValueType::INT64 => {list.extend(&[0xff;8])},
                            ValueType::BYTES => { list.push(0xff)},
                            Other=> {panic!()}
                        }
                    }
                }
                else {
                    self.encodeVal(i,list);                 
                }
            }
            else {
                panic!("Column in indexes is not found!")
            }
        }
    }

    pub fn decodeKeyPartrial(&mut self,idxIndexes:usize,list:&[u8])
    {
        let mut pos: usize = 4;
        let mut idx: usize = 0;
        while idx < self.def.Indexes[idxIndexes].len() 
        {
           
            if let Some(i) = self.GetColumnIndex(&self.def.Indexes[idxIndexes][idx])
            {
                pos = self.decodeVal(list, i, pos);
                idx += 1;
            }
            else {
                panic!("Column in indexes is not found!")
            }
        }
    }

    fn encodeVal(&self, idx: usize, list:&mut Vec<u8>) {

        self.Vals[idx].encodeVal(list);
        // match &self.Vals[idx]
        //  {
        //     Value::INT8(v) => list.extend_from_slice(&v.to_le_bytes()),
        //     Value::INT16(v) => list.extend_from_slice(&v.to_le_bytes()),
        //     Value::INT32(v) => list.extend_from_slice(&v.to_le_bytes()),
        //     Value::INT64(v) => list.extend_from_slice(&v.to_le_bytes()),
        //     Value::BOOL(v) => {
        //         if *v == true {
        //             list.extend_from_slice(&[1;1]);
        //         } else {
        //             list.extend_from_slice(&[0;1]);
        //         }
        //     },
        //     Value::BYTES(v) => {
        //         crate::btree::util::escapeString(v, list);
        //         //list.extend_from_slice(v);
        //         list.push(0);
        //     },
        //     _Other =>
        //     {

        //     }
        // }
    }

    fn decodeVal(&mut self, val:&[u8], idx: usize, pos: usize) -> usize {

        let (v,len) = Value::decodeVal(&self.def.Types[idx],val, pos);
        self.Vals[idx] = v;
        return  pos + len;;

        // match (self.def.Types[idx]) {
        //     ValueType::INT8 => {
        //         self.Vals[idx] = Value::INT8(i8::from_le_bytes([val[pos];1]));
        //         return pos + 1;
        //     },
        //     ValueType::INT16 => {
        //         self.Vals[idx] = Value::INT16(i16::from_le_bytes( val[pos..pos+2].try_into().unwrap() ));
        //         return pos + 2;
        //     },
        //     ValueType::INT32 => {
        //         self.Vals[idx] = Value::INT32(i32::from_le_bytes( val[pos..pos+4].try_into().unwrap() ));
        //         return pos + 4;
        //     },
        //     ValueType::INT64 => {
        //         self.Vals[idx] = Value::INT64(i64::from_le_bytes( val[pos..pos+8].try_into().unwrap() ));
        //         return pos + 8;
        //     },
        //     ValueType::BOOL => {
        //         if val[pos] == 1 {
        //             self.Vals[idx] = Value::BOOL(true);
        //         } else {
        //             self.Vals[idx] = Value::BOOL(false);
        //         }
        //         return pos + 1;
        //     },            
        //     ValueType::BYTES => {
        //         let mut end = pos;
        //         while val[end] != 0
        //         {
        //             end += 1;
        //         }   
        //         let ret = crate::btree::util::deescapeString(val[pos..end].try_into().unwrap());
        //         match &mut self.Vals[idx]
        //         {
        //             Value::BYTES(v) =>
        //             {
        //                 v.clear();
        //             },
        //             Other => {}
        //         }
        //         self.Vals[idx] = Value::BYTES(ret);                        
        //         return end + 1;
        //     },
        //     _=>{
        //         panic!()
        //     }
        //}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_get_set()
    {
        let mut table = TableDef{
            Prefix:3,
            Name: "person".as_bytes().to_vec(),
            Types : vec![ValueType::BYTES, ValueType::BYTES,ValueType::BYTES, ValueType::INT16, ValueType::BOOL ] ,
            Cols : vec!["id".as_bytes().to_vec() , "name".as_bytes().to_vec(),"address".as_bytes().to_vec(),"age".as_bytes().to_vec(),"married".as_bytes().to_vec() ] ,
            PKeys : 1,
            Indexes : vec![vec!["address".as_bytes().to_vec() , "married".as_bytes().to_vec()],vec!["age".as_bytes().to_vec()]],
            IndexPrefixes : vec![],
        };
        table.FixIndexes();

        let mut rc = Record::new(&table);
        rc.Set("id".as_bytes(), Value::BYTES("20".as_bytes().to_vec())).unwrap();
        rc.Set("name".as_bytes(), Value::BYTES("John Water".as_bytes().to_vec())).unwrap();
        rc.Set("address".as_bytes(), Value::BYTES("Pointe-Claire".as_bytes().to_vec())).unwrap();
        rc.Set("age".as_bytes(), Value::INT16(20)).unwrap();
        rc.Set("married".as_bytes(), Value::BOOL(false)).unwrap();

        let ret = rc.Get("name".as_bytes());
        match  ret {
            Some(v) => println!("Name:{v}"),
            None=>{}
        }

        let mut list:Vec<u8> = Vec::new();
        rc.encodeValues(&mut list);

        let mut listKey:Vec<u8> = Vec::new();
        rc.encodeKey(table.Prefix,&mut listKey);

        println!("Keys:{:?} Vals:{:?} \n",listKey, list);

        let mut rc1 = Record::new(&table);
        rc1.decodeValues(&list);
        rc1.deencodeKey(&listKey);

        println!("After Decode:{}",rc1);


    }

    #[test]
    fn test_record_find_index()
    {
        let mut table = TableDef{
            Prefix:3,
            Name: "person".as_bytes().to_vec(),
            Types : vec![ValueType::BYTES, ValueType::BYTES,ValueType::BYTES, ValueType::INT16, ValueType::BOOL ] ,
            Cols : vec!["id".as_bytes().to_vec() , "name".as_bytes().to_vec(),"address".as_bytes().to_vec(),"age".as_bytes().to_vec(),"married".as_bytes().to_vec() ] ,
            PKeys : 0,
            Indexes : vec![vec!["address".as_bytes().to_vec() , "age".as_bytes().to_vec()],vec!["name".as_bytes().to_vec(),"age".as_bytes().to_vec()]],
            IndexPrefixes : vec![],
        };
        table.FixIndexes();

        let mut rc = Record::new(&table);
        rc.Set("id".as_bytes(), Value::BYTES("20".as_bytes().to_vec())).unwrap();

        let ret = rc.findIndexes();
        assert!(ret.is_ok());
        assert!(ret.unwrap() == -1);

        let mut rc = Record::new(&table);
        rc.Set("address".as_bytes(), Value::BYTES("PC".as_bytes().to_vec())).unwrap();

        let ret = rc.findIndexes();
        assert!(ret.is_ok());
        assert!(ret.unwrap() == 0);

        let mut rc = Record::new(&table);
        rc.Set("age".as_bytes(), Value::INT16(30)).unwrap();

        let ret = rc.findIndexes();
        assert!(ret.is_err());

        let mut rc = Record::new(&table);
        rc.Set("name".as_bytes(), Value::BYTES("Bob".as_bytes().to_vec())).unwrap();

        let ret = rc.findIndexes();
        assert!(ret.is_ok());
        assert!(ret.unwrap() == 1);

        let mut rc = Record::new(&table);
        rc.Set("name".as_bytes(), Value::BYTES("Bob".as_bytes().to_vec())).unwrap();
        rc.Set("age".as_bytes(), Value::INT16(30)).unwrap();

        let ret = rc.findIndexes();
        assert!(ret.is_ok());
        assert!(ret.unwrap() == 1);

    }

    #[test]
    fn test_encode_decode_keyParital()
    {
        let mut table = TableDef{
            Prefix:3,
            Name: "person".as_bytes().to_vec(),
            Types : vec![ValueType::BYTES, ValueType::BYTES,ValueType::BYTES, ValueType::INT16, ValueType::BOOL ] ,
            Cols : vec!["id".as_bytes().to_vec() , "name".as_bytes().to_vec(),"address".as_bytes().to_vec(),"age".as_bytes().to_vec(),"married".as_bytes().to_vec() ] ,
            PKeys : 0,
            Indexes : vec![vec!["address".as_bytes().to_vec() , "age".as_bytes().to_vec()],vec!["name".as_bytes().to_vec(),"age".as_bytes().to_vec()]],
            IndexPrefixes : vec![],
        };
        table.FixIndexes();
        println!("{}",table);

        let mut rc = Record::new(&table);
        rc.Set("id".as_bytes(), Value::BYTES("20".as_bytes().to_vec())).unwrap();
        rc.Set("name".as_bytes(), Value::BYTES("Bob".as_bytes().to_vec())).unwrap();
        rc.Set("age".as_bytes(), Value::INT16(30)).unwrap();

        let mut key = Vec::new();
        rc.encodeKeyPartial(1,&mut key,&OP_CMP::CMP_LE);
        println!("{:?}",key);

        let mut rc = Record::new(&table);
        rc.decodeKeyPartrial(1, &key);
        println!("{}",rc);

    }
}