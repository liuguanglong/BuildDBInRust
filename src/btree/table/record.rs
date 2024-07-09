use crate::btree::table::table::TableDef;
use crate::btree::table::value::Value;
use crate::btree::table::value::ValueType;
use std::fmt;

//Record Table Row
pub struct Record<'a>{
    Vals: Vec<Value>,
    def: &'a TableDef,
}

impl<'a> fmt::Display for Record<'a> {

    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f,"Record Content:\n|");
        for i in 0..self.def.Cols.len()
        {
            write!(f,"{}|", String::from_utf8(self.def.Cols[i].to_vec()).unwrap());
        } 
        
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
            vals.push(Value::INT8(0));
        }
        Record{
            Vals : vals,
            def : def
        }
    }

    pub fn Set(&mut self, key: &[u8], val: Value){
        let idx = self.GetColumnIndex(key);
        match idx 
        {
            Some(i) => {
                self.Vals[i] = val;
            },
            None=>{}
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

    fn GetColumnIndex(&self, key: &[u8])-> Option<usize> {
        for i in 0..self.def.Cols.len()
        {
            let cmp = crate::btree::util::compare_arrays(&self.def.Cols[i], key);
            if  cmp == 0
            {
                return Some(i);
            }
        }
        return None;
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

    fn encodeVal(&self, idx: usize, list:&mut Vec<u8>) {

        match &self.Vals[idx]
         {
            Value::INT8(v) => list.extend_from_slice(&v.to_le_bytes()),
            Value::INT16(v) => list.extend_from_slice(&v.to_le_bytes()),
            Value::INT32(v) => list.extend_from_slice(&v.to_le_bytes()),
            Value::INT64(v) => list.extend_from_slice(&v.to_le_bytes()),
            Value::BOOL(v) => {
                if *v == true {
                    list.extend_from_slice(&[1;1]);
                } else {
                    list.extend_from_slice(&[0;1]);
                }
            },
            Value::BYTES(v) => {
                crate::btree::util::escapeString(v, list);
                //list.extend_from_slice(v);
                list.push(0);
            },
        }
    }

    fn decodeVal(&mut self, val:&[u8], idx: usize, pos: usize) -> usize {
        match (self.def.Types[idx]) {
            ValueType::INT8 => {
                self.Vals[idx] = Value::INT8(i8::from_le_bytes([val[pos];1]));
                return pos + 1;
            },
            ValueType::INT16 => {
                self.Vals[idx] = Value::INT16(i16::from_le_bytes( val[pos..pos+2].try_into().unwrap() ));
                return pos + 2;
            },
            ValueType::INT32 => {
                self.Vals[idx] = Value::INT32(i32::from_le_bytes( val[pos..pos+4].try_into().unwrap() ));
                return pos + 4;
            },
            ValueType::INT64 => {
                self.Vals[idx] = Value::INT64(i64::from_le_bytes( val[pos..pos+8].try_into().unwrap() ));
                return pos + 8;
            },
            ValueType::BOOL => {
                if val[pos] == 1 {
                    self.Vals[idx] = Value::BOOL(true);
                } else {
                    self.Vals[idx] = Value::BOOL(false);
                }
                return pos + 1;
            },            
            ValueType::BYTES => {
                let mut end = pos;
                while val[end] != 0
                {
                    end += 1;
                }   

                match &mut self.Vals[idx]
                {
                    Value::BYTES(v) =>
                    {
                        let ret = crate::btree::util::deescapeString(val[pos..end].try_into().unwrap());
                        v.clear();
                        v.extend_from_slice(&ret);                        
                    },
                    Other => {}
                }
                return end + 1;
            },
        }
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
            PKeys : 0,
            Indexes : vec![vec!["address".as_bytes().to_vec() , "married".as_bytes().to_vec()],vec!["age".as_bytes().to_vec()]],
            IndexPrefixes : vec![],
        };

        let mut rc = Record::new(&table);
        rc.Set("id".as_bytes(), Value::BYTES("20".as_bytes().to_vec()));
        rc.Set("name".as_bytes(), Value::BYTES("John Water".as_bytes().to_vec()));
        rc.Set("address".as_bytes(), Value::BYTES("Pointe-Claire".as_bytes().to_vec()));
        rc.Set("age".as_bytes(), Value::INT16(20));
        rc.Set("married".as_bytes(), Value::BOOL(false));

        let ret = rc.Get("name".as_bytes());
        match  ret {
            Some(v) => println!("Name:{v}"),
            None=>{}
        }

        let mut list:Vec<u8> = Vec::new();
        rc.encodeValues(&mut list);

        println!("Vals Result:{:?} \n", list);

        let mut rc1 = Record::new(&table);
        rc1.decodeValues(&list);

        println!("After Decode:{}",rc1);

    }

}