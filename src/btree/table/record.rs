use crate::btree::table::table::TableDef;
use crate::btree::table::value::Value;
use crate::btree::table::value::ValueType;


//Record Table Row
pub struct Record<'a>{
    Vals: Vec<Value>,
    def: &'a TableDef,
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

    pub fn GetColumnIndex(&self, key: &[u8])-> Option<usize> {
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_set()
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

        let mut rc = Record::new(&table);
        rc.Set("name".as_bytes(), Value::BYTES("John Water".as_bytes().to_vec()));

        let ret = rc.Get("name".as_bytes());
        match  ret {
            Some(v) => println!("Name:{v}"),
            None=>{}
        }
    }
}