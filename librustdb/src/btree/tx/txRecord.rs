use std::fmt;
use crate::btree::{db::INDEX_ADD, table::{table::TableDef, value::{Value, ValueType}}};

pub struct DataTable{
    pub Name:Vec<u8>,
    pub Types: Vec<ValueType>,
    pub Cols: Vec<Vec<u8>>,
    pub Rows: Vec<DataRow>
}
impl<'a> fmt::Display for DataTable{

    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f,"Table:{}\n",String::from_utf8(self.Name.to_vec()).unwrap());

        if self.Rows.len() > 0
        {
            for i in 0..self.Cols.len()
            {
                write!(f,"{}:{}|",String::from_utf8(self.Cols[i].to_vec()).unwrap(),self.Types[i]);
            }
            write!(f,"\n");
            for r in &self.Rows
            {
                write!(f,"{}\n",*r);
            }
        }
        write!(f,"")
    }
}


pub struct DataRow{
    pub Vals: Vec<Value>,
}

impl DataRow{
    pub fn new()->Self{
        DataRow{
            Vals:Vec::new(),
        }
    }

    pub fn Seralize(&self)->Vec<u8>
    {
        let mut list = Vec::new();
        for v in &self.Vals
        {
            v.encodeVal(&mut list);
        }
        list
    }

    pub fn Deserialize(types:Vec<ValueType>,val:&[u8])->DataRow
    {
        let vals = Vec::new();
        let mut pos:usize = 0;
        for t in types
        {
            let (v,len) = Value::decodeVal(&t, val, pos);
            pos += len;

        }
        DataRow{
            Vals:vals
        }
    }
}



impl<'a> fmt::Display for DataRow{

    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for i in 0..self.Vals.len()
        {
            write!(f,"{}|", self.Vals[i]);
        } 
        write!(f,"")
    }
}


impl DataTable{
    pub fn new(tdef:&TableDef)->Self{
        DataTable{
            Name:tdef.Name.clone(),
            Types:Vec::new(),
            Cols:Vec::new(),
            Rows: Vec::new(),
        }
    }
}
