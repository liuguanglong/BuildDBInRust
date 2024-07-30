use std::fmt;

use crate::btree::table::{table::TableDef, value::{Value, ValueType}};


pub struct DataTable{
    pub Name:Vec<u8>,
    pub Types: Vec<ValueType>,
    pub Cols: Vec<Vec<u8>>,
    pub Rows: Vec<DataRow>
}
impl<'a> fmt::Display for DataTable{

    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f,"Table:{}\n",String::from_utf8(self.Name.to_vec()).unwrap());
        for i in 0..self.Cols.len()
        {
            write!(f,"{}:{}|",String::from_utf8(self.Cols[i].to_vec()).unwrap(),self.Types[i]);
        }
        println!("");
        for r in &self.Rows
        {
            write!(f,"{}\n",*r);
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


pub struct TxRecord{
    pub Vals: Vec<Value>,
}

impl TxRecord{
    pub fn new()->Self
    {
        TxRecord{
            Vals:Vec::new()
        }
    }
}

impl<'a> fmt::Display for TxRecord{

    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for i in 0..self.Vals.len()
        {
            write!(f,"{}|", self.Vals[i]);
        } 
        write!(f,"")
    }
}

pub struct TxTable{
    pub Name:Vec<u8>,
    pub Types: Vec<ValueType>,
    pub Cols: Vec<Vec<u8>>,
}

impl TxTable{
    pub fn new(tdef:&TableDef)->Self{
        TxTable{
            Name:tdef.Name.clone(),
            Types:tdef.Types.clone(),
            Cols:tdef.Cols.clone()
        }
    }
}