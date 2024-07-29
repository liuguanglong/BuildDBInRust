use crate::btree::table::{table::TableDef, value::{Value, ValueType}};


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