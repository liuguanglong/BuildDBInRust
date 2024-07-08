use crate::btree::table::value::Value;
use crate::btree::table::value::ValueType;
use std::fmt;

pub struct TableDef{
    Name:Vec<u8>,
    Types: Vec<ValueType>,
    Cols: Vec<Vec<u8>>,
    PKeys: u16,
    Prefix: u32,
    Indexes: Vec<Vec<Vec<u8>>>,
    IndexPrefixes: Vec<u32>,
}

impl fmt::Display for TableDef {

    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f,"\nTable Definition:\n|");
        for i in 0..self.Cols.len()
        {
            write!(f,"{}|", String::from_utf8(self.Cols[i].to_vec()).unwrap());
        } 
        write!(f,"\n|",);
        for i in 0..self.Types.len()
        {
            write!(f,"{}|", self.Types[i]);
        } 
        write!(f,"\nPrimary Keys\n");
        for i in 0..self.PKeys + 1
        {
            write!(f,"{}|",  String::from_utf8(self.Cols[i as usize].to_vec()).unwrap());
        } 
        write!(f,"\nIndexes\n");

        for i in 0..self.Indexes.len()
        {
            for j in 0..self.Indexes[i].len()
            {
                write!(f,"{}|",  String::from_utf8(self.Indexes[i][j].to_vec()).unwrap());
            }
            write!(f,"\n");
        } 
        write!(f,"\n")
    }

}

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


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format()
    {
        println!("{}", *TDEF_META);
        println!("{}", *TDEF_TABLE);
    }
}