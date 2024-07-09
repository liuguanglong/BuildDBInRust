use crate::btree::table::value::Value;
use crate::btree::table::value::ValueType;
use std::fmt;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct TableDef{
    pub Name:Vec<u8>,
    pub Types: Vec<ValueType>,
    pub Cols: Vec<Vec<u8>>,
    pub PKeys: u16,
    pub Prefix: u32,
    pub Indexes: Vec<Vec<Vec<u8>>>,
    pub IndexPrefixes: Vec<u32>,
}

impl TableDef{

    pub fn new(content:&String) -> Self{
        let t: TableDef = serde_json::from_str(content).unwrap();
        return t
    }

    pub fn FixIndexes(&mut self)
    {
        //Add Primary Key To Indexes
        for i in 0..self.Indexes.len()
        {
            for j in 0..self.PKeys + 1
            {
                self.Indexes[i].push(self.Cols[j as usize].clone());
            }
            self.IndexPrefixes.push(self.Prefix as u32 + i as u32  + 1);
        }
    }

    pub fn Marshal(&self) ->String{
        return serde_json::to_string(self).unwrap();
    }
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

    #[test]
    fn test_table_marsh()
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
        let str = table.Marshal();
        println!("{}", str);

        let t = TableDef::new(&str);
        println!("{}", t);
    }
}