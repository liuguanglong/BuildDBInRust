use crate::btree::table::value::Value;
use crate::btree::table::value::ValueType;
use crate::btree::BTreeError;
use std::fmt;
use std::str::FromStr;
use serde::{Serialize, Deserialize};

#[derive(Serialize,Clone,Deserialize, Debug)]
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

    pub fn create(content:String) -> Self{
        TableDef{
            Name:content.as_bytes().to_vec(),
            Types:Vec::new(),
            Cols:Vec::new(),
            Indexes:Vec::new(),
            PKeys:0,
            
            IndexPrefixes:Vec::new(),
            Prefix:0,
        }
    }

    pub fn new(content:&String) -> Self{
        let t: TableDef = serde_json::from_str(content).unwrap();
        return t
    }

    pub fn GetColumnIndex(&self, key: &[u8])-> Option<usize> {
        for i in 0..self.Cols.len()
        {
            let cmp = crate::btree::util::compare_arrays(&self.Cols[i], key);
            if  cmp == 0
            {
                return Some(i);
            }
        }
        return None;
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

    pub fn findIndexWithRecord(&self,vals:&Vec<Value>)-> Result<i16,BTreeError>
    {
        let mut keys: Vec<&Vec<u8>> = Vec::new();
        for i in 0..self.Cols.len()
        {
            if let Value::None = vals[i] 
            {
            }
            else {
                keys.push(&self.Cols[i]);
            }
        }

        self.findIndex(&keys)
    }

    pub fn findIndex(&self,keys:&Vec<&Vec<u8>>) -> Result<i16,BTreeError>
    {
        let pk = &self.Cols[0..self.PKeys as usize+1];
        if Self::isPrefix(pk,keys)
        {
            return Ok(-1);
        }

        let mut winner:i16 = -2;
        for i in 0..self.Indexes.len()
        {
            if Self::isPrefix(&self.Indexes[i],keys) == false
            {
                continue;
            }
            if winner == -2 || self.Indexes[i].len() < self.Indexes[winner as usize].len()
            {
                winner = i as i16;
            }
        }

        if winner == -2
        {
            return Err(BTreeError::NoIndexFound);
        }
        return Ok(winner);
        
    }

    fn isPrefix(long:&[Vec<u8>], short:&Vec<&Vec<u8>>)->bool{
        if long.len() < short.len()
        {
            return false;
        }

        for i in 0..short.len(){
            let ret = crate::btree::util::compare_arrays(&long[i],&short[i]);
            if ret != 0{
                return false;
            }
        }

        return true;
    }
}
impl fmt::Display for TableDef {

    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f,"\nTable {}:\n|",String::from_utf8(self.Name.to_vec()).unwrap());
        write!(f,"\nTable Prefix{}\n",self.Prefix);
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


#[cfg(test)]
mod tests {
    use super::*;

    // #[test]
    // fn test_format()
    // {
    //     println!("{}", *TDEF_META);
    //     println!("{}", *TDEF_TABLE);
    // }

    #[test]
    fn test_table_marsh()
    {
        let mut table = TableDef{
            Prefix:3,
            Name: "person".as_bytes().to_vec(),
            Types : vec!["BYTES".into(), "BYTES".into(),"BYTES".into(), "INT16".into(), "BOOL".into()] ,
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