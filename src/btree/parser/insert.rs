use std::fmt;
use crate::btree::{btree::request::InsertReqest, table::{record::Record, table::TableDef, value::Value}, BTreeError};
use super::{expr::{id, Constant, Expr}, lib::*, sqlerror::SqlError, statement::*};

pub struct InsertExpr{
    Scan:Vec<u8>,
    Name:Vec<Vec<u8>>,
    Values:Vec<Vec<Value>>,
}

impl InsertExpr{
    pub fn new(expr:String)->Self
    {
        InsertExpr{
            Scan:expr.into_bytes(),
            Name:Vec::new(),
            Values:Vec::new(),
        }
    }

    pub fn createQuest<'a>(&'a self,tdef:&'a TableDef) -> Result<Vec<Record>,BTreeError>
    {
        let mut list = Vec::with_capacity(self.Values.len());
        for row in &self.Values
        {
            let mut r: Record = Record::new(&tdef);
            for i in 0..self.Name.len()
            {
                if let Err(err) = r.Set(&self.Name[i], row[i].clone())
                {
                    return Err(err);
                }
            }
            list.push(r);
        }

        Ok(list)
    }
    
}

impl fmt::Display for InsertExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Expr: => {} \n",String::from_utf8(self.Scan.to_vec()).unwrap());
        write!(f, "Columns:  \n");
        for i in 0..self.Name.len()
        {
            write!(f,"{},",String::from_utf8(self.Name[i].to_vec()).unwrap());
        }
        write!(f, "\nRows:  \n");
        for row in &self.Values
        {
            for col in row
            {
                write!(f,"{},",col);
            }
            write!(f, "\n");
        }
        write!(f, " ")
        }
}


fn Values<'a>() -> impl Parser<'a,Vec<Value>>
{
    right(
        match_literal("("),
        tuple3(
            remove_lead_space(Constant()),
            zero_or_more(
                    right(
                        remove_lead_space(match_literal(",")),
                        remove_lead_space(Constant())
                    )
                ),
                remove_lead_space(match_literal(")"))
        )
        )
    .map(|(first,mut tail,_)|
    {
        tail.insert(0, first);
        tail
    }
    )
}

fn Rows<'a>()-> impl Parser<'a,Vec<Vec<Value>>>
{
    tuple3(
        remove_lead_space_and_newline(Values()),
        zero_or_more(
            right(
                remove_lead_space(match_literal(",")),
                remove_lead_space_and_newline(Values())
                ),
        ),
        remove_lead_space_and_newline(match_literal(";"))
    ).map( |(first,mut tail,_)|
    {
        tail.insert(0, first);
        tail
    }
    )    
}


fn cols<'a>() -> impl Parser<'a,Vec<String>>
{
    right(
        match_literal("("),
        tuple3(
            remove_lead_space(id_string()),
            zero_or_more(
                    right(
                        remove_lead_space(match_literal(",")),
                        remove_lead_space(id_string())
                    )
                ),
                remove_lead_space(match_literal(")"))
        )
        )
    .map(|(first,mut tail,_)|
    {
        tail.insert(0, first);
        tail
    }
    )
}

//INSERT INTO MyTable
// ( Column1, Column2, Column3 )
// VALUES
//   ('John', 123, 'Lloyds Office'), 
//   ('Jane', 124, 'Lloyds Office'), 
//   ('Billy', 125, 'London Office'),
//   ('Miranda', 126, 'Bristol Office');

pub fn ExprInsert<'a>() -> impl Parser<'a,InsertExpr>
{
    right(
    pair(
        remove_lead_space_and_newline(match_literal("insert")),
        remove_lead_space_and_newline(match_literal("into")),
        ),
        tuple4(
            remove_lead_space_and_newline(id_string()), 
            remove_lead_space_and_newline(cols()),
            remove_lead_space_and_newline(match_literal("values")),
            remove_lead_space_and_newline(Rows()),
        )
    ).map( |(table,cols,_,values)| 
        {
            let mut insert = InsertExpr::new(table);
            for c in cols
            {
                insert.Name.push(c.as_bytes().to_vec());
            }
            for row in values
            {
                let mut r = Vec::new();
                for  c  in row {
                    r.push(c);
                }
                insert.Values.push(r);
            }
            insert
        }
    )
}

// fn ExprInsert<'a>-> impl Parser<'a,InsertExpr>
// {
//     pair(
//         ExprInsertInto(),
//         pair(
//             right(space0(),
//                 right(
//                     match_literal("("),
//                     cols(),
//                 )
//             ),
//             right(space0(),
//                 right(
//                     match_literal("("),
//                     Values(),
//                 ),
//             )

//         }
//     )
// }

#[test]
fn test_statement_expr() {

    let exp = "('John', 123, 'Lloyds Office')";
    let ret = Values().parse(exp).unwrap();
    println!("{} Next:{} | Values:",exp,ret.0);
    for v in ret.1
    {
        print!("{}|",v);
    }
    println!("");

    let exp = "( Column1, Column2, Column3 ) ";
    let ret = cols().parse(exp).unwrap();
    println!("{} Next:{}",exp,ret.0);
    for item in ret.1
    {
        print!("{}|",item)
    }

    let s = r#"
      ('John', 123, 'Lloyds Office'), 
    ('Jane', 124, 'Lloyds Office'), 
  ('Billy', 125, 'London Office'),
  ('Miranda', 126, 'Bristol Office'); "#;
  let ret = Rows().parse(s).unwrap();
  println!("\n{} Next:{}\n",exp,ret.0);
  for row in ret.1
  {
    for c in row
    {
        print!("{}|",c)
    }
    println!("");
  }
 
    let insert = r#"
    insert into MyTable
    ( Column1, Column2, Column3 )
    values
    ('John', 123, 'Lloyds Office'), 
    ('Jane', 124, 'Lloyds Office'), 
    ('Billy', 125, 'London Office'),
    ('Miranda', 126, 'Bristol Office');
   "#;
   let ret = ExprInsert().parse(insert).unwrap();
   println!("\n{} Next:{}\n Insert:{}",exp,ret.0,ret.1);
    
}