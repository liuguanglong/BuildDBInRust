use std::fmt;
use crate::btree::{btree::request::InsertReqest, table::{record::Record, table::TableDef, value::Value}, BTreeError};
use super::{expr::{id, Constant, Expr}, lib::*, sqlerror::SqlError, statement::*};

pub struct InsertExpr{
    pub TableName:Vec<u8>,
    pub Name:Vec<Vec<u8>>,
    pub Values:Vec<Vec<Value>>,
}

impl InsertExpr{
    pub fn new(expr:String)->Self
    {
        InsertExpr{
            TableName:expr.into_bytes(),
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
        write!(f, "Expr: => {} \n",String::from_utf8(self.TableName.to_vec()).unwrap());
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


#[cfg(test)]
mod tests {

    use std::{sync::{Arc, Mutex, RwLock}, thread, time::Duration};
    use rand::Rng;

    use crate::btree::{db::{TDEF_META, TDEF_TABLE}, parser::createtable::ExprCreateTable, scan::comp::OP_CMP, table::{record::Record, table::TableDef, value::{Value, ValueType}}, tx::{database::Database, dbcontext::DbContext, memoryContext::memoryContext, txfreelist::FreeListData, txinterface::{DBReadInterface, DBTxInterface, TxContent, TxReadContext}, txwriter::txwriter}, BTREE_PAGE_SIZE, MODE_UPSERT};
    use super::*;
    use crate::btree::{btree::request::{DeleteRequest, InsertReqest}, db::{scanner::Scanner, INDEX_ADD, INDEX_DEL}};

    #[test]
    fn test_insert()
    {
        let mut mctx = Arc::new(RwLock::new(memoryContext::new(BTREE_PAGE_SIZE,1000)));
        let mut context = DbContext::new(mctx.clone());
        let mut db = Arc::new(Mutex::new(Database::new(context).unwrap()));

        let mut db1 = db.clone();
        let mut dbinstance =  db1.lock().unwrap();
        let mut tx = dbinstance.begin().unwrap();
        drop(dbinstance);

        let mut sql:String = String::new();
        let createTable = r#"
        create table person
        ( 
            id vchar,
            name vchar,
            address vchar,
            age int16,
            married bool,
            primary key (id),
            index (address,married),
            index (name),
        );
       "#;
       sql.push_str(createTable);

       let insert = r#"
            insert into person
            ( id, name, address, age, married )
            values

        "#;
        sql.push_str(&insert);

        for i in 0..100{
            sql.push_str(format!("('{}','Bob{}','Montrel Canada H9T 1R5',20,false),", i,i).as_str())
        }

        sql.remove(sql.len() -1 );
        sql.push(';');

        let delete = "delete from person index by id = '45';";
        sql.push_str(delete);

        let update = "update person set name = 'Bob800' index by id = '44';";
        sql.push_str(update);

        let statements = "select id,name,address, age + 40 as newage, age from person index by name < 'Bob50' and name > 'Bob4' filter id < '48' limit 6 offset 2;";
        sql.push_str(statements);

        println!("sql:{}",sql);

        if let Ok(tables) = tx.ExecuteSQLStatments(sql)
        {
            for t in tables
            {
                println!("{}",t);
            }
        }
    }

}