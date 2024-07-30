use std::fmt;
use super::{lib::*, statement::*, expr::{ Expr}};

pub struct SelectExpr{
    pub Scan:ScanExpr,
    pub Name:Vec<Vec<u8>>,
    pub Ouput:Vec<Expr>,
}
impl fmt::Display for SelectExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Expr: => {} | Columns:=> ",self.Scan);
        for i in 0..self.Name.len()
        {
            write!(f,"{}:{}",String::from_utf8(self.Name[i].to_vec()).unwrap(),self.Ouput[i]);
        }
        write!(f, " ")
    }
}


impl SelectExpr{
    pub fn new(expr:ScanExpr)->Self
    {
        SelectExpr{
            Scan:expr,
            Name:Vec::new(),
            Ouput:Vec::new(),
        }
    }
}


fn ExprSelectItem<'a>() -> impl Parser<'a,(Expr,Vec<u8>)>
{
    //a, | a*c | a*c as f | d + 'abc ' as g 
    pair(
        Expr(),
        remove_lead_space(
            either3(
                map(is_literal(","),|c| Vec::new()),
                right(
                    match_literal("as"),
                    remove_lead_space(id_string())
                ).map( |c| c.into_bytes()),
                map(is_literal("from"),|c| Vec::new()),
            )      
        )
    )
}  

fn ExprSelectItems<'a>() -> impl Parser<'a,Vec<(Expr,Vec<u8>)>> 
{
    //select a,b,c,a*c as f, d + 'abc ' as g from
    right(
        match_literal("select"),
        pair(
            remove_lead_space(ExprSelectItem()),
            zero_or_more(
                right(
                    remove_lead_space(match_literal(",")),
                    remove_lead_space(ExprSelectItem())
                    )
                )
            )
        ).map( |(item1,mut tail)|
        {
            tail.insert(0, item1);
            tail
        }
    )
}

//stmt select
pub fn ExprSelect<'a>() -> impl Parser<'a,SelectExpr> 
{
    left(
        pair(
            remove_lead_space(ExprSelectItems()),
            remove_lead_space(ExprFrom()),
        ),
        remove_lead_space(match_literal(";")
            )
    )
        .map(|(v1,v2)|
        {
            let mut select = SelectExpr::new(v2);
            for item in v1
            {
                select.Name.push(item.1);
                select.Ouput.push(item.0);
            }
            select
        }
    )   
}

#[test]
fn test_selectitem_expr() {
    
    let exp = "a*c as f , ";
    let ret = ExprSelectItem().parse(exp).unwrap();
    println!("{} Next:{} as:{}  expr:{}",exp,ret.0, String::from_utf8(ret.1.1).unwrap(), ret.1.0);

    let exp = "select id,name,dsaf,afdmasdf,2,dsaf,3,456,address from ;";
    let ret = ExprSelectItems().parse(exp).unwrap();
    println!("{} Next:{}",exp,ret.0);
    for item in ret.1
    {
        println!("{}:{}",String::from_utf8(item.1).unwrap(),item.0)
    }

    let exp = "select a,b,c,a*c as f, d + 'abc ' as g from tableA index by a >= 20 and a < 80 filter married = 1 offset 1000 ;";
    let ret = ExprSelect().parse(exp).unwrap();
    println!("{} Next:{} | Select:{}",exp,ret.0, ret.1);

}

#[test]
fn test_scan_expr() {

    let exp = "from tableA index by a >= 20 and a < 80 filter married = 1 limit 200 offset 1000 ;";
    let ret = ExprFrom().parse(exp).unwrap();
    println!("{}  Scan:|{}  Next:{}",exp,ret.1,ret.0);

    let exp = "from tableA ;";
    let ret = ExprFrom().parse(exp).unwrap();
    println!("{}  Scan:|{}  Next:{}",exp,ret.1,ret.0);

    let exp = "from tableA index by a >= 20 and a < 80  ;";
    let ret = ExprFrom().parse(exp).unwrap();
    println!("{}  Scan:|{}  Next:{}",exp,ret.1,ret.0);

    let exp = "from tableA filter married = 1 limit 200 offset 1000 ;";
    let ret = ExprFrom().parse(exp).unwrap();
    println!("{}  Scan:|{}  Next:{}",exp,ret.1,ret.0);

    let exp = "from tableA index by a >= 20 and a < 80 limit 200;";
    let ret = ExprFrom().parse(exp).unwrap();
    println!("{}  Scan:|{}  Next:{}",exp,ret.1,ret.0);

    let exp = "from tableA index by a >= 20 and a < 80 filter married = 1 offset 1000 ;";
    let ret = ExprFrom().parse(exp).unwrap();
    println!("{}  Scan:|{}  Next:{}",exp,ret.1,ret.0);
    
}

#[cfg(test)]
mod tests {

    use std::{sync::{Arc, Mutex, RwLock}, thread, time::Duration};
    use rand::Rng;

    use crate::btree::{db::{TDEF_META, TDEF_TABLE}, scan::comp::OP_CMP, table::{record::Record, table::TableDef, value::{Value, ValueType}}, tx::{database::Database, dbcontext::DbContext, memoryContext::memoryContext, txfreelist::FreeListData, txinterface::{DBReadInterface, DBTxInterface, TxContent, TxReadContext}, txwriter::txwriter}, BTREE_PAGE_SIZE, MODE_UPSERT};
    use super::*;
    use crate::btree::{btree::request::{DeleteRequest, InsertReqest}, db::{scanner::Scanner, INDEX_ADD, INDEX_DEL}};

    #[test]
    fn test_query()
    {
        let mut mctx = Arc::new(RwLock::new(memoryContext::new(BTREE_PAGE_SIZE,1000)));
        let mut context = DbContext::new(mctx.clone());
        let mut db = Arc::new(Mutex::new(Database::new(context).unwrap()));

        let mut db1 = db.clone();
        let mut dbinstance =  db1.lock().unwrap();
        let mut tx = dbinstance.begin().unwrap();
        drop(dbinstance);

        let mut table = TableDef{
            Prefix:0,
            Name: "person".as_bytes().to_vec(),
            Types : vec![ValueType::BYTES, ValueType::BYTES,ValueType::BYTES, ValueType::INT16, ValueType::BOOL ] ,
            Cols : vec!["id".as_bytes().to_vec() , "name".as_bytes().to_vec(),"address".as_bytes().to_vec(),"age".as_bytes().to_vec(),"married".as_bytes().to_vec() ] ,
            PKeys : 0,
            Indexes : vec![vec!["address".as_bytes().to_vec() , "married".as_bytes().to_vec()],vec!["name".as_bytes().to_vec()]],
            IndexPrefixes : vec![],
        };

        let ret = tx.AddTable(&mut table);
        if let Err(ret) = ret
        {
            println!("Error when add table:{}",ret);
        }

        let mut dbinstance =  db.lock().unwrap();
        dbinstance.commmit(&mut tx);
        drop(dbinstance);

        let mut dbinstance =  db.lock().unwrap();
        let mut tx  = dbinstance.begin().unwrap();        
        drop(dbinstance);   

        let ret = tx.getTableDef("person".as_bytes());
        if let Some(tdef) = ret
        {
            println!("Table define:{}",tdef);
            let mut r = Record::new(&tdef);

            for i in 0..100 {
                r.Set("id".as_bytes(), Value::BYTES(format!("{}", i).as_bytes().to_vec()));
                r.Set( "name".as_bytes(), Value::BYTES(format!("Bob{}", i).as_bytes().to_vec()));
                r.Set("address".as_bytes(), Value::BYTES("Montrel Canada H9T 1R5".as_bytes().to_vec()));
                r.Set("age".as_bytes(), Value::INT16(20));
                r.Set("married".as_bytes(), Value::BOOL(false));

                tx.UpdateRecord(&mut r,crate::btree::MODE_UPSERT);
            }

            r.Set("id".as_bytes(), Value::BYTES(("21").as_bytes().to_vec()));
            r.Set( "name".as_bytes(), Value::BYTES(("Bob504").as_bytes().to_vec()));
            r.Set("address".as_bytes(), Value::BYTES("Montrel Canada H9T 1R5".as_bytes().to_vec()));
            r.Set("age".as_bytes(), Value::INT16(20));
            r.Set("married".as_bytes(), Value::BOOL(false));

            tx.UpdateRecord(&mut r,crate::btree::MODE_UPSERT);


            r.Set("id".as_bytes(), Value::BYTES(("22").as_bytes().to_vec()));
            tx.DeleteRecord(&mut r);

            let statements = "select id,name,address, age + 40 as newage, age from person index by  name < 'Bob50' and name > 'Bob4' ;";
            if let Ok((ret,sqlExprList)) = ExprSQLList().parse(&statements)
            {
                for sql in sqlExprList
                {
                    if let SQLExpr::Select(sql) = sql
                    {
                        if let Ok((table,rows)) = tx.Query(&sql)
                        {
                            println!("Table:{}\n",String::from_utf8(table.Name.to_vec()).unwrap());
                            for i in 0..table.Cols.len()
                            {
                                print!("{}:{}|",String::from_utf8(table.Cols[i].to_vec()).unwrap(),table.Types[i]);
                            }
                            println!("");
                            for r in rows
                            {
                                print!("{}\n",r);
                            }
                        }
                    }
                }
            }
        }
    }

}