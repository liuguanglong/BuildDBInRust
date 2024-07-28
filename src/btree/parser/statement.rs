use std::collections::HashMap;
use std::f64::consts::E;
use std::fmt;
use crate::btree::parser::expr::ExpressionType;
use crate::btree::scan::comp::{self, OP_CMP};
use crate::btree::table::record::Record;
use crate::btree::table::table::TableDef;
use crate::btree::table::value::{Value, ValueType};
use crate::btree::BTreeError;

use super::createtable::ExprCreateTable;
use super::delete::{DeleteExpr, ExprDelete};
use super::expr::{id, number_i64};
use super::insert::{ExprInsert, InsertExpr};
use super::select::{ExprSelect, SelectExpr};
use super::update::{ExprUpdate, UpdateExpr};
use super::{expr::Expr};
use super::lib::*;

pub enum SQLExpr{
    Select(SelectExpr),
    Update(UpdateExpr),
    Delete(DeleteExpr),
    Insert(InsertExpr),
    CreatTable(TableDef),
}

impl fmt::Display for SQLExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SQLExpr::Select(v) => write!(f, "Select:{}",v),
            SQLExpr::Update(v)  => write!(f,"Update:{}",v),
            SQLExpr::Delete(v)  => write!(f,"Delete:{}",v),
            SQLExpr::Insert(v)  => write!(f,"Insert:{}",v),
            SQLExpr::CreatTable(v)  => write!(f,"Create Table:{}",v),
        }
    }
}

pub struct ScanExpr{
    pub Table:Vec<u8>,

    // INDEX BY xxx
    pub Index: Option<Box<Expr>>,
    // FILTER xxx
    pub Filter: Option<Box<Expr>>, // boolean, optional
    // LIMIT x, y
    pub Offset:usize,
    pub Limit:usize,
}

impl ScanExpr{
    pub fn new(name:Vec<u8>)->Self
    {
        ScanExpr{
            Table:name,
            Index:None,
            Filter:None,
            Offset:0,
            Limit:100,
        }
    }

    fn ExtractConditionItem(n:&Expr)->Option<(ExpressionType,Vec<u8>,Value)>
    {
        if n.op == ExpressionType::EQ || n.op == ExpressionType::LE || n.op == ExpressionType::LT
        || n.op == ExpressionType::GT || n.op == ExpressionType::GE 
        {
            if let Some(Value::ID(keyName)) = &n.left.as_ref().unwrap().val
            {
                if let Some(val) =  &n.right.as_ref().unwrap().val
                {
                    return Some((n.op.clone(),keyName.to_vec(),val.clone()));
                }
                else {
                    return None;
                }
            }
            else {
                return None;
            }
        }
        else {
            return None;
        }
    }

    fn ExtractCondtion(n:&Expr,conditions:&mut Vec<(ExpressionType,Vec<u8>,Value)>)->Result<(),BTreeError>
    {
        if n.op == ExpressionType::EQ || n.op == ExpressionType::LE || n.op == ExpressionType::LT
        || n.op == ExpressionType::GT || n.op == ExpressionType::GE 
        {
            if let Some(item) = Self::ExtractConditionItem(n)
            {
                conditions.push(item);
            }
        }
        else if n.op == ExpressionType::AND
        {
            if let Some(n) = &n.left
            {
                Self::ExtractCondtion(n, conditions);
            }
            if let Some(n) = &n.right
            {
                Self::ExtractCondtion(n, conditions);
            }
        }
        else {
            return Err(BTreeError::NoIndexFound);         
        }
        Ok(())
    }

    //only support one column
    pub fn createScan<'a>(&'a self,tdef:&'a TableDef)->Result<Option<(Record,Option<Record>,OP_CMP,Option<OP_CMP>)>,BTreeError>
    {
        let mut cmp1 = OP_CMP::CMP_GE;
        let mut cmp2 = OP_CMP::CMP_GE;

        let mut key1 = Record::new(&tdef);
        let mut key2 = Record::new(&tdef);
        let mut numOpCmp = 0;

        let mut node = &self.Index;

        let mut conditions:Vec<(ExpressionType,Vec<u8>,Value)> = Vec::new();

        if let Some(n) = node
        {
            if let Err(err) = Self::ExtractCondtion(n,&mut conditions)
            {
                return Err(err);
            }
        }

        let mut CompareColumn:Option<Vec<u8>> = None;
        let mut Compare1:Option<(ExpressionType,Value)> = None;
        let mut Compare2:Option<(ExpressionType,Value)> = None;
        for item in conditions
        {
            if item.0 == ExpressionType::EQ
            {
                key1.Set(&item.1, item.2.clone());
                key2.Set(&item.1, item.2.clone());
                continue;
            }

            if CompareColumn.is_none()
            {
                CompareColumn = Some(item.1);
            }
            else{
                if let Some(v) = &CompareColumn
                {
                    if( *v != item.1)
                    {
                        return Err(BTreeError::BadSearchCondition);
                    }
                }
            }

            if Compare1.is_none()
            {
                Compare1 = Some((item.0,item.2));
            }
            else if Compare2.is_none()
            {
                Compare2 = Some((item.0,item.2));
            }
            else {
                return Err(BTreeError::BadSearchCondition);
            }
        }

        if let Some(col) = &CompareColumn
        {
            if let Some(c) = &Compare1
            {
                key1.Set(col, c.1.clone());
                cmp1 = ExprType2OP_CMP(&c.0);
            }
            if let Some(c) = &Compare2
            {
                key2.Set(col, c.1.clone());
                cmp2 = ExprType2OP_CMP(&c.0);
            }
        }
        else {
            cmp1 = OP_CMP::CMP_GE;
            cmp2 = OP_CMP::CMP_LE;

            return Ok(Some((key1,Some(key2),cmp1,Some(cmp2))));
        }

        if Compare2.is_some()
        {
            Ok(Some((key1,Some(key2),cmp1,Some(cmp2))))
        }
        else
        {
            Ok(Some((key1,None,cmp1,None)))
        }

    }
}

fn ExprType2OP_CMP(op:&ExpressionType)->OP_CMP
{
    match op {
        ExpressionType::LT => OP_CMP::CMP_LT,
        ExpressionType::LE => OP_CMP::CMP_LE,
        ExpressionType::GE => OP_CMP::CMP_GE,
        ExpressionType::GT => OP_CMP::CMP_GT,
        _Other => panic!("Bad Index"),
    }
}

struct TableExpr{
    def:TableDef
}

impl fmt::Display for ScanExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Table:{} |",String::from_utf8(self.Table.to_vec()).unwrap());
        if let Some(Index) = &self.Index
        {
            write!(f, "Index:{} |",Index);
        }

        if let Some(filter) = &self.Filter
        {
            write!(f, "filter:{} |",filter);
        }
        write!(f, "offset:{} limit{}|",self.Offset,self.Limit);

        write!(f, " ")
    }
}


fn ExprIndex<'a>() -> impl Parser<'a,Expr>
{
    //index by a >= 20 and a < 80 
    right(
        match_literal("index"),
        right(
            remove_lead_space(match_literal("by")),
            remove_lead_space(Expr())
        )
    )
}

fn ExprFilter<'a>() -> impl Parser<'a,Expr>
{
    //index by a >= 20 and a < 80 
    right(
        match_literal("filter"),
        remove_lead_space(Expr())
    )
}

fn ExprLimit<'a>() -> impl Parser<'a,Expr>
{
    right(
        match_literal("limit"),
        remove_lead_space(number_i64())
    ).map(|v| Expr::constExpr(v))
}

fn ExprOffset<'a>() -> impl Parser<'a,Expr>
{
    right(
        match_literal("offset"),
        remove_lead_space(number_i64())
    ).map(|v| Expr::constExpr(v))
}

pub fn ExprScanItems<'a>() -> impl Parser<'a,Vec<(String,Expr)>>
{
    zero_or_more(
        remove_lead_space(
            either4(
                    ExprIndex().map(|v| ("Index".to_string(),v)),
                    ExprFilter().map(|v| ("Filter".to_string(),v)), 
                    ExprLimit().map(|v| ("Limit".to_string(),v)),
                    ExprOffset().map(|v| ("Offset".to_string(),v)),
            )
        )    
    )
}

pub fn ExprFrom<'a>() -> impl Parser<'a,ScanExpr> 
{
    //from tableA index by a >= 20 and a < 80 filter married = 1 limit 200 offset 1000;
    right(
        match_literal("from"), 
        pair(
            remove_lead_space(id()), 
            ExprScanItems(),
        ),
    ).map(move |(table, props)| 
        {
            if let Value::ID(table) = table
            {
                let mut scan = ScanExpr::new(table);
                for p in props
                {
                    if p.0 == "Limit"
                    {
                        if let Some(Value::INT64(limit)) = p.1.val
                        {
                            scan.Limit = limit as usize;
                         }
                    }
                    if p.0 == "Offset"
                    {
                        if let Some(Value::INT64(offset)) = p.1.val
                        {
                            scan.Offset = offset as usize;
                         }
                    }
                    if p.0 == "Index"
                    {
                        scan.Index = Some(Box::new(p.1.clone()));
                    }
                    if p.0 == "Filter"
                    {
                        scan.Filter = Some(Box::new(p.1.clone()));
                    }
                }    
                scan
            }
            else {
                panic!()
            }
        }
    ) 
}

pub fn ExprSQL<'a>() -> impl Parser<'a,SQLExpr> 
{
    either(
    either4(
        ExprSelect().map(|v| SQLExpr::Select(v)), 
        ExprInsert().map(|v| SQLExpr::Insert(v)), 
        ExprUpdate().map(|v| SQLExpr::Update(v)), 
        ExprDelete().map(|v| SQLExpr::Delete(v)), 
    ),
    ExprCreateTable().map(|v| SQLExpr::CreatTable(v)), 
    )
}

pub fn ExprSQLList<'a>() -> impl Parser<'a,Vec<SQLExpr>> 
{
    one_or_more(ExprSQL())
}

#[test]
fn test_createScan(){
    let expr = "from tableA index by age >= 20 and age < 80";
    let expr1 = "from tableA index by age = 35 ";
    let expr2 = "from tableA index by age > 25";
    let expr3 = "from tableA index by address = 'China' and age >= 20 and age < 80";
    let expr4 = "from tableA index by address = 'China' and name = 'Bob' and age > 30 and age < 90";
    let expr5 = "from tableA index by address = 'China' and name = 'Bob' and age = 30 ";
    
    let mut table = TableDef{
        Prefix:0,
        Name: "tableA".as_bytes().to_vec(),
        Types : vec![ValueType::BYTES, ValueType::BYTES,ValueType::BYTES, ValueType::INT8, ValueType::BOOL ] ,
        Cols : vec!["id".as_bytes().to_vec() , "name".as_bytes().to_vec(),"address".as_bytes().to_vec(),"age".as_bytes().to_vec(),"married".as_bytes().to_vec() ] ,
        PKeys : 0,
        Indexes : vec![vec!["address".as_bytes().to_vec() , "married".as_bytes().to_vec()],vec!["age".as_bytes().to_vec()]],
        IndexPrefixes : vec![],
    };


    let scan = ExprFrom().parse(expr);
    if let Ok(scan) = scan{
        if let Ok(Some((key1,key2,cmp1,cmp2))) = scan.1.createScan(&table)
        {
            println!("key1:{}|key2:{}|cmp1:{}|cmp2:{}| \n",key1,key2.unwrap(),cmp1,cmp2.unwrap());
        }
    }

    let scan = ExprFrom().parse(expr1);
    if let Ok(scan) = scan{
        if let Ok(Some((key1,key2,cmp1,cmp2))) = scan.1.createScan(&table)
        {
            println!("key1:{}|key2:{}|cmp1:{}|cmp2:{}| \n",key1,key2.unwrap(),cmp1,cmp2.unwrap());
        }
    }


    let scan = ExprFrom().parse(expr2);
    if let Ok(scan) = scan{
        if let Ok(Some((key1,key2,cmp1,cmp2))) = scan.1.createScan(&table)
        {
            println!("key1:{}|key2:{}|cmp1:{}|cmp2:{}| \n",key1,key2.is_none(),cmp1,cmp2.is_none());
        }
    }

    let scan = ExprFrom().parse(expr3);
    if let Ok(scan) = scan{
        if let Ok(Some((key1,key2,cmp1,cmp2))) = scan.1.createScan(&table)
        {
            println!("key1:{}|key2:{}|cmp1:{}|cmp2:{}| \n",key1,key2.unwrap(),cmp1,cmp2.unwrap());
        }
    }

    let scan = ExprFrom().parse(expr4);
    if let Ok(scan) = scan{
        if let Ok(Some((key1,key2,cmp1,cmp2))) = scan.1.createScan(&table)
        {
            println!("key1:{}|key2:{}|cmp1:{}|cmp2:{}| \n",key1,key2.unwrap(),cmp1,cmp2.unwrap());
        }
    }

    let scan = ExprFrom().parse(expr5);
    if let Ok(scan) = scan{
        if let Ok(Some((key1,key2,cmp1,cmp2))) = scan.1.createScan(&table)
        {
            println!("key1:{}|key2:{}|cmp1:{}|cmp2:{}| \n",key1,key2.unwrap(),cmp1,cmp2.unwrap());
        }
    }
}


#[test]
fn test_sql_list() {
    let expr = r#"

    create table table1123
    ( 
        a int8,
        b int64,
        c vchar,
        d bool,
        e int16,
        primary key (a,b),
        index (c,d),
        index (e,f),
    );

    select a,b,c,a*c as f, d + 'abc ' as g from tableA index by a >= 20 and a < 80 filter married = 1 offset 1000 ;

    insert into MyTable
       ( Column1, Column2, Column3 )
    values
       ('John', 123, 'Lloyds Office'), 
       ('Jane', 124, 'Lloyds Office'), 
       ('Billy', 125, 'London Office'),
       ('Miranda', 126, 'Bristol Office');

    update tableA set a = 30, b = 'abc' ,d = 26 index by a >= 20 and a < 80;  

    delete from tableA index by a >= 20 and a < 80;

   "#;

    let ret = ExprSQLList().parse(&expr).unwrap();
    println!("Next:{}",ret.0);
    for s in ret.1
    {
        println!("{}",s);
    }

}

#[test]
fn test_sql_expr() {
    let exprSelect = "select a,b,c,a*c as f, d + 'abc ' as g from tableA index by a >= 20 and a < 80 filter married = 1 offset 1000 ;";
    let exprInsert = r#"
    insert into MyTable
    ( Column1, Column2, Column3 )
    values
    ('John', 123, 'Lloyds Office'), 
    ('Jane', 124, 'Lloyds Office'), 
    ('Billy', 125, 'London Office'),
    ('Miranda', 126, 'Bristol Office');
   "#;

    let exprUpdate = "update tableA set a = 30, b = 'abc' ,d = 26 index by a >= 20 and a < 80;";
    let exprDelete = "delete from tableA index by a >= 20 and a < 80;";
    let exprCreateTable =r#"
    create table table1123
    ( 
        a int8,
        b int64,
        c vchar,
        d bool,
        e int16,
        primary key (a,b),
        index (c,d),
        index (e,f),
    );
   "#;

    let ret = ExprSQL().parse(&exprSelect).unwrap();
    println!("Select  Expr:{}  Next:{}",ret.1,ret.0);

    let ret = ExprSQL().parse(&exprInsert).unwrap();
    println!("Insert  Expr:{}  Next:{}",ret.1,ret.0);

    let ret = ExprSQL().parse(&exprUpdate).unwrap();
    println!("Update  Expr:{}  Next:{}",ret.1,ret.0);

    let ret = ExprSQL().parse(&exprDelete).unwrap();
    println!("Delete  Expr:{}  Next:{}",ret.1,ret.0);

    let ret = ExprSQL().parse(&exprCreateTable).unwrap();
    println!("Create Table  Expr:{}  Next:{}",ret.1,ret.0);
}



#[test]
fn test_index_expr() {
    let exp = "index by a >= 20 and a < 80 ;";
    let ret = ExprIndex().parse(exp).unwrap();
    println!("{}  Expr:{}  Next:{}",exp,ret.1,ret.0);

    let exp = "index by a >= 20 ;";
    let ret = ExprIndex().parse(exp).unwrap();
    println!("{}  Expr:{}  Next:{}",exp,ret.1,ret.0);

    let exp = "filter a = 20 ;";
    let ret = ExprFilter().parse(exp).unwrap();
    println!("{}  Expr:{}  Next:{}",exp,ret.1,ret.0);
}
