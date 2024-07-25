use std::fmt;

use crate::btree::table;

use super::Expr::{id, number_i64, Value};
use super::{Expr::Expr, Parser};
use super::*;

struct ScanExpr{
    Table:Vec<u8>,

    // INDEX BY xxx
    Key1: Option<Box<Expr>>,
    Key2: Option<Box<Expr>>,
    // FILTER xxx
    Filter: Option<Box<Expr>>, // boolean, optional
    // LIMIT x, y
    Offset:usize,
    Limit:usize,
}

impl fmt::Display for ScanExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Table:{} |",String::from_utf8(self.Table.to_vec()).unwrap());
        if let Some(key1) = &self.Key1
        {
            write!(f, "Key1:{} |",key1);
        }
        if let Some(key2) = &self.Key2
        {
            write!(f, "Key2:{} |",key2);
        }

        if let Some(filter) = &self.Filter
        {
            write!(f, "filter:{} |",filter);
        }
        write!(f, "offset:{} limit{}|",self.Offset,self.Limit);

        write!(f, " ")
    }
}

impl ScanExpr{
    pub fn new(name:Vec<u8>)->Self
    {
        ScanExpr{
            Table:name,
            Key1:None,
            Key2:None,
            Filter:None,
            Offset:0,
            Limit:100,
        }
    }

}

fn ExprIndex<'a>() -> impl Parser<'a,Expr>
{
    //index by a >= 20 and a < 80 
    right(
        match_literal("index"),
        right(
            right(space0(),match_literal("by")),
            right(space0(),Expr())
        )
    )
}

fn ExprFilter<'a>() -> impl Parser<'a,Expr>
{
    //index by a >= 20 and a < 80 
    right(
        match_literal("filter"),
            right(space0(),Expr())
    )
}

fn ExprLimit<'a>() -> impl Parser<'a,Expr>
{
    right(
        match_literal("limit"),
            right(space0(),number_i64())
    ).map(|v| Expr::constExpr(v))
}

fn ExprOffset<'a>() -> impl Parser<'a,Expr>
{
    right(
        match_literal("offset"),
            right(space0(),number_i64())
    ).map(|v| Expr::constExpr(v))
}

fn ExprScanItems<'a>() -> impl Parser<'a,Vec<(String,Expr)>>
{
    zero_or_more(
        right(space0(),
            either(ExprIndex().map(|v| ("Index".to_string(),v)), 
                either(
                        ExprFilter().map(|v| ("Filter".to_string(),v)), 
                            either(
                                ExprLimit().map(|v| ("Limit".to_string(),v)),
                                ExprOffset().map(|v| ("Offset".to_string(),v))
                            )
                    )
                )
            )
        )    
}

fn ExprScan<'a>() -> impl Parser<'a,ScanExpr> 
{
    //from tableA index by a >= 20 and a < 80 filter married = 1 limit 200 offset 1000;
    right(
        match_literal("from"), 
        pair(
            right(space0(),id()), 
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
                        scan.Key1 = p.1.left.clone();
                        scan.Key2 = p.1.right.clone();
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

#[test]
fn test_scan_expr() {
    let exp = "index by a >= 20 and a < 80 ;";
    let ret = ExprIndex().parse(exp).unwrap();
    println!("{}  Expr:{}  Next:{}",exp,ret.1,ret.0);

    let exp = "index by a >= 20 ;";
    let ret = ExprIndex().parse(exp).unwrap();
    println!("{}  Expr:{}  Next:{}",exp,ret.1,ret.0);

    let exp = "filter a = 20 ;";
    let ret = ExprFilter().parse(exp).unwrap();
    println!("{}  Expr:{}  Next:{}",exp,ret.1,ret.0);

    let exp = "from tableA index by a >= 20 and a < 80 filter married = 1 limit 200 offset 1000 ;";
    let ret = ExprScan().parse(exp).unwrap();
    println!("{}  Scan:|{}  Next:{}",exp,ret.1,ret.0);

    let exp = "from tableA ;";
    let ret = ExprScan().parse(exp).unwrap();
    println!("{}  Scan:|{}  Next:{}",exp,ret.1,ret.0);

    let exp = "from tableA index by a >= 20 and a < 80  ;";
    let ret = ExprScan().parse(exp).unwrap();
    println!("{}  Scan:|{}  Next:{}",exp,ret.1,ret.0);

    let exp = "from tableA filter married = 1 limit 200 offset 1000 ;";
    let ret = ExprScan().parse(exp).unwrap();
    println!("{}  Scan:|{}  Next:{}",exp,ret.1,ret.0);

    let exp = "from tableA index by a >= 20 and a < 80 limit 200;";
    let ret = ExprScan().parse(exp).unwrap();
    println!("{}  Scan:|{}  Next:{}",exp,ret.1,ret.0);

    let exp = "from tableA index by a >= 20 and a < 80 filter married = 1 offset 1000 ;";
    let ret = ExprScan().parse(exp).unwrap();
    println!("{}  Scan:|{}  Next:{}",exp,ret.1,ret.0);
    
}

