use std::fmt;
use crate::btree::table::value::Value;
use super::{lib::*, statement::*, expr::{id, Expr}};


pub struct UpdateExpr{
    pub Scan:ScanExpr,
    pub Name:Vec<Vec<u8>>,
    pub Values:Vec<Expr>,
}

impl UpdateExpr{
    pub fn new(expr:ScanExpr)->Self
    {
        UpdateExpr{
            Scan:expr,
            Name:Vec::new(),
            Values:Vec::new(),
        }
    }
}

impl fmt::Display for UpdateExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Expr: => {} | Values:=> ",self.Scan);
        for i in 0..self.Name.len()
        {
            write!(f,"{}:{}",String::from_utf8(self.Name[i].to_vec()).unwrap(),self.Values[i]);
        }
        write!(f, " ")
        }
}


fn ExprUpdateItem<'a>() -> impl Parser<'a,(String,Expr)> 
{
    pair(
        remove_lead_space(id_string()),
        right( 
            remove_lead_space(match_literal("=")),
            remove_lead_space(Expr())
        )
    )   
}

//update items
fn ExprUpdateItems<'a>() -> impl Parser<'a,Vec<(String,Expr)>> 
{
    right(
        match_literal("set"),
            pair(
                    ExprUpdateItem(),                
                    zero_or_more(
                        right(
                            remove_lead_space(match_literal(",")),
                            remove_lead_space(ExprUpdateItem())
                        )
                    )
            )        
        ).map(|(first,mut tail)|{
            tail.insert(0,first);
            tail
        }
    )
    
}

//update 
pub fn ExprUpdate<'a>() -> impl Parser<'a,UpdateExpr> 
{
    right(
        remove_lead_space_and_newline(match_literal("update")),
        tuple3(
            remove_lead_space_and_newline(id_string()),
            remove_lead_space_and_newline(ExprUpdateItems()),
            left(
               ExprScanItems(),
               remove_lead_space_and_newline(match_literal(";")))
        )
    ).map(|(table,values,props)|
    {
        let mut scan = ScanExpr::new(table.into_bytes());
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

        let mut update = UpdateExpr::new(scan);
        for item in values
        {
            update.Name.push(item.0.into_bytes());
            update.Values.push(item.1);
        }
        update
    })
}

#[test]
fn test_statement_expr() {

    let exp = "update tableA set a = 30, b = 'abc' ,d = 26 index by a >= 20 and a < 80;";
    let ret = ExprUpdate().parse(exp).unwrap();
    println!("{} Next:{} | update:{}",exp,ret.0, ret.1);


    let exp = "set a = 30, b = 'abc' ,d = 26 index ";
    let ret = ExprUpdateItems().parse(exp).unwrap();
    println!("{} Next:{}",exp,ret.0);
    for item in ret.1
    {
        println!("{}:{}",item.0,item.1)
    }
}