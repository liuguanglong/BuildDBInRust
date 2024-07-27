use std::fmt;
use super::{lib::*, statement::*, expr::{ Expr}};

pub struct SelectExpr{
    Scan:ScanExpr,
    Name:Vec<Vec<u8>>,
    Ouput:Vec<Expr>,
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
            either(
                map(is_literal(","),|c| Vec::new()),
                right(
                    match_literal("as"),
                    remove_lead_space(id_string())
                ).map( |c| c.into_bytes())
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

    let exp = "select a,b,c,a*c as f, d + 'abc ' as g from";
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
