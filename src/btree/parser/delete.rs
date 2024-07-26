use std::fmt;
use crate::btree::table::value::Value;
use super::{lib::*, statement::*, expr::{id, Expr}};


struct DeleteExpr{
    Scan:ScanExpr,
}

impl fmt::Display for DeleteExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Expr: =>| {} |",self.Scan)
    }
}

//stmt delete
fn ExprDelete<'a>() -> impl Parser<'a,DeleteExpr> 
{
    left(
        right(
            right(space0(),match_literal("delete")),
            right(space0(),ExprFrom()),
        ),
        right(space0(),match_literal(";")
            )
        )
        .map(|(expr)|
        {
            let mut delete = DeleteExpr{Scan:expr};
            delete
        }
    )   
}

#[test]
fn test_statement_expr() {

    let exp = "delete from tableA index by a >= 20 and a < 80;";
    let ret = ExprDelete().parse(exp).unwrap();
    println!("{} Next:{} | Delete:{}",exp,ret.0, ret.1);
}