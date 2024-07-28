use std::fmt;
use crate::btree::{table::{record::Record, table::TableDef, value::Value}, BTreeError};
use super::{lib::*, statement::*, expr::{id, Expr}};


pub struct DeleteExpr{
    Scan:ScanExpr,
}

impl DeleteExpr{
    pub fn createQuest<'a>(&'a self,tdef:&'a TableDef) -> Result<Record,BTreeError>
    {
        let mut r: Record = Record::new(&tdef);

        Ok(r)
    }

}
impl fmt::Display for DeleteExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Expr: =>| {} |",self.Scan)
    }
}

//stmt delete
pub fn ExprDelete<'a>() -> impl Parser<'a,DeleteExpr> 
{
    left(
        right(
            remove_lead_space_and_newline(match_literal("delete")),
            remove_lead_space_and_newline(ExprFrom()),
        ),
        remove_lead_space_and_newline(match_literal(";")
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