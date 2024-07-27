use std::fmt;
use serde_json::Value;

use crate::btree::table::{table::TableDef, value::ValueType};

use super::{lib::*, statement::*, expr::{ Expr}};


fn ExprValueType<'a>() -> impl Parser<'a,ValueType>
{
    either(
        either4(
            match_literal("int8").map(|_| ValueType::INT8), 
            match_literal("int16").map(|_| ValueType::INT16), 
            match_literal("int32").map(|_| ValueType::INT32), 
            match_literal("int64").map(|_| ValueType::INT64), 
        ),
        either(
            match_literal("vchar").map(|_| ValueType::BYTES), 
            match_literal("bool").map(|_| ValueType::BOOL), 
        ),
    )
}  

fn ExprTableName<'a>() -> impl Parser<'a,String>
{
    tuple3(
        remove_lead_space_and_newline(match_literal("create")),
        remove_lead_space_and_newline(match_literal("table")),
        remove_lead_space_and_newline(id_string()),
        ).map(|(_,_,name)| name)
}

fn ExprColumn<'a>() -> impl Parser<'a,(String,ValueType)>
{
    tuple3(
        remove_lead_space_and_newline(id_string()),
        remove_lead_space_and_newline(ExprValueType()),
        remove_lead_space_and_newline(match_literal(","))                      
    ).map(|(id,colType,_)| (id,colType))
}

fn ExprIndex<'a>() -> impl Parser<'a,Vec<String>>
{
    tuple3(
        remove_lead_space_and_newline(match_literal("index")),
        remove_lead_space_and_newline(match_literal("(")),
        tuple4(
            remove_lead_space_and_newline(id_string()),
            zero_or_more(
            right(
               remove_lead_space_and_newline(match_literal(",")),
               remove_lead_space_and_newline(id_string())
                )),
            remove_lead_space_and_newline(match_literal(")")),
            remove_lead_space_and_newline(match_literal(","))
        )     
    ).map( |(_,_,(first,mut tail,_,_))|
        {
            tail.insert(0,first);
            tail
        }
    )
}

fn ExprPrimaryKey<'a>() -> impl Parser<'a,Vec<String>>
{
    tuple4(
        remove_lead_space_and_newline(match_literal("primary")),
        remove_lead_space_and_newline(match_literal("key")),
        remove_lead_space_and_newline(match_literal("(")),
        tuple4(
            remove_lead_space_and_newline(id_string()),
            zero_or_more(
            right(
               remove_lead_space_and_newline(match_literal(",")),
               remove_lead_space_and_newline(id_string())
                )),
            remove_lead_space_and_newline(match_literal(")")),
            remove_lead_space_and_newline(match_literal(","))
        )     
    ).map( |(_,_,_,(first,mut tail,_,_))|
        {
            tail.insert(0,first);
            tail
        }
    )    
    
}

fn ExprColumns<'a>() -> impl Parser<'a,(Vec<(String,ValueType)>,Vec<String>,Vec<Vec<String>>)>
{
    tuple4(
        remove_lead_space_and_newline(match_literal("(")), 
        tuple3(
            one_or_more(remove_lead_space_and_newline(ExprColumn())),
            remove_lead_space_and_newline(ExprPrimaryKey()),                    
            one_or_more(remove_lead_space_and_newline(ExprIndex())),  
           ),
           remove_lead_space_and_newline(match_literal(")")),
           remove_lead_space_and_newline(match_literal(";")),
        ).map(|(_,v,_,_)| v)

}

pub fn ExprCreateTable<'a>() -> impl Parser<'a,TableDef>
{
    pair(
        ExprTableName(),
        ExprColumns(),
    ).map (|(name,
        (cols,primaryKey,indexes))|
    {
        let mut def = TableDef::create(name);
        for c in cols
        {
            def.Cols.push(c.0.as_bytes().to_vec());
            def.Types.push(c.1);
        }

        let mut pkey = 0;
        for i in 0..primaryKey.len(){
            if def.Cols[i] != primaryKey[i].as_bytes().to_vec()
            {
                pkey = i;
            ;   break;
            }
        }

        def.PKeys = pkey as u16 + 1;

        for i in indexes
        {
            let mut index = Vec::new();
            for c in i
            {
                index.push(c.as_bytes().to_vec());
            }
            def.Indexes.push(index);
        }
        def
    }
    )
}

#[test]
fn test_statement_expr() {

    let exp = r#"
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
   let ret = ExprCreateTable().parse(exp).unwrap();
   println!("\n{} Next:{}\n Table:{}",exp,ret.0,ret.1);
    
}