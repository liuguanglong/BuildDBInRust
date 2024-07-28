use std::fmt;
use crate::btree::table::value::Value;

use super::lib::*;

const KEYS: [&str; 17] = ["select", "not", "and", "index", "from","filter","or","limit","by","as","insert","into","values","create","table","primary","key"];

#[derive(Clone, Debug, PartialEq)]
pub struct Expr {
    pub op: ExpressionType,
    pub val: Option<Value>,
    pub left: Option<Box<Expr>>,
    pub right: Option<Box<Expr>>
}

impl fmt::Display for Expr {

    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(l) = &self.left{
            if l.val.is_some()
            {
                write!(f,"{}",l);
            }
            else {
                write!(f,"({})",l);
            }
        }
        if self.op != ExpressionType::None
        {
            write!(f,"{}",self.op);
        }
        if let Some(r) = &self.right{
            if r.val.is_some()
            {
                write!(f,"{}",r);
            }
            else {
                write!(f,"({})",r);
            }
        }
        if let Some(v) = &self.val{
            write!(f,"{}",v);
        }
        
        write!(f," ")
    }
}

impl Expr{

    pub fn constExpr(v:Value)->Self
    {
        Expr{
            op:ExpressionType::None,
            left:None,
            right:None,
            val :Some(v),
        }
    }

    pub fn UnaryExpr(op:ExpressionType,left:Value)->Self
    {
        Expr{
            op:op,
            left: Some(Box::new(Expr::constExpr(left))),
            right:None,
            val :None,
        }
    }

    pub fn BinaryExpr(op:ExpressionType,left:Expr,right:Expr)->Self
    {
        Expr{
            op:op,
            left: Some(Box::new(left)),
            right:Some(Box::new(right)),
            val :None,
        }
    }

}
#[derive(Clone,Debug,PartialEq)]
pub enum ExpressionType
{
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    Power,
    LT,  //Less Than
    LE, 
    GE,  
    GT, //Great Then
    NOT,
    AND,
    OR,
    UnOP,
    EQ,
    UnEQ,
    None,
}

impl fmt::Display for ExpressionType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ExpressionType::Add => write!(f, " + "),
            ExpressionType::Subtract => write!(f, " - "),
            ExpressionType::Multiply => write!(f, " * "),
            ExpressionType::Divide => write!(f, " / "),
            ExpressionType::Modulo => write!(f, " % "),
            ExpressionType::Power => write!(f, "^"),
            ExpressionType::LT => write!(f, " < "),
            ExpressionType::LE => write!(f, " <= "),
            ExpressionType::GE => write!(f, " > "),
            ExpressionType::GT => write!(f, " >= "),
            ExpressionType::NOT => write!(f, " NOT "),
            ExpressionType::AND => write!(f, " AND "),
            ExpressionType::OR => write!(f, " OR "),
            ExpressionType::UnOP => write!(f, "-"),
            ExpressionType::None => write!(f, ""),
            ExpressionType::EQ => write!(f, " = "),
            ExpressionType::UnEQ => write!(f, " != "),

        }
    }
}

fn OpEQ<'a>() -> impl Parser<'a,ExpressionType>{
    match_literal("=").map( |c| ExpressionType::EQ)
}

fn OpUnEQ<'a>() -> impl Parser<'a,ExpressionType>{
    match_literal("!=").map( |c| ExpressionType::UnEQ)
}


fn OpNot<'a>() -> impl Parser<'a,ExpressionType>{
    match_literal("not").map( |c| ExpressionType::NOT)
}

fn OpAnd<'a>() -> impl Parser<'a,ExpressionType>{
    match_literal("and").map( |c| ExpressionType::AND)
}

fn OpOr<'a>() -> impl Parser<'a,ExpressionType>{
    match_literal("or").map( |c| ExpressionType::OR)
}

fn OpUnOp<'a>() -> impl Parser<'a,ExpressionType>{
    match_literal("-").map( |c| ExpressionType::UnOP)
}

fn OpAdd<'a>() -> impl Parser<'a,ExpressionType>{
    match_literal("+").map( |c| ExpressionType::Add)
}

fn OpSubstract<'a>() -> impl Parser<'a,ExpressionType>{
    match_literal("- ").map( |c| ExpressionType::Subtract)
}

fn OpMultiply<'a>() -> impl Parser<'a,ExpressionType>{
    match_literal("*").map( |c| ExpressionType::Multiply)
}

fn OpDivide<'a>() -> impl Parser<'a,ExpressionType>{
    match_literal("/").map( |c| ExpressionType::Divide)
}

fn OpModulo<'a>() -> impl Parser<'a,ExpressionType>{
    match_literal("%").map( |c| ExpressionType::Modulo)
}

fn OpPower<'a>() -> impl Parser<'a,ExpressionType>{
    match_literal("^").map( |c| ExpressionType::Power)
}

fn OpLT<'a>() -> impl Parser<'a,ExpressionType>{
    match_literal("<").map( |c| ExpressionType::LT)
}

fn OpLE<'a>() -> impl Parser<'a,ExpressionType>{
    match_literal("<=").map( |c| ExpressionType::LE)
}

fn OpGE<'a>() -> impl Parser<'a,ExpressionType>{
    match_literal(">=").map( |c| ExpressionType::GE)
}

fn OpGT<'a>() -> impl Parser<'a,ExpressionType>{
    match_literal(">").map( |c| ExpressionType::GT)
}

pub fn number_i64<'a>() -> impl Parser<'a,Value>
{
    map(
        one_or_more(any_char.pred( |c| c.is_numeric())),
    |(chars)| 
        {
            let s:String = chars.into_iter().collect();
            let v = s.parse::<i64>().unwrap();
            Value::INT64(v)
        }
    )
}

pub fn id<'a>() -> impl Parser<'a,Value>{
    pred(identifier,|v| notKey(v)).map( |v| {
        let mut bytes: Vec<u8> = Vec::new();

        for c in v.chars() {
            let mut buf = [0; 4]; //
            let encoded = c.encode_utf8(&mut buf);
            bytes.extend_from_slice(encoded.as_bytes());
        }
        Value::ID(bytes)    
    })
}



fn notKey(v:&String) -> bool
{
    for s in KEYS.iter() {
        if s == v
        {
            return false;
        }
    }
    true
}

fn singlequoted_string<'a>() -> impl Parser<'a,Value>
{
    right(
        match_literal("'"),
        left(
            zero_or_more(any_char.pred(|c| *c != '\'')),
            match_literal("'"),
        ),
        )
    .map(|chars| {
        let mut bytes: Vec<u8> = Vec::new();

        for c in chars {
            let mut buf = [0; 4]; //
            let encoded = c.encode_utf8(&mut buf);
            bytes.extend_from_slice(encoded.as_bytes());
        }
        Value::BYTES(bytes)    
    })
}

pub fn Constant<'a>() -> impl Parser<'a,Value>
{
    either(number_i64(), either(id(),singlequoted_string()))
}

fn OpNegtive<'a>() -> impl Parser<'a,ExpressionType>
{
    either(OpUnOp() ,OpNot())
}

fn Operand<'a>() -> impl Parser<'a,Expr>
{   
    either(
    Constant().map(|v| Expr::constExpr(v)),
    either(
        pair(OpUnOp(),Constant()),
        pair(OpNot(),right(space1(),Constant())),
    ).map( |(op,v)| 
            Expr::UnaryExpr(op, v)
        )
    )
}

fn OpMulType<'a>() -> impl Parser<'a,ExpressionType>
{
    right(space0(),either(
        OpMultiply(), 
        either(
            OpDivide(), 
            OpModulo()
        )))
}

fn OpAddType<'a>() -> impl Parser<'a,ExpressionType>
{
    right(space0(),either(
        OpAdd(), 
        OpSubstract(), 
    ))
}


fn ExprMul<'a>() -> impl Parser<'a,Expr>
{
    chain(
        Operand(),
        pair( OpMulType(),right(space0(),Operand())),
        newExpr,
        initExpr,
        addExpr
    )
}

fn newExpr() -> Expr
{
    Expr::constExpr(Value::None)
}

fn initExpr(r:&Expr,v:Expr) -> Expr
{
    v
}

fn addExpr(r:&Expr,v:(ExpressionType,Expr))-> Expr
{
    Expr::BinaryExpr(v.0, r.clone(), v.1)
}

fn ExprAdd<'a>() -> impl Parser<'a,Expr>
{
    chain(
        ExprMul(),
        pair( OpAddType(),right(space0(),ExprMul())),
        newExpr,
        initExpr,
        addExpr
    )
}

fn OpCmpType<'a>() -> impl Parser<'a,ExpressionType>
{
    right(space0(),
        either(
            OpLE(), 
            either(
                OpLT(),
                either(OpGE(),OpGT())
            )
        )
    )
}

fn ExprCmp<'a>() -> impl Parser<'a,Expr>
{
    chain(
        ExprAdd(),
        pair( OpCmpType(),right(space0(),ExprAdd())),
        newExpr,
        initExpr,
        addExpr
    )
}

fn OpEQType<'a>() -> impl Parser<'a,ExpressionType>
{
    right(space0(),
        either(
            OpEQ(), 
            OpUnEQ()
        )
    )
}

fn ExprEq<'a>() -> impl Parser<'a,Expr>
{
    chain(
        ExprCmp(),
        pair( OpEQType(),right(space0(),ExprCmp())),
        newExpr,
        initExpr,
        addExpr
    )
}

fn ExprLogicAnd<'a>() -> impl Parser<'a,Expr>
{
    chain(
        ExprEq(),
        pair( right(space0(),OpAnd()),right(space0(),ExprEq())),
        newExpr,
        initExpr,
        addExpr
    )
}

pub fn Expr<'a>() -> impl Parser<'a,Expr>
{
    chain(
        ExprLogicAnd(),
        pair( right(space0(),OpOr()),right(space0(),ExprLogicAnd())),
        newExpr,
        initExpr,
        addExpr
    )
}

// fn TupleItems<'a>() -> impl Parser<'a,Value>
// {
//     pair(
//         pred(identifier,|v| notKey(v)),
//         zero_or_more(right(space0(),
//                             right(
//                                 match_literal(","),
//                                 right(space0(),pred(identifier,|v| notKey(v)))
//                             )
//                         )
//                     )
//     ).map( |(first,mut tail)| 
//     {
//         tail.insert(0, first);
//         Value::Tuple(tail)
//     }
//     )
// }

// fn Tuple<'a>() -> impl Parser<'a,Value>
// {
//     right(
//         match_literal("("),
//         right(space0(), 
//             left(TupleItems(), right(space0(),match_literal(")"))
//             )
//         )
//     )
// }

#[test]
fn singlequoted_string_parse()
{
    // let exp = "name, age ,address, id, e";
    // let ret = TupleItems().parse(exp).unwrap();
    // println!("{}  Expr:{}  Next:{}",exp,ret.1,ret.0);

    // println!("");
    // let exp = "( name, age ,address, id, e )";
    // let ret = Tuple().parse(exp).unwrap();
    // println!("{}  Expr:{}  Next:{}",exp,ret.1,ret.0);
}

#[test]
fn Expr_()
{
    let exp = Expr::constExpr(Value::INT64(3));
    let exp2 = Expr::constExpr(Value::ID("abc".as_bytes().to_vec()));
    assert_eq!(
        Ok((" + 40",Expr::BinaryExpr(ExpressionType::Multiply,exp,exp2))),
        ExprMul().parse("3 * abc + 40")
    );

    let exp = "3 * abc / 20 + 40";
    let ret = ExprMul().parse(exp).unwrap();
    println!("{}  Expr:{}  Next:{}",exp,ret.1,ret.0);

    let exp = "3";
    let ret = ExprMul().parse(exp).unwrap();
    println!("{}  Expr:{}  Next:{}",exp,ret.1,ret.0);

    let exp = "3 * abc / 20 + 40";
    let ret = ExprAdd().parse(exp).unwrap();
    println!("{}  Expr:{}  Next:{}",exp,ret.1,ret.0);

    let exp = "3 * abc / 20 + 40 > ced";
    let ret = ExprCmp().parse(exp).unwrap();
    println!("{}  Expr:{}  Next:{}",exp,ret.1,ret.0);

    let exp = "3 * abc / 20 + 40 * ced = 400";
    let ret = ExprEq().parse(exp).unwrap();
    println!("{}  Expr:{}  Next:{}",exp,ret.1,ret.0);

    let exp = "3 * abc / 20 > 20 and 40 * ced = 400";
    let ret = ExprLogicAnd().parse(exp).unwrap();
    println!("{}  Expr:{}  Next:{}",exp,ret.1,ret.0);

    let exp = "3 * abc / 20 > 20";
    let ret = ExprLogicAnd().parse(exp).unwrap();
    println!("{}  Expr:{}  Next:{}",exp,ret.1,ret.0);

    let exp = "3 * abc / 20 > 20";
    let ret = Expr().parse(exp).unwrap();
    println!("{}  Expr:{}  Next:{}",exp,ret.1,ret.0);    

    let exp = "a > 20 and b < 40 or c < 'sdfsdf'";
    let ret = Expr().parse(exp).unwrap();
    println!("{}  Expr:{}  Next:{}",exp,ret.1,ret.0);    

    let exp = "20";
    let ret = Expr().parse(exp).unwrap();
    println!("{}  Expr:{}  Next:{}",exp,ret.1,ret.0);   

    let exp = "abc > 'edf'";
    let ret = Expr().parse(exp).unwrap();
    println!("{}  Expr:{}  Next:{}",exp,ret.1,ret.0);   

}

#[test]
fn Op_Parser()
{
    assert_eq!(
        Ok((" * abc",Value::INT64(3))),
        number_i64().parse("3 * abc"));
    assert_eq!(
        Ok(("",Value::BYTES(("Hello Rust").as_bytes().to_vec()))),
            singlequoted_string().parse("'Hello Rust'"));
    assert_eq!(
        Ok((" * abc",Value::INT64(3))),
        Constant().parse("3 * abc"));

    assert_eq!(
        Ok(("",Value::BYTES(("Hello Rust").as_bytes().to_vec()))),
        Constant().parse("'Hello Rust'"));    

    assert_eq!(
        Ok((" > 20",Value::ID(("col1").as_bytes().to_vec()))),
        Constant().parse("col1 > 20")
    );    
    
    assert_eq!(
        Ok((" > 20",Expr::UnaryExpr(ExpressionType::UnOP,Value::ID(("col1").as_bytes().to_vec())))),
        Operand().parse("-col1 > 20")
    );

    assert_eq!(
        Ok((" * abc",Expr::constExpr(Value::INT64(3)))),
        Operand().parse("3 * abc")
    );

    assert_eq!(
        Ok((" OR",Expr::UnaryExpr(ExpressionType::NOT,Value::ID(("col1").as_bytes().to_vec())))),
        Operand().parse("NOT col1 OR")
    );

    let exp = Expr::constExpr(Value::INT64(3));
    let exp2 = Expr::constExpr(Value::ID("abc".as_bytes().to_vec()));
    assert_eq!(
        Ok((" AND",Expr::BinaryExpr(ExpressionType::Multiply,exp,exp2))),
        ExprMul().parse("3 * abc AND")
    );

    let exp = Expr::constExpr(Value::INT64(3));
    let exp2 = Expr::constExpr(Value::ID("abc".as_bytes().to_vec()));
    let exp3 = Expr::constExpr(Value::INT64(20));
    let exp4 = Expr::constExpr(Value::INT64(4));

    let op1 = Expr::BinaryExpr(ExpressionType::Multiply,exp,exp2);
    let op2 = Expr::BinaryExpr(ExpressionType::Divide,exp3,exp4);
    assert_eq!(
        Ok(("",Expr::BinaryExpr(ExpressionType::Add,op1,op2))),
        ExprAdd().parse("3 * abc + 20 / 4")
    );

}


