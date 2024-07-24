use winapi::shared::evntrace::EVENT_INSTANCE_HEADER_u2;

use super::*;

const KEYS: [&str; 6] = ["SELECT", "NOT", "AND", "INDEX", "FROM","FILTER"];

#[derive(Clone,Debug,PartialEq)]
pub enum Value{
    BYTES(Vec<u8>),
    INT64(i64),
    BOOL(bool),
    ID(Vec<u8>),
    None,
}

#[derive(Clone, Debug, PartialEq)]
struct Expr {
    op: ExpressionType,
    val: Option<Value>,
    left: Option<Box<Expr>>,
    right: Option<Box<Expr>>
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
enum ExpressionType
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
    None,
}
fn OpNot<'a>() -> impl Parser<'a,ExpressionType>{
    match_literal("NOT").map( |c| ExpressionType::NOT)
}

fn OpAnd<'a>() -> impl Parser<'a,ExpressionType>{
    match_literal("AND").map( |c| ExpressionType::AND)
}

fn OpOr<'a>() -> impl Parser<'a,ExpressionType>{
    match_literal("OR").map( |c| ExpressionType::OR)
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

fn number_i64<'a>() -> impl Parser<'a,Value>
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

fn id<'a>() -> impl Parser<'a,Value>{
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

fn Constant<'a>() -> impl Parser<'a,Value>
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

fn OpTerm<'a>() -> impl Parser<'a,ExpressionType>
{
    right(space0(),either(
        OpMultiply(), 
        either(
            OpDivide(), 
            OpModulo()
        )))
}

fn Term<'a>() -> impl Parser<'a,Expr>
{
    pair(Operand(),
        pair( OpTerm(),right(space0(),Operand()))        
    ).map(|(v,
        (op,v1))|
        Expr::BinaryExpr(op, v, v1)        
    )
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
        Term().parse("3 * abc AND")
    );


}


#[test]
fn singlequoted_string_parse()
{
}
