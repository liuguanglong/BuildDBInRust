
use std::{num::ParseFloatError};

pub type ParserResult<'a,Output> = Result<(&'a str,Output),&'a str>;

//Boxed Parser
pub trait Parser<'a,Output>{
    fn parse(&self,input:&'a str)->ParserResult<'a,Output>;

    fn map<F,NewOutPut>(self,map_fn:F)->BoxedParser<'a,NewOutPut>
    where 
       Self:Sized + 'a,
       Output:'a,
       NewOutPut:'a,
       F: Fn(Output) -> NewOutPut + 'a,
    {
        BoxedParser::new(map(self,map_fn))
    }

    fn pred<F>(self,pref_fn:F)->BoxedParser<'a,Output>
    where 
        Self:Sized + 'a,
        Output:'a,
        F: Fn(&Output) -> bool + 'a,
    {
        BoxedParser::new(pred(self,pref_fn))
    }

    fn and_then<F,NextParser,NewOutput>(self,f:F)->BoxedParser<'a,NewOutput>
    where 
       Self:Sized + 'a,
       Output: 'a,
       NewOutput: 'a,
       NextParser: Parser<'a,NewOutput> + 'a,
       F: Fn(Output) -> NextParser + 'a,
    {
        BoxedParser::new(and_then(self, f))
    }
}

pub fn map<'a,P,F,A,B>(parser:P,map_fn:F)-> impl Parser<'a,B>
where 
   P: Parser<'a,A>,
   F: Fn(A) -> B,
 {
    move |input| 
        parser.parse(input).map(|(last,a)| (last,map_fn(a))
    )
 }


 pub struct BoxedParser<'a,Output> {
    parser:Box<dyn Parser<'a,Output> + 'a>,
}

impl<'a,Output> BoxedParser<'a,Output>{

    fn new<P>(parser:P)->Self
    where 
        P: Parser<'a,Output> + 'a,
    {
        BoxedParser{
            parser: Box::new(parser),
        }
    }
}

impl<'a,Output> Parser<'a,Output> for BoxedParser<'a,Output>
{
    fn parse(&self,input:&'a str)->ParserResult<'a,Output> {
        self.parser.parse(input)
    }
}

impl<'a,F,Output> Parser<'a,Output> for F
where 
  F: Fn(&'a str) -> ParserResult<Output>
{
    fn parse(&self,input:&'a str)->ParserResult<'a,Output> {
        self(input)
    }
}

pub fn identifier(input:&str) -> ParserResult<String>
{
    let mut matched = String::new();
    let mut chars = input.chars();

    match chars.next(){
        Some(c) if c.is_alphabetic() =>{
            matched.push(c)
        },
        _=> return Err(input),
    }

    while let Some(next) = chars.next()
    {
        if next.is_alphanumeric() || next == '-'
        {
            matched.push(next);
        }
        else {
            break;;
        }
    }

    Ok((&input[matched.len()..],matched))
}

pub fn id_string<'a>() -> impl Parser<'a,String>{
    move |input| identifier.parse(input)
}

pub fn pair<'a,P1,P2,R1,R2>(p1:P1,p2:P2) -> impl Parser<'a,(R1,R2)>
where 
 P1 : Parser<'a,R1>,
 P2 : Parser<'a,R2>
 {
    move |input| {
        p1.parse(input).and_then(|(next,r1)| 
            {
                p2.parse(next).map(|(last,r2)|(last,(r1,r2)))
            }
        )

    }
 }

pub fn tuple3<'a,P1,P2,P3,R1,R2,R3>(p1:P1,p2:P2,p3:P3) -> impl Parser<'a,(R1,R2,R3)>
where 
    P1 : Parser<'a,R1> + 'a,
    P2 : Parser<'a,R2> + 'a,
    P3 : Parser<'a,R3> + 'a
{
move |input| {
    p1.parse(input).and_then(|(next, r1)| {
        p2.parse(next).and_then(|(next, r2)| {
            p3.parse(next).map(|(last, r3)| (last, (r1, r2, r3)))
        })
    })
}
}

pub fn tuple4<'a,P1,P2,P3,P4,R1,R2,R3,R4>(p1:P1,p2:P2,p3:P3,p4:P4) -> impl Parser<'a,(R1,R2,R3,R4)>
where 
    P1 : Parser<'a,R1> + 'a,
    P2 : Parser<'a,R2> + 'a,
    P3 : Parser<'a,R3> + 'a,
    P4 : Parser<'a,R4> + 'a
{
    move |input| {
        p1.parse(input).and_then(|(next, r1)| {
            p2.parse(next).and_then(|(next, r2)| {
                p3.parse(next).and_then(|(next, r3)| {
                    p4.parse(next).map(|(last, r4)| (last, (r1, r2, r3,r4)))
                })
            })
        })
    }
}

pub fn left<'a,P1,P2,R1,R2>(p1:P1,p2:P2) -> impl Parser<'a,R1>
where 
P1 : Parser<'a,R1>,
P2 : Parser<'a,R2>
{
    map(pair(p1,p2), |(left,_Right)| left)
}

pub fn remove_lead_space<'a,P1,R1>(p1:P1) -> impl Parser<'a, R1>
where
  P1 : Parser<'a,R1>
{
    right(space0(),p1)
}

pub fn remove_lead_space_and_newline<'a,P1,R1>(p1:P1) -> impl Parser<'a, R1>
where
  P1 : Parser<'a,R1>
{
    right(zero_or_more(space_char()),p1)
}

pub fn right<'a,P1,P2,R1,R2>(p1:P1,p2:P2) -> impl Parser<'a,R2>
where 
P1 : Parser<'a,R1>,
P2 : Parser<'a,R2>
{
    map(pair(p1,p2), |(_left,Right)| Right)
}

pub fn match_literal<'a>(expected:&'static str)-> 
    impl Parser<'a,()>
{
    move |input:&'a str| match input.get(0..expected.len()){
        Some(next) if next == expected => Ok((&input[expected.len()..],())),
        _=>Err(input)
    } 
}

pub fn is_literal<'a>(expected:&'static str)-> 
    impl Parser<'a,()>
{
    move |input:&'a str| match input.get(0..expected.len()){
        Some(next) if next == expected => Ok((&input[0..],())),
        _=>Err(input)
    } 
}

pub fn zero_or_more<'a,P1,R1>(p1:P1) -> impl Parser<'a, Vec<R1>>
where
  P1 : Parser<'a,R1>
{
    move |mut input|{
        let mut ret = Vec::new();

            while let Ok((next,r)) = p1.parse(input)
            {
                input = next;
                ret.push(r);
            }

            Ok((input,ret))
    }
}

pub fn one_or_more<'a,P1,R1>(p1:P1) -> impl Parser<'a, Vec<R1>>
where
  P1 : Parser<'a,R1>
{
    move |mut input| {
        let mut result = Vec::new();

        if let Ok((next_input, first_item)) = p1.parse(input) {
            input = next_input;
            result.push(first_item);
        } else {
            return Err(input);
        }

        while let Ok((next_input, next_item)) = p1.parse(input) {
            input = next_input;
            result.push(next_item);
        }

        Ok((input, result))
    }
}

pub fn chain<'a,P1,P2,R1,R2,R3,F1,F2,F3>(parser1:P1,parser2:P2,fn1:F1,fn2:F2,fn3:F3) -> impl Parser<'a,R3>
where 
  P1: Parser<'a,R1>,
  P2: Parser<'a,R2>,  
  F1: Fn()->R3,
  F2: Fn(&R3,R1)->R3,
  F3: Fn(&R3,R2)->R3,
{
    move |mut input| {

        let mut result = fn1();
        if let Ok((next_input, first_item)) = parser1.parse(input) {
            result = fn2(&result,first_item);
            input = next_input;
        } else {
            return Err(input);
        }
 
        while let Ok((next_input, next_item)) = parser2.parse(input) {
            input = next_input;
            result = fn3(&result,next_item)
        }

        Ok((input, result))
    }
}


pub fn either<'a,P1,P2,A>(parser1:P1,parser2:P2)-> impl Parser<'a,A>
where 
   P1: Parser<'a,A>,
   P2: Parser<'a,A>,
{
    move |input| match parser1.parse(input){
        ok @ Ok(_) => ok,
        Err(_) => parser2.parse(input),
    }
}

pub fn either3<'a,P1,P2,P3,A>(parser1:P1,parser2:P2,parser3:P3)-> impl Parser<'a,A>
where 
   P1: Parser<'a,A>,
   P2: Parser<'a,A>,
   P3: Parser<'a,A>,
{
    either(parser1,either(parser2, parser3))
}

pub fn either4<'a,P1,P2,P3,P4,A>(parser1:P1,parser2:P2,parser3:P3,parser4:P4)-> impl Parser<'a,A>
where 
   P1: Parser<'a,A>,
   P2: Parser<'a,A>,
   P3: Parser<'a,A>,
   P4: Parser<'a,A>,
{
    either(parser1,either(parser2,  either(parser3, parser4)))
}

pub fn any_char(input:&str)->ParserResult<char>
{
    match input.chars().next(){
        Some(c) => Ok((&input[c.len_utf8()..],c)),
        _ => Err(input)
    }
}

pub fn pred<'a,P,A,F>(parser:P,pred:F)->impl Parser<'a,A>
where 
   P: Parser<'a,A>,
   F: Fn(&A)->bool,
{
    move |input| 
    {
        if let Ok((next,v)) = parser.parse(input)
        {
            if pred(&v) == true
            {
                return Ok((next,v));
            }
        }
        Err(input)
    }
}

pub fn and_then<'a,P,F,A,B,NextP>(parser:P,f:F)->impl Parser<'a,B>
where 
  P: Parser<'a,A>,
  NextP: Parser<'a,B>,
  F: Fn(A)->NextP,
{
    move |input| match parser.parse(input)
    {
        Ok((input_next,result)) => f(result).parse(input_next),
        Err(err) => Err(err)
    }

}


pub fn whitesapce_char<'a>() -> impl Parser<'a,char>
{
    pred(any_char,|c| c.is_whitespace())
}

pub fn space_char<'a>() -> impl Parser<'a,char>
{
    pred(any_char,|c| 
        c.is_whitespace() || *c == '\r' || *c == '\n'
    )
}

pub fn space1<'a>() -> impl Parser<'a,Vec<char>>{
    one_or_more(whitesapce_char())
}

pub fn space0<'a>() -> impl Parser<'a,Vec<char>>{
    zero_or_more(whitesapce_char())
}

pub fn quoted_string<'a>() -> impl Parser<'a,String>
{
    right(
        match_literal("\""),
        left(
            zero_or_more(any_char.pred(|c| *c != '"')),
            match_literal("\""),
        ),
        )
    .map(|chars| chars.into_iter().collect())    
}

pub fn number_string<'a>() -> impl Parser<'a,String>
{
    map(
        one_or_more(any_char.pred( |c| c.is_numeric())),
    |(chars)| chars.into_iter().collect()
    )
}

pub fn number_string_withlead<'a>() -> impl Parser<'a,String>
{
    map(
        right(
            match_literal("."),
            one_or_more(any_char.pred( |c| c.is_numeric()))),
        |(chars)| chars.into_iter().collect()
    )
}

pub fn f64_string<'a>() -> impl Parser<'a,String>
{
    number_string().and_then( |v| {
            either(
                map(is_literal(" "),|c| String::from("")),
                number_string_withlead()
            ).map(
                move |v1| {
                    if v1.len() != 0
                    {
                       v.clone() + "." + &v1
                    }
                    else
                    {
                        v.clone()
                    }
                }
            )
        }
    )
}



#[test]
fn quoted_number_parse()
{
    assert_eq!(
        Ok(("","2345".to_string())),
        number_string().parse("2345"));

    assert_eq!(
        Ok((" column1","2345".to_string())),
          number_string().parse("2345 column1"));

    assert_eq!(
        Ok(("","2345".to_string())),
        number_string_withlead().parse(".2345"));
    
    assert_eq!(
        Err((" abcd")),
        number_string_withlead().parse(" abcd"));

    assert_eq!(
        Ok((" abc","234".to_string())),
        f64_string().parse("234 abc") 
    );

    assert_eq!(
        Ok(("  abc","234.345".to_string())),
        f64_string().parse("234.345  abc") 
    );
    
    assert_eq!(
        Err("abc"),
        f64_string().parse("234.abc") 
    );

    assert_eq!(
        Err(" abc"),
        f64_string().parse("234. abc") 
    );


}

#[test]
fn quoted_string_parse()
{
    assert_eq!(
        Ok(("","Hello Rust".to_string())),
        quoted_string().parse("\"Hello Rust\""))
}

#[test]
fn predicate_combinator() {
    let parser = pred(any_char, |c| *c == 'o');
    assert_eq!(Ok(("mg", 'o')), parser.parse("omg"));
    assert_eq!(Err("lol"), parser.parse("lol"));
}


#[test]
fn one_or_more_combinator() {
    let parser = one_or_more(match_literal("ha"));
    assert_eq!(Ok(("", vec![(), (), ()])), parser.parse("hahaha"));
    assert_eq!(Err("ahah"), parser.parse("ahah"));
    assert_eq!(Err(""), parser.parse(""));
}

#[test]
fn zero_or_more_combinator() {
    let parser = zero_or_more(match_literal("ha"));
    assert_eq!(Ok(("", vec![(), (), ()])), parser.parse("hahaha"));
    assert_eq!(Ok(("ahah", vec![])), parser.parse("ahah"));
    assert_eq!(Ok(("", vec![])), parser.parse(""));
}


#[test]
fn test_match_literal()
{
    let parseHelloRust = match_literal("Hello Rust!");
    assert_eq!(Ok(("",())),parseHelloRust.parse("Hello Rust!"));
    assert_eq!(Ok(("Hello",())),parseHelloRust.parse("Hello Rust!Hello"));
    assert_eq!(Err("Hello Zig!"),parseHelloRust.parse("Hello Zig!"));
}

#[test]
fn test_match_id()
{
    assert_eq!(
        Ok(("", "i-am-an-identifier".to_string())),
        identifier.parse("i-am-an-identifier")
    );
    assert_eq!(
        Ok((" entirely an identifier", "not".to_string())),
        identifier.parse("not entirely an identifier")
    );
    assert_eq!(
        Err("!not at all an identifier"),
        identifier.parse("!not at all an identifier")
    );
}

#[test]
fn test_air_combinator() {
    let tag_opener = pair(match_literal("<"), identifier);
    assert_eq!(
        Ok(("/>", ((), "my-first-element".to_string()))),
        tag_opener.parse("<my-first-element/>")
    );
    assert_eq!(Err("oops"), tag_opener.parse("oops"));
    assert_eq!(Err("!oops"), tag_opener.parse("<!oops"));
}