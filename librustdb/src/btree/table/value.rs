use core::panic;
use std::{cmp::Ordering, fmt, ops::{Add, Div, Mul, Rem, Sub}};
use serde::{Serialize, Deserialize};
use crate::btree::{scan::comp::OP_CMP, BTreeError};
use std::cmp::PartialOrd;

pub enum ValueError{
    OperationNotSupported(String),
    ParamNotFound(String),
}

#[derive(Serialize,Clone,Deserialize, Debug,PartialEq)]
pub enum ValueType {
    BYTES,
    INT64,
    INT32,
    INT16,
    INT8,
    ID,
    BOOL
}

impl From<&str> for ValueType {
    fn from(item: &str) -> Self {
        match item{
            "BYTES" => ValueType::BYTES,
            "INT64" => ValueType::INT64,
            "INT32" => ValueType::INT32,
            "INT16" => ValueType::INT16,
            "INT8" => ValueType::INT8,
            "ID" => ValueType::ID,
            "BOOL" => ValueType::BOOL,
            _Other => panic!("Not Support!"),
        }
    }
}

impl fmt::Display for ValueType {

    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ValueType::BOOL => write!(f, "BOOL"),
            ValueType::INT8  => write!(f,"INT8"),
            ValueType::INT16  => write!(f,"INT16"),
            ValueType::INT32  => write!(f,"INT32"),
            ValueType::INT64  => write!(f,"INT64"),
            ValueType::BYTES  => write!(f,"BYTES"),
            ValueType::ID  => write!(f,"ID"),
        }
    }
}

#[derive(Clone, Debug,PartialEq)]
pub enum Value{
    BYTES(Vec<u8>),
    INT64(i64),
    INT32(i32),
    INT16(i16),
    INT8(i8),
    BOOL(bool),
    ID(Vec<u8>),
    None,
}


impl Value{

    pub fn GetValueType(&self)->ValueType
    {
        match  self {
            Value::BYTES(_) => ValueType::BYTES,
            Value::INT64(_) => ValueType::INT64,
            Value::INT32(_) => ValueType::INT32,
            Value::INT16(_) => ValueType::INT16,
            Value::INT8(_) => ValueType::INT8,
            Value::BOOL(_) => ValueType::BOOL,
            Value::ID(_) => ValueType::ID,
            Value::None => {panic!()},
        }
    }

    pub fn LogicOp(&self,v:Value,f: fn(bool,bool) -> bool)->Result<Value,BTreeError>
    {
        match (self,v) {
            (Value::BOOL(v), Value::BOOL(v1)) => Ok(Value::BOOL( f(*v,v1))),
            _Other => Err(BTreeError::OperationNotSupported(String::from("Compare"))),
        }
    }

    pub fn encodeVal(&self, list:&mut Vec<u8>) {

        match &self
         {
            Value::INT8(v) => list.extend_from_slice(&v.to_le_bytes()),
            Value::INT16(v) => list.extend_from_slice(&v.to_le_bytes()),
            Value::INT32(v) => list.extend_from_slice(&v.to_le_bytes()),
            Value::INT64(v) => list.extend_from_slice(&v.to_le_bytes()),
            Value::BOOL(v) => {
                if *v == true {
                    list.extend_from_slice(&[1;1]);
                } else {
                    list.extend_from_slice(&[0;1]);
                }
            },
            Value::BYTES(v) => {
                crate::btree::util::escapeString(v, list);
                //list.extend_from_slice(v);
                list.push(0);
            },
            _Other =>
            {

            }
        }
    }

    pub fn decodeVal(t:&ValueType,val:&[u8],pos: usize) -> (Value,usize) {
        match (t) {
            ValueType::INT8 => {
                return (Value::INT8(i8::from_le_bytes([val[pos];1])),1);
            },
            ValueType::INT16 => {
                return (Value::INT16(i16::from_le_bytes( val[pos..pos+2].try_into().unwrap() )),2);
            },
            ValueType::INT32 => {
                return (Value::INT32(i32::from_le_bytes( val[pos..pos+4].try_into().unwrap() )),4);
            },
            ValueType::INT64 => {
                return (Value::INT64(i64::from_le_bytes( val[pos..pos+8].try_into().unwrap() )),8);
            },
            ValueType::BOOL => {
                if val[pos] == 1 {
                    return (Value::BOOL(true),1);
                } else {
                    return (Value::BOOL(false),1);
                }
            },            
            ValueType::BYTES => {
                let mut end = pos;
                while val[end] != 0
                {
                    end += 1;
                }   
                let ret = crate::btree::util::deescapeString(val[pos..end].try_into().unwrap());
                return (Value::BYTES(ret), end - pos);
            },
            _=>{
                panic!()
            }
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Value::BOOL(val) => if *val {write!(f, "true")} else {write!(f, "false")},
            Value::INT8 (val) => write!(f,"{}",*val),
            Value::INT16 (val) => write!(f,"{}",*val),
            Value::INT32 (val) => write!(f,"{}",*val),
            Value::INT64 (val) => write!(f,"{}",*val),
            Value::BYTES (val) => write!(f,"{}",String::from_utf8(val.to_vec()).unwrap()),
            Value::ID (val) => write!(f,"{}",String::from_utf8(val.to_vec()).unwrap()),
            Value::None => write!(f,"None"),
        }
    }
}

impl Add for Value{
    type Output = Result<Value,BTreeError>;

    fn add(self, other: Value) ->Result<Value,BTreeError> {
        match (self,other) {
            (Value::BYTES(v), Value::BYTES(v1)) => { let mut r = v.clone(); r.extend(&v1); Ok(Value::BYTES(r))},
            (Value::INT64(v), Value::INT64(v1)) => Ok(Value::INT64(v + v1)),
            (Value::INT64(v), Value::INT32(v1)) => Ok(Value::INT64(v + v1 as i64 )),
            (Value::INT64(v), Value::INT16(v1)) => Ok(Value::INT64(v + v1 as i64 )),
            (Value::INT64(v), Value::INT8(v1)) => Ok(Value::INT64(v + v1 as i64)),
            (Value::INT32(v), Value::INT64(v1)) => Ok(Value::INT32(v + v1 as i32)),
            (Value::INT32(v), Value::INT32(v1)) => Ok(Value::INT32(v + v1 as i32)),
            (Value::INT32(v), Value::INT16(v1)) => Ok(Value::INT32(v + v1 as i32)),
            (Value::INT32(v), Value::INT8(v1)) => Ok(Value::INT32(v + v1 as i32)),
            (Value::INT16(v), Value::INT64(v1)) => Ok(Value::INT16(v + v1 as i16)),
            (Value::INT16(v), Value::INT32(v1)) => Ok(Value::INT16(v + v1 as i16)),
            (Value::INT16(v), Value::INT16(v1)) => Ok(Value::INT16(v + v1 as i16)),
            (Value::INT16(v), Value::INT8(v1)) => Ok(Value::INT16(v + v1 as i16)),
            (Value::INT8(v), Value::INT64(v1)) => Ok(Value::INT8(v + v1 as i8)),
            (Value::INT8(v), Value::INT32(v1)) => Ok(Value::INT8(v + v1 as i8)),
            (Value::INT8(v), Value::INT16(v1)) => Ok(Value::INT8(v + v1 as i8)),
            (Value::INT8(v), Value::INT8(v1)) => Ok(Value::INT8(v + v1 as i8)),
            _Other => Err(BTreeError::OperationNotSupported(String::from("Add"))),
        }
    }
}

impl Sub for Value
{
    type Output = Result<Value,BTreeError>;

    fn sub(self, rhs: Self) -> Self::Output {
        match (self,rhs) {
            (Value::INT64(v), Value::INT64(v1)) => Ok(Value::INT64(v - v1)),
            (Value::INT64(v), Value::INT32(v1)) => Ok(Value::INT64(v - v1 as i64 )),
            (Value::INT64(v), Value::INT16(v1)) => Ok(Value::INT64(v - v1 as i64 )),
            (Value::INT64(v), Value::INT8(v1)) => Ok(Value::INT64(v - v1 as i64)),
            (Value::INT32(v), Value::INT64(v1)) => Ok(Value::INT32(v - v1 as i32)),
            (Value::INT32(v), Value::INT32(v1)) => Ok(Value::INT32(v - v1 as i32)),
            (Value::INT32(v), Value::INT16(v1)) => Ok(Value::INT32(v - v1 as i32)),
            (Value::INT32(v), Value::INT8(v1)) => Ok(Value::INT32(v - v1 as i32)),
            (Value::INT16(v), Value::INT64(v1)) => Ok(Value::INT16(v - v1 as i16)),
            (Value::INT16(v), Value::INT32(v1)) => Ok(Value::INT16(v - v1 as i16)),
            (Value::INT16(v), Value::INT16(v1)) => Ok(Value::INT16(v - v1 as i16)),
            (Value::INT16(v), Value::INT8(v1)) => Ok(Value::INT16(v - v1 as i16)),
            (Value::INT8(v), Value::INT64(v1)) => Ok(Value::INT8(v - v1 as i8)),
            (Value::INT8(v), Value::INT32(v1)) => Ok(Value::INT8(v - v1 as i8)),
            (Value::INT8(v), Value::INT16(v1)) => Ok(Value::INT8(v - v1 as i8)),
            (Value::INT8(v), Value::INT8(v1)) => Ok(Value::INT8(v - v1 as i8)),
            _Other => Err(BTreeError::OperationNotSupported(String::from("Subtract"))),
        }
    }
}

impl Mul for Value
{
    type Output = Result<Value,BTreeError>;

    fn mul(self, rhs: Self) -> Self::Output {
        match (self,rhs) {
            (Value::INT64(v), Value::INT64(v1)) => Ok(Value::INT64(v * v1)),
            (Value::INT64(v), Value::INT32(v1)) => Ok(Value::INT64(v * v1 as i64 )),
            (Value::INT64(v), Value::INT16(v1)) => Ok(Value::INT64(v * v1 as i64 )),
            (Value::INT64(v), Value::INT8(v1)) => Ok(Value::INT64(v * v1 as i64)),
            (Value::INT32(v), Value::INT64(v1)) => Ok(Value::INT32(v * v1 as i32)),
            (Value::INT32(v), Value::INT32(v1)) => Ok(Value::INT32(v * v1 as i32)),
            (Value::INT32(v), Value::INT16(v1)) => Ok(Value::INT32(v * v1 as i32)),
            (Value::INT32(v), Value::INT8(v1)) => Ok(Value::INT32(v * v1 as i32)),
            (Value::INT16(v), Value::INT64(v1)) => Ok(Value::INT16(v * v1 as i16)),
            (Value::INT16(v), Value::INT32(v1)) => Ok(Value::INT16(v * v1 as i16)),
            (Value::INT16(v), Value::INT16(v1)) => Ok(Value::INT16(v * v1 as i16)),
            (Value::INT16(v), Value::INT8(v1)) => Ok(Value::INT16(v * v1 as i16)),
            (Value::INT8(v), Value::INT64(v1)) => Ok(Value::INT8(v * v1 as i8)),
            (Value::INT8(v), Value::INT32(v1)) => Ok(Value::INT8(v * v1 as i8)),
            (Value::INT8(v), Value::INT16(v1)) => Ok(Value::INT8(v * v1 as i8)),
            (Value::INT8(v), Value::INT8(v1)) => Ok(Value::INT8(v * v1 as i8)),
            _Other => Err(BTreeError::OperationNotSupported(String::from("Multiply"))),
        }
    }
}

impl Div for Value
{
    type Output = Result<Value,BTreeError>;

    fn div(self, rhs: Self) -> Self::Output {
        match (self,rhs) {
            (Value::INT64(v), Value::INT64(v1)) => Ok(Value::INT64(v / v1)),
            (Value::INT64(v), Value::INT32(v1)) => Ok(Value::INT64(v / v1 as i64 )),
            (Value::INT64(v), Value::INT16(v1)) => Ok(Value::INT64(v / v1 as i64 )),
            (Value::INT64(v), Value::INT8(v1)) => Ok(Value::INT64(v / v1 as i64)),
            (Value::INT32(v), Value::INT64(v1)) => Ok(Value::INT32(v / v1 as i32)),
            (Value::INT32(v), Value::INT32(v1)) => Ok(Value::INT32(v / v1 as i32)),
            (Value::INT32(v), Value::INT16(v1)) => Ok(Value::INT32(v / v1 as i32)),
            (Value::INT32(v), Value::INT8(v1)) => Ok(Value::INT32(v / v1 as i32)),
            (Value::INT16(v), Value::INT64(v1)) => Ok(Value::INT16(v / v1 as i16)),
            (Value::INT16(v), Value::INT32(v1)) => Ok(Value::INT16(v / v1 as i16)),
            (Value::INT16(v), Value::INT16(v1)) => Ok(Value::INT16(v / v1 as i16)),
            (Value::INT16(v), Value::INT8(v1)) => Ok(Value::INT16(v / v1 as i16)),
            (Value::INT8(v), Value::INT64(v1)) => Ok(Value::INT8(v / v1 as i8)),
            (Value::INT8(v), Value::INT32(v1)) => Ok(Value::INT8(v / v1 as i8)),
            (Value::INT8(v), Value::INT16(v1)) => Ok(Value::INT8(v / v1 as i8)),
            (Value::INT8(v), Value::INT8(v1)) => Ok(Value::INT8(v / v1 as i8)),
            _Other => Err(BTreeError::OperationNotSupported(String::from("Divide"))),
        }
    }
}

impl Rem for Value
{
    type Output = Result<Value,BTreeError>;

    fn rem(self, rhs: Self) -> Self::Output {
        match (self,rhs) {
            (Value::INT64(v), Value::INT64(v1)) => Ok(Value::INT64(v % v1)),
            (Value::INT64(v), Value::INT32(v1)) => Ok(Value::INT64(v % v1 as i64 )),
            (Value::INT64(v), Value::INT16(v1)) => Ok(Value::INT64(v % v1 as i64 )),
            (Value::INT64(v), Value::INT8(v1)) => Ok(Value::INT64(v % v1 as i64)),
            (Value::INT32(v), Value::INT64(v1)) => Ok(Value::INT32(v % v1 as i32)),
            (Value::INT32(v), Value::INT32(v1)) => Ok(Value::INT32(v % v1 as i32)),
            (Value::INT32(v), Value::INT16(v1)) => Ok(Value::INT32(v % v1 as i32)),
            (Value::INT32(v), Value::INT8(v1)) => Ok(Value::INT32(v % v1 as i32)),
            (Value::INT16(v), Value::INT64(v1)) => Ok(Value::INT16(v % v1 as i16)),
            (Value::INT16(v), Value::INT32(v1)) => Ok(Value::INT16(v % v1 as i16)),
            (Value::INT16(v), Value::INT16(v1)) => Ok(Value::INT16(v % v1 as i16)),
            (Value::INT16(v), Value::INT8(v1)) => Ok(Value::INT16(v % v1 as i16)),
            (Value::INT8(v), Value::INT64(v1)) => Ok(Value::INT8(v % v1 as i8)),
            (Value::INT8(v), Value::INT32(v1)) => Ok(Value::INT8(v % v1 as i8)),
            (Value::INT8(v), Value::INT16(v1)) => Ok(Value::INT8(v % v1 as i8)),
            (Value::INT8(v), Value::INT8(v1)) => Ok(Value::INT8(v % v1 as i8)),
            _Other => Err(BTreeError::OperationNotSupported(String::from("Modulo"))),
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self,other) {
            (Value::INT64(v), Value::INT64(v1)) => v.partial_cmp(&v1) ,
            (Value::INT64(v), Value::INT32(v1)) => v.partial_cmp(&(*v1 as i64)),
            (Value::INT64(v), Value::INT16(v1)) => v.partial_cmp(&(*v1 as i64)),
            (Value::INT64(v), Value::INT8(v1)) =>  v.partial_cmp(&(*v1 as i64)),
            (Value::INT32(v), Value::INT64(v1)) => v.partial_cmp(&(*v1 as i32)),
            (Value::INT32(v), Value::INT32(v1)) => v.partial_cmp(&(*v1 as i32)),
            (Value::INT32(v), Value::INT16(v1)) => v.partial_cmp(&(*v1 as i32)),
            (Value::INT32(v), Value::INT8(v1)) => v.partial_cmp(&(*v1 as i32)),
            (Value::INT16(v), Value::INT64(v1)) => v.partial_cmp(&(*v1 as i16)),
            (Value::INT16(v), Value::INT32(v1)) =>  v.partial_cmp(&(*v1 as i16)),
            (Value::INT16(v), Value::INT16(v1)) =>  v.partial_cmp(&(*v1 as i16)),
            (Value::INT16(v), Value::INT8(v1)) =>  v.partial_cmp(&(*v1 as i16)),
            (Value::INT8(v), Value::INT64(v1)) =>  v.partial_cmp(&(*v1 as i8)),
            (Value::INT8(v), Value::INT32(v1)) => v.partial_cmp(&(*v1 as i8)),
            (Value::INT8(v), Value::INT16(v1)) =>  v.partial_cmp(&(*v1 as i8)),
            (Value::INT8(v), Value::INT8(v1)) =>  v.partial_cmp(&(*v1 as i8)),
            (Value::BYTES(v), Value::BYTES(v1)) => v.partial_cmp(&v1) ,
            (Value::BOOL(v), Value::BOOL(v1)) =>  v.partial_cmp(&v1) ,
            _Other => None,
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format()
    {
        let v1 = Value::BOOL(true);
        let v2 = Value::INT8(20);
        let v3 = Value::INT16(256);
        let v4 = Value::INT32(123);
        let v5 = Value::INT64(4567);
        let v6 = Value::BYTES("test".as_bytes().to_vec());

        println!("Format Result: {}|{}|{}|{}|{}|{}",v1,v2,v3,v4,v5,v6);
    }
}