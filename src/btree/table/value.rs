use std::fmt;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum ValueType {
    BYTES,
    INT64,
    INT32,
    INT16,
    INT8,
    BOOL,
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
        }
    }
}


pub enum Value{
    BYTES(Vec<u8>),
    INT64(i64),
    INT32(i32),
    INT16(i16),
    INT8(i8),
    BOOL(bool),
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