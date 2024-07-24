pub mod btree;
pub mod kv;
pub mod util;
pub mod table;
pub mod scan;
pub mod db;
pub mod tx;
pub mod parser;

use std::fmt;

pub const BTREE_PAGE_SIZE:usize = 4096;
pub const BTREE_MAX_KEY_SIZE :usize= 1000;
pub const BTREE_MAX_VALUE_SIZE:usize = 3000;

pub const BNODE_NODE: u16 = 1;
pub const BNODE_LEAF: u16 = 2;
pub const BNODE_FREE_LIST: u16 = 3;
pub const HEADER:u16 = 4;

pub const MODE_UPSERT: u16 = 0; // insert or replac
pub const MODE_UPDATE_ONLY: u16 = 1; // update existing keys
pub const MODE_INSERT_ONLY: u16 = 2; // only add new keys

pub const TABLE_PREFIX_MIN: u32 = 4;

#[derive(Debug)]
pub enum BTreeError{
    ColumnNotFound(String),    
    ValueTypeWrong(String),
    PrevNotFound, 
    NextNotFound,
    PrimaryKeyIsNotSet,
    TableAlreadyExist,
    ColumnValueMissing,
    TableNotFind,
    BadArrange,
    KeyError,
    IndexesValueMissing,
    NoIndexFound,
    RecordNotFound,
    IndexNotFoundError

}

// 实现 fmt::Display 特征
impl fmt::Display for BTreeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BTreeError::ColumnNotFound(err) => write!(f, "Column is not found! :{}", err),
            BTreeError::ValueTypeWrong(err) => write!(f, "Value's type is wrong!: {}", err),
            BTreeError::PrevNotFound  => write!(f, "Prev value notFound"), 
            BTreeError::NextNotFound  => write!(f, "Next value notFound"),
            BTreeError::PrimaryKeyIsNotSet  => write!(f, "Primary key's value is null!"), 
            BTreeError::TableAlreadyExist  => write!(f, "Table already exists!"),
            BTreeError::ColumnValueMissing  => write!(f, "Column's Value is null!"),
            BTreeError::TableNotFind  => write!(f, "Table is not found!"),
            BTreeError::BadArrange  => write!(f, "The seek range is wrong!"),
            BTreeError::KeyError  => write!(f, "The seek key is not given!"),
            BTreeError::IndexesValueMissing => write!(f,"The value of Index is not found!"),
            BTreeError::NoIndexFound => write!(f,"No index is found!"),
            BTreeError::RecordNotFound => write!(f,"Record is found!"),
            BTreeError::IndexNotFoundError => write!(f,"Index is not found!"),
        }
    }
}

// // 实现 std::error::Error 特征
// impl Error for BTreeError {
//     fn source(&self) -> Option<&(dyn Error + 'static)> {
//         match self {
//             MyError::IoError(err) => Some(err),
//             MyError::ParseError(err) => Some(err),
//             MyError::CustomError(_) => None,
//         }
//     }
// }

// // 为 std::io::Error 和 std::num::ParseIntError 实现 From 特征
// impl From<std::io::Error> for BTreeError {
//     fn from(err: std::io::Error) -> MyError {
//         MyError::IoError(err)
//     }
// }

// impl From<std::num::ParseIntError> for BTreeError {
//     fn from(err: std::num::ParseIntError) -> MyError {
//         MyError::ParseError(err)
//     }
// }