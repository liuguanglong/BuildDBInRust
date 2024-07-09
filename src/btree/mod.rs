pub mod btree;
pub mod kv;
pub mod util;
pub mod table;
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

pub const TABLE_PREFIX_MIN: u16 = 4;

#[derive(Debug)]
pub enum BTreeError{
    ColumnNotFound(String),    
    ValueTypeWrong(String),
}

// 实现 fmt::Display 特征
impl fmt::Display for BTreeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BTreeError::ColumnNotFound(err) => write!(f, "Column is not found! :{}", err),
            BTreeError::ValueTypeWrong(err) => write!(f, "Value's type is wrong!: {}", err),
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