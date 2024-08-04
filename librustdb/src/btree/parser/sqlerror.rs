use std::fmt;


#[derive(Debug)]
pub enum SqlError{
    ColumnNotFoundError,
}

// 实现 fmt::Display 特征
impl fmt::Display for SqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SqlError::ColumnNotFoundError => write!(f,"Column is not found!"),
        }
    }
}