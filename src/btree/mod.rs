pub mod btree;
pub mod kv;
pub mod util;


pub const BTREE_PAGE_SIZE:usize = 4096;
pub const BTREE_MAX_KEY_SIZE :usize= 1000;
pub const BTREE_MAX_VALUE_SIZE:usize = 3000;

pub const BNODE_NODE: u16 = 1;
pub const BNODE_LEAF: u16 = 2;
pub const BNODE_FREE_LIST: u16 = 3;
pub const HEADER:u16 = 4;