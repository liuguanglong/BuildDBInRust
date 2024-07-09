pub mod btree;
pub mod kv;
pub mod util;
pub mod table;

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