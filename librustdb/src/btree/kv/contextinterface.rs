use crate::btree::kv::node::BNode;
use crate::btree::kv::nodeinterface::BNodeReadInterface;

use super::ContextError;

pub trait KVContextInterface {
    fn open(&mut self)->Result<(),ContextError>;
    fn close(&mut self);
    fn get_root(&self)->u64;
    fn set_root(&mut self,ptr:u64);
    fn save(&mut self)->Result<(), ContextError>;
    fn add(&mut self,node:BNode) -> u64;
    fn get(&self,key:u64) ->  Option<BNode>;
    fn del(&mut self,key:u64)-> Option<BNode>;
}
