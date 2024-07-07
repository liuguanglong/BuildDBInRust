use crate::btree::kv::noderef::BNodeRef;
use crate::btree::kv::node::BNode;
use crate::btree::kv::nodeinterface::BNodeReadInterface;

pub trait KVContextInterface {
    fn open(&mut self);
    fn close(&mut self);
    fn get_root(&self)->u64;
    fn set_root(&mut self,ptr:u64);
    fn save(&mut self);
    fn add(&mut self,node:BNode) -> u64;
    fn get(&self,key:&u64) ->  Option<BNodeRef>;
    fn del(&mut self,key:&u64)-> Option<BNode>;
}
