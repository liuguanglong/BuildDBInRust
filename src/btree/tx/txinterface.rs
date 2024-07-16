use std::collections::HashMap;

use crate::btree::{btree::request::{DeleteRequest, InsertReqest}, kv::{node::BNode, ContextError}, scan::{biter::BIter, comp::OP_CMP}};
use super::txbiter::TxBIter;

pub trait TxReaderInterface {
    fn Get(&self, key:&[u8])  -> Option<Vec<u8>>;
    fn Seek(&self, key:&[u8], cmp:OP_CMP) -> TxBIter;
}

pub trait TxInterface {
    fn Set(&mut self,req:&mut InsertReqest);
    fn Delete(&mut self, req: &mut DeleteRequest) -> bool;
}

pub trait TxReadContext{
    fn get_root(&self)->u64;
    fn get(&self,key:u64) ->  Option<BNode>;
}

pub trait TxWriteContext{
    fn set_root(&mut self,ptr:u64);
    fn add(&mut self,node:BNode) -> u64;
    fn del(&mut self,key:u64)-> Option<BNode>;
    fn getUpdates(&self) -> &HashMap<u64,Option<BNode>>;
}

pub trait TxContent{
    fn open(&mut self)->Result<(),ContextError>;
    fn save(&mut self,updates:&HashMap<u64,Option<BNode>>)->Result<(), ContextError>;
    fn copy(&self)->Vec<u8>;
}

pub trait TxFreeListInterface{
    fn GetFreeNode(&self, topN: u16)-> Result<u64,ContextError>;
    fn TotalFreeNode(&self)-> Result<u64,ContextError>;
    fn UpdateFreeList(&mut self, popn: u16, freed:&Vec<u64>)->Result<(),ContextError>;
    fn flPush(&mut self, listFreeNode: &mut Vec<u64>, listReuse:  &mut Vec<u64>);
}