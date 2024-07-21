use std::collections::HashMap;

use crate::btree::{btree::request::{DeleteRequest, InsertReqest}, db::scanner::Scanner, kv::{node::BNode, ContextError}, scan::{biter::BIter, comp::OP_CMP}, table::{record::Record, table::TableDef}, BTreeError};
use super::{txScanner::TxScanner, txbiter::TxBIter};

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

pub trait DBReadInterface{
    fn Scan(&self, cmp1: OP_CMP, cmp2: OP_CMP, key1:&Record, key2:&Record)->Result<TxScanner,BTreeError>;
}

pub trait DBTxInterface{
    fn Scan(&self, cmp1: OP_CMP, cmp2: OP_CMP, key1:&Record, key2:&Record)->Result<TxScanner,BTreeError>;
    fn DeleteRecord(&mut self, rec:&Record)->Result<bool,BTreeError>;
    fn AddTable(&mut self, tdef:&mut TableDef)-> Result<(),BTreeError>;
    fn UpdateRecord(&mut self, rec:&mut Record, mode: u16) -> Result<(),BTreeError>;

}