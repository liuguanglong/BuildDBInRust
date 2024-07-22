use std::{collections::HashMap, sync::{Arc, RwLock}};

use crate::btree::{btree::request::{DeleteRequest, InsertReqest}, db::scanner::Scanner, kv::{node::BNode, ContextError}, scan::{biter::BIter, comp::OP_CMP}, table::{record::Record, table::TableDef}, BTreeError};
use super::{tx::Tx, txScanner::TxScanner, txbiter::TxBIter, txreader::{self, TxReader}, txwriter::txwriter, winmmap::Mmap};

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
    fn begin(& mut self)->Result<txwriter,ContextError>;
    fn commmit(& mut self, tx:&mut txwriter)->Result<(),ContextError>;
    fn abort(& mut self,tx:&txwriter);
    fn beginread(&mut self)->Result<TxReader,ContextError>;
    fn endread(&mut self, reader:& TxReader);
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

pub trait MmapInterface{
    fn getMmap(&mut self)->Arc<RwLock<Mmap>>;
    fn getContextSize(&self)->usize;
    fn extendContext(&mut self,pageCount:usize)->Result<(),ContextError>;
    fn extendPages(&mut self,totalpages:usize) -> Result<(),ContextError>;
    fn syncContext(&mut self) -> Result<(),ContextError>;
}

