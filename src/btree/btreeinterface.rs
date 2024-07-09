use crate::btree::request::DeleteRequest;
use crate::btree::request::InsertReqest;

pub trait BTreeKVInterface {
    fn Set(&mut self,key: &[u8], val: &[u8], mode: u16);
    fn Get(&self, key:&[u8])  -> Option<Vec<u8>>;
    fn Delete(&mut self, key: &[u8]) -> bool;
}


pub trait BTreeInterface {
    fn SetEx(&mut self,req:&InsertReqest);
    fn DeleteEx(&mut self, req: &DeleteRequest) -> bool;
}
