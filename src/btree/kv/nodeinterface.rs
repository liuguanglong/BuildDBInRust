use super::{node::BNode, ContextError};


// 定义读取接口
pub trait BNodeReadInterface {
    fn size(&self) ->usize;
    fn data(&self) ->&[u8];
    fn nkeys(&self) -> u16;
    fn get_ptr(&self, idx: usize) -> u64 ;
    fn print(&self);
    fn btype(&self)->u16;
    fn offset_pos(&self, idx: u16)->usize;
    fn get_offSet(&self,idx:u16) -> u16;
    fn kvPos(&self, idx: u16)-> usize;
    fn get_key(&self, idx: u16)-> &[u8];
    fn get_val(&self, idx: u16)-> &[u8];
    fn nodeLookupLE(&self, key: &[u8])-> u16;
    fn nbytes(&self)-> usize;
}

pub trait BNodeWriteInterface{
    fn set_ptr(&mut self, idx: usize, value: u64);
    fn set_header(& mut self, nodetype: u16, keynumber: u16);
    fn copy_value(&mut self,s :&str);
    fn copy_Data(&mut self, data:&Vec<u8>,offset:usize,length:usize);
    fn copy_Content(&mut self, s :*mut u8,offset:usize,length:usize);
    fn set_offSet(&mut self,idx:u16,offset:u16);
    fn node_append_kv(&mut self, idx: u16, ptr: u64, key: &[u8], val: &[u8]);
    fn node_append_range<T:BNodeReadInterface>(&mut self, old: &T, dst_new: u16, src_old: u16, number: u16);
    fn leaf_insert<T:BNodeReadInterface>(&mut self, old:&T, idx: u16, key: &[u8], val: &[u8]);
    fn leaf_update<T:BNodeReadInterface>(&mut self, old:&T, idx: u16, key: &[u8], val: &[u8]);
    fn leaf_delete<T:BNodeReadInterface>(&mut self, old:&T, idx: u16);
}

pub trait BNodeOperationInterface{
    fn findSplitIdx(&self)-> u16;
    fn nodeSplit2<T:BNodeWriteInterface>(&self, right: &mut T, left: &mut T);
    fn nodeSplit3(&self) -> (u16,Option<BNode>,Option<BNode>,Option<BNode>);
    fn nodeMerge<T:BNodeReadInterface>(&mut self, left: &T, right: &T);
    fn nodeReplace2Kid<T:BNodeReadInterface>(&mut self, oldNode: &T, idx: u16, ptrMergedNode: u64, key: &[u8]); 
    fn nodeReplaceKidN<T:BNodeReadInterface>(&mut self, oldNode: &T, idx: u16,kvs:Vec<(u64,Vec<u8>)>);
}

pub trait BNodeFreeListInterface{
    fn flnSetHeader(&mut self, keynumber: u16, next: u64);
    fn flnSetNext(&mut self, next: u64);
    fn flnSize(&self)->u16;
    fn flnNext(&self)->u64;
    fn flnPtr(&self, idx: usize)->u64;
    fn flnSetPtr(&mut self, idx: usize, value: u64);
    fn flnSetTotal(&mut self, value: u64);
    fn flnGetTotal(&self)->u64;
    fn flnSetPtrWithVersion(&mut self, idx: usize, value: u64, version: u64);
    fn flnPtrWithVersion(&self, idx: usize)->(u64,u64);
}

pub trait FreeListInterface{
    fn GetFreeNode(&self, topN: u16)-> Result<u64,ContextError>;
    fn TotalFreeNode(&self)-> Result<u64,ContextError>;
    fn UpdateFreeList(&mut self, popn: u16, freed:&Vec<u64>)->Result<(),ContextError>;
    fn flPush(&mut self, listFreeNode: &mut Vec<u64>, listReuse:  &mut Vec<u64>);
}