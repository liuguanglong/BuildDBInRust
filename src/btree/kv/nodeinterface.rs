
// 定义读取接口
pub trait BNodeReadInterface {
    fn size(&self) ->usize;
    fn data(&self) ->&[u8];
    fn nkeys(&self) -> u16;
    fn getPtr(&self, idx: usize) -> u64 ;
    fn print(&self);
    fn btype(&self)->u16;
    fn offsetPos(&self, idx: u16)->usize;
    fn getOffSet(&self,idx:u16) -> u16;
    fn kvPos(&self, idx: u16)-> usize;
    fn getKey(&self, idx: u16)-> &[u8];
    fn getVal(&self, idx: u16)-> &[u8];
    fn nodeLookupLE(&self, key: &[u8])-> u16;
}

pub trait BNodeWriteInterface{
    fn setPtr(&mut self, idx: usize, value: u64);
    fn setHeader(& mut self, nodetype: u16, keynumber: u16);
    fn copy_value(&mut self,s :&str);
    fn setOffSet(&mut self,idx:u16,offset:u16);
    fn nodeAppendKV(&mut self, idx: u16, ptr: u64, key: &[u8], val: &[u8]);
    fn nodeAppendRange<T:BNodeReadInterface>(&mut self, old: &T, dstNew: u16, srcOld: u16, number: u16);
    fn leafInsert<T:BNodeReadInterface>(&mut self, old:&T, idx: u16, key: &[u8], val: &[u8]);
}