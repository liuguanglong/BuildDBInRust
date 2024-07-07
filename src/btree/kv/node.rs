

use crate::btree::kv::nodeinterface::BNodeReadInterface;
use crate::btree::kv::nodeinterface::BNodeWriteInterface;
use crate::btree::kv::HEADER;
use crate::btree::BNODE_NODE;

pub struct BNode {
    data: Box<[u8]>,
    size: usize,
}

impl BNode{
    pub fn new(size:usize) -> Self {
        BNode {
            data: vec![0; size].into_boxed_slice(),
            size:size
        }
    }


}

impl BNodeWriteInterface for BNode{
    fn copy_value(&mut self,s :&str){
        let content = s.as_bytes();
        for (i, &item) in content.iter().enumerate() 
        {
            self.data[i] = item;
        }
    }

    fn setHeader(& mut self, nodetype: u16, keynumber: u16) {

        let bytes_nodetype: [u8; 2] = nodetype.to_le_bytes();
        let bytes_nkeys: [u8; 2] = keynumber.to_le_bytes();

        self.data[0..2].copy_from_slice(&bytes_nodetype);
        self.data[2..4].copy_from_slice(&bytes_nkeys);
    }
    //Pointers
    fn setPtr(&mut self, idx: usize, value: u64) {
        assert!(idx < self.nkeys().into(), "Assertion failed: idx is large or equal nkeys!");
        let bytes_le: [u8; 8] = value.to_le_bytes();
        let pos:usize = HEADER + 8 * idx;

        self.data[pos..pos + 8].copy_from_slice(&bytes_le);
    }

    fn setOffSet(&mut self,idx:u16,offset:u16){
        let pos = self.offsetPos(idx);

        let bytes_le: [u8; 2] = offset.to_le_bytes();
        self.data[pos..pos + 2].copy_from_slice(&bytes_le);
    }

    fn nodeAppendKV(&mut self, idx: u16, ptr: u64, key: &[u8], val: &[u8])
    {
        self.setPtr(idx as usize, ptr);
        let pos = self.kvPos(idx);

        let klen = key.len() as u16;
        let bytes_keylen: [u8; 2] =klen.to_le_bytes();
        self.data[pos..pos + 2].copy_from_slice(&bytes_keylen);

        let vlen = val.len() as u16;
        let bytes_vlen: [u8; 2] =vlen.to_le_bytes();
        self.data[pos+2..pos + 4].copy_from_slice(&bytes_vlen);

        for i in 0..klen {
            let idx = i as usize;
            self.data[pos+4+idx] = key[idx];
        }

        for i in 0..vlen {
            let idx1 = i as usize;
            self.data[pos+4+key.len()+idx1] = val[idx1];
        }

        let offset = self.getOffSet(idx) + 4 + klen + vlen;
        self.setOffSet(idx+1,offset);
    }   
    
    fn nodeAppendRange<T:BNodeReadInterface>(&mut self, old: &T, dstNew: u16, srcOld: u16, number: u16){
        assert!(srcOld + number <= old.nkeys());
        assert!(dstNew + number <= self.nkeys());

        if number == 0 {
            return;
        }

        //Copy Pointers
        for i in 0..number {
            self.setPtr((dstNew + i) as usize, old.getPtr((srcOld + i) as usize));
        }

        println!("SrcOld:{:?} number:{:?}",srcOld,number);
        //Copy Offsets
        let dstBegin = self.getOffSet(dstNew);
        let srcBegin = old.getOffSet(srcOld);

        for i in 1..number+1 //Range [1..n]
        {
            let offset = old.getOffSet(srcOld + i) - srcBegin + dstBegin;
            self.setOffSet(dstNew + i, offset);
        }

        //Copy kvs
        let begin = old.kvPos(srcOld);
        let end = old.kvPos(srcOld + number);
        println!("Begin:{:?} End:{:?}",begin,end);
        let len: u16 = (end - begin) as u16;
        for i in 0..len {
            let idx = i as usize;
            let newBegin = self.kvPos(dstNew);
            self.data[ newBegin + idx] = old.data()[begin+idx];
        }
    }

    fn leafInsert<T:BNodeReadInterface>(&mut self, old:&T, idx: u16, key: &[u8], val: &[u8]){
        self.setHeader(crate::btree::BNODE_LEAF, old.nkeys() + 1);
        self.nodeAppendRange(old, 0, 0, idx);
        self.nodeAppendKV(idx, 0, key, val);
        self.nodeAppendRange(old, idx + 1, idx, old.nkeys() - idx);
    }
}

impl BNodeReadInterface for BNode {

    fn size(&self) ->usize {
        self.size
    }

    fn data(&self) ->&[u8]
    {
        return &self.data;
    }

    fn btype(&self)->u16{
        return u16::from_le_bytes(self.data[0..2].try_into().unwrap());
    }

    // fn set_value(&mut self, index: usize, value: u8) {
    //     if index < self.data.len() {
    //         self.data[index] = value;
    //     } else {
    //         println!("Index out of bounds");
    //     }
    // }

    // fn get_value(&self, index: usize) -> Option<u8> {
    //     if index < self.data.len() {
    //         Some(self.data[index])
    //     } else {
    //         None
    //     }
    // }

    fn nkeys(&self) -> u16 {
        return u16::from_le_bytes(self.data[2..4].try_into().unwrap());
    }
    fn getPtr(&self, idx: usize) -> u64 {
        assert!(idx < self.nkeys().into(), "Assertion failed: idx is large or equal nkeys!");
        let pos:usize = HEADER + 8 * idx;
        let value: u64 = u64::from_le_bytes(self.data[pos..pos + 8].try_into().unwrap());

        return value;
    }

    fn offsetPos(&self, idx: u16)->usize{
        assert!(1 <= idx && idx <= self.nkeys());
        let r =  8 * self.nkeys() + 2 * (idx - 1);
        let value_usize: usize = HEADER +  r as usize;
        return value_usize;
    }

    fn getOffSet(&self,idx:u16) -> u16{
        if idx == 0
        {
            return 0;
        }

        let pos = self.offsetPos(idx);
        return u16::from_le_bytes(self.data[pos..pos+2].try_into().unwrap());
    }
    fn kvPos(&self, idx: u16)-> usize{
        assert!(idx <= self.nkeys());
        let r =  8 * self.nkeys() + 2 * self.nkeys() + self.getOffSet(idx);
        let value_usize: usize = HEADER +  r as usize;
        return value_usize;
    }

    fn getVal(&self, idx: u16)-> &[u8]{
        assert!(idx <= self.nkeys());
        let pos = self.kvPos(idx);
        let klen = u16::from_le_bytes(self.data[pos..pos+2].try_into().unwrap()) as usize;
        let vlen = u16::from_le_bytes(self.data[pos+2..pos+4].try_into().unwrap()) as usize;
        return &self.data[pos+4+klen..pos+4+klen+vlen];
    }

    fn getKey(&self, idx: u16)-> &[u8]{
        assert!(idx <= self.nkeys());
        let pos = self.kvPos(idx);
        let klen = u16::from_le_bytes(self.data[pos..pos+2].try_into().unwrap()) as usize;
        return &self.data[pos+4..pos+4+klen];
    }

    fn nodeLookupLE(&self, key: &[u8])-> u16{
        let count = self.nkeys();
        let mut found:u16 = 0;
        for i in 0..count{
            let k = self.getKey(i);
            let comp = crate::btree::util::compare_arrays(k,key);
            if comp <= 0 {found = i;}
            if comp > 0 { break; } 
        }
        return found;
    }


    fn print(&self) {
        for i in 0..self.size {
            if  i > 0 {
                print!("{:02x} ", self.data[i]);
            }
            if i % 50 == 0
            {
                println!();
            }
        }
        println!();
        // println!("{:?}", self.data);
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_setPtr()
    {
        let mut nodeA = BNode::new(1024);
        const ptr: u64 = 23;
        nodeA.setHeader(BNODE_NODE, 20);
        nodeA.setPtr(19, ptr);
        assert_eq!(ptr, nodeA.getPtr(19));
        
        let t = nodeA.btype();

        //test nbyte()
        assert_eq!(t,BNODE_NODE);

        let offset: u16 = 0x1234;
        nodeA.setOffSet(1, offset);
        nodeA.setOffSet(2, offset);

        assert_eq!(offset,nodeA.getOffSet(1));
    }

    #[test]
    fn test_nodeAppendKV()
    {
        let mut root = BNode::new(1024);
        const ptr: u64 = 23;
        root.setHeader(BNODE_NODE, 3);
        root.nodeAppendKV(0, 0, "".as_bytes(), "".as_bytes());
        root.nodeAppendKV(1, 0, "1111".as_bytes(), "5555555".as_bytes());
        root.nodeAppendKV(2, 0, "2222".as_bytes(), "eeeeeee".as_bytes());

        //root.print();

        println!("Key:{:?} Val:{:?} OffSet:{:?} KVPos:{:?}",root.getKey(0),root.getVal(0),root.getOffSet(0),root.kvPos(0));
        println!("Key:{:?} Val:{:?} OffSet:{:?} KVPos:{:?}",root.getKey(1),root.getVal(1),root.getOffSet(1),root.kvPos(1));
        println!("Key:{:?} Val:{:?} OffSet:{:?} KVPos:{:?}",root.getKey(2),root.getVal(2),root.getOffSet(2),root.kvPos(2));

    }

    #[test]
    fn test_leafinsert()
    {
        std::env::set_var("RUST_BACKTRACE", "1");
        let mut root = BNode::new(1024);
        root.setHeader(BNODE_NODE, 3);
        root.nodeAppendKV(0, 0, "".as_bytes(), "".as_bytes());
        root.nodeAppendKV(1, 0, "1111".as_bytes(), "1111111".as_bytes());
        root.nodeAppendKV(2, 0, "3333".as_bytes(), "3333333".as_bytes());

        let mut node = BNode::new(1024);
        node.setHeader(BNODE_NODE, 4);
        node.leafInsert(&root,1,"2222".as_bytes(), "2222222".as_bytes());
        node.print();

        println!("Key:{:?} Val:{:?} OffSet:{:?} KVPos:{:?}",node.getKey(1),node.getVal(1),node.getOffSet(1),node.kvPos(1));
        println!("Key:{:?} Val:{:?} OffSet:{:?} KVPos:{:?}",node.getKey(2),node.getVal(2),node.getOffSet(2),node.kvPos(2));

    }
}