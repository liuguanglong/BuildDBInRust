use crate::btree::kv::nodeinterface::BNodeReadInterface;
use crate::btree::kv::nodeinterface::BNodeWriteInterface;
use crate::btree::kv::nodeinterface::BNodeOperationInterface;
use crate::btree::kv::FREE_LIST_HEADER;
use crate::btree::HEADER;
use crate::btree::BNODE_NODE;
use crate::btree::BNODE_LEAF;

use super::nodeinterface::BNodeFreeListInterface;
use super::BNODE_FREE_LIST;
use super::BTREE_PAGE_SIZE;

pub struct BNode {
    data: Box<[u8]>,
    size: usize,
}

impl Clone for BNode {
    fn clone(&self) -> BNode {
        BNode {
            data: self.data.clone(), 
            size:self.size,       
        }
    }
}

impl BNode{
    pub fn new(size:usize) -> Self {
        BNode {
            data: vec![0; size].into_boxed_slice(),
            size:size,
        }
    }
}

impl BNodeOperationInterface for BNode{

    fn findSplitIdx(&self)-> u16{
        let number = self.nkeys();
        let lastPos = self.kvPos(number) as u16;

        let mut find:u16 = number -1;
        let mut pos:u16 = lastPos as u16;

        let mut keyCount: u16 = 1;
        let mut kvSize: u16 = (((crate::btree::BTREE_PAGE_SIZE - HEADER as usize - 10 * keyCount as usize) * 2) / 3 ) as u16; //Todo,just page plit

        while find > 0 {
            kvSize = (((crate::btree::BTREE_PAGE_SIZE - HEADER as usize - 10 * keyCount as usize) * 2) / 3) as u16;
            pos = self.kvPos(find) as u16;

            //std.debug.print("KVSize{d} pos:{d} lastpos:{d}\n", .{ kvSize, pos, lastPos });
            if  lastPos - pos < kvSize {
                keyCount = keyCount + 1;
                find = find - 1;
            } else {
                break;
            }
        }

        return find + 1;
    } 

    // split a bigger-than-allowed node into two.
    // the second node always fits on a page.
    fn nodeSplit2<T:BNodeWriteInterface>(&self,left: &mut T, right: &mut T){
        let idx = self.findSplitIdx();
        //std.debug.print("Split Index:{d} Old Node Type {d}", .{ idx, old.btype() });
        left.set_header(self.btype(), idx);
        right.set_header(self.btype(), self.nkeys() - idx);

        left.node_append_range(self, 0, 0, idx);
        right.node_append_range(self, 0, idx, self.nkeys() - idx);
    }

    // split a node if it's too big. the results are 1~3 nodes.
    fn nodeSplit3(&self) -> (u16,Option<BNode>,Option<BNode>,Option<BNode>){

        if self.nbytes() <= crate::btree::BTREE_PAGE_SIZE {
            let mut nodeA = BNode::new(crate::btree::BTREE_PAGE_SIZE);
            for i in 0..crate::btree::BTREE_PAGE_SIZE
            {
                nodeA.data[i] = self.data[i];
            }
            return (1,Some(nodeA),None,None);
        }

        let mut left = BNode::new(crate::btree::BTREE_PAGE_SIZE * 2);
        let mut right = BNode::new(crate::btree::BTREE_PAGE_SIZE);

        self.nodeSplit2(&mut left,&mut right);
        if (left.nbytes() <= crate::btree::BTREE_PAGE_SIZE) {
            return (2,Some(left),Some(right),None);
        }

        let mut leftleft = BNode::new(crate::btree::BTREE_PAGE_SIZE);
        let mut middle = BNode::new(crate::btree::BTREE_PAGE_SIZE);
        left.nodeSplit2(&mut leftleft,&mut middle);
        return (3,Some(leftleft),Some(middle),Some(right));
    }

    fn nodeMerge<T:BNodeReadInterface>(&mut self, left: &T, right: &T)
    {
        self.set_header(left.btype(), left.nkeys() + right.nkeys());
        self.node_append_range(left, 0, 0, left.nkeys());
        self.node_append_range(right, left.nkeys(), 0, right.nkeys());
    }

    fn nodeReplace2Kid<T:BNodeReadInterface>(&mut self, oldNode: &T, idx: u16, ptrMergedNode: u64, key: &[u8]){
        self.set_header(BNODE_NODE, oldNode.nkeys() - 1);
        //oldNode.print();

        self.node_append_range(oldNode, 0, 0, idx);
        self.node_append_kv(idx, ptrMergedNode, key, &[0;1]);
        self.node_append_range(oldNode, idx + 1, idx + 2, oldNode.nkeys() - idx - 2);
        //std.debug.print("Node after nodereplace2kid.\n", .{});
        //newNode.print();
    }

    fn nodeReplaceKidN<T:BNodeReadInterface>(&mut self, oldNode: &T, idx: u16,kvs:Vec<(u64,Vec<u8>)>) {

        self.set_header(crate::btree::BNODE_NODE, oldNode.nkeys() + kvs.len() as u16 - 1);
        self.node_append_range(oldNode, 0, 0, idx);

        for i in 0..kvs.len()
        {
            self.node_append_kv(idx + i as u16, kvs[i].0, &kvs[i].1, &[0;1]);
        }

        self.node_append_range(oldNode, idx + kvs.len() as u16, idx + 1, oldNode.nkeys() - (idx + 1));
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

    fn copy_Data(&mut self, data:&Vec<u8>,offset:usize,length:usize){
        self.data[0..BTREE_PAGE_SIZE].copy_from_slice(&data[offset..offset+length]);
    }

    fn copy_Content(&mut self,ptr :*mut u8,offset:usize,length:usize){
        unsafe {
            for i in 0..BTREE_PAGE_SIZE {
                self.data[i] = *ptr.add(offset + i);
            }

            //std::ptr::copy_nonoverlapping(ptr, self.data.as_mut_ptr(), length);
        }
    }

    fn set_header(& mut self, nodetype: u16, keynumber: u16) {

        let bytes_nodetype: [u8; 2] = nodetype.to_le_bytes();
        let bytes_nkeys: [u8; 2] = keynumber.to_le_bytes();

        self.data[0..2].copy_from_slice(&bytes_nodetype);
        self.data[2..4].copy_from_slice(&bytes_nkeys);
    }
    //Pointers
    fn set_ptr(&mut self, idx: usize, value: u64) {
        assert!(idx < self.nkeys().into(), "Assertion failed: idx is large or equal nkeys!");
        let bytes_le: [u8; 8] = value.to_le_bytes();
        let pos:usize = (HEADER + 8 * (idx as u16)) as usize;

        self.data[pos..pos + 8].copy_from_slice(&bytes_le);
    }

    fn set_offSet(&mut self,idx:u16,offset:u16){
        let pos = self.offset_pos(idx);

        let bytes_le: [u8; 2] = offset.to_le_bytes();
        self.data[pos..pos + 2].copy_from_slice(&bytes_le);
    }

    fn node_append_kv(&mut self, idx: u16, ptr: u64, key: &[u8], val: &[u8])
    {
        self.set_ptr(idx as usize, ptr);
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

        let offset = self.get_offSet(idx) + 4 + klen + vlen;
        self.set_offSet(idx+1,offset);
    }   
    
    fn node_append_range<T:BNodeReadInterface>(&mut self, old: &T, dst_new: u16, src_old: u16, number: u16){
        assert!(src_old + number <= old.nkeys());
        assert!(dst_new + number <= self.nkeys());

        if number == 0 {
            return;
        }

        //Copy Pointers
        for i in 0..number {
            self.set_ptr((dst_new + i) as usize, old.get_ptr((src_old + i) as usize));
        }
        //Copy Offsets
        let dstBegin = self.get_offSet(dst_new);
        let srcBegin = old.get_offSet(src_old);

        for i in 1..number+1 //Range [1..n]
        {
            let offset = old.get_offSet(src_old + i) - srcBegin + dstBegin;
            self.set_offSet(dst_new + i, offset);
        }

        //Copy kvs
        let begin = old.kvPos(src_old);
        let end = old.kvPos(src_old + number);
        //println!("Begin:{:?} End:{:?}",begin,end);
        let len: u16 = (end - begin) as u16;
        for i in 0..len {
            let idx = i as usize;
            let newBegin = self.kvPos(dst_new);
            self.data[ newBegin + idx] = old.data()[begin+idx];
        }
    }

    fn leaf_insert<T:BNodeReadInterface>(&mut self, old:&T, idx: u16, key: &[u8], val: &[u8]){
        self.set_header(crate::btree::BNODE_LEAF, old.nkeys() + 1);
        self.node_append_range(old, 0, 0, idx);
        self.node_append_kv(idx, 0, key, val);
        self.node_append_range(old, idx + 1, idx, old.nkeys() - idx);
    }

    fn leaf_update<T:BNodeReadInterface>(&mut self, old:&T, idx: u16, key: &[u8], val: &[u8]){
        self.set_header(crate::btree::BNODE_LEAF, old.nkeys());
        self.node_append_range(old, 0, 0, idx);
        self.node_append_kv(idx, 0, key, val);
        self.node_append_range(old, idx + 1, idx + 1, old.nkeys() - idx - 1);
    }

     // remove a key from a leaf node
     fn leaf_delete<T:BNodeReadInterface>(&mut self, old:&T, idx: u16) {
        self.set_header(BNODE_LEAF, old.nkeys() - 1);
        self.node_append_range(old, 0, 0, idx);
        self.node_append_range(old, idx, idx + 1, old.nkeys() - (idx + 1));
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

    fn nkeys(&self) -> u16 {
        return u16::from_le_bytes(self.data[2..4].try_into().unwrap());
    }
    fn get_ptr(&self, idx: usize) -> u64 {
        assert!(idx < self.nkeys().into(), "Assertion failed: idx is large or equal nkeys!");
        let pos:usize = (HEADER + 8 * (idx as u16)) as usize;
        let value: u64 = u64::from_le_bytes(self.data[pos..pos + 8].try_into().unwrap());

        return value;
    }

    fn offset_pos(&self, idx: u16)->usize{
        assert!(1 <= idx && idx <= self.nkeys());
        let r =  8 * self.nkeys() + 2 * (idx - 1);
        let value_usize: usize = (HEADER +  r) as usize;
        return value_usize;
    }

    fn get_offSet(&self,idx:u16) -> u16{
        if idx == 0
        {
            return 0;
        }

        let pos = self.offset_pos(idx);
        return u16::from_le_bytes(self.data[pos..pos+2].try_into().unwrap());
    }
    fn kvPos(&self, idx: u16)-> usize{
        assert!(idx <= self.nkeys());
        let r =  8 * self.nkeys() + 2 * self.nkeys() + self.get_offSet(idx);
        let value_usize: usize = (HEADER +  r) as usize;
        return value_usize;
    }

    fn get_val(&self, idx: u16)-> &[u8]{
        assert!(idx <= self.nkeys());
        let pos = self.kvPos(idx);
        let klen = u16::from_le_bytes(self.data[pos..pos+2].try_into().unwrap()) as usize;
        let vlen = u16::from_le_bytes(self.data[pos+2..pos+4].try_into().unwrap()) as usize;
        return &self.data[pos+4+klen..pos+4+klen+vlen];
    }

    fn get_key(&self, idx: u16)-> &[u8]{
        assert!(idx <= self.nkeys());
        let pos = self.kvPos(idx);
        let klen = u16::from_le_bytes(self.data[pos..pos+2].try_into().unwrap()) as usize;
        return &self.data[pos+4..pos+4+klen];
    }

    fn nodeLookupLE(&self, key: &[u8])-> u16{
        let count = self.nkeys();
        let mut found:u16 = 0;
        for i in 0..count{
            let k = self.get_key(i);
            let comp = crate::btree::util::compare_arrays(k,key);
            if comp <= 0 {found = i;}
            if comp > 0 { break; } 
        }
        return found;
    }

        //node size in bytes
        fn nbytes(&self)-> usize {
            return self.kvPos(self.nkeys());
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

impl BNodeFreeListInterface for BNode
{
    fn flnSetHeader(&mut self, keynumber: u16, next: u64) {

        let bytes_nodetype: [u8; 2] = BNODE_FREE_LIST.to_le_bytes();
        let bytes_nkeys: [u8; 2] = keynumber.to_le_bytes();

        self.data[0..2].copy_from_slice(&bytes_nodetype);
        self.data[2..4].copy_from_slice(&bytes_nkeys);

        let pos: usize = (HEADER + 8) as usize;
        self.data[pos..pos+8].copy_from_slice(&next.to_le_bytes());
    }

    fn flnSetNext(&mut self, next: u64) {
        let pos: usize = (HEADER + 8) as usize;
        self.data[pos..pos+8].copy_from_slice(&next.to_le_bytes());
    }

    fn flnSize(&self)->u16 {
        return u16::from_le_bytes(self.data[2..4].try_into().unwrap());
    }

    fn flnNext(&self)->u64 {
        let pos: usize = HEADER as usize + 8;
        return u64::from_le_bytes(self.data[pos..pos+8].try_into().unwrap());
    }

    fn flnPtr(&self, idx: usize)->u64 {
        let pos:usize = FREE_LIST_HEADER + 8 * idx;    
        return u64::from_le_bytes(self.data[pos..pos+8].try_into().unwrap());
    }

    fn flnSetPtr(&mut self, idx: usize, value: u64) {
        let pos: usize = FREE_LIST_HEADER + 8 * idx;
        self.data[pos..pos+8].copy_from_slice(&value.to_le_bytes());
    }

    fn flnSetPtrWithVersion(&mut self, idx: usize, value: u64, version: u64)
    {
        let pos: usize = FREE_LIST_HEADER + 16 * idx;
        self.data[pos..pos+8].copy_from_slice(&value.to_le_bytes());
        self.data[pos+8..pos+16].copy_from_slice(&version.to_le_bytes());

    }
    fn flnPtrWithVersion(&self, idx: usize)->(u64,u64){
        let pos:usize = FREE_LIST_HEADER + 16 * idx;    
        return (u64::from_le_bytes(self.data[pos..pos+8].try_into().unwrap()),
            u64::from_le_bytes(self.data[pos+8..pos+16].try_into().unwrap())
        );
    }

    fn flnSetTotal(&mut self, value: u64) {
        self.data[4..4+8].copy_from_slice(&value.to_le_bytes());
    }

    fn flnGetTotal(&self)->u64 {
        return u64::from_le_bytes(self.data[4..4+8].try_into().unwrap());
    }
}


#[cfg(test)]
mod tests {
    use crate::btree::BTREE_PAGE_SIZE;

    use super::*;

    #[test]
    fn test_setPtr()
    {
        let mut nodeA = BNode::new(1024);
        const ptr: u64 = 23;
        nodeA.set_header(BNODE_NODE, 20);
        nodeA.set_ptr(19, ptr);
        assert_eq!(ptr, nodeA.get_ptr(19));
        
        let t = nodeA.btype();

        //test nbyte()
        assert_eq!(t,BNODE_NODE);

        let offset: u16 = 0x1234;
        nodeA.set_offSet(1, offset);
        nodeA.set_offSet(2, offset);

        assert_eq!(offset,nodeA.get_offSet(1));
    }

    #[test]
    fn test_nodeAppendKV()
    {
        let mut root = BNode::new(1024);
        const ptr: u64 = 23;
        root.set_header(BNODE_NODE, 3);
        root.node_append_kv(0, 0, "".as_bytes(), "".as_bytes());
        root.node_append_kv(1, 0, "1111".as_bytes(), "5555555".as_bytes());
        root.node_append_kv(2, 0, "2222".as_bytes(), "eeeeeee".as_bytes());

        //root.print();

        //println!("Key:{:?} Val:{:?} OffSet:{:?} KVPos:{:?}",root.get_key(0),root.get_val(0),root.get_offSet(0),root.kvPos(0));
        //println!("Key:{:?} Val:{:?} OffSet:{:?} KVPos:{:?}",root.get_key(1),root.get_val(1),root.get_offSet(1),root.kvPos(1));
        //println!("Key:{:?} Val:{:?} OffSet:{:?} KVPos:{:?}",root.get_key(2),root.get_val(2),root.get_offSet(2),root.kvPos(2));

    }

    #[test]
    fn test_leafinsert()
    {
        std::env::set_var("RUST_BACKTRACE", "1");
        let mut root = BNode::new(1024);
        root.set_header(BNODE_NODE, 3);
        root.node_append_kv(0, 0, "".as_bytes(), "".as_bytes());
        root.node_append_kv(1, 0, "1111".as_bytes(), "1111111".as_bytes());
        root.node_append_kv(2, 0, "3333".as_bytes(), "3333333".as_bytes());

        let mut node = BNode::new(1024);
        node.set_header(BNODE_NODE, 4);
        node.leaf_insert(&root,1,"2222".as_bytes(), "2222222".as_bytes());
        //node.print();

        //println!("Key:{:?} Val:{:?} OffSet:{:?} KVPos:{:?}",node.get_key(1),node.get_val(1),node.get_offSet(1),node.kvPos(1));
        //println!("Key:{:?} Val:{:?} OffSet:{:?} KVPos:{:?}",node.get_key(2),node.get_val(2),node.get_offSet(2),node.kvPos(2));

    }

    #[test]
    fn test_leafupdate()
    {
        std::env::set_var("RUST_BACKTRACE", "1");
        let mut root = BNode::new(1024);
        root.set_header(BNODE_NODE, 2);
        root.node_append_kv(0, 0, "".as_bytes(), "".as_bytes());
        root.node_append_kv(1, 0, "1111".as_bytes(), "1111111".as_bytes());

        let mut node = BNode::new(1024);
        node.set_header(BNODE_NODE, 2);
        node.leaf_update(&root,1,"1111".as_bytes(), "33333333".as_bytes());
        //node.print();

        //println!("Key:{:?} Val:{:?} OffSet:{:?} KVPos:{:?}",node.get_key(1),node.get_val(1),node.get_offSet(1),node.kvPos(1));

    }

    #[test]
    fn test_nodesplit2()
    {
        std::env::set_var("RUST_BACKTRACE", "1");
        let mut root = BNode::new(4096);
        root.set_header(BNODE_NODE, 2);
        root.node_append_kv(0, 0, "".as_bytes(), "".as_bytes());
        root.node_append_kv(1, 0, "1".as_bytes(), &[49; 2000]);

        let mut node = BNode::new(4096 * 2);
        node.leaf_insert(&root,2,"2".as_bytes(), &[50; 2000]);
        //node.print();

        let idx = node.findSplitIdx();

        let mut left = BNode::new(4096);
        let mut right = BNode::new(4096);
        node.nodeSplit2(&mut left,&mut right);

        //left.print();
        //right.print();

    }

    #[test]
    fn test_nodesplit3()
    {
        std::env::set_var("RUST_BACKTRACE", "1");
        let mut root = BNode::new(4096);
        root.set_header(BNODE_NODE, 2);
        root.node_append_kv(0, 0, "".as_bytes(), "".as_bytes());
        root.node_append_kv(1, 0, "1".as_bytes(), &[49; 2500]);

        let mut node = BNode::new(4096 * 2);
        node.leaf_insert(&root,2,"2".as_bytes(), &[50; 2500]);
        //node.print();

        let mut node1 = BNode::new(4096 * 2);
        node1.leaf_insert(&node,2,"3".as_bytes(), &[51; 2500]);
       //node1.print();

        let (count,n1,n2,n3) = node1.nodeSplit3();

        // match n1{
        //     Some(n) => n.print(),
        //     None => {}
        // }

        // match n2{
        //     Some(n) => n.print(),
        //     None => {}
        // }
        // match n3{
        //     Some(n) => n.print(),
        //     None => {}
        // }
    }

    #[test]
    fn test_nodefln()
    {
        let mut n1 = BNode::new(BTREE_PAGE_SIZE);
        n1.flnSetHeader(0, 0);
        n1.flnSetTotal(0);

        let t = n1.flnSize();
        assert_eq!(0,t);
    }
}