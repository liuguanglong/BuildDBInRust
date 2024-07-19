use std::{collections::HashMap, sync::{Arc, RwLock}};
use crate::btree::{kv::{node::{self, BNode}, nodeinterface::{BNodeFreeListInterface, BNodeReadInterface, BNodeWriteInterface}, ContextError, FREE_LIST_CAP_WITH_VERSION}, BTREE_PAGE_SIZE};
use super::{txinterface::TxReadContext, txreader::TxReader, winmmap::Mmap};

pub struct FreeListData{
    head: u64, //head of freeelist
    // cached pointers to list nodes for accessing both ends.
    nodes:Vec<u64>,   //from the tail to the head.  tail is the oldese node|head is the newest node
    // cached total number of items; stored in the head node.
    total:usize,
    // cached number of discarded items in the tail node.
    offset:usize,
}
impl FreeListData {
    pub fn new(head:u64,nodes:&Vec<u64>,total:usize)->Self
    {
        let mut n:Vec<u64> = Vec::with_capacity(nodes.len());
        for i in nodes
        {
            n.push(i.clone());
        }

        FreeListData{
            head:head,
            nodes:n,
            total:total,
            offset:0,
        }
    }
}

pub struct TxFreeList{
    data: FreeListData,
    // newly allocated or deallocated pages keyed by the pointer.
    // nil value denotes a deallocated page.
    updates: HashMap<u64, Option<BNode>>,

    // for each transaction
    freed: Vec<u64>, // pages that will be added to the free list
    version: u64,    // current version
    minReader: u64,  // minimum reader version
}

impl TxFreeList{
    pub fn new(head:u64,version:u64,minReader:u64,nodes:&Vec<u64>,total:usize)->Self
    {
        TxFreeList{
            data: FreeListData::new(head,nodes,total),
            version:version,
            minReader:minReader,
            updates: HashMap::new(),
            freed: Vec::new(),
        }
    }
}

pub struct Tx{
    freelist:TxFreeList,
    reader:TxReader,
    pageflushed: u64, // database size in number of pages
    nappend: u16, //number of pages to be appended
}

impl Tx{
    pub fn new(data:Arc<RwLock<Mmap>>,pageflushed:u64,len:usize,nodes:&Vec<u64>,head:u64,total:usize,offset:usize,version:u64,minReader:u64)->Self
    {
        let mut reader = TxReader::new(data, len);
        Tx{
            reader: reader,
            freelist: TxFreeList::new(head,version,minReader,nodes,total),
            pageflushed:pageflushed,
            nappend:0,
        }
    }

    // try to remove an item from the tail. returns 0 on failure.
    // the removed pointer must not be reachable by the minimum version reader.
    pub fn GetFreeNode(&mut self)->u64 {
        self.loadCache();
        return self.PopFreeNode()
    }

    fn PopFreeNode(&mut self)->u64{
        
        if self.freelist.data.total == 0 || self.freelist.data.head == 0
        {
            return 0;
        }

        let mut node = self.get(self.freelist.data.nodes[0]).unwrap();
        if  self.freelist.data.offset == node.flnSize() as usize
        {
            let ptrNode = self.freelist.data.nodes.remove(0);
            self.freelist.data.total -= 1;

            self.freelist.data.offset = 0;
            if self.freelist.data.nodes.len() != 0 
            {
                self.freelist.data.head = self.freelist.data.nodes[0];
            }
            else {
                self.freelist.data.head = 0;
            }
            return ptrNode;
        }

        // remove one item from the tail
        assert!(self.freelist.data.offset < node.flnSize() as usize);
        let (ptr,ver) = node.flnPtrWithVersion(self.freelist.data.offset);
        if Self::versionbefore(ver,self.freelist.minReader)
        {
            // cannot use; possibly reachable by the minimum version reader.
            return 0;
        }
        self.freelist.data.offset += 1;
        self.freelist.data.total -= 1;

        ptr
    }

    fn updatefreelist(&mut self) -> Result<(),ContextError>{

        if self.freelist.data.offset == 0 && self.freelist.freed.len() == 0
        {
            return Ok(());
        }

        let count = self.freelist.freed.len();
        self.freelist.freed.reverse();
        //update head
        let mut i:usize = 0;
        if let Some(&ptrTail) = self.freelist.data.nodes.last()
        {
            let mut tail = self.get(ptrTail).unwrap();
            let mut idx = tail.flnSize() as usize;
    
            while i< count && idx < FREE_LIST_CAP_WITH_VERSION 
            {
                let ptr = self.freelist.freed.pop().unwrap();
                tail.flnSetPtrWithVersion( idx, ptr,self.freelist.version);
                i += 1;
                idx += 1;
            }
            tail.flnSetHeader(idx as u16, 0);
            self.useNode(ptrTail,&tail);
        }

        while i < count
        {
            let mut ptr = self.PopFreeNode();
            let mut newNode = BNode::new(BTREE_PAGE_SIZE);
            //construc new node
            let mut size: usize = self.freelist.freed.len();
            if size > FREE_LIST_CAP_WITH_VERSION
            {
                size = FREE_LIST_CAP_WITH_VERSION;
            }
            i += size;

            newNode.flnSetHeader(size as u16, 0);
            for idx in 0..size 
            {
                let ptr = self.freelist.freed.pop().unwrap();
                newNode.flnSetPtrWithVersion( idx, ptr,self.freelist.version);
            }


            if ptr != 0
            {
                self.useNode(ptr, &newNode);
            }
            else {
                ptr = self.appendNode(&newNode);
                self.freelist.data.total += 1;
            }

            if let Some(&ptrTail) = self.freelist.data.nodes.last()
            {
                let mut tail = self.get(ptrTail).unwrap();
                tail.flnSetNext(ptr);
                self.useNode(ptrTail,&tail);
    
                self.freelist.data.nodes.push(ptr);
            }
            else {
                newNode.flnSetTotal(size as u64 + 1);
                self.useNode(ptr, &newNode);
                self.freelist.data.nodes.push(ptr);
                self.freelist.data.head = ptr;
            }
        }
               
        //update head
        if self.freelist.data.offset != 0
        {
            let ptrHead = self.freelist.data.nodes[0].clone();
            let mut head = self.get(ptrHead).unwrap();

            let mut newNode = BNode::new(BTREE_PAGE_SIZE);
            let mut idx:usize = 0;

            for i in (self.freelist.data.offset as usize)..head.flnSize() as usize
            {
                let (ptr,ver) = head.flnPtrWithVersion(i);
                newNode.flnSetPtrWithVersion( idx, ptr,self.freelist.version);
                idx += 1;
            }
            newNode.flnSetTotal(self.freelist.data.total as u64 + count as u64);
            newNode.flnSetHeader(head.flnSize() - self.freelist.data.offset as u16,head.flnNext());
            self.useNode(ptrHead, &newNode);
        }
        else 
        {
            let ptrHead = self.freelist.data.nodes[0].clone();
            let mut head = self.get(ptrHead).unwrap();
            
            head.flnSetTotal(self.freelist.data.total as u64 + count as u64);
            self.useNode(ptrHead, &head);
        }

        Ok(())
    } 


    pub fn appendNode(&mut self, bnode: &BNode)-> u64 {
        let newNode = bnode.copy();

        let ptr = self.pageflushed + self.nappend as u64;
        self.nappend += 1;

        self.freelist.updates.insert(ptr, Some(newNode));

        return ptr;
    }

    fn get(&self,key:u64) -> Option<BNode>
    {
        let node = self.freelist.updates.get(&key);
        match &node
        {
            Some(Some(x)) => {
                Some(x.copy())    
            },
            Some(None) =>{
                None
            },
            Other=>
            {
                if let Some(n) = self.reader.get(key)
                {
                    Some(n)
                }
                else {
                    None
                }
            },
        }
    }

    pub fn useNode(&mut self, ptr: u64, bnode: &BNode) {

        let newNode = bnode.copy();
        self.freelist.updates.insert(ptr, Some(newNode));
    }

    fn versionbefore(ver:u64,minReader:u64)->bool
    {
        return ver < minReader;
    }

    pub fn loadCache(&mut self)
    {
        if self.freelist.data.nodes.len() != 0
        {
            return; 
        }

        let node = self.reader.get(self.freelist.data.head).unwrap();
        self.freelist.data.nodes.push(self.freelist.data.head);
        self.freelist.data.total = node.flnGetTotal() as usize;
        self.freelist.data.offset = node.flnSize() as usize;
        let mut next = node.flnNext();
        while next != 0
        {
            self.freelist.data.nodes.push(next);
            next = node.flnNext();
        }        
    }
}

#[cfg(test)]
mod tests {

    use std::{borrow::BorrowMut, hash::Hash, sync::{Arc, RwLock}, time::Duration};
    use rand::Rng;
    use crate::btree::{kv::nodeinterface::BNodeReadInterface, tx::txdemo::Shared, BNODE_NODE};

    use super::*;
    use std::thread;

    use super::*;

    #[test]
    fn test_freelist_updatefreelist()
    {   
        println!("Max Free Node:{}",FREE_LIST_CAP_WITH_VERSION);

        //Free all node to tail
        let mut data: Vec<u8> = vec![0; BTREE_PAGE_SIZE*15];
        let mut tx = preparenormalcase(&mut data,1);

        let mut f:Vec<u64> = Vec::new();
        for i in 0..252
        {
            f.push(i);
        }
        tx.freelist.freed.append(&mut f);
        tx.updatefreelist();

        let ptrTail = tx.freelist.data.nodes.last().unwrap();
        assert_eq!(14,*ptrTail);
        let nodeTail = tx.get(*ptrTail).unwrap();
        assert_eq!(254,nodeTail.flnSize());

        let nodeHead = tx.get(tx.freelist.data.head).unwrap();
        assert_eq!(264,nodeHead.flnGetTotal());

        //Free all node to tail + new tail + remove freenode
        let mut data: Vec<u8> = vec![0; BTREE_PAGE_SIZE*15];
        let mut tx = preparenormalcase(&mut data,1);

        let mut f:Vec<u64> = Vec::new();
        for i in 0..253
        {
            f.push(i);
        }
        tx.freelist.freed.append(&mut f);
        tx.updatefreelist();

        let ptrTail = tx.freelist.data.nodes.last().unwrap();
        assert_eq!(2,*ptrTail);
        let nodeTail = tx.get(*ptrTail).unwrap();
        assert_eq!(1,nodeTail.flnSize());
        let nodeSecondTail = tx.get(14).unwrap();
        assert_eq!(2,nodeSecondTail.flnNext());

        let nodeHead = tx.get(tx.freelist.data.head).unwrap();
        assert_eq!(264,nodeHead.flnGetTotal());
        assert_eq!(1,nodeHead.flnSize());

    }

    #[test]
    fn test_freelist_popnode()
    {   
        let mut data: Vec<u8> = vec![0; BTREE_PAGE_SIZE*15];
        let mut tx = preparenormalcase(&mut data,1);

        //pop one node
        let ptr = tx.PopFreeNode();
        assert_eq!(2,ptr);
        assert_eq!(11,tx.freelist.data.total);
        assert_eq!(1,tx.freelist.data.offset);        

        let ptr = tx.PopFreeNode();
        assert_eq!(3,ptr);
        assert_eq!(10,tx.freelist.data.total);
        assert_eq!(2,tx.freelist.data.offset);

        let ptr = tx.PopFreeNode();
        assert_eq!(11,ptr);
        assert_eq!(9,tx.freelist.data.total);
        assert_eq!(0,tx.freelist.data.offset);
        assert_eq!(12,tx.freelist.data.head);

        let ptr = tx.PopFreeNode();
        assert_eq!(4,ptr);
        assert_eq!(8,tx.freelist.data.total);
        assert_eq!(1,tx.freelist.data.offset);
        assert_eq!(12,tx.freelist.data.head);

        let ptr = tx.PopFreeNode();  //5
        let ptr = tx.PopFreeNode();  //12
        let ptr = tx.PopFreeNode();  //6
        let ptr = tx.PopFreeNode();  //7

        let ptr = tx.PopFreeNode();  //13
        assert_eq!(13,ptr);
        assert_eq!(3,tx.freelist.data.total);
        assert_eq!(0,tx.freelist.data.offset);
        assert_eq!(14,tx.freelist.data.head);

        let ptr = tx.PopFreeNode();  //8
        assert_eq!(8,ptr);
        assert_eq!(2,tx.freelist.data.total);
        assert_eq!(1,tx.freelist.data.offset);
        assert_eq!(14,tx.freelist.data.head);

        let ptr = tx.PopFreeNode();  //9
        assert_eq!(9,ptr);
        assert_eq!(1,tx.freelist.data.total);
        assert_eq!(2,tx.freelist.data.offset);
        assert_eq!(14,tx.freelist.data.head);

        let ptr = tx.PopFreeNode();  //9
        assert_eq!(14,ptr);
        assert_eq!(0,tx.freelist.data.total);
        assert_eq!(0,tx.freelist.data.offset);
        assert_eq!(0,tx.freelist.data.head);
    }

    #[test]
    fn test_freelist_freenodewithnull()
    {   
        //one free node
        let mut data: Vec<u8> = vec![0; BTREE_PAGE_SIZE*11];
        let mut tx = prepaircase_nonefreelist(&mut data);
       
        tx.freelist.freed.push(1);
        tx.updatefreelist();

        assert_eq!(1,tx.freelist.updates.len());
        let n = tx.freelist.updates.get(&11);
        assert!(n.is_some());

        let ptr = tx.freelist.data.head;
        let freenode = tx.get(ptr).unwrap();

        assert_eq!(1,freenode.flnSize());
        assert_eq!(2,freenode.flnGetTotal());
        let (ptr1,v) = freenode.flnPtrWithVersion(0);
        assert!(ptr1 == 1);
        assert!(v == 1);

        //free two node
        let mut data: Vec<u8> = vec![0; BTREE_PAGE_SIZE*11];
        let mut tx = prepaircase_nonefreelist(&mut data);
       
        tx.freelist.freed.push(1);
        tx.freelist.freed.push(2);
        tx.updatefreelist();

        assert_eq!(1,tx.freelist.updates.len());
        let n = tx.freelist.updates.get(&11);
        assert!(n.is_some());

        let ptr = tx.freelist.data.head;
        let freenode = tx.get(ptr).unwrap();

        assert_eq!(2,freenode.flnSize());
        assert_eq!(3,freenode.flnGetTotal());
        let (ptr1,v) = freenode.flnPtrWithVersion(0);
        //freenode.print();
        assert!(ptr1 == 1);
        assert!(v == 1);

        let (ptr1,v) = freenode.flnPtrWithVersion(1);
        assert!(ptr1 == 2);
        assert!(v == 1);

    }
    
    #[test]
    fn test_use_node()
    {   
        let mut data: Vec<u8> = vec![0; BTREE_PAGE_SIZE*11];
        let mut tx = prepaircase_nonefreelist(&mut data);
       
        let mut n: BNode = tx.get(2).unwrap();
        n.set_header(BNODE_NODE, 1);
        n.node_append_kv(0, 0, "Key1".as_bytes(), "Val1".as_bytes());
        tx.useNode(2, &n);

        assert_eq!(1,tx.freelist.updates.len());
        let n1 = tx.get(2).unwrap();
        //n1.print();

        assert_eq!(1,n1.nkeys());
        let v: &[u8] = n1.get_val(0);
        assert_eq!("Val1".as_bytes(),v);
    }

    fn prepaircase_nonefreelist(data:&mut Vec<u8>)->Tx
    {
        //master
        let mut master = BNode::new(BTREE_PAGE_SIZE);

        //node from 1..10
        let mut nodes:Vec<BNode> = Vec::new();
        for i in 1..11
        {
            let mut n = BNode::new(BTREE_PAGE_SIZE);
            n.set_header(BNODE_NODE,0);
            nodes.push(n);
            //println!("Node Key:{i}");
        }

        data[0..BTREE_PAGE_SIZE].copy_from_slice(master.data());

        for i in 0..nodes.len()
        {
            data[(i+1)*BTREE_PAGE_SIZE..(i+2)*BTREE_PAGE_SIZE].copy_from_slice(nodes[i].data());
        }
        //println!("{:?}",data);
        let data_ptr: *mut u8 = data.as_mut_ptr();
        let mmap = Mmap { ptr: data_ptr, writer: Shared::new(())};
        let mmap =  Arc::new(RwLock::new(mmap));
        let mut nodes = Vec::new();
        let tx = Tx::new(mmap,11,BTREE_PAGE_SIZE * 15, &nodes,0,0,
            0,1,1);

        tx

    }

    fn preparenormalcase(data:&mut Vec<u8>,minReaderVersion:u64)->Tx
    {
        //master
        let mut master = BNode::new(BTREE_PAGE_SIZE);

        //node from 1..10
        let mut nodes:Vec<BNode> = Vec::new();
        for i in 0..10
        {
            let mut n = BNode::new(BTREE_PAGE_SIZE);
            n.set_header(BNODE_NODE,0);
            nodes.push(n);
            //println!("Node Key:{}",i+1);
        }

        //free node from 11..14,free node 23,45,67,89
        let mut freenodes:Vec<BNode> = Vec::new();
        for i in 0..4
        {
            let mut n = BNode::new(BTREE_PAGE_SIZE);
            if( i == 3)
            {
                n.flnSetHeader(2, 0);
            }
            else
            {
                n.flnSetHeader(2, 10 +i+2);
            }
            for j in 0..2
            {
                n.flnSetPtrWithVersion(j,(2 *i + 2 + j as u64) as u64 , i + 2);
                //println!("Free Node SetPtr:{} {}",10 + i + 1, 2 *i + 2 + j as u64);
            }
            //println!("Free Node Key:{}  Next:{}",10 + i + 1, n.flnNext());
            if i== 0
            {
                n.flnSetTotal(8);
                //n.print();
            }
            freenodes.push(n);
        }

        let mut idx:usize = 0;
        //let mut data: Vec<u8> = vec![0; BTREE_PAGE_SIZE*15];
        data[idx*BTREE_PAGE_SIZE..(idx+1)*BTREE_PAGE_SIZE].copy_from_slice(master.data());
        idx += 1;

        for i in 0..nodes.len()
        {
            data[ idx*BTREE_PAGE_SIZE..(idx+1)*BTREE_PAGE_SIZE].copy_from_slice(nodes[i].data());
            idx += 1;
        }
        for i in 0..freenodes.len()
        {
            data[ idx*BTREE_PAGE_SIZE..(idx+1)*BTREE_PAGE_SIZE].copy_from_slice(freenodes[i].data());
            idx += 1;
        }

        //println!("{:?}",&data[11*BTREE_PAGE_SIZE..12*BTREE_PAGE_SIZE]);

        // 获取 Vec<u8> 的指针
        let data_ptr: *mut u8 = data.as_mut_ptr();
        let mmap = Mmap { ptr: data_ptr, writer: Shared::new(())};
        let mmap =  Arc::new(RwLock::new(mmap));
        let mut nodes = Vec::new();
        nodes.push(11);
        nodes.push(12);
        nodes.push(13);
        nodes.push(14);
 
        let tx = Tx::new(mmap,15,BTREE_PAGE_SIZE * 15, &nodes,11,12,0,
            3,minReaderVersion);

        tx
    }
}

