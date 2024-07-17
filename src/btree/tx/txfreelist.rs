use std::{collections::HashMap, sync::{Arc, RwLock}};
use crate::btree::kv::{node::BNode, nodeinterface::BNodeFreeListInterface, FREE_LIST_CAP_WITH_VERSION};
use super::{txinterface::TxReadContext, txreader::TxReader, winmmap::Mmap};

pub struct FreeListData{
    head: u64, //head of freeelist
    // cached pointers to list nodes for accessing both ends.
    nodes:Vec<u64>,   //from the tail to the head
    // cached total number of items; stored in the head node.
    total:usize,
    // cached number of discarded items in the tail node.
    offset:usize,
}
impl FreeListData {
    pub fn new(head:u64)->Self
    {
        FreeListData{
            head:head,
            nodes:Vec::new(),
            total:0,
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
    pub fn new(head:u64,version:u64,minReader:u64)->Self
    {
        TxFreeList{
            data: FreeListData::new(head),
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
}

impl Tx{
    pub fn new(data:Arc<RwLock<Mmap>>,len:usize,nodes:&Vec<u64>,head:u64,total:usize,offset:usize,version:u64,minReader:u64)->Self
    {
        let mut reader = TxReader::new(data, len);
        Tx{
            reader: reader,
            freelist: TxFreeList::new(head,version,minReader)
        }
    }

    // try to remove an item from the tail. returns 0 on failure.
    // the removed pointer must not be reachable by the minimum version reader.
    pub fn GetFreeNode(&mut self)->u64 {
        self.loadCache();
        return self.PopFreeNode()
    }

    fn PopFreeNode(&mut self)->u64{
        if self.freelist.data.total == 0
        {
            return 0;
        }

        // remove one item from the tail
        let mut node = self.get(self.freelist.data.nodes[0]).unwrap();
        assert!(self.freelist.data.offset < node.flnSize() as usize);
        let (ptr,ver) = node.flnPtrWithVersion(self.freelist.data.offset);
        if Self::versionbefore(ver,self.freelist.minReader)
        {
            // cannot use; possibly reachable by the minimum version reader.
            return 0;
        }
        self.freelist.data.offset += 1;
        self.freelist.data.total -= 1;

        // discard the empty node and move to the next node
        if self.freelist.data.offset == FREE_LIST_CAP_WITH_VERSION 
        {
            let emptynode = self.freelist.data.nodes.remove(0);
            self.freelist.freed.push(emptynode);
            self.freelist.data.offset = 0;
            self.freelist.data.head = self.freelist.data.nodes[0];
        }

        ptr
    }

    // add new pointers to the head and finalize the update
    pub fn UpdateFreeList(&mut self)
    {   
        let mut ptr = self.freelist.data.nodes.last().unwrap().clone();
        let mut node = self.get(ptr).unwrap();
        let mut idx = node.flnSize() as usize;

        let mut i :usize = 0;

        while i < self.freelist.freed.len()
        {
            if idx != FREE_LIST_CAP_WITH_VERSION
            {
                node.flnSetPtrWithVersion(idx, self.freelist.freed[i], self.freelist.minReader);
                idx += 1;
                i += 1;
            }

            if(idx == FREE_LIST_CAP_WITH_VERSION)
            {
                //Save Changed FreeNode
                self.useNode(ptr, &node);

                //Push New FreeNode To Tail
                ptr = self.freelist.freed[i];
                self.freelist.data.nodes.push(ptr);
                node = self.get(ptr).unwrap();
            }
        }
        //Save Changed FreeNode
        self.useNode(ptr, &node);

    }

    fn get(&self,key:u64) -> Option<BNode>
    {
        let node = self.freelist.updates.get(&key);
        match node
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



