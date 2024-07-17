use std::{collections::HashMap, sync::{Arc, RwLock}};
use crate::btree::{kv::{node::BNode, nodeinterface::BNodeFreeListInterface, ContextError, FREE_LIST_CAP_WITH_VERSION}, BTREE_PAGE_SIZE};
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
    pageflushed: u64, // database size in number of pages
    nappend: u16, //number of pages to be appended
}

impl Tx{
    pub fn new(data:Arc<RwLock<Mmap>>,len:usize,nodes:&Vec<u64>,head:u64,total:usize,offset:usize,version:u64,minReader:u64)->Self
    {
        let mut reader = TxReader::new(data, len);
        Tx{
            reader: reader,
            freelist: TxFreeList::new(head,version,minReader),
            pageflushed:0,
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
    fn UpdateFreeList(&mut self) -> Result<(),ContextError>{
        if self.freelist.data.offset == 0 && self.freelist.freed.len() == 0
        {
            return Ok(());
        }

        // prepare to construct the new list
        let mut total = self.freelist.data.total;
        let mut count = self.freelist.data.offset;
        let mut listReuse:Vec<u64> = Vec::new();
        let mut listFreeNode:Vec<u64> = Vec::new();
        let mut listOldFreeNode:Vec<u64> = Vec::new();

        for i in 0..self.freelist.freed.len() 
        {
            listFreeNode.push(self.freelist.freed[i]);
        }

        while self.freelist.data.head != 0 && listReuse.len() * FREE_LIST_CAP_WITH_VERSION < listFreeNode.len() 
        {
            let node = self.get(self.freelist.data.head);
            if let None = node 
            {
                return Err(ContextError::RootNotFound);
            };

            listFreeNode.push(self.freelist.data.head);
            //std.debug.print("Head Ptr:{d}  Size {d}\n", .{ self.head, flnSize(node1) });
            let node = node.unwrap();

            // remove some pointers
            let mut remain = node.flnSize() as usize - count;
            count = 0;
            let mut idx:usize = 0;
            // reuse pointers from the free list itself
            while remain > 0 &&idx < remain && (listReuse.len() - 1) * FREE_LIST_CAP_WITH_VERSION < listFreeNode.len() as usize
            {
                //std.debug.print("Handle Remain.\n", .{});\
                let (ptr,version) = node.flnPtrWithVersion(remain as usize);
                if Self::versionbefore(version,self.freelist.minReader) == false
                {
                    break;
                }
                idx += 1;
                listReuse.push(node.flnPtr(idx + self.freelist.data.offset as usize));
            }

            // move the node into the `old freed` list
            for idx in idx..remain as usize
            {
                //std.debug.print("Handle Freed. {d}\n", .{idx});
                listOldFreeNode.push(node.flnPtr(self.freelist.data.offset + idx));
            }

            total -= node.flnSize() as usize;
            self.freelist.data.head = node.flnNext();
        }

        let newTotal = total + listFreeNode.len() as usize + listOldFreeNode.len() as usize;
        assert!(listReuse.len() * FREE_LIST_CAP_WITH_VERSION >= listReuse.len() || self.freelist.data.head == 0);
        self.flPush(&mut listFreeNode, &mut listReuse,&mut listOldFreeNode, newTotal);

        // let mut headnode = self.get(self.freelist.data.head);
        // if let Some( mut h) = headnode{
        //     h.flnSetTotal(newTotal);  
        //     self.useNode(self.freehead, &h);          
        // } 

        Ok(())
    }

    fn flPush(&mut self, listFreeNode: &mut Vec<u64>, listReuse:  &mut Vec<u64> , listOldFreeNode:& mut Vec<u64>, newTotal: usize) {

        //Set Head
        if(listOldFreeNode.len() > 0)
        {
            assert!(listOldFreeNode.len() < FREE_LIST_CAP_WITH_VERSION);
            let mut newNode = BNode::new(BTREE_PAGE_SIZE);
            newNode.flnSetHeader(listOldFreeNode.len() as u16, self.freelist.data.head);
            newNode.flnSetTotal(newTotal as u64);

            for idx in 0..listOldFreeNode.len() 
            {
                let ptr = listOldFreeNode.pop().unwrap();
                newNode.flnSetPtrWithVersion( idx, ptr,self.freelist.version);
            }
            if listReuse.len() > 0 
            {
                //reuse a pointer from the list
                let ptrHead = listReuse.pop().unwrap();
                self.freelist.data.head = ptrHead;
                self.useNode(ptrHead, &newNode);
            }  
            else {
                self.freelist.data.head = self.appendNode(&newNode);
            }
        }
        else {
            //Set New Total
            let mut head = self.get(self.freelist.data.head).unwrap();
            head.flnSetTotal(newTotal as u64);
            self.useNode(self.freelist.data.head, &head)
        }

        //Set Tail
        while listFreeNode.len() > 0 
        {
            let mut newNode = BNode::new(BTREE_PAGE_SIZE);

            //construc new node
            let mut size: usize = listFreeNode.len();
            if size > FREE_LIST_CAP_WITH_VERSION
            {
                size = FREE_LIST_CAP_WITH_VERSION;
            }

            newNode.flnSetHeader(size as u16, 0);
            for idx in 0..size 
            {
                let ptr = listFreeNode.pop().unwrap();
                newNode.flnSetPtrWithVersion( idx, ptr,self.freelist.version);
            }

            if listReuse.len() > 0 
            {
                //reuse a pointer from the list
                let ptrNewTail = listReuse.pop().unwrap();
                //std.debug.print("Reuse Ptr {d} \n", .{self.head});['']
                self.useNode(ptrNewTail, &newNode);

                let ptrTail = self.freelist.data.nodes.last().unwrap().clone();
                let mut tail = self.get(ptrTail).unwrap();
                tail.flnSetNext(ptrNewTail);
                self.useNode(ptrTail,&tail);

            } else {
                let ptrNewTail = self.appendNode(&newNode);
                
                let ptrTail = self.freelist.data.nodes.last().unwrap().clone();
                let mut tail = self.get(ptrTail).unwrap();
                tail.flnSetNext(ptrNewTail);
                self.useNode(ptrTail,&tail);
            }
        }

        assert!(listReuse.len() == 0);

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



