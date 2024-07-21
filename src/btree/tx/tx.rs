use std::{collections::HashMap, sync::{Arc, RwLock}};
use crate::btree::{kv::{node::{self, BNode}, nodeinterface::{BNodeFreeListInterface, BNodeReadInterface, BNodeWriteInterface}, ContextError, FREE_LIST_CAP_WITH_VERSION}, BTREE_PAGE_SIZE};
use super::{txfreelist::TxFreeList, txinterface::TxReadContext, txreader::TxReader, winmmap::Mmap};

pub struct Tx{
    pub freelist:TxFreeList,
    pub pageflushed: u64, // database size in number of pages
    pub nappend: u16, //number of pages to be appended
    pub root:u64,

    //pub reader:TxReader,
    len:usize,
    data:Arc<RwLock<Mmap>>,
}

impl TxReadContext for Tx{
    fn get_root(&self)->u64{
        return self.root;
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
                self.getMapped(key)
            },
        }
    }
}

impl Tx{
    pub fn new(data:Arc<RwLock<Mmap>>,root:u64,pageflushed:u64,filelen:usize,
        freenodes:&Vec<u64>,freehead:u64,freetotal:usize,version:u64,minReader:u64)->Self
    {
        //let mut reader = TxReader::new(data, filelen,readerversion,readerindex);
        Tx{
            data: data,
            len:filelen,
            freelist: TxFreeList::new(freehead,version,minReader,freenodes,freetotal),
            pageflushed:pageflushed,
            nappend:0,
            root:root,
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

    pub fn UpdateFreeList(&mut self) -> Result<(),ContextError>{

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

    pub fn add(&mut self,node:BNode) -> u64 
    {
        let mut ptr: u64 = 0;
        let totalfree = self.freelist.data.total as u16;

        let mut ptr= self.PopFreeNode();
        if ptr == 0
        {
            ptr = self.pageflushed + self.nappend as u64;
            self.nappend += 1;
        }

        let newNode = node.copy();
        self.freelist.updates.insert(ptr, Some(newNode));

        ptr
    }

    pub fn set_root(&mut self,ptr:u64){
        self.root = ptr;
    }

    pub fn del(&mut self,key:u64)-> Option<BNode>
    {
        let node = self.get(key);
        self.freelist.freed.push(key);
        node
    }

    pub fn appendNode(&mut self, bnode: &BNode)-> u64 {
        let newNode = bnode.copy();

        let ptr = self.pageflushed + self.nappend as u64;
        self.nappend += 1;

        self.freelist.updates.insert(ptr, Some(newNode));

        return ptr;
    }

    pub fn useNode(&mut self, ptr: u64, bnode: &BNode) {

        let newNode = bnode.copy();
        self.freelist.updates.insert(ptr, Some(newNode));
    }

    fn versionbefore(ver:u64,minReader:u64)->bool
    {
        return ver < minReader;
    }

    
    fn getMapped(&self,key:u64) -> Option<BNode>
    {
        let offset = key as usize * BTREE_PAGE_SIZE;
        assert!(offset + BTREE_PAGE_SIZE <= self.len);
        
        if let Ok(mmap) = self.data.read(){

            let mut newNode = BNode::new(BTREE_PAGE_SIZE);
            //println!("index:{}",key);
            newNode.copy_Content(mmap.ptr, offset, BTREE_PAGE_SIZE);
            drop(mmap);
            //newNode.copy_Data(&self.data,offset,BTREE_PAGE_SIZE);
            //newNode.print();
            return Some(newNode);    

        }
        println!("Get Lock Error!");
        None
    }


    fn loadCache(&mut self)
    {
        if self.freelist.data.nodes.len() != 0
        {
            return; 
        }

        let mut node = self.getMapped(self.freelist.data.head).unwrap();
        self.freelist.data.nodes.push(self.freelist.data.head);
        self.freelist.data.total = node.flnGetTotal() as usize;
        self.freelist.data.offset = node.flnSize() as usize;
        let mut next = node.flnNext();
        while next != 0
        {
            self.freelist.data.nodes.push(next);
            node = self.getMapped(next).unwrap();
            next = node.flnNext();
        }        
    }
}

