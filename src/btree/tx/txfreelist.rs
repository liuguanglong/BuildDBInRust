use std::{collections::HashMap, sync::{Arc, RwLock}};
use crate::btree::{kv::{node::{self, BNode}, nodeinterface::{BNodeFreeListInterface, BNodeReadInterface, BNodeWriteInterface}, ContextError, FREE_LIST_CAP_WITH_VERSION}, BTREE_PAGE_SIZE};
use super::{txinterface::TxReadContext, txreader::TxReader, winmmap::Mmap};

pub struct FreeListData{
    pub head: u64, //head of freeelist
    // cached pointers to list nodes for accessing both ends.
    pub nodes:Vec<u64>,   //from the tail to the head.  tail is the oldese node|head is the newest node
    // cached total number of items; stored in the head node.
    pub total:usize,
    // cached number of discarded items in the tail node.
    pub offset:usize,
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
    pub data: FreeListData,
    // newly allocated or deallocated pages keyed by the pointer.
    // nil value denotes a deallocated page.
    pub updates: HashMap<u64, Option<BNode>>,

    // for each transaction
    pub freed: Vec<u64>, // pages that will be added to the free list
    pub version: u64,    // current version
    pub minReader: u64,  // minimum reader version
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


#[cfg(test)]
mod tests {

    use std::{borrow::BorrowMut, hash::Hash, sync::{Arc, RwLock}, time::Duration};
    use rand::Rng;
    use crate::btree::{kv::nodeinterface::BNodeReadInterface, tx::{tx::Tx, txdemo::Shared}, BNODE_NODE};

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
        tx.UpdateFreeList();

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
        tx.UpdateFreeList();

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
        let ptr = tx.GetFreeNode();
        assert_eq!(2,ptr);
        assert_eq!(11,tx.freelist.data.total);
        assert_eq!(1,tx.freelist.data.offset);        

        let ptr = tx.GetFreeNode();
        assert_eq!(3,ptr);
        assert_eq!(10,tx.freelist.data.total);
        assert_eq!(2,tx.freelist.data.offset);

        let ptr = tx.GetFreeNode();
        assert_eq!(11,ptr);
        assert_eq!(9,tx.freelist.data.total);
        assert_eq!(0,tx.freelist.data.offset);
        assert_eq!(12,tx.freelist.data.head);

        let ptr = tx.GetFreeNode();
        assert_eq!(4,ptr);
        assert_eq!(8,tx.freelist.data.total);
        assert_eq!(1,tx.freelist.data.offset);
        assert_eq!(12,tx.freelist.data.head);

        let ptr = tx.GetFreeNode();  //5
        let ptr = tx.GetFreeNode();  //12
        let ptr = tx.GetFreeNode();  //6
        let ptr = tx.GetFreeNode();  //7

        let ptr = tx.GetFreeNode();  //13
        assert_eq!(13,ptr);
        assert_eq!(3,tx.freelist.data.total);
        assert_eq!(0,tx.freelist.data.offset);
        assert_eq!(14,tx.freelist.data.head);

        let ptr = tx.GetFreeNode();  //8
        assert_eq!(8,ptr);
        assert_eq!(2,tx.freelist.data.total);
        assert_eq!(1,tx.freelist.data.offset);
        assert_eq!(14,tx.freelist.data.head);

        let ptr = tx.GetFreeNode();  //9
        assert_eq!(9,ptr);
        assert_eq!(1,tx.freelist.data.total);
        assert_eq!(2,tx.freelist.data.offset);
        assert_eq!(14,tx.freelist.data.head);

        let ptr = tx.GetFreeNode();  //9
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
        tx.UpdateFreeList();

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
        tx.UpdateFreeList();

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
        let tx = Tx::new(mmap,1,11,BTREE_PAGE_SIZE * 15, 0,
            0,&nodes,0,0,
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
 
        let tx = Tx::new(mmap,1,15,BTREE_PAGE_SIZE * 15,
            0,0, &nodes,
            11,12,0,
            3,minReaderVersion);

        tx
    }
}

