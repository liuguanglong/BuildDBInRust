use std::sync::{Arc, RwLock, RwLockReadGuard};

use crate::btree::{kv::{node::BNode, nodeinterface::{BNodeReadInterface, BNodeWriteInterface}}, scan::{biter::BIter, comp::OP_CMP}, BTREE_PAGE_SIZE};
use super::{txbiter::TxBIter, txinterface::{TxReadContext, TxReaderInterface}, winmmap::Mmap};

pub struct TxReader{
    data:Arc<RwLock<Mmap>>,
    root: u64,
    version:u64,
    index:u64,
    len:usize
}

impl TxReader{
    pub fn new(data:Arc<RwLock<Mmap>>,len:usize) -> TxReader{
        TxReader{
            data:data,
            len:len,
            root:0,
            version:0,
            index:0,
        }
    }

    fn SeekLE(&self, key:&[u8]) -> TxBIter
    {
        let mut iter = TxBIter::new(self);

        let mut ptr = self.get_root();
        let mut n = self.get(ptr).unwrap();
        let mut idx: usize = 0;
        while (ptr != 0) {
            n = self.get(ptr).unwrap();
            idx = n.nodeLookupLE(key) as usize;

            if n.btype() == crate::btree::BNODE_NODE {
                ptr = n.get_ptr(idx);
            } else {
                ptr = 0;
            }

            iter.path.push(n);
            iter.pos.push(idx);
        }
        iter.valid = true;
        return iter;
    }

    
    // Search a key from the tree
    fn treeSearch<T:BNodeReadInterface>(&self, treenode: &T, key: &[u8]) -> Option<Vec<u8>> {
        // where to find the key?
        let idx = treenode.nodeLookupLE(key);
        // act depending on the node type
        match  treenode.btype() {
            crate::btree::BNODE_LEAF => {
                // leaf, node.getKey(idx) <= key
                let key1 = treenode.get_key(idx);
                let comp = crate::btree::util::compare_arrays(key, key1);
                if  comp == 0 {
                    return Some(treenode.get_val(idx).to_vec());
                } else {
                    // not found
                    return None;
                }
            },
            crate::btree::BNODE_NODE => {
                let ptr = treenode.get_ptr(idx as usize);
                let subNode = self.get(ptr);
                match subNode{
                    Some(node) => {
                        return self.treeSearch(&node,key);
                    } 
                    None => return None
                }
            },
            other=> return None
        }
    }
}

impl TxReaderInterface for TxReader{

    fn Get(&self, key:&[u8])  -> Option<Vec<u8>> {
        let rootNode = self.get(self.get_root());
        match rootNode{
            Some(root) => return self.treeSearch(&root,key),
            None => return None
        }
    }

    fn Seek(&self, key:&[u8], cmp:crate::btree::scan::comp::OP_CMP) -> TxBIter {
        let mut iter = self.SeekLE(key);
        if iter.Valid() {
            if let OP_CMP::CMP_LE = cmp  
            {
                return iter;
            }

            let cur = iter.Deref();
            if crate::btree::scan::comp::cmpOK(cur.0, key, &cmp) == false {
                //off by one
                if cmp.value() > 0 {
                    _ = iter.Next();
                } else {
                    _ = iter.Prev();
                }
                return iter;
            }
        }
        return iter;
    }
}

impl TxReadContext for TxReader{    
    fn get_root(&self)->u64{
        return self.root;
    }

    fn get(&self,key:u64) -> Option<BNode>{
        let offset = key as usize * BTREE_PAGE_SIZE;
        assert!(offset + BTREE_PAGE_SIZE < self.len);

        if let Ok(mmap) = self.data.read(){
            let mut newNode = BNode::new(BTREE_PAGE_SIZE);
            newNode.copy_Content(mmap.ptr, offset, BTREE_PAGE_SIZE);
            drop(mmap);
            //newNode.copy_Data(&self.data,offset,BTREE_PAGE_SIZE);
            return Some(newNode);    
        }
        println!("Get Lock Error!");
        None
    }
}

