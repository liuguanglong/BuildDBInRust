use crate::btree::{btree::request::{DeleteRequest, InsertReqest}, kv::{node::BNode, nodeinterface::{BNodeOperationInterface, BNodeReadInterface, BNodeWriteInterface}}, scan::comp::OP_CMP};

use super::{tx::{self, Tx}, txbiter::TxBIter, txinterface::{TxInterface, TxReadContext, TxReaderInterface}};


pub struct txwriter{
    context : Tx,
}

impl TxReaderInterface for txwriter{

    fn Get(&self, key:&[u8])  -> Option<Vec<u8>> {
        let rootNode = self.context.get(self.context.get_root());
        match rootNode{
            Some(root) => return self.treeSearch(&root,key),
            None => return None
        }
    }

    fn Seek(&self, key:&[u8], cmp:crate::btree::scan::comp::OP_CMP) -> super::txbiter::TxBIter {
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


impl TxInterface for txwriter{
    fn Set(&mut self,req:&mut InsertReqest){
        self.InsertKV(req);
    }

    fn Delete(&mut self, req: &mut DeleteRequest) -> bool{
        self.DeleteKV(req)
    }
}

impl txwriter{

    fn SeekLE(&self, key:&[u8]) -> TxBIter
    {
        let mut iter = TxBIter::new(&self.context);

        let mut ptr = self.context.get_root();
        let mut n = self.context.get(ptr).unwrap();
        let mut idx: usize = 0;
        while (ptr != 0) {
            n = self.context.get(ptr).unwrap();
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
                let subNode = self.context.get(ptr);
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

    fn DeleteKV(&mut self, request: &mut DeleteRequest) -> bool
    {
        assert!(request.Key.len() != 0);
        assert!(request.Key.len() <= crate::btree::BTREE_MAX_KEY_SIZE);

        if (self.context.get_root() == 0) {
            return false;
        }

        //const n1 = try self.kv.get(self.kv.getRoot());
        let n1 = self.context.get(self.context.get_root());
        if(n1.is_none())
        {
            panic!("Root not found!");
        }        

        let updated = self.treeDelete(&n1.unwrap(), request);
        match updated{
            Some(n) => {
                _ = self.context.del(self.context.get_root());
                if n.btype() == crate::btree::BNODE_NODE && n.nkeys() == 1 {
                    // remove a level
                    self.context.set_root(n.get_ptr(0));
                } else {
                    let newroot = self.context.add(n);
                    self.context.set_root(newroot);
                }
                return true
            },
            None=> return false
        }
    }

     // delete a key from the tree
     fn treeDelete<T:BNodeReadInterface>(&mut self, treenode: &T, request: &mut DeleteRequest) -> Option<BNode> {
        // where to find the key?
        let idx = treenode.nodeLookupLE(request.Key);
        // act depending on the node type
        match treenode.btype() {
            crate::btree::BNODE_LEAF => {
                // leaf, node.getKey(idx) <= key
                let comp = crate::btree::util::compare_arrays(request.Key, treenode.get_key(idx));
                if comp == 0 {
                    // delete the key in the leaf
                    //std.debug.print("Node Delete! {d}", .{idx});
                    //treenode.print();
                    let mut node = BNode::new(crate::btree::BTREE_PAGE_SIZE);
                    node.leaf_delete(treenode, idx);

                    let v = treenode.get_val(idx);
                    request.OldValue.extend_from_slice(v);
                    //updatedNode.print();
                    return Some(node);
                } else {
                    // not found
                    return None;
                }
            },
            crate::btree::BNODE_NODE => {
                // internal node, insert it to a kid node.
                return self.nodeDelete(treenode, idx, request);
            },
            other => {
                panic!("Exception Insert Node!\n");
            },
        }
    }

    fn nodeDelete<T:BNodeReadInterface>(&mut self, treenode:&T, idx: u16, request: &mut DeleteRequest) -> Option<BNode> {
        // recurse into the kid
        let kptr = treenode.get_ptr(idx as usize);
        let realnode = self.context.get(kptr);
        if realnode.is_none()
        {
            panic!("Node is not found! idx:{:?} Key:{:?}", idx, request.Key);
        }

        let updated = self.treeDelete(&realnode.unwrap(), request);
        if  updated.is_none() {
            return None; // not found
        }
        _ = self.context.del(kptr);

        let mut nodeUpdated = updated.unwrap();
        let mut newNode = BNode::new(crate::btree::BTREE_PAGE_SIZE);
        // check for merging
        let (flagMerged,sibling) = self.shouldMerge(treenode, idx, &nodeUpdated);
        match flagMerged {
            0 => {
                assert!(nodeUpdated.nkeys() > 0);
                let ptr = self.context.add(nodeUpdated);

                let updatedNode = self.context.get(ptr).unwrap();
                let key = updatedNode.get_key(0);
                let nodes = vec![(ptr,key.to_vec())]; 
                newNode.nodeReplaceKidN(treenode, idx,nodes);
            },
            -1 => { //left
                //print!("Merge Left.\n");
                let mut merged = BNode::new(crate::btree::BTREE_PAGE_SIZE);
                let nodeMerged = self.context.get(sibling.unwrap());
                match nodeMerged
                {
                    Some(n) => {
                        merged.nodeMerge(&n, &nodeUpdated);
                        let prtMerged = self.context.add(merged);
                        _ = self.context.del(treenode.get_ptr(idx as usize - 1));

                        let nodeMerged = self.context.get(prtMerged).unwrap();
                        newNode.nodeReplace2Kid(treenode, idx - 1, prtMerged, nodeMerged.get_key(0));
                    },
                    None => panic!("Get Node Exception idx: {:?}", sibling)
                }
            },
            1 => { //right
                //std.debug.print("Merge Right.\n", .{});
                let mut merged = BNode::new(crate::btree::BTREE_PAGE_SIZE);
                let nodeMerged = self.context.get(sibling.unwrap());
                match nodeMerged
                {
                    Some(n) => {
                        merged.nodeMerge( &nodeUpdated,&n);
                        let prtMerged = self.context.add(merged);
                        _ = self.context.del(treenode.get_ptr(idx as usize + 1));

                        let nodeMerged = self.context.get(prtMerged).unwrap();
                        newNode.nodeReplace2Kid(treenode, idx, prtMerged, nodeMerged.get_key(0));
                    },
                    None => panic!("Get Node Exception idx: {:?}", sibling)
                }
            },
            other => {
                panic!("Exception Merge Flag!");
            },
        }

        return Some(newNode);
    }

    fn shouldMerge<T:BNodeReadInterface>(&self, treenode: &T, idx: u16, updated: &BNode)-> (i16,Option<u64>) {
        if updated.nbytes() > crate::btree::BTREE_PAGE_SIZE / 4 {
            return (0, None);
        }

        if  idx > 0 {
            let sibling = self.context.get(treenode.get_ptr(idx as usize - 1));
            match sibling{
                Some(n) => {
                    let merged:usize = n.nbytes() as usize + updated.nbytes() as usize - crate::btree::HEADER as usize;
                    if merged <= crate::btree::BTREE_PAGE_SIZE {
                        return (-1, Some(treenode.get_ptr(idx as usize - 1)));
                    }        
                },
                None => panic!("Get Node Exception idx: {:?}", idx - 1)
            }

        }
        if  idx + 1 < treenode.nkeys() {
            let sibling = self.context.get(treenode.get_ptr(idx as usize + 1));
            match sibling{
                Some(n) => {
                    let merged:usize = n.nbytes() as usize + updated.nbytes() as usize - crate::btree::HEADER as usize;
                    if merged <= crate::btree::BTREE_PAGE_SIZE {
                        return (1, Some(treenode.get_ptr(idx as usize + 1)));
                    }        
                },
                None => panic!("Get Node Exception idx: {:?}", idx - 1)
            }
        }

        return (0,None);
    }
    //Interface for Insert KV
    fn InsertKV(&mut self, request:&mut InsertReqest) {
        assert!(request.Key.len() != 0);
        assert!(request.Key.len() <= crate::btree::BTREE_MAX_KEY_SIZE);
        assert!(request.Val.len() <= crate::btree::BTREE_MAX_VALUE_SIZE);
    
        if self.context.get_root() == 0 {
            let mut root = BNode::new(crate::btree::BTREE_PAGE_SIZE);
            root.set_header(crate::btree::BNODE_LEAF, 2);
            root.node_append_kv(0, 0, &[0;1], &[0;1]);
            root.node_append_kv(1, 0, request.Key, request.Val);

            let newroot = self.context.add(root);
            self.context.set_root(newroot);

            request.Added = true;
            request.Updated = false;

            return;
        }
    
        let oldRootPtr = self.context.get_root();
        let nodeRoot = self.context.get(oldRootPtr).unwrap();
    
        let mut nodeTmp = self.treeInsert(&nodeRoot, request);
        match(nodeTmp)
        {
            Some(node) => {
            let (count,n1,n2,n3) = node.nodeSplit3();
            if count == 1
            {
                    let ptr1 = self.context.add(n1.unwrap());
                    self.context.set_root(ptr1);
                    _ = self.context.del(oldRootPtr);

                    return;
            }
                
            let mut root = BNode::new(crate::btree::BTREE_PAGE_SIZE);
            root.set_header(crate::btree::BNODE_NODE, count);

            let ptr1 = self.context.add(n1.unwrap());
            let node1 = self.context.get(ptr1).unwrap();
            let node1key = node1.get_key(0);
            root.node_append_kv(0, ptr1, node1key, &[0;1]);

            if n2.is_none() == false
            {
                let ptr = self.context.add(n2.unwrap());
                let node = self.context.get(ptr).unwrap();
                let nodekey = node.get_key(0);
                root.node_append_kv(1, ptr, nodekey, &[0;1]);
            }

            if n3.is_none() == false
            {
                let ptr = self.context.add(n3.unwrap());
                let node = self.context.get(ptr).unwrap();
                let nodekey = node.get_key(0);
                root.node_append_kv(2, ptr, nodekey, &[0;1]);    
            }
            let rootPtr = self.context.add(root);
            self.context.set_root(rootPtr);
            },
            None => {}
        }
        _ = self.context.del(oldRootPtr);
    }

    // insert a KV into a node, the result might be split into 2 nodes.
    // the caller is responsible for deallocating the input node
    // and splitting and allocating result nodes.
    fn treeInsert<T:BNodeReadInterface>(&mut self, oldNode:&T,request:&mut InsertReqest) -> Option<BNode> {
        // where to insert the key?
        let idx = oldNode.nodeLookupLE(request.Key);
        //println!("Find  Key:{:?} Index:{:?}", key, idx );
        // act depending on the node type
        let mut newNode = BNode::new(2 * crate::btree::BTREE_PAGE_SIZE);
        match oldNode.btype() {
            crate::btree::BNODE_LEAF => {
                // leaf, node.getKey(idx) <= key
                let comp = crate::btree::util::compare_arrays(request.Key, oldNode.get_key(idx));
                if  comp == 0 {
                    if request.Mode == crate::btree::MODE_INSERT_ONLY 
                    {
                        return None;
                    }
                    // found the key, update it.
                    newNode.leaf_update(oldNode, idx, request.Key, request.Val);
                    let v = oldNode.get_val(idx);
                    request.OldValue.extend_from_slice(v);
                    request.Added = false;
                    request.Updated = true;
                } 
                else {
                    if request.Mode == crate::btree::MODE_UPDATE_ONLY {
                        return None;
                    }
                    // insert it after the position.
                    newNode.leaf_insert(oldNode, idx + 1,  request.Key, request.Val);
                    request.Added = true;
                    request.Updated = true;
                }
            },
            crate::btree::BNODE_NODE => {
                // internal node, insert it to a kid node.
                self.nodeInsert(& mut newNode, oldNode, idx, request);
            },
            other => {
                panic!("Exception Insert Node!\n");
            },
        }
        return Some(newNode);
    }

     // part of the treeInsert(): KV insertion to an internal node
     fn nodeInsert<T:BNodeReadInterface>(&mut self, newNode: &mut BNode, oldNode: &T, idx:u16, request: &mut InsertReqest) {
        //get and deallocate the kid node
        let kptr = oldNode.get_ptr(idx as usize);
        let mut knode = self.context.get(kptr).unwrap();

        let insertNode = self.treeInsert(&knode, request);
        match insertNode{            
            Some(node) => {
                let mut nodes = Vec::new();
                let (_,n1,n2,n3) = node.nodeSplit3();
                match n1
                {
                    Some(subn) => {
                        let ptr = self.context.add(subn);
                        let subnode = self.context.get(ptr).unwrap();
                        let key = subnode.get_key(0);
                        nodes.push((ptr,key.to_vec()));
                    },
                    None => {}
                }
                match n2
                {
                    Some(subn) => {
                        let ptr = self.context.add(subn);
                        let subnode = self.context.get(ptr).unwrap();
                        let key = subnode.get_key(0);
                        nodes.push((ptr,key.to_vec()));
                    },
                    None => {}
                }
                match n3
                {
                    Some(subn) => {
                        let ptr = self.context.add(subn);
                        let subnode = self.context.get(ptr).unwrap();
                        let key = subnode.get_key(0);
                        nodes.push((ptr,key.to_vec()));
                    },
                    None => {}
                }
                newNode.nodeReplaceKidN(oldNode, idx, nodes);
            },
            None => {}
        }
        //_ = self.context.del(kptr);
        //std.debug.print("Split Count:{d}", .{subNodes.Count});
    }
}
