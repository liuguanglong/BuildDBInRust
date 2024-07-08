use crate::btree::kv::contextinterface::KVContextInterface;
use crate::btree::kv::nodeinterface::BNodeReadInterface;
use crate::btree::kv::nodeinterface::BNodeWriteInterface;
use crate::btree::kv::nodeinterface::BNodeOperationInterface;

use crate::btree::kv::node::BNode;

pub struct BTree<'a> {
    context: &'a mut dyn KVContextInterface,    
}

impl <'a> BTree <'a>{
    pub fn new(context:&'a mut dyn KVContextInterface) -> Self {
        BTree {
            context: context,
        }
    }

    pub fn print(&self){
        let root = self.context.get_root();

        println!("BTree content: Root:{:?} \n", root);
        if root == 0
        {
            return;
        }

        let nodeRoot = self.context.get(root);
        match nodeRoot{
            Some(r) => self.printNode(&r),
            None => println!("Root is not set!")
        }
        println!();
    }

    fn printNode<T:BNodeReadInterface>(&self, treenode: &T) {
        if treenode.btype() == crate::btree::BNODE_LEAF {
            treenode.print();
        } else if treenode.btype() == crate::btree::BNODE_FREE_LIST {
            //treenode.print();
        } else {
            treenode.print();
            let nkeys = treenode.nkeys();
            println!("NKeys {:?}", nkeys);
            let mut idx: u16 = 0;
            while idx < nkeys {
                let prtNode = treenode.get_ptr(idx as usize);
                let subNode = self.context.get(prtNode);
                match subNode{
                    Some(r) => self.printNode(&r),
                    None => println!("Root is not set!")
                }
                idx = idx + 1;
            }
        }
    }

    pub fn Set(&mut self,key: &[u8], val: &[u8], mode: u16) {
        self.InsertKV(key, val, mode);
        self.context.save();
    }
        //Interface for Insert KV
        pub fn InsertKV(&mut self, key: &[u8], val: &[u8], mode: u16) {
            assert!(key.len() != 0);
            assert!(key.len() <= crate::btree::BTREE_MAX_KEY_SIZE);
            assert!(val.len() <= crate::btree::BTREE_MAX_VALUE_SIZE);
    
            if self.context.get_root() == 0 {
                let mut root = BNode::new(crate::btree::BTREE_PAGE_SIZE);
                root.set_header(crate::btree::BNODE_NODE, 1);
                root.node_append_kv(0, 0, key, val);

                let newroot = self.context.add(root);
                self.context.set_root(newroot);
                return;
            }
    
            let oldRootPtr = self.context.get_root();
            _ = self.context.del(oldRootPtr);
            let nodeRoot = self.context.get(oldRootPtr).unwrap();
    
            let mut nodeTmp = self.treeInsert(&nodeRoot, key, val, mode);
            match(nodeTmp)
            {
                Some(node) => {
                    let (count,n1,n2,n3) = node.nodeSplit3();
                    if(count == 1)
                    {
                        let ptr1 = self.context.add(n1.unwrap());
                        self.context.set_root(ptr1);
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
        }

        
    // Search a key from the tree
    pub fn treeSearch<T:BNodeReadInterface>(&self, treenode: &T, key: &[u8]) -> Option<Vec<u8>> {
        // where to find the key?
        let idx = treenode.nodeLookupLE(key);
        // act depending on the node type
        match  treenode.btype() {
            crate::btree::BNODE_LEAF => {
                // leaf, node.getKey(idx) <= key
                let comp = crate::btree::util::compare_arrays(key, treenode.get_key(idx));
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

        // delete a key from the tree
        pub fn treeDelete<T:BNodeReadInterface>(&mut self, treenode: &T, key: &[u8]) -> Option<BNode> {
            // where to find the key?
            let idx = treenode.nodeLookupLE(key);
            // act depending on the node type
            match treenode.btype() {
                crate::btree::BNODE_LEAF => {
                    // leaf, node.getKey(idx) <= key
                    let comp = crate::btree::util::compare_arrays(key, treenode.get_key(idx));
                    if comp == 0 {
                        // delete the key in the leaf
                        //std.debug.print("Node Delete! {d}", .{idx});
                        //treenode.print();
                        let mut node = BNode::new(crate::btree::BTREE_PAGE_SIZE);
                        node.leaf_delete(treenode, idx);
                        //updatedNode.print();
                        return Some(node);
                    } else {
                        // not found
                        return None;
                    }
                },
                crate::btree::BNODE_NODE => {
                    // internal node, insert it to a kid node.
                    return self.nodeDelete(treenode, idx, key);
                },
                other => {
                    panic!("Exception Insert Node!\n");
                },
            }
        }

        pub fn shouldMerge<T:BNodeReadInterface>(&self, treenode: &T, idx: u16, updated: &BNode)-> (i16,Option<u64>) {
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

        pub fn nodeDelete<T:BNodeReadInterface>(&mut self, treenode:&T, idx: u16, key: &[u8]) -> Option<BNode> {
            // recurse into the kid
            let kptr = treenode.get_ptr(idx as usize);
            let realnode = self.context.get(kptr);
            if realnode.is_none()
            {
                panic!("Node is not found! idx:{:?} Key:{:?}", idx, key);
            }
    
            let updated = self.treeDelete(&realnode.unwrap(), key).unwrap();
            //nodeTmp.print();
            _ = self.context.del(kptr);
    
            let mut newNode = BNode::new(crate::btree::BTREE_PAGE_SIZE);
            // check for merging
            let (flagMerged,sibling) = self.shouldMerge(treenode, idx, &updated);
            match flagMerged {
                0 => {
                    assert!(updated.nkeys() > 0);
                    let ptr = self.context.add(updated);

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
                            merged.nodeMerge(&n, &updated);
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
                            merged.nodeMerge( &updated,&n);
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
    
    // insert a KV into a node, the result might be split into 2 nodes.
    // the caller is responsible for deallocating the input node
    // and splitting and allocating result nodes.
    pub fn treeInsert<T:BNodeReadInterface>(&mut self, oldNode:&T, key:&[u8], val:&[u8], mode: u16) -> Option<BNode> {
        // where to insert the key?
        let idx = oldNode.nodeLookupLE(key);
        //std.debug.print("Find  Key:{s} Index:{d}", .{ key, idx });
        // act depending on the node type
        let mut newNode = BNode::new(2 * crate::btree::BTREE_PAGE_SIZE);
        match oldNode.btype() {
            crate::btree::BNODE_LEAF => {
                // leaf, node.getKey(idx) <= key
                let comp = crate::btree::util::compare_arrays(key, oldNode.get_key(idx));
                if  comp == 0 {
                    if mode == crate::btree::MODE_INSERT_ONLY 
                    {
                        return None;
                    }
                    // found the key, update it.
                    newNode.leaf_update(oldNode, idx, key, val);
                } 
                else {
                    if mode == crate::btree::MODE_UPDATE_ONLY {
                        return None;
                    }
                    // insert it after the position.
                    newNode.leaf_insert(oldNode, idx + 1, key, val);
                }
            },
            crate::btree::BNODE_NODE => {
                // internal node, insert it to a kid node.
                self.nodeInsert(& mut newNode, oldNode, idx, key, val, mode);
            },
            other => {
                panic!("Exception Insert Node!\n");
            },
        }
        return Some(newNode);
    }

    // part of the treeInsert(): KV insertion to an internal node
    pub fn nodeInsert<T:BNodeReadInterface>(&mut self, newNode: &mut BNode, oldNode: &T, idx: u16, key:&[u8], val:&[u8], mode: u16) {
        //get and deallocate the kid node
        let kptr = oldNode.get_ptr(idx as usize);
        let mut knode = self.context.get(kptr).unwrap();
        _ = self.context.del(kptr);

        let insertNode = self.treeInsert(&knode, key, val, mode);
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
        //std.debug.print("Split Count:{d}", .{subNodes.Count});
    }

}







