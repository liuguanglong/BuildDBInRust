use crate::btree::kv::nodeinterface::BNodeReadInterface;
use crate::btree::kv::nodeinterface::BNodeWriteInterface;
use std::collections::HashMap;
use crate::btree::kv::node::BNode;
use crate::btree::kv::noderef::BNodeRef;

struct MemoryContext{
    idx:u64,
    pages:HashMap<u64, BNode>,
}

impl MemoryContext {
    fn new() -> Self{
        MemoryContext{
            idx:0,
            pages:HashMap::new(),
        }
    }

    fn newNode(&mut self,node:BNode) -> u64 
    {
        self.idx += 1;
        self.pages.insert(self.idx,node);
        return self.idx; 
    }

    fn getNode(& self,key:&u64) ->  Option<BNodeRef>
    {
        let node = self.pages.get(key);
        match node
        {
            Some(x) => {
                let s = BNodeRef{data:x.data(),size:x.size()};
                Some(s)    
            },
            None =>  None,
        }
    }

    fn delNode(&mut self,key:&u64)-> Option<BNode>
    {
        self.pages.remove(key)
    }

}

#[cfg(test)]
mod tests {
    use super::*;

    //#[test]
    fn it_works() {
        println!("it_works");
        let novel1 = String::from("Call me Ishmael.");
        let mut node = BNode::new(16);
        node.copy_value(&novel1);

        let mut context = MemoryContext::new();
        let idx = context.newNode(node);
        println!("Index:{idx}");

        let n1 = context.getNode(&idx);
        match n1 {
            // The division was valid
            Some(x) =>{
                //x.print();
                for i in 0..x.size() {
                    print!("{:02x} ", x.data()[i]);
                }
            },
            // The division was invalid
            None => println!("Cannot divide by 0"),
        }

        let mut node1 = BNode::new(16);
        node1.copy_value("test");
        let idx = context.newNode(node1);
        println!("Index:{idx}");
        let n1 = context.getNode(&idx);
        match n1 {
            // The division was valid
            Some(x) =>{
                //x.print();
                for i in 0..x.size() {
                    print!("{:02x} ", x.data()[i]);
                }
            },
            // The division was invalid
            None => println!("Cannot divide by 0"),
        }


    }

}