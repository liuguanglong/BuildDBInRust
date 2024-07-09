use crate::btree::table::table::TableDef;
use crate::btree::table::value::Value;
use crate::btree::table::value::ValueType;
use std::fmt;
use std::error::Error;
use crate::btree::BTreeError;
use crate::btree::kv::contextinterface::KVContextInterface;
use crate::btree::kv::node::BNode;
use crate::btree::kv::nodeinterface::BNodeReadInterface;
use crate::btree::kv::nodeinterface::BNodeWriteInterface;

pub struct BIter<'a>{
    context: &'a dyn KVContextInterface,    
    pub path: Vec<BNode>, // from root to leaf
    pub pos: Vec<usize>, //indexes into nodes
    pub valid: bool,
}

impl<'a> BIter<'a> {

    pub fn new(context:&'a dyn KVContextInterface) -> Self {
        BIter {
            context: context,
            path: Vec::new(),
            pos: Vec::new(),
            valid: false
        }
    }

    pub fn Valid(&self) -> bool {
        return self.valid;
    }

    pub fn Deref(&'a self) -> (&'a [u8],&'a [u8]) {
        //println!("Path Count:{} Pos:{} \n", self.path.items.len(), self.pos.last());
        let n = self.path.last().unwrap();
        return (n.get_key(*self.pos.last().unwrap() as u16), n.get_val(*self.pos.last().unwrap() as u16))
    }

    pub fn Prev(&mut self) -> bool {
        
        let ret = self.interPrev(self.path.len() - 1);
        if let Result::Err(_) = ret
        {
            return false;
        } 
        return true;
    }

    pub fn Next(&mut self) -> bool {
        let ret = self.interNext(self.path.len() - 1);
        if let Result::Err(_) = ret
        {
            return false;
        } 
        return true;
    }

    //           root
    //           1                        2 3
    //11      12             13
    //   121 122 123    131 132 133
    //133-》Prev =》 level 2 + Path(root,1,13) Pos(0,2,2) -> 132 Path(root,1,13,) Pos(0,2,1)
    //132-》Prev =》 level 2 + Path(root,1,13) Pos(0,2,1) -> 131 Path(root,1,13)  Pos(0,2,0)
    //131-》Prev =》 level 2 + Path(root,1,13) Pos(0,2,0) -> level 1 + Path(root,1) Pos(0,2) -> level 1 + Path(root,1) Pos(0,1) -> level 2 + Path(root,1,12) Pos(0,1,2)

    //122-》Next => level 2 + Path(root,1,12) Pos(0,1,1) -> 123  Path(root,1,12) Pos(0,1,2)
    //123-》Next => level 2 + Path(root,1,12) Pos(0,1,2) -> level 1 + Path(root,1) Pos(0,1) -> level 1 + Path(root,1) Pos(0,2) -> 131 level 2 + Path(root,1,13) Pos(0,1,0)
    fn interPrev(&mut self, level: usize)-> Result<(),BTreeError> {

        if  self.pos[level] > 0 { // move within this node
            self.pos[level] -= 1;

        } 
        else if level > 0 
        {   
            // move to a slibing node
            _ = self.path.pop();
            _ = self.pos.pop();
            self.interPrev(level - 1);
        } 
        else {
            self.valid = false;
            return Err(BTreeError::PrevNotFound);
        }

        if  level + 1 < self.pos.len() 
        {
            // update the kid node
            let idx = self.path[level].get_ptr(self.pos[level]);
            let kid = self.context.get(idx).unwrap();
            let pos = kid.nkeys() as usize -1;

            self.path.push(kid);
            self.pos.push(pos);
        }
        Ok(())
    }

    fn interNext(&mut self, level: usize) -> Result<(),BTreeError> {

        let poslen = self.pos.len();
        if self.pos[level] < self.path[level].nkeys() as usize - 1 
        { // move within this node
            self.pos[level] += 1;
        } 
        else if level > 0 
        { // move to a slibing node
            self.interNext(level - 1);
            //_ = self.path.pop();
            //_ = self.pos.pop();
        } 
        else {
            self.valid = false;
            return Err(BTreeError::NextNotFound);
        }

        //println!("Level:{} Path.len:{} Pos.len:{} Old.Pos.Len{}\n", level, self.path.items.len, self.pos.items.len, poslen);
        if level + 1 < poslen 
        {
            // update the kid node
            let idx = self.path[level].get_ptr(self.pos[level]);
            let kid = self.context.get(idx).unwrap();
            //kid.print();

            //std.debug.print("First Node {s} {s}\n", .{ kid.getKey(0), kid.getValue(0) });
            _ = self.path.pop();
            _ = self.pos.pop();
            self.path.push(kid);
            self.pos.push(0);
        }

        Ok(())
    }

}