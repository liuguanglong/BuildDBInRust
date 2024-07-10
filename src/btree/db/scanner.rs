use crate::btree::{btree::btree::BTree, scan::{biter::BIter, comp::OP_CMP, scaninterface::ScanInterface}, table::{record::Record, table::TableDef}, BTreeError};


pub struct Scanner<'a>{
     // the range, from Key1 to Key2
     Cmp1: OP_CMP,
     Cmp2: OP_CMP,
     iter: BIter<'a>,
     keyEnd: Vec<u8>,
     keyStart: Vec<u8>,
}

impl<'a> Scanner<'a> {
    
    pub fn new(cmp1: OP_CMP, cmp2: OP_CMP,keyStart:Vec<u8>,keyEnd:Vec<u8>,iter:BIter<'a>) -> Self{
        Scanner{
            Cmp1:cmp1,
            Cmp2:cmp2,
            iter:iter,
            keyEnd: keyEnd,
            keyStart: keyStart,
        }
    }

    pub fn Valid(&self)-> bool {
            if self.iter.Valid() == false
            {
                return false;
            }
            let (key,_) = self.iter.Deref();
            return crate::btree::scan::comp::cmpOK(key, &self.keyEnd, &self.Cmp2);
    }

    pub fn Deref(&self, rec: &mut Record) {
            let (key,val) = self.iter.Deref();
            if (val.len() > 0) {
                rec.deencodeKey(key);
                rec.decodeValues(&val.to_vec());
            }
    }

    pub fn Next(&mut self) {
        assert!(self.Valid());
        if self.Cmp1.value() > 0 {
            _ = self.iter.Next();
        } else {
            _ = self.iter.Prev();
        }
    }
}