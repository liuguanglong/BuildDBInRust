use crate::btree::{btree::btree::BTree, scan::biter::BIter, BTreeError};


pub struct Scanner<'a>{
     // the range, from Key1 to Key2
     Cmp1: OP_CMP,
     Cmp2: OP_CMP,
     Key1: &Record,
     Key2: &Record,
     //Internal
     tdef: &TableDef,
     iter: Option<BIter>,
     keyEnd: Vec<u8>,
     keyStart:Vec<u8>,
}

impl<'a> Scanner<'a> {
    
    pub fn new(cmp1: OP_CMP, cmp2: OP_CMP, key1: &Record, key2: &Record) -> Self{
        Scanner{
            Cmp1:cmp1,
            Cmp2:cmp2,
            Key1:key1,
            Key2:key2,
            tdef:key1.def,
            iter:None,
            keyEnd: Vec::new(),
            keyStart: Vec::new(),
        }
    }


    pub fn Seek(&self, db:&BTree)->Result<Scanner,BTreeError> {
        // sanity checks
        if self.Cmp1.value() > 0 && self.Cmp2.value() < 0 
        {} 
        else if self.Cmp2.value() > 0 && self.Cmp1.value() < 0 
        {} 
        else {
            return Err(BTreeError::BadArrange);
        }

        let bCheck1 = self.tdef.checkPrimaryKey(self.Key1);
        if  bCheck1 == false {
            return Err(BTreeError::KeyError);
        }
        let bCheck2 = self.tdef.checkPrimaryKey(self.Key2);
        if  bCheck2 == false {
            return ScannerError.KeyError;
        }

        self.Key1.encodeKey(self.tdef.Prefix, &self.keyStart);
        self.Key2.encodeKey(self.tdef.Prefix, &self.keyEnd);

        self.iter = Some(db.Seek(self.keyStart.items, self.Cmp1));
        self
    }

    pub fn Valid(&self)-> bool {
        if let Some(&iter) = self.iter
        {
            let (key,_) = self.iter.Deref();
            return biter.cmpOK(key, self.keyEnd.items, self.Cmp2);
        }
        else {
            return false;
        }
    }

    pub fn Deref(&self, rec: &mut Record) {
        let (key,val) = self.iter.Deref();
        if (val.len() > 0) {
            rec.deencodeKey(key);
            rec.decodeValues(val);
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