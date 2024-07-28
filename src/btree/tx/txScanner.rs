use crate::btree::{scan::comp::OP_CMP, table::record::Record, BTreeError};

use super::{txbiter::TxBIter, txinterface::TxReaderInterface, txwriter::txwriter};


pub struct TxScanner<'a>{
    // the range, from Key1 to Key2
    Cmp1: OP_CMP,
    Cmp2: Option<OP_CMP>,
    iter: TxBIter<'a>,
    indexNo : i16,
    keyEnd: Option<Vec<u8>>,
    keyStart: Vec<u8>,
}

impl<'a> TxScanner<'a> {
   
   pub fn new(indexNo:i16,cmp1: OP_CMP, cmp2: Option<OP_CMP>,keyStart:Vec<u8>,keyEnd:Option<Vec<u8>>,iter:TxBIter<'a>) -> Self{
    TxScanner{
           indexNo:indexNo,
           Cmp1:cmp1,
           Cmp2:cmp2,
           iter:iter,
           keyEnd: keyEnd,
           keyStart: keyStart,
       }
   }

   pub fn Valid(&self)-> bool {
           if self.iter.Valid() == true
           {
                if self.Cmp2.is_some()
                {
                let (key,_) = self.iter.Deref();
                return crate::btree::scan::comp::cmpOK(key, &self.keyEnd.as_ref().unwrap(), &self.Cmp2.unwrap());
                }
                else {
                    return true;
                }
            }
           else {
               return false;
           }

   }

   pub fn Deref(&self,db:&dyn TxReaderInterface, rec: &mut Record)-> Result<(),BTreeError> {
           let (key,val) = self.iter.Deref();
           if self.indexNo < 0
           {
               if (val.len() > 0) {
                   rec.deencodeKey(key);
                   rec.decodeValues(&val.to_vec());
               }
               return Ok(());
           }
           else {
               // secondary index
               // The "value" part of the KV store is not used by indexes
               assert!(val.len() == 1);
               // decode the primary key first
               rec.decodeKeyPartrial(self.indexNo as usize, &key);
               let ret = db.dbGet(rec);
               if let Ok(v) = ret
               {
                   if v == true
                   {
                       return Ok(());
                   }
                   else
                   {
                       return Err(BTreeError::RecordNotFound);
                   }
               }
               return Err(BTreeError::RecordNotFound);
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