use crate::btree::scan::biter::BIter;
use crate::btree::scan::comp::OP_CMP;

pub trait ScanInterface {
    fn SeekLE(&self, key:&[u8]) -> BIter;
    fn Seek(&self, key:&[u8], cmp:OP_CMP) -> BIter;
}
