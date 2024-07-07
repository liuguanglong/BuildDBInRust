use crate::btree::kv::nodeinterface::BNodeReadInterface;
use crate::btree::HEADER;

pub struct BNodeRef<'a> {
    pub data: &'a [u8],
    pub size: usize,
}

impl <'a> BNodeRef <'a>{
    pub fn new(size:usize,val:&'a [u8]) -> Self {
        BNodeRef {
            data: val,
            size:size
        }
    }
}

impl<'a> BNodeReadInterface for BNodeRef<'a> {

    fn size(&self) ->usize {
        self.size
    }

    fn data(&self) ->&[u8]
    {
        return &self.data;
    }
    fn btype(&self)->u16{
        return u16::from_le_bytes(self.data[0..2].try_into().unwrap());
    }
    fn nkeys(&self) -> u16 {
        return u16::from_le_bytes(self.data[2..4].try_into().unwrap());
    }
    fn get_ptr(&self, idx: usize) -> u64 {
        assert!(idx < self.nkeys().into(), "Assertion failed: idx is large or equal nkeys!");
        let pos:usize = HEADER + 8 * idx;
        let value: u64 = u64::from_le_bytes(self.data[pos..pos + 8].try_into().unwrap());

        return value;
    }

    fn offset_pos(&self, idx: u16)->usize{
        assert!(1 <= idx && idx <= self.nkeys());
        let r =  8 * self.nkeys() + 2 * (idx - 1);
        let value_usize: usize = HEADER +  r as usize;
        return value_usize;
    }

    fn get_offSet(&self,idx:u16) -> u16{
        if idx == 0
        {
            return 0;
        }

        let pos = self.offset_pos(idx);
        return u16::from_le_bytes(self.data[pos..pos+2].try_into().unwrap());
    }
    fn kvPos(&self, idx: u16)-> usize{
        assert!(idx <= self.nkeys());
        let r =  8 * self.nkeys() + 2 * self.nkeys() + self.get_offSet(idx);
        let value_usize: usize = HEADER +  r as usize;
        return value_usize;
    }

    fn get_val(&self, idx: u16)-> &[u8]{
        assert!(idx <= self.nkeys());
        let pos = self.kvPos(idx);
        let klen = u16::from_le_bytes(self.data[pos..pos+2].try_into().unwrap()) as usize;
        let vlen = u16::from_le_bytes(self.data[pos+2..pos+4].try_into().unwrap()) as usize;
        return &self.data[pos+4+klen..pos+4+klen+vlen];
    }

    fn get_key(&self, idx: u16)-> &[u8]{
        assert!(idx <= self.nkeys());
        let pos = self.kvPos(idx);
        let klen = u16::from_le_bytes(self.data[pos..pos+2].try_into().unwrap()) as usize;
        return &self.data[pos+4..pos+4+klen];
    }

    fn nodeLookupLE(&self, key: &[u8])-> u16{
        let count = self.nkeys();
        let mut found:u16 = 0;
        for i in 0..count{
            let k = self.get_key(i);
            let comp = crate::btree::util::compare_arrays(k,key);
            if comp <= 0 {found = i;}
            if comp > 0 { break; } 
        }
        return found;
    }

    fn print(&self) {
        for i in 0..self.size {
            if  i > 0 {
                print!("{:02x} ", self.data[i]);
            }
            if i % 50 == 0
            {
                println!();
            }
        }
        println!();
        // println!("{:?}", self.data);
    }
}

