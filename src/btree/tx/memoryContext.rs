use std::sync::{Arc, RwLock};

use crate::btree::kv::{nodeinterface::BNodeReadInterface, ContextError};

use super::{ shared::Shared, txinterface::MmapInterface, winmmap::Mmap};

unsafe impl Send for memoryContext {}
unsafe impl Sync for memoryContext {}

pub struct memoryContext{
    mmap:Arc<RwLock<Mmap>>,
    fileSize:usize,
    dwPageSize:usize,
    maxMemoryPageCount:usize,
    data:Vec<u8>,
}

impl memoryContext{

    pub fn new(pagesize:usize,maxPageCount:usize) -> Self
    {
        let mut data: Vec<u8> = vec![0; pagesize*maxPageCount];
        let data_ptr: *mut u8 = data.as_mut_ptr();
        
        let mmap = Mmap { ptr: data_ptr, writer: Shared::new(())};
        let mmap =  Arc::new(RwLock::new(mmap));

        memoryContext{
            data:data,
            mmap:mmap,
            dwPageSize:pagesize,
            maxMemoryPageCount:maxPageCount,
            fileSize:0,
        }
    }
}

impl MmapInterface for memoryContext{

    fn getContextSize(&self)->usize {
        self.fileSize
    }

    fn getMmap(&self)->Arc<RwLock<Mmap>> {
        return self.mmap.clone();
    }

    fn extendContext(&mut self,pageCount:usize)->Result<(),crate::btree::kv::ContextError> {
        self.fileSize += pageCount * self.dwPageSize;
        Ok(())
    }

    fn extendPages(&mut self,totalpages:usize) -> Result<(),ContextError>{

        let mut filePages = self.fileSize / self.dwPageSize;
        if filePages >= totalpages 
        {
            return Ok(());
        }

        let mut nPageExtend = 0;
        while (filePages < totalpages) {
            let mut inc = filePages/ 8;
            if (inc < 1) {
                inc = 1;
            }
            nPageExtend += inc;
            filePages += inc;
        }

        if let Err(er) = self.extendContext(nPageExtend)
        {
            return Err(er);
        }
        else {
            
            return Ok(());
        }
    }

    fn syncContext(&mut self) -> Result<(),crate::btree::kv::ContextError> {
        Ok(())
    }
}