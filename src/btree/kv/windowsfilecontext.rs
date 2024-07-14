use std::collections::HashMap;
use std::ffi::{CString};

use crate::btree::kv::nodeinterface::{BNodeFreeListInterface, BNodeReadInterface};
use crate::btree::kv::{BTREE_PAGE_SIZE, DB_SIG, FREE_LIST_CAP};
#[cfg(windows)]extern crate ntapi;
use ntapi::ntmmapi::{NtExtendSection,NtUnmapViewOfSection,NtMapViewOfSection,NtCreateSection,ViewUnmap,};
use winapi::shared::ntdef::PHANDLE;
use winapi::shared::ntdef::{HANDLE, LARGE_INTEGER, NT_SUCCESS, NULL};

#[cfg(windows)] extern crate winapi;
use winapi::um::memoryapi::MapViewOfFile;
use winapi::um::fileapi::{CreateFileA,GetFileSizeEx,FlushFileBuffers};
use winapi::um::processthreadsapi::GetCurrentProcess;
use winapi::um::handleapi::CloseHandle;
use winapi::um::memoryapi::FlushViewOfFile;

//use winapi::um::winnt::HANDLE;

use std::ptr::{null, null_mut};
use winapi::um::handleapi::INVALID_HANDLE_VALUE;
use winapi::um::winnt::{ENLISTMENT_INFORMATION_CLASS, FILE_ATTRIBUTE_NORMAL, GENERIC_READ, GENERIC_WRITE, MEM_RESERVE, PAGE_READWRITE, SECTION_EXTEND_SIZE, SECTION_MAP_READ, SECTION_MAP_WRITE, SEC_COMMIT};
use winapi::um::fileapi::{CREATE_NEW, OPEN_EXISTING,OPEN_ALWAYS};
use winapi::shared::minwindef::DWORD;

use super::contextinterface::KVContextInterface;
use super::node::BNode;
use super::nodeinterface::{BNodeWriteInterface, FreeListInterface};
use super::{ContextError, BNODE_FREE_LIST};

struct WindowsFileContext {
    fHandle: HANDLE,
    hSection: HANDLE,
    lpBaseAddress: *mut winapi::ctypes::c_void,
    fileSize:i64,
    dwPageSize:usize,
    root: u64,
    pageflushed: u64, // database size in number of pages
    nfreelist: u16, //number of pages taken from the free list
    nappend: u16, //number of pages to be appended
    freehead: u64, //head of freeelist
    // newly allocated or deallocated pages keyed by the pointer.
    // nil value denotes a deallocated page.
    updates: HashMap<u64, Option<BNode>>,
}

impl FreeListInterface for WindowsFileContext{
    fn GetFreeNode(&self, topN: u16)-> Result<u64,ContextError> {
        assert!(topN >= 0 && topN < self.TotalFreeNode().unwrap() as u16);
        let mut count = topN;

        let curNode = self.get(self.freehead);
        if let None = curNode 
        {
            return Err(ContextError::RootNotFound);
        };
        
        let mut curNode = curNode.unwrap();
        while (curNode.flnSize() <= count) {

            count -= curNode.flnSize();

            let next = curNode.flnNext();
            assert!(next != 0);
            let tmp = self.get(next);  
            if let Some(n) = tmp 
            {
                curNode = n;
            }
            else {
                return Err(ContextError::NodeNotFound);
            }
        }

        return Ok(curNode.flnPtr( curNode.flnSize() as usize - count as usize - 1));
    }

    fn TotalFreeNode(&self)-> Result<u64,ContextError> {

        let node = self.get(self.freehead);
        if let Some(root) = node
        {
            return Ok(root.flnGetTotal());
        }

        Err(ContextError::RootNotFound)
    }

    fn UpdateFreeList(&mut self, popn: u16, freed:&Vec<u64>) -> Result<(),ContextError>{

        assert!(popn <= self.TotalFreeNode().unwrap() as u16);
        //std.debug.print("Total:{d} PopN:{d} FreeList Len :{d}\n", .{ self.Total(), popn, freed.len });

        if popn == 0 && freed.len() == 0
        {
            return Ok(());

        }

        // prepare to construct the new list
        let mut total = self.TotalFreeNode().unwrap();
        let mut count = popn;
        let mut listReuse:Vec<u64> = Vec::new();
        let mut listFreeNode:Vec<u64> = Vec::new();

        for i in 0..freed.len() 
        {
            listFreeNode.push(freed[i]);
        }

        while self.freehead != 0 && listReuse.len() * FREE_LIST_CAP < listFreeNode.len() 
        {
            let node = self.get(self.freehead);
            if let None = node 
            {
                return Err(ContextError::RootNotFound);
            };

            listFreeNode.push(self.freehead);
            //std.debug.print("Head Ptr:{d}  Size {d}\n", .{ self.head, flnSize(node1) });
            let node = node.unwrap();
            if count >= node.flnSize()
            {
                // remove all pointers in this node
                count -= node.flnSize();
            } else {
                // remove some pointers
                let mut remain = node.flnSize() - count;
                count = 0;

                // reuse pointers from the free list itself
                while remain > 0 && listReuse.len() * FREE_LIST_CAP < listFreeNode.len() + remain as usize
                {
                    //std.debug.print("Handle Remain.\n", .{});
                    remain -= 1;
                    listReuse.push(node.flnPtr(remain as usize));
                }

                // move the node into the `freed` list
                for idx in 0..remain as usize
                {
                    //std.debug.print("Handle Freed. {d}\n", .{idx});
                    listFreeNode.push(node.flnPtr(idx));
                }
            }
            total -= node.flnSize() as u64;
            self.freehead = node.flnNext();
        }

        let newTotal = total + listFreeNode.len() as u64;
        assert!(listReuse.len() * FREE_LIST_CAP >= listReuse.len() || self.freehead == 0);
        self.flPush(&mut listFreeNode, &mut listReuse);

        let mut headnode = self.get(self.freehead);
        if let Some( mut h) = headnode{
            h.flnSetTotal(newTotal);            
        } 

        Ok(())
    }

    fn flPush(&mut self, listFreeNode: &mut Vec<u64>, listReuse:  &mut Vec<u64>) {

        while listFreeNode.len() > 0 
        {
            let mut newNode = BNode::new(BTREE_PAGE_SIZE);

            //construc new node
            let mut size: usize = listFreeNode.len();
            if size > FREE_LIST_CAP
            {
                size = FREE_LIST_CAP;
            }

            newNode.flnSetHeader(size as u16, self.freehead);

            for idx in 0..size 
            {
                let ptr = listFreeNode.pop().unwrap();
                newNode.flnSetPtr( idx, ptr);
                //std.debug.print("Free node Ptr:{d}\n", .{ptr});
            }

            if listReuse.len() > 0 
            {
                //reuse a pointer from the list
                let ptrHead = listReuse.pop().unwrap();
                self.freehead = ptrHead;
                //std.debug.print("Reuse Ptr {d} \n", .{self.head});['']
                self.useNode(self.freehead, &newNode);
            } else {
                self.freehead = self.appendNode(&newNode);
                //std.debug.print("New Head Ptr {d} \n", .{self.head});
            }
        }

        assert!(listReuse.len() == 0);

    }

}

impl KVContextInterface for  WindowsFileContext {

    fn add(&mut self,node:BNode) -> u64 
    {
        let mut ptr: u64 = 0;
        if self.nfreelist < self.TotalFreeNode().unwrap() as u16 
        {
            // reuse a deallocated page
            ptr = self.GetFreeNode(self.nfreelist).unwrap();
            self.nfreelist += 1;
        } else {
            ptr = self.pageflushed + self.nappend as u64;
            self.nappend += 1;
        }

        let newNode = node.copy();
        self.updates.insert(ptr, Some(newNode));
        return ptr;
    }

    fn get(&self,key:u64) -> Option<BNode>
    {
        let node = self.updates.get(&key);
        match node
        {
            Some(Some(x)) => {
                Some(x.copy())    
            },
            Some(None) =>{
                None
            },
            Other=>
            {
                if let Ok(n) = self.getMapped(key)
                {
                    Some(n)
                }
                else {
                    None
                }
            },
        }
    }

    fn del(&mut self,key:u64)-> Option<BNode>
    {
        let node = self.get(key);
        self.updates.insert(key, None);
        node
    }
   
    fn open(&mut self)->Result<(),ContextError>{
        self.masterload()
    }

    fn close(&mut self){

    }
    fn get_root(&self)->u64{
        return self.root;
    }
    fn set_root(&mut self,ptr:u64){
        self.root = ptr;
    }
    fn save(&mut self)->Result<(), ContextError>{
        if let Err(err) = self.writePages()
        {
            return Err(err);
        }
        self.syncPages()
    }


}

impl Drop for WindowsFileContext {
    fn drop(&mut self) {
        unsafe {
            // 释放映射的内存
            let status = NtUnmapViewOfSection(GetCurrentProcess(), self.lpBaseAddress);
            if !NT_SUCCESS(status) {
                eprintln!("Failed to unmap view of section");
            }
            if self.hSection != INVALID_HANDLE_VALUE {
                CloseHandle(self.hSection);
            }
            if self.fHandle != INVALID_HANDLE_VALUE {
                CloseHandle(self.fHandle);
            }
        }
    }
}

impl WindowsFileContext {
    // 构造函数
    fn new(fileName: &[u8], pageSize: usize, maxPageCount: usize) -> Result<Self,ContextError> {

        let name = CString::new(fileName).expect("CString::new failed");
        let mut SectionSize: LARGE_INTEGER = unsafe { std::mem::zeroed() };
        let mut handle: HANDLE;
        let mut hSection: HANDLE = null_mut();
        let mut lpZwMapping: *mut winapi::ctypes::c_void = null_mut();
        let mut view_size: usize =  pageSize * maxPageCount; 
        let mut filesize:i64 = 0;

        unsafe {
            handle = CreateFileA(name.as_ptr(), GENERIC_READ | GENERIC_WRITE, 0, null_mut(), OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, null_mut());
            //check hanlde
            if handle == INVALID_HANDLE_VALUE {
                eprintln!("Failed to open file");
                return  Err(ContextError::OpenFileError);
            } 

            //get File Size
            let mut file_size: LARGE_INTEGER = std::mem::zeroed();
            let success = GetFileSizeEx(handle, &mut file_size);
            if (success == 0) {
                eprintln!("Failed to get file size");
                return  Err(ContextError::GetFileSizeError);            
            }
            filesize = file_size.QuadPart().abs();

            *SectionSize.QuadPart_mut()= pageSize as i64;
            let status = NtCreateSection(  &mut hSection, SECTION_EXTEND_SIZE | SECTION_MAP_READ | SECTION_MAP_WRITE, null_mut(), 
                    &mut SectionSize, PAGE_READWRITE, SEC_COMMIT, handle);

            if !NT_SUCCESS(status) {
                eprintln!("Failed to create section");
                return Err(ContextError::CreateNTSectionError);
            }

            // 映射部分
            let status = NtMapViewOfSection(
                hSection,
                winapi::um::processthreadsapi::GetCurrentProcess(),
                &mut lpZwMapping,
                0,
                0,
                null_mut(),
                &mut view_size,
                ViewUnmap,
                MEM_RESERVE,
                PAGE_READWRITE,
            );

            if !NT_SUCCESS(status) {
                eprintln!("Failed to map view of section");
                return Err(ContextError::MapSectionViewError);
            }

            if (lpZwMapping == INVALID_HANDLE_VALUE) {
                eprintln!("Failed to ap view of section");
                return Err(ContextError::MapSectionViewError);
            }
        }
 
        Ok(WindowsFileContext {
            fHandle:handle,
            hSection : hSection,
            lpBaseAddress : lpZwMapping,
            fileSize:filesize,
            dwPageSize:pageSize,
            root:0,
            pageflushed:0,
            nfreelist:0,
            nappend:0,
            freehead:0,
            updates: HashMap::new(),
        })
    }

    pub fn useNode(&mut self, ptr: u64, bnode: &BNode) {

        let newNode = bnode.copy();
        self.updates.insert(ptr, Some(newNode));
    }

    pub fn appendNode(&mut self, bnode: &BNode)-> u64 {
        let newNode = bnode.copy();

        let ptr = self.pageflushed + self.nappend as u64;
        self.nappend += 1;

        self.updates.insert(ptr, Some(newNode));

        return ptr;
    }

    pub fn getMapped(&self, ptr: u64)-> Result<BNode,ContextError> {
        if (ptr > self.pageflushed + self.nappend as u64) {
            return Err(ContextError::NodeNotFound);
        }
        let offset = ptr as usize * BTREE_PAGE_SIZE;

        let mut newNode = BNode::new(BTREE_PAGE_SIZE);
        unsafe {
            let buffer = self.lpBaseAddress as *mut u8;
            newNode.copy_Content(buffer,BTREE_PAGE_SIZE);
        }
        Ok(newNode)
    }

    fn writePages(&mut self)->Result<(),ContextError>{

        let mut listFreeNode:Vec<u64> = Vec::new();
        for entry in &self.updates
        {
            if let Some(v) = entry.1 
            {
                let ptr = entry.0;
                listFreeNode.push(*ptr);
            }
        }

        self.UpdateFreeList(self.nfreelist, &listFreeNode);
        let nPages: usize = (self.pageflushed + self.nappend as u64) as usize;
        self.extendFile(nPages);

        for entry in &self.updates
        {
            if let Some(v) = entry.1 
            {
                let ptr:u64 = *entry.0;
                let offset:usize = ptr as usize * BTREE_PAGE_SIZE;
                unsafe {
                    let buffer = self.lpBaseAddress as *mut u8;
                    for i in 0..BTREE_PAGE_SIZE
                    {
                        *buffer.add(i + offset as usize) = v.data()[i];
                    }
                }
            }
        }
        Ok(())
    }

    fn syncPages(& mut self)-> Result<(),ContextError> {
        
        let ret = self.syncFile();
        if let Err(err) = ret
        {
            return Err(err);
        }

        self.updates.clear();
        self.pageflushed += self.nappend as u64;
        self.nfreelist = 0;
        self.nappend = 0;

        self.masterStore();
        let ret = self.syncFile(); 
        ret
    }

    // fn set(&mut self,content:&[u8]){
    //     unsafe{
    //         let buffer = self.lpBaseAddress as *mut u8;
    //         for i  in 0..content.len()
    //         {
    //             *buffer.add(i) = content[i];
    //         }
    //     }
    // }

    // fn get(&self,begin:usize,end:usize) -> Vec<u8> {
    //     let mut ret:Vec<u8> = Vec::new();
    //     unsafe {
    //         for i in begin..end
    //         {
    //             let buffer = self.lpBaseAddress as *mut u8;
    //             ret.push(*buffer.add(i))
    //         }
    //     }
    //     return ret;
    // }

    // the master page format.
    // it contains the pointer to the root and other important bits.
    // | sig | btree_root | page_used |
    // | 16B | 8B | 8B |
    fn masterload(&mut self)->Result<(),ContextError>
    {
        //Init Db file
        if self.fileSize == 0 {
            if let Err(er) = self.extendFile(3){
                return Err(ContextError::ExtendNTSectionError);
            };

            self.pageflushed = 2;
            self.nfreelist = 0;
            self.nappend = 0;
            self.root = 0;

            let mut newNode = BNode::new(BTREE_PAGE_SIZE);
            newNode.flnSetHeader(0, 0);
            newNode.flnSetTotal(0);

            unsafe {
                let buffer = self.lpBaseAddress as *mut u8;
                for i  in 0..BTREE_PAGE_SIZE
                {
                    *buffer.add(BTREE_PAGE_SIZE + i) = newNode.data()[i];
                }
            }

            self.freehead = 1;
            self.masterStore();
            let ret = self.syncFile();
            if let Err(err) = ret
            {
                return Err(err);
            };

            return Ok(());
        }

        //Load Db File
        unsafe {
            let buffer = self.lpBaseAddress as *mut u8;
            for i in 0..16
            {
                if *buffer.add(i) != DB_SIG[i]
                {
                    return Err(ContextError::NotDataBaseFile);
                }
            }

            let mut pos: usize = 16;
            let mut content:[u8;8] = [0;8];
            
            for i in 0..8
            {
                content[i] = *buffer.add(i+ pos);
            }
            let root = u64::from_le_bytes(content[0..8].try_into().unwrap());

            pos = 24;
            for i in 0..8
            {
                content[i] = *buffer.add(i+ pos);
            }
            let used = u64::from_le_bytes(content[0..8].try_into().unwrap());

            pos = 32;
            for i in 0..8
            {
                content[i] = *buffer.add(i+ pos);
            }
            let freehead = u64::from_le_bytes(content[0..8].try_into().unwrap());

            let mut bad: bool = !(1 <= used && used <= (self.fileSize as u64)/ BTREE_PAGE_SIZE as u64);
            bad = bad || !(0 <= root && root < used);
            if (bad == true) {
                return Err(ContextError::LoadDataException);
            }
    
            self.root = root;
            self.pageflushed = used;
            self.nfreelist = 0;
            self.nappend = 0;    
        }

       Ok(())
    }


    // update the master page. it must be atomic.
    fn masterStore(&mut self) {
        unsafe {
            let buffer = self.lpBaseAddress as *mut u8;
            
            let mut data: [u8;32] = [0;32];
            for i in 0..16
            {
                data[i] = DB_SIG[i];
            }

            let mut pos: usize = 16;
            data[pos..pos+8].copy_from_slice(&self.root.to_le_bytes());

            pos = 24;
            data[pos..pos+8].copy_from_slice(&self.pageflushed.to_le_bytes());
    
            pos = 32;
            data[pos..pos+8].copy_from_slice(&self.freehead.to_le_bytes());

            for i in 0..32
            {
                *buffer.add(i) = data[i];
            }
        }
    }

    pub fn syncFile(&mut self) -> Result<(),ContextError> {

        unsafe{
            if  FlushViewOfFile(self.lpBaseAddress, 0) == 0 {
                eprintln!("Failed to flush view of file");
                return Err(ContextError::FlushViewofFileError);
            }

            if  FlushFileBuffers(self.fHandle) == 0 {
                eprintln!("Failed to flush file buffers.");
                return Err(ContextError::FlushFileBUffersError);
            }
        }
        Ok(())
    }

    pub fn extendFile(&mut self, pageCount: usize) -> Result<(),ContextError>{

        let mut SectionSize: LARGE_INTEGER = unsafe { std::mem::zeroed() };
        unsafe {
            *SectionSize.QuadPart_mut() = self.fileSize + (pageCount * self.dwPageSize) as i64;

            let statusExtend = NtExtendSection(self.hSection, &mut SectionSize);
            if !NT_SUCCESS(statusExtend) {
                eprintln!("Failed ExtendSection.\n");
                return Err(ContextError::ExtendNTSectionError);
            }                
            self.fileSize = SectionSize.QuadPart().abs();
        }
        Ok(())
    }

    fn print(&self) {
        // for i in 0..1024 {
        //     if  i > 0 {
        //         print!("{:02x} ", self.data[i]);
        //     }
        //     if i % 50 == 0
        //     {
        //         println!();
        //     }
        // }
        println!();
        // println!("{:?}", self.data);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_FileContent()
    {
        let mut context = WindowsFileContext::new("c:/temp/rustfile1.txt".as_bytes(),4096,10);
        if let Ok(mut dbContext) = context
        {
            println!("File Size:{}",dbContext.fileSize);
            //dbContext.set("1234567890abcdefghighk".as_bytes());
            let ret = dbContext.extendFile(20);
            if let Ok(_) = ret
            {
                //let ret = dbContext.get(0,15);
                //println!("Key:{} \n", String::from_utf8(ret).unwrap());
            }
            let ret = dbContext.syncFile();
            assert!(ret.is_ok());
        }
    }
    
}