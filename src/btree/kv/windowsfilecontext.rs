use std::ffi::{CString};

use crate::btree::kv::nodeinterface::{BNodeFreeListInterface, BNodeReadInterface};
use crate::btree::kv::{BTREE_PAGE_SIZE, DB_SIG};
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
use winapi::um::winnt::{FILE_ATTRIBUTE_NORMAL, GENERIC_READ, GENERIC_WRITE,SECTION_EXTEND_SIZE,SECTION_MAP_READ,SECTION_MAP_WRITE,PAGE_READWRITE,SEC_COMMIT,MEM_RESERVE};
use winapi::um::fileapi::{CREATE_NEW, OPEN_EXISTING,OPEN_ALWAYS};
use winapi::shared::minwindef::DWORD;

use super::contextinterface::KVContextInterface;
use super::node::BNode;
use super::ContextError;

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
    freehead: u64 //head of freeelist
}

// impl KVContextInterface for  WindowsFileContext {

//     fn add(&mut self,node:BNode) -> u64 
//     {
//         self.idx += 1;
//         self.pages.insert(self.idx,node);
//         return self.idx; 
//     }

//     fn get(&self,key:u64) -> Option<BNode>
//     {
//         let node = self.pages.get(&key);
//         match node
//         {
//             Some(x) => {
//                 Some(x.copy())    
//             },
//             None =>  None,
//         }
//     }

//     fn del(&mut self,key:u64)-> Option<BNode>
//     {
//         self.pages.remove(&key)
//     }
   
//     fn open(&mut self){

//     }
//     fn close(&mut self){

//     }
//     fn get_root(&self)->u64{
//         return self.root;
//     }
//     fn set_root(&mut self,ptr:u64){
//         self.root = ptr;
//     }
//     fn save(&mut self){

//     }


// }

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
        })
    }

    fn set(&mut self,content:&[u8]){
        unsafe{
            let buffer = self.lpBaseAddress as *mut u8;
            for i  in 0..content.len()
            {
                *buffer.add(i) = content[i];
            }
        }
    }

    fn get(&self,begin:usize,end:usize) -> Vec<u8> {
        let mut ret:Vec<u8> = Vec::new();
        unsafe {
            for i in begin..end
            {
                let buffer = self.lpBaseAddress as *mut u8;
                ret.push(*buffer.add(i))
            }
        }
        return ret;
    }

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
            dbContext.set("1234567890abcdefghighk".as_bytes());
            let ret = dbContext.extendFile(20);
            if let Ok(_) = ret
            {
                let ret = dbContext.get(0,15);
                println!("Key:{} \n", String::from_utf8(ret).unwrap());
            }
            let ret = dbContext.syncFile();
            assert!(ret.is_ok());
        }
    }
    
}