use std::ffi::{CString};

use crate::btree::kv::nodeinterface::BNodeReadInterface;
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
}

// impl KVContextInterface for WinFileContext {

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

    pub fn syncFile(&mut self) -> Result<(),ContextError> {

        unsafe{
            if (FlushViewOfFile(self.lpBaseAddress, 0) == 0) {
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
        // SectionSize.u_mut().HighPart = self.fileSize + pageCount * self.dwPageSize;
        // SectionSize.u_mut().LowPart = 
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
        let mut context = WindowsFileContext::new("c:/temp/rustfile.txt".as_bytes(),4096,10);
        if let Ok(mut dbContext) = context
        {
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