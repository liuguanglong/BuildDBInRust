
use std::collections::HashMap;
use std::ffi::{CString};
use std::sync::{Arc, RwLock};

use crate::btree::kv::node::BNode;
use crate::btree::kv::nodeinterface::{BNodeFreeListInterface, BNodeReadInterface, BNodeWriteInterface};
use crate::btree::kv::{ContextError, BTREE_PAGE_SIZE, DB_SIG};
#[cfg(windows)]extern crate ntapi;
use ntapi::ntmmapi::{NtExtendSection,NtUnmapViewOfSection,NtMapViewOfSection,NtCreateSection,ViewUnmap,};
use winapi::shared::ntdef::{HANDLE, LARGE_INTEGER, NT_SUCCESS};

#[cfg(windows)] extern crate winapi;
use winapi::um::fileapi::{CreateFileA,GetFileSizeEx,FlushFileBuffers};
use winapi::um::processthreadsapi::GetCurrentProcess;
use winapi::um::handleapi::CloseHandle;
use winapi::um::memoryapi::FlushViewOfFile;

use std::ptr::{null_mut};
use winapi::um::handleapi::INVALID_HANDLE_VALUE;
use winapi::um::winnt::{ FILE_ATTRIBUTE_NORMAL, GENERIC_READ, GENERIC_WRITE, MEM_RESERVE, PAGE_READWRITE, SECTION_EXTEND_SIZE, SECTION_MAP_READ, SECTION_MAP_WRITE, SEC_COMMIT};
use winapi::um::fileapi::{CREATE_NEW, OPEN_EXISTING,OPEN_ALWAYS};

use super::txdemo::Shared;

#[derive(Debug)]
pub struct Mmap{
    pub ptr:*mut u8,
    pub writer:Shared<()>,
}
unsafe impl Send for Mmap {}
unsafe impl Sync for Mmap {}

#[derive(Debug)]
pub struct WinMmap {
    fHandle: HANDLE,
    hSection: HANDLE,
    mmap:Arc<RwLock<Mmap>>,
    //lpBaseAddress: *mut winapi::ctypes::c_void,
    fileSize:i64,
    dwPageSize:usize,
}


impl Drop for WinMmap {
    fn drop(&mut self) {
        unsafe {
            // 释放映射的内存
            let status = NtUnmapViewOfSection(GetCurrentProcess(), self.mmap.read().unwrap().ptr as *mut winapi::ctypes::c_void);
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

impl WinMmap{

    #[inline]
    pub fn as_ptr(&self) -> *const u8 {
        self.mmap.read().unwrap().ptr
    }

    #[inline]
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.mmap.read().unwrap().ptr
    }

    #[inline]
    pub fn size(&self) -> usize {
        self.fileSize as usize
    }

    pub fn getMmap(&self) -> Arc<RwLock<Mmap>>
    {
        self.mmap.clone()
    }

    pub fn new(fileName: &[u8], pageSize: usize, maxPageCount: usize) -> Result<Self,ContextError> {

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

        let buffer = lpZwMapping as *mut u8;
        let mmap = Mmap { ptr: buffer, writer: Shared::new(())};
        Ok(WinMmap {
            fHandle:handle,
            hSection : hSection,
            //lpBaseAddress : lpZwMapping,
            dwPageSize:pageSize,
            fileSize:filesize,
            mmap: Arc::new(RwLock::new(mmap))
        })
    }

    pub fn syncFile(&mut self) -> Result<(),ContextError> {

        unsafe{
            if  FlushViewOfFile(self.mmap.read().unwrap().ptr as *mut winapi::ctypes::c_void, 0) == 0 {
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

    pub fn extendPages(&mut self,npages:i64) -> Result<(),ContextError>{

        let mut filePages: i64 = self.fileSize / BTREE_PAGE_SIZE as i64;
        if filePages >= npages 
        {
            return Ok(());
        }

        let mut nPageExtend: i64 = 0;
        while (filePages < npages) {
            let mut inc = filePages/ 8;
            if (inc < 1) {
                inc = 1;
            }
            nPageExtend += inc;
            filePages += inc;
        }

        if let Err(er) = self.extendFile(nPageExtend as usize)
        {
            return Err(er);
        }
        else {
            
            return Ok(());
        }
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

}

#[cfg(test)]
mod tests {

    use std::{borrow::BorrowMut, hash::Hash, sync::{Arc, RwLock}, time::Duration};
    use rand::Rng;
    use super::*;
    use std::thread;

    use super::*;

    #[test]
    fn test_FileContent()
    {
        let mut context = WinMmap::new("c:/temp/rustdb.dat".as_bytes(),4096,10);
        if let Ok(mut context) = context
        {
            let mut mmap = context.getMmap();

            let mut handles = vec![];

            for i in 0..10 {
                //let reader = context.beginread();
                let ct =  mmap.clone();
                let handle = thread::spawn(move || {
                    read(i, ct)
                });
                handles.push(handle);
            }

            for i in 1..10 {
                //let reader = context.beginread();
                let ct =  mmap.clone();
                let handle = thread::spawn(move || {
                    write(i, ct)
                });
                handles.push(handle);
            }

            for handle in handles {
                handle.join().unwrap();
            }

        }
    }

    fn read(i:usize, context:Arc<RwLock<Mmap>>)
    {
        let mut rng = rand::thread_rng();
        let random_number: u64 = rng.gen_range(2..30);
        
        let mut mmap = context.read().unwrap();
        println!("Begin Read Value:{}",i);       
        thread::sleep(Duration::from_millis(random_number));
        unsafe {
            let data = *mmap.ptr.add(i);
            println!("Read Char in {} is {:?}!", i,data);
        }
        println!("End Read Value:{}",i);       
    }

    fn write(i:u8, context:Arc<RwLock<Mmap>>)
    {
        let mut rng = rand::thread_rng();
        let random_number: u64 = rng.gen_range(2..15);
        let mut mmap = context.read().unwrap();
        let lock = mmap.writer.lock().unwrap();
        println!("Begin write Value:{}",i);       
        thread::sleep(Duration::from_millis(random_number));
        unsafe {
            *mmap.ptr.add(i as usize) = i.to_be_bytes()[0];
            println!("Write Char in {}!", i);
        }
        drop(lock);
        println!("End write Value:{}",i);       
    }

}