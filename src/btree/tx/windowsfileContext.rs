use std::collections::HashMap;
use std::ffi::{CString};

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

use super::txinterface::{TxContent};

pub struct WindowsFileContext {
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

impl TxContent for WindowsFileContext{

    fn open(&mut self)->Result<(),crate::btree::kv::ContextError> {
        self.masterload()
    }

    fn save(&mut self,updats:&HashMap<u64,Option<BNode>>)->Result<(), crate::btree::kv::ContextError> {
        if let Err(err) = self.writePages(updats)
        {
            return Err(err);
        }
        else {
            Ok(())
        }
    }
    
    fn begin(&mut self)->Result<super::txwriter::txwriter,crate::btree::BTreeError> {
        todo!()
    }
    
    fn commmit(&mut self, tx:&mut super::txwriter::txwriter)->Result<(),crate::btree::BTreeError> {
        todo!()
    }
    
    fn abort(&mut self,tx:&mut super::txwriter::txwriter) {
        todo!()
    }
    
    fn beginread(&mut self)->Result<super::txreader::TxReader,crate::btree::BTreeError> {
        todo!()
    }
    
    fn endread(&mut self, reader:& super::txreader::TxReader) {
        todo!()
    }

}

impl WindowsFileContext{

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
            freehead:0
        })
    }

    fn writePages(&mut self,updates:&HashMap<u64,Option<BNode>>)->Result<(),ContextError>{

        let nPages: usize = (self.pageflushed + self.nappend as u64) as usize;
        self.extendPages(nPages as i64);

        for entry in updates
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

        let ret = self.syncFile();
        if let Err(err) = ret
        {
            return Err(err);
        }

        self.pageflushed += self.nappend as u64;
        self.nfreelist = 0;
        self.nappend = 0;

        self.masterStore();
        let ret = self.syncFile(); 

        Ok(())
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


            let mut newNode = BNode::new(BTREE_PAGE_SIZE);
            newNode.flnSetHeader(0, 0);
            newNode.flnSetTotal(0);

            unsafe {
                let buffer = self.lpBaseAddress as *mut u8;
                for i  in 0..BTREE_PAGE_SIZE
                {
                    *buffer.add(BTREE_PAGE_SIZE*2 + i) = newNode.data()[i];
                }
            }

            self.freehead = 2;

            if self.root == 0 {
                let mut root = BNode::new(BTREE_PAGE_SIZE);
                root.set_header(crate::btree::BNODE_LEAF, 1);
                root.node_append_kv(0, 0, &[0;1], &[0;1]);
                unsafe {
                    let buffer = self.lpBaseAddress as *mut u8;
                    for i  in 0..BTREE_PAGE_SIZE
                    {
                        *buffer.add(BTREE_PAGE_SIZE + i) = root.data()[i];
                    }
                }
            }
            self.root = 1;

            self.pageflushed = 3;
            self.nfreelist = 0;
            self.nappend = 0;

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
            self.freehead = freehead;
        }

       Ok(())
    }


    // update the master page. it must be atomic.
    fn masterStore(&mut self) {
        unsafe {
            let buffer = self.lpBaseAddress as *mut u8;
            
            let mut data: [u8;40] = [0;40];
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

            for i in 0..40
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