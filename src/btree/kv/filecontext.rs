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

struct FileContext {
    fHandle: HANDLE,
    hSection: HANDLE,
    data: [u8; 1024],
    lpBaseAddress: *mut winapi::ctypes::c_void,
    fileSize:i64,
    dwPageSize:usize,
}

impl Drop for FileContext {
    fn drop(&mut self) {
        unsafe {
            // 释放映射的内存
            let status = NtUnmapViewOfSection(GetCurrentProcess(), self.lpBaseAddress);
            if !NT_SUCCESS(status) {
                eprintln!("Failed to unmap view of section");
            } else {
                println!("View unmapped successfully");
            }

            if self.hSection != INVALID_HANDLE_VALUE {
                CloseHandle(self.hSection);
                println!("Mapping Section closed in Drop");
            }
            if self.fHandle != INVALID_HANDLE_VALUE {
                CloseHandle(self.fHandle);
                println!("File handle closed in Drop");
            }
        }
    }
}

impl FileContext {
    // 构造函数
    fn new(fileName: &[u8], pageSize: usize, maxPageCount: usize) -> Self {

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
                println!("Failed to create file");
            } else {
                println!("File created successfully");
                // winapi::um::handleapi::CloseHandle(handle);
            }

            //get File Size
            let mut file_size: LARGE_INTEGER = std::mem::zeroed();
            let success = GetFileSizeEx(handle, &mut file_size);
            if (success == 0) {
                println!("Failed to get file size. \n");            
            }
            filesize = file_size.QuadPart().abs();
            println!("file size: {}\n", file_size.QuadPart());

            *SectionSize.QuadPart_mut()= pageSize as i64;
            let status = NtCreateSection(  &mut hSection, SECTION_EXTEND_SIZE | SECTION_MAP_READ | SECTION_MAP_WRITE, null_mut(), 
                    &mut SectionSize, PAGE_READWRITE, SEC_COMMIT, handle);

            if !NT_SUCCESS(status) {
                eprintln!("Failed to create section");
            }
            else {
                println!("Mapping Section created successfully");
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
            }

            if (lpZwMapping == INVALID_HANDLE_VALUE) {
                eprintln!("Failed to ap view of section");
            }
        }
 
        FileContext {
            data: [0; 1024],  // 初始化数组
            fHandle:handle,
            hSection : hSection,
            lpBaseAddress : lpZwMapping,
            fileSize:filesize,
            dwPageSize:pageSize
        }
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

    pub fn extendFile(&mut self, pageCount: usize) {
        let mut SectionSize: LARGE_INTEGER = unsafe { std::mem::zeroed() };
        // SectionSize.u_mut().HighPart = self.fileSize + pageCount * self.dwPageSize;
        // SectionSize.u_mut().LowPart = 
        unsafe {
            *SectionSize.QuadPart_mut() = self.fileSize + (pageCount * self.dwPageSize) as i64;

            let statusExtend = NtExtendSection(self.hSection, &mut SectionSize);
            if !NT_SUCCESS(statusExtend) {
                println!("Failed ExtendSection.\n");
            }                
            self.fileSize = SectionSize.QuadPart().abs();
        }
        println!("Extend File Successfully.\n");
    }

    fn print(&self) {
        for i in 0..1024 {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_FileContent()
    {
        let mut context = FileContext::new("c:/temp/rustfile.txt".as_bytes(),4096,10);
        context.set("1234567890abcdefghighk".as_bytes());
        context.extendFile(20);
        let ret = context.get(0,15);
        println!("Key:{} \n", String::from_utf8(ret).unwrap());
    }
    
}