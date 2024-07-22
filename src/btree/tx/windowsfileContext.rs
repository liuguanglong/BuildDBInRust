use std::collections::HashMap;
use std::sync::{Arc, MutexGuard, RwLock};

use crate::btree::db::{TDEF_META, TDEF_TABLE};
use crate::btree::kv::node::BNode;
use crate::btree::kv::{ContextError, BTREE_PAGE_SIZE, DB_SIG};
use crate::btree::table::table::TableDef;

use super::tx::Tx;
use super::txdemo::Shared;
use super::txinterface::{TxContent};
use super::txreader::TxReader;
use super::txwriter::txwriter;
use super::winmmap::{self, Mmap, WinMmap};
use scopeguard::defer;

pub struct WindowsFileContext<'a> {
    context:WinMmap,
    tables: Arc<RwLock<HashMap<Vec<u8>,TableDef>>>,
    writer:Shared<()>,
    reader:Shared<()>,
    lock: Option<MutexGuard<'a,()>>,
    readers: Vec<u64>,
}

impl<'a> Drop for WindowsFileContext<'a> {
    fn drop(&mut self) {
        if let Some(l) = &self.lock
        {
            drop(l);
            self.lock = None;
        }
    }
}

impl<'a> TxContent<'a> for WindowsFileContext<'a>{

    fn open(&mut self)->Result<(),crate::btree::kv::ContextError> {
        self.context.masterload();
        self.tables.write().unwrap().insert("@meta".as_bytes().to_vec(),TDEF_META.clone());
        self.tables.write().unwrap().insert("@table".as_bytes().to_vec(),TDEF_TABLE.clone());
        Ok(())
    }

    fn save(&mut self,updats:&HashMap<u64,Option<BNode>>)->Result<(), crate::btree::kv::ContextError> {
        if let Err(err) = self.context.writePages(updats)
        {
            return Err(err);
        }
        else {
            Ok(())
        }
    }
    
    fn begin(&'a mut self)->Result<super::txwriter::txwriter,crate::btree::BTreeError> {
        
        self.lock = Some(self.writer.lock().unwrap());
        let tx = Tx::new(self.context.mmap.clone(),
            self.context.root,self.context.pageflushed,            
            self.context.fileSize as usize, 
            self.context.freehead,
            self.context.version, self.context.version);

        
        if self.readers.len() > 0 
        {
            self.context.version = self.readers[0];
        }
    
        let lock = self.reader.lock();
        let mut txwriter = txwriter{
            context:tx,
            tables:self.tables.clone(),
        };
        drop(lock);

        Ok(txwriter)
    }
    
    fn commmit(&'a mut self, tx:&mut super::txwriter::txwriter)->Result<(),crate::btree::BTreeError> {
        self.context.writePages(&tx.context.freelist.updates);
        self.context.nappend = tx.context.nappend;
        self.context.freehead = tx.context.freelist.data.head;

        let lock = self.reader.lock();
        self.context.root = tx.context.root;
        drop(lock);

        self.context.SaveMaster();

        defer! {
            if let Some(l) = &self.lock
            {
                drop(l);
                self.lock = None;
            }
        }
        Ok(())
    }
    
    fn abort(&'a mut self,tx:&mut super::txwriter::txwriter) {
        if let Some(l) = &self.lock
        {
            drop(l);
            self.lock = None;
        }
    }
    
    fn beginread(&mut self)->Result<super::txreader::TxReader,crate::btree::BTreeError> {
        let lock = self.reader.lock();
        let index = self.readers.len();
        let reader = TxReader::new(
            self.context.mmap.clone(),
            self.context.fileSize as usize,
            self.context.version,
            index);

        self.readers.push(self.context.version);
        drop(lock);
        Ok(reader)        
    }
    
    fn endread(&mut self, reader:& super::txreader::TxReader) {
        let lock = self.reader.lock();
        self.readers.remove(reader.index);
        drop(lock);
    }

}

impl<'a> WindowsFileContext<'a>{

    pub fn new(fileName: &[u8], pageSize: usize, maxPageCount: usize) -> Result<Self,ContextError> {

        let mut mmap = WinMmap::new(fileName,pageSize,maxPageCount);
        if let Err(err) = mmap
        {
            return Err(ContextError::LoadDataException);
        }

        let tables = Arc::new(RwLock::new(HashMap::new()));
        let mut context = WindowsFileContext {
            context:mmap.unwrap(),
            tables : tables,
            writer : Shared::new(()),
            reader : Shared::new(()),
            lock : None,
            readers : Vec::new(),
        };
        context.open();

        Ok(context)
    }
}