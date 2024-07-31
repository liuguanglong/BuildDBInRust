use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, RwLock};

use crate::btree::db::{TDEF_META, TDEF_TABLE};
use crate::btree::kv::node::BNode;
use crate::btree::kv::{ContextError, BTREE_PAGE_SIZE, DB_SIG};
use crate::btree::table::table::TableDef;
use crate::btree::{table, BTreeError};

use super::tx::Tx;
use super::shared::Shared;
use super::txinterface::{TxContent};
use super::txreader::TxReader;
use super::txwriter::txwriter;
use super::winmmap::{self, Mmap, WinMmap};
use scopeguard::defer;

pub struct WindowsFileContext{
    context:WinMmap,
    tables: Arc<RwLock<HashMap<Vec<u8>,TableDef>>>,
    writer: Shared<()>,
    reader: Shared<()>,
    lock: Option<MutexGuard<'static,()>>,
    readers: Vec<u64>,
}

impl Drop for WindowsFileContext {
    fn drop(&mut self) {
    }
}

// impl TxContent for WindowsFileContext{

//     fn open(&mut self)->Result<(),crate::btree::kv::ContextError> {
//         self.context.masterload();
//         self.tables.write().unwrap().insert("@meta".as_bytes().to_vec(),TDEF_META.clone());
//         self.tables.write().unwrap().insert("@table".as_bytes().to_vec(),TDEF_TABLE.clone());
//         Ok(())
//     }
    
//     fn begin(& mut self)->Result<super::txwriter::txwriter,ContextError> {
        
//         let guard = self.writer.lock().unwrap();
//         let static_guard: MutexGuard<'static, ()> = unsafe { std::mem::transmute(guard) };
//         self.lock = Some(static_guard);
//         let tx =self.context.createTx().unwrap();
//         if self.readers.len() > 0 
//         {
//             self.context.version = self.readers[0];
//         }
    
//         let lock = self.reader.lock();
//         defer! {
//             drop(lock);
//         }
//         let mut txwriter = txwriter{
//             context:tx,
//             tables:self.tables.clone(),
//         };

//         Ok(txwriter)
//     }
    
//     fn commmit(& mut self, tx:&mut super::txwriter::txwriter)->Result<(),ContextError> {
//         self.context.writePages(&tx.context.freelist.updates);
//         self.context.nappend = tx.context.nappend;
//         self.context.freehead = tx.context.freelist.data.head;

//         let lock = self.reader.lock();
//         self.context.root = tx.context.root;
//         drop(lock);

//         self.context.SaveMaster();

//         defer! {
//             if let Some(l) = &self.lock
//             {
//                 drop(l);
//                 self.lock = None;
//             }
//         }
//         Ok(())
//     }
    
//     fn abort(& mut self,tx:& super::txwriter::txwriter) {
//         if let Some(l) = &self.lock
//         {
//             drop(l);
//             self.lock = None;
//         }
//     }
    
//     fn beginread(&mut self)->Result<super::txreader::TxReader,ContextError> {
//         let lock = self.reader.lock();
//         defer! {
//             drop(lock);
//         }

//         let index = self.readers.len();
//         let reader = self.context.createReader(index,tables);
//         if let Ok(r) = reader
//         {
//             self.readers.push(self.context.version);
//             return Ok(r);        
//         }
//         else {
//             return Err(ContextError::CreateReaderError);
//         }
//     }
    
//     fn endread(&mut self, reader:& super::txreader::TxReader) {
//         let lock = self.reader.lock();
//         self.readers.remove(reader.index);
//         drop(lock);
//     }

// }

impl WindowsFileContext{

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
        //context.open();

        Ok(context)
    }
}