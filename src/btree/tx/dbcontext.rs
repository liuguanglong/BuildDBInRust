use std::{collections::HashMap, sync::{Arc, Mutex, RwLock}};

use crate::btree::{kv::{node::BNode, nodeinterface::{BNodeFreeListInterface, BNodeReadInterface, BNodeWriteInterface}, ContextError, DB_SIG}, table::table::TableDef, BTREE_PAGE_SIZE};
use super::{memoryContext::memoryContext, tx::Tx, txinterface::MmapInterface, txreader::TxReader, winmmap::Mmap};

pub struct DbContext{
    //mmapObj:&'a mut dyn MmapInterface,
    mmapObj:Arc<RwLock<dyn MmapInterface>>,
    //lpBaseAddress: *mut winapi::ctypes::c_void,
    pub root: u64,
    pub nappend: u16, //number of pages to be appended
    pub freehead: u64, //head of freeelist
    pub version:u64, //verison of db data
    
    pageflushed: u64, // database size in number of pages
    nfreelist: u16, //number of pages taken from the free list
}

impl DbContext{

    pub fn new(mmap:Arc<RwLock<dyn MmapInterface>>)->Self
    {
        DbContext{
            mmapObj:mmap,
            root:0,
            pageflushed:0,
            nfreelist:0,
            nappend:0,
            freehead:0,
            version : 0, 
        }
    }

    pub fn createReader(&mut self,index:usize,tables: Arc<RwLock<HashMap<Vec<u8>,TableDef>>>)->Result<TxReader,ContextError>
    {  
        let reader = TxReader::new(
            self.mmapObj.read().unwrap().getMmap().clone(),
            self.root,
            self.mmapObj.read().unwrap().getContextSize(),
            self.version,
            index,
            tables
        );

        Ok(reader)
    }

    pub fn createTx(&mut self)->Result<Tx,ContextError>
    {
        let tx = Tx::new(self.mmapObj.read().unwrap().getMmap().clone(),
            self.root,self.pageflushed,            
            self.mmapObj.read().unwrap().getContextSize() as usize, 
            self.freehead,
            self.version, self.version
        );

        Ok(tx)
    }

    // the master page format.
    // it contains the pointer to the root and other important bits.
    // | sig | btree_root | page_used |
    // | 16B | 8B | 8B |
    pub fn masterload(&mut self)->Result<(),ContextError>
    {
        //Init Db file
        if self.mmapObj.read().unwrap().getContextSize() == 0 {
            if let Err(er) = self.mmapObj.write().unwrap().extendContext(2){
                return Err(ContextError::ExtendNTSectionError);
            };


            let mut newNode = BNode::new(BTREE_PAGE_SIZE);
            newNode.flnSetHeader(0, 0);
            newNode.flnSetTotal(0);

            // unsafe {
            //     let buffer = self.mmap.read().unwrap().ptr;
            //     for i  in 0..BTREE_PAGE_SIZE
            //     {
            //         *buffer.add(BTREE_PAGE_SIZE*2 + i) = newNode.data()[i];
            //     }
            // }

            self.freehead = 0;
            if self.root == 0 {
                let mut root = BNode::new(BTREE_PAGE_SIZE);
                root.set_header(crate::btree::BNODE_LEAF, 1);
                root.node_append_kv(0, 0, &[0;1], &[0;1]);
                unsafe {
                    let mut mmap = self.mmapObj.read().unwrap().getMmap();
                    let buffer =  mmap.read().unwrap().ptr;
                    for i  in 0..BTREE_PAGE_SIZE
                    {
                        *buffer.add(BTREE_PAGE_SIZE + i) = root.data()[i];
                    }
                }
            }
            self.root = 1;

            self.pageflushed = 2;
            self.nfreelist = 0;
            self.nappend = 0;

            self.masterStore();
            let ret = self.mmapObj.write().unwrap().syncContext();
            if let Err(err) = ret
            {
                return Err(err);
            };

            return Ok(());
        }

        //Load Db File
        unsafe {
            let mut mmap = self.mmapObj.read().unwrap().getMmap();
            let buffer =  mmap.read().unwrap().ptr;
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

            pos = 40;
            for i in 0..8
            {
                content[i] = *buffer.add(i+ pos);
            }
            let version = u64::from_le_bytes(content[0..8].try_into().unwrap());

            let mut bad: bool = !(1 <= used && used <= (self.mmapObj.read().unwrap().getContextSize() as u64)/ BTREE_PAGE_SIZE as u64);
            bad = bad || !(0 <= root && root < used);
            if (bad == true) {
                return Err(ContextError::LoadDataException);
            }
    
            self.root = root;
            self.pageflushed = used;
            self.nfreelist = 0;
            self.nappend = 0;    
            self.freehead = freehead;
            self.version = version;
        }

       Ok(())
    }

    // update the master page. it must be atomic.
    pub fn masterStore(&mut self) {
        unsafe {
            
            let mut data: [u8;48] = [0;48];
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

            pos = 40;
            data[pos..pos+8].copy_from_slice(&self.version.to_le_bytes());

            let mut mmap = self.mmapObj.read().unwrap().getMmap();
            let buffer =  mmap.read().unwrap().ptr;
            for i in 0..48
            {
                *buffer.add(i + 16) = data[i];
            }
        }
    }

    pub fn writePages(&mut self,updates:&HashMap<u64,Option<BNode>>,totalPages:usize)->Result<(),ContextError>{

        self.mmapObj.write().unwrap().extendPages(totalPages);

        for entry in updates
        {
            if let Some(v) = entry.1 
            {
                let ptr:u64 = *entry.0;
                let offset:usize = ptr as usize * BTREE_PAGE_SIZE;
                unsafe {
                    let mut mmap = self.mmapObj.read().unwrap().getMmap();
                    let buffer =  mmap.read().unwrap().ptr;;
                    for i in 0..BTREE_PAGE_SIZE
                    {
                        *buffer.add(i + offset as usize) = v.data()[i];
                    }
                }
            }
        }

        let ret = self.mmapObj.write().unwrap().syncContext();
        if let Err(err) = ret
        {
            return Err(err);
        }

        Ok(())
    }

    pub fn SaveMaster(&mut self)->Result<(),ContextError>
    {
        self.pageflushed += self.nappend as u64;
        self.nfreelist = 0;
        self.nappend = 0;

        self.masterStore();

        let mut writer = self.mmapObj.write().unwrap();
        let ret = writer.syncContext(); 
        drop(writer);

        Ok(())
    }

}

#[cfg(test)]
mod tests {

    use std::sync::{Arc, RwLock};
    use crate::btree::{db::{TDEF_META, TDEF_TABLE}, scan::comp::OP_CMP, table::{record::Record, table::TableDef, value::{Value, ValueType}}, tx::{memoryContext::memoryContext,  shared::Shared, txinterface::DBTxInterface, txwriter::txwriter, winmmap::Mmap}, BTREE_PAGE_SIZE, MODE_UPSERT};
    use super::*;
    use crate::btree::{btree::request::{DeleteRequest, InsertReqest}, db::{scanner::Scanner, INDEX_ADD, INDEX_DEL}};

    #[test]
    fn test_memorycontext()
    {
        let mut mctx = Arc::new(RwLock::new(memoryContext::new(BTREE_PAGE_SIZE,1000)));
        let mut context = DbContext::new(mctx.clone());
        context.masterload();

        let tables = Arc::new(RwLock::new(HashMap::new()));
        tables.write().unwrap().insert("@meta".as_bytes().to_vec(),TDEF_META.clone());
        tables.write().unwrap().insert("@table".as_bytes().to_vec(),TDEF_TABLE.clone());
        
        let tx = context.createTx().unwrap();
        
        let mut dbinstance = txwriter{
            context:tx,
            tables:tables.clone(),
        };

        let mut table = TableDef{
            Prefix:0,
            Name: "person".as_bytes().to_vec(),
            Types : vec![ValueType::BYTES, ValueType::BYTES,ValueType::BYTES, ValueType::INT16, ValueType::BOOL ] ,
            Cols : vec!["id".as_bytes().to_vec() , "name".as_bytes().to_vec(),"address".as_bytes().to_vec(),"age".as_bytes().to_vec(),"married".as_bytes().to_vec() ] ,
            PKeys : 0,
            Indexes : vec![vec!["address".as_bytes().to_vec() , "married".as_bytes().to_vec()],vec!["name".as_bytes().to_vec()]],
            IndexPrefixes : vec![],
        };

        let ret = dbinstance.AddTable(&mut table);
        if let Err(ret) = ret
        {
            println!("Error when add table:{}",ret);
        }

        let ret = dbinstance.getTableDef("person".as_bytes());
        if let Some(tdef) = ret
        {
            println!("Table define:{}",tdef);
            let mut r = Record::new(&tdef);

            for i in 0..100 {
                r.Set("id".as_bytes(), Value::BYTES(format!("{}", i).as_bytes().to_vec()));
                r.Set( "name".as_bytes(), Value::BYTES(format!("Bob{}", i).as_bytes().to_vec()));
                r.Set("address".as_bytes(), Value::BYTES("Montrel Canada H9T 1R5".as_bytes().to_vec()));
                r.Set("age".as_bytes(), Value::INT16(20));
                r.Set("married".as_bytes(), Value::BOOL(false));

                dbinstance.UpdateRecord(&mut r,crate::btree::MODE_UPSERT);
            }

            r.Set("id".as_bytes(), Value::BYTES(("21").as_bytes().to_vec()));
            r.Set( "name".as_bytes(), Value::BYTES(("Bob504").as_bytes().to_vec()));
            r.Set("address".as_bytes(), Value::BYTES("Montrel Canada H9T 1R5".as_bytes().to_vec()));
            r.Set("age".as_bytes(), Value::INT16(20));
            r.Set("married".as_bytes(), Value::BOOL(false));

            dbinstance.UpdateRecord(&mut r,crate::btree::MODE_UPSERT);


            r.Set("id".as_bytes(), Value::BYTES(("22").as_bytes().to_vec()));
            dbinstance.DeleteRecord(&mut r);

            let mut key1 = Record::new(&tdef);
            let mut key2 = Record::new(&tdef);
            key1.Set("name".as_bytes(), Value::BYTES("Bob1".as_bytes().to_vec()));
            key2.Set("name".as_bytes(), Value::BYTES("Bob5".as_bytes().to_vec()));
            //let mut scanner = dbinstance.Seek(1,OP_CMP::CMP_GT, OP_CMP::CMP_LE, &key1, &key2);
            let mut scanner = dbinstance.Scan(OP_CMP::CMP_GT, Some(OP_CMP::CMP_LE), &key1, Some(&key2));
    
            let mut r3 = Record::new(&tdef);
            match &mut scanner {
                Ok(cursor) =>{
                    while cursor.Valid(){
                            cursor.Deref(&dbinstance,&mut r3);
                            println!("{}", r3);
                            cursor.Next();
                        }                
                },
                Err(err) => { println!("Error when add table:{}",err)}
                
            }    
        }
    }
}