use std::{collections::HashMap, hash::Hash, sync::{Arc, MutexGuard, RwLock}};

use scopeguard::defer;

use crate::btree::{db::{TDEF_META, TDEF_TABLE}, kv::{node::BNode, ContextError}, table::table::TableDef, tx::txwriter::txwriter};

use super::{dbcontext::{self, DbContext}, txdemo::Shared, txinterface::{MmapInterface, TxContent}};



pub struct Database{
    context:DbContext,
    tables: Arc<RwLock<HashMap<Vec<u8>,TableDef>>>,
    writer:Shared<()>,
    reader:Shared<()>,
    readers: HashMap<usize,u64>,
}

impl Drop for Database {
    fn drop(&mut self) {
    }
}

impl Database{
    pub fn new(context:DbContext) -> Result<Self,ContextError> {
        let tables = Arc::new(RwLock::new(HashMap::new()));
        let mut context = Database {
            context: context,
            tables : tables,
            writer : Shared::new(()),
            reader : Shared::new(()),
            //lock : None,
            readers : HashMap::new(),
        };
        context.open();


        Ok(context)
    }

    fn getMinReadVersion(&self)->u64
    {
        let mut minversion:u64 = u64::MAX;
        for value in self.readers.values() {
            if minversion > *value
            {
                minversion = *value
            }
        }
        minversion
    }

}

impl TxContent for Database
{
    fn open(&mut self)->Result<(),crate::btree::kv::ContextError> {
        self.context.masterload();
        self.tables.write().unwrap().insert("@meta".as_bytes().to_vec(),TDEF_META.clone());
        self.tables.write().unwrap().insert("@table".as_bytes().to_vec(),TDEF_TABLE.clone());
        Ok(())
    }
    
    fn begin(& mut self)->Result<txwriter,ContextError> {
       
        let tx =self.context.createTx().unwrap();
        let lock = self.reader.lock();
        if self.readers.len() > 0 
        {
            self.context.version = self.getMinReadVersion();
        }
        drop(lock);
        let mut txwriter: txwriter = txwriter{
            context:tx,
            tables:self.tables.clone(),
        };
        Ok(txwriter)
    }
    
    fn commmit(&mut self, tx:&mut super::txwriter::txwriter)->Result<(),ContextError> {

        let nPages: usize = (tx.context.pageflushed + tx.context.nappend as u64) as usize;
        self.context.writePages(&tx.context.freelist.updates,nPages);

        self.context.nappend = tx.context.nappend;
        self.context.freehead = tx.context.freelist.data.head;

        let lock = self.reader.lock();
        self.context.root = tx.context.root;
        drop(lock);

        self.context.SaveMaster();

        Ok(())
    }
    
    fn abort(& mut self,tx:&super::txwriter::txwriter) {

    }
    
    fn beginread(&mut self)->Result<super::txreader::TxReader,ContextError> {
        let lock = self.reader.lock();
        defer! {
            drop(lock);
        }

        let index = self.readers.len();
        let reader = self.context.createReader(index,self.tables.clone());
        if let Ok(r) = reader
        {
            self.readers.insert(index,self.context.version);
            return Ok(r);        
        }
        else {
            return Err(ContextError::CreateReaderError);
        }
    }
    
    fn endread(&mut self, reader:& super::txreader::TxReader) {
        let lock = self.reader.lock();
        self.readers.remove(&reader.index);
        drop(lock);
    }
}



#[cfg(test)]
mod tests {

    use std::{sync::{Arc, Mutex, RwLock}, thread, time::Duration};
    use rand::Rng;

    use crate::btree::{db::{TDEF_META, TDEF_TABLE}, scan::comp::OP_CMP, table::{record::Record, table::TableDef, value::{Value, ValueType}}, tx::{memoryContext::memoryContext, txdemo::Shared, txinterface::{DBReadInterface, DBTxInterface, TxReadContext}, txwriter::txwriter, winmmap::Mmap}, BTREE_PAGE_SIZE, MODE_UPSERT};
    use super::*;
    use crate::btree::{btree::request::{DeleteRequest, InsertReqest}, db::{scanner::Scanner, INDEX_ADD, INDEX_DEL}};

    #[test]
    fn test_memorycontext()
    {
        let mut mctx = Arc::new(RwLock::new(memoryContext::new(BTREE_PAGE_SIZE,1000)));
        let mut context = DbContext::new(mctx.clone());
        let mut db = Arc::new(Mutex::new(Database::new(context).unwrap()));

        let mut db1 = db.clone();
        let mut dbinstance =  db1.lock().unwrap();
        let mut tx = dbinstance.begin().unwrap();
        drop(dbinstance);

        let mut table = TableDef{
            Prefix:0,
            Name: "person".as_bytes().to_vec(),
            Types : vec![ValueType::BYTES, ValueType::BYTES,ValueType::BYTES, ValueType::INT16, ValueType::BOOL ] ,
            Cols : vec!["id".as_bytes().to_vec() , "name".as_bytes().to_vec(),"address".as_bytes().to_vec(),"age".as_bytes().to_vec(),"married".as_bytes().to_vec() ] ,
            PKeys : 0,
            Indexes : vec![vec!["address".as_bytes().to_vec() , "married".as_bytes().to_vec()],vec!["name".as_bytes().to_vec()]],
            IndexPrefixes : vec![],
        };

        let ret = tx.AddTable(&mut table);
        if let Err(ret) = ret
        {
            println!("Error when add table:{}",ret);
        }

        let mut dbinstance =  db.lock().unwrap();
        dbinstance.commmit(&mut tx);
        drop(dbinstance);

        let mut dbinstance =  db.lock().unwrap();
        let mut tx  = dbinstance.begin().unwrap();        
        drop(dbinstance);   
        let ret = tx.getTableDef("person".as_bytes());
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

                tx.UpdateRecord(&mut r,crate::btree::MODE_UPSERT);
            }

            r.Set("id".as_bytes(), Value::BYTES(("21").as_bytes().to_vec()));
            r.Set( "name".as_bytes(), Value::BYTES(("Bob504").as_bytes().to_vec()));
            r.Set("address".as_bytes(), Value::BYTES("Montrel Canada H9T 1R5".as_bytes().to_vec()));
            r.Set("age".as_bytes(), Value::INT16(20));
            r.Set("married".as_bytes(), Value::BOOL(false));

            tx.UpdateRecord(&mut r,crate::btree::MODE_UPSERT);


            r.Set("id".as_bytes(), Value::BYTES(("22").as_bytes().to_vec()));
            tx.DeleteRecord(&mut r);

            let mut key1 = Record::new(&tdef);
            let mut key2 = Record::new(&tdef);
            key1.Set("name".as_bytes(), Value::BYTES("Bob1".as_bytes().to_vec()));
            key2.Set("name".as_bytes(), Value::BYTES("Bob5".as_bytes().to_vec()));
            //let mut scanner = dbinstance.Seek(1,OP_CMP::CMP_GT, OP_CMP::CMP_LE, &key1, &key2);
            let mut scanner = tx.Scan(OP_CMP::CMP_GE, Some(OP_CMP::CMP_LT), &key1, Some(&key2));
    
            let mut r3 = Record::new(&tdef);
            match &mut scanner {
                Ok(cursor) =>{
                    while cursor.Valid(){
                            cursor.Deref(&tx,&mut r3);
                            println!("{}", r3);
                            cursor.Next();
                        }                
                },
                Err(err) => { println!("Error when add table:{}",err)}
                
            }    
        }
    }

    #[test]
    fn test_concurrent()
    {
        let mut mctx = Arc::new(RwLock::new(memoryContext::new(BTREE_PAGE_SIZE,1000)));
        let mut context = DbContext::new(mctx.clone());
        let db = Shared::new(Database::new(context).unwrap());

        let mut table = TableDef{
            Prefix:0,
            Name: "person".as_bytes().to_vec(),
            Types : vec![ValueType::BYTES, ValueType::BYTES,ValueType::BYTES, ValueType::INT16, ValueType::BOOL ] ,
            Cols : vec!["id".as_bytes().to_vec() , "name".as_bytes().to_vec(),"address".as_bytes().to_vec(),"age".as_bytes().to_vec(),"married".as_bytes().to_vec() ] ,
            PKeys : 0,
            Indexes : vec![vec!["address".as_bytes().to_vec() , "married".as_bytes().to_vec()],vec!["name".as_bytes().to_vec()]],
            IndexPrefixes : vec![],
        };

        let mut db1 = db.clone();
        let mut dbinstance =  db1.lock().unwrap();
        let mut tx = dbinstance.begin().unwrap();
        let ret = tx.AddTable(&mut table);
        if let Err(ret) = ret
        {
            println!("Error when add table:{}",ret);
        }
        dbinstance.commmit(&mut tx);
        drop(dbinstance);        


        let mut handles = vec![];

        for i in 1..10 {
            //let reader = context.beginread();
            let ct =  db.clone();
            let handle = thread::spawn(move || {
                write(i, ct)
            });
            handles.push(handle);
        }

        thread::sleep(Duration::from_millis(10));
        for i in 1..10 {
            //let reader = context.beginread();
            let instance =  db.clone();
            let handle = thread::spawn(move || {
                read(i, instance)
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

    }

    fn write(i:u64,db:Shared<Database>)
    {
        let mut rng = rand::thread_rng();
        let random_number: u64 = rng.gen_range(2..10);
        thread::sleep(Duration::from_millis(random_number));

        //Try to get write lock,stay until get lock
        let mut dbinstance =  db.lock().unwrap();
        let mut writer = dbinstance.writer.clone();
        drop(dbinstance);
        let lockWriter = writer.lock().unwrap();

        println!("Begin Set Value:{}-{}",i,i);        
        //begin tx 
        let mut dbinstance =  db.lock().unwrap();
        let mut tx = dbinstance.begin().unwrap();
        drop(dbinstance);

        let ret = tx.getTableDef("person".as_bytes());
         if let Some(tdef) = ret
         {
            //println!("Table define:{}",tdef);
            let mut r = Record::new(&tdef);

            r.Set("id".as_bytes(), Value::BYTES(format!("{}", i).as_bytes().to_vec()));
            r.Set( "name".as_bytes(), Value::BYTES(format!("Bob{}", i).as_bytes().to_vec()));
            r.Set("address".as_bytes(), Value::BYTES("Montrel Canada H9T 1R5".as_bytes().to_vec()));
            r.Set("age".as_bytes(), Value::INT16(20));
            r.Set("married".as_bytes(), Value::BOOL(false));

            tx.UpdateRecord(&mut r,crate::btree::MODE_UPSERT);
        }

        println!("root :{}",tx.context.get_root());
        //commit tx
        let mut dbinstance =  db.lock().unwrap();
        dbinstance.commmit(&mut tx);
        drop(dbinstance);

        //drop writelock
        drop(lockWriter);
        println!("End Set Value:{}-{}",i,i);        
    }


    fn read(i:u64,db:Shared<Database>)
    {
        let mut rng = rand::thread_rng();
        let random_number: u64 = rng.gen_range(5..20);
        //thread::sleep(Duration::from_millis(random_number));
        
        let mut dbinstance =  db.lock().unwrap();
        let mut reader = dbinstance.beginread().unwrap();
        drop(dbinstance);

        println!("Begin Read:{}",i);        
        
        let ret = reader.getTableDef("person".as_bytes());
        if let Some(tdef) = ret
        {
            let mut key1 = Record::new(&tdef);
            let mut key2 = Record::new(&tdef);
            key1.Set("id".as_bytes(), Value::BYTES(format!("{}", i).as_bytes().to_vec()));
            key2.Set("id".as_bytes(), Value::BYTES(format!("{}", i).as_bytes().to_vec()));
            //let mut scanner = dbinstance.Seek(1,OP_CMP::CMP_GT, OP_CMP::CMP_LE, &key1, &key2);
            let mut scanner = reader.Scan(OP_CMP::CMP_GE, Some(OP_CMP::CMP_LE), &key1, Some(&key2));
    
            let mut r3 = Record::new(&tdef);
            match &mut scanner {
                Ok(cursor) =>{
                    while cursor.Valid(){
                            cursor.Deref(&reader,&mut r3);
                            println!("{}", r3);
                            cursor.Next();
                        }                
                },
                Err(err) => { println!("Error when add table:{}",err)}
                
            }    
        }
        println!("End Read:{}",i);        

        let mut dbinstance =  db.lock().unwrap();
        dbinstance.endread(&reader);
        drop(dbinstance);
    }

}