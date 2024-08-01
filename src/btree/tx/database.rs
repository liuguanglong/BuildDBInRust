use std::{collections::HashMap, hash::Hash, sync::{Arc, MutexGuard, RwLock}};

use scopeguard::defer;

use crate::btree::{db::{TDEF_META, TDEF_TABLE}, kv::{node::BNode, ContextError}, table::table::TableDef, tx::txwriter::txwriter};

use super::{dbcontext::{self, DbContext}, shared::Shared, txinterface::{MmapInterface, TxContent}};

pub struct Database{
    context:DbContext,
    tables: Arc<RwLock<HashMap<Vec<u8>,TableDef>>>,
    pub writer:Shared<()>,
    reader:Shared<()>,
    readers: HashMap<usize,u64>,
}

impl Drop for Database {
    fn drop(&mut self) {
    }
}


impl From<DbContext> for Database {
    fn from(context: DbContext) -> Self {
        Database::new(context).unwrap()
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

    use std::{fmt::Write, sync::{Arc, Mutex, RwLock}, thread, time::Duration};
    use rand::Rng;

    use crate::btree::{db::{TDEF_META, TDEF_TABLE}, scan::comp::OP_CMP, table::{record::Record, table::TableDef, value::{Value, ValueType}}, tx::{dbinstance::DBInstance, memoryContext::memoryContext, shared::Shared, txinterface::{DBReadInterface, DBTxInterface, TxReadContext}, txwriter::txwriter, winmmap::Mmap}, BTREE_PAGE_SIZE, MODE_UPSERT};
    use super::*;
    use crate::btree::{btree::request::{DeleteRequest, InsertReqest}, db::{scanner::Scanner, INDEX_ADD, INDEX_DEL}};

    #[test]
    fn test_memorycontext()
    {
        let mut mctx = Arc::new(RwLock::new(memoryContext::new(BTREE_PAGE_SIZE,1000)));
        let mut context = DbContext::new(mctx.clone());
        let db = DBInstance::new(Database::new(context).unwrap());

        let mut tx = db.beginTx().unwrap();

        let mut table = TableDef{
            Prefix:0,
            Name: "person".as_bytes().to_vec(),
            Types : vec!["BYTES".into(), "BYTES".into(),"BYTES".into(), "INT16".into(), "BOOL".into() ] ,
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
        db.commitTx(&mut tx);

        let mut tx  = db.beginTx().unwrap();        
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
                    cursor.into_iter().for_each(|r| println!("{}",r));
                },
                Err(err) => { println!("Error when add table:{}",err)}
                
            }    
        }
        db.commitTx(&mut tx);

    }

    #[test]
    fn test_concurrent()
    {
        let mut mctx = Arc::new(RwLock::new(memoryContext::new(BTREE_PAGE_SIZE,1000)));
        let mut context = DbContext::new(mctx.clone());
        let db = DBInstance::new(Database::new(context).unwrap());

        let createTable = r#"
        create table person
        ( 
            id vchar,
            name vchar,
            address vchar,
            age int16,
            married bool,
            primary key (id),
            index (address,married),
            index (name),
        );
       "#;

        let mut db1 = db.clone();

        let mut tx = db1.beginTx().unwrap();
        let ret = tx.ExecuteSQLStatments(createTable.to_string());
        if let Err(ret) = ret
        {
            println!("Error when add table:{}",ret);
        }
        db1.commitTx(&mut tx);

        let mut handles = vec![];
        for i in 1..10 {
            let ct =  db.clone();
            let handle = thread::spawn(move || {
                write(i, ct)
            });
            handles.push(handle);
        }

        thread::sleep(Duration::from_millis(10));
        for i in 1..10 {
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

    fn write(i:u64,db:DBInstance)
    {
        let mut rng = rand::thread_rng();
        let random_number: u64 = rng.gen_range(2..10);
        thread::sleep(Duration::from_millis(random_number));

        //Try to get write lock,stay until get lock
        let mut writer = db.getLocker();
        let lock = writer.lock().unwrap();

        println!("Begin Set Value:{}-{}",i,i);        
        //begin tx 
        let mut tx = db.beginTx().unwrap();

        let mut sql:String = String::new();
        let insert = r#"
        insert into person
        ( id, name, address, age, married )
        values
        "#;
        sql.push_str(&insert);
        sql.push_str(format!("('{}','Bob{}','Montrel Canada H9T 1R5',20,false),", i,i).as_str());
        sql.remove(sql.len() -1 );
        sql.push(';');

        let ret = tx.ExecuteSQLStatments(sql);
        //println!("root :{}",tx.context.get_root());
        //commit tx
        db.commitTx(&mut tx);
        //drop writelock
        drop(lock);
        println!("End Set Value:{}-{}",i,i);        
    }


    fn read(i:u64,db:DBInstance)
    {
        let mut rng = rand::thread_rng();
        let random_number: u64 = rng.gen_range(10..20);
        let mut reader = db.beginRead().unwrap();

        println!("Begin Read:{}",i);        
        let statements = format!("select id,name,address, age from person index by id = '{}';",i);
        if let Ok(list) = reader.ExecuteSQLStatments(statements)
        {
            list.iter().for_each(|table| println!("{}",table));
        }
        println!("End Read:{}",i);        
        db.endRead(&mut reader);
    }

}