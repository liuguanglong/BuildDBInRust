# Build Your Own Database From Scratch in Rust

How to use database with Memory Context.
```rust
fn main() {
   
    let mut context :DbContext = memoryContext::new(BTREE_PAGE_SIZE,1000).into();
    let db:DBInstance = context.into();

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

    thread::sleep(Duration::from_millis(20));
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
        let insert = format!(
            r#"
            insert into person
            ( id, name, address, age, married )
            values
            ('{}','Bob{}','Montrel Canada H9T 1R5',20,false);
            "#,
            i,i
        );

        let ret = tx.ExecuteSQLStatments(insert);
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
        thread::sleep(Duration::from_millis(random_number));

        let mut reader = db.beginRead().unwrap();
        
        println!("Begin Read:{}",i);        
        let statements = format!("select id,name,address, age from person index by id = '{}';",i);
        if let Ok(list) = reader.ExecuteSQLStatments(statements)
        {
            list.iter().for_each(
                |table| { println!("Read Result:{} -{}",i,table);}
            );
        }
        println!("End Read:{}",i);        
        db.endRead(&mut reader);
    }


```



## 01. Files Vs Databases | 06. Persist to Disk

Source:    
   src/btree/kv/windowsfilecontext.rs     
Implement file mapping on windows OS
(CreateFileA|NtCreateSection|NtMapViewOfSection|NtExtendSection|FlushViewOfFile|FlushFileBuffers)

## 04. B-Tree: The Practice (Part I) 
Source:    
  src/btree/kv/node.rs

## 05. B-Tree: The Practice (Part II)
Source:    
  src/btree/btree/btree.rs

## 07. Free List: Reusing Pages
Source:    
  src/btree/kv/windowsfilecontext.rs  

## 08. Rows and Columns

Data Struct    
Source:    
  src/btree/table/value.rs
  src/btree/table/record.rs
  src/btree/table/table.rs   

Point Query|Update|CreateTable   
Source:    
  src/btree/db/database.rs

## 09. Range Query

Source:    
  src/btree/scan/biter.rs
  src/btree/db/sacnner.rs
  src/btree/db/database.rs

## 10. Secondary Index

Source     
  src/btree/db/database.rs

## 11. Atomic Transactions | 12. Concurrent Readers and Writers

Source    
  src/btree/tx/tx.rs    
  src/btree/tx/txreader.rs    
  src/btree/tx/txwriter.rs    
  src/btree/tx/txrecord.rs   
  src/btree/tx/dbcontext.rs   
  src/btree/tx/database.rs   
  src/btree/tx/dbinstance.rs   
  src/btree/tx/memorycontext.rs   
  src/btree/tx/txbiter.rs   
  src/btree/tx/txscanner.rs   
  src/btree/tx/txfreelist.rs   

## 13. Query Language: Parser

Source    
  src/btree/parser/lib.rs    
  src/btree/parser/expr.rs    
  src/btree/parser/statement.rs    
  src/btree/parser/createtable.rs   
  src/btree/parser/delete.rs    
  src/btree/parser/insert.rs    
  src/btree/parser/select.rs    
  src/btree/parser/update.rs   

## 14. Query Language: Execution

Source    
  src/btree/tx/txreader.rs  ExecuteSQLStatments()|ExecuteReader()    
  src/btree/tx/txwriter.rs  ExecuteSQLStatments()|ExecuteNoQuery()|ExecuteReader()    



