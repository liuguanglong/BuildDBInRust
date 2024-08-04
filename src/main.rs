use std::{sync::{Arc, RwLock}, thread, time::Duration};
extern crate librustdb;

use librustdb::btree::{tx::{database::Database, dbcontext::DbContext, dbinstance::DBInstance, memoryContext::memoryContext, windowsfileContext::WinMmap}, BTREE_PAGE_SIZE};
use rand::Rng;

#[macro_use]
extern crate lazy_static;
 
fn main() {
    let mut context:DbContext = WinMmap::new("c:/temp/rustdb.dat".as_bytes(),BTREE_PAGE_SIZE,1000).unwrap().into();
    let mut context :DbContext = memoryContext::new(BTREE_PAGE_SIZE,1000).into();
    
    let db:DBInstance = context.into();
    createTable(db.clone());


    let mut handles = vec![];
    //insert records
    for i in 1..2 {
        let ct =  db.clone();
        let handle = thread::spawn(move || {
            write(i, ct)
        });
        handles.push(handle);
    }

    thread::sleep(Duration::from_millis(20));
    //read records
    for i in 1..2 {
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

fn createTable(db:DBInstance)
{
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

    let mut tx = db.beginTx().unwrap();
    let ret = tx.ExecuteSQLStatments(createTable.to_string());
    if let Err(ret) = ret
    {
        println!("Error when add table:{}",ret);
    }
    db.commitTx(&mut tx);
    
}

fn write(i:u64,db:DBInstance)
    {
        let mut sql:String = String::new();

        let insert = format!(
            r#"
            insert into person
            ( id, name, address, age, married )
            values
            ('{}','Bob{}','Montrel Canada H9T 1R5',20,false);
            "#,
            i,i
        );

        let mut rng = rand::thread_rng();
        let random_number: u64 = rng.gen_range(2..10);
        thread::sleep(Duration::from_millis(random_number));

        //Try to get write lock,stay until get lock
        let mut writer = db.getLocker();
        let lock = writer.lock().unwrap();

        println!("Begin Set Value:{}-{}",i,i);        
        //begin tx 
        let mut tx = db.beginTx().unwrap();

        let ret = tx.ExecuteSQLStatments(insert);
        //commit tx
        db.commitTx(&mut tx);
        
        //drop writelock
        drop(lock);
        println!("End Set Value:{}-{}",i,i);        
    }


    fn read(i:u64,db:DBInstance)
    {
        let statements = format!("select id,name,address, age, age > 18 as adult from person index by id = '{}';",i);

        let mut rng = rand::thread_rng();
        let random_number: u64 = rng.gen_range(10..20);
        thread::sleep(Duration::from_millis(random_number));

        let mut reader = db.beginRead().unwrap();
        
        println!("Begin Read:{}",i);        
        if let Ok(list) = reader.ExecuteSQLStatments(statements)
        {
            list.iter().for_each(
                |table| { println!("Read Result:{} -{}",i,table);}
            );
        }
        println!("End Read:{}",i);        
        db.endRead(&mut reader);
    }
