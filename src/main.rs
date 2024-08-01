use std::{sync::{Arc, RwLock}, thread, time::Duration};

use btree::{tx::{database::Database, dbcontext::DbContext, dbinstance::DBInstance, memoryContext::memoryContext}, BTREE_PAGE_SIZE};
use rand::Rng;

mod btree;
#[macro_use]
extern crate lazy_static;

fn main() {
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
