# Build Your Own Database From Scratch in Rust

How to use database.

A sample about concurrent requests with insert and read from database. 
```rust
fn main() {
    let mut handles = vec![];

    let mut context:DbContext = WinMmap::new("c:/temp/rustdb.dat".as_bytes(),BTREE_PAGE_SIZE,1000).unwrap().into();
    //let mut context :DbContext = memoryContext::new(BTREE_PAGE_SIZE,1000).into();
    let db:DBInstance = context.into();

    //create table
    createTable(db.clone());

    //insert records
    for i in 1..10 {
        let ct =  db.clone();
        let handle = thread::spawn(move || {
            write(i, ct)
        });
        handles.push(handle);
    }
    
    thread::sleep(Duration::from_millis(20));
    //read records
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
```
Support Two Type of Context.

  1.Memory Context
```rust
    let mut context :DbContext = memoryContext::new(BTREE_PAGE_SIZE,1000).into();
```

  2.File Context
```rust
    let mut context:DbContext = WinMmap::new("d:/rustdb.dat".as_bytes(),BTREE_PAGE_SIZE,1000).unwrap().into();
```

Suppored Sql Grammar
```sql
create table table_name (
  a type1,
  b type2,
  
  index (c, b, a),
  index (d, e, f),
  primary key (a, b),
);

select expr from table_name index by expr  limit 10 offset 200;
insert into table_name (c1,c2,c3) values (a, b, c),(a1,b1,c1);
delete from table_name index expr filter expr;
update table_name set a = expr, b = expr, index by expr filter expr;

```
The INDEX BY clause explicitly selects the index for the query. It represents an indexed     
point query or an indexed range query, and the range can be either open-ended or     
closed. It also controls the order of the rows.     
```
select expr... from table_name index by a = 1;
select expr... from table_name index by a > 1;
select expr... from table_name index by a > 1 and a < 5
select expr... from table_name index by a = 1 and b = 2 and c > 5;  //index a,b,c
select expr... from table_name index by a = 1 and b = 2 and c > 5 and c < 3;  //index a,b,c
```

The FILTER clause selects rows without using indexes. Both the INDEX BY and the     
FILTER clauses are optional.
The whole filter condition is calc as a whole expr.
```
select expr... from table_name index by ... filter a > 20 or b < 30 and name > 'bob';
```

Supported operator in Expr and Operator precedence
```
-a, Not a
a * b, a / b
a + b, a - b
a < b, a > b, a <= b, a >= b 
a = b, a != b
NOT a
a AND b
a OR b
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



