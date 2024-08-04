mod db_service;

use std::{fmt, path::Display, sync::{Arc, RwLock}, thread, time::Duration};
use db_service::{sql_executor_client::SqlExecutorClient, Column, DataTable, SqlRequest, ValueType};

impl fmt::Display for DataTable {

    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f,"Table :{}\n|",self.name);

        for i in 0..self.columns.len()
        {
            write!(f,"{}|", self.columns[i].name);
        } 
        write!(f,"\n");
        for i in 0..self.rows.len()
        {
            writeln!(f,"{:?}", self.rows[i]);
            printRow(&self.rows[i],f,&self.columns);
        } 
        write!(f,"\n")
    }
}

fn printRow(content:&Vec<u8>,f: &mut fmt::Formatter,columns:&Vec<Column>)
{
    let mut pos = 0;
    for c in columns
    {
        let len = printCell(&c.r#type, &content, pos,f);
        pos += len;
    }
    write!(f,"\n");
}

pub fn deescapeString(content: &[u8]) -> Vec<u8> {
    let mut list:Vec<u8> = Vec::new();
    //println!("Before dedescapString: {:?}", content);
    let mut idx: usize = 0;
    if content[idx] == 0xfe
    {
        idx +=1 ;
    }

    while idx < content.len() - 1 {
        if content[idx] == 1 {
            if content[idx + 1] == 1 {
                list.push(0x00);
                idx += 2;
            } 
            else if content[idx + 1] == 2 
            {
                list.push(0x01);
                idx += 2;
            } else 
            {
                list.push(content[idx]);
                idx += 1;
            }
        } else 
        {
            list.push(content[idx]);
            idx += 1;
        }
    }
    if idx == content.len() -1
    {
        list.push(content[idx]);
    }
    //println!("decoded:{}",String::from_utf8(list.to_vec()).unwrap());
    return list;
}

pub fn printCell(t:&i32,val:&[u8],pos: usize,f: &mut fmt::Formatter)->usize {
    match ValueType::try_from(*t).unwrap() {
        ValueType::Bytes => {
            let mut end = pos;
            while val[end] != 0
            {
                end += 1;
            }   
            if end != pos
            {
                let ret = deescapeString(val[pos..end].try_into().unwrap());
                write!(f,"{}|", String::from_utf8(ret).unwrap());
                return  end - pos + 1;
            }
            else {
                return  end - pos + 1;
            }
        },
        ValueType::Int64 => {
            write!(f,"{}|", i64::from_le_bytes( val[pos..pos+8].try_into().unwrap() ));
            return 8;
        },
        ValueType::Int32 => {
            write!(f,"{}|", i32::from_le_bytes( val[pos..pos+4].try_into().unwrap() ));
            return 4;
        },
        ValueType::Int16 => {
            write!(f,"{}|", i16::from_le_bytes(val[pos..pos+2].try_into().unwrap()));
            return 2;
        },
        ValueType::Int8 => {
            write!(f,"{}|", i8::from_le_bytes([val[pos];1]));
            return 1;
        },
        ValueType::Bool => {
            if val[pos] == 1 {
                write!(f,"{}|", true);
            }
            else {
                write!(f,"{}|", false);
            }
            return 1;
        },
    };
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = SqlExecutorClient::connect("http://[::1]:50051").await?;

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

   let insert = format!(
    r#"
        insert into person
        ( id, name, address, age, married )
        values
        ('{}','Bob{}','Montrel Canada H9T 1R5',20,false);
        "#,
    1,1
    );

    let query = format!("select id,name,address, age, age > 18 as adult from person index by id = '{}';",1);

   
    let request = tonic::Request::new(SqlRequest {
        sql_statement: createTable.into(),
    });

    let response = client.execute_command(request).await?;
    println!("RESPONSE={:?}", response);

    let request = tonic::Request::new(SqlRequest {
        sql_statement: insert.into(),
    });

    let response = client.execute_command(request).await?;
    println!("RESPONSE={:?}", response);

    let request = tonic::Request::new(SqlRequest {
        sql_statement: query.into(),
    });

    let response = client.execute_query(request).await?;
    println!("{}", response.into_inner());


    Ok(())
}