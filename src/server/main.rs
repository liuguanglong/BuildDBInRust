mod db_service;
use std::{sync::{Arc, RwLock}, thread, time::Duration};
use librustdb::btree::{tx::{dbcontext::DbContext, dbinstance::DBInstance, memoryContext::memoryContext}, BTREE_PAGE_SIZE};
use db_service::{sql_executor_server::SqlExecutor, Column, DataTable, SqlRequest, SqlResult, ValueType};
use tonic::Response;

#[macro_use]
extern crate lazy_static;

struct DatabaseInstance
{
    db: DBInstance,
}

impl Default for DatabaseInstance {
    fn default() -> Self {
        let mut context :DbContext = memoryContext::new(BTREE_PAGE_SIZE,1000).into();
        let db:DBInstance = context.into();
        Self { db: db }
    }
}


#[derive(Default)]
pub struct DBServer {
    db:DatabaseInstance
}

impl DBServer {
    
}

#[tonic::async_trait]
impl SqlExecutor for DBServer {
    async fn execute_query(
        &self,
        request: tonic::Request<SqlRequest>,
    ) -> std::result::Result<tonic::Response<DataTable>, tonic::Status>
    {
        let db = &self.db.db;
        let sql = request.into_inner().sql_statement;

        let mut reader = db.beginRead().unwrap();
        if let Ok(list) = reader.ExecuteSQLStatments(sql)
        {
            assert!(list.len() == 1);
            let table = list.get(1).unwrap();
            
            let mut dt:DataTable  = DataTable{
                name: String::from_utf8(table.Name.to_vec()).unwrap(),
                columns: Vec::new(),
                rows:Vec::new(),
            };

            for col in &table.Cols
            {
                dt.columns.push(Column{
                    name:String::from_utf8(col.to_vec()).unwrap(),
                    r#type:ValueType::Bytes.into(),  
                });
            }

            for row in &table.Rows
            {
                dt.rows.push(row.Seralize())
            }            
            return Ok(Response::new(dt));
        }
        Err(tonic::Status::aborted("message"))
    }

    async fn execute_command(
        &self,
        request: tonic::Request<SqlRequest>,
    ) -> std::result::Result<tonic::Response<SqlResult>, tonic::Status>
    {
        Err(tonic::Status::aborted("message"))
    }
}

fn main() {


}