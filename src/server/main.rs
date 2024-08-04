mod db_service;
use std::{sync::{Arc, RwLock}, thread, time::Duration};
use librustdb::btree::{parser::{lib::Parser, select::{ExprSelect, SelectExpr}, statement::ExprSQL}, tx::{dbcontext::DbContext, dbinstance::DBInstance, memoryContext::memoryContext}, BTREE_PAGE_SIZE};
use db_service::{sql_executor_server::{self, SqlExecutor}, Column, DataTable, SqlRequest, SqlResult, ValueType};
use tonic::{transport::Server, Response};

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
        
        println!("Query:{}", sql);
        if let Ok((_,expr)) = ExprSelect().parse(&sql)
        {
            let mut reader = db.beginRead().unwrap();
            if let Ok(table) = reader.ExecuteReader(&expr)
            {
                let mut dt:DataTable  = DataTable{
                    name: String::from_utf8(table.Name.to_vec()).unwrap(),
                    columns: Vec::new(),
                    rows:Vec::new(),
                };
    
                for i in 0..table.Cols.len()
                {
                    let rtype = table.Types[i].clone() as i32;
                    dt.columns.push(Column{
                        name:String::from_utf8(table.Cols[i].to_vec()).unwrap(),
                        r#type:rtype,  
                    });
                }
    
                for row in &table.Rows
                {
                    let content = row.Seralize();
                    //println!("Row:{:?}",content);
                    dt.rows.push(content)
                }            
                return Ok(Response::new(dt));
            }
            return Err(tonic::Status::aborted("Execute Sql Error"))
        }
        
        Err(tonic::Status::aborted("Parse Sql Error"))
    }

    async fn execute_command(
        &self,
        request: tonic::Request<SqlRequest>,
    ) -> std::result::Result<tonic::Response<SqlResult>, tonic::Status>
    {
        let db = &self.db.db;
        let sql = request.into_inner().sql_statement;
        println!("Command:{}", sql);

        if let Ok((_,expr)) = ExprSQL().parse(&sql)
        {
            let mut writer = db.getLocker();
            let lock = writer.lock().unwrap();

            let mut tx = db.beginTx().unwrap();
            if let Ok(affected) = tx.ExecuteNoQuery(&expr)
            {
                db.commitTx(&mut tx);
                drop(lock);
                return Ok(Response::new(SqlResult{ affected: affected as u32}));
            }
            else {
                db.abortTx(&mut tx);
                drop(lock);
                return Err(tonic::Status::aborted("Execute Sql Error"))
            }
    
        }
        Err(tonic::Status::aborted("Parse Sql Error"))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;
    let greeter = DBServer::default();

    Server::builder()
        .add_service(sql_executor_server::SqlExecutorServer::new(greeter))
        .serve(addr)
        .await?;

    Ok(())
}