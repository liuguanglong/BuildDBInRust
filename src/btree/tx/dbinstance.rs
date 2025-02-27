use std::{ops::{Deref, DerefMut}, sync::{Arc, Mutex}};
use crate::btree::kv::ContextError;

use super::{database::Database, dbcontext::DbContext, shared::Shared, txinterface::TxContent, txreader::{self, TxReader}, txwriter::txwriter};

pub struct DBInstance {
    inner: Arc<Mutex<Database>>,
}

impl From<DbContext> for DBInstance {
    fn from(context: DbContext) -> Self {
        DBInstance::new(Database::new(context).unwrap())
    }
}

impl DBInstance {
    pub fn new(db: Database) -> Self {
        DBInstance {
            inner: Arc::new(Mutex::new(db)),
        }
    }

    pub fn clone(&self) -> Self {
        DBInstance {
            inner: Arc::clone(&self.inner),
        }
    }

    pub fn beginTx(&self)->Result<txwriter,ContextError>
    {
        let mut dbinstance =  self.lock().unwrap();
        let mut tx = dbinstance.begin();
        drop(dbinstance);
        return tx;
    }

    pub fn commitTx(&self,tx:&mut txwriter)->Result<(),ContextError>
    {
        let mut dbinstance =  self.lock().unwrap();
        let mut ret = dbinstance.commmit(tx);
        drop(dbinstance);
        return ret;
    }

    pub fn abortTx(&self,tx:&mut txwriter)
    {
        let mut dbinstance =  self.lock().unwrap();
        let mut ret = dbinstance.abort(tx);
        drop(dbinstance);
    }

    pub fn beginRead(&self)->Result<TxReader,ContextError>
    {
        let mut dbinstance =  self.lock().unwrap();
        let mut tx = dbinstance.beginread();
        drop(dbinstance);
        return tx;
    }

    pub fn endRead(&self,tx:&mut TxReader)
    {
        let mut dbinstance =  self.lock().unwrap();
        let mut ret = dbinstance.endread(tx);
        drop(dbinstance);
    }

    pub fn getLocker(&self)->Shared<()>
    {
        let mut dbinstance =  self.lock().unwrap();
        let mut writer = dbinstance.writer.clone();
        drop(dbinstance);
        writer
    }
}

impl Deref for DBInstance {
    type Target = Mutex<Database>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for DBInstance {
    fn deref_mut(&mut self) -> &mut Self::Target {
        Arc::get_mut(&mut self.inner).expect("Multiple strong references exist")
    }
}
