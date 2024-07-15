use std::{collections::HashMap, ops::{Deref, DerefMut}, sync::{Arc, Mutex}};


pub trait KVReaderInterface {
    fn Get(&self,key:&[u8])->Option<&[u8]>;
}

pub trait KVTxInterface {
    fn Set(&mut self,key:&[u8],val:&[u8]);
    fn Del(&mut self,key:&[u8]);
}

pub struct KVReader{
    data:HashMap<Vec<u8>,Vec<u8>>,    
}

impl KVReaderInterface for KVReader{
    fn Get(&self,key:&[u8])->Option<&[u8]> {
        let node = self.data.get(&key.to_vec());
        if let Some(v) = node
        {
            return Some(v);
        }
        else {
            return None;
        }
    }
}

pub struct KVTx{
    reader:KVReader,
    updates:HashMap<Vec<u8>,Option<Vec<u8>>>
}

impl KVReaderInterface for KVTx{
    fn Get(&self,key:&[u8])->Option<&[u8]> {

        if(self.updates.contains_key(key))
        {
            if let Some(v) = self.updates.get(key)
            {
                if let Some(V1) = v
                {
                    return Some(V1);
                }
                else {
                    return None;                    
                }
            }
        }

        let node = self.reader.Get(&key);
        node
    }
}

impl KVTxInterface for KVTx {
    fn Set(&mut self,key:&[u8],val:&[u8]) {
        self.updates.insert(key.to_vec(), Some(val.to_vec()));
    }

    fn Del(&mut self,key:&[u8]) {
        self.updates.insert(key.to_vec(), None);
    }
}

pub struct KVContext{
    data:HashMap<Vec<u8>,Vec<u8>>,  
    writer:Shared<()>,
    //mu:Mutex<u16>,
    //writer:Mutex<u16>,  
}

impl KVContext {

    pub fn beginread(&mut self)->KVReader{
        let kv = KVReader{ data: self.data.clone()};
        kv
    }

    pub fn endread(&self,reader:&KVReader)
    {
    }

    pub fn begintx(&mut self)->KVTx
    {
        let kv = KVReader{ data: self.data.clone()};
        let tx = KVTx{ updates:HashMap::new(), reader:kv};
        tx
    }

    pub fn abort(&mut self, tx:&KVTx)
    {

    }

    pub fn commit(&mut self, tx:&mut KVTx)
    {
        for kv in &tx.updates
        {
            if let Some(v) = kv.1
            {
                self.data.insert(kv.0.clone(),v.clone());
            }
            else {
                self.data.remove(kv.0);
            }
        }
        tx.updates.clear();
    }
}

struct Shared<T> {
    inner: Arc<Mutex<T>>,
}

impl<T> Shared<T> {
    fn new(data: T) -> Self {
        Shared {
            inner: Arc::new(Mutex::new(data)),
        }
    }

    fn clone(&self) -> Self {
        Shared {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T> Deref for Shared<T> {
    type Target = Mutex<T>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> DerefMut for Shared<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        Arc::get_mut(&mut self.inner).expect("Multiple strong references exist")
    }
}


#[cfg(test)]
mod tests {
    use std::{borrow::BorrowMut, hash::Hash, sync::Arc, time::Duration};
    use rand::Rng;
    use super::*;
    use std::thread;

    fn read(i:u64,ct:Shared<KVContext>)
    {
        let mut rng = rand::thread_rng();
        let random_number: u64 = rng.gen_range(1..20);
        thread::sleep(Duration::from_millis(random_number));
        let mut ct1 = ct.lock().unwrap();
        let reader = ct1.beginread();
        drop(ct1);

        let t = reader.Get(format!("{}", i).as_bytes());
        if let Some(t) = t
        {
            println!("Ret {}:{:?}",i,t);
        }
        else {
            println!("Ret {}:None",i);
        }

        let mut ct1 = ct.lock().unwrap();
        ct1.endread(&reader);
        drop(ct1);
    }

    fn write(i:u64,ct:Shared<KVContext>)
    {
        let mut rng = rand::thread_rng();
        let random_number: u64 = rng.gen_range(1..10);

        let mut ct1 = ct.lock().unwrap();
        let mut tx = ct1.begintx();
        let mut writer = ct1.writer.clone();
        println!("Begin Set Value:{}-{}",i,i);        
        drop(ct1);
        let lockWriter = writer.lock().unwrap();
        thread::sleep(Duration::from_millis(random_number));

        let  t = tx.Set(format!("{}", i).as_bytes(), format!("{}", i ).as_bytes());
        
        let mut ct1 = ct.lock().unwrap();
        ct1.commit(&mut tx);
        drop(lockWriter);
        println!("End Set Value:{}-{}",i,i);        
        drop(ct1);
    }

    #[test]
    fn test_muti_thread_access(){
        
        let mut data:HashMap<Vec<u8>,Vec<u8>> = HashMap::new();
        data.insert("1".as_bytes().to_vec(), "a".as_bytes().to_vec());
        data.insert("2".as_bytes().to_vec(), "b".as_bytes().to_vec());
        data.insert("3".as_bytes().to_vec(), "c".as_bytes().to_vec());
        data.insert("4".as_bytes().to_vec(), "d".as_bytes().to_vec());

        let mut context = KVContext{data:data,writer:Shared::new(())};
        let instance = Shared::new(context);
        let mut handles = vec![];

        for i in 0..10 {
            //let reader = context.beginread();
            let ct =  instance.clone();
            let handle = thread::spawn(move || {
                read(i, ct)
            });
            handles.push(handle);
        }

        for i in 1..10 {
            //let reader = context.beginread();
            let ct =  instance.clone();
            let handle = thread::spawn(move || {
                write(i, ct)
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }
    

    }
    #[test]
    fn test_KvReader() {
        let mut data:HashMap<Vec<u8>,Vec<u8>> = HashMap::new();
        data.insert("1".as_bytes().to_vec(), "a".as_bytes().to_vec());
        data.insert("2".as_bytes().to_vec(), "b".as_bytes().to_vec());
        data.insert("3".as_bytes().to_vec(), "c".as_bytes().to_vec());
        data.insert("4".as_bytes().to_vec(), "d".as_bytes().to_vec());

        let reader = KVReader{ data:data};
        let v1 = reader.Get("1".as_bytes());


        println!("Ret1:{:?}",v1);
    }

    #[test]
    fn test_KvTx() {
        let mut data:HashMap<Vec<u8>,Vec<u8>> = HashMap::new();
        data.insert("1".as_bytes().to_vec(), "a".as_bytes().to_vec());
        data.insert("2".as_bytes().to_vec(), "b".as_bytes().to_vec());
        data.insert("3".as_bytes().to_vec(), "c".as_bytes().to_vec());
        data.insert("4".as_bytes().to_vec(), "d".as_bytes().to_vec());

        let mut tx: KVTx = KVTx{ updates:HashMap::new(), reader:KVReader{data:data}};
        let v1 = tx.Get("1".as_bytes());
        println!("Ret1:{:?}",v1);

        tx.Set("1".as_bytes(), "aa".as_bytes());
        let v1 = tx.Get("1".as_bytes());
        println!("Ret1:{:?}",v1);

        tx.Del("2".as_bytes());
        let v1 = tx.Get("2".as_bytes());
        assert_eq!(true,v1.is_none());

    }
}