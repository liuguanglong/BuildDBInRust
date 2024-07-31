use std::{ops::{Deref, DerefMut}, sync::{Arc, Mutex}};


#[derive(Debug)]
pub struct Shared<T> {
    inner: Arc<Mutex<T>>,
}

impl<T> Shared<T> {
    pub fn new(data: T) -> Self {
        Shared {
            inner: Arc::new(Mutex::new(data)),
        }
    }

    pub fn clone(&self) -> Self {
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