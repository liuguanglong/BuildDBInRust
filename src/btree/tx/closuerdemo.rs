use std::{cell::RefCell, collections::HashMap, rc::{Rc, Weak}};


struct Parent {
    data: HashMap<u64,u64>,
    root: u64,
    child: Rc<RefCell<Child>>,
}

impl Parent {
    fn new(root: u64,data:HashMap<u64,u64>) -> Rc<RefCell<Self>> {

        let parent = Rc::new(RefCell::new(Parent {
            root:root,
            data:HashMap::new(),
            child: Rc::new(RefCell::new(Child::default())), 
        }));

        parent.borrow_mut().child = Rc::new(RefCell::new(Child {
            parent: Rc::downgrade(&parent),
            data:data,
            root:root
        }));

        parent
    }

    fn get_root(&self)->u64 {
        self.root
    }

    fn get(&self,key:&u64) -> Option<u64>
    {
        if self.data.contains_key(&key)
        {
            return Some(self.data.get(&key).unwrap().clone());
        }
        else {
            return self.child.borrow().get(key);
        }
    }

    fn set(&mut self, key : u64, v:u64)
    {
        self.data.insert(key, v);
    }

}

struct Child {
    parent: Weak<RefCell<Parent>>,
    data:HashMap<u64,u64>,
    root:u64,
}

impl Child {
    pub fn new(root:u64,data:HashMap<u64,u64>)->Self
    {
        Child{
            data:data,
            root:root,
            parent: Weak::new(),
        }
    }
    fn get(&self,key:&u64)->Option<u64> {
        if let Some(parent) = self.parent.upgrade() 
        {
            return parent.borrow().get(key);
        } else
        {
            return Some(self.data.get(&key).unwrap().clone());
        }
    }
    fn get_root(&self)->u64
    {
        if let Some(parent) = self.parent.upgrade() 
        {
            return parent.borrow().get_root();
        } else
        {
            return self.root;
        }
    }
}

impl Default for Child {
    fn default() -> Self {
        Child {
            parent: Weak::new(),
            data:HashMap::new(),
            root:0,
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    
    #[test]
    fn test_parent() {

        let mut data = HashMap::new();
        for i in 0..100
        {
            data.insert(i,i );
        }
        let mut parent = Parent::new(0,data);
        for i in 0..100
        {
            parent.borrow_mut().set(i, i + 100);
        }        

        for i in 0..100
        {
            assert_eq!(i + 100, parent.borrow().get(&i).unwrap());
        }

        for i in 0..100
        {
            assert_eq!(i + 100, parent.borrow().child.borrow().get(&i).unwrap());
        }
    }

    #[test]
    fn test_child() {

        let mut data = HashMap::new();
        for i in 0..100
        {
            data.insert(i,i );
        }

        let child =  Child::new(0,data);

        for i in 0..100
        {
            assert_eq!(i, child.get(&i).unwrap());
        }        
    }
}