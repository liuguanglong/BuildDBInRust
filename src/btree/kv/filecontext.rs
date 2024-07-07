use crate::btree::kv::noderef::BNodeRef;
use crate::btree::kv::nodeinterface::BNodeReadInterface;

struct FileContext {
    data: [u8; 1024],
}

impl FileContext {
    // 构造函数
    fn new() -> Self {
        FileContext {
            data: [0; 1024],  // 初始化数组
        }
    }

    fn copy_value(&mut self,s :&str){
        let content = s.as_bytes();
        for (i, &item) in content.iter().enumerate() 
        {
            self.data[i] = item;
        }
    }

    fn getNode(&self,_:&u64) -> Option<BNodeRef> {
        let d = &self.data[0..64];
        let s = BNodeRef{data:d,size:64};
        return Some(s);
    }

    fn print(&self) {
        for i in 0..1024 {
            if  i > 0 {
                print!("{:02x} ", self.data[i]);
            }
            if i % 50 == 0
            {
                println!();
            }
        }
        println!();
        // println!("{:?}", self.data);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    //#[test]
    fn test_FileContent()
    {
        println!("test_FileContent");
        let novel1 = String::from("Call me Ishmael.");
        let mut f = FileContext::new();
        f.copy_value(&novel1);
        f.print();
        
        let n = f.getNode(&1);
        match n
        {
            Some(nr) => {
                for i in 0..32 {
                    print!("{:02x} ", nr.data()[i]);
                }        
                f.copy_value("22222222222222222222222222222");
                println!();
            },
            None => {}
        }

        f.copy_value("eeeeeeeeeeeeeeeeeee");
        let nr1 = f.getNode(&2);
        match nr1
        {
            Some(nr) => {
                for i in 0..32 {
                    print!("{:02x} ", nr.data()[i]);
                }        
                f.copy_value("22222222222222222222222222222");
                println!();
            },
            None => {}
        }
    }
}