use std::fmt;



#[derive(Clone,Debug,Copy)]
pub enum OP_CMP {
    CMP_GE,
    CMP_GT,
    CMP_LT,
    CMP_LE,
    CMP_EQ,
    CMP_UnEQ,
}

impl OP_CMP {
    pub fn value(&self) -> i16 {
        match self {
            OP_CMP::CMP_GE => 3,
            OP_CMP::CMP_GT => 2,
            OP_CMP::CMP_LT => -2,
            OP_CMP::CMP_LE => -3,
            OP_CMP::CMP_EQ => 1,
            OP_CMP::CMP_UnEQ => -1,
        }
    }
}


impl fmt::Display for OP_CMP {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            OP_CMP::CMP_GE => write!(f, "CMP_GE"),
            OP_CMP::CMP_GT => write!(f, "CMP_GT"),
            OP_CMP::CMP_LT => write!(f, "CMP_LT"),
            OP_CMP::CMP_LE => write!(f, "CMP_LE"),
            OP_CMP::CMP_EQ => write!(f, "CMP_EQ"),
            OP_CMP::CMP_UnEQ => write!(f, "CMP_UnEQ"),
        }
    }
}


pub fn cmpOK(key:&[u8], val: &[u8], cmp:&OP_CMP) -> bool {
    let ret = crate::btree::util::compare_arrays(key, val);
    match cmp {
        OP_CMP::CMP_GE => {
            return ret >= 0;
        },
        OP_CMP::CMP_GT => {
            return ret > 0;
        },
        OP_CMP::CMP_LT => {
            return ret < 0;
        },
        OP_CMP::CMP_LE => {
            return ret <= 0;
        },
        _Other => {
            panic!()
        }
    } 
}
