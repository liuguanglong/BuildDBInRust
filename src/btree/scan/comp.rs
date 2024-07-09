pub enum OP_CMP {
    CMP_GE,
    CMP_GT,
    CMP_LT,
    CMP_LE,
}

impl OP_CMP {
    pub fn value(&self) -> i16 {
        match self {
            OP_CMP::CMP_GE => 3,
            OP_CMP::CMP_GT => 2,
            OP_CMP::CMP_LT => -2,
            OP_CMP::CMP_LE => -3,
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
    } 
}
