

pub struct InsertReqest<'a>{
    //Out
    pub Added: bool,
    pub Updated: bool,
    pub OldValue: Vec<u8>,
    //in
    pub Key: &'a [u8],
    pub Val: &'a [u8],
    pub Mode: u16,
}

impl<'a> InsertReqest<'a> {
    pub fn new(key:&'a [u8],val:&'a [u8],mode:u16) -> Self{
        InsertReqest{
            Key:key,
            Val:val,
            Mode:mode,
            Added:false,
            Updated:false,
            OldValue:Vec::new()
        }
    }
}

pub struct DeleteRequest<'a>{
    //Out
    pub OldValue: Vec<u8>,
    //in
    pub Key: &'a [u8],
}

impl<'a> DeleteRequest<'a> {
    pub fn new(key:&'a [u8]) -> Self{
        DeleteRequest{
            Key:key,
            OldValue:Vec::new()
        }
    }
}
