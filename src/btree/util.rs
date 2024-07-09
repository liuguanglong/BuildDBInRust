

pub fn compare_arrays(a: &[u8], b: &[u8]) ->i32 {

    let min_length = if a.len() < b.len() {a.len()} else {b.len()};

    for i in 0..min_length {
        if a[i] < b[i] {
            return -1;
        } else if a[i] > b[i] {
            return 1;
        }
    }
    if a.len() < b.len() {
        return -1;
    } else if a.len() > b.len() {
        return 1;
    }
    return 0;
}

pub fn deescapeString(content: &[u8]) -> Vec<u8> {
    let mut list:Vec<u8> = Vec::new();
    //println!("Before dedescapString: {}", content);
    let mut idx: usize = 0;
    while (idx < content.len() - 1) {
        if content[idx] == 1 {
            if content[idx + 1] == 1 {
                list.push(0x00);
                idx += 2;
            } 
            else if content[idx + 1] == 2 
            {
                list.push(0x01);
                idx += 2;
            } else 
            {
                list.push(content[idx]);
                idx += 1;
            }
        } else 
        {
            list.push(content[idx]);
            idx += 1;
        }
    }
    list.push(content[idx]);
    println!("decoded:{}",String::from_utf8(list.to_vec()).unwrap());
    return list;
}


// Strings are encoded as nul terminated strings,
// escape the nul byte so that strings contain no nul byte.
pub fn escapeString(content: &[u8], list:&mut Vec<u8>) {
    let mut idx: usize = 0;
    while idx < content.len() 
    {
        if content[idx] <= 1 
        {
            list.push(0x01);
            list.push(content[idx] + 1);
        } else 
        {
            list.push(content[idx]);
        }
        idx += 1;
    }
}