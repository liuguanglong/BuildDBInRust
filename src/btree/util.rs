

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
