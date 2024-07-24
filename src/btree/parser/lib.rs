use crate::btree::parser::number_string;


//Fn(&str)->Reulst<(&str,Element>,&str);
fn the_letter_a(input:&str)-> Result<(&str,()),&str>
{
    match input.chars().next() {
        Some('a') => Ok((&input['a'.len_utf8()..],())),
        _Other=>Err(input)
    }
}

#[test]
fn test_the_letter_a()
{
    assert_eq!(Ok(("bcde",())),the_letter_a("abcde"));
    assert_eq!(Err("bcde"),the_letter_a("bcde"));
}
