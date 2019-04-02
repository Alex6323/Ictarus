use super::number;
use super::tryte_string;
use super::trytes::*;

pub type ASCII = u8;

pub fn from_tryte_string(tryte_string: &str) -> String {
    let tryte_string = tryte_string::unpad_right(tryte_string);
    let mut trytes = tryte_string.as_bytes().to_vec();
    for _ in 0..trytes.len() % 3 {
        trytes.push(TRYTE_NULL);
    }

    let mut ascii_chars = vec![0; trytes.len() / 3 * 2];
    let mut index;

    for i in 0..trytes.len() / 3 {
        index =
            number::i64_from_trytes_max11(&trytes[(i * 3)..(i * 3 + 3)]) + MAX_TRYTE_TRIPLET_ABS;

        ascii_chars[i * 2] = (index / 127) as u8;
        ascii_chars[i * 2 + 1] = (index % 127) as u8;
    }

    if ascii_chars[ascii_chars.len() - 1] == 0 {
        ascii_chars.remove(ascii_chars.len() - 1);
    }

    String::from_utf8(ascii_chars).unwrap()
}

#[cfg(test)]
mod tests {
    use super::super::tryte_string;
    use super::*;

    #[test]
    fn test_from_tryte_string() {
        let tryte_string = "YEZNMEQWF";
        let ascii_text = from_tryte_string(tryte_string);

        //println!("{}", ascii_text);
        assert_eq!("Hello", ascii_text);
    }

    #[test]
    fn test_encode_decode_ascii() {
        assert_eq!(
            "Hello",
            from_tryte_string(&tryte_string::from_ascii("Hello"))
        );
    }
}
