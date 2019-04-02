use super::luts::*;
use super::trits::*;
use super::trytes::{self, *};

pub fn from_trits_243(trits: &Trits243) -> String {
    String::from_utf8(trytes::from_trits_fixed81(&trits).to_vec()).unwrap()
}

pub fn from_trits(trits: &[Trit]) -> String {
    String::from_utf8(trytes::from_trits(&trits)).unwrap()
}

pub fn from_trytes(trytes: &[Tryte]) -> String {
    String::from_utf8(trytes.to_vec()).unwrap()
}

pub fn from_ascii(text: &str) -> String {
    String::from_utf8(trytes::from_ascii(text)).unwrap()
}

pub fn pad_right(tryte_string: &str, length: usize) -> String {
    if length <= tryte_string.len() {
        return String::from(tryte_string);
    };
    let trytes = tryte_string.as_bytes();

    let mut chars = vec![TRYTE_TO_ASCII[0]; length];
    chars[0..trytes.len()].copy_from_slice(&trytes[..]);

    String::from_utf8(chars).unwrap()
}

pub fn unpad_right(tryte_string: &str) -> String {
    match tryte_string.rfind(|c| c != '9') {
        Some(index) => String::from(&tryte_string[0..=index]),
        None => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::*;

    #[test]
    fn test_from_ascii() {
        let ascii_text = "Hello";
        let tryte_string = from_ascii(ascii_text);
        //println!("{}", tryte_string);

        assert_eq!("YEZNMEQWF", tryte_string);
    }

    #[test]
    fn test_pad_right() {
        let text = "HELLO9WORLD";
        let padded30 = pad_right(&text, 30);
        assert_eq!("HELLO9WORLD9999999999999999999", padded30);

        let padded3 = pad_right(&text, 3);
        assert_eq!("HELLO9WORLD", padded3);
    }

    #[test]
    fn test_unpad_right() {
        let text = "HELLO9WORLD99999999999999999999";
        assert_eq!("HELLO9WORLD", unpad_right(text));

        let nines = "99999999";
        assert_eq!("", unpad_right(nines));
    }

    #[test]
    fn test_pad_unpad_right() {
        assert_eq!("HELLO9WORLD", unpad_right(&pad_right("HELLO9WORLD", 100)));
    }

    #[test]
    fn test_from_trits() {
        let tryte = from_trits(&[1, 1, 1]);

        assert_eq!(tryte, "M".to_string());
    }
}
