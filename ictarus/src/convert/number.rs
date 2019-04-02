use super::luts::*;
use super::trits::*;
use super::trytes::*;

pub const MAX_I64_TRYTE_LENGTH: usize = 11;

pub fn i64_from_trytes_max11(trytes: &[Tryte]) -> i64 {
    let mut number = 0;
    trytes
        .iter()
        .take(MAX_I64_TRYTE_LENGTH)
        .enumerate()
        .for_each(|(i, &t)| {
            number += TRYTE_AS_UTF8_TO_I64[i][t as usize];
        });

    number
}

pub fn i64_from_trits(trits: &[Trit]) -> i64 {
    assert!(trits.len() <= 20);

    let mut number = 0;

    for n in (0..trits.len()).rev() {
        number = number * 3 + i64::from(trits[n]);
    }

    number
}

#[cfg(test)]
mod tests {
    use super::super::trytes;
    use super::*;
    use rand::*;

    #[test]
    fn test_value_to_from_i64() {
        for _ in 0..1000 {
            let a = i64::from(rand::thread_rng().next_u32());
            let b = i64::from(rand::thread_rng().next_u32());
            let c = a - b;
            assert_eq!(c, i64_from_trytes_max11(&trytes::from_i64_fixed27(c)));
        }
    }

    //#[test]
    fn test_from_i64() {
        let trytes = String::from_utf8(from_i64(26, 10)).unwrap();
        println!("{}", trytes);
        //TODO
        assert!(false);
    }

    #[test]
    fn test_to_long_value() {
        let number1 = i64_from_trits(&[1, 1, 1]);
        let number2 = i64_from_trits(&[-1, -1, -1]);
        let number1 = i64_from_trits(&[1, 1, 1]);
        assert_eq!(number1, 13);
        assert_eq!(number2, -13);
    }
}
