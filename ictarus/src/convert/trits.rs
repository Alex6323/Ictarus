use crate::constants::*;
use crate::convert::bytes::TxBytes;
use crate::convert::luts::*;
use crate::convert::trytes::TxTrytes;

pub type Trit = i8;
pub type Trits243 = [i8; 243];
pub type TxTrits = [i8; TRANSACTION_SIZE_TRITS];

pub fn from_tx_bytes_2enc9(bytes: &[u8]) -> TxTrits {
    let mut trits = [0_i8; TRANSACTION_SIZE_TRITS];

    for i in 0..TRANSACTION_SIZE_TRITS / 9 {
        let b0 = bytes[i * 2] as usize;
        let b1 = bytes[i * 2 + 1] as usize;

        let i9 = i * 9;
        trits[i9..i9 + 3].copy_from_slice(&TRYTE_TO_TRIT_TRIPLET[b0 / 8][..]);
        trits[(i9 + 3)..(i9 + 6)].copy_from_slice(&TRYTE_TO_TRIT_TRIPLET[b1 / 8]);
        trits[(i9 + 6)..(i9 + 9)].copy_from_slice(&TRYTE_TO_TRIT_TRIPLET[b0 % 8 + 8 * (b1 % 8)]);
    }

    trits
}

pub fn from_bytes_2enc9(bytes: &[u8], offset: usize, len: usize) -> Vec<Trit> {
    assert!(len % 2 == 0);

    let mut trits = vec![0_i8; len / 2 * 9];

    for i in 0..(trits.len() / 9) {
        let pos = offset + 2 * i;

        let b0 = bytes[pos] as usize;
        let b1 = bytes[pos + 1] as usize;

        let i9 = i * 9;
        trits[i9..i9 + 3].copy_from_slice(&TRYTE_TO_TRIT_TRIPLET[b0 / 8][..]);
        trits[(i9 + 3)..(i9 + 6)].copy_from_slice(&TRYTE_TO_TRIT_TRIPLET[b1 / 8]);
        trits[(i9 + 6)..(i9 + 9)].copy_from_slice(&TRYTE_TO_TRIT_TRIPLET[b0 % 8 + 8 * (b1 % 8)]);
    }

    trits
}

pub fn from_tx_tryte_string(tryte_string: &str) -> TxTrits {
    assert!(IS_TRYTES.is_match(tryte_string));
    let bytes = tryte_string.as_bytes();
    assert_eq!(TRANSACTION_SIZE_TRYTES, bytes.len());

    let mut trits = [0i8; TRANSACTION_SIZE_TRITS];

    bytes.iter().enumerate().for_each(|(i, c)| {
        trits[(i * 3)..(i * 3) + 3]
            .copy_from_slice(&TRYTE_TO_TRIT_TRIPLET[*ASCII_TO_TRYTE.get(&c).unwrap()][..]);
    });

    trits
}

pub fn from_tryte_string(tryte_string: &str) -> Vec<Trit> {
    assert!(IS_TRYTES.is_match(tryte_string));
    let bytes = tryte_string.as_bytes();

    let mut trits = vec![0i8; tryte_string.len() * 3];

    bytes.iter().enumerate().for_each(|(i, c)| {
        trits[(i * 3)..(i * 3) + 3]
            .copy_from_slice(&TRYTE_TO_TRIT_TRIPLET[*ASCII_TO_TRYTE.get(&c).unwrap()][..]);
    });

    trits
}

pub fn from_tx_trytes(trytes: &TxTrytes) -> TxTrits {
    let mut trits = [0_i8; TRANSACTION_SIZE_TRITS];

    trytes.iter().enumerate().for_each(|(i, t)| {
        trits[(i * 3)..(i * 3 + 3)].copy_from_slice(&ASCII_TO_TRIT_TRIPLET.get(t).unwrap()[..]);
    });

    trits
}

pub fn from_trytes(trytes: &[u8]) -> Vec<Trit> {
    let mut trits = vec![0_i8; trytes.len() * 3];

    trytes.iter().enumerate().for_each(|(i, t)| {
        trits[(i * 3)..(i * 3 + 3)].copy_from_slice(&ASCII_TO_TRIT_TRIPLET.get(t).unwrap()[..]);
    });

    trits
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::prelude::*;

    #[test]
    fn test_from_trytes() {
        let trytes = "HELLO9WORLD";
        let trits = from_tryte_string(trytes);

        println!("{:?}", trits);
        assert_eq!(33, trits.len());
    }

    #[test]
    fn test_trits_from_trytes() {
        let trytes = "HELLO9WORLD";
        //let trits = trits_from_trytes(trytes);
    }

    /// Converts bytes representing trit quintuplets to its corresponding trit representation.
    /// Ported from Cfb's Java version. Just for testing purposes.
    fn from_tx_bytes_cfb(bytes: &[u8]) -> [i8; TRANSACTION_SIZE_TRITS] {
        // NOTE: we need to convert &[u8] to &[i8] for the following code to work
        let bytes = &bytes.iter().map(|u| *u as i8).collect::<Vec<i8>>()[0..bytes.len()];

        let mut result = [0_i8; TRANSACTION_SIZE_TRITS];
        let mut offset = 0_usize;
        let mut index: usize;
        let mut count: usize;

        for i in 0..bytes.len() {
            if offset >= TRANSACTION_SIZE_TRITS {
                break;
            }

            index = if bytes[i] < 0 {
                (bytes[i] as i32 + 243) as usize
            } else {
                bytes[i] as usize
            };
            count = if (TRANSACTION_SIZE_TRITS - offset) < 5 {
                TRANSACTION_SIZE_TRITS - offset
            } else {
                5
            };

            result[offset..offset + count].copy_from_slice(&BYTE_TO_TRITS[index][0..count]);
            offset += 5;
        }

        // unnecessary
        while offset < result.len() {
            result[offset] = 0;
            offset += 1;
        }

        result
    }
}
