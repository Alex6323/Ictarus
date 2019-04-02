use super::luts::*;
use super::number::*;
use super::trits::*;
use crate::constants::*;

pub type Tryte = u8;
pub type Trytes9 = [u8; 9];
pub type Trytes27 = [u8; 27];
pub type Trytes81 = [u8; 81];
pub type TxTrytes = [u8; TRANSACTION_SIZE_TRYTES];

pub const MAX_TRYTE_TRIPLET_ABS: i64 = 9841; // (3^9-1)/2
pub const TRYTE_NULL: Tryte = 57;
pub const TRYTES_9_NULL: Trytes9 = [57; 9];
pub const TRYTES_27_NULL: Trytes27 = [57; 27];
pub const TRYTES_81_NULL: Trytes81 = [57; 81];

pub const TRYTE_NULL_STR: &str = "9";

macro_rules! from_bytes_2enc9_fixed_size {
    ($func:ident, $length:expr) => {
        pub fn $func(bytes: &[u8]) -> [Tryte; $length] {
            let mut trytes = [TRYTE_TO_ASCII[0]; $length];

            for i in 0..($length / 3) {
                let b0 = bytes[2 * i] as usize;
                let b1 = bytes[2 * i + 1] as usize;

                trytes[3 * i] = TRYTE_TO_ASCII[b0 / 8];
                trytes[3 * i + 1] = TRYTE_TO_ASCII[b1 / 8];
                trytes[3 * i + 2] = TRYTE_TO_ASCII[b0 % 8 + 8 * (b1 % 8)];
            }
            trytes
        }
    };
}

from_bytes_2enc9_fixed_size!(from_54_bytes_2enc9, 81);
from_bytes_2enc9_fixed_size!(from_18_bytes_2enc9, 27);
from_bytes_2enc9_fixed_size!(from_tx_bytes_2enc9, TRANSACTION_SIZE_TRYTES);

pub fn from_bytes_2enc9(bytes: &[u8], offset: usize, len: usize) -> Vec<Tryte> {
    assert!(len % 2 == 0);

    let mut trytes = vec![TRYTE_TO_ASCII[0]; len / 2 * 3];

    for i in 0..(trytes.len() / 3) {
        let pos = offset + 2 * i;

        let b0 = bytes[pos] as usize;
        let b1 = bytes[pos + 1] as usize;

        trytes[3 * i] = TRYTE_TO_ASCII[b0 / 8];
        trytes[3 * i + 1] = TRYTE_TO_ASCII[b1 / 8];
        trytes[3 * i + 2] = TRYTE_TO_ASCII[b0 % 8 + 8 * (b1 % 8)];
    }

    trytes
}

pub fn from_trits(trits: &[Trit]) -> Vec<Tryte> {
    assert!(trits.len() % 3 == 0);
    let mut trytes = vec![TRYTE_TO_ASCII[0]; trits.len() / 3];
    let mut index;

    for (i, t) in trytes.iter_mut().enumerate() {
        index = trits[i * 3] + 3 * trits[i * 3 + 1] + 9 * trits[i * 3 + 2];
        index = if index < 0 { index + 27 } else { index };
        *t = TRYTE_TO_ASCII[index as usize];
    }

    trytes
}

pub fn from_trits_fixed81(trits: &Trits243) -> Trytes81 {
    let mut trytes = [TRYTE_TO_ASCII[0]; 81];
    let mut index;

    for (i, t) in trytes.iter_mut().enumerate() {
        index = trits[i * 3] + 3 * trits[i * 3 + 1] + 9 * trits[i * 3 + 2];
        index = if index < 0 { index + 27 } else { index };
        *t = TRYTE_TO_ASCII[index as usize];
    }

    trytes
}

pub fn from_tryte_string_trytes_81(tryte_string: &str) -> [Tryte; 81] {
    assert!(IS_TRYTES.is_match(tryte_string));

    let chars = tryte_string.as_bytes();
    assert_eq!(81, chars.len());

    let mut trytes = [TRYTE_TO_ASCII[0]; 81];
    trytes[..].copy_from_slice(&chars[..]);

    trytes
}

pub fn from_tryte_string_to_fragment_trytes(tryte_string: &str) -> [u8; SIGNATURE_FRAGMENTS.3] {
    let mut trytes = [TRYTE_NULL; SIGNATURE_FRAGMENTS.3];

    if tryte_string.is_empty() {
        return trytes;
    }

    assert!(IS_TRYTES.is_match(tryte_string));
    let chars = tryte_string.as_bytes();

    for (i, &c) in chars.iter().enumerate().take(SIGNATURE_FRAGMENTS.3) {
        trytes[i] = c;
    }

    trytes
}

pub fn from_i64(number: i64, num_trytes: usize) -> Vec<Tryte> {
    let range_abs = (3i64.pow(num_trytes as u32 * 3) - 1) / 2;
    assert!(number >= -range_abs && number <= range_abs);

    let mut trytes = vec![TRYTE_TO_ASCII[0]; num_trytes as usize];

    let is_positive = number > 0;
    let mut number = number.abs();
    let mut remainder;

    for t in trytes.iter_mut() {
        remainder = number % 27;
        number = if remainder > 13 {
            number / 27 + 1
        } else {
            number / 27
        };

        *t = if is_positive {
            TRYTE_TO_ASCII[remainder as usize]
        } else {
            ETYRT_TO_ASCII[remainder as usize]
        };

        if number == 0 {
            break;
        }
    }

    trytes
}

pub fn from_i64_fixed27(number: i64) -> [Tryte; 27] {
    let is_positive = number > 0;
    let mut trytes = [TRYTE_TO_ASCII[0]; 27];
    let mut number = number.abs();
    let mut remainder;

    for t in trytes.iter_mut().take(MAX_I64_TRYTE_LENGTH) {
        remainder = number % 27;
        number = if remainder > 13 {
            number / 27 + 1
        } else {
            number / 27
        };
        *t = if is_positive {
            TRYTE_TO_ASCII[remainder as usize]
        } else {
            ETYRT_TO_ASCII[remainder as usize]
        };
        if number == 0 {
            break;
        }
    }

    trytes
}

pub fn from_i64_fixed9(number: i64) -> [Tryte; 9] {
    let is_positive = number > 0;
    let mut trytes = [TRYTE_TO_ASCII[0]; 9];
    let mut number = number.abs();
    let mut remainder;

    for t in trytes.iter_mut() {
        remainder = number % 27;
        number = if remainder > 13 {
            number / 27 + 1
        } else {
            number / 27
        };
        *t = if is_positive {
            TRYTE_TO_ASCII[remainder as usize]
        } else {
            ETYRT_TO_ASCII[remainder as usize]
        };
        if number == 0 {
            break;
        }
    }

    trytes
}

pub fn from_ascii_to_trytes_27(text: &str) -> [Tryte; 27] {
    let mut trytes = [TRYTE_NULL; 27];
    if text.is_empty() {
        return trytes;
    }

    assert!(text.is_ascii());
    let mut ascii = text
        .chars()
        .take(18)
        .map(|c| c as i64)
        .collect::<Vec<i64>>();

    if ascii.len() % 2 != 0 {
        ascii.push(0);
    }

    let mut index;
    let mut tryte_index = 0;
    let mut tryte_triplet;
    for i in (0..(ascii.len() - 1)).step_by(2) {
        index = ascii[i] * 127 + ascii[i + 1] - MAX_TRYTE_TRIPLET_ABS;
        tryte_triplet = from_i64(index, 3);
        trytes[tryte_index] = tryte_triplet[0];
        trytes[tryte_index + 1] = tryte_triplet[1];
        trytes[tryte_index + 2] = tryte_triplet[2];
        tryte_index += 3;
    }

    trytes
}

pub fn from_ascii(text: &str) -> Vec<Tryte> {
    if text.is_empty() {
        return vec![];
    }

    assert!(text.is_ascii());
    let mut ascii = text.chars().map(|c| c as i64).collect::<Vec<i64>>();

    if ascii.len() % 2 != 0 {
        ascii.push(0);
    }

    let mut trytes = vec![TRYTE_NULL; ascii.len() / 2 * 3];

    let mut index;
    let mut tryte_index = 0;
    let mut tryte_triplet;
    for i in (0..(ascii.len() - 1)).step_by(2) {
        index = ascii[i] * 127 + ascii[i + 1] - MAX_TRYTE_TRIPLET_ABS;
        tryte_triplet = from_i64(index, 3);
        trytes[tryte_index] = tryte_triplet[0];
        trytes[tryte_index + 1] = tryte_triplet[1];
        trytes[tryte_index + 2] = tryte_triplet[2];
        tryte_index += 3;
    }

    trytes
}

#[cfg(test)]
mod tests {
    use super::super::ascii;
    use super::super::tryte_string;
    use super::*;
    use crate::model::transaction::Transaction;
    use rand::*;

    #[test]
    fn test_from_ascii_with_empty_str() {
        let trytes = from_ascii("");

        assert_eq!(0, trytes.len());
    }

    #[test]
    fn test_from_tryte_string_to_fragment_trytes_with_empty_str() {
        /*
        println!(
            "{}",
            tryte_string::from_trytes(&from_tryte_string_to_fragment_trytes(""))
        );
        */
        let trytes = from_tryte_string_to_fragment_trytes("");

        assert_eq!(2187, trytes.len());
        assert!(trytes.iter().any(|t| *t == 57));
    }

    #[test]
    fn test_from_tryte_string_to_fragment_trytes() {
        let trytes = from_tryte_string_to_fragment_trytes("ABC");

        assert_eq!(2187, trytes.len());
        assert_eq!(65, trytes[0]);
        assert_eq!(66, trytes[1]);
        assert_eq!(67, trytes[2]);
        assert!(trytes.iter().skip(3).any(|t| *t == 57));
    }

    const MAINNET_TRYTES: &str = "SEGQSWYCJHRLJYEGZLRYQAZPLVRAYIWGWJUMFFX99UZUKBQNFYAOQLOFARIKNEBKDRHJJWDJARXTNPHPAODJRSGJBVVYBVJHZALJWDCJHZRSACOVCVVAVHZVTPFTAJWVGFSVLSYXHNNXEGSMJHDBZKGFQNYJJJBAPDHFFGZ9POSOMWTDPGXI9KQRLMUVWNEQDANMXROVORJVALWVGDDJAFOOBXUKVCCIVXSSHZUCZV9XVBASLWX9NXPWGMGYCRD9ILQMKIGPBGGMKAIJKNALBLABATYFVIRBKTXTWNUZAUXRASB9EEIQHWBD9ZYUDBUPBSWXVYXQXECRCHQAYH9ZBUZBASPOIGBSGWJYFKFRITUBVMCYGCMAPTXOIWEVTUXSUOUPTUQOPMMPUTHXMOP9CW9THAZXEPMOMNEOBLUBPOAIOBEBERRZCIKHSTDWUSUPUWNJOCLNZDCEKWWAAJDPJXJEHHSYFN9MH9BGUDQ9CSZBIHRC9PSQJPGKH9ILZDWUWLEKWFKUFFFIMOQKRMKOYXEJHXLCEGCGGKHGJUHOXINSWCKRNMUNAJDCVLZGEBII9ASTYFTDYDZIZSNHIWHSQ9HODQMVNDKMKHCFDXIIGDIVJSBOOE9GRIXCD9ZUTWCUDKFTETSYSRBQABXCXZFOWQMQFXHYZWD9JZXUWHILMRNWXSGUMIIXZYCTWWHCWMSSTCNSQXQXMQPTM9MOQMIVDYNNARDCVNQEDTBKWOIOSKPKPOZHJGJJGNYWQWUWAZMBZJ9XEJMRVRYFQPJ9NOIIXEGIKMMN9DXYQUILRSCSJDIDN9DCTFGQIYWROZQIEQTKMRVLGGDGA9UVZPNRGSVTZYAPMWFUWDEUULSEEGAGITPJQ9DBEYEN9NVJPUWZTOTJHEQIXAPDOICBNNCJVDNM9YRNXMMPCOYHJDUFNCYTZGRCBZKOLHHUK9VOZWHEYQND9WUHDNGFTAS99MRCAU9QOYVUZKTIBDNAAPNEZBQPIRUFUMAWVTCXSXQQIYQPRFDUXCLJNMEIKVAINVCCZROEWEX9XVRM9IHLHQCKC9VLK9ZZWFBJUZKGJCSOPQPFVVAUDLKFJIJKMLZXFBMXLMWRSNDXRMMDLE9VBPUZB9SVLTMHA9DDDANOKIPY9ULDWAKOUDFEDHZDKMU9VMHUSFG9HRGZAZULEJJTEH9SLQDOMZTLVMBCXVNQPNKXRLBOUCCSBZRJCZIUFTFBKFVLKRBPDKLRLZSMMIQNMOZYFBGQFKUJYIJULGMVNFYJWPKPTSMYUHSUEXIPPPPPJTMDQLFFSFJFEPNUBDEDDBPGAOEJGQTHIWISLRDAABO9H9CSIAXPPJYCRFRCIH9TVBZKTCK9SPQZUYMUOKMZYOMPRHRGF9UAKZTZZG9VVVTIHMSNDREUOUOSLKUHTNFXTNSJVPVWCQXUDIMJIAMBPXUGBNDTBYPKYQYJJCDJSCTTWHOJKORLHGKRJMDCMRHSXHHMQBFJWZWHNUHZLYOAFQTRZFXDBYASYKWEVHKYDTJIAUKNCCEPSW9RITZXBOFKBAQOWHKTALQSCHARLUUGXISDMBVEUKOVXTKTEVKLGYVYHPNYWKNLCVETWIHHVTBWT9UPMTQWBZPRPRSISUBIBECVDNIZQULAGLONGVFLVZPBMHJND9CEVIXSYGFZAGGN9MQYOAKMENSEOGCUNKEJTDLEDCD9LGKYANHMZFSSDDZJKTKUJSFL9GYFDICTPJEPDSBXDQTARJQEWUVWDWSQPKIHPJONKHESSQH9FNQEO9WUCFDWPPPTIQPWCVDYTTWPLCJJVYNKE9ZEJNQBEJBMDBLNJKQDOQOHVS9VY9UPSU9KZVDFOESHNRRWBK9EZCYALAUYFGPCEWJQDXFENSNQEAUWDXJGOMCLQUQWMCPHOBZZ9SZJ9KZXSHDLPHPNYMVUJQSQETTN9SG9SIANJHWUYQXZXAJLYHCZYRGITZYQLAAYDVQVNKCDIYWAYBAFBMAYEAEAGMTJGJRSNHBHCEVIQRXEFVWJWOPU9FPDOWIFL9EWGHICRBNRITJDZNYACOGTUDBZYIYZZWAOCDBQFFNTTSTGKECWTVWZSPHX9HNRUYEAEWXENEIDLVVFMZFVPUNHMQPAIOKVIBDIHQIHFGRJOHHONPLGBSJUD9HHDTQQUZN9NVJYOAUMXMMOCNUFLZ9BAJSZMDMPQHPWSFVWOJQDPHV9DYSQPIBL9LYZHQKKOVF9TFVTTXQEUWFQSLGLVTGK99VSUEDXIBIWCQHDQQSQLDHZ9999999999999999999TRINITY99999999999999999999TNXSQ9D99A99999999B99999999MXKZAGDGKVADXOVCAXEQYZGOGQKDLKIUPYXIL9PXYBQXGYDEGNXTFURSWQYLJDFKEV9VVBBQLTLHIBTFYOGBHPUUHS9CKWSAPIMDIRNSUJ9CFPGKTUFAGQYVMFKOZSVAHIFJXWCFBZLICUWF9GNDZWCOWDUIIZ9999OXNRVXLBKJXEZMVABR9UQBVSTBDFSAJVRRNFEJRL9UFTOFPJHQMQKAJHDBIQAETS9OUVTQ9DSPAOZ9999TRINITY99999999999999999999LPZYMWQME999999999MMMMMMMMMDTIZE9999999999999999999999";
    fn get_example_tx_tryte_string() -> String {
        let sig_msg_frag = MAINNET_TRYTES.get(0..2187).unwrap();
        let extra_data_digest = MAINNET_TRYTES.get((2187 + 162)..(2187 + 162 + 81)).unwrap(); //copied bundle hash
        let addr_value_tag_timestamps = MAINNET_TRYTES.get(2187..(2187 + 162)).unwrap();
        let rest = MAINNET_TRYTES.get((2187 + 162 + 81)..).unwrap();

        format!(
            "{}{}{}{}",
            sig_msg_frag, extra_data_digest, addr_value_tag_timestamps, rest
        )
    }
}
