use super::luts::*;
use super::trits::*;
use super::trytes::*;
use crate::constants::*;

pub type Byte = u8;
pub type TxBytes = [Byte; TRANSACTION_SIZE_BYTES];
pub type Bytes54 = [Byte; 54];

const NINE: u8 = TRYTE_TO_ASCII[0];
const A: u8 = TRYTE_TO_ASCII[1];
const TRANSACTION_SIZE_TRITS_DIV_9: usize = TRANSACTION_SIZE_TRITS / 9;

pub fn from_trytes_2enc9(trytes: &[Tryte]) -> Vec<Byte> {
    assert_eq!(0, trytes.len() % 3);

    let mut bytes = vec![0u8; trytes.len() / 3 * 2];

    for i in 0..trytes.len() / 3 {
        let t0 = trytes[3 * i];
        let t1 = trytes[3 * i + 1];
        let t2 = trytes[3 * i + 2];

        let i0 = if t0 == NINE { 0 } else { t0 - A + 1 };
        let i1 = if t1 == NINE { 0 } else { t1 - A + 1 };
        let i2 = if t2 == NINE { 0 } else { t2 - A + 1 };

        bytes[2 * i] = i0 * 8 + i2 % 8;
        bytes[2 * i + 1] = i1 * 8 + i2 / 8;
    }
    bytes
}

pub fn from_tx_trytes_2enc9(trytes: &TxTrytes) -> TxBytes {
    let mut bytes = [0u8; TRANSACTION_SIZE_BYTES];

    for i in 0..trytes.len() / 3 {
        let t0 = trytes[3 * i];
        let t1 = trytes[3 * i + 1];
        let t2 = trytes[3 * i + 2];

        let i0 = if t0 == NINE { 0 } else { t0 - A + 1 };
        let i1 = if t1 == NINE { 0 } else { t1 - A + 1 };
        let i2 = if t2 == NINE { 0 } else { t2 - A + 1 };

        bytes[2 * i] = i0 * 8 + i2 % 8;
        bytes[2 * i + 1] = i1 * 8 + i2 / 8;
    }
    bytes
}

pub fn from_81_trytes_2enc9(trytes: &[Tryte]) -> Bytes54 {
    let mut bytes = [0u8; 54];

    for i in 0..27 {
        let t0 = trytes[3 * i];
        let t1 = trytes[3 * i + 1];
        let t2 = trytes[3 * i + 2];

        let i0 = if t0 == NINE { 0 } else { t0 - A + 1 };
        let i1 = if t1 == NINE { 0 } else { t1 - A + 1 };
        let i2 = if t2 == NINE { 0 } else { t2 - A + 1 };

        bytes[2 * i] = i0 * 8 + i2 % 8;
        bytes[2 * i + 1] = i1 * 8 + i2 / 8;
    }
    bytes
}

pub fn from_tx_trits_2enc9(trits: &TxTrits) -> TxBytes {
    let mut bytes = [0u8; TRANSACTION_SIZE_BYTES];

    for i in 0..TRANSACTION_SIZE_TRITS_DIV_9 {
        let mut i0 = trits[9 * i] + 3 * trits[9 * i + 1] + 9 * trits[9 * i + 2];
        let mut i1 = trits[9 * i + 3] + 3 * trits[9 * i + 4] + 9 * trits[9 * i + 5];
        let mut i2 = trits[9 * i + 6] + 3 * trits[9 * i + 7] + 9 * trits[9 * i + 8];

        i0 = if i0 < 0 { i0 + 27 } else { i0 };
        i1 = if i1 < 0 { i1 + 27 } else { i1 };
        i2 = if i2 < 0 { i2 + 27 } else { i2 };

        bytes[2 * i] = (i0 * 8 + i2 % 8) as u8;
        bytes[2 * i + 1] = (i1 * 8 + i2 / 8) as u8;
    }
    bytes
}

pub fn from_243_trits_2enc9(trits: &[i8; 243]) -> [u8; 54] {
    let mut bytes = [0u8; 54];

    for i in 0..27 {
        let mut i0 = trits[9 * i] + 3 * trits[9 * i + 1] + 9 * trits[9 * i + 2];
        let mut i1 = trits[9 * i + 3] + 3 * trits[9 * i + 4] + 9 * trits[9 * i + 5];
        let mut i2 = trits[9 * i + 6] + 3 * trits[9 * i + 7] + 9 * trits[9 * i + 8];

        i0 = if i0 < 0 { i0 + 27 } else { i0 };
        i1 = if i1 < 0 { i1 + 27 } else { i1 };
        i2 = if i2 < 0 { i2 + 27 } else { i2 };

        bytes[2 * i] = (i0 * 8 + i2 % 8) as u8;
        bytes[2 * i + 1] = (i1 * 8 + i2 / 8) as u8;
    }
    bytes
}

#[cfg(test)]
mod tests {
    use super::super::trytes;
    use super::*;

    const MAINNET_TRYTES: &str = "SEGQSWYCJHRLJYEGZLRYQAZPLVRAYIWGWJUMFFX99UZUKBQNFYAOQLOFARIKNEBKDRHJJWDJARXTNPHPAODJRSGJBVVYBVJHZALJWDCJHZRSACOVCVVAVHZVTPFTAJWVGFSVLSYXHNNXEGSMJHDBZKGFQNYJJJBAPDHFFGZ9POSOMWTDPGXI9KQRLMUVWNEQDANMXROVORJVALWVGDDJAFOOBXUKVCCIVXSSHZUCZV9XVBASLWX9NXPWGMGYCRD9ILQMKIGPBGGMKAIJKNALBLABATYFVIRBKTXTWNUZAUXRASB9EEIQHWBD9ZYUDBUPBSWXVYXQXECRCHQAYH9ZBUZBASPOIGBSGWJYFKFRITUBVMCYGCMAPTXOIWEVTUXSUOUPTUQOPMMPUTHXMOP9CW9THAZXEPMOMNEOBLUBPOAIOBEBERRZCIKHSTDWUSUPUWNJOCLNZDCEKWWAAJDPJXJEHHSYFN9MH9BGUDQ9CSZBIHRC9PSQJPGKH9ILZDWUWLEKWFKUFFFIMOQKRMKOYXEJHXLCEGCGGKHGJUHOXINSWCKRNMUNAJDCVLZGEBII9ASTYFTDYDZIZSNHIWHSQ9HODQMVNDKMKHCFDXIIGDIVJSBOOE9GRIXCD9ZUTWCUDKFTETSYSRBQABXCXZFOWQMQFXHYZWD9JZXUWHILMRNWXSGUMIIXZYCTWWHCWMSSTCNSQXQXMQPTM9MOQMIVDYNNARDCVNQEDTBKWOIOSKPKPOZHJGJJGNYWQWUWAZMBZJ9XEJMRVRYFQPJ9NOIIXEGIKMMN9DXYQUILRSCSJDIDN9DCTFGQIYWROZQIEQTKMRVLGGDGA9UVZPNRGSVTZYAPMWFUWDEUULSEEGAGITPJQ9DBEYEN9NVJPUWZTOTJHEQIXAPDOICBNNCJVDNM9YRNXMMPCOYHJDUFNCYTZGRCBZKOLHHUK9VOZWHEYQND9WUHDNGFTAS99MRCAU9QOYVUZKTIBDNAAPNEZBQPIRUFUMAWVTCXSXQQIYQPRFDUXCLJNMEIKVAINVCCZROEWEX9XVRM9IHLHQCKC9VLK9ZZWFBJUZKGJCSOPQPFVVAUDLKFJIJKMLZXFBMXLMWRSNDXRMMDLE9VBPUZB9SVLTMHA9DDDANOKIPY9ULDWAKOUDFEDHZDKMU9VMHUSFG9HRGZAZULEJJTEH9SLQDOMZTLVMBCXVNQPNKXRLBOUCCSBZRJCZIUFTFBKFVLKRBPDKLRLZSMMIQNMOZYFBGQFKUJYIJULGMVNFYJWPKPTSMYUHSUEXIPPPPPJTMDQLFFSFJFEPNUBDEDDBPGAOEJGQTHIWISLRDAABO9H9CSIAXPPJYCRFRCIH9TVBZKTCK9SPQZUYMUOKMZYOMPRHRGF9UAKZTZZG9VVVTIHMSNDREUOUOSLKUHTNFXTNSJVPVWCQXUDIMJIAMBPXUGBNDTBYPKYQYJJCDJSCTTWHOJKORLHGKRJMDCMRHSXHHMQBFJWZWHNUHZLYOAFQTRZFXDBYASYKWEVHKYDTJIAUKNCCEPSW9RITZXBOFKBAQOWHKTALQSCHARLUUGXISDMBVEUKOVXTKTEVKLGYVYHPNYWKNLCVETWIHHVTBWT9UPMTQWBZPRPRSISUBIBECVDNIZQULAGLONGVFLVZPBMHJND9CEVIXSYGFZAGGN9MQYOAKMENSEOGCUNKEJTDLEDCD9LGKYANHMZFSSDDZJKTKUJSFL9GYFDICTPJEPDSBXDQTARJQEWUVWDWSQPKIHPJONKHESSQH9FNQEO9WUCFDWPPPTIQPWCVDYTTWPLCJJVYNKE9ZEJNQBEJBMDBLNJKQDOQOHVS9VY9UPSU9KZVDFOESHNRRWBK9EZCYALAUYFGPCEWJQDXFENSNQEAUWDXJGOMCLQUQWMCPHOBZZ9SZJ9KZXSHDLPHPNYMVUJQSQETTN9SG9SIANJHWUYQXZXAJLYHCZYRGITZYQLAAYDVQVNKCDIYWAYBAFBMAYEAEAGMTJGJRSNHBHCEVIQRXEFVWJWOPU9FPDOWIFL9EWGHICRBNRITJDZNYACOGTUDBZYIYZZWAOCDBQFFNTTSTGKECWTVWZSPHX9HNRUYEAEWXENEIDLVVFMZFVPUNHMQPAIOKVIBDIHQIHFGRJOHHONPLGBSJUD9HHDTQQUZN9NVJYOAUMXMMOCNUFLZ9BAJSZMDMPQHPWSFVWOJQDPHV9DYSQPIBL9LYZHQKKOVF9TFVTTXQEUWFQSLGLVTGK99VSUEDXIBIWCQHDQQSQLDHZ9999999999999999999TRINITY99999999999999999999TNXSQ9D99A99999999B99999999MXKZAGDGKVADXOVCAXEQYZGOGQKDLKIUPYXIL9PXYBQXGYDEGNXTFURSWQYLJDFKEV9VVBBQLTLHIBTFYOGBHPUUHS9CKWSAPIMDIRNSUJ9CFPGKTUFAGQYVMFKOZSVAHIFJXWCFBZLICUWF9GNDZWCOWDUIIZ9999OXNRVXLBKJXEZMVABR9UQBVSTBDFSAJVRRNFEJRL9UFTOFPJHQMQKAJHDBIQAETS9OUVTQ9DSPAOZ9999TRINITY99999999999999999999LPZYMWQME999999999MMMMMMMMMDTIZE9999999999999999999999";
    fn get_example_trytes() -> String {
        let sig_msg_frag = MAINNET_TRYTES.get(0..2187).unwrap();
        let extra_data_digest = MAINNET_TRYTES.get((2187 + 162)..(2187 + 162 + 81)).unwrap(); //copied bundle hash
        let addr_value_tag_timestamps = MAINNET_TRYTES.get(2187..(2187 + 162)).unwrap();
        let rest = MAINNET_TRYTES.get((2187 + 162 + 81)..).unwrap();

        format!(
            "{}{}{}{}",
            sig_msg_frag, extra_data_digest, addr_value_tag_timestamps, rest
        )
    }

    #[test]
    fn test_from_to_tx_bytes() {
        let example_tryte_string = get_example_trytes();
        let example_trytes = example_tryte_string.as_bytes();
        let mut tx_trytes = [0; TRANSACTION_SIZE_TRYTES];
        tx_trytes[..].copy_from_slice(&example_trytes[..]);

        assert_eq!(TRANSACTION_SIZE_TRYTES, example_trytes.len());
        assert_eq!(
            example_tryte_string,
            String::from_utf8(
                trytes::from_tx_bytes_2enc9(&from_tx_trytes_2enc9(&tx_trytes)).to_vec()
            )
            .unwrap()
        );
    }
}
