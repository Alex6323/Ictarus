use crate::constants::*;
use crate::convert::bytes::{self, *};
use crate::convert::number;
use crate::convert::trits::{self, *};
use crate::convert::tryte_string;
use crate::convert::trytes::{self, *};
use crate::crypto::curl;
use crate::time;

pub const MAX_TIME_TRYTE_LENGTH: usize = 9;

#[derive(Clone, Debug)]
pub struct Transaction {
    pub signature_fragments: String,
    pub extra_data_digest: String,
    pub address: String,
    pub value: i64,
    pub issuance_timestamp: i64,
    pub timelock_lower_bound: i64,
    pub timelock_upper_bound: i64,
    pub bundle_nonce: String,
    pub trunk: String,
    pub branch: String,
    pub tag: String,
    pub attachment_timestamp: i64,
    pub attachment_timestamp_lower_bound: i64,
    pub attachment_timestamp_upper_bound: i64,
    pub nonce: String,
}

impl Transaction {
    pub fn from_tx_bytes(bytes: &[u8]) -> Self {
        Transaction::from_tx_trytes(&trytes::from_tx_bytes_2enc9(bytes))
    }

    pub fn from_tryte_string(tryte_string: &str) -> Self {
        assert!(IS_TRYTES.is_match(tryte_string));
        let bytes = tryte_string.as_bytes();
        assert_eq!(bytes.len(), TRANSACTION_SIZE_TRYTES);

        let mut trytes = [0; TRANSACTION_SIZE_TRYTES];
        trytes[..].copy_from_slice(&bytes[..]);

        Transaction::from_tx_trytes(&trytes)
    }

    pub fn from_tx_trytes(trytes: &TxTrytes) -> Self {
        let signature_fragments =
            tryte_string::from_trytes(&trytes[SIGNATURE_FRAGMENTS.2..EXTRA_DATA_DIGEST.2]);

        let extra_data_digest = tryte_string::from_trytes(&trytes[EXTRA_DATA_DIGEST.2..ADDRESS.2]);

        let address = tryte_string::from_trytes(&trytes[ADDRESS.2..VALUE.2]);

        let value = number::i64_from_trytes_max11(&trytes[VALUE.2..ISSUANCE_TIMESTAMP.2]);

        let issuance_timestamp =
            number::i64_from_trytes_max11(&trytes[ISSUANCE_TIMESTAMP.2..TIMELOCK_LOWER_BOUND.2]);

        let timelock_lower_bound =
            number::i64_from_trytes_max11(&trytes[TIMELOCK_LOWER_BOUND.2..TIMELOCK_UPPER_BOUND.2]);

        let timelock_upper_bound =
            number::i64_from_trytes_max11(&trytes[TIMELOCK_UPPER_BOUND.2..BUNDLE_NONCE.2]);

        let bundle_nonce = tryte_string::from_trytes(&trytes[BUNDLE_NONCE.2..TRUNK_HASH.2]);

        let trunk = tryte_string::from_trytes(&trytes[TRUNK_HASH.2..BRANCH_HASH.2]);

        let branch = tryte_string::from_trytes(&trytes[BRANCH_HASH.2..TAG.2]);

        let tag = tryte_string::from_trytes(&trytes[TAG.2..ATTACHMENT_TIMESTAMP.2]);

        let attachment_timestamp = number::i64_from_trytes_max11(
            &trytes[ATTACHMENT_TIMESTAMP.2..ATTACHMENT_TIMESTAMP_LOWER_BOUND.2],
        );

        let attachment_timestamp_lower_bound = number::i64_from_trytes_max11(
            &trytes[ATTACHMENT_TIMESTAMP_LOWER_BOUND.2..ATTACHMENT_TIMESTAMP_UPPER_BOUND.2],
        );

        let attachment_timestamp_upper_bound =
            number::i64_from_trytes_max11(&trytes[ATTACHMENT_TIMESTAMP_UPPER_BOUND.2..NONCE.2]);

        let nonce = tryte_string::from_trytes(&trytes[NONCE.2..TRANSACTION_SIZE_TRYTES]);

        Transaction {
            signature_fragments,
            extra_data_digest,
            address,
            value,
            issuance_timestamp,
            timelock_lower_bound,
            timelock_upper_bound,
            bundle_nonce,
            trunk,
            branch,
            tag,
            attachment_timestamp,
            attachment_timestamp_lower_bound,
            attachment_timestamp_upper_bound,
            nonce,
        }
    }

    pub fn as_bytes(&self) -> TxBytes {
        bytes::from_tx_trytes_2enc9(&self.as_trytes())
    }

    pub fn as_tryte_string(&self) -> String {
        tryte_string::from_trytes(&self.as_trytes())
    }

    pub fn as_trits(&self) -> TxTrits {
        trits::from_tx_trytes(&self.as_trytes())
    }

    pub fn as_trytes(&self) -> TxTrytes {
        let mut trytes = [0; TRANSACTION_SIZE_TRYTES];

        trytes[SIGNATURE_FRAGMENTS.2..EXTRA_DATA_DIGEST.2].copy_from_slice(
            &trytes::from_tryte_string_to_fragment_trytes(&self.signature_fragments)
                [..SIGNATURE_FRAGMENTS.3],
        );

        trytes[EXTRA_DATA_DIGEST.2..ADDRESS.2]
            .copy_from_slice(&self.extra_data_digest.as_bytes()[..EXTRA_DATA_DIGEST.3]);

        trytes[ADDRESS.2..VALUE.2].copy_from_slice(&self.address.as_bytes()[..ADDRESS.3]);

        trytes[VALUE.2..ISSUANCE_TIMESTAMP.2].copy_from_slice(&from_i64_fixed27(self.value));

        trytes[ISSUANCE_TIMESTAMP.2..TIMELOCK_LOWER_BOUND.2]
            .copy_from_slice(&from_i64_fixed9(self.issuance_timestamp));

        trytes[TIMELOCK_LOWER_BOUND.2..TIMELOCK_UPPER_BOUND.2]
            .copy_from_slice(&from_i64_fixed9(self.timelock_lower_bound));

        trytes[TIMELOCK_UPPER_BOUND.2..BUNDLE_NONCE.2]
            .copy_from_slice(&from_i64_fixed9(self.timelock_upper_bound));

        trytes[BUNDLE_NONCE.2..TRUNK_HASH.2]
            .copy_from_slice(&self.bundle_nonce.as_bytes()[..BUNDLE_NONCE.3]);

        trytes[TRUNK_HASH.2..BRANCH_HASH.2].copy_from_slice(&self.trunk.as_bytes()[..TRUNK_HASH.3]);

        trytes[BRANCH_HASH.2..TAG.2].copy_from_slice(&self.branch.as_bytes()[..BRANCH_HASH.3]);

        trytes[TAG.2..ATTACHMENT_TIMESTAMP.2].copy_from_slice(&self.tag.as_bytes()[0..TAG.3]);

        trytes[ATTACHMENT_TIMESTAMP.2..ATTACHMENT_TIMESTAMP_LOWER_BOUND.2]
            .copy_from_slice(&from_i64_fixed9(self.attachment_timestamp));

        trytes[ATTACHMENT_TIMESTAMP_LOWER_BOUND.2..ATTACHMENT_TIMESTAMP_UPPER_BOUND.2]
            .copy_from_slice(&from_i64_fixed9(self.attachment_timestamp_lower_bound));

        trytes[ATTACHMENT_TIMESTAMP_UPPER_BOUND.2..NONCE.2]
            .copy_from_slice(&from_i64_fixed9(self.attachment_timestamp_upper_bound));

        trytes[NONCE.2..TRANSACTION_SIZE_TRYTES].copy_from_slice(&self.nonce.as_bytes()[..NONCE.3]);

        trytes
    }

    pub fn get_hash(&self) -> Trytes81 {
        trytes::from_trits_fixed81(&curl::curl_tx(
            self.as_trits(),
            CURL_ROUNDS_TRANSACTION_HASH,
        ))
    }

    pub fn message(mut self, message: &str) -> Self {
        assert!(message.len() <= SIGNATURE_FRAGMENTS.3);

        self.signature_fragments =
            tryte_string::pad_right(&tryte_string::from_ascii(message), SIGNATURE_FRAGMENTS.3);
        self
    }

    pub fn tag(mut self, tag: &str) -> Self {
        assert!(IS_TRYTES.is_match(tag));
        assert!(tag.len() <= TAG.3);

        self.tag = tryte_string::pad_right(tag, TAG.3);
        self
    }
}

impl Default for Transaction {
    fn default() -> Self {
        let timestamp = time::get_unix_time_millis();

        Transaction {
            signature_fragments: TRYTE_NULL_STR.repeat(SIGNATURE_FRAGMENTS.3),
            extra_data_digest: TRYTE_NULL_STR.repeat(EXTRA_DATA_DIGEST.3),
            address: TRYTE_NULL_STR.repeat(ADDRESS.3),
            value: 0,
            issuance_timestamp: timestamp,
            timelock_lower_bound: 0,
            timelock_upper_bound: 0,
            bundle_nonce: TRYTE_NULL_STR.repeat(BUNDLE_NONCE.3),
            trunk: TRYTE_NULL_STR.repeat(TRUNK_HASH.3),
            branch: TRYTE_NULL_STR.repeat(BRANCH_HASH.3),
            tag: TRYTE_NULL_STR.repeat(TAG.3),
            attachment_timestamp: timestamp,
            attachment_timestamp_lower_bound: 0,
            attachment_timestamp_upper_bound: 0,
            nonce: TRYTE_NULL_STR.repeat(NONCE.3),
        }
    }
}

pub struct TransactionBuilder {
    transaction: Transaction,
}

impl TransactionBuilder {
    pub fn default() -> Self {
        TransactionBuilder {
            transaction: Transaction::default(),
        }
    }
    pub fn value(mut self, value: i64) -> Self {
        assert!(value.abs() <= MAX_TOKEN_SUPPLY);

        self.transaction.value = value;
        self
    }
    pub fn trunk(mut self, trunk: &str) -> Self {
        assert!(IS_TRYTES.is_match(trunk));
        assert!(trunk.len() <= TRUNK_HASH.3);

        self.transaction.trunk = trunk.to_string();
        self
    }
    pub fn branch(mut self, branch: &str) -> Self {
        assert!(IS_TRYTES.is_match(branch));
        assert!(branch.len() <= BRANCH_HASH.3);

        self.transaction.branch = branch.to_string();
        self
    }
    pub fn message(mut self, message: &str) -> Self {
        assert!(message.len() <= SIGNATURE_FRAGMENTS.3);

        self.transaction.signature_fragments =
            tryte_string::pad_right(&tryte_string::from_ascii(message), SIGNATURE_FRAGMENTS.3);
        self
    }
    pub fn tag(mut self, tag: &str) -> Self {
        assert!(IS_TRYTES.is_match(tag));
        assert!(tag.len() <= TAG.3);

        self.transaction.tag = tryte_string::pad_right(tag, TAG.3);
        self
    }
    pub fn build(self) -> Transaction {
        self.transaction
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::convert::{tryte_string, trytes};
    use crate::crypto::curl;
    use rayon::prelude::*;
    use std::time::{Duration, Instant};

    // first we need to convert mainnet trytes to ict trytes
    // NOTE: length is already 2754 (instead of 2673 on the mainnet)
    const MAINNET_TRYTES: &str = "SEGQSWYCJHRLJYEGZLRYQAZPLVRAYIWGWJUMFFX99UZUKBQNFYAOQLOFARIKNEBKDRHJJWDJARXTNPHPAODJRSGJBVVYBVJHZALJWDCJHZRSACOVCVVAVHZVTPFTAJWVGFSVLSYXHNNXEGSMJHDBZKGFQNYJJJBAPDHFFGZ9POSOMWTDPGXI9KQRLMUVWNEQDANMXROVORJVALWVGDDJAFOOBXUKVCCIVXSSHZUCZV9XVBASLWX9NXPWGMGYCRD9ILQMKIGPBGGMKAIJKNALBLABATYFVIRBKTXTWNUZAUXRASB9EEIQHWBD9ZYUDBUPBSWXVYXQXECRCHQAYH9ZBUZBASPOIGBSGWJYFKFRITUBVMCYGCMAPTXOIWEVTUXSUOUPTUQOPMMPUTHXMOP9CW9THAZXEPMOMNEOBLUBPOAIOBEBERRZCIKHSTDWUSUPUWNJOCLNZDCEKWWAAJDPJXJEHHSYFN9MH9BGUDQ9CSZBIHRC9PSQJPGKH9ILZDWUWLEKWFKUFFFIMOQKRMKOYXEJHXLCEGCGGKHGJUHOXINSWCKRNMUNAJDCVLZGEBII9ASTYFTDYDZIZSNHIWHSQ9HODQMVNDKMKHCFDXIIGDIVJSBOOE9GRIXCD9ZUTWCUDKFTETSYSRBQABXCXZFOWQMQFXHYZWD9JZXUWHILMRNWXSGUMIIXZYCTWWHCWMSSTCNSQXQXMQPTM9MOQMIVDYNNARDCVNQEDTBKWOIOSKPKPOZHJGJJGNYWQWUWAZMBZJ9XEJMRVRYFQPJ9NOIIXEGIKMMN9DXYQUILRSCSJDIDN9DCTFGQIYWROZQIEQTKMRVLGGDGA9UVZPNRGSVTZYAPMWFUWDEUULSEEGAGITPJQ9DBEYEN9NVJPUWZTOTJHEQIXAPDOICBNNCJVDNM9YRNXMMPCOYHJDUFNCYTZGRCBZKOLHHUK9VOZWHEYQND9WUHDNGFTAS99MRCAU9QOYVUZKTIBDNAAPNEZBQPIRUFUMAWVTCXSXQQIYQPRFDUXCLJNMEIKVAINVCCZROEWEX9XVRM9IHLHQCKC9VLK9ZZWFBJUZKGJCSOPQPFVVAUDLKFJIJKMLZXFBMXLMWRSNDXRMMDLE9VBPUZB9SVLTMHA9DDDANOKIPY9ULDWAKOUDFEDHZDKMU9VMHUSFG9HRGZAZULEJJTEH9SLQDOMZTLVMBCXVNQPNKXRLBOUCCSBZRJCZIUFTFBKFVLKRBPDKLRLZSMMIQNMOZYFBGQFKUJYIJULGMVNFYJWPKPTSMYUHSUEXIPPPPPJTMDQLFFSFJFEPNUBDEDDBPGAOEJGQTHIWISLRDAABO9H9CSIAXPPJYCRFRCIH9TVBZKTCK9SPQZUYMUOKMZYOMPRHRGF9UAKZTZZG9VVVTIHMSNDREUOUOSLKUHTNFXTNSJVPVWCQXUDIMJIAMBPXUGBNDTBYPKYQYJJCDJSCTTWHOJKORLHGKRJMDCMRHSXHHMQBFJWZWHNUHZLYOAFQTRZFXDBYASYKWEVHKYDTJIAUKNCCEPSW9RITZXBOFKBAQOWHKTALQSCHARLUUGXISDMBVEUKOVXTKTEVKLGYVYHPNYWKNLCVETWIHHVTBWT9UPMTQWBZPRPRSISUBIBECVDNIZQULAGLONGVFLVZPBMHJND9CEVIXSYGFZAGGN9MQYOAKMENSEOGCUNKEJTDLEDCD9LGKYANHMZFSSDDZJKTKUJSFL9GYFDICTPJEPDSBXDQTARJQEWUVWDWSQPKIHPJONKHESSQH9FNQEO9WUCFDWPPPTIQPWCVDYTTWPLCJJVYNKE9ZEJNQBEJBMDBLNJKQDOQOHVS9VY9UPSU9KZVDFOESHNRRWBK9EZCYALAUYFGPCEWJQDXFENSNQEAUWDXJGOMCLQUQWMCPHOBZZ9SZJ9KZXSHDLPHPNYMVUJQSQETTN9SG9SIANJHWUYQXZXAJLYHCZYRGITZYQLAAYDVQVNKCDIYWAYBAFBMAYEAEAGMTJGJRSNHBHCEVIQRXEFVWJWOPU9FPDOWIFL9EWGHICRBNRITJDZNYACOGTUDBZYIYZZWAOCDBQFFNTTSTGKECWTVWZSPHX9HNRUYEAEWXENEIDLVVFMZFVPUNHMQPAIOKVIBDIHQIHFGRJOHHONPLGBSJUD9HHDTQQUZN9NVJYOAUMXMMOCNUFLZ9BAJSZMDMPQHPWSFVWOJQDPHV9DYSQPIBL9LYZHQKKOVF9TFVTTXQEUWFQSLGLVTGK99VSUEDXIBIWCQHDQQSQLDHZ9999999999999999999TRINITY99999999999999999999TNXSQ9D99A99999999B99999999MXKZAGDGKVADXOVCAXEQYZGOGQKDLKIUPYXIL9PXYBQXGYDEGNXTFURSWQYLJDFKEV9VVBBQLTLHIBTFYOGBHPUUHS9CKWSAPIMDIRNSUJ9CFPGKTUFAGQYVMFKOZSVAHIFJXWCFBZLICUWF9GNDZWCOWDUIIZ9999OXNRVXLBKJXEZMVABR9UQBVSTBDFSAJVRRNFEJRL9UFTOFPJHQMQKAJHDBIQAETS9OUVTQ9DSPAOZ9999TRINITY99999999999999999999LPZYMWQME999999999MMMMMMMMMDTIZE9999999999999999999999";
    const EXAMPLE_ADDR: &str =
        "BAJSZMDMPQHPWSFVWOJQDPHV9DYSQPIBL9LYZHQKKOVF9TFVTTXQEUWFQSLGLVTGK99VSUEDXIBIWCQHD";

    #[test]
    fn test_transaction_decoding() {
        let example_trytes = get_example_trytes();
        let tx = Transaction::from_tryte_string(&example_trytes);

        assert_eq!(EXAMPLE_ADDR, tx.address);
        assert_eq!(-7_297_419_313, tx.value);
        assert_eq!(1_544_207_541_879, tx.attachment_timestamp);
    }

    #[test]
    fn test_transaction_encoding_decoding() {
        let orig = Transaction::from_tryte_string(&get_example_trytes());
        let copy = Transaction::from_tryte_string(&orig.as_tryte_string());

        assert_eq!(orig.address, copy.address);
        assert_eq!(orig.tag, copy.tag);
        assert_eq!(orig.value, copy.value);
        assert_eq!(orig.as_tryte_string(), copy.as_tryte_string());

        let trits = trits::from_tx_trytes(&orig.as_trytes());

        let orig_hash = tryte_string::from_trits_243(&curl::curl_tx(orig.as_trits(), 123));
        let copy_hash = tryte_string::from_trits_243(&curl::curl_tx(copy.as_trits(), 123));

        assert_eq!(orig_hash, copy_hash);
    }

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

    /// This test creates 1000 different transactions and hashes them sequentially with the default
    /// transaction hashing algorithm (at the time of writing: Curl-27).
    ///
    /// Use `cargo test bench_create_1000_transactions_with_hash` --release -- --nocapture
    /// to get production results.
    ///
    /// Last results:
    ///     ~542 ms (roughly 2000 PoW-less tps)
    #[test]
    fn bench_create_1000_transactions_with_hash() {
        let start = Instant::now();
        for i in 0..1000 {
            let hash = Transaction::default().message(&i.to_string()).get_hash();
        }
        let stop = start.elapsed();

        println!(
            "{} ms",
            stop.as_secs() * 1000 + u64::from(stop.subsec_millis())
        );
    }

    /// Same as test before, but parallel using 'rayon'.
    ///
    /// Last results:
    ///     ~203 ms (roughly 5000 PoW-less tps)
    #[test]
    fn bench_create_1000_transactions_with_hash_par() {
        let start = Instant::now();
        (0..1000_u32).into_par_iter().for_each(|i: u32| {
            let hash = Transaction::default().message(&i.to_string()).get_hash();
        });
        let stop = start.elapsed();

        println!(
            "{} ms",
            stop.as_secs() * 1000 + u64::from(stop.subsec_millis())
        );
    }
}
