use lazy_static::lazy_static;
use regex::Regex;

// App
pub const APP_NAME: &str = "Ictarus";
pub const APP_VERSION: &str = "0.1.0";
pub const APP_ENV_VAR: &str = "ICTARUS_LOG";
pub const DEBUG_LEVEL: &str = "debug"; // !!!
pub const HEATING_UP_MS: u64 = 1000;
pub const COOLING_DOWN_MS: u64 = 1000;
pub const SOCKET_POLL_SLEEP_MS: u64 = 50; // !!!
pub const MAX_NEIGHBOR_COUNT: usize = 3;

// Config
pub const CONFIG_FILEPATH: &str = "./ictarus.cfg";
pub const MIN_FORWARD_DELAY_PARAM: &str = "min_forward_delay";
pub const MAX_FORWARD_DELAY_PARAM: &str = "max_forward_delay";
pub const PORT_PARAM: &str = "port";
pub const HOST_PARAM: &str = "host";
pub const ROUND_DURATION_PARAM: &str = "round_duration";
pub const NEIGHBORS_PARAM: &str = "neighbors";
pub const TANGLE_CAPACITY: usize = 10000;

// Transaction (trits offset, trits length, trytes offset, trytes length, bytes offset, bytes length)
type Field = (usize, usize, usize, usize, usize, usize);
pub const SIGNATURE_FRAGMENTS: Field = (0, 6561, 0, 2187, 0, 1458);
pub const EXTRA_DATA_DIGEST: Field = (6561, 243, 2187, 81, 1458, 54);
pub const ADDRESS: Field = (6804, 243, 2268, 81, 1512, 54);
pub const VALUE: Field = (7047, 81, 2349, 27, 1566, 18);
pub const ISSUANCE_TIMESTAMP: Field = (7128, 27, 2376, 9, 1584, 6);
pub const TIMELOCK_LOWER_BOUND: Field = (7155, 27, 2385, 9, 1590, 6);
pub const TIMELOCK_UPPER_BOUND: Field = (7182, 27, 2394, 9, 1596, 6);
pub const BUNDLE_NONCE: Field = (7209, 81, 2403, 27, 1602, 18);
pub const TRUNK_HASH: Field = (7290, 243, 2430, 81, 1620, 54);
pub const BRANCH_HASH: Field = (7533, 243, 2511, 81, 1674, 54);
pub const TAG: Field = (7776, 81, 2592, 27, 1728, 18);
pub const ATTACHMENT_TIMESTAMP: Field = (7857, 27, 2619, 9, 1746, 6);
pub const ATTACHMENT_TIMESTAMP_LOWER_BOUND: Field = (7884, 27, 2628, 9, 1752, 6);
pub const ATTACHMENT_TIMESTAMP_UPPER_BOUND: Field = (7911, 27, 2637, 9, 1758, 6);
pub const NONCE: Field = (7938, 81, 2646, 27, 1764, 18);
pub const TRANSACTION_SIZE_TRITS: usize = 8019;
pub const TRANSACTION_SIZE_TRYTES: usize = TRANSACTION_SIZE_TRITS / 3; // =2673
pub const TRANSACTION_SIZE_BYTES: usize = TRANSACTION_SIZE_TRITS / 9 * 2; // =1782
pub const PACKET_SIZE: usize = TRANSACTION_SIZE_BYTES;

pub const REQUEST_HASH: Field = (8019, 243, 2673, 81, 1782, 54);
pub const PACKET_SIZE_WITH_REQUEST: usize = 1836;

lazy_static! {
    pub static ref IS_TRYTES: Regex = Regex::new("^[9A-Z]*$").unwrap();
}

pub const HASH_LENGTH_TRITS: usize = 243;
pub const CURL_ROUNDS_TRANSACTION_HASH: usize = 27;
pub const MAX_TOKEN_SUPPLY: i64 = (3 ^ 33) / 2 - 1;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction_trit_length_constants() {
        let sum = SIGNATURE_FRAGMENTS.1
            + EXTRA_DATA_DIGEST.1
            + ADDRESS.1
            + VALUE.1
            + ISSUANCE_TIMESTAMP.1
            + TIMELOCK_LOWER_BOUND.1
            + TIMELOCK_UPPER_BOUND.1
            + BUNDLE_NONCE.1
            + TRUNK_HASH.1
            + BRANCH_HASH.1
            + TAG.1
            + ATTACHMENT_TIMESTAMP.1
            + ATTACHMENT_TIMESTAMP_LOWER_BOUND.1
            + ATTACHMENT_TIMESTAMP_UPPER_BOUND.1
            + NONCE.1;

        assert_eq!(sum, TRANSACTION_SIZE_TRITS);
    }

    #[test]
    fn test_transaction_trit_offset_constants() {
        assert_eq!(
            EXTRA_DATA_DIGEST.0,
            SIGNATURE_FRAGMENTS.0 + SIGNATURE_FRAGMENTS.1
        );
        assert_eq!(ADDRESS.0, EXTRA_DATA_DIGEST.0 + EXTRA_DATA_DIGEST.1);
        assert_eq!(VALUE.0, ADDRESS.0 + ADDRESS.1);
        assert_eq!(ISSUANCE_TIMESTAMP.0, VALUE.0 + VALUE.1);
        assert_eq!(
            TIMELOCK_LOWER_BOUND.0,
            ISSUANCE_TIMESTAMP.0 + ISSUANCE_TIMESTAMP.1
        );
        assert_eq!(
            TIMELOCK_UPPER_BOUND.0,
            TIMELOCK_LOWER_BOUND.0 + TIMELOCK_LOWER_BOUND.1
        );
        assert_eq!(
            BUNDLE_NONCE.0,
            TIMELOCK_UPPER_BOUND.0 + TIMELOCK_UPPER_BOUND.1
        );
        assert_eq!(TRUNK_HASH.0, BUNDLE_NONCE.0 + BUNDLE_NONCE.1);
        assert_eq!(BRANCH_HASH.0, TRUNK_HASH.0 + TRUNK_HASH.1);
        assert_eq!(TAG.0, BRANCH_HASH.0 + BRANCH_HASH.1);
        assert_eq!(ATTACHMENT_TIMESTAMP.0, TAG.0 + TAG.1);
        assert_eq!(
            ATTACHMENT_TIMESTAMP_LOWER_BOUND.0,
            ATTACHMENT_TIMESTAMP.0 + ATTACHMENT_TIMESTAMP.1
        );
        assert_eq!(
            ATTACHMENT_TIMESTAMP_UPPER_BOUND.0,
            ATTACHMENT_TIMESTAMP_LOWER_BOUND.0 + ATTACHMENT_TIMESTAMP_LOWER_BOUND.1
        );
        assert_eq!(
            NONCE.0,
            ATTACHMENT_TIMESTAMP_UPPER_BOUND.0 + ATTACHMENT_TIMESTAMP_UPPER_BOUND.1
        );
        assert_eq!(REQUEST_HASH.0, NONCE.0 + NONCE.1);
    }

    #[test]
    fn test_transaction_tryte_length_constants() {
        let sum = SIGNATURE_FRAGMENTS.3
            + EXTRA_DATA_DIGEST.3
            + ADDRESS.3
            + VALUE.3
            + ISSUANCE_TIMESTAMP.3
            + TIMELOCK_LOWER_BOUND.3
            + TIMELOCK_UPPER_BOUND.3
            + BUNDLE_NONCE.3
            + TRUNK_HASH.3
            + BRANCH_HASH.3
            + TAG.3
            + ATTACHMENT_TIMESTAMP.3
            + ATTACHMENT_TIMESTAMP_LOWER_BOUND.3
            + ATTACHMENT_TIMESTAMP_UPPER_BOUND.3
            + NONCE.3;

        assert_eq!(sum, TRANSACTION_SIZE_TRYTES);
    }

    #[test]
    fn test_transaction_tryte_offset_constants() {
        assert_eq!(SIGNATURE_FRAGMENTS.0 / 3, SIGNATURE_FRAGMENTS.2);
        assert_eq!(EXTRA_DATA_DIGEST.0 / 3, EXTRA_DATA_DIGEST.2);
        assert_eq!(ADDRESS.0 / 3, ADDRESS.2);
        assert_eq!(VALUE.0 / 3, VALUE.2);
        assert_eq!(ISSUANCE_TIMESTAMP.0 / 3, ISSUANCE_TIMESTAMP.2);
        assert_eq!(TIMELOCK_LOWER_BOUND.0 / 3, TIMELOCK_LOWER_BOUND.2);
        assert_eq!(TIMELOCK_UPPER_BOUND.0 / 3, TIMELOCK_UPPER_BOUND.2);
        assert_eq!(BUNDLE_NONCE.0 / 3, BUNDLE_NONCE.2);
        assert_eq!(TRUNK_HASH.0 / 3, TRUNK_HASH.2);
        assert_eq!(BRANCH_HASH.0 / 3, BRANCH_HASH.2);
        assert_eq!(TAG.0 / 3, TAG.2);
        assert_eq!(ATTACHMENT_TIMESTAMP.0 / 3, ATTACHMENT_TIMESTAMP.2);
        assert_eq!(
            ATTACHMENT_TIMESTAMP_LOWER_BOUND.0 / 3,
            ATTACHMENT_TIMESTAMP_LOWER_BOUND.2
        );
        assert_eq!(
            ATTACHMENT_TIMESTAMP_UPPER_BOUND.0 / 3,
            ATTACHMENT_TIMESTAMP_UPPER_BOUND.2
        );
        assert_eq!(NONCE.0 / 3, NONCE.2);
        assert_eq!(REQUEST_HASH.0 / 3, REQUEST_HASH.2);
    }

    #[test]
    fn test_transaction_byte_length_constants() {
        let sum = SIGNATURE_FRAGMENTS.5
            + EXTRA_DATA_DIGEST.5
            + ADDRESS.5
            + VALUE.5
            + ISSUANCE_TIMESTAMP.5
            + TIMELOCK_LOWER_BOUND.5
            + TIMELOCK_UPPER_BOUND.5
            + BUNDLE_NONCE.5
            + TRUNK_HASH.5
            + BRANCH_HASH.5
            + TAG.5
            + ATTACHMENT_TIMESTAMP.5
            + ATTACHMENT_TIMESTAMP_LOWER_BOUND.5
            + ATTACHMENT_TIMESTAMP_UPPER_BOUND.5
            + NONCE.5;

        assert_eq!(sum, TRANSACTION_SIZE_BYTES);
    }

    #[test]
    fn test_transaction_byte_offset_constants() {
        assert_eq!(SIGNATURE_FRAGMENTS.2 / 3 * 2, SIGNATURE_FRAGMENTS.4);
        assert_eq!(EXTRA_DATA_DIGEST.2 / 3 * 2, EXTRA_DATA_DIGEST.4);
        assert_eq!(ADDRESS.2 / 3 * 2, ADDRESS.4);
        assert_eq!(VALUE.2 / 3 * 2, VALUE.4);
        assert_eq!(ISSUANCE_TIMESTAMP.2 / 3 * 2, ISSUANCE_TIMESTAMP.4);
        assert_eq!(TIMELOCK_LOWER_BOUND.2 / 3 * 2, TIMELOCK_LOWER_BOUND.4);
        assert_eq!(TIMELOCK_UPPER_BOUND.2 / 3 * 2, TIMELOCK_UPPER_BOUND.4);
        assert_eq!(BUNDLE_NONCE.2 / 3 * 2, BUNDLE_NONCE.4);
        assert_eq!(TRUNK_HASH.2 / 3 * 2, TRUNK_HASH.4);
        assert_eq!(BRANCH_HASH.2 / 3 * 2, BRANCH_HASH.4);
        assert_eq!(TAG.2 / 3 * 2, TAG.4);
        assert_eq!(ATTACHMENT_TIMESTAMP.2 / 3 * 2, ATTACHMENT_TIMESTAMP.4);
        assert_eq!(
            ATTACHMENT_TIMESTAMP_LOWER_BOUND.2 / 3 * 2,
            ATTACHMENT_TIMESTAMP_LOWER_BOUND.4
        );
        assert_eq!(
            ATTACHMENT_TIMESTAMP_UPPER_BOUND.2 / 3 * 2,
            ATTACHMENT_TIMESTAMP_UPPER_BOUND.4
        );
        assert_eq!(NONCE.2 / 3 * 2, NONCE.4);
        assert_eq!(REQUEST_HASH.2 / 3 * 2, REQUEST_HASH.4);
    }
}
