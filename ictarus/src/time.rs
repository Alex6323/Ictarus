use std::time::{SystemTime, UNIX_EPOCH};

#[macro_export]
macro_rules! sleep {
    ($ms:expr) => {
        thread::sleep(Duration::from_millis($ms));
    };
}

// NOTE: this should be u64
pub fn get_unix_time_millis() -> i64 {
    let unix_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("error determining system time");
    (unix_time.as_secs() * 1000 + u64::from(unix_time.subsec_millis())) as i64
}
