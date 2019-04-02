use std::sync::{Arc, RwLock};

use ictarus::config::*;

#[macro_export]
macro_rules! sleep {
    ($ms:expr) => {
        thread::sleep(Duration::from_millis($ms));
    };
}

// Helper function to quickly create local nodes
pub fn get_config(port: u16, neighbors: Vec<u16>) -> SharedConfig {
    let mut builder = ConfigBuilder::default().port(port);
    for port in neighbors {
        builder = builder.neighbor(&format!("127.0.0.1:{}", port));
    }
    Arc::new(RwLock::new(builder.build()))
}
