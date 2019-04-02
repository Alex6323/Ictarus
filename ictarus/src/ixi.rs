use crate::convert::trytes::Trytes81;
use crate::ictarus::*;
use crate::model::transaction::Transaction;
use crate::network::listener::GossipEventListener;

use std::str;

pub type GossipListener = Box<dyn GossipEventListener + Send>;

pub struct MonitorIxi<'a> {
    pegasus: &'a Ictarus,
}

impl<'a> MonitorIxi<'a> {
    pub fn new(pegasus: &'a Ictarus) -> Self {
        println!("[MONITOR] Started Transaction Monitoring IXI");
        MonitorIxi { pegasus }
    }
}

impl<'a> GossipEventListener for MonitorIxi<'a> {
    fn on_transaction_received(&mut self, tx: &Transaction, hash: Trytes81) {
        println!(
            "[MONITOR] Received transaction: {} with timestamp {}",
            std::str::from_utf8(&hash).unwrap(),
            tx.issuance_timestamp,
        );
    }
    fn on_transaction_sent(&mut self, tx: &Transaction, hash: Trytes81) {
        println!(
            "[MONITOR] Sent transaction: {} with timestamp {}",
            std::str::from_utf8(&hash).unwrap(),
            tx.issuance_timestamp
        );
    }
}
