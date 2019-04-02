use std::sync::{Arc, Mutex};

use crate::convert::trytes::Trytes81;
use crate::model::transaction::Transaction;

pub type SharedListeners = Arc<Mutex<Vec<Box<dyn GossipEventListener + Send>>>>;

pub trait GossipEventListener {
    fn on_transaction_received(&mut self, tx: &Transaction, hash: Trytes81);
    fn on_transaction_sent(&mut self, tx: &Transaction, hash: Trytes81);
}
