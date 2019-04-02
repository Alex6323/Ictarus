use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

pub type SharedNeighbors = Arc<RwLock<HashMap<SocketAddr, Neighbor>>>;

/// Represents a neighbored node.
#[derive(Clone)]
pub struct Neighbor {
    pub index: usize,
    pub address: SocketAddr,
    pub stats: NeighborStats,
}

/// Holds neighbor stats.
#[derive(Default, Clone)]
pub struct NeighborStats {
    pub received_all: u64,
    pub received_new: u64,
    pub received_invalid: u64,
    pub prev_received_all: u64,
    pub prev_received_new: u64,
    pub prev_received_invalid: u64,
}

impl Neighbor {
    pub fn new(index: usize, address: SocketAddr, stats: NeighborStats) -> Self {
        Neighbor {
            index,
            address,
            stats,
        }
    }
}

impl PartialEq for Neighbor {
    fn eq(&self, other: &Neighbor) -> bool {
        self.address == other.address
    }
}
impl Eq for Neighbor {}

impl NeighborStats {
    pub fn println(&self, addr: &SocketAddr) {
        println!(
            "{:<20} | {:>5} | {:>5} | {:>5} | {:>5} | {:>5} | {:>5} |",
            addr,
            self.received_all,
            self.received_new,
            self.received_invalid,
            self.prev_received_all,
            self.prev_received_new,
            self.prev_received_invalid,
        );
    }
    pub fn new_round(&mut self) {
        self.prev_received_all = self.received_all;
        self.prev_received_new = self.received_new;
        self.prev_received_invalid = self.received_invalid;

        self.received_all = 0;
        self.received_new = 0;
        self.received_invalid = 0;
    }
}
