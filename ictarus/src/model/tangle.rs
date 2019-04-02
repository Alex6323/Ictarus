use std::collections::{HashMap, HashSet};
use std::fmt::{self, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, RwLock};

use lazy_static::*;

use crate::constants::*;
use crate::convert::bytes::*;
use crate::convert::trytes::{self, *};
use crate::model::transaction::Transaction;

pub struct Key81(pub Trytes81);
pub struct Key27(pub Trytes27);

#[derive(Clone)]
pub struct SharedKey81_(pub Arc<Key81>);

#[derive(Clone)]
pub struct SharedKey27_(pub Arc<Key27>);

pub type SharedKey81 = Arc<Key81>;
pub type SharedKey27 = Arc<Key27>;
pub type SharedTangle = Arc<RwLock<Tangle>>;
pub type SharedVertex = Arc<Vertex>;
pub type SharedMutableVertex = Arc<RwLock<Vertex>>;

pub type Flags = u8;
pub type MaybeTrunk = Option<SharedVertex>;
pub type MaybeBranch = Option<SharedVertex>;

pub const NULL_FLAGS: u8 = 0;
pub const SENDER_OFFSET: usize = 0;
pub const REQUESTER_OFFSET: usize = 4;
pub const SUBMITTED_INDEX: usize = 3;
pub const REQUESTED_INDEX: usize = 7;

macro_rules! clone {
    ($instance:expr) => {
        Arc::clone(&$instance);
    };
}

lazy_static! {
    pub static ref NULL_VERTEX: SharedVertex = Arc::new(Vertex {
        bytes: [0; TRANSACTION_SIZE_BYTES],
        key: Arc::new(Key81(TRYTES_81_NULL)),
        addr_key: Arc::new(Key81(TRYTES_81_NULL)),
        tag_key: Arc::new(Key27(TRYTES_27_NULL)),
    });
}

pub struct Tangle {
    vertices_by_hash: HashMap<SharedKey81, (SharedVertex, Flags, MaybeTrunk, MaybeBranch)>,
    vertices_by_addr: HashMap<SharedKey81, HashSet<SharedVertex>>,
    vertices_by_tag: HashMap<SharedKey27, HashSet<SharedVertex>>,
}

pub struct Vertex {
    pub bytes: TxBytes,
    pub key: SharedKey81,
    pub addr_key: SharedKey81,
    pub tag_key: SharedKey27,
}

#[derive(Copy, Clone, Debug)]
pub struct TangleStats {
    pub num_vertices_by_hash: usize,
    pub num_vertices_by_addr: usize,
    pub num_vertices_by_tag: usize,
}

impl Tangle {
    pub fn new(capacity: usize) -> Self {
        let mut vertices_by_hash = HashMap::with_capacity(capacity);
        vertices_by_hash.insert(
            NULL_VERTEX.key.clone(),
            (
                clone!(NULL_VERTEX),
                NULL_FLAGS,
                Some(clone!(NULL_VERTEX)),
                Some(clone!(NULL_VERTEX)),
            ),
        );

        Tangle {
            vertices_by_hash,
            vertices_by_addr: HashMap::with_capacity(capacity / 2),
            vertices_by_tag: HashMap::with_capacity(capacity / 2),
        }
    }

    pub fn get_stats(&self) -> TangleStats {
        TangleStats {
            num_vertices_by_hash: self.vertices_by_hash.len(),
            num_vertices_by_addr: self.vertices_by_addr.len(),
            num_vertices_by_tag: self.vertices_by_tag.len(),
        }
    }

    pub fn update_senders(&mut self, key: &Key81, sender: usize) {
        match &mut self.vertices_by_hash.get_mut(key) {
            Some((_, flags, _, _)) => *flags |= 0x1 << (SENDER_OFFSET + sender),
            None => (),
        }
    }

    pub fn update_requesters(&mut self, key: &Key81, requester: usize) {
        match &mut self.vertices_by_hash.get_mut(key) {
            Some((_, flags, _, _)) => *flags |= 0x1 << (REQUESTER_OFFSET + requester),
            None => (),
        }
    }

    pub fn update_trunk(&mut self, key: &Key81, trunk: SharedVertex) {
        match &mut self.vertices_by_hash.get_mut(key) {
            Some((_, _, tr, _)) => *tr = Some(trunk),
            None => (),
        }
    }

    pub fn update_branch(&mut self, key: &Key81, branch: SharedVertex) {
        match &mut self.vertices_by_hash.get_mut(key) {
            Some((_, _, _, br)) => *br = Some(branch),
            None => (),
        }
    }

    pub fn attach_vertex(
        &mut self,
        tx_key: SharedKey81,
        vertex: Vertex,
        flags: Flags,
        trunk: MaybeTrunk,
        branch: MaybeBranch,
    ) -> SharedVertex {
        assert!(!self.vertices_by_hash.contains_key(&tx_key));

        let shareable_vertex = Arc::new(vertex);

        // Store new vertex
        self.vertices_by_hash.insert(
            tx_key.clone(),
            (shareable_vertex.clone(), flags, trunk, branch),
        );

        // Store vertices by address
        if let Some(vertices) = self.vertices_by_addr.get_mut(&shareable_vertex.addr_key) {
            vertices.insert(shareable_vertex.clone());
        } else {
            let mut vertices = HashSet::new();
            vertices.insert(shareable_vertex.clone());
            self.vertices_by_addr
                .insert(shareable_vertex.addr_key.clone(), vertices);
        }

        // Store transaction by tag
        if let Some(vertices) = self.vertices_by_tag.get_mut(&shareable_vertex.tag_key) {
            vertices.insert(shareable_vertex.clone());
        } else {
            let mut vertices = HashSet::new();
            vertices.insert(shareable_vertex.clone());
            self.vertices_by_tag
                .insert(shareable_vertex.tag_key.clone(), vertices);
        }

        shareable_vertex.clone()
    }

    pub fn get_vertex_and_flags(&self, tx_key: &Key81) -> Option<(SharedVertex, Flags)> {
        match self.vertices_by_hash.get(tx_key) {
            Some((vertex, flags, _, _)) => Some((vertex.clone(), *flags)),
            None => None,
        }
    }

    pub fn get_vertex(&self, tx_key: &Key81) -> Option<SharedVertex> {
        match self.vertices_by_hash.get(tx_key) {
            Some((vertex, _, _, _)) => Some(vertex.clone()),
            None => None,
        }
    }

    pub fn get_edges(&self, tx_key: &Key81) -> (MaybeTrunk, MaybeBranch) {
        match self.vertices_by_hash.get(tx_key) {
            Some((_, _, trunk, branch)) => (trunk.clone(), branch.clone()),
            None => (None, None),
        }
    }

    pub fn has_transaction(&self, tx_key: &Key81) -> bool {
        self.vertices_by_hash.contains_key(tx_key)
    }

    pub fn remove_vertex(&mut self, tx_key: &Key81) -> bool {
        if let Some((vertex, _, _, _)) = self.vertices_by_hash.get(tx_key) {
            if if let Some(vertices) = self.vertices_by_addr.get_mut(&vertex.addr_key) {
                vertices.remove(vertex);
                vertices.is_empty()
            } else {
                false
            } {
                self.vertices_by_addr.remove(&vertex.addr_key);
            }

            if if let Some(t) = self.vertices_by_tag.get_mut(&vertex.tag_key) {
                t.remove(vertex);
                t.is_empty()
            } else {
                false
            } {
                self.vertices_by_tag.remove(&vertex.tag_key);
            }

            self.vertices_by_hash.remove(tx_key);

            true
        } else {
            false
        }
    }
}

impl Vertex {
    pub fn new(bytes: TxBytes, key: SharedKey81) -> Self {
        let addr_key = Arc::new(Key81::from(trytes::from_54_bytes_2enc9(
            &bytes[ADDRESS.4..ADDRESS.4 + ADDRESS.5],
        )));

        let tag_key = Arc::new(Key27::from(trytes::from_18_bytes_2enc9(
            &bytes[TAG.4..TAG.4 + TAG.5],
        )));

        Vertex {
            bytes,
            key,
            addr_key,
            tag_key,
        }
    }

    pub fn from_buffer(buffer: &[u8], key: SharedKey81) -> Self {
        let mut bytes = [0u8; TRANSACTION_SIZE_BYTES];
        bytes.copy_from_slice(&buffer[..TRANSACTION_SIZE_BYTES]);

        let addr_key = Arc::new(Key81::from(trytes::from_54_bytes_2enc9(
            &bytes[ADDRESS.4..ADDRESS.4 + ADDRESS.5],
        )));

        let tag_key = Arc::new(Key27::from(trytes::from_18_bytes_2enc9(
            &bytes[TAG.4..TAG.4 + TAG.5],
        )));

        Vertex {
            bytes,
            key,
            addr_key,
            tag_key,
        }
    }

    pub fn from_transaction(tx: &Transaction) -> Self {
        Vertex {
            bytes: tx.as_bytes(),
            key: Arc::new(Key81::from(tx.get_hash())),
            addr_key: Arc::new(Key81::from(&tx.address[..])),
            tag_key: Arc::new(Key27::from(&tx.tag[..])),
        }
    }
}

impl Display for TangleStats {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "#hashes: {}, #addresses: {}, #tags: {}",
            self.num_vertices_by_hash, self.num_vertices_by_addr, self.num_vertices_by_tag
        )
    }
}

impl Eq for Key81 {}
impl Eq for Key27 {}
impl Eq for SharedKey81_ {}
impl Eq for SharedKey27_ {}

impl PartialEq for Key81 {
    fn eq(&self, other: &Key81) -> bool {
        self.0[..] == other.0[..]
    }
}
impl PartialEq for Key27 {
    fn eq(&self, other: &Key27) -> bool {
        self.0[..] == other.0[..]
    }
}
impl PartialEq for SharedKey81_ {
    fn eq(&self, other: &SharedKey81_) -> bool {
        self.0.eq(&other.0)
    }
}
impl PartialEq for SharedKey27_ {
    fn eq(&self, other: &SharedKey27_) -> bool {
        self.0.eq(&other.0)
    }
}

impl Hash for Key81 {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state)
    }
}
impl Hash for Key27 {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state)
    }
}
impl Hash for SharedKey81_ {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state)
    }
}
impl Hash for SharedKey27_ {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state)
    }
}

impl From<&str> for Key81 {
    fn from(s: &str) -> Self {
        let mut buf = [0; 81];
        buf.copy_from_slice(&s.as_bytes()[0..81]);
        Key81(buf)
    }
}
impl From<&str> for Key27 {
    fn from(s: &str) -> Self {
        let mut buf = [0; 27];
        buf.copy_from_slice(&s.as_bytes()[0..27]);
        Key27(buf)
    }
}
impl From<&str> for SharedKey81_ {
    fn from(s: &str) -> Self {
        SharedKey81_(Arc::new(Key81::from(s)))
    }
}
impl From<&str> for SharedKey27_ {
    fn from(s: &str) -> Self {
        SharedKey27_(Arc::new(Key27::from(s)))
    }
}

// From 9/2 encoded bytes (1.5 trytes per byte)
impl From<&[u8]> for Key81 {
    fn from(b: &[u8]) -> Self {
        let mut buf = [0; 81];
        buf.copy_from_slice(&b[0..81]);
        Key81(buf)
    }
}
impl From<&[u8]> for Key27 {
    fn from(b: &[u8]) -> Self {
        let mut buf = [0; 27];
        buf.copy_from_slice(&b[0..27]);
        Key27(buf)
    }
}

// From 6/2 encoded bytes (1 tryte per byte)
impl From<Trytes81> for Key81 {
    fn from(b: Trytes81) -> Self {
        Key81(b)
    }
}
impl From<Trytes27> for Key27 {
    fn from(b: Trytes27) -> Self {
        Key27(b)
    }
}

impl ToString for Key81 {
    fn to_string(&self) -> String {
        String::from_utf8(self.0.to_vec()).unwrap()
    }
}
impl ToString for Key27 {
    fn to_string(&self) -> String {
        String::from_utf8(self.0.to_vec()).unwrap()
    }
}
impl Eq for Vertex {}
impl PartialEq for Vertex {
    fn eq(&self, other: &Vertex) -> bool {
        self.key == other.key
    }
}
impl Hash for Vertex {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.key.hash(state)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::crypto::curl;
    use crate::model::transaction::Transaction;

    use std::sync::mpsc;
    use std::thread;
    use std::time::{Duration, Instant};

    /// This test creates a new Tangle datastructure. Then it creates a Vertex A
    /// and adds it to the Tangle. It then creates an identical transaction A
    /// and tries to add it as well.
    ///
    /// Then it deletes this transaction. After each step it ensures
    /// the Tangle is always of correct size.
    #[test]
    fn test_attach_and_remove_transaction() {
        let mut tangle = Tangle::new(TANGLE_CAPACITY);

        // The Tangle always contains the NULL_VERTEX
        assert_eq!(1, tangle.get_stats().num_vertices_by_hash);
        assert_eq!(1, tangle.get_stats().num_vertices_by_addr);
        assert_eq!(1, tangle.get_stats().num_vertices_by_tag);

        // Create vertex A and add it
        let a = create_vertex("hello");
        let key_a = a.key.clone();
        tangle.attach_vertex(key_a.clone(), a, 0, None, None);
        println!("{}", tangle.get_stats());

        assert_eq!(2, tangle.get_stats().num_vertices_by_hash);
        assert_eq!(1, tangle.get_stats().num_vertices_by_addr);
        assert_eq!(1, tangle.get_stats().num_vertices_by_tag);

        // Create an identical vertex B and try to add
        let b = create_vertex("hello");
        let key_b = b.key.clone();
        tangle.attach_vertex(key_b.clone(), b, 0, None, None);
        println!("{}", tangle.get_stats());

        // There should be no change at all
        assert_eq!(2, tangle.get_stats().num_vertices_by_hash);
        assert_eq!(1, tangle.get_stats().num_vertices_by_addr);
        assert_eq!(1, tangle.get_stats().num_vertices_by_tag);

        tangle.remove_vertex(&key_b);

        assert_eq!(1, tangle.get_stats().num_vertices_by_hash);
        assert_eq!(1, tangle.get_stats().num_vertices_by_addr);
        assert_eq!(1, tangle.get_stats().num_vertices_by_tag);
    }

    /// This test creates a new Tangle, adds a transaction to it with
    /// a specific sender index and then tries to find this transaction.
    #[test]
    fn test_find_transaction_and_senders_bitmask() {
        let mut tangle = Tangle::new(TANGLE_CAPACITY);
        //let (tx_key, tx_bytes) = create_transaction("hello");
        let vertex = create_vertex("hello");
        let tx_key = vertex.key.clone();

        const FLAGS: u8 = 0x02;

        tangle.attach_vertex(tx_key.clone(), vertex, FLAGS, None, None);

        let (_, flags) = tangle.get_vertex_and_flags(&tx_key).unwrap();

        println!("{:08b}", flags);
        assert!((0x1 << 2) & flags != 0);
    }

    fn create_transaction_bytes(message: &str) -> (Trytes81, TxBytes) {
        let tx = Transaction::default().message(message);
        let tx_hash = tx.get_hash();
        let tx_bytes = tx.as_bytes();

        (tx_hash, tx_bytes)
    }

    fn create_vertex(message: &str) -> Vertex {
        let tx = Transaction::default().message(message);
        Vertex::from_transaction(&tx)
    }

    /// This test creates 1000 different transactions and attaches them sequentially to
    /// the Tangle.
    ///
    /// Use `cargo test bench_attach_1000_transactions` --release -- --nocapture
    /// to get production results.
    ///
    /// Last results:
    ///     ~ 542
    #[test]
    fn bench_attach_1000_transactions() {
        let mut tangle = Tangle::new(TANGLE_CAPACITY);

        let start = Instant::now();
        for i in 0..1000 {
            //let (tx_hash, tx_bytes) = create_transaction(&i.to_string());
            //let tx_hash_as_key = Key81::from(tx_hash);
            let vertex = create_vertex(&i.to_string());
            tangle.attach_vertex(vertex.key.clone(), vertex, 1, None, None);
        }
        let stop = start.elapsed();

        println!(
            "{} ms",
            stop.as_secs() * 1000 + u64::from(stop.subsec_millis())
        );
    }

    // TODO: compare performance of mpsc channels with Arc<RwLock>

    /// This test creates 40000 transactions in parallel and adds them to the Tangle.
    ///
    /// While hashing and validation can happen in parallel upon receiving on the socket,
    /// attaching them to the Tangle must happen sequentially.
    ///
    /// Here we are simulating 4 cores that are simultaneously hashing and validating
    /// transactions. They write their results into an mpsc channel. The receiving
    /// end is attaching those transactions to the Tangle.
    ///
    /// Last results:
    ///     40000 txs in 6018 ms (~6666 tps) on a Core i5 16Gb (Curl-27)
    #[test]
    fn bench_attach_40000_transactions_par() {
        let mut tangle = Tangle::new(TANGLE_CAPACITY);
        let (sender, receiver) = mpsc::channel();
        const NUM_TRANSACTIONS_PER_CORE: usize = 10000;
        const NUM_CORES: usize = 4; // determine this for the current platform

        for j in 0..NUM_CORES {
            let sender = sender.clone();
            thread::spawn(move || {
                for i in 0..NUM_TRANSACTIONS_PER_CORE {
                    let tx = Transaction::default().message(&format!("{}-{}", i, j));
                    let tx_bytes = tx.as_bytes();
                    let tx_hash = tx.get_hash();

                    sender
                        .send((j * NUM_TRANSACTIONS_PER_CORE + i, tx_bytes, tx_hash))
                        .unwrap();
                }
            });
            println!("Spawned thread {}", j);
        }

        const FLAGS: u8 = 0x01;
        let start = Instant::now();
        for (i, tx_bytes, tx_hash) in receiver.iter() {
            //let tx_hash_as_key = Key81::from(tx_hash);
            //tangle.attach_vertex(&tx_hash_as_key, tx_bytes, 1, None, None);
            let vertex = Vertex::new(tx_bytes, Arc::new(Key81::from(tx_hash)));
            tangle.attach_vertex(vertex.key.clone(), vertex, FLAGS, None, None);

            if tangle.get_stats().num_vertices_by_hash == NUM_CORES * NUM_TRANSACTIONS_PER_CORE {
                break;
            }
        }
        let stop = start.elapsed();
        println!("{}", tangle.get_stats());
        assert_eq!(
            NUM_CORES * NUM_TRANSACTIONS_PER_CORE,
            tangle.get_stats().num_vertices_by_hash
        );

        println!(
            "{} ms",
            stop.as_secs() * 1000 + u64::from(stop.subsec_millis())
        );
    }
}
