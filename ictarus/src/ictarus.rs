use std::cmp::Reverse;
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use futures::stream::Stream;
use futures::*;

use log::*;
use priority_queue::PriorityQueue;
use stream_cancel::{StreamExt, Trigger, Tripwire};

use tokio::net::UdpSocket;
use tokio::reactor::Handle;
use tokio::runtime::current_thread::block_on_all;
use tokio::runtime::Runtime;
use tokio::timer::Interval;

use crate::config::*;
use crate::constants::*;
use crate::convert::bytes::{self, Bytes54};
use crate::ixi::*;
use crate::model::tangle::*;
use crate::model::transaction::*;
use crate::network::listener::*;
use crate::network::neighbor::{Neighbor, NeighborStats, SharedNeighbors};
use crate::network::receiver::*;
use crate::network::sender::*;

macro_rules! mutex {
    ($data:expr) => {
        Arc::new(Mutex::new($data))
    };
}

macro_rules! rwlock {
    ($data:expr) => {
        Arc::new(RwLock::new($data))
    };
}

pub type SharedSendingQueue =
    Arc<Mutex<PriorityQueue<(SharedKey81, SenderMode), Reverse<Instant>>>>;
pub type SharedRequestQueue = Arc<RwLock<VecDeque<Bytes54>>>;

pub struct Ictarus {
    config: SharedConfig,
    runtime: Runtime,
    state: State,
    tangle: SharedTangle,
    neighbors: SharedNeighbors,
    listeners: SharedListeners,
    sending_queue: SharedSendingQueue,
    request_queue: SharedRequestQueue,
    kill_switch: (Trigger, Tripwire),
}

impl Ictarus {
    pub fn new(config: SharedConfig) -> Self {
        let neighbors = {
            let mut neighbors = HashMap::with_capacity(MAX_NEIGHBOR_COUNT);
            let config = config.read().unwrap();
            for (index, &addr) in config.neighbors.iter().enumerate() {
                neighbors.insert(addr, Neighbor::new(index, addr, NeighborStats::default()));
            }
            neighbors
        };

        Ictarus {
            config,
            runtime: Runtime::new().unwrap(),
            state: State::Off,
            listeners: mutex!(vec![]),
            tangle: rwlock!(Tangle::new(TANGLE_CAPACITY)),
            neighbors: rwlock!(neighbors),
            sending_queue: mutex!(PriorityQueue::new()),
            request_queue: rwlock!(VecDeque::new()),
            kill_switch: Tripwire::new(),
        }
    }

    pub fn run(&mut self) -> Result<(), Box<std::error::Error>> {
        if self.state != State::Off {
            panic!("error: the node hasn't been turned off");
        }
        self.update_state(State::Initializing);

        // Get two owned references to the underlying UDP socket
        let (tx, rx) = {
            let config = self.config.read().unwrap();
            let socket = std::net::UdpSocket::bind(&config.get_socket_addr())?;
            info!("Listening on '{}'", socket.local_addr()?);

            let handle = Handle::default();
            (
                UdpSocket::from_std(socket.try_clone()?, &handle)?,
                UdpSocket::from_std(socket, &handle)?,
            )
        };

        info!("Spawning receiver ...");
        let receiver = Receiver::new(
            self.config.clone(),
            rx,
            self.tangle.clone(),
            self.sending_queue.clone(),
            self.request_queue.clone(),
            self.neighbors.clone(),
            self.listeners.clone(),
        )
        .take_until(self.kill_switch.1.clone())
        .for_each(|_| Ok(()))
        .map_err(|e| panic!("error in receiver stream: {}", e));

        self.runtime.spawn(receiver);

        info!("Spawning sender ...");
        let sender = Sender::new(
            self.config.clone(),
            tx,
            self.tangle.clone(),
            self.sending_queue.clone(),
            self.request_queue.clone(),
            self.neighbors.clone(),
            self.listeners.clone(),
        )
        .take_until(self.kill_switch.1.clone())
        .for_each(|_| Ok(()))
        .map_err(|e| panic!("error in sender stream: {}", e));

        self.runtime.spawn(sender);

        info!("Starting rounds...");
        let round_duration = {
            let config = self.config.read().unwrap();
            config.round_duration
        };

        let neighbors_move = self.neighbors.clone();
        let neighbors_task = Interval::new_interval(Duration::from_millis(round_duration))
            .take_until(self.kill_switch.1.clone())
            .for_each(move |_| {
                info!("Neighbor stats:");
                {
                    //TEMP
                    println!("{}", "-".repeat(70));
                    let mut neighbors = neighbors_move.write().unwrap();
                    neighbors.iter_mut().for_each(|(addr, neighbor)| {
                        neighbor.stats.println(addr);
                        neighbor.stats.new_round();
                    });
                    println!();
                }
                Ok(())
            })
            .map_err(|e| panic!("error in stats task: {}", e));

        self.runtime.spawn(neighbors_task);
        self.update_state(State::Running);

        Ok(())
    }

    pub fn wait_for_kill_signal(self) {
        if self.state != State::Running {
            panic!("error: node is not running");
        }

        let kill_signal = tokio_signal::ctrl_c()
            .flatten_stream()
            .take(1)
            .for_each(|_| Ok(()));

        block_on_all(kill_signal).unwrap();

        drop(self.kill_switch.0);

        self.runtime.shutdown_on_idle().wait().unwrap();
    }

    pub fn kill(mut self) {
        if self.state != State::Running {
            panic!("error: node is not running");
        }

        self.update_state(State::Terminating);

        // Send signal to stop async streams
        drop(self.kill_switch.0);

        // wait for async tasks to end gracefully
        self.runtime.shutdown_on_idle().wait().unwrap();
    }

    pub fn submit_message(&mut self, message: &str, tag: Option<&str>) -> String {
        if self.state != State::Running {
            panic!("error: node is not running");
        }

        let tag = match tag {
            Some(tag) => tag,
            None => "",
        };

        let tx = Transaction::default().message(message).tag(tag);

        self.submit_transaction(tx)
    }

    pub fn submit_transaction(&mut self, tx: Transaction) -> String {
        use SenderMode::*;

        if self.state != State::Running {
            panic!("error: node is not running");
        }

        let vertex = Vertex::from_transaction(&tx);
        let key = vertex.key.clone();

        // Attach transaction to the Tangle
        {
            let mut tangle = self.tangle.write().unwrap();

            tangle.attach_vertex(
                key.clone(),
                vertex,
                0x1 << SUBMITTED_INDEX,
                Some(NULL_VERTEX.clone()),
                Some(NULL_VERTEX.clone()),
            );
        }

        // Put missing trunk or branch transaction into the request queue
        if !self.has_transaction(&tx.trunk) {
            #[cfg(debug_assertions)]
            println!(
                "{} > Issuing request for trunk: {}",
                self.config.read().unwrap().port,
                tx.trunk
            );

            self.request_transaction(&tx.trunk);
        }
        if !self.has_transaction(&tx.branch) && tx.branch != tx.trunk {
            #[cfg(debug_assertions)]
            println!(
                "{} > Issuing request for branch: {}",
                self.config.read().unwrap().port,
                tx.branch
            );

            self.request_transaction(&tx.branch);
        }

        // Put transaction into the sending queue with the intention to
        // broadcast it to all neighbors
        {
            let mut queue = self.sending_queue.lock().unwrap();
            queue.push((key.clone(), Broadcasting), Reverse(Instant::now()));
        }

        // Notify listeners
        let num_listeners = {
            let mut listeners = self.listeners.lock().unwrap();
            for listener in listeners.iter_mut() {
                listener.on_transaction_sent(&tx, key.clone().0);
            }
            listeners.len()
        };
        debug!("Notified {} listener(s)", num_listeners);

        key.to_string()
    }

    pub fn has_transaction(&self, hash: &str) -> bool {
        if self.state != State::Running {
            panic!("error: node is not running");
        }

        let key = Key81::from(hash);
        let tangle = self.tangle.read().unwrap();
        tangle.get_vertex(&key).is_some()
    }

    pub fn get_transaction(&self, hash: &str) -> Option<Transaction> {
        if self.state != State::Running {
            panic!("error: node is not running");
        }

        let key = Key81::from(hash);
        let tangle = self.tangle.read().unwrap();

        if let Some(vertex) = tangle.get_vertex(&key) {
            Some(Transaction::from_tx_bytes(&vertex.bytes))
        } else {
            None
        }
    }

    pub fn get_vertex(&self, hash: &str) -> Option<SharedVertex> {
        if self.state != State::Running {
            panic!("error: node is not running");
        }

        let key = Key81::from(hash);
        let tangle = self.tangle.read().unwrap();

        if let Some(vertex) = tangle.get_vertex(&key) {
            Some(vertex)
        } else {
            None
        }
    }

    pub fn request_transaction(&mut self, hash: &str) -> bool {
        if self.state != State::Running {
            panic!("error: node is not running");
        }
        if self.get_transaction(hash).is_some() || !IS_TRYTES.is_match(hash) || hash.len() != 81 {
            return false;
        }

        // Determine the byte representation
        let request_trytes = hash.as_bytes();
        let request_bytes = bytes::from_81_trytes_2enc9(&request_trytes);

        // Determine the hashmap key
        {
            let mut request_queue = self.request_queue.write().unwrap();
            request_queue.push_back(request_bytes);
        }
        true
    }

    pub fn add_gossip_listener(&mut self, listener: GossipListener) {
        let mut listeners = self.listeners.lock().unwrap();
        listeners.push(listener);
    }

    pub fn get_neighbor_by_index(&self, index: usize) -> Neighbor {
        if index >= MAX_NEIGHBOR_COUNT {
            panic!("error: neighbor index out of bounds");
        }

        let config = self.config.read().unwrap();
        let address = config.neighbors[index];
        let neighbors = self.neighbors.read().unwrap();
        neighbors[&address].clone()
    }

    pub fn get_tangle_stats(&self) -> TangleStats {
        self.tangle.read().unwrap().get_stats()
    }

    pub fn get_neighbors(&self) -> HashMap<SocketAddr, Neighbor> {
        self.neighbors.read().unwrap().clone()
    }

    pub fn get_config(&self) -> Config {
        self.config.read().unwrap().clone()
    }

    fn update_state(&mut self, new_state: State) {
        if new_state == self.state {
            return;
        }
        self.state = new_state;
        self.state.print();
    }

    pub fn get_name(&self) -> String {
        self.config.read().unwrap().get_socket_addr().to_string()
    }

    // Compiled when testing
    #[cfg(test)]
    pub fn remove_transaction(&mut self, tx_hash: &str) {
        let mut tangle = self.tangle.write().unwrap();
        let key = Key81::from(tx_hash);
        tangle.remove_vertex(&key);
    }

    pub fn add_neighbor(&mut self, addr: std::net::SocketAddr) {
        self.config.write().unwrap().add_neighbor(&addr.to_string());

        let mut neighbors = self.neighbors.write().unwrap();
        let new_neighbor_index = neighbors.len();
        let neighbor = Neighbor::new(new_neighbor_index, addr, NeighborStats::default());
        neighbors.insert(addr, neighbor);
    }
}

#[derive(PartialEq, Eq, Debug)]
pub enum State {
    Initializing,
    Running,
    Terminating,
    Off,
}

impl State {
    pub fn print(&self) {
        match *self {
            State::Initializing => info!("Starting Ictarus..."),
            State::Running => info!("Ictarus is running."),
            State::Terminating => info!("Ictarus is shutting down..."),
            State::Off => info!("Ictarus has been turned off."),
        }
    }
}

#[cfg(test)]
mod pegasus_tests {
    use super::*;

    use crate::config::*;
    use crate::time::*;

    use std::sync::{Arc, RwLock};
    use std::thread;
    use std::time::{Duration, Instant};

    macro_rules! wait {
        ($ms:expr) => {
            thread::sleep(Duration::from_millis($ms));
        };
    }

    /// This test creates a node A and runs it.
    ///
    /// The test passes, if the node state correctly changes from 'Off'
    /// to 'Running'   
    #[test]
    fn test_init_node() {
        let config = Arc::new(RwLock::new(ConfigBuilder::default().port(1340).build()));
        let mut a = Ictarus::new(config);

        assert_eq!(State::Off, a.state);
        assert!(a.run().is_ok());
        assert_eq!(State::Running, a.state);

        a.kill();

        wait!(100);
    }

    /// This test creates a node A from a configuration where host
    /// is a domain name.
    ///
    /// The test passes, if the node can be created and its socket
    /// address is correctly resolved.
    #[test]
    fn test_bind_to_localhost() {
        let config = Arc::new(RwLock::new(
            ConfigBuilder::default()
                .host("localhost")
                .port(1339)
                .build(),
        ));

        let a = Ictarus::new(config);

        assert_eq!(State::Off, a.state);
        assert_eq!(
            "127.0.0.1:1339".to_string(),
            a.get_config().get_socket_addr().to_string()
        );
    }

    /// This test creates a node A without any neighbors, which then submits a message.
    ///
    /// The test passes, if the associated transaction is stored locally.
    //#[test]
    fn test_submit_message() {
        let config = Arc::new(RwLock::new(ConfigBuilder::default().port(1341).build()));
        let mut a = Ictarus::new(config);

        a.run().unwrap();

        let key = a.submit_message("PING", Some("PEGASUS"));

        assert!(a.get_transaction(&key).is_some());
        assert_eq!(1, a.get_tangle_stats().num_vertices_by_hash);

        // Print the transaction
        let tx = a.get_transaction(&key).unwrap();
        println!("{:?}", tx);

        a.kill();

        wait!(100);
    }

    /// This test creates to nodes A and B. A submits a message and "forgets" about
    /// it. Later it queries that transaction from B which still has
    /// it stored in its Tangle.
    ///
    /// The test passes, if the A can successfully retrieve the transaction from B.
    ///
    /// NOTE: We need to simulate "forgetting" by explicitly removing the transaction
    /// from the Tangle (that's why this test needs to remain here)
    //#[test]
    fn test_request_transaction() {
        let mut a = Ictarus::new(get_config(1301, vec![1302]));
        let mut b = Ictarus::new(get_config(1302, vec![1301]));

        a.run().expect("couldn't start node A");
        b.run().expect("coudln't start node B");

        // Wait for nodes to initalize
        wait!(1000);

        println!("Sending THE message...");
        let tx_hash = a.submit_message("I will be forgotten by A", None);

        // Wait for B to receive the message
        wait!(2000);

        assert!(b.get_transaction(&tx_hash).is_some());

        // A "forgets" about the message
        a.remove_transaction(&tx_hash);
        assert!(a.get_transaction(&tx_hash).is_none());

        // A requests the transaction from his neighbors
        assert_eq!(true, a.request_transaction(&tx_hash));

        println!("Sending request carrier...");
        a.submit_message("request carrier", None);

        // Wait for B to receive the carrier with the piggybacked request
        // and reply to A's request
        wait!(4000);

        // A should now have the tx again
        assert!(a.get_transaction(&tx_hash).is_some());

        a.kill();
        b.kill();

        wait!(100);
    }

    /// This simple benchmark creates two nodes A and B. A submits 1000 unique messages
    /// sequentially.
    ///
    /// The test is always passed. However, you can uncomment one assert, that
    /// fails, if not all messages reach B. In that case you have to wait a
    /// little between each send, otherwise messages are issued too fast for
    /// the socket.
    ///
    /// Use `cargo test bench_submit_1000_messages --release -- --nocapture`
    /// to get production results.
    ///
    /// Last results:
    ///     ~578 ms (initial impl.)
    ///     ~540 ms (major changes and refactoring in Sender)
    //#[test]
    fn bench_submit_1000_messages() -> Result<(), Box<std::error::Error>> {
        let mut a = Ictarus::new(get_config(1401, vec![1402]));
        let mut b = Ictarus::new(get_config(1402, vec![1401]));

        a.run()?;
        b.run()?;

        // Wait for nodes to initalize
        thread::sleep(Duration::from_millis(1000));

        const NUM_MESSAGES: usize = 1000;
        let start = Instant::now();
        for i in 0..NUM_MESSAGES {
            // NOTE:
            //sleep!(6);
            a.submit_message(&i.to_string(), None);
        }
        let stop = start.elapsed();

        println!(
            "{} ms",
            stop.as_secs() * 1000 + u64::from(stop.subsec_millis())
        );

        // Wait for the communication to end
        thread::sleep(Duration::from_millis(5000));

        //assert_eq!(NUM_MESSAGES, b.tangle_stats().num_transactions_by_hash);

        a.kill();
        b.kill();

        thread::sleep(Duration::from_millis(1000));

        Ok(())
    }

    // Helper function to quickly create local nodes
    fn get_config(port: u16, neighbors: Vec<u16>) -> SharedConfig {
        let mut builder = ConfigBuilder::default().port(port);
        for port in neighbors {
            builder = builder.neighbor(&format!("127.0.0.1:{}", port));
        }
        Arc::new(RwLock::new(builder.build()))
    }

    use crate::model::transaction::*;

    /// This test creates 3 nodes A, B and C with a topology like A <-> B <-> C.
    /// A and B then issue a unique transaction respectively. Node C should after
    /// some time have received both messages. It then issues a new transaction
    /// referencing both initial transactions in its trunk and branch.
    ///
    /// The test passes, if all nodes end up synchronized holding C's transaction
    /// referencing the two initial transactions.
    //#[test]
    fn test_reference_creation_ordered() {
        let mut a = Ictarus::new(get_config(1500, vec![1501]));
        let mut b = Ictarus::new(get_config(1501, vec![1500, 1502]));
        let mut c = Ictarus::new(get_config(1502, vec![1501]));

        a.run().expect("couldn't start node A");
        b.run().expect("couldn't start node B");
        c.run().expect("couldn't start node C");

        let tr_hash = b.submit_message("PING", None);
        let br_hash = a.submit_message("PONG", None);

        println!("trunk_hash:  {}", tr_hash);
        println!("branch_hash: {}", br_hash);

        sleep!(1000);

        assert!(c.has_transaction(&tr_hash));
        assert!(c.has_transaction(&br_hash));

        let referrer_hash = send_referrer(&mut c, &tr_hash, &br_hash);

        println!("referrer_hash: {}", referrer_hash);

        sleep!(1000);

        assert_correct_references(&a, &referrer_hash);
        assert_correct_references(&b, &referrer_hash);
        assert_correct_references(&c, &referrer_hash);

        a.kill();
        b.kill();
        c.kill();
    }

    /// This test creates two nodes A and B, which are NOT neighbored (at first).
    /// Both issue a unique transaction the other node doesn't know about. Then both
    /// nodes add eachother as neighbors. Node B issues another transaction which
    /// references both initially issued transactions. Because it is missing the
    /// branch transaction it attaches a request for it to the issued transaction
    /// itself. Node A receives that transaction, realizes it can serve the request
    /// and attaches a request to its reply, which then can be served by B.
    ///
    /// The test passes, if both nodes end up synchronized.
    //#[test]
    fn test_reference_old_transactions() {
        let mut a = Ictarus::new(get_config(1503, vec![]));
        let mut b = Ictarus::new(get_config(1504, vec![]));

        a.run().expect("couldn't start node A");
        b.run().expect("couldn't start node B");

        // Since both nodes aren't connected to eachother (just yet)
        // they basically submit that message for themselves
        let tr_hash = a.submit_message("PING", None);
        let br_hash = b.submit_message("PONG", None);

        sleep!(1000);

        // Connect both nodes, but they remain unsynchronized for now
        connect(&mut a, &mut b);

        // referrer is also used as request carrier
        let referrer_hash = send_referrer(&mut b, &tr_hash, &br_hash);

        sleep!(1000);

        assert_correct_references(&a, &referrer_hash);
        assert_correct_references(&b, &referrer_hash);

        a.kill();
        b.kill();
    }

    /// This test creates two nodes A and B.
    //#[test]
    fn test_receive_inverse_order() {
        let mut a = Ictarus::new(get_config(1505, vec![]));
        let mut b = Ictarus::new(get_config(1506, vec![]));

        a.run().expect("couldn't start node A");
        b.run().expect("couldn't start node B");

        connect(&mut a, &mut b);

        let tr = TransactionBuilder::default().message("PING").build();
        let br = TransactionBuilder::default().message("PONG").build();

        let tr_hash = Key81::from(tr.get_hash()).to_string();
        let br_hash = Key81::from(br.get_hash()).to_string();

        println!();
        println!("Submitting referrer transaction...");
        println!();

        // We send the referring transaction first
        // Upon submitting the referrer tx A will put trunk and branch
        // hashes into its request queue and send a request along with
        // the referrer transaction which can't be fulfilled by B.
        let referrer_hash = send_referrer(&mut a, &tr_hash, &br_hash);

        sleep!(1000);

        println!();
        println!("Submitting referred transactions...");
        println!();

        // Now we send the referred transactions. The request queues
        // should be emptied by both nodes.
        a.submit_transaction(tr);
        a.submit_transaction(br);

        sleep!(1000);

        println!();
        assert_correct_references(&a, &referrer_hash);
        assert_correct_references(&b, &referrer_hash);

        assert_eq!(0, a.request_queue.read().unwrap().len());
        assert_eq!(0, b.request_queue.read().unwrap().len());

        a.kill();
        b.kill();
    }

    /// This test creates a node A without any neighbors (at first). A submits
    /// a transaction X and another Y which references X in its trunk
    /// and its branch. Since it has no neighbors it will not actually send any
    /// of those but simply store them. After that it neighbors with node
    /// B and publishes another transaction Z, which now will reach B. B in turn
    /// requests Y using its own message as carrier. A will fulfill the request
    /// by sending Y.
    ///
    /// The test passes, if B no longer requests trunk/branch transactions
    /// from request replies.
    //#[test]
    fn test_dont_request_old_transactions() {
        let mut a = Ictarus::new(get_config(1507, vec![]));
        a.run().expect("couldn't start node A");

        // A creates two messages X and Y, the Y referencing X twice
        let x = a.submit_message("PING", None);
        let y = send_referrer(&mut a, &x, &x);

        // Wait a little to make sure B isn't live yet
        sleep!(100);

        // Create node B and connect both nodes
        let mut b = Ictarus::new(get_config(1508, vec![]));
        b.run().expect("couldn't start node B");
        connect(&mut a, &mut b);

        // A publishes another transaction referencing Y, and this
        // time it reaches B, which doesn't know transaction Y, so
        // B will issue a request for Y with its next message
        let z = send_referrer(&mut a, &y, &y);

        sleep!(1000);

        // B publishes a message, which carries the request for Y.
        // A will send B transaction Y, which then realizes that
        // it doesn't have X either. There are a few things to consider here:
        // * An honest node should not further request older transactions
        //   and drain on its neighbor's CPU cycles and bandwidth
        // * As a protection measure a node should not answer requests for
        //   trunk/branch transactions of replies it has already given to
        //   a particular neighbor
        b.submit_message("PONG", None);

        sleep!(1000);

        // B should only request trunk and branch of z, but not
        // the whole history of the Tangle
        assert_correct_references(&b, &z);
        assert!(!b.has_transaction(&x));
    }

    fn connect(a: &mut Ictarus, b: &mut Ictarus) {
        a.add_neighbor(b.get_config().get_socket_addr());
        b.add_neighbor(a.get_config().get_socket_addr());
    }

    fn send_referrer(node: &mut Ictarus, trunk: &str, branch: &str) -> String {
        let referrer_tx = TransactionBuilder::default()
            .trunk(&trunk)
            .branch(&branch)
            .build();

        //println!("{:?}", referrer_tx);
        node.submit_transaction(referrer_tx)
    }

    fn assert_correct_references(node: &Ictarus, referrer_hash: &str) {
        println!("Checking assertions for {}:", node.get_name());
        let referrer_tx = node.get_transaction(referrer_hash);

        // Node has the referrer transaction
        assert!(referrer_tx.is_some());

        // Node has both of the referred transactions
        let referrer_tx = referrer_tx.unwrap();
        assert!(node.has_transaction(&referrer_tx.trunk));
        assert!(node.has_transaction(&referrer_tx.branch));

        //
        let trunk_vertex = node.get_vertex(&referrer_tx.trunk).unwrap();
        assert_eq!(referrer_tx.trunk, trunk_vertex.key.to_string());
        println!("trunk_vertex.hash = {}", trunk_vertex.key.to_string());

        let branch_vertex = node.get_vertex(&referrer_tx.branch).unwrap();
        assert_eq!(referrer_tx.branch, branch_vertex.key.to_string());
        println!("branch_vertex.hash = {}", branch_vertex.key.to_string());
    }
}
