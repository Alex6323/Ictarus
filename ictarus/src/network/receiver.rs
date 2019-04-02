use std::cmp::Reverse;
use std::collections::{HashMap, HashSet, VecDeque};
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::*;
use log::*;
use rand::Rng;
use tokio::net::UdpSocket;

use super::listener::SharedListeners;
use super::neighbor::SharedNeighbors;
use super::sender::SenderMode;

use crate::config::*;
use crate::constants::*;
use crate::convert::trits;
use crate::convert::trytes;
use crate::crypto::curl;
use crate::ictarus::*;
use crate::model::tangle::*;
use crate::model::transaction::Transaction;

const WAITING_DURATION_MS: u64 = 10000;

#[derive(Eq, PartialEq, Hash)]
enum WaitingFor {
    Trunk(SharedKey81),
    Branch(SharedKey81),
    Both(SharedKey81),
}

/// Type for handling incoming messages from the neighbors.
pub struct Receiver {
    config: SharedConfig,
    socket: UdpSocket,
    tangle: SharedTangle,
    sending_queue: SharedSendingQueue,
    request_queue: SharedRequestQueue,
    neighbors: SharedNeighbors,
    listeners: SharedListeners,
    buffer: [u8; PACKET_SIZE_WITH_REQUEST],
    waiting: HashMap<SharedKey81, HashSet<WaitingFor>>,
    deadlines: VecDeque<(SharedKey81, Instant)>,
}

impl Receiver {
    pub fn new(
        config: SharedConfig,
        socket: UdpSocket,
        tangle: SharedTangle,
        sending_queue: SharedSendingQueue,
        request_queue: SharedRequestQueue,
        neighbors: SharedNeighbors,
        listeners: SharedListeners,
    ) -> Self {
        Receiver {
            config,
            socket,
            tangle,
            sending_queue,
            request_queue,
            neighbors,
            listeners,
            buffer: [0_u8; PACKET_SIZE_WITH_REQUEST],
            waiting: HashMap::with_capacity(1000),
            deadlines: VecDeque::with_capacity(1000),
        }
    }

    #[inline]
    fn debug_on_receive(&self, num_bytes: usize, sender_addr: &SocketAddr) {
        debug!("Received {} bytes from {}.", num_bytes, sender_addr);

        #[cfg(debug_assertions)]
        println!(
            "{} > Received {} bytes from {}.",
            self.config.read().unwrap().port,
            num_bytes,
            sender_addr
        );
    }

    #[inline]
    fn warn_on_unknown_sender(&self, sender_addr: &SocketAddr) {
        warn!("{} is unknown. Ignoring packet.", sender_addr);

        #[cfg(debug_assertions)]
        println!(
            "{} > {} is unknown. Ignoring packet.",
            self.config.read().unwrap().port,
            sender_addr
        );
    }

    #[inline]
    fn warn_on_unknown_packet_size(&self, num_bytes: usize, sender_addr: &SocketAddr) {
        warn!(
            "Received wrong packet size ({} bytes) from: {}. Ignoring it.",
            num_bytes, sender_addr
        );

        #[cfg(debug_assertions)]
        println!(
            "{} > Received wrong packet size from: {}. Ignoring it.",
            self.config.read().unwrap().port,
            sender_addr
        );
    }

    #[inline]
    fn get_shareable_key81_from_buffer(&self, start: usize, length: usize) -> Arc<Key81> {
        Arc::new(Key81::from(trytes::from_54_bytes_2enc9(
            &self.buffer[start..start + length],
        )))
    }

    #[inline]
    fn get_shareable_key27_from_buffer(&self, start: usize, length: usize) -> Arc<Key27> {
        Arc::new(Key27::from(trytes::from_18_bytes_2enc9(
            &self.buffer[start..start + length],
        )))
    }

    #[inline]
    fn update_received_all(&mut self, sender_addr: &SocketAddr) {
        let mut neighbors = self.neighbors.write().unwrap();
        let mut neighbor = neighbors.get_mut(&sender_addr).unwrap();
        neighbor.stats.received_all += 1;
    }

    #[inline]
    fn update_received_invalid(&mut self, sender_addr: &SocketAddr) {
        let mut neighbors = self.neighbors.write().unwrap();
        let mut neighbor = neighbors.get_mut(&sender_addr).unwrap();
        neighbor.stats.received_invalid += 1;
    }

    #[inline]
    fn update_received_new(&mut self, sender_addr: &SocketAddr) {
        let mut neighbors = self.neighbors.write().unwrap();
        let mut neighbor = neighbors.get_mut(&sender_addr).unwrap();
        neighbor.stats.received_new += 1;
    }

    #[inline]
    fn add_response(&mut self, sender_index: usize, requested_key: SharedKey81, flags: Flags) {
        use SenderMode::*;

        if flags & (0x1 << (REQUESTER_OFFSET + sender_index)) == 0 {
            let mut queue = self.sending_queue.lock().unwrap();
            queue.push(
                (requested_key, Responding(sender_index)),
                Reverse(Instant::now()),
            );
            debug!("Added requested transaction to sending queue");
        } else {
            warn!("Request rejected due to 'repeated request' from same neighbor.");
        }
    }

    #[inline]
    fn add_request(&mut self, requested_key: SharedKey81, referrer_key: WaitingFor) {
        let mut request_queue = self.request_queue.write().unwrap();

        let mut hash_as_bytes = [0_u8; 54];
        hash_as_bytes.copy_from_slice(&self.buffer[TRUNK_HASH.4..TRUNK_HASH.4 + TRUNK_HASH.5]);

        // Add request bytes to the request queue
        request_queue.push_back(hash_as_bytes);

        // Remember issuing the request for a certain amount of time
        if let Some(referrers) = self.waiting.get_mut(&requested_key) {
            referrers.insert(referrer_key);
        } else {
            let mut referrers = HashSet::new();
            referrers.insert(referrer_key);
            self.waiting.insert(requested_key.clone(), referrers);
        }

        self.deadlines.push_back((
            requested_key.clone(),
            Instant::now() + Duration::from_millis(WAITING_DURATION_MS),
        ));
    }

    /*
    #[inline]
    fn create_vertex(&self, shareable_tx_key: SharedKey81) -> Vertex {
        // Extract transaction bytes
        let mut tx_bytes = [0u8; TRANSACTION_SIZE_BYTES];
        tx_bytes.copy_from_slice(&self.buffer[..TRANSACTION_SIZE_BYTES]);

        // Get address and tag based key
        let shareable_addr_key = self.get_shareable_key81_from_buffer(ADDRESS.4, ADDRESS.5);
        let shareable_tag_key = self.get_shareable_key27_from_buffer(TAG.4, TAG.5);

        Vertex::new(
            tx_bytes,
            shareable_tx_key,
            shareable_addr_key,
            shareable_tag_key,
        )
    }
    */
}

impl Stream for Receiver {
    type Item = ();
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<()>, io::Error> {
        loop {
            // Forget about issued requests after a certain time. Edges of older vertices
            // will get updated during that time if they were missing.
            {
                while let Some((_, deadline)) = self.deadlines.front() {
                    if *deadline > Instant::now() {
                        break;
                    }

                    // If we're here then deadline is due
                    let (waiting_key, _) = self.deadlines.pop_front().unwrap();

                    // Remove knowledge about that transaction being referenced
                    self.waiting.remove(&waiting_key);
                }
            }

            // Poll the socket for new data
            let (num_bytes, sender_addr) = try_ready!(self.socket.poll_recv_from(&mut self.buffer));
            self.debug_on_receive(num_bytes, &sender_addr);

            // Determine which neighbor sent the packet, ignore packet if sender is unknown
            let sender_index = {
                if let Some(neighbor) = self.neighbors.read().unwrap().get(&sender_addr) {
                    neighbor.index
                } else {
                    self.warn_on_unknown_sender(&sender_addr);
                    // why not 'continue' ? Busy loop ?
                    task::current().notify();
                    return Ok(Async::NotReady);
                }
            };

            // Update 'received_all' stat for this neighbor
            self.update_received_all(&sender_addr);

            // Determine whether the packet contains a request, ignore packet if size is unexpected
            // TODO: only one packet_size is sent in Java Ict
            let request_key_or_none = match num_bytes {
                PACKET_SIZE_WITH_REQUEST => {
                    Some(self.get_shareable_key81_from_buffer(REQUEST_HASH.4, REQUEST_HASH.5))
                }
                PACKET_SIZE => None,
                // Unknown packet size
                _ => {
                    self.warn_on_unknown_packet_size(num_bytes, &sender_addr);
                    self.update_received_invalid(&sender_addr);
                    // why not 'continue' ? Busy loop ?
                    task::current().notify();
                    return Ok(Async::NotReady);
                }
            };

            // Calculate hash of the received transaction bytes
            // TODO: use a fast hash algo (fnv or xxhash) to detect already known transactions
            // and prevent unnecessary slow Curl hashing
            let cloneable_tx_key = {
                let tx_trits = trits::from_tx_bytes_2enc9(&self.buffer[..TRANSACTION_SIZE_BYTES]);
                let hash_trits = curl::curl_tx(tx_trits, CURL_ROUNDS_TRANSACTION_HASH);
                Arc::new(Key81::from(trytes::from_trits_fixed81(&hash_trits)))
            };

            // Determine, if the transaction is known already. If that is the case we only
            // need to process attached requests but nothing else; also get th requested data
            // from the Tangle if available
            let (has_tx, requested_with_flags_or_none) = {
                let tangle = self.tangle.read().unwrap();
                (
                    tangle.has_transaction(&cloneable_tx_key),
                    match request_key_or_none.as_ref() {
                        Some(request_key) => tangle.get_vertex_and_flags(request_key),
                        None => None,
                    },
                )
            };

            // If there was a request attached we send a reply if that particular transaction
            // hasn't been requested already by that neighbor
            let tx_with_request = requested_with_flags_or_none.is_some();
            if tx_with_request {
                // Update flags for the requested transaction
                {
                    let mut tangle = self.tangle.write().unwrap();
                    tangle.update_requesters(request_key_or_none.as_ref().unwrap(), sender_index);
                }

                // Add the requested transaction as a reply to the sending queue
                let requested_key = request_key_or_none.as_ref().unwrap().clone();
                let (_, flags) = requested_with_flags_or_none.as_ref().unwrap();
                self.add_response(sender_index, requested_key, *flags);
            }

            // If this transaction was received already and was stored in the Tangle,
            // then we just need to update the vertex flags.
            if has_tx {
                let mut tangle = self.tangle.write().unwrap();

                // Update flags for the received transaction
                tangle.update_senders(&cloneable_tx_key, sender_index);

                continue;
            }

            // !!! If we're here then the incoming transaction is new to the node !!!

            // Update 'received_new' neighbor stat
            self.update_received_new(&sender_addr);

            // Notify listeners about the new transaction
            {
                let mut listeners = self.listeners.lock().unwrap();
                if !listeners.is_empty() {
                    let tx = Transaction::from_tx_bytes(&self.buffer[..TRANSACTION_SIZE_BYTES]);
                    for listener in listeners.iter_mut() {
                        listener.on_transaction_received(&tx, cloneable_tx_key.0);
                    }
                }
            }

            // Forward the newly received transaction to our neighbors
            {
                let mut queue = self.sending_queue.lock().unwrap();

                // Forwarding transactions is randomized (why exactly?)
                let forward_delay = {
                    let config = self.config.read().unwrap();
                    let random_delta = rand::thread_rng()
                        .gen_range(0, config.max_forward_delay - config.min_forward_delay);

                    Duration::from_millis(config.min_forward_delay + random_delta)
                };

                // Put received transaction into the sending queue for broadcasting
                queue.push(
                    (cloneable_tx_key.clone(), SenderMode::Broadcasting),
                    Reverse(Instant::now() + forward_delay),
                );

                debug!(
                        "Added new transaction to sending queue, which will be forwarded after {} milliseconds",
                        forward_delay.subsec_millis()
                    );
            }

            // !!! Now we need to build the edges of the DAG !!!

            // Determine trunk and branch keys
            let trunk_key = self.get_shareable_key81_from_buffer(TRUNK_HASH.4, TRUNK_HASH.5);
            let branch_key = self.get_shareable_key81_from_buffer(BRANCH_HASH.4, BRANCH_HASH.5);

            // Try to retreive references to trunk and branch
            let (maybe_trunk, maybe_branch) = {
                let tangle = self.tangle.read().unwrap();
                (
                    tangle.get_vertex(&trunk_key),
                    tangle.get_vertex(&branch_key),
                )
            };

            // Determine whether this node has received the trunk and/or branch already
            let has_trunk = maybe_trunk.is_some();
            let has_branch = maybe_branch.is_some();

            let was_requested = self.waiting.contains_key(&cloneable_tx_key);

            // We can try to fix missing trunks/branches by sending requests to our neighbors
            let can_request = (!has_trunk || !has_branch) && !was_requested;
            if can_request {
                // Add a request for missing trunk
                if !has_trunk {
                    if trunk_key == branch_key {
                        self.add_request(
                            trunk_key.clone(),
                            WaitingFor::Both(cloneable_tx_key.clone()),
                        );
                    } else {
                        self.add_request(
                            trunk_key.clone(),
                            WaitingFor::Trunk(cloneable_tx_key.clone()),
                        );
                    }
                }
                // Add a request for missing branch
                if !has_branch && branch_key != trunk_key {
                    self.add_request(
                        branch_key.clone(),
                        WaitingFor::Branch(cloneable_tx_key.clone()),
                    );
                }
            }

            // !!! If we're here, then this node has both trunk and branch in the Tangle

            // Attach already completed vertex to the Tangle
            {
                let mut tangle = self.tangle.write().unwrap();

                let cloneable_vertex = {
                    let mut tangle = self.tangle.write().unwrap();

                    tangle.attach_vertex(
                        cloneable_tx_key.clone(),
                        Vertex::from_buffer(&self.buffer, cloneable_tx_key.clone()),
                        0x1 << sender_index,
                        maybe_trunk,
                        maybe_branch,
                    )
                };

                // If this transaction was requested then update the edges of its referrers
                if let Some(referrers) = self.waiting.remove(&cloneable_tx_key) {
                    for waiting_for in referrers {
                        match waiting_for {
                            WaitingFor::Trunk(referrer_key) => {
                                tangle.update_trunk(&referrer_key, cloneable_vertex.clone());
                            }
                            WaitingFor::Branch(referrer_key) => {
                                tangle.update_branch(&referrer_key, cloneable_vertex.clone());
                            }
                            WaitingFor::Both(referrer_key) => {
                                tangle.update_trunk(&referrer_key, cloneable_vertex.clone());
                                tangle.update_branch(&referrer_key, cloneable_vertex.clone());
                            }
                        }
                    }
                }
            }
        }
    }
}
