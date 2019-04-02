use std::cmp::Reverse;
use std::io;
use std::thread;
use std::time::{Duration, Instant};

use futures::*;
use log::*;
use tokio::net::UdpSocket;

use super::listener::SharedListeners;
use super::neighbor::SharedNeighbors;

use crate::config::*;
use crate::constants::*;
use crate::convert::trytes;
use crate::ictarus::*;
use crate::model::tangle::*;

pub type NeighborIndex = usize;

/// Sending modes for a node. A node is either broadcasting transactions to
/// all of its neighbors, or its answering to one of its neighbors.
#[derive(Hash, Eq, PartialEq)]
pub enum SenderMode {
    /// Broadcast a transaction to all neighbors.
    Broadcasting,

    /// Send a transaction to a specific neighbor (e.g. as a reply to a request)
    Responding(NeighborIndex),
}

/// Type for handling outgoing messages to the neighbors.
pub struct Sender {
    config: SharedConfig,
    socket: UdpSocket,
    tangle: SharedTangle,
    sending_queue: SharedSendingQueue,
    request_queue: SharedRequestQueue,
    neighbors: SharedNeighbors,
    listeners: SharedListeners,
}

impl Sender {
    /// Creates a new sender.
    pub fn new(
        config: SharedConfig,
        socket: UdpSocket,
        tangle: SharedTangle,
        sending_queue: SharedSendingQueue,
        request_queue: SharedRequestQueue,
        neighbors: SharedNeighbors,
        listeners: SharedListeners,
    ) -> Self {
        Sender {
            config,
            socket,
            tangle,
            sending_queue,
            request_queue,
            neighbors,
            listeners,
        }
    }
}

impl Stream for Sender {
    type Item = ();
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<()>, io::Error> {
        use SenderMode::*;

        loop {
            try_ready!(self.socket.poll_write_ready());

            let ready_to_send = {
                let queue = self.sending_queue.lock().unwrap();
                if let Some((_, Reverse(next_sending_time))) = queue.peek() {
                    *next_sending_time <= Instant::now()
                } else {
                    false
                }
            };

            if !ready_to_send {
                // NOTE: I don't know of a better way to do this. If we don't sleep here for a while
                // we max out some cores.
                sleep!(10);
                task::current().notify();
                return Ok(Async::NotReady);
            }

            let ((key, mut sender_mode), _) = { self.sending_queue.lock().unwrap().pop().unwrap() };

            let (mut packet, send_with_request) = {
                let num_requests = {
                    let mut request_queue = self.request_queue.write().unwrap();
                    let tangle = self.tangle.read().unwrap();

                    // Remove all invalid requests up until the first that is valid
                    while !request_queue.is_empty() {
                        // Determine its key
                        let request_hash_as_bytes = request_queue.front().unwrap();
                        let request_hash_as_trytes =
                            trytes::from_54_bytes_2enc9(&request_hash_as_bytes[..]);
                        let request_key = Key81::from(request_hash_as_trytes);

                        // Check if it has arrived in the meantime
                        if tangle.has_transaction(&request_key) {
                            request_queue.pop_front();
                        } else {
                            break;
                        }
                    }
                    #[cfg(debug_assertions)]
                    println!(
                        "{} > Number of requests = {}",
                        self.config.read().unwrap().port,
                        request_queue.len()
                    );

                    request_queue.len()
                };

                if num_requests == 0 {
                    (vec![0u8; PACKET_SIZE], false)
                } else {
                    // Override sender_mode to 'Broadcasting', as we have valid pending requests in our
                    // request queue (requests are always broadcasted to all neighbors to increase chance
                    // of success)
                    sender_mode = Broadcasting;
                    (vec![0u8; PACKET_SIZE_WITH_REQUEST], true)
                }
            };

            // Copy the data of the transaction to send into the packet
            let flags = {
                let tangle = self.tangle.read().unwrap();
                // we know that the transaction exists in the Tangle, because we got the
                // key from the sending queue, which only contains known transaction hashes,
                // so calling 'unwrap' is safe
                let (vertex, flags) = tangle.get_vertex_and_flags(&key).unwrap();
                packet[..PACKET_SIZE].copy_from_slice(&vertex.bytes[..]);

                flags
            };

            // Append the hash of the requested transaction
            if send_with_request {
                let mut request_queue = self.request_queue.write().unwrap();
                if let Some(request_bytes) = request_queue.pop_front() {
                    packet[PACKET_SIZE..PACKET_SIZE_WITH_REQUEST]
                        .copy_from_slice(&request_bytes[..]);
                };
                #[cfg(debug_assertions)]
                println!(
                    "{} > Number of requests after send = {}",
                    self.config.read().unwrap().port,
                    request_queue.len()
                );
            }

            // Send the packet
            match sender_mode {
                Broadcasting => {
                    // Send transaction to neighbors that didn't send that tx to us, unless we
                    // have a request attached to it (to increase likelihood of success)
                    let config = self.config.read().unwrap();
                    for (neighbor_index, address) in config.neighbors.iter().enumerate() {
                        if flags & (0x1 << neighbor_index) == 0 || send_with_request {
                            self.socket.poll_send_to(&packet, &address).unwrap();
                            debug!(
                                "Broadcasted tx to '{}' (with request: {})",
                                &address, send_with_request
                            );

                            #[cfg(debug_assertions)]
                            println!(
                                "{} > Broadcasted tx to '{}' (with request: {})",
                                self.config.read().unwrap().port,
                                &address,
                                send_with_request
                            );
                        }
                    }
                }
                Responding(index) => {
                    let config = self.config.read().unwrap();
                    let address = config.neighbors[index];
                    self.socket.poll_send_to(&packet, &address).unwrap();
                    debug!("Sent requested tx to {}", &address);

                    #[cfg(debug_assertions)]
                    println!(
                        "{} > Sent requested tx to {}",
                        self.config.read().unwrap().port,
                        &address
                    );
                }
            }
        }
    }
}
