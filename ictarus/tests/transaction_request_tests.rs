use ictarus::config::*;
use ictarus::ictarus::*;

use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Duration;

/// This test first creates two nodes A and B with A submitting a message to the network.
/// Sometime later node C joins the network by neighboring with A and B. C then requests
/// the former exchanged message (it just happens to know the transaction hash).
///
/// The test passes, if C get served the requested transaction by both A and B.
//#[test]
fn test_request_transaction_upon_joining() {
    let mut a = Ictarus::new(get_config(1304, vec![1305]));
    let mut b = Ictarus::new(get_config(1305, vec![1304]));

    a.run().expect("couldn't start node A");
    b.run().expect("coudln't start node B");

    // Wait for nodes to initalize
    thread::sleep(Duration::from_millis(1000));

    println!("Sending THE message...");
    let tx_hash = a.submit_message("Once ... I will be requested.", None);

    // Wait for B to receive the message
    thread::sleep(Duration::from_millis(2000));

    // Ensure A and B have the transaction
    assert!(a.has_transaction(&tx_hash));
    assert!(b.has_transaction(&tx_hash));

    // C joins the network
    let mut c = Ictarus::new(get_config(1306, vec![1304, 1305]));
    let addr = c.get_config().get_socket_addr();
    println!("Node C ({}) is joining the network", addr);
    a.add_neighbor(addr);
    b.add_neighbor(addr);

    // Ensure C is correctly neighbored to A and B
    assert!(is_neighbored_with(&a, &c));
    assert!(is_neighbored_with(&b, &c));
    assert!(is_neighbored_with(&c, &a));
    assert!(is_neighbored_with(&c, &b));

    c.run().expect("couldn't start node C");

    // Because of how requests work we need a regular transaction as request carrier
    assert_eq!(true, c.request_transaction(&tx_hash));
    let carrier_hash = c.submit_message("request carrier", None);

    // Wait for the communication to end
    thread::sleep(Duration::from_millis(4000));

    assert!(a.has_transaction(&carrier_hash));
    assert!(b.has_transaction(&carrier_hash));

    // C should now have the transaction
    assert!(c.has_transaction(&tx_hash));

    // A and B should both have served the request
    assert_eq!(1, c.get_neighbor_by_index(0).stats.received_all);
    assert_eq!(1, c.get_neighbor_by_index(1).stats.received_all);

    a.kill();
    b.kill();
    c.kill();

    thread::sleep(Duration::from_millis(1000));
}

// Helper function to quickly check if
fn is_neighbored_with(node_a: &Ictarus, node_b: &Ictarus) -> bool {
    let address = node_b.get_config().get_socket_addr();
    node_a.get_neighbors().contains_key(&address)
}

// Helper function to quickly create local nodes
fn get_config(port: u16, neighbors: Vec<u16>) -> SharedConfig {
    let mut builder = ConfigBuilder::default().port(port);
    for port in neighbors {
        builder = builder.neighbor(&format!("127.0.0.1:{}", port));
    }
    Arc::new(RwLock::new(builder.build()))
}
