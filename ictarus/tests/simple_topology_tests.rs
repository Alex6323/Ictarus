use ictarus::config::*;
use ictarus::convert::ascii;
use ictarus::ictarus::*;

use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{Duration, Instant};

// Helper function to quickly create local nodes
fn get_config(port: u16, neighbors: Vec<u16>) -> SharedConfig {
    let mut builder = ConfigBuilder::default().port(port);
    for port in neighbors {
        builder = builder.neighbor(&format!("127.0.0.1:{}", port));
    }
    Arc::new(RwLock::new(builder.build()))
}

/// This test creates two nodes A and B. Both nodes submit a unique message
/// to the network.
///
/// The test is passed, if in the end both nodes hold both messages in their
/// Tangle datastructure.
//#[test]
fn test_connection_2nodes() {
    let mut a = Ictarus::new(get_config(1337, vec![1338]));
    let mut b = Ictarus::new(get_config(1338, vec![1337]));

    assert!(a.run().is_ok());
    assert!(b.run().is_ok());

    // Let each node submit a message
    let msg_a_hash = a.submit_message("PING", None);
    let msg_b_hash = b.submit_message("PONG", None);

    println!("Hash of message PING: \n{}", msg_a_hash);
    println!("Hash of message PONG: \n{}", msg_b_hash);

    // Wait for the network
    thread::sleep(Duration::from_millis(1000));

    // Node A holds both messages
    assert!(a.has_transaction(&msg_a_hash));
    assert!(a.has_transaction(&msg_b_hash));

    // Node B holds both messages
    assert!(b.has_transaction(&msg_a_hash));
    assert!(b.has_transaction(&msg_b_hash));

    // Their Tangles should contain exactly two transactions
    let a_tangle_stats = a.get_tangle_stats();
    let b_tangle_stats = b.get_tangle_stats();
    assert_eq!(2, a_tangle_stats.num_vertices_by_hash);
    assert_eq!(2, b_tangle_stats.num_vertices_by_hash);

    // Both nodes received 1 message from the other node
    assert_eq!(1, a.get_neighbor_by_index(0).stats.received_all);
    assert_eq!(1, b.get_neighbor_by_index(0).stats.received_all);

    let txa = b.get_transaction(&msg_a_hash).unwrap();
    let txb = a.get_transaction(&msg_b_hash).unwrap();
    println!(
        "Deserialized signature/message fragments of message PING: \n{}",
        ascii::from_tryte_string(&txa.signature_fragments)
    );
    println!(
        "Deserialized signature/message fragments of message PONG: \n{}",
        ascii::from_tryte_string(&txb.signature_fragments)
    );

    println!(
        "Stats for A's neighbor '{}': received_all: {}",
        a.get_neighbor_by_index(0).address,
        a.get_neighbor_by_index(0).stats.received_all
    );
    println!(
        "Stats for B's neighbor '{}': received_all: {}",
        b.get_neighbor_by_index(0).address,
        b.get_neighbor_by_index(0).stats.received_all
    );

    a.kill();
    b.kill();

    thread::sleep(Duration::from_millis(1000));
}

/// This test creates two nodes A and B. A will then send a bunch of messages
/// to B.
///
/// The test is passed, if B receives all messages from A.
//#[test]
fn test_b_receives_all_messages_from_a() -> Result<(), Box<std::error::Error>> {
    let mut a = Ictarus::new(get_config(1341, vec![1342]));
    let mut b = Ictarus::new(get_config(1342, vec![1341]));

    a.run()?;
    b.run()?;

    thread::sleep(Duration::from_millis(1000));

    const NUM_MESSAGES: u64 = 10;
    for i in 0..NUM_MESSAGES {
        let message = &format!("Message #{}", i);
        println!("A: Submitting {}", message);
        a.submit_message(message, None);
    }

    // Wait for the network to propagate all messages. If this is set too low the test will fail.
    thread::sleep(Duration::from_millis(2000));

    println!(
        "Stats for B's neighbor '{}': received_all: {}",
        b.get_neighbor_by_index(0).address,
        b.get_neighbor_by_index(0).stats.received_all
    );
    assert_eq!(NUM_MESSAGES, b.get_neighbor_by_index(0).stats.received_all);

    a.kill();
    b.kill();

    thread::sleep(Duration::from_millis(1000));

    Ok(())
}

/// This test creates 3 nodes A, B and C, which are all neighbored to eachother.
/// A will submit a bunch of messages to the network.
///
/// The test is passed, if messages are not echoed back to the sender. That's why
/// node A's received_all stat for node B and C should be 0.
//#[test]
fn test_no_infinite_loop_message_forwarding() -> Result<(), Box<std::error::Error>> {
    let mut a = Ictarus::new(get_config(1350, vec![1351, 1352]));
    let mut b = Ictarus::new(get_config(1351, vec![1350, 1352]));
    let mut c = Ictarus::new(get_config(1352, vec![1350, 1351]));

    a.run()?;
    b.run()?;
    c.run()?;

    thread::sleep(Duration::from_millis(1000));

    const NUM_MESSAGES: u64 = 10;
    for i in 0..NUM_MESSAGES {
        let message = &format!("Message #{}", i);
        println!("A: Submitting {}", message);
        a.submit_message(message, None);
    }

    // Wait for the network
    thread::sleep(Duration::from_millis(5000));

    println!(
        "Stats for C's neighbor '{}': received_all: {}",
        c.get_neighbor_by_index(0).address,
        c.get_neighbor_by_index(0).stats.received_all
    );
    println!(
        "Stats for C's neighbor '{}': received_all: {}",
        c.get_neighbor_by_index(1).address,
        c.get_neighbor_by_index(1).stats.received_all
    );

    // C should get those transactions directly from A and indirectly from B
    assert!(c.get_neighbor_by_index(0).stats.received_all <= NUM_MESSAGES);
    assert!(
        c.get_neighbor_by_index(0).stats.received_all
            + c.get_neighbor_by_index(1).stats.received_all
            >= NUM_MESSAGES
    );

    // A was the sender of all messages, so its neighbors shouldn't sent those
    // transactions back
    assert_eq!(0, a.get_neighbor_by_index(0).stats.received_all);
    assert_eq!(0, a.get_neighbor_by_index(1).stats.received_all);

    a.kill();
    b.kill();
    c.kill();

    thread::sleep(Duration::from_millis(1000));

    Ok(())
}

/// This test creates three nodes A, B and C, whereby A and C are not
/// directly neighbored. For them to be able to communicate their
/// messages have to be routed through node B. The topology looks
/// like this: A <-> B <-> C.
///
/// A will send a bunch of messages. The test is passed if all messages
/// reach C.
//#[test]
fn test_3node_bridge_topology() -> Result<(), Box<std::error::Error>> {
    let mut a = Ictarus::new(get_config(1360, vec![1361]));
    let mut b = Ictarus::new(get_config(1361, vec![1360, 1362]));
    let mut c = Ictarus::new(get_config(1362, vec![1361]));

    a.run()?;
    b.run()?;
    c.run()?;

    thread::sleep(Duration::from_millis(1000));

    const NUM_MESSAGES: u64 = 10;
    for i in 0..NUM_MESSAGES {
        let message = &format!("Message #{}", i);
        println!("A: Submitting {}", message);
        a.submit_message(&format!("Message #{}", i), None);
    }

    thread::sleep(Duration::from_millis(5000));

    assert_eq!(NUM_MESSAGES, c.get_neighbor_by_index(0).stats.received_all);

    println!(
        "Stats for C's neighbor '{}': received_all: {}",
        c.get_neighbor_by_index(0).address,
        c.get_neighbor_by_index(0).stats.received_all
    );

    a.kill();
    b.kill();
    c.kill();

    thread::sleep(Duration::from_millis(1000));

    Ok(())
}
