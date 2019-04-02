use rand::{thread_rng, Rng};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{Duration, Instant};

use ictarus::config::ConfigBuilder;
use ictarus::constants::*;
use ictarus::ictarus::*;

/// This test creates a random network (neighbor selection and neighbor count
/// are random) of configurable size.
///
/// Results:
///     * Setting 'MIN_NEIGHBOR_COUNT = 2' drastically improves message
///       propagation; for a network up to 50 nodes (creating a larger network
///       gave me an OS error) in all test runs so far 100% propagation ratio.
///     * With 'MIN_NEIGHBOR_COUNT = 1' things look quite differently. In many
///       test runs the propagation ratio is very low.
///
/// Open questions:
///     * How many nodes with 1 working neighbor can be tolerated?
///     * What happens if nodes can only pick a neighbor randomly from a set
///       of nearby nodes? We could simulate the vicinity of nodes by their
///       port numbers, for example node '1400' can only pick from '1395..1405'.
///
/// !!!WARNING: DO NOT RUN THIS TEST TOGETHER WITH ALL OTHER TESTS!!!
//#[test]
fn test_random_topology() {
    const MIN_NEIGHBOR_COUNT: usize = 1;
    const NUM_NODES: u16 = 10;
    const WAIT_FOR_COMMUNICATION_TO_END_MS: u64 = 10000;
    const START_PORT: u16 = 1370;

    let mut rng = thread_rng();

    let mut configs = HashMap::new();
    let mut excluded = HashSet::new();
    let mut num_slots;
    let mut num_open_slots;
    let mut current_node;
    let mut neighbor;

    // Set up configs and randomly select a number of neighbors for each node
    for i in 0..NUM_NODES {
        let config = ConfigBuilder::default().port(START_PORT + i).build();

        // determine how many neighbors this node will have
        num_slots = rng.gen_range(MIN_NEIGHBOR_COUNT, MAX_NEIGHBOR_COUNT + 1);

        configs.insert(START_PORT + i, (num_slots, config));
    }

    // Create a random network of nodes, where each node has at least 1
    // and at most MAX_NEIGHBOR_COUNT neighbors
    for i in 0..NUM_NODES {
        current_node = START_PORT + i;

        let (num_slots, own_config) = {
            let (n, c) = configs.get(&current_node).unwrap();
            (n.clone(), c.clone())
        };

        num_open_slots = num_slots - own_config.neighbors.len();

        print!(
            "{} (total slots: {}, open slots left: {}): ",
            current_node, num_slots, num_open_slots
        );

        // Skip this node if its slots are already full (because it was picked by previous nodes)
        if num_open_slots == 0 {
            let (_, config) = configs.get(&current_node).unwrap();
            for addr in config.neighbors.iter() {
                print!("{} ", addr.port());
            }
            println!();
            continue;
        }

        excluded.clear();

        // Exclude own port and ports of already connected neighbors
        excluded.insert(own_config.port);
        let (_, config) = configs.get(&current_node).unwrap();
        for addr in config.neighbors.iter() {
            excluded.insert(addr.port());
        }

        // For each open slot find a neighbor randomly (if possible)
        for _ in 0..num_open_slots {
            // find a neighbor port
            loop {
                // if we picked every possible port number already we break out of the loop
                if excluded.len() == NUM_NODES as usize {
                    //println!("no more neighbors available");
                    print!("OPEN ");
                    break;
                }

                neighbor = rng.gen_range(START_PORT, START_PORT + NUM_NODES);
                if excluded.contains(&neighbor) {
                    continue;
                }

                let (other_num_neighbors, other_config) = {
                    let (other_num_neighbors, other_config) = configs.get(&neighbor).unwrap();
                    (other_num_neighbors.clone(), other_config.clone())
                };

                // other node is already full
                if other_config.neighbors.len() >= other_num_neighbors {
                    excluded.insert(neighbor);
                    continue;
                }

                // connect neighbor to us
                {
                    let (_, config) = configs.get_mut(&neighbor).unwrap();
                    config.add_neighbor(&format!("{}:{}", &own_config.host, &own_config.port));
                }

                // connect to neighbor
                {
                    let (_, config) = configs.get_mut(&own_config.port).unwrap();
                    config.add_neighbor(&format!("{}:{}", other_config.host, other_config.port));
                }

                // exclude paired node so it can't be selected again
                excluded.insert(neighbor);
                break;
            }
        }
        let (_, config) = configs.get(&current_node).unwrap();
        for addr in config.neighbors.iter() {
            print!("{} ", addr.port());
        }
        println!();
    }

    println!();

    // create the network of nodes from the configs
    let mut nodes = vec![];
    for (_, (_, config)) in configs {
        nodes.push(Ictarus::new(Arc::new(RwLock::new(config.clone()))));
    }

    // fire up all nodes
    for node in nodes.iter_mut() {
        node.run()
            .unwrap_or_else(|e| panic!("couldn't start node {}: {}", node.get_config().port, e));
    }

    // TEMP: First node sends a message. We want to see how many nodes on average
    // will receive the message
    let tx_hash = {
        let a = nodes.get_mut(0).unwrap();
        println!("\n{} is submitting THE message\n", a.get_config().port);
        a.submit_message("HELLO NETWORK", None)
    };

    // wait for the network to settle
    thread::sleep(Duration::from_millis(WAIT_FOR_COMMUNICATION_TO_END_MS));

    // print how many nodes hold the transaction
    let mut num_received_message = 0;
    for node in nodes.iter() {
        if node.get_transaction(&tx_hash).is_some() {
            num_received_message += 1;
            println!("{} received the message", node.get_config().port);
        } else {
            println!("{} didn't receive the message", node.get_config().port);
        }
    }

    println!();
    println!("+--------------------RESULTS--------------------+");
    println!(
        "num nodes total: {}\nnum messages received: {}\n==> {:.1}%",
        NUM_NODES,
        num_received_message,
        num_received_message as f64 / NUM_NODES as f64 * 100_f64
    );
    println!("+-----------------------------------------------+");
    println!();

    // kill all nodes
    for node in nodes.drain(..) {
        node.kill();
    }
}
