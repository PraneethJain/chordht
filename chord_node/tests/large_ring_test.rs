use chord_proto::chord::chord_server::Chord;
use chord_proto::chord::{GetRequest, PutRequest};
use chord_proto::hash_addr;
use std::time::Duration;
use tonic::Request;

mod common;
use common::{stabilize_ring, start_node};

#[tokio::test]
async fn test_large_chord_ring() {
    const NUM_NODES: usize = 20;
    const BASE_PORT: u16 = 61000;

    println!("Creating {} nodes...", NUM_NODES);
    let mut nodes = Vec::new();
    let mut addresses = Vec::new();

    for i in 0..NUM_NODES {
        let addr = format!(
            "{}:{}",
            chord_node::constants::LOCALHOST,
            BASE_PORT + i as u16
        );
        let id = hash_addr(&addr);
        addresses.push(addr.clone());

        println!("Node {}: {} ({})", i, id, addr);
        let (node, _handle) = start_node(id, addr).await;
        nodes.push(node);

        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    println!("\nWaiting for all nodes to be fully ready...");
    tokio::time::sleep(Duration::from_millis(500)).await;

    println!("\nJoining nodes to ring...");
    for i in 1..NUM_NODES {
        nodes[i]
            .join(addresses[0].clone())
            .await
            .unwrap_or_else(|_| panic!("Node {} failed to join", i));
        println!("Node {} joined", i);

        tokio::time::sleep(Duration::from_millis(100)).await;

        if i % 3 == 0 || i == NUM_NODES - 1 {
            println!("Stabilizing after {} nodes...", i + 1);
            stabilize_ring(&nodes[0..=i], 5).await;
        }
    }

    println!("\nFinal stabilization of complete ring...");
    stabilize_ring(&nodes, 30).await;

    println!("\nVerifying ring structure...");

    // First, print all node IDs sorted to see what we have
    let mut node_ids: Vec<u64> = nodes.iter().map(|n| n.id).collect();
    node_ids.sort();
    println!("All node IDs (sorted): {:?}", node_ids);

    let mut visited = std::collections::HashSet::new();
    let mut current_id = nodes[0].id;

    for _ in 0..NUM_NODES {
        visited.insert(current_id);

        // Find the node with this ID
        let current_node = nodes.iter().find(|n| n.id == current_id).unwrap();
        let state = current_node.state.read().await;
        let successor = state.successor_list[0].clone();

        println!("Node {} -> {}", current_id, successor.id);
        current_id = successor.id;
    }

    // After NUM_NODES hops, we should be back at the start
    assert_eq!(current_id, nodes[0].id, "Ring is not properly formed");
    assert_eq!(visited.len(), NUM_NODES, "Not all nodes are in the ring");
    println!("✓ Ring structure is valid");

    println!("\nTesting Put/Get operations...");
    let test_cases = [
        ("key1", "value1"),
        ("key2", "value2"),
        ("key3", "value3"),
        ("hello", "world"),
        ("foo", "bar"),
        ("test", "data"),
        ("chord", "dht"),
        ("distributed", "hash_table"),
    ];

    // Put keys from different nodes
    for (i, (key, value)) in test_cases.iter().enumerate() {
        let put_node = &nodes[i % NUM_NODES];
        let key_id = hash_addr(key);
        println!(
            "Putting '{}' (ID: {}) via node {}",
            key, key_id, put_node.id
        );

        let put_req = Request::new(PutRequest {
            key: key.to_string(),
            value: value.to_string(),
        });

        put_node
            .put(put_req)
            .await
            .unwrap_or_else(|_| panic!("Put failed for key '{}'", key));
    }

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Get keys from different nodes (different from where they were put)
    for (i, (key, expected_value)) in test_cases.iter().enumerate() {
        let get_node = &nodes[(i + NUM_NODES / 2) % NUM_NODES];
        let key_id = hash_addr(key);
        println!(
            "Getting '{}' (ID: {}) via node {}",
            key, key_id, get_node.id
        );

        let get_req = Request::new(GetRequest {
            key: key.to_string(),
        });

        let response = get_node
            .get(get_req)
            .await
            .unwrap_or_else(|_| panic!("Get failed for key '{}'", key));
        let resp = response.into_inner();

        assert!(resp.found, "Key '{}' not found", key);
        assert_eq!(
            resp.value, *expected_value,
            "Value mismatch for key '{}'",
            key
        );
        println!("✓ Got '{}' = '{}'", key, resp.value);
    }

    println!("\n✓ All Put/Get operations successful!");

    println!("\nVerifying key distribution...");
    let mut stored_keys = std::collections::BTreeSet::new();
    let mut nodes_with_keys = 0;

    for (i, node) in nodes.iter().enumerate() {
        let state = node.state.read().await;
        let num_keys = state.store.len();
        if num_keys > 0 {
            nodes_with_keys += 1;
            println!(
                "Node {} (ID: {}) has {} keys: {:?}",
                i,
                node.id,
                num_keys,
                state.store.keys().collect::<Vec<_>>()
            );
        }
        stored_keys.extend(state.store.keys().cloned());
    }

    let total_keys = stored_keys.len();
    assert_eq!(total_keys, test_cases.len(), "Total keys mismatch");
    println!(
        "✓ Total keys: {}, stored across {} nodes",
        total_keys, nodes_with_keys
    );

    println!("\n✓✓✓ Large ring test passed! ✓✓✓");
}
