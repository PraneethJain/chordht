use chord_node::Node;
use chord_proto::chord::chord_server::{Chord, ChordServer};
use chord_proto::chord::{GetRequest, PutRequest};
use chord_proto::hash_addr;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tonic::transport::Server;
use tonic::Request;

// Helper to start a node in a background task
async fn start_node(id: u64, addr: String) -> Arc<Node> {
    let node = Arc::new(Node::new(id, addr.clone()));
    let node_clone = node.clone();
    let addr_clone = addr.clone();

    tokio::spawn(async move {
        let addr: SocketAddr = addr_clone.parse().unwrap();
        let listener = TcpListener::bind(addr).await.unwrap();
        println!("Node {} listening on {}", id, addr_clone);

        Server::builder()
            .add_service(ChordServer::new((*node_clone).clone()))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    // Give it a moment to start - increased to ensure server is fully ready
    tokio::time::sleep(Duration::from_millis(150)).await;
    node
}

#[tokio::test]
async fn test_large_chord_ring() {
    const NUM_NODES: usize = 20;
    const BASE_PORT: u16 = 60000;

    println!("Creating {} nodes...", NUM_NODES);
    let mut nodes = Vec::new();
    let mut addresses = Vec::new();

    // 1. Create all nodes with delays to ensure they're fully started
    for i in 0..NUM_NODES {
        let addr = format!("127.0.0.1:{}", BASE_PORT + i as u16);
        let id = hash_addr(&addr);
        addresses.push(addr.clone());

        println!("Node {}: {} ({})", i, id, addr);
        let node = start_node(id, addr).await;
        nodes.push(node);

        // Small delay between node creations to avoid overwhelming the system
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Extra delay to ensure all nodes are fully ready
    println!("\nWaiting for all nodes to be fully ready...");
    tokio::time::sleep(Duration::from_millis(500)).await;

    // 2. Join nodes gradually with stabilization in between
    println!("\nJoining nodes to ring...");
    for i in 1..NUM_NODES {
        nodes[i]
            .join(addresses[0].clone())
            .await
            .unwrap_or_else(|_| panic!("Node {} failed to join", i));
        println!("Node {} joined", i);

        // Small delay after each join to let it settle
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Stabilize after every few joins
        if i % 3 == 0 || i == NUM_NODES - 1 {
            println!("Stabilizing after {} nodes...", i + 1);
            for _ in 0..5 {
                for node in nodes.iter().take(i + 1) {
                    node.stabilize().await;
                    node.fix_fingers().await;
                    node.check_predecessor().await;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }

    // 3. Final stabilization of the complete ring - more rounds to ensure convergence
    println!("\nFinal stabilization of complete ring...");
    for round in 0..30 {
        if round % 5 == 0 {
            println!("Stabilization round {}", round);
        }
        for node in &nodes {
            node.stabilize().await;
            node.fix_fingers().await;
            node.check_predecessor().await;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // 4. Verify ring structure - check that successors form a valid ring
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

    // 5. Test Put/Get operations with multiple keys
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

    // Small delay to ensure all puts complete
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

    // 6. Verify that keys are distributed across nodes
    println!("\nVerifying key distribution...");
    let mut total_keys = 0;
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
        total_keys += num_keys;
    }

    assert_eq!(total_keys, test_cases.len(), "Total keys mismatch");
    println!(
        "✓ Total keys: {}, stored across {} nodes",
        total_keys, nodes_with_keys
    );

    println!("\n✓✓✓ Large ring test passed! ✓✓✓");
}
