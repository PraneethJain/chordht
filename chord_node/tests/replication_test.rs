use chord_proto::chord::chord_client::ChordClient;
use chord_proto::chord::{GetRequest, PutRequest};
use chord_proto::hash_addr;
use std::time::Duration;
use tonic::Request;

mod common;
use common::{stabilize_ring, start_node};

#[tokio::test]
async fn test_replication() {
    const BASE_PORT: u16 = 62000;
    const NUM_NODES: usize = 3;

    println!("Creating {} nodes for replication test...", NUM_NODES);
    let mut nodes = Vec::new();
    let mut addresses = Vec::new();
    let mut handles = Vec::new();

    for i in 0..NUM_NODES {
        let addr = format!("127.0.0.1:{}", BASE_PORT + i as u16);
        let id = hash_addr(&addr);
        addresses.push(addr.clone());

        println!("Node {}: {} ({})", i, id, addr);
        let (node, handle) = start_node(id, addr).await;
        nodes.push(node);
        handles.push(handle);
    }

    println!("\nJoining nodes...");
    for (i, node) in nodes.iter().enumerate().skip(1) {
        node.join(addresses[0].clone())
            .await
            .unwrap_or_else(|_| panic!("Node {} failed to join", i));
        println!("Node {} joined", i);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    println!("\nStabilizing ring...");
    stabilize_ring(&nodes, 10).await;

    let key = "replication_key";
    let value = "replication_value";
    let key_id = hash_addr(key);

    println!("\nPutting key '{}' (ID: {})", key, key_id);

    let client_addr = format!("http://{}", addresses[0]);
    let mut client = ChordClient::connect(client_addr)
        .await
        .expect("Failed to connect to Node 0");

    client
        .put(Request::new(PutRequest {
            key: key.to_string(),
            value: value.to_string(),
        }))
        .await
        .expect("Put failed");

    println!("Waiting for replication...");
    tokio::time::sleep(Duration::from_secs(2)).await;

    println!("\nVerifying data on all nodes...");
    for (i, node) in nodes.iter().enumerate() {
        let state = node.state.read().await;
        if let Some(val) = state.store.get(key) {
            println!("Node {} (ID: {}) HAS key. Value: {}", i, node.id, val);
            assert_eq!(val, value, "Value mismatch on Node {}", i);
        } else {
            panic!("Node {} (ID: {}) MISSING key '{}'", i, node.id, key);
        }
    }

    println!("\nSimulating failure of Node 0 (Aborting task)...");
    handles[0].abort();

    println!("Waiting for stabilization after failure...");
    let survivors = &nodes[1..];
    stabilize_ring(survivors, 10).await;

    let client_addr_1 = format!("http://{}", addresses[1]);
    let mut client_1 = ChordClient::connect(client_addr_1)
        .await
        .expect("Failed to connect to Node 1");

    println!("Getting key '{}' from Node 1...", key);
    let response = client_1
        .get(Request::new(GetRequest {
            key: key.to_string(),
        }))
        .await
        .expect("Get failed from Node 1");

    assert_eq!(
        response.into_inner().value,
        value,
        "Value mismatch from Node 1 after Node 0 failure"
    );
    println!("✓ Data retrieved successfully from surviving node.");

    println!("\n✓ Replication test passed!");
}
