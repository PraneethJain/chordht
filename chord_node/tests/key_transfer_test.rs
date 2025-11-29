use chord_node::Node;
use chord_proto::chord::chord_client::ChordClient;
use chord_proto::chord::{GetRequest, PutRequest};
use chord_proto::hash_addr;
use std::time::Duration;
use tokio::time::sleep;
use tonic::Request;

mod common;
use common::{stabilize_ring, start_node};

#[tokio::test]
async fn test_key_transfer_on_join_and_leave() {
    let (node_a, _h1) = start_node("127.0.0.1:0".to_string()).await;
    let addr_a = node_a.addr.clone();
    println!("Node A started at {} with ID {}", addr_a, node_a.id);

    let key = "test_key";
    let key_id = hash_addr(key);
    println!("Key '{}' has ID {}", key, key_id);

    let mut client_a = ChordClient::connect(format!("http://{}", addr_a))
        .await
        .unwrap();
    client_a
        .put(Request::new(PutRequest {
            key: key.to_string(),
            value: "value1".to_string(),
        }))
        .await
        .unwrap();

    let resp = client_a
        .get(Request::new(GetRequest {
            key: key.to_string(),
        }))
        .await
        .unwrap();
    assert_eq!(resp.into_inner().value, "value1");

    {
        let state = node_a.state.read().await;
        assert!(state.store.contains_key(key));
    }

    println!("Starting Node B at a random port (0)");
    let (node_b, _h2) = start_node("127.0.0.1:0".to_string()).await;
    let addr_b = node_b.addr.clone();
    let id_b = node_b.id;
    println!("Node B started at {} with ID {}", addr_b, id_b);

    node_b.join(addr_a.clone()).await.expect("Failed to join");

    println!("Stabilizing...");
    stabilize_ring(&[node_a.clone(), node_b.clone()], 20).await;

    let key_owner_id = if Node::is_in_range_inclusive(key_id, node_a.id, node_b.id) {
        node_b.id
    } else {
        node_a.id
    };

    if key_owner_id == node_b.id {
        println!(
            "Key '{}' (ID {}) should be on Node B (ID {})",
            key, key_id, node_b.id
        );
        {
            let state = node_b.state.read().await;
            assert!(state.store.contains_key(key), "Node B should have the key");
        }
        {
            let state = node_a.state.read().await;
            assert!(
                !state.store.contains_key(key),
                "Node A should NOT have the key"
            );
        }
    } else {
        println!(
            "Key '{}' (ID {}) should be on Node A (ID {})",
            key, key_id, node_a.id
        );
        {
            let state = node_a.state.read().await;
            assert!(state.store.contains_key(key), "Node A should have the key");
        }
        {
            let state = node_b.state.read().await;
            assert!(
                !state.store.contains_key(key),
                "Node B should NOT have the key"
            );
        }
    }

    println!("Node B leaving...");
    node_b.leave_network().await;

    sleep(Duration::from_secs(1)).await;

    {
        let state = node_a.state.read().await;
        assert!(
            state.store.contains_key(key),
            "Node A should have the key back"
        );
    }
}
