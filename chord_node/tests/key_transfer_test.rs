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
    let port_a = 15000;
    let addr_a = format!("{}:{}", chord_node::constants::LOCALHOST, port_a);
    let id_a = hash_addr(&addr_a);
    let (node_a, _h1) = start_node(id_a, addr_a.clone()).await;
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

    let mut addr_b = format!("{}:{}", chord_node::constants::LOCALHOST, 15001);
    let mut id_b = hash_addr(&addr_b);

    let mut found = false;
    for p in 15001..16000 {
        let a = format!("{}:{}", chord_node::constants::LOCALHOST, p);
        let i = hash_addr(&a);

        if Node::is_in_range_inclusive(key_id, node_a.id, i) {
            addr_b = a;
            id_b = i;
            found = true;
            break;
        }
    }

    if !found {
        panic!("Could not find a suitable port for Node B to take key");
    }

    println!("Starting Node B at {} with ID {}", addr_b, id_b);
    let (node_b, _h2) = start_node(id_b, addr_b.clone()).await;
    node_b.join(addr_a.clone()).await.expect("Failed to join");

    println!("Stabilizing...");
    stabilize_ring(&[node_a.clone(), node_b.clone()], 20).await;

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
