use chord_proto::chord::{GetRequest, PutRequest};
use chord_proto::hash_addr;

use tonic::Request;

mod common;
use common::{stabilize_ring, start_node};

#[tokio::test]
async fn test_chord_ring_formation_and_routing() {
    let (node1, _h1) = start_node("127.0.0.1:0".to_string()).await;
    let addr1 = node1.addr.clone();
    let (node2, _h2) = start_node("127.0.0.1:0".to_string()).await;
    let addr2 = node2.addr.clone();
    let (node3, _h3) = start_node("127.0.0.1:0".to_string()).await;
    let addr3 = node3.addr.clone();

    println!("Node 1: {} ({})", node1.id, addr1);
    println!("Node 2: {} ({})", node2.id, addr2);
    println!("Node 3: {} ({})", node3.id, addr3);

    node2
        .join(addr1.clone())
        .await
        .expect("Node 2 failed to join Node 1");

    node3
        .join(addr1.clone())
        .await
        .expect("Node 3 failed to join Node 1");

    let nodes = vec![node1.clone(), node2.clone(), node3.clone()];

    println!("Stabilizing...");
    stabilize_ring(&nodes, 10).await;

    for node in &nodes {
        let state = node.state.read().await;
        let succ = state.successor_list[0].clone();
        println!("Node {} successor is {}", node.id, succ.id);
    }

    let key = "test_key";
    let value = "test_value";
    let key_id = hash_addr(key);
    println!("Key '{}' has ID {}", key, key_id);

    let put_req = Request::new(PutRequest {
        key: key.to_string(),
        value: value.to_string(),
    });
    use chord_proto::chord::chord_server::Chord;
    node1.put(put_req).await.expect("Put failed");

    println!("Getting key from Node 3...");
    let get_req = Request::new(GetRequest {
        key: key.to_string(),
    });
    let response = node3.get(get_req).await.expect("Get failed");
    let resp = response.into_inner();

    assert!(resp.found, "Key not found");
    assert_eq!(resp.value, value, "Value mismatch");
    println!("Test passed!");
}
