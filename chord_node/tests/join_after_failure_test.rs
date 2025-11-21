use chord_proto::hash_addr;
use std::time::Duration;

mod common;
use common::{stabilize_ring, start_node};

#[tokio::test]
async fn test_join_failure_after_node_departure() {
    let addr1 = "127.0.0.1:51001".to_string();
    let addr2 = "127.0.0.1:51002".to_string();
    let addr3 = "127.0.0.1:51003".to_string();

    let id1 = hash_addr(&addr1);
    let id2 = hash_addr(&addr2);
    let id3 = hash_addr(&addr3);

    println!("Node 1: {} ({})", id1, addr1);
    println!("Node 2: {} ({})", id2, addr2);
    println!("Node 3: {} ({})", id3, addr3);

    let (node1, _h1) = start_node(id1, addr1.clone()).await;

    let (node2, h2) = start_node(id2, addr2.clone()).await;
    node2
        .join(addr1.clone())
        .await
        .expect("Node 2 failed to join Node 1");

    let (node3, _h3) = start_node(id3, addr3.clone()).await;
    node3
        .join(addr1.clone())
        .await
        .expect("Node 3 failed to join Node 1");

    let nodes = vec![node1.clone(), node2.clone(), node3.clone()];

    println!("Stabilizing...");
    stabilize_ring(&nodes, 5).await;

    println!("Killing Node 2...");
    h2.abort();
    // Wait for port to be freed? Abort just kills the task.
    tokio::time::sleep(Duration::from_millis(500)).await;

    println!("Stabilizing after node death...");
    stabilize_ring(&[node1.clone(), node3.clone()], 5).await;

    let addr4 = "127.0.0.1:51004".to_string();
    let id4: u64 = 3000000000000000000;
    println!("Node 4: {} ({})", id4, addr4);

    let (node4, _h4) = start_node(id4, addr4.clone()).await;

    println!("Node 4 joining via Node 1...");
    // This is expected to SUCCEED now
    match node4.join(addr1.clone()).await {
        Ok(_) => println!("Node 4 joined successfully"),
        Err(e) => {
            panic!("Node 4 failed to join: {}", e);
        }
    }
}
