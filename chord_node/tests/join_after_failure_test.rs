use std::time::Duration;

mod common;
use common::{stabilize_ring, start_node};

#[tokio::test]
async fn test_join_failure_after_node_departure() {
    let (node1, _h1) = start_node("127.0.0.1:0".to_string()).await;
    let (node2, _h2) = start_node("127.0.0.1:0".to_string()).await;
    let (node3, _h3) = start_node("127.0.0.1:0".to_string()).await;

    println!("Node 1: {} ({})", node1.id, node1.addr);
    println!("Node 2: {} ({})", node2.id, node2.addr);
    println!("Node 3: {} ({})", node3.id, node3.addr);

    node2.join(node1.addr.clone()).await.unwrap();
    node3.join(node1.addr.clone()).await.unwrap();

    println!("Stabilizing...");
    stabilize_ring(&[node1.clone(), node2.clone(), node3.clone()], 10).await;

    println!("Node 2 leaving...");
    node2.leave_network().await;

    tokio::time::sleep(Duration::from_millis(500)).await;

    println!("Stabilizing after node death...");
    stabilize_ring(&[node1.clone(), node3.clone()], 10).await;

    println!("Node 4 joining...");
    let (node4, _h4) = start_node("127.0.0.1:0".to_string()).await;
    println!("Node 4: {} ({})", node4.id, node4.addr);

    println!("Node 4 joining via Node 3...");
    match node4.join(node3.addr.clone()).await {
        Ok(_) => println!("Node 4 joined successfully"),
        Err(e) => panic!("Node 4 failed to join: {:?}", e),
    }
}
