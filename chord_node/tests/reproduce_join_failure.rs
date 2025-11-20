use chord_node::Node;
use chord_proto::chord::chord_server::ChordServer;
use chord_proto::hash_addr;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tonic::transport::Server;

// Helper to start a node in a background task
async fn start_node(id: u64, addr: String) -> (Arc<Node>, tokio::task::JoinHandle<()>) {
    let node = Node::new(id, addr.clone());
    let node = Arc::new(node);
    let node_clone = node.clone();
    let addr_clone = addr.clone();

    let handle = tokio::spawn(async move {
        let addr: SocketAddr = addr_clone.parse().unwrap();
        let listener = TcpListener::bind(addr).await.unwrap();
        println!("Node {} listening on {}", id, addr);

        Server::builder()
            .add_service(ChordServer::new((*node_clone).clone()))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    // Give it a moment to start
    tokio::time::sleep(Duration::from_millis(100)).await;
    (node, handle)
}

#[tokio::test]
async fn test_join_failure_after_node_departure() {
    // 1. Start 3 nodes
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

    // 4. Wait for stabilization
    let nodes = vec![node1.clone(), node2.clone(), node3.clone()];

    println!("Stabilizing...");
    for _ in 0..5 {
        for node in &nodes {
            node.stabilize().await;
            node.fix_fingers().await;
            node.check_predecessor().await;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // 5. Kill Node 2
    println!("Killing Node 2...");
    h2.abort();
    // Wait for port to be freed? Abort just kills the task.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Wait for stabilization after node death
    println!("Stabilizing after node death...");
    for _ in 0..5 {
        for node in &[node1.clone(), node3.clone()] {
            node.stabilize().await;
            node.fix_fingers().await;
            node.check_predecessor().await;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // 6. Try to add Node 4, joining via Node 1
    // Node 1: 7184679561330953239
    // Node 2: 201651167335605680
    // Node 3: 3529977946114422751
    // Ring: 2 -> 3 -> 1 -> 2
    // We killed Node 2.
    // We join via Node 1.
    // We want Node 1 to route to Node 2.
    // Node 1 successor is Node 2.
    // If target ID is in (Node 1, Node 2], Node 1 returns Node 2 (no RPC).
    // If target ID is NOT in (Node 1, Node 2], Node 1 checks fingers.
    // Closest finger to target will be Node 2 (since it's successor and covers a large range).
    // So we need target ID to be in (Node 2, Node 1].
    // E.g. 3000000000000000000.

    let addr4 = "127.0.0.1:51004".to_string();
    let id4: u64 = 3000000000000000000;
    println!("Node 4: {} ({})", id4, addr4);

    let (node4, _h4) = start_node(id4, addr4.clone()).await;

    println!("Node 4 joining via Node 1...");
    // This is expected to fail if the bug exists and Node 1 tries to route via Node 2
    match node4.join(addr1.clone()).await {
        Ok(_) => println!("Node 4 joined successfully (Bug not reproduced or lucky routing)"),
        Err(e) => {
            println!("Node 4 failed to join: {}", e);
            // If we get here, we reproduced the issue (or a similar one)
            // We want to assert that this failure happens to confirm the bug.
            // But for a test, we usually want it to PASS if the bug is fixed.
            // So this test will FAIL now, and PASS after fix.
            panic!("Reproduced bug: Node 4 failed to join due to transport error");
        }
    }
}
