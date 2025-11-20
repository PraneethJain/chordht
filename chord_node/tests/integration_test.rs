use chord_node::Node;
use chord_proto::chord::chord_server::ChordServer;
use chord_proto::chord::{GetRequest, PutRequest};
use chord_proto::hash_addr;
use std::net::SocketAddr;

use std::time::Duration;
use tokio::net::TcpListener;
use tonic::transport::Server;
use tonic::Request;

// Helper to start a node in a background task
async fn start_node(id: u64, addr: String) -> Node {
    let node = Node::new(id, addr.clone());
    let node_clone = node.clone();
    let addr_clone = addr.clone();

    tokio::spawn(async move {
        let addr: SocketAddr = addr_clone.parse().unwrap();
        let listener = TcpListener::bind(addr).await.unwrap();
        println!("Node {} listening on {}", id, addr);

        Server::builder()
            .add_service(ChordServer::new(node_clone))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    // Give it a moment to start
    tokio::time::sleep(Duration::from_millis(100)).await;
    node
}

#[tokio::test]
async fn test_chord_ring_formation_and_routing() {
    // 1. Start 3 nodes
    // Node IDs will be manually assigned for predictability or we can use hash_addr
    // Let's use ports 50001, 50002, 50003
    let addr1 = "127.0.0.1:50001".to_string();
    let addr2 = "127.0.0.1:50002".to_string();
    let addr3 = "127.0.0.1:50003".to_string();

    let id1 = hash_addr(&addr1);
    let id2 = hash_addr(&addr2);
    let id3 = hash_addr(&addr3);

    println!("Node 1: {} ({})", id1, addr1);
    println!("Node 2: {} ({})", id2, addr2);
    println!("Node 3: {} ({})", id3, addr3);

    let node1 = start_node(id1, addr1.clone()).await;

    // 2. Node 2 joins Node 1
    let node2 = start_node(id2, addr2.clone()).await;
    node2
        .join(addr1.clone())
        .await
        .expect("Node 2 failed to join Node 1");

    // 3. Node 3 joins Node 1
    let node3 = start_node(id3, addr3.clone()).await;
    node3
        .join(addr1.clone())
        .await
        .expect("Node 3 failed to join Node 1");

    // 4. Wait for stabilization
    // We need to call stabilize and fix_fingers periodically
    // In a real run, main loop does this. Here we simulate it.
    let nodes = vec![node1.clone(), node2.clone(), node3.clone()];

    println!("Stabilizing...");
    for _ in 0..10 {
        for node in &nodes {
            node.stabilize().await;
            node.fix_fingers().await;
            node.check_predecessor().await;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // 5. Verify Ring Topology (Successors)
    // We can check node state directly since we have Arc<Node>
    // Expected order depends on IDs.
    // Let's print successors
    for node in &nodes {
        let state = node.state.read().await;
        let succ = state.successor_list[0].clone();
        println!("Node {} successor is {}", node.id, succ.id);
    }

    // 6. Test Put/Get
    let key = "test_key";
    let value = "test_value";
    let key_id = hash_addr(key);
    println!("Key '{}' has ID {}", key, key_id);

    // Put on Node 1
    println!("Putting key on Node 1...");
    let put_req = Request::new(PutRequest {
        key: key.to_string(),
        value: value.to_string(),
    });
    // We need to call the trait method or the struct method.
    // Node implements Chord trait, but we can also call helper methods if they were public.
    // `put` is in the trait implementation.
    // We can use a client to send RPC to Node 1 to simulate real traffic, or call method directly.
    // Calling method directly on `Node` struct requires it to be exposed or use trait.
    // `Node` implements `Chord`.
    use chord_proto::chord::chord_server::Chord;
    node1.put(put_req).await.expect("Put failed");

    // Wait a bit for propagation if needed (though put is direct routing)

    // Get from Node 3
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
