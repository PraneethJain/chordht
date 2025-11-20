use chord_node::Node;
use chord_proto::chord::chord_client::ChordClient;
use chord_proto::chord::{GetRequest, PutRequest};
use chord_proto::hash_addr;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tonic::transport::Server;
use tonic::Request;

async fn start_node(port: u16, join_addr: Option<String>) -> Arc<Node> {
    let addr_str = format!("127.0.0.1:{}", port);
    let id = hash_addr(&addr_str);
    let node = Arc::new(Node::new(id, addr_str.clone()));

    if let Some(join) = join_addr {
        node.join(join).await.expect("Failed to join");
    }

    let node_clone = node.clone();
    tokio::spawn(async move {
        loop {
            sleep(Duration::from_millis(100)).await;
            node_clone.stabilize().await;
            node_clone.fix_fingers().await;
            node_clone.check_predecessor().await;
        }
    });

    let node_server = node.clone();
    let addr = addr_str.parse().unwrap();
    tokio::spawn(async move {
        Server::builder()
            .add_service(chord_proto::chord::chord_server::ChordServer::new(
                (*node_server).clone(),
            ))
            .serve(addr)
            .await
            .unwrap();
    });

    sleep(Duration::from_millis(500)).await; // Wait for server to start
    node
}

#[tokio::test]
async fn test_key_transfer_on_join_and_leave() {
    // 1. Start Node A
    let port_a = 15000;
    let node_a = start_node(port_a, None).await;
    let addr_a = format!("127.0.0.1:{}", port_a);
    println!("Node A started at {} with ID {}", addr_a, node_a.id);

    // 2. Put key "test_key"
    // Calculate hash to know where it goes.
    let key = "test_key";
    let key_id = hash_addr(key);
    println!("Key '{}' has ID {}", key, key_id);

    // Initially, Node A should have it (it's the only node)
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

    // Verify A has it
    let resp = client_a
        .get(Request::new(GetRequest {
            key: key.to_string(),
        }))
        .await
        .unwrap();
    assert_eq!(resp.into_inner().value, "value1");

    // Check internal store of A
    {
        let state = node_a.state.read().await;
        assert!(state.store.contains_key(key));
    }

    // 3. Start Node B such that it takes responsibility for the key.
    // Key ID is likely large or small.
    // If Key ID < Node A ID, it belongs to A.
    // If Key ID > Node A ID, it belongs to A (wrap around).
    // We want Node B such that Key ID <= Node B ID.
    // And Node B should be between A's predecessor and A? No.
    // If we want B to take the key from A, B must become the new successor for the key.
    // If key is currently at A, it means A is the successor of key.
    // This means key is in (A.pred, A].
    // If we insert B in (A.pred, A] such that key is in (A.pred, B], then B takes the key.
    // So we need A.pred < key <= B < A.
    // If A is alone, A.pred = A. Range is (A, A] (full circle).
    // So we just need key <= B < A (or wrapping cases).
    // Let's pick a port for B and check its ID.
    // We can try a few ports until we find a suitable ID.

    let mut port_b = 15001;
    let mut addr_b = format!("127.0.0.1:{}", port_b);
    let mut id_b = hash_addr(&addr_b);

    // We want B such that it takes the key from A.
    // If A is the only node, it holds everything.
    // If B joins, the ring is split.
    // A holds (B, A]. B holds (A, B].
    // We want key to land in (A, B]. So A < key <= B (or wrapping).
    // Or if key was in (B, A], it stays at A.
    // We want key to move to B. So key should be in (A, B].
    // So we need B such that B is the successor of key.
    // i.e., Key <= B.
    // And B should be the *first* node after Key.
    // Since A is already there, if Key <= A, then for B to take it, we need Key <= B < A.
    // If Key > A, then Key wraps around. Then we need Key <= B (and B > A? No, B < A is impossible if Key > A and B > Key).
    // If Key > A, then Key is in (A, A].
    // If we add B > Key, then Key is in (A, B]. B takes it.
    // So we need B such that B >= Key.
    // And B should be "closer" than A?
    // If Key > A, then B > Key is closer than A (since A is wrapped).
    // If Key < A, then we need B >= Key AND B < A.

    // Let's find a port that satisfies this.
    let mut found = false;
    for p in 15001..16000 {
        let a = format!("127.0.0.1:{}", p);
        let i = hash_addr(&a);

        // Check if B takes key from A.
        // A is at node_a.id.
        // Key is at key_id.
        // If we insert B at i.
        // New ring: A, B.
        // Ranges:
        // A covers (B, A].
        // B covers (A, B].
        // We want key in (A, B].
        // i.e. is_in_range_inclusive(key_id, node_a.id, i)

        if Node::is_in_range_inclusive(key_id, node_a.id, i) {
            port_b = p;
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
    let node_b = start_node(port_b, Some(addr_a.clone())).await;

    // Wait for stabilization and transfer
    sleep(Duration::from_secs(2)).await;

    // Verify key is on B
    {
        let state = node_b.state.read().await;
        assert!(state.store.contains_key(key), "Node B should have the key");
    }

    // Verify key is NOT on A
    {
        let state = node_a.state.read().await;
        assert!(
            !state.store.contains_key(key),
            "Node A should NOT have the key"
        );
    }

    // 4. Node B leaves
    println!("Node B leaving...");
    node_b.leave_network().await;

    // Wait for transfer
    sleep(Duration::from_secs(1)).await;

    // Verify key is back on A
    {
        let state = node_a.state.read().await;
        assert!(
            state.store.contains_key(key),
            "Node A should have the key back"
        );
    }
}
