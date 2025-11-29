use chord_node::Node;
use chord_proto::chord::chord_server::ChordServer;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tonic::transport::Server;

/// Helper to start a node in a background task.
/// Returns the Node Arc and a JoinHandle to the server task (allowing it to be aborted).
pub async fn start_node(addr: String) -> (Arc<Node>, tokio::task::JoinHandle<()>) {
    let addr: SocketAddr = addr.parse().unwrap();
    let listener = TcpListener::bind(addr).await.unwrap();
    let local_addr = listener.local_addr().unwrap();
    let local_addr_str = local_addr.to_string();

    // Calculate ID based on the actual bound address
    let id = chord_proto::hash_addr(&local_addr_str);

    let node = Node::new(id, local_addr_str.clone());
    let node = Arc::new(node);
    let node_clone = node.clone();

    let handle = tokio::spawn(async move {
        Server::builder()
            .add_service(ChordServer::new((*node_clone).clone()))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    // Give it a moment to start
    tokio::time::sleep(Duration::from_millis(200)).await;
    (node, handle)
}

pub async fn stabilize_ring(nodes: &[Arc<Node>], rounds: usize) {
    println!("Stabilizing ring for {} rounds...", rounds);
    for _ in 0..rounds {
        for node in nodes {
            node.stabilize().await;
            node.fix_fingers().await;
            node.check_predecessor().await;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
