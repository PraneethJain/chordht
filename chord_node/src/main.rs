use chord_proto::chord::chord_server::ChordServer;
use clap::Parser;

use std::net::SocketAddr;
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use tonic::transport::Server;

use chord_node::constants::{
    CHECK_PREDECESSOR_INTERVAL_MS, DEFAULT_PORT, FIX_FINGERS_INTERVAL_MS, LOCALHOST,
    MAINTAIN_REPLICATION_INTERVAL_MS, STABILIZATION_INTERVAL_MS,
};
use chord_node::Node;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Port to listen on
    #[arg(short, long, default_value_t = DEFAULT_PORT)]
    port: u16,

    /// Address of a node to join
    #[arg(short, long)]
    join: Option<String>,

    /// Monitor address
    #[arg(short, long)]
    monitor: Option<String>,
}

use chord_proto::hash_addr;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let args = Args::parse();

    let addr_str = format!("{}:{}", LOCALHOST, args.port);
    let addr: SocketAddr = addr_str.parse()?;
    let id = hash_addr(&addr_str);

    println!("Node starting at {} with ID {}", addr_str, id);

    let node = Node::new(id, addr_str.clone());
    let node = Arc::new(node);

    // Join if requested
    if let Some(join_addr) = args.join {
        println!("Joining ring via {}", join_addr);
        node.join(join_addr).await?;
        println!("Joined successfully");
    }

    // Background tasks
    let node_clone = node.clone();
    let monitor_addr = args.monitor.clone();
    tokio::spawn(async move {
        loop {
            sleep(Duration::from_millis(STABILIZATION_INTERVAL_MS)).await;
            node_clone.stabilize().await;
            sleep(Duration::from_millis(FIX_FINGERS_INTERVAL_MS)).await;
            node_clone.fix_fingers().await;
            sleep(Duration::from_millis(CHECK_PREDECESSOR_INTERVAL_MS)).await;
            node_clone.check_predecessor().await;
            sleep(Duration::from_millis(MAINTAIN_REPLICATION_INTERVAL_MS)).await;
            node_clone.maintain_replication().await;

            if let Some(ref m_addr) = monitor_addr {
                node_clone.report_to_monitor(m_addr.clone()).await;
            }
        }
    });

    println!("Server listening on {}", addr);

    Server::builder()
        .add_service(ChordServer::new((*node).clone()))
        .serve(addr)
        .await?;

    Ok(())
}
