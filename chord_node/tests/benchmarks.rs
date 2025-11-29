use chord_proto::chord::chord_server::Chord;
use chord_proto::chord::{GetRequest, PutRequest};
use chord_proto::hash_addr;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tonic::Request;

mod common;
use common::{stabilize_ring, start_node};

// Helper for range checks (local implementation since Node::is_in_range is private)
fn is_in_range(id: u64, start: u64, end: u64) -> bool {
    if start < end {
        id > start && id < end
    } else {
        id > start || id < end
    }
}

fn is_in_range_inclusive(id: u64, start: u64, end: u64) -> bool {
    if start < end {
        id > start && id <= end
    } else {
        id > start || id <= end
    }
}

// Helper to simulate lookup and count hops locally
async fn simulate_lookup_hops(
    start_node_id: u64,
    key_id: u64,
    nodes_map: &HashMap<u64, &Arc<chord_node::Node>>,
) -> usize {
    let mut current_node = nodes_map.get(&start_node_id).expect("Start node not found");
    let mut hops = 0;
    let mut visited = std::collections::HashSet::new();

    loop {
        if visited.contains(&current_node.id) {
            break;
        }
        visited.insert(current_node.id);

        let state = current_node.state.read().await;
        let successor = state.successor_list[0].clone();

        if is_in_range_inclusive(key_id, current_node.id, successor.id) {
            return hops + 1;
        }

        let mut next_node_info = successor.clone();
        // Find closest preceding finger
        for finger in state.finger_table.iter().rev() {
            if finger.address.is_empty() {
                continue;
            }
            if is_in_range(finger.id, current_node.id, key_id) {
                next_node_info = finger.clone();
                break;
            }
        }
        drop(state);

        if let Some(next_node) = nodes_map.get(&next_node_info.id) {
            current_node = next_node;
            hops += 1;
        } else {
            // If next node is not in our map (e.g. not fully stabilized or wrong address), stop
            break;
        }
    }
    hops
}

#[tokio::test]
async fn benchmark_scalability_hops() {
    println!("\n=== Benchmark 1: Scalability (Average Hops vs Network Size) ===");
    println!("Nodes,Avg_Hops");

    let sizes = [10, 20, 30, 40, 50];

    for &num_nodes in &sizes {
        let mut nodes = Vec::new();
        let mut addresses = Vec::new();

        for _ in 0..num_nodes {
            let (node, _handle) = start_node("127.0.0.1:0".to_string()).await;
            addresses.push(node.addr.clone());
            nodes.push(node);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        for node in nodes.iter().take(num_nodes).skip(1) {
            node.join(addresses[0].clone()).await.unwrap();
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        stabilize_ring(&nodes, num_nodes * 2).await;

        let mut nodes_map = HashMap::new();
        for node in &nodes {
            nodes_map.insert(node.id, node);
        }

        let num_lookups = 50;
        let mut total_hops = 0;
        use rand::Rng;
        let mut rng = rand::thread_rng();

        for _ in 0..num_lookups {
            let start_idx = rng.gen_range(0..num_nodes);
            let key_id: u64 = rng.gen();
            let hops = simulate_lookup_hops(nodes[start_idx].id, key_id, &nodes_map).await;
            total_hops += hops;
        }

        let avg_hops = total_hops as f64 / num_lookups as f64;
        println!("{},{:.2}", num_nodes, avg_hops);
    }
}

#[tokio::test]
async fn benchmark_load_balancing() {
    println!("\n=== Benchmark 2: Load Balancing (Key Distribution) ===");
    const NUM_NODES: usize = 20;
    const NUM_KEYS: usize = 1000;

    let mut nodes = Vec::new();
    let mut addresses = Vec::new();

    for _ in 0..NUM_NODES {
        let (node, _handle) = start_node("127.0.0.1:0".to_string()).await;
        addresses.push(node.addr.clone());
        nodes.push(node);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    for node in nodes.iter().take(NUM_NODES).skip(1) {
        node.join(addresses[0].clone()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    stabilize_ring(&nodes, NUM_NODES * 2).await;

    println!("Inserting {} keys...", NUM_KEYS);
    for i in 0..NUM_KEYS {
        let key = format!("key-{}", i);
        let req = Request::new(PutRequest {
            key: key.clone(),
            value: "val".to_string(),
        });
        nodes[i % NUM_NODES].put(req).await.expect("Put failed");
    }

    tokio::time::sleep(Duration::from_millis(500)).await;

    println!("Node_ID,Key_Count");
    for node in &nodes {
        let state = node.state.read().await;
        println!("{},{}", node.id, state.store.len());
    }
}

#[tokio::test]
async fn benchmark_concurrent_throughput() {
    println!("\n=== Benchmark 3: Concurrent Throughput ===");
    println!("Clients,Ops_Per_Sec");

    const NUM_NODES: usize = 10;
    let mut nodes = Vec::new();
    let mut addresses = Vec::new();

    for _ in 0..NUM_NODES {
        let (node, _handle) = start_node("127.0.0.1:0".to_string()).await;
        addresses.push(node.addr.clone());
        nodes.push(node);
    }
    for node in nodes.iter().take(NUM_NODES).skip(1) {
        node.join(addresses[0].clone()).await.unwrap();
    }
    stabilize_ring(&nodes, 20).await;

    let client_counts = [1, 5, 10, 15, 20, 25, 30, 35, 40];
    let ops_per_client = 100;

    for &num_clients in &client_counts {
        let mut handles = Vec::new();
        let start = Instant::now();

        for i in 0..num_clients {
            let node = nodes[i % NUM_NODES].clone();
            let handle = tokio::spawn(async move {
                for j in 0..ops_per_client {
                    let key = format!("client_{}_key_{}", i, j);
                    let _ = node
                        .put(Request::new(PutRequest {
                            key: key.clone(),
                            value: "val".to_string(),
                        }))
                        .await;
                    let _ = node.get(Request::new(GetRequest { key })).await;
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap();
        }

        let duration = start.elapsed();
        let total_ops = num_clients * ops_per_client * 2; // put + get
        let ops_per_sec = total_ops as f64 / duration.as_secs_f64();

        println!("{},{:.2}", num_clients, ops_per_sec);
    }
}

#[tokio::test]
async fn benchmark_replication_delay() {
    println!("\n=== Benchmark 4: Replication Delay ===");
    const NUM_NODES: usize = 5;

    let mut nodes = Vec::new();
    let mut addresses = Vec::new();

    for _ in 0..NUM_NODES {
        let (node, _handle) = start_node("127.0.0.1:0".to_string()).await;
        addresses.push(node.addr.clone());
        nodes.push(node);
    }
    for node in nodes.iter().take(NUM_NODES).skip(1) {
        node.join(addresses[0].clone()).await.unwrap();
    }
    stabilize_ring(&nodes, 20).await;

    println!("Trial,Delay_ms");
    let num_trials = 20;
    let mut total_delay = 0.0;

    for i in 0..num_trials {
        let key = format!("rep_key_{}", i);
        let key_id = hash_addr(&key);

        // Find primary
        let mut primary_idx = 0;
        for (idx, node) in nodes.iter().enumerate() {
            let state = node.state.read().await;
            let pred = state.predecessor.clone().map(|p| p.id).unwrap_or(node.id);
            if is_in_range_inclusive(key_id, pred, node.id) {
                primary_idx = idx;
                break;
            }
        }

        let primary = &nodes[primary_idx];
        let state = primary.state.read().await;
        let successor_info = state.successor_list[0].clone();
        drop(state);

        let successor = nodes
            .iter()
            .find(|n| n.id == successor_info.id)
            .expect("Successor not found");

        let start = Instant::now();
        let req = Request::new(PutRequest {
            key: key.clone(),
            value: "val".to_string(),
        });
        primary.put(req).await.expect("Put failed");

        // Poll successor
        loop {
            let state = successor.state.read().await;
            if state.store.contains_key(&key) {
                break;
            }
            drop(state);
            tokio::time::sleep(Duration::from_millis(1)).await;
            if start.elapsed().as_secs() > 5 {
                println!("Timeout waiting for replication");
                break;
            }
        }

        let duration = start.elapsed().as_millis();
        println!("{},{}", i, duration);
        total_delay += duration as f64;

        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    println!(
        "Average Replication Delay: {:.2} ms",
        total_delay / num_trials as f64
    );
}

#[tokio::test]
async fn benchmark_latency_cdf() {
    println!("\n=== Benchmark 5: Latency CDF ===");
    const NUM_NODES: usize = 10;

    let mut nodes = Vec::new();
    let mut addresses = Vec::new();

    for _ in 0..NUM_NODES {
        let (node, _handle) = start_node("127.0.0.1:0".to_string()).await;
        addresses.push(node.addr.clone());
        nodes.push(node);
    }
    for node in nodes.iter().take(NUM_NODES).skip(1) {
        node.join(addresses[0].clone()).await.unwrap();
    }
    stabilize_ring(&nodes, 20).await;

    // Populate some data
    for i in 0..50 {
        let key = format!("data_{}", i);
        nodes[0]
            .put(Request::new(PutRequest {
                key,
                value: "x".to_string(),
            }))
            .await
            .ok();
    }

    println!("Latency_us");
    let num_reqs = 500;
    use rand::Rng;
    let mut rng = rand::thread_rng();

    for _ in 0..num_reqs {
        let key = format!("data_{}", rng.gen_range(0..50));
        let node_idx = rng.gen_range(0..NUM_NODES);

        let start = Instant::now();
        let _ = nodes[node_idx].get(Request::new(GetRequest { key })).await;
        let duration = start.elapsed().as_micros();
        println!("{}", duration);
    }
}
