use chord_proto::chord::chord_client::ChordClient;
use chord_proto::chord::chord_server::Chord;
use chord_proto::chord::{GetRequest, PutRequest};
use chord_proto::hash_addr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tonic::Request;

mod common;
use common::{stabilize_ring, start_node};

#[tokio::test]
async fn test_churn_and_concurrency() {
    let mut nodes = Vec::new();
    let mut handles = Vec::new();
    let mut addresses: Vec<String> = Vec::new();

    for i in 0..3 {
        let addr = format!("{}:{}", chord_node::constants::LOCALHOST, 54000 + i);
        let id = hash_addr(&addr);
        println!("Starting Node {} ({})", i, addr);
        let (node, handle) = start_node(id, addr.clone()).await;

        if i > 0 {
            node.join(addresses[0].clone())
                .await
                .expect("Failed to join");
        }

        nodes.push(node);
        handles.push(handle);
        addresses.push(addr);
    }

    println!("Initial stabilization...");
    tokio::time::sleep(Duration::from_secs(2)).await;
    stabilize_ring(&nodes, 1).await;

    let running = Arc::new(AtomicBool::new(true));
    let running_clone = running.clone();
    let addresses_lock = Arc::new(RwLock::new(addresses.clone()));
    let addresses_clone = addresses_lock.clone();

    let traffic_handle = tokio::spawn(async move {
        let mut success_count = 0;
        let mut failure_count = 0;
        let mut i = 0;

        while running_clone.load(Ordering::SeqCst) {
            i += 1;
            let key = format!("key_{}", i);
            let value = format!("value_{}", i);

            // Pick a random node to connect to
            let addr = {
                let addrs = addresses_clone.read().await;
                if addrs.is_empty() {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
                addrs[i % addrs.len()].clone()
            };

            // Connect and Put
            let client_res = ChordClient::connect(format!("http://{}", addr)).await;
            if let Ok(mut client) = client_res {
                let put_res = client
                    .put(Request::new(PutRequest {
                        key: key.clone(),
                        value: value.clone(),
                    }))
                    .await;

                if put_res.is_ok() {
                    // Try to Get it back immediately (might fail if not propagated or during churn)
                    let get_res = client
                        .get(Request::new(GetRequest { key: key.clone() }))
                        .await;
                    if let Ok(resp) = get_res {
                        if resp.into_inner().value == value {
                            success_count += 1;
                        } else {
                            failure_count += 1;
                        }
                    } else {
                        failure_count += 1;
                    }
                } else {
                    failure_count += 1;
                }
            } else {
                failure_count += 1;
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        println!(
            "Traffic finished. Success: {}, Failure: {}",
            success_count, failure_count
        );
        (success_count, failure_count)
    });

    println!("Adding 2 new nodes...");
    for i in 3..5 {
        let addr = format!("{}:{}", chord_node::constants::LOCALHOST, 54000 + i);
        let id = hash_addr(&addr);
        println!("Starting Node {} ({})", i, addr);
        let (node, handle) = start_node(id, addr.clone()).await;

        node.join(addresses[0].clone())
            .await
            .expect("Failed to join");

        nodes.push(node);
        handles.push(handle);

        addresses.push(addr);

        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    println!("Killing Node 1...");
    let killed_index = 1;
    handles[killed_index].abort();

    {
        let mut addrs = addresses_lock.write().await;
        let killed_addr = &addresses[killed_index];
        addrs.retain(|a| a != killed_addr);
    }

    println!("Stabilizing after kill...");
    // Stabilize all remaining nodes
    let mut alive_nodes = Vec::new();
    for (i, node) in nodes.iter().enumerate() {
        if i != killed_index {
            alive_nodes.push(node.clone());
        }
    }
    stabilize_ring(&alive_nodes, 10).await;

    running.store(false, Ordering::SeqCst);
    let (success, failure) = traffic_handle.await.unwrap();

    println!("Final Stats - Success: {}, Failure: {}", success, failure);

    assert!(success > 0, "Should have some successful operations");

    println!("Verifying consistency with new key...");
    let key = "final_consistency_check";
    let value = "persistent_value";

    let node0 = &nodes[0];
    node0
        .put(Request::new(PutRequest {
            key: key.to_string(),
            value: value.to_string(),
        }))
        .await
        .expect("Final put failed");

    tokio::time::sleep(Duration::from_millis(200)).await;

    let node4 = &nodes[4];
    let resp = node4
        .get(Request::new(GetRequest {
            key: key.to_string(),
        }))
        .await
        .expect("Final get failed");

    assert_eq!(resp.into_inner().value, value, "Value mismatch after churn");
    println!("Test passed!");
}
