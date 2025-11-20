use chord_proto::chord::{
    chord_server::Chord, Empty, FindSuccessorRequest, GetRequest, GetResponse, NodeInfo,
    NodeState as ProtoNodeState, PutRequest, PutResponse, SuccessorList, TransferKeysRequest,
};
use chord_proto::hash_addr;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};

const FINGER_TABLE_SIZE: usize = 64;

#[derive(Debug, Clone)]
pub struct Node {
    pub id: u64,
    pub addr: String,
    pub state: Arc<RwLock<NodeState>>,
}

#[derive(Debug)]
pub struct NodeState {
    pub predecessor: Option<NodeInfo>,
    pub finger_table: Vec<NodeInfo>,
    pub successor_list: Vec<NodeInfo>,
    pub store: HashMap<String, String>,
}

impl Node {
    pub fn new(id: u64, addr: String) -> Self {
        let mut finger_table = Vec::with_capacity(FINGER_TABLE_SIZE);
        // Initially finger table points to self
        let self_info = NodeInfo {
            id,
            address: addr.clone(),
        };
        for _ in 0..FINGER_TABLE_SIZE {
            finger_table.push(self_info.clone());
        }

        Node {
            id,
            addr,
            state: Arc::new(RwLock::new(NodeState {
                predecessor: None,
                finger_table,
                successor_list: vec![self_info], // Successor list initially contains self
                store: HashMap::new(),
            })),
        }
    }

    fn is_in_range(id: u64, start: u64, end: u64) -> bool {
        if start < end {
            id > start && id < end
        } else {
            id > start || id < end
        }
    }

    pub fn is_in_range_inclusive(id: u64, start: u64, end: u64) -> bool {
        if start < end {
            id > start && id <= end
        } else {
            id > start || id <= end
        }
    }

    pub async fn find_successor_internal(&self, id: u64) -> Result<NodeInfo, Status> {
        let state = self.state.read().await;
        let successor = state
            .successor_list
            .first()
            .cloned()
            .expect("Successor list should never be empty");

        if Self::is_in_range_inclusive(id, self.id, successor.id) {
            return Ok(successor);
        }
        drop(state); // Release lock

        // Get all unique candidates from finger table that are strictly closer to id
        // We want to try the closest ones first.
        let candidates = self.get_closest_candidates(id).await;

        if candidates.is_empty() {
            // If no candidates, fall back to successor
            let state = self.state.read().await;
            return Ok(state.successor_list[0].clone());
        }

        for candidate in candidates {
            if candidate.id == self.id {
                continue;
            }

            let client_addr = format!("http://{}", candidate.address);
            match self.find_successor_rpc(client_addr, id).await {
                Ok(info) => return Ok(info),
                Err(e) => {
                    println!(
                        "Node {}: Failed to contact candidate {} ({}) for id {}: {}",
                        self.id, candidate.id, candidate.address, id, e
                    );
                    // Continue to next candidate
                }
            }
        }

        // If all fingers failed, try successor list as fallback
        // This helps if the best finger (likely immediate successor) is dead.
        // We try to find *any* live node in our successor list to forward the query to.
        // Even if they are not strictly "closest preceding", they are better than failing.
        // And in a small ring, they are likely the next best hop.
        let state = self.state.read().await;
        let successors = state.successor_list.clone();
        drop(state);

        for succ in successors {
            // Skip if we already tried it (it was in candidates)
            if succ.id == self.id {
                continue;
            }

            let client_addr = format!("http://{}", succ.address);
            println!(
                "Node {}: Fallback: trying successor {} for id {}",
                self.id, succ.id, id
            );
            match self.find_successor_rpc(client_addr, id).await {
                Ok(info) => return Ok(info),
                Err(e) => {
                    println!(
                        "Node {}: Fallback successor {} failed: {}",
                        self.id, succ.id, e
                    );
                }
            }
        }

        Err(Status::unavailable("All candidates and successors failed"))
    }

    async fn get_closest_candidates(&self, id: u64) -> Vec<NodeInfo> {
        let state = self.state.read().await;
        let mut candidates = Vec::new();

        // Collect valid fingers
        for i in (0..FINGER_TABLE_SIZE).rev() {
            let finger = &state.finger_table[i];
            if finger.address.is_empty() {
                continue;
            }
            if Self::is_in_range(finger.id, self.id, id) {
                candidates.push(finger.clone());
            }
        }

        // Sort by ID to approximate closeness
        candidates.sort_by(|a, b| b.id.cmp(&a.id));
        candidates.dedup_by(|a, b| a.id == b.id);

        candidates
    }

    pub async fn join(&self, join_addr: String) -> Result<(), Box<dyn std::error::Error>> {
        let join_addr = format!("http://{}", join_addr);
        let info = self.find_successor_rpc(join_addr, self.id).await?;

        let mut state = self.state.write().await;
        state.successor_list[0] = info;
        Ok(())
    }

    pub async fn stabilize(&self) {
        let successor = {
            let state = self.state.read().await;
            state
                .successor_list
                .first()
                .cloned()
                .expect("Successor list should never be empty")
        };

        let successor_addr = format!("http://{}", successor.address);
        let x_result = self.get_predecessor_rpc(successor_addr.clone()).await;

        match x_result {
            Ok(x) => {
                let should_update = if x.id != 0 || !x.address.is_empty() {
                    Self::is_in_range(x.id, self.id, successor.id)
                } else {
                    false
                };

                if should_update {
                    let mut state = self.state.write().await;
                    state.successor_list[0] = x;
                }
            }
            Err(e) => {
                // NotFound means the successor doesn't have a predecessor yet - this is OK
                // Only treat Unavailable/transport errors as dead nodes
                if e.code() == tonic::Code::NotFound {
                    // Successor is alive but has no predecessor yet, continue normally
                } else {
                    println!("Node {}: Successor {} failed: {}", self.id, successor.id, e);
                    // Successor failed. If we have more successors in the list, promote the next one.
                    let mut state = self.state.write().await;
                    if state.successor_list.len() > 1 {
                        println!(
                            "Node {}: Removing dead successor {}, promoting next",
                            self.id, successor.id
                        );
                        state.successor_list.remove(0);
                        return;
                    }
                }
            }
        }

        let successor = {
            let state = self.state.read().await;
            state
                .successor_list
                .first()
                .cloned()
                .expect("Successor list should never be empty")
        };

        let successor_addr = format!("http://{}", successor.address);
        let me = NodeInfo {
            id: self.id,
            address: self.addr.clone(),
        };

        if let Err(e) = self.notify_rpc(successor_addr.clone(), me).await {
            println!(
                "Node {}: Failed to notify successor {}: {}",
                self.id, successor.id, e
            );
        }

        let _ = self.update_successor_list(successor_addr).await;
    }

    pub async fn fix_fingers(&self) {
        let i = {
            use rand::Rng;
            let mut rng = rand::thread_rng();
            rng.gen_range(0..FINGER_TABLE_SIZE)
        };

        // For u64 space, finger[i] should point to successor of (n + 2^i) mod 2^64
        // wrapping_add handles the modulo automatically
        let target = self.id.wrapping_add(1u64 << i);

        if let Ok(successor) = self.find_successor_internal(target).await {
            let mut state = self.state.write().await;
            state.finger_table[i] = successor;
        }
    }

    pub async fn check_predecessor(&self) {
        let predecessor = {
            let state = self.state.read().await;
            state.predecessor.clone()
        };

        if let Some(pred) = predecessor {
            let pred_addr = format!("http://{}", pred.address);
            // Try to ping
            match self.ping_rpc(pred_addr).await {
                Ok(_) => {}
                Err(_) => {
                    // Predecessor failed
                    let mut state = self.state.write().await;
                    state.predecessor = None;
                }
            }
        }
    }

    async fn update_successor_list(&self, successor_addr: String) -> Result<(), Status> {
        match self.get_successor_list_rpc(successor_addr).await {
            Ok(list) => {
                let mut state = self.state.write().await;
                // New successor list = successor + successor.successors (trimmed)
                let mut new_list = vec![state.successor_list[0].clone()];
                new_list.extend(list.successors);
                if new_list.len() > 5 {
                    // Keep k successors
                    new_list.truncate(5);
                }
                state.successor_list = new_list;
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    // RPC Helpers
    async fn find_successor_rpc(&self, addr: String, id: u64) -> Result<NodeInfo, Status> {
        let mut client = self.connect_rpc(addr).await?;
        let request = Request::new(FindSuccessorRequest { id });
        let response = client.find_successor(request).await?;
        Ok(response.into_inner())
    }

    async fn get_predecessor_rpc(&self, addr: String) -> Result<NodeInfo, Status> {
        let mut client = self.connect_rpc(addr).await?;
        let request = Request::new(Empty {});
        let response = client.get_predecessor(request).await?;
        Ok(response.into_inner())
    }

    async fn notify_rpc(&self, addr: String, node: NodeInfo) -> Result<(), Status> {
        let mut client = self.connect_rpc(addr).await?;
        let request = Request::new(node);
        client.notify(request).await?;
        Ok(())
    }

    async fn get_successor_list_rpc(&self, addr: String) -> Result<SuccessorList, Status> {
        let mut client = self.connect_rpc(addr).await?;
        let request = Request::new(Empty {});
        let response = client.get_successor_list(request).await?;
        Ok(response.into_inner())
    }

    async fn ping_rpc(&self, addr: String) -> Result<(), Status> {
        let mut client = self.connect_rpc(addr).await?;
        let request = Request::new(Empty {});
        client.ping(request).await?;
        Ok(())
    }

    pub async fn report_to_monitor(&self, monitor_addr: String) {
        use chord_proto::chord::chord_monitor_client::ChordMonitorClient;
        let state = self.state.read().await;

        let node_state = ProtoNodeState {
            id: self.id,
            address: self.addr.clone(),
            predecessor: state.predecessor.clone(),
            successors: state.successor_list.clone(),
            finger_table: state.finger_table.clone(),
            stored_keys: state.store.keys().cloned().collect(),
        };

        // Fire and forget
        let monitor_addr = format!("http://{}", monitor_addr);
        if let Ok(mut client) = ChordMonitorClient::connect(monitor_addr).await {
            let _ = client.report_state(Request::new(node_state)).await;
        }
    }
    pub async fn leave_network(&self) {
        let state = self.state.read().await;
        let successor = state.successor_list.first().cloned();
        let store = state.store.clone();
        drop(state);

        if let Some(successor) = successor {
            if successor.id != self.id {
                println!(
                    "Node {}: Transferring {} keys to successor {} before leaving",
                    self.id,
                    store.len(),
                    successor.id
                );
                let successor_addr = format!("http://{}", successor.address);
                if let Err(e) = self.transfer_keys_rpc(successor_addr, store).await {
                    println!("Node {}: Failed to transfer keys on leave: {}", self.id, e);
                }
            }
        }
    }

    async fn transfer_keys_rpc(
        &self,
        addr: String,
        keys: HashMap<String, String>,
    ) -> Result<(), Status> {
        use chord_proto::chord::TransferKeysRequest;
        let mut client = self.connect_rpc(addr).await?;
        let request = Request::new(TransferKeysRequest { keys });
        client.transfer_keys(request).await?;
        Ok(())
    }

    // Helper for RPC connection
    async fn connect_rpc(
        &self,
        addr: String,
    ) -> Result<chord_proto::chord::chord_client::ChordClient<tonic::transport::Channel>, Status>
    {
        use chord_proto::chord::chord_client::ChordClient;
        ChordClient::connect(addr)
            .await
            .map_err(|e| Status::unavailable(e.to_string()))
    }
}

#[tonic::async_trait]
impl Chord for Node {
    async fn get_successor(&self, _request: Request<Empty>) -> Result<Response<NodeInfo>, Status> {
        let state = self.state.read().await;
        if let Some(successor) = state.successor_list.first() {
            Ok(Response::new(successor.clone()))
        } else {
            Err(Status::internal("No successor found"))
        }
    }

    async fn get_predecessor(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<NodeInfo>, Status> {
        let state = self.state.read().await;
        if let Some(predecessor) = &state.predecessor {
            Ok(Response::new(predecessor.clone()))
        } else {
            Err(Status::not_found("No predecessor"))
        }
    }

    async fn find_successor(
        &self,
        request: Request<FindSuccessorRequest>,
    ) -> Result<Response<NodeInfo>, Status> {
        let req = request.into_inner();
        let successor = self.find_successor_internal(req.id).await?;
        Ok(Response::new(successor))
    }

    async fn notify(&self, request: Request<NodeInfo>) -> Result<Response<Empty>, Status> {
        let potential_predecessor = request.into_inner();
        let mut state = self.state.write().await;

        let should_update = if let Some(current_predecessor) = &state.predecessor {
            Self::is_in_range(potential_predecessor.id, current_predecessor.id, self.id)
        } else {
            true
        };

        if should_update {
            let _old_predecessor = state.predecessor.clone();
            state.predecessor = Some(potential_predecessor.clone());

            // Identify keys to transfer
            // Keys that belong to new predecessor are those <= potential_predecessor.id
            // But we only hold keys that are <= self.id.
            // And previously we held keys > old_predecessor.id (or all if None).
            // So we transfer keys k where k <= potential_predecessor.id.
            // We need to be careful about wrapping.
            // The range we are giving up is (old_predecessor, potential_predecessor].
            // If old_predecessor is None, we give up (-inf, potential_predecessor].

            let mut keys_to_transfer = HashMap::new();
            let mut keys_to_remove = Vec::new();

            for (k, v) in &state.store {
                let key_id = hash_addr(k);
                // Check if key_id is in (old_pred, new_pred]
                // If key_id is NOT in (new_pred, self], then it belongs to new_pred (or someone else behind).

                if !Self::is_in_range_inclusive(key_id, potential_predecessor.id, self.id) {
                    keys_to_transfer.insert(k.clone(), v.clone());
                    keys_to_remove.push(k.clone());
                }
            }

            if !keys_to_transfer.is_empty() {
                println!(
                    "Node {}: Transferring {} keys to new predecessor {}",
                    self.id,
                    keys_to_transfer.len(),
                    potential_predecessor.id
                );

                // We can't do async RPC while holding the write lock easily if we want to be safe/clean.
                // For this implementation, let's remove now and spawn a task to send.

                for k in keys_to_remove {
                    state.store.remove(&k);
                }

                let target_addr = format!("http://{}", potential_predecessor.address);

                tokio::spawn(async move {
                    use chord_proto::chord::chord_client::ChordClient;
                    use chord_proto::chord::TransferKeysRequest;

                    let mut client = match ChordClient::connect(target_addr).await {
                        Ok(c) => c,
                        Err(e) => {
                            println!(
                                "Failed to connect to new predecessor for key transfer: {}",
                                e
                            );
                            return;
                        }
                    };

                    let request = Request::new(TransferKeysRequest {
                        keys: keys_to_transfer,
                    });
                    if let Err(e) = client.transfer_keys(request).await {
                        println!("Failed to transfer keys: {}", e);
                    }
                });
            }
        }

        Ok(Response::new(Empty {}))
    }

    async fn get_successor_list(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<SuccessorList>, Status> {
        let state = self.state.read().await;
        Ok(Response::new(SuccessorList {
            successors: state.successor_list.clone(),
        }))
    }

    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        let req = request.into_inner();
        let key_id = hash_addr(&req.key);
        println!(
            "Node {}: Received Put request for key '{}' (ID: {})",
            self.id, req.key, key_id
        );

        let successor = self.find_successor_internal(key_id).await?;
        println!(
            "Node {}: Successor for key '{}' is {}",
            self.id, req.key, successor.id
        );

        if successor.id == self.id {
            println!("Node {}: Storing key '{}' locally", self.id, req.key);
            let mut state = self.state.write().await;
            state.store.insert(req.key, req.value);
            Ok(Response::new(PutResponse { success: true }))
        } else {
            println!(
                "Node {}: Forwarding Put for key '{}' to {}",
                self.id, req.key, successor.id
            );
            let endpoint = format!("http://{}", successor.address);
            let mut client = self.connect_rpc(endpoint).await?;
            let response = client.put(Request::new(req)).await?;
            Ok(Response::new(response.into_inner()))
        }
    }

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let req = request.into_inner();
        let key_id = hash_addr(&req.key);
        println!(
            "Node {}: Received Get request for key '{}' (ID: {})",
            self.id, req.key, key_id
        );

        let successor = self.find_successor_internal(key_id).await?;
        println!(
            "Node {}: Successor for key '{}' is {}",
            self.id, req.key, successor.id
        );

        if successor.id == self.id {
            println!("Node {}: Looking up key '{}' locally", self.id, req.key);
            let state = self.state.read().await;
            if let Some(value) = state.store.get(&req.key) {
                println!("Node {}: Found key '{}'", self.id, req.key);
                Ok(Response::new(GetResponse {
                    value: value.clone(),
                    found: true,
                }))
            } else {
                println!("Node {}: Key '{}' not found", self.id, req.key);
                Ok(Response::new(GetResponse {
                    value: "".to_string(),
                    found: false,
                }))
            }
        } else {
            println!(
                "Node {}: Forwarding Get for key '{}' to {}",
                self.id, req.key, successor.id
            );
            let endpoint = format!("http://{}", successor.address);
            let mut client = self.connect_rpc(endpoint).await?;
            let response = client.get(Request::new(req)).await?;
            Ok(Response::new(response.into_inner()))
        }
    }

    async fn ping(&self, _request: Request<Empty>) -> Result<Response<Empty>, Status> {
        Ok(Response::new(Empty {}))
    }

    async fn transfer_keys(
        &self,
        request: Request<TransferKeysRequest>,
    ) -> Result<Response<Empty>, Status> {
        let req = request.into_inner();
        println!("Node {}: Received {} keys", self.id, req.keys.len());
        let mut state = self.state.write().await;
        for (k, v) in req.keys {
            state.store.insert(k, v);
        }
        Ok(Response::new(Empty {}))
    }

    async fn leave(&self, _request: Request<Empty>) -> Result<Response<Empty>, Status> {
        println!("Node {}: Received Leave request", self.id);
        self.leave_network().await;

        // Spawn a task to exit the process after a short delay to allow the response to be sent
        tokio::spawn(async {
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            std::process::exit(0);
        });

        Ok(Response::new(Empty {}))
    }
}
