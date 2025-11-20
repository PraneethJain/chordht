use axum::{
    extract::State,
    routing::{get, post},
    Json, Router,
};
use chord_proto::chord::{
    chord_client::ChordClient,
    chord_monitor_server::{ChordMonitor, ChordMonitorServer},
    Empty, GetRequest, NodeState, PutRequest,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::process::Command;
use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;
use tonic::{transport::Server, Request, Response, Status};
use tower_http::cors::CorsLayer;

#[derive(Debug, Default)]
struct MonitorState {
    nodes: HashMap<u64, NodeState>,
    next_port: u16,
}

impl MonitorState {
    fn new() -> Self {
        Self {
            nodes: HashMap::new(),
            next_port: 5010, // Start allocating node ports from 5010 to avoid conflicts
        }
    }
}

type SharedState = Arc<Mutex<MonitorState>>;

struct MonitorService {
    state: SharedState,
}

#[tonic::async_trait]
impl ChordMonitor for MonitorService {
    async fn report_state(&self, request: Request<NodeState>) -> Result<Response<Empty>, Status> {
        let node_state = request.into_inner();
        println!("Received state from node {}", node_state.id);
        let mut state = self.state.lock().unwrap();
        state.nodes.insert(node_state.id, node_state);
        Ok(Response::new(Empty {}))
    }
}

#[derive(Deserialize)]
struct ApiPutRequest {
    key: String,
    value: String,
}

#[derive(Deserialize)]
struct ApiGetRequest {
    key: String,
}

#[derive(Serialize)]
struct ApiGetResponse {
    found: bool,
    value: String,
}

#[derive(Serialize)]
struct ApiStatusResponse {
    success: bool,
    message: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let state = Arc::new(Mutex::new(MonitorState::new()));

    let grpc_state = state.clone();
    tokio::spawn(async move {
        let addr = "0.0.0.0:50051".parse().unwrap();
        println!("Monitor gRPC listening on {}", addr);
        Server::builder()
            .add_service(ChordMonitorServer::new(MonitorService {
                state: grpc_state,
            }))
            .serve(addr)
            .await
            .unwrap();
    });

    let app = Router::new()
        .route("/api/state", get(get_state))
        .route("/api/put", post(handle_put))
        .route("/api/get", post(handle_get))
        .route("/api/add_node", post(handle_add_node))
        .route("/api/leave_node", post(handle_leave_node))
        .nest_service("/", tower_http::services::ServeDir::new("frontend/dist"))
        .layer(CorsLayer::permissive())
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    println!("Monitor Web listening on {}", addr);
    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

#[derive(Serialize, Clone)]
struct NodeInfoDto {
    id: String,
    address: String,
}

impl From<chord_proto::chord::NodeInfo> for NodeInfoDto {
    fn from(info: chord_proto::chord::NodeInfo) -> Self {
        Self {
            id: info.id.to_string(),
            address: info.address,
        }
    }
}

#[derive(Serialize, Clone)]
struct NodeStateDto {
    id: String,
    address: String,
    predecessor: Option<NodeInfoDto>,
    successors: Vec<NodeInfoDto>,
    finger_table: Vec<NodeInfoDto>,
    stored_keys: Vec<String>,
}

impl From<NodeState> for NodeStateDto {
    fn from(state: NodeState) -> Self {
        Self {
            id: state.id.to_string(),
            address: state.address,
            predecessor: state.predecessor.map(Into::into),
            successors: state.successors.into_iter().map(Into::into).collect(),
            finger_table: state.finger_table.into_iter().map(Into::into).collect(),
            stored_keys: state.stored_keys,
        }
    }
}

async fn get_state(State(state): State<SharedState>) -> Json<Vec<NodeStateDto>> {
    let state = state.lock().unwrap();
    let nodes: Vec<NodeStateDto> = state.nodes.values().cloned().map(Into::into).collect();
    Json(nodes)
}

async fn get_any_node_address(state: SharedState) -> Option<String> {
    let state = state.lock().unwrap();
    if state.nodes.is_empty() {
        return None;
    }
    // Pick a random node to demonstrate that any node can act as an entry point
    // and the protocol handles the routing.
    use rand::seq::IteratorRandom;
    let mut rng = rand::thread_rng();
    state
        .nodes
        .values()
        .choose(&mut rng)
        .map(|n| n.address.clone())
}

async fn connect_to_node(addr: String) -> Result<ChordClient<tonic::transport::Channel>, String> {
    let endpoint = format!("http://{}", addr);
    ChordClient::connect(endpoint)
        .await
        .map_err(|e| format!("Connection error: {}", e))
}

async fn handle_put(
    State(state): State<SharedState>,
    Json(payload): Json<ApiPutRequest>,
) -> Json<ApiStatusResponse> {
    let node_addr = match get_any_node_address(state).await {
        Some(addr) => addr,
        None => {
            return Json(ApiStatusResponse {
                success: false,
                message: "No nodes available".into(),
            })
        }
    };

    match connect_to_node(node_addr).await {
        Ok(mut client) => {
            let request = Request::new(PutRequest {
                key: payload.key,
                value: payload.value,
            });
            match client.put(request).await {
                Ok(response) => {
                    let resp = response.into_inner();
                    if resp.success {
                        Json(ApiStatusResponse {
                            success: true,
                            message: "Put successful".into(),
                        })
                    } else {
                        Json(ApiStatusResponse {
                            success: false,
                            message: "Put failed".into(),
                        })
                    }
                }
                Err(e) => Json(ApiStatusResponse {
                    success: false,
                    message: format!("RPC error: {}", e),
                }),
            }
        }
        Err(e) => Json(ApiStatusResponse {
            success: false,
            message: e,
        }),
    }
}

async fn handle_get(
    State(state): State<SharedState>,
    Json(payload): Json<ApiGetRequest>,
) -> Json<ApiGetResponse> {
    let node_addr = match get_any_node_address(state).await {
        Some(addr) => addr,
        None => {
            return Json(ApiGetResponse {
                found: false,
                value: "No nodes available".into(),
            })
        }
    };

    match connect_to_node(node_addr).await {
        Ok(mut client) => {
            let request = Request::new(GetRequest { key: payload.key });
            match client.get(request).await {
                Ok(response) => {
                    let resp = response.into_inner();
                    Json(ApiGetResponse {
                        found: resp.found,
                        value: resp.value,
                    })
                }
                Err(e) => Json(ApiGetResponse {
                    found: false,
                    value: format!("RPC error: {}", e),
                }),
            }
        }
        Err(e) => Json(ApiGetResponse {
            found: false,
            value: e,
        }),
    }
}

async fn handle_add_node(State(state): State<SharedState>) -> Json<ApiStatusResponse> {
    let (port, join_addr) = {
        let mut state_guard = state.lock().unwrap();
        let port = state_guard.next_port;
        state_guard.next_port += 1;

        // If there are existing nodes, pick one to join
        let join_addr = state_guard
            .nodes
            .values()
            .next()
            .map(|first_node| first_node.address.clone());
        (port, join_addr)
    };

    let mut cmd = Command::new("cargo");
    cmd.current_dir(".."); // Run from workspace root
    cmd.arg("run")
        .arg("--bin")
        .arg("chord_node")
        .arg("--")
        .arg("--port")
        .arg(port.to_string())
        .arg("--monitor")
        .arg("127.0.0.1:50051");

    if let Some(join) = join_addr {
        cmd.arg("--join").arg(join);
    }

    // Spawn in background
    match cmd.spawn() {
        Ok(_) => Json(ApiStatusResponse {
            success: true,
            message: format!("Spawned node on port {}", port),
        }),
        Err(e) => Json(ApiStatusResponse {
            success: false,
            message: format!("Failed to spawn node: {}", e),
        }),
    }
}

#[derive(Deserialize)]
struct ApiLeaveRequest {
    id: String, // u64 as string to avoid JS precision issues
}

async fn handle_leave_node(
    State(state): State<SharedState>,
    Json(payload): Json<ApiLeaveRequest>,
) -> Json<ApiStatusResponse> {
    let node_id = match payload.id.parse::<u64>() {
        Ok(id) => id,
        Err(_) => {
            return Json(ApiStatusResponse {
                success: false,
                message: "Invalid node ID".into(),
            })
        }
    };

    let node_addr = {
        let state = state.lock().unwrap();
        if let Some(node) = state.nodes.get(&node_id) {
            node.address.clone()
        } else {
            return Json(ApiStatusResponse {
                success: false,
                message: "Node not found".into(),
            });
        }
    };

    match connect_to_node(node_addr).await {
        Ok(mut client) => {
            match client.leave(Request::new(Empty {})).await {
                Ok(_) => {
                    // Remove from state
                    let mut state = state.lock().unwrap();
                    state.nodes.remove(&node_id);

                    Json(ApiStatusResponse {
                        success: true,
                        message: "Node left successfully".into(),
                    })
                }
                Err(e) => Json(ApiStatusResponse {
                    success: false,
                    message: format!("RPC error: {}", e),
                }),
            }
        }
        Err(e) => Json(ApiStatusResponse {
            success: false,
            message: e,
        }),
    }
}
