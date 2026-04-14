use std::fs;
use std::sync::Arc;
use arc_swap::ArcSwap;
use axum::{routing::get, Router, Json};
use axum::extract::{State, Query};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{broadcast, mpsc, RwLock};
// Add this to your imports
use xbricks_lib::toml_parser::XBricksConfig;
use axum::{
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
};
use axum::extract::ws::Utf8Bytes;
use futures_util::{sink::SinkExt, stream::StreamExt};

// A type alias to make the signatures much cleaner
pub type SharedConfig = Arc<RwLock<XBricksConfig>>;


/// Commands sent from the API to the main lifecycle loop
#[derive(Debug, Clone)]
pub enum SystemCommand {
    Start,    // Boot the dataplane
    Stop,     // Teardown the dataplane, but keep the API alive
    Restart,  // Teardown and immediately reboot
    Check,    // <-- NEW: Validate config without touching hardware
    Shutdown, // Kill the entire application
}

// --- The Master Application State ---
#[derive(Clone)]
pub struct AppState {
    pub config: Arc<tokio::sync::RwLock<XBricksConfig>>,
    // NEW: Wrapped in ArcSwap so the main thread can update it!
    pub shared_state: Arc<ArcSwap<SharedSystemState>>,
    pub console_tx: tokio::sync::broadcast::Sender<String>,
    pub cmd_tx: tokio::sync::mpsc::Sender<SystemCommand>,
    /// Path to the active .toml configuration file (mutable so saves can redirect)
    pub config_path: Arc<tokio::sync::RwLock<String>>,
    pub telemetry_tx: tokio::sync::broadcast::Sender<LiveStats>,
}

#[derive(Deserialize)]
pub struct NodeUpdatePayload {
    pub pipeline_name: String,
    pub node_id: String,
    pub routes: Option<Vec<String>>,
    pub default_route: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct Position {
    pub x: f32,
    pub y: f32,
}

#[derive(Serialize)]
pub struct TopologyResponse {
    nodes: Vec<UiNode>,
    edges: Vec<UiEdge>,
    has_saved_layout: bool,
    config_path: String,
}

#[derive(Serialize)]
struct FileEntry {
    name: String,
    is_dir: bool,
}

#[derive(Serialize)]
struct DirListing {
    path: String,
    parent: Option<String>,
    entries: Vec<FileEntry>,
}

#[derive(Deserialize)]
struct FileQuery {
    path: Option<String>,
}

#[derive(Deserialize)]
struct SaveConfigQuery {
    save_path: Option<String>,
    /// If false, do NOT restart the dataplane after saving. Default: true.
    restart: Option<bool>,
    /// If false, only update the in-memory config; do NOT write to disk. Default: true.
    save_to_disk: Option<bool>,
}


#[derive(Serialize)]
pub struct ConfigResponse {
    nodes: Vec<UiNode>,
    edges: Vec<UiEdge>,
}

#[derive(Serialize)]
struct UiNode {
    id: String,
    pub r#type: String, // "iface", "umem", or "pipeline_node"
    pub position: Position,
    pub data: Value, // We can dump any of your TOML structs in here!
}

#[derive(Serialize)]
struct UiEdge {
    id: String,
    source: String,
    target: String,
}

use axum::response::IntoResponse;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tower_http::services::ServeDir;
use xbricks_base::NodeId;
use xbricks_lib::config::hot_config::SharedSystemState;
use xbricks_lib::telemetry::LiveStats;

// Update the signature to return the shutdown sender AND the task handle
pub(crate) fn spawn_management_api(state: AppState) -> (oneshot::Sender<()>, JoinHandle<()>) {
    // Create a one-way channel for the shutdown signal
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let handle = tokio::spawn(async move {
        let app = Router::new()
            .route("/api/topology", get(get_topology))
            .route("/api/config", get(get_config))
            .route("/api/files", get(list_files))
            .route("/api/ws/terminal", get(terminal_ws_handler))
            .route("/api/hardware/update", axum::routing::post(save_hardware_config))
            .route("/api/pipeline/update", axum::routing::post(hot_update_node))
            .route("/api/layout", axum::routing::post(save_layout))
            .route("/api/dataplane/command", axum::routing::post(dataplane_command))
            .route("/api/ws/telemetry", get(telemetry_ws_handler))
            .with_state(state)
            .fallback_service(ServeDir::new("./xbricks-ui/dist"));

        let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();

        // Add .with_graceful_shutdown() to the server!
        if let Err(e) = axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                // The server will run until it receives a message on this channel
                shutdown_rx.await.ok();
                log::info!("Axum server shutting down gracefully...");
            })
            .await
        {
            log::error!("Management API crashed: {}", e);
        }
    });

    // Return the trigger and the handle back to main
    (shutdown_tx, handle)
}

// The API Endpoint Handler
async fn get_topology(State(state): State<AppState>) -> Json<TopologyResponse> {
    let config_guard = state.config.read().await;
    let layout = &config_guard.layout;
    let has_saved_layout = !layout.is_empty();

    let saved_pos = |id: &str| -> Position {
        layout.get(id)
            .map(|p| Position { x: p.x, y: p.y })
            .unwrap_or(Position { x: 0.0, y: 0.0 })
    };

    let mut ui_nodes = Vec::new();

    // 1. Add Interfaces
    for iface in &config_guard.xbricks.ifaces {
        let id = format!("iface_{}", iface.name);
        ui_nodes.push(UiNode {
            position: saved_pos(&id),
            id,
            r#type: "iface".to_string(),
            data: serde_json::to_value(iface).unwrap(),
        });
    }

    // 2. Add UMEMs
    for umem in &config_guard.xbricks.umem {
        let id = format!("umem_{}", umem.id);
        ui_nodes.push(UiNode {
            position: saved_pos(&id),
            id,
            r#type: "umem".to_string(),
            data: serde_json::to_value(umem).unwrap(),
        });
    }

    // 3. Add Workers
    for worker in &config_guard.xbricks.workers {
        let id = format!("worker_{}", worker.id);
        ui_nodes.push(UiNode {
            position: saved_pos(&id),
            id,
            r#type: "worker".to_string(),
            data: serde_json::to_value(worker).unwrap(),
        });
    }

    // 4. Add XSK mappings
    for (idx, xsk) in config_guard.xbricks.xsk.iter().enumerate() {
        let id = format!("xsk_{}", idx);
        ui_nodes.push(UiNode {
            position: saved_pos(&id),
            id,
            r#type: "xsk".to_string(),
            data: serde_json::to_value(xsk).unwrap(),
        });
    }

    // 5. Add Pipeline Nodes AND inject pipeline_name!
    for pipeline in &config_guard.pipelines {
        for node in &pipeline.nodes {
            let mut node_data = serde_json::to_value(node).unwrap();
            if let Some(obj) = node_data.as_object_mut() {
                obj.insert("pipeline_name".to_string(), Value::String(pipeline.name.clone()));
            }
            ui_nodes.push(UiNode {
                position: saved_pos(&node.id),
                id: node.id.clone(),
                r#type: node.node_type.clone(),
                data: node_data,
            });
        }
    }

    let config_path = state.config_path.read().await.clone();
    Json(TopologyResponse {
        nodes: ui_nodes,
        edges: vec![],
        has_saved_layout,
        config_path,
    })
}


async fn list_files(
    Query(query): Query<FileQuery>,
) -> impl axum::response::IntoResponse {
    let path_str = query.path.unwrap_or_else(|| ".".to_string());

    let canonical = match fs::canonicalize(&path_str) {
        Ok(p) => p,
        Err(e) => {
            return (axum::http::StatusCode::BAD_REQUEST, format!("Invalid path: {}", e))
                .into_response();
        }
    };

    let mut entries = Vec::new();
    match fs::read_dir(&canonical) {
        Ok(dir) => {
            let mut items: Vec<_> = dir.filter_map(|e| e.ok()).collect();
            // Directories first, then files; alphabetically within each group
            items.sort_by_key(|e| {
                let is_file = e.file_type().map(|t| t.is_file()).unwrap_or(false);
                (is_file, e.file_name())
            });
            for entry in items {
                let name = entry.file_name().to_string_lossy().to_string();
                let is_dir = entry.file_type().map(|t| t.is_dir()).unwrap_or(false);
                if !name.starts_with('.') {
                    entries.push(FileEntry { name, is_dir });
                }
            }
        }
        Err(e) => {
            return (axum::http::StatusCode::BAD_REQUEST, format!("Cannot read directory: {}", e))
                .into_response();
        }
    }

    let parent = canonical.parent().map(|p| p.to_string_lossy().to_string());

    Json(DirListing {
        path: canonical.to_string_lossy().to_string(),
        parent,
        entries,
    })
    .into_response()
}

async fn get_config(State(state): State<AppState>) -> Json<XBricksConfig> {
    let config_guard = state.config.read().await;
    // Returns the exact, raw Rust configuration structure as JSON
    Json(config_guard.clone())
}

// --- WEBSOCKET HANDLERS ---

/// Upgrades the standard HTTP request to a persistent WebSocket connection
async fn terminal_ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> axum::response::Response {
    ws.on_upgrade(|socket| handle_terminal_socket(socket, state))
}

/// Upgrades the standard HTTP request to a persistent WebSocket connection
async fn telemetry_ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> axum::response::Response {
    // Note the change here: handle_telemetry_socket
    ws.on_upgrade(|socket| handle_telemetry_socket(socket, state))
}

async fn handle_terminal_socket(mut socket: WebSocket, state: AppState) {
    // Subscribe to the global broadcast channel
    let mut rx = state.console_tx.subscribe();

    let welcome = "\x1b[1;36m[Rust Backend]\x1b[0m Connected to XBricks Console!\r\n";
    let prompt = "\x1b[1;32mroot@xbricks\x1b[0m:~$ ";

    let _ = socket.send(Message::Text(welcome.into())).await;
    let _ = socket.send(Message::Text(prompt.into())).await;

    // Buffer to hold the user's typing until they press Enter
    let mut input_buffer = String::new();

    loop {
        // tokio::select! allows us to wait on multiple async streams simultaneously!
        tokio::select! {
            // 1. OUTPUT: Did a Rust thread broadcast a log message?
            Ok(msg) = rx.recv() => {
                // Send it directly to the UI terminal
                let _ = socket.send(Message::Text(msg.into())).await;
                // Reprint the prompt so it looks clean
                let _ = socket.send(Message::Text(prompt.into())).await;
            }

            // 2. INPUT: Did the user type something in the UI?
            Some(Ok(msg)) = socket.next() => {
                if let Message::Text(keystroke) = msg {
                    if keystroke.as_str() == "\r" {
                        // --- COMMAND EXECUTED ---
                        let cmd = input_buffer.clone();
                        input_buffer.clear();

                        let _ = socket.send(Message::Text("\r\n".into())).await;

                        // Process the command!
                        match cmd.trim() {
                            "help" => {
                                let _ = socket.send(Message::Text("Available commands: help, status, ping\r\n".into())).await;
                            }
                            "ping" => {
                                let _ = socket.send(Message::Text("pong\r\n".into())).await;
                            }
                            "status" => {
                                let config = state.config.read().await;
                                let msg = format!("Running with {} pipelines configured.\r\n", config.pipelines.len());
                                let _ = socket.send(Message::Text(msg.into())).await;
                            }
                            other => {
                                if !other.is_empty() {
                                    let msg = format!("bash: {}: command not found\r\n", other);
                                    let _ = socket.send(Message::Text(msg.into())).await;
                                }
                            }
                        }

                        let _ = socket.send(Message::Text(prompt.into())).await;

                    } else if keystroke.as_str() == "\u{7f}" {
                        // Backspace pressed
                        if !input_buffer.is_empty() {
                            input_buffer.pop();
                            let _ = socket.send(Message::Text("\x08 \x08".into())).await;
                        }
                    } else {
                        // Normal typing
                        input_buffer.push_str(keystroke.as_str());
                        let _ = socket.send(Message::Text(keystroke)).await;
                    }
                }
            }

            // If either stream closes, break the loop and disconnect
            else => break,
        }
    }
}


async fn handle_telemetry_socket(mut socket: WebSocket, state: AppState) {
    // 1. Subscribe to the global telemetry broadcast channel
    let mut rx = state.telemetry_tx.subscribe();

    loop {
        tokio::select! {
            // EVENT A: The ethtool background worker sent a new stats update
            Ok(stats) = rx.recv() => {
                // Convert the Rust struct into a clean JSON string
                if let Ok(json_str) = serde_json::to_string(&stats) {
                    // Blast it to the React UI
                    if socket.send(Message::Text(Utf8Bytes::from(json_str))).await.is_err() {
                        // If sending fails, the user closed the browser tab.
                        log::debug!("Telemetry UI client disconnected.");
                        break;
                    }
                }
            }

            // EVENT B: Listen for client disconnects
            Some(msg) = socket.next() => {
                if let Ok(Message::Close(_)) = msg {
                    log::debug!("Telemetry UI client closed connection gracefully.");
                    break;
                }
            }

            // EVENT C: Channel closed or network error
            else => break,
        }
    }
}


#[derive(Deserialize)]
struct DataplaneCommandPayload {
    command: String,
}

async fn dataplane_command(
    State(state): State<AppState>,
    Json(payload): Json<DataplaneCommandPayload>,
) -> impl axum::response::IntoResponse {
    let cmd = match payload.command.to_lowercase().as_str() {
        "start"    => SystemCommand::Start,
        "stop"     => SystemCommand::Stop,
        "restart"  => SystemCommand::Restart,
        "check"    => SystemCommand::Check,
        "shutdown" => SystemCommand::Shutdown,
        other => {
            log::error!("Unknown dataplane command: {}", other);
            return axum::http::StatusCode::BAD_REQUEST;
        }
    };
    log::info!("API: sending dataplane command {:?}", cmd);
    if let Err(e) = state.cmd_tx.send(cmd).await {
        log::error!("Failed to send command: {}", e);
        return axum::http::StatusCode::INTERNAL_SERVER_ERROR;
    }
    axum::http::StatusCode::OK
}

async fn save_hardware_config(
    axum::extract::State(state): axum::extract::State<AppState>,
    Query(query): Query<SaveConfigQuery>,
    axum::Json(new_config): axum::Json<XBricksConfig>,
) -> impl axum::response::IntoResponse {

    // Determine target file; optionally redirect to a new path
    let save_path = if let Some(new_path) = query.save_path {
        let mut cp = state.config_path.write().await;
        *cp = new_path.clone();
        new_path
    } else {
        state.config_path.read().await.clone()
    };

    // 1. Update the in-memory config; optionally persist to disk (default: true)
    {
        let mut config_guard = state.config.write().await;
        *config_guard = new_config.clone();
        if query.save_to_disk.unwrap_or(true) {
            if let Ok(toml_string) = toml::to_string_pretty(&new_config) {
                if let Err(e) = std::fs::write(&save_path, toml_string) {
                    log::error!("Failed to write config to {}: {}", save_path, e);
                }
            }
        }
    }

    // 2. Optionally trigger a Hard Restart (default: true)
    if query.restart.unwrap_or(true) {
        if let Err(e) = state.cmd_tx.send(SystemCommand::Restart).await {
            log::error!("Failed to send restart command to main loop: {}", e);
            return axum::http::StatusCode::INTERNAL_SERVER_ERROR;
        }
        log::info!("Hardware configuration saved to {}. Restarting datapath...", save_path);
    } else {
        log::info!("Hardware configuration saved to {} (no restart).", save_path);
    }

    axum::http::StatusCode::OK
}


async fn hot_update_node(
    State(state): State<AppState>,
    Json(payload): Json<NodeUpdatePayload>,
) -> impl axum::response::IntoResponse {

    // =================================================================
    // PART 1: RCU HOT SWAP (Updates the Live Data Plane)
    // =================================================================

    // 1. Load the active outer system state
    let current_system = state.shared_state.load();

    // 2. Get the specific RCU pointer for this pipeline
    let pipeline_rcu = match current_system.pipelines.get(&payload.pipeline_name) {
        Some(rcu) => rcu,
        None => {
            log::error!("Pipeline '{}' not found for hot update", payload.pipeline_name);
            return axum::http::StatusCode::NOT_FOUND;
        }
    };

    // 2. READ & COPY: Load the current state and clone it
    let current_arc = pipeline_rcu.load();
    let mut new_config = (**current_arc).clone();

    // 3. Find the target node's integer index
    let target_node_index = match new_config.name_to_index.get(&payload.node_id) {
        Some(&idx) => idx,
        None => {
            log::error!("Node '{}' not found in pipeline '{}'", payload.node_id, payload.pipeline_name);
            return axum::http::StatusCode::NOT_FOUND;
        }
    };

    // 4. MODIFY ROUTES: Translate Strings from UI into fast NodeIds
    if let Some(new_route_strings) = &payload.routes {
        let mut resolved_routes = Vec::new();
        for route_str in new_route_strings {
            if let Some(&target_idx) = new_config.name_to_index.get(route_str) {
                resolved_routes.push(NodeId::new(target_idx as u32, route_str.clone()));
            } else {
                log::error!("Target route '{}' not found", route_str);
                return axum::http::StatusCode::BAD_REQUEST;
            }
        }
        new_config.fast_nodes[target_node_index].routes = resolved_routes;
    }

    // 5. MODIFY DEFAULT ROUTE: Translate String into fast NodeId
    if let Some(new_default_string) = &payload.default_route {
        if new_default_string.trim().is_empty() {
            // Treat empty string as removing the default route
            new_config.fast_nodes[target_node_index].default_route = None;
        } else if let Some(&target_idx) = new_config.name_to_index.get(new_default_string) {
            new_config.fast_nodes[target_node_index].default_route =
                Some(NodeId::new(target_idx as u32, new_default_string.clone()));
        } else {
            log::error!("Default target route '{}' not found", new_default_string);
            return axum::http::StatusCode::BAD_REQUEST;
        }
    }

    // 6. UPDATE: Atomically swap the pointer.
    // The next time a worker running this pipeline hits a batch boundary,
    // it will instantly use the new routing array.
    pipeline_rcu.store(Arc::new(new_config));
    log::info!("Live update applied to node: {} in pipeline: {}", payload.node_id, payload.pipeline_name);


    // =================================================================
    // PART 2: BASE CONFIG SYNC (Persists for Hard Restarts)
    // =================================================================
    {
        let mut base_config = state.config.write().await;

        // Find the right pipeline and node in the base TOML structure
        if let Some(pipeline) = base_config.pipelines.iter_mut().find(|p| p.name == payload.pipeline_name) {
            if let Some(node) = pipeline.nodes.iter_mut().find(|n| n.id == payload.node_id) {

                // Keep the string representations updated in the base config
                if let Some(new_routes) = payload.routes {
                    node.routes = new_routes;
                }

                if let Some(new_default) = payload.default_route {
                    node.default = if new_default.trim().is_empty() {
                        None
                    } else {
                        Some(new_default)
                    };
                }
            }
        }
        let config_path = state.config_path.read().await.clone();
        match toml::to_string_pretty(&*base_config) {
            Ok(toml_string) => {
                if let Err(e) = std::fs::write(&config_path, toml_string) {
                    log::error!("Failed to write config to disk: {}", e);
                } else {
                    log::info!("Config updated on disk.");
                }
            }
            Err(e) => log::error!("Failed to serialize TOML: {}", e),
        }
    }

    axum::http::StatusCode::OK
}

/// Updates node positions from the UI into the in-memory [layout] section only.
/// The layout is persisted to disk only when the user explicitly saves via File > Save to Disk.
async fn save_layout(
    State(state): State<AppState>,
    Json(positions): Json<std::collections::HashMap<String, Position>>,
) -> impl axum::response::IntoResponse {
    use xbricks_lib::toml_parser::SavedPosition;

    let mut config = state.config.write().await;
    config.layout = positions.into_iter()
        .map(|(id, pos)| (id, SavedPosition { x: pos.x, y: pos.y }))
        .collect();

    log::debug!("Layout updated in-memory ({} nodes)", config.layout.len());
    axum::http::StatusCode::OK
}