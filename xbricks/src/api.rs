use std::collections::HashMap;
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
use axum::response::IntoResponse;
use tokio::sync::broadcast::Sender;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use toml_edit::{ser, DocumentMut, Item};
use tower_http::services::ServeDir;
use xbricks_protocol::helpers::parse_suffixed_number;
use xbricks_base::{ControlEvent, NodeId};
use xbricks_lib::config::hot_config::{NodeDynamicState, SharedSystemState};
use xbricks_lib::generator_node::GenState;
use xbricks_base::telemetry::{TMLiveStats};
use xbricks_protocol::wbatch::MAX_BATCH_SIZE;

// A type alias to make the signatures much cleaner
pub type SharedConfig = Arc<RwLock<XBricksConfig>>;


/// Commands sent from the API to the main lifecycle loop
#[derive(Debug, Clone)]
pub enum SystemCommand {
    Start,    // Boot the dataplane
    Stop,     // Teardown the dataplane, but keep the API alive
    Check,    // Validate config without touching hardware
    Shutdown, // Kill the entire application
}

#[derive(Deserialize)]
pub struct GuiEventRequest {
    pub opcode: u32,
    pub payload: Vec<u8>,
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
    pub telemetry_tx: tokio::sync::broadcast::Sender<TMLiveStats>,
    pub ctrl_tx: Sender<ControlEvent>,
}

#[derive(Deserialize)]
pub struct PipelineConfigUpdatePayload {
    pub pipeline_name: String,
    pub poll_frequency: u64,
}

#[derive(Deserialize)]
pub struct NodeUpdatePayload {
    pub pipeline_name: String,
    pub node_id: String,
    pub routes: Option<Vec<String>>,
    pub default_route: Option<String>,
    /// Simple state transition: "start" → Generating, "stop" → Idle.
    /// Preserves pps_limit / packet_limit already in the hot config.
    /// Ignored when dynamic_state is also set.
    pub cmd: Option<String>,
    /// Full typed hot state replacement. Takes precedence over cmd.
    pub dynamic_state: Option<NodeDynamicState>,
    /// Hot-updatable args. Updated in the in-memory base config so File→Save
    /// persists them; takes effect on next pipeline restart.
    pub hot_args: Option<HashMap<String, String>>,
}

#[derive(Serialize, Deserialize)]
pub struct Position {
    pub x: f32,
    pub y: f32,
}

#[derive(Serialize)]
pub struct PipelineSummary {
    name: String,
    workers: Vec<usize>,
}

#[derive(Serialize)]
pub struct TopologyResponse {
    nodes: Vec<UiNode>,
    edges: Vec<UiEdge>,
    has_saved_layout: bool,
    config_path: String,
    pipelines: Vec<PipelineSummary>,
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


/// Information about a network interface that supports XDP native mode with zerocopy.
#[derive(Serialize)]
struct XdpIfaceInfo {
    name: String,
    max_rxd: u32,
    max_txd: u32,
}

/// GET /api/interfaces/xdp
/// Returns all physical interfaces that support both XDP native mode and AF_XDP zerocopy,
/// together with their maximum hardware ring sizes.
/// Delegates discovery to `xbricks_base::system::query_xdp_ifaces`.
async fn list_xdp_ifaces() -> impl IntoResponse {
    let caps = tokio::task::spawn_blocking(xbricks_base::system::query_xdp_ifaces)
        .await
        .unwrap_or_default();

    let result: Vec<XdpIfaceInfo> = caps
        .into_iter()
        .filter(|c| c.xdp_native && c.xdp_zerocopy)
        .map(|c| XdpIfaceInfo {
            name:    c.name,
            max_rxd: c.rx_ring_max.unwrap_or(4096),
            max_txd: c.tx_ring_max.unwrap_or(4096),
        })
        .collect();

    Json(result).into_response()
}

// Update the signature to return the shutdown sender AND the task handle
pub(crate) fn spawn_management_api(state: AppState) -> (oneshot::Sender<()>, JoinHandle<()>) {
    // Create a one-way channel for the shutdown signal
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let handle = tokio::spawn(async move {
        let app = Router::new()
            .route("/api/topology", get(get_topology))
            .route("/api/config", get(get_config))
            .route("/api/version", get(get_version))
            .route("/api/files", get(list_files))
            .route("/api/interfaces/xdp", get(list_xdp_ifaces))
            .route("/api/ws/terminal", get(terminal_ws_handler))
            .route("/api/hardware/update", axum::routing::post(save_hardware_config))
            .route("/api/pipeline/update", axum::routing::post(hot_update_node))
            .route("/api/pipeline/update_config", axum::routing::post(hot_update_pipeline_config))
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
    // Node IDs are scoped to their pipeline ("pipeline_name/node_id") so that
    // nodes with the same id in different pipelines don't collide in the UI.
    for pipeline in &config_guard.pipelines {
        for node in &pipeline.nodes {
            let scoped_id = format!("{}/{}", pipeline.name, node.id);
            let mut node_data = serde_json::to_value(node).unwrap();
            if let Some(obj) = node_data.as_object_mut() {
                obj.insert("pipeline_name".to_string(), Value::String(pipeline.name.clone()));
                // Scope routes/default so edge targets resolve to scoped node IDs.
                let scoped_routes: Vec<Value> = node.routes.iter()
                    .map(|r| Value::String(format!("{}/{}", pipeline.name, r)))
                    .collect();
                obj.insert("routes".to_string(), Value::Array(scoped_routes));
            }
            ui_nodes.push(UiNode {
                position: saved_pos(&scoped_id),
                id: scoped_id,
                r#type: node.node_type.clone(),
                data: node_data,
            });
        }
    }

    // 6. Add PipelineHeader nodes — one per pipeline, carries name+workers+poll_frequency
    for pipeline in &config_guard.pipelines {
        let id = format!("pipeline_header/{}", pipeline.name);
        let data = serde_json::json!({
            "name": pipeline.name,
            "workers": pipeline.workers,
            "poll_frequency": pipeline.poll_frequency,
        });
        ui_nodes.push(UiNode { position: saved_pos(&id), id, r#type: "pipeline_header".to_string(), data });
    }

    let pipeline_summaries: Vec<PipelineSummary> = config_guard.pipelines.iter()
        .map(|p| PipelineSummary { name: p.name.clone(), workers: p.workers.clone() })
        .collect();

    let config_path = state.config_path.read().await.clone();
    Json(TopologyResponse {
        nodes: ui_nodes,
        edges: vec![],
        has_saved_layout,
        config_path,
        pipelines: pipeline_summaries,
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

// Axum Handler
pub async fn handle_gui_event(
    State(ctrl_tx): State<Sender<ControlEvent>>,
    Json(request): Json<GuiEventRequest>,
) {
    let event = ControlEvent {
        opcode: request.opcode,
        payload: request.payload,
    };

    // Shoot it across the thread boundary into the dataplane!
    let _ = ctrl_tx.send(event);
}

/// Serialize `config` to a human-readable TOML file at `path`, applying the same
/// inline-array formatting used by `save_hardware_config`.
fn write_config_to_disk(config: &XBricksConfig, path: &str) {
    let mut config = config.clone();
    // do not save the cmd property of PacketGenerator nodes, as it is not a valid TOML property
    for pipeline in &mut config.pipelines {
        for node in &mut pipeline.nodes {
            if node.node_type == "PacketGenerator" {
                node.args.remove("cmd");
            }
        }
    }

    let config = &config;
    if let Ok(toml_string) = toml::to_string_pretty(config) {
        if let Ok(mut doc) = toml_string.parse::<DocumentMut>() {
            if let Some(Item::Table(xbricks)) = doc.get_mut("xbricks") {
                for key in ["ifaces", "umem", "xsk", "workers"] {
                    if let Some(item) = xbricks.get_mut(key) {
                        format_vertical_array_of_inlines(item);
                    }
                }
            }
            // --- Format [[pipelines]] ---
            if let Some(toml_edit::Item::ArrayOfTables(pipelines)) = doc.get_mut("pipelines") {
                for pipeline in pipelines.iter_mut() {

                    // --- NEW: INJECT DIVIDER COMMENTS ---
                    // Attempt to read the pipeline's name to include it in the comment
                    let pipeline_name = pipeline.get("name")
                        .and_then(|n| n.as_str())
                        .unwrap_or("UNNAMED");

                    // Set a decorative comment block right before the [[pipelines]] header
                    let divider = format!(
                        "\n# ===================================================================\n# PIPELINE: {}\n# ===================================================================\n",
                        pipeline_name.to_uppercase()
                    );
                    pipeline.decor_mut().set_prefix(divider);
                    // ------------------------------------

                    // Make `workers = [0]` a true single-line array
                    if let Some(workers) = pipeline.get_mut("workers") {
                        format_single_line_inline(workers);
                    }

                    // Traverse the [[pipelines.nodes]] tables
                    if let Some(toml_edit::Item::ArrayOfTables(nodes)) = pipeline.get_mut("nodes") {
                        for node in nodes.iter_mut() {

                            // Target ONLY the inner properties
                            let props_to_inline = ["args", "hot_args", "queues", "routes", "labels"];

                            for prop in props_to_inline {
                                if let Some(item) = node.get_mut(prop) {
                                    format_single_line_inline(item);
                                }
                            }
                        }
                    }
                }
            }
            if let Err(e) = std::fs::write(path, doc.to_string()) {
                log::error!("Failed to write config to {}: {}", path, e);
            }
        } else {
            log::error!("Failed to parse TOML string into toml_edit AST.");
        }
    } else {
        log::error!("Failed to serialize config to base TOML string.");
    }
}

// Helper to format an array so it appears vertically, but its items remain inline horizontal tables
fn format_vertical_array_of_inlines(item: &mut toml_edit::Item) {
    // 1. Force the vertical [[table]] to become a standard array [...]
    item.make_value();

    // 2. Format the array items
    if let toml_edit::Item::Value(toml_edit::Value::Array(arr)) = item {
        arr.set_trailing_comma(true);

        for val in arr.iter_mut() {
            // Inject a newline and 4 spaces before each inline table
            val.decor_mut().set_prefix("\n    ");

            // --- Clean up nested arrays INSIDE the inline table ---
            if let toml_edit::Value::InlineTable(table) = val {
                for (_key, inner_val) in table.iter_mut() {
                    // If the property is an array (like 'queues' or 'partitions')
                    if let toml_edit::Value::Array(inner_arr) = inner_val {
                        inner_arr.set_trailing(""); // Remove newline before closing ']'
                        inner_arr.set_trailing_comma(false); // Clean up trailing comma

                        // Strip newlines from the elements and replace with spaces
                        for (i, item_val) in inner_arr.iter_mut().enumerate() {
                            if i == 0 {
                                item_val.decor_mut().set_prefix("");
                            } else {
                                item_val.decor_mut().set_prefix(" "); // Add space after comma
                            }
                            item_val.decor_mut().set_suffix(""); // Strip trailing newlines
                        }
                    }
                }
            }
            // ---------------------------------------------------------------
        }

        if !arr.is_empty() {
            arr.set_trailing("\n");
            arr.decor_mut().set_suffix("");
        }
    }
}

// Helper to strip newlines from simple arrays like [0, 1] to keep them on a single line
fn format_single_line_inline(item: &mut toml_edit::Item) {
    item.make_value();

    if let toml_edit::Item::Value(toml_edit::Value::Array(arr)) = item {
        arr.set_trailing(""); // Remove the newline before the closing ']'
        arr.set_trailing_comma(false); // Clean up the trailing comma for single-line arrays

        for (i, val) in arr.iter_mut().enumerate() {
            if i == 0 {
                val.decor_mut().set_prefix("");
            } else {
                // Add a single space after the comma for readability: [0, 1] instead of [0,1]
                val.decor_mut().set_prefix(" ");
            }
            val.decor_mut().set_suffix(""); // Strip trailing newlines from the element
        }
    }
}

async fn save_hardware_config(
    State(state): State<AppState>,
    Query(query): Query<SaveConfigQuery>,
    Json(new_config): Json<XBricksConfig>,
) -> impl IntoResponse {

    let save_path = if let Some(new_path) = query.save_path {
        let mut cp = state.config_path.write().await;
        *cp = new_path.clone();
        new_path
    } else {
        state.config_path.read().await.clone()
    };

    {
        let mut config_guard = state.config.write().await;
        *config_guard = new_config.clone();

        if query.save_to_disk.unwrap_or(true) {
            write_config_to_disk(&new_config, &save_path);
        }
    }

    if query.restart.unwrap_or(true) {
        if let Err(e) = state.cmd_tx.send(SystemCommand::Start).await {
            log::error!("Failed to send restart command to main loop: {}", e);
            return axum::http::StatusCode::INTERNAL_SERVER_ERROR;
        }
        log::info!("Hardware configuration saved to {}. Restarting...", save_path);
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

    // 6. APPLY TYPED STATE — live RCU only, not persisted to disk
    if let Some(new_state) = payload.dynamic_state {
        new_config.fast_nodes[target_node_index].dynamic_state = new_state;
    } else if let Some(cmd) = &payload.cmd {
        // Simple start/stop: flip only the state field, keep pps_limit / packet_limit as-is.
        if let NodeDynamicState::Generator { state, .. } =
            &mut new_config.fast_nodes[target_node_index].dynamic_state
        {
            *state = match cmd.as_str() {
                "start" => GenState::Generating,
                "stop" => GenState::Idle,
                other => {
                    log::warn!("Unknown cmd '{}' for node {}", other, payload.node_id);
                    return axum::http::StatusCode::BAD_REQUEST;
                }
            };
        }
    }

    // 6b. Apply hot_args that map to Generator dynamic state (pps, packet_count, batch_size).
    //     Must run before the RCU store so the running dataplane picks them up.
    if let Some(ref hot_args) = payload.hot_args {
        if let NodeDynamicState::Generator { pps_limit, packet_limit, batch_size, .. } =
            &mut new_config.fast_nodes[target_node_index].dynamic_state
        {
            if let Some(pps_str) = hot_args.get("pps") {
                match parse_suffixed_number(pps_str) {
                    Ok(v) => { *pps_limit = v; }
                    Err(e) => { log::warn!("hot_args pps parse error: {}", e); }
                }
            }
            if let Some(count_str) = hot_args.get("packet_count") {
                match parse_suffixed_number(count_str) {
                    Ok(v) => { *packet_limit = v as usize; }
                    Err(e) => { log::warn!("hot_args packet_count parse error: {}", e); }
                }
            }
            if let Some(bs_str) = hot_args.get("batch_size") {
                match bs_str.parse::<usize>() {
                    Ok(v) if v > 0 && v <= MAX_BATCH_SIZE => { *batch_size = v; }
                    Ok(v) if v == 0 => { log::warn!("hot_args batch_size must be > 0"); }
                    Ok(v) => { log::warn!("hot_args batch_size {} exceeds MAX_BATCH_SIZE {}", v, MAX_BATCH_SIZE); }
                    Err(e) => { log::warn!("hot_args batch_size parse error: {}", e); }
                }
            }
        }
    }

    // 7. UPDATE: Atomically swap the pointer.
    // The next time a worker running this pipeline hits a batch boundary,
    // it will instantly use the new routing array.
    pipeline_rcu.store(Arc::new(new_config));
    log::info!("Live update applied to node: {} in pipeline: {}", payload.node_id, payload.pipeline_name);

    // 8. PERSIST hot_args to the in-memory base config so File→Save captures them.
    if let Some(hot_args) = payload.hot_args {
        let mut base_config = state.config.write().await;
        if let Some(pipeline) = base_config.pipelines.iter_mut().find(|p| p.name == payload.pipeline_name) {
            if let Some(node) = pipeline.nodes.iter_mut().find(|n| n.id == payload.node_id) {
                node.hot_args = hot_args;
            }
        }
    }

/*
    hot updates we do not write to disk
    TODO: remove this if really not needed
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
            }
        }
        let config_path = state.config_path.read().await.clone();
        write_config_to_disk(&*base_config, &config_path);
        log::info!("Config updated on disk.");
    }
*/
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
async fn get_version() -> impl IntoResponse {
    Json(serde_json::json!({ "version": env!("CARGO_PKG_VERSION") }))
}

async fn hot_update_pipeline_config(
    State(state): State<AppState>,
    Json(payload): Json<PipelineConfigUpdatePayload>,
) -> impl axum::response::IntoResponse {
    let current_system = state.shared_state.load();
    let pipeline_rcu = match current_system.pipelines.get(&payload.pipeline_name) {
        Some(rcu) => rcu,
        None => {
            log::error!("Pipeline '{}' not found for poll_frequency update", payload.pipeline_name);
            return axum::http::StatusCode::NOT_FOUND;
        }
    };

    let current_arc = pipeline_rcu.load();
    let mut new_config = (**current_arc).clone();
    new_config.poll_frequency = payload.poll_frequency.max(1);
    pipeline_rcu.store(Arc::new(new_config));
    log::info!("poll_frequency for pipeline '{}' set to {} Hz", payload.pipeline_name, payload.poll_frequency);

    let mut base_config = state.config.write().await;
    if let Some(pipeline) = base_config.pipelines.iter_mut().find(|p| p.name == payload.pipeline_name) {
        pipeline.poll_frequency = payload.poll_frequency.max(1);
    }

    axum::http::StatusCode::OK
}
