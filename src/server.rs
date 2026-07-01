use axum::{
    Json,
    extract::{
        ConnectInfo, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use base64::{Engine, engine::general_purpose};
use futures::{sink::SinkExt, stream::StreamExt};
use log::{debug, error, info, warn};
use qrcode::QrCode;
use std::{env, net::SocketAddr, sync::Arc};
use tokio::sync::watch;

use crate::{clipboard::ClipboardData, rpc_server};

pub struct ServerState {
    pub rx: watch::Receiver<ClipboardData>,
    pub tx: watch::Sender<ClipboardData>,
}

pub async fn start_server(
    addr: SocketAddr,
    clipboard_receiver: watch::Receiver<ClipboardData>,
    clipboard_sender: watch::Sender<ClipboardData>,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("Starting HTTP server on {addr}");

    let state = Arc::new(ServerState {
        rx: clipboard_receiver,
        tx: clipboard_sender,
    });

    let app = tonic::service::Routes::default()
        .add_service(rpc_server::ClipboardSyncServiceServer::new(
            rpc_server::ClipboardSyncServer::new(Arc::clone(&state)),
        ))
        .into_axum_router()
        .with_state(())
        .route("/ping", get(ping))
        .route("/clipboard", get(get_clipboard).post(set_clipboard))
        .route(
            "/clipboard/text",
            get(get_clipboard_text).post(set_clipboard_text),
        )
        .route(
            "/clipboard/base64",
            get(get_clipboard_base64).post(set_clipboard_base64),
        )
        .route("/clipboard/qrcode_show", post(qrcode_show))
        .route("/clipboard/qrcode_decode", post(qrcode_decode))
        .route("/ws", get(websocket_handler))
        .route("/shutdown", post(shutdown_server))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(addr).await.map_err(|e| {
        error!("Failed to bind to address {addr}: {e}");
        e
    })?;

    info!("Server listening on {addr}");

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await?;

    Ok(())
}

async fn ping() -> Result<String, StatusCode> {
    info!("GET /ping called");
    Ok("pong".into())
}

async fn get_clipboard(
    State(state): State<Arc<ServerState>>,
) -> Result<Json<ClipboardData>, StatusCode> {
    info!("GET /clipboard called");

    let data = state.rx.borrow();
    Ok(Json(data.clone()))
}

async fn set_clipboard(
    State(state): State<Arc<ServerState>>,
    Json(payload): Json<ClipboardData>,
) -> Result<(), StatusCode> {
    debug!("POST /clipboard called");

    state.tx.send_if_modified(|data| {
        if data.content != payload.content {
            data.content = payload.content;
            data.source = payload.source;
            true
        } else {
            false
        }
    });

    Ok(())
}

async fn get_clipboard_text(State(state): State<Arc<ServerState>>) -> Result<String, StatusCode> {
    info!("GET /clipboard/text called");

    let data = state.rx.borrow();
    let text = String::from_utf8(data.content.clone()).unwrap_or("Invaild utf-8 string".into());
    debug!("GET: [{text}]");
    Ok(text)
}

async fn set_clipboard_text(
    State(state): State<Arc<ServerState>>,
    text: String,
) -> Result<(), StatusCode> {
    debug!("POST /clipboard/text called");
    debug!("SET: [{text}]");
    state.tx.send_if_modified(|data| {
        let content = text.as_bytes();
        if data.content != content {
            data.content = content.into();
            data.source = "text".into();
            true
        } else {
            false
        }
    });

    Ok(())
}

async fn get_clipboard_base64(State(state): State<Arc<ServerState>>) -> Result<String, StatusCode> {
    info!("GET /clipboard/base64 called");

    let data = state.rx.borrow();
    Ok(general_purpose::STANDARD.encode(&data.content))
}

async fn set_clipboard_base64(
    State(state): State<Arc<ServerState>>,
    text: String,
) -> Result<(), StatusCode> {
    debug!("POST /clipboard/base64 called");
    debug!("SET: [{text}]");

    state.tx.send_if_modified(|data| {
        if let Ok(content) = general_purpose::STANDARD.decode(text)
            && data.content != content
        {
            data.content = content;
            data.source = "text".into();
            true
        } else {
            false
        }
    });

    Ok(())
}

async fn qrcode_show(State(state): State<Arc<ServerState>>) -> Result<(), (StatusCode, String)> {
    let data = state.rx.borrow();
    let code = QrCode::new(&data.content).map_err(internal_error)?;
    let image = code.render::<image::Rgb<u8>>().build();
    let img_path = env::temp_dir().join("clipboard_qrcode.png");
    image.save(&img_path).map_err(internal_error)?;
    open::that_detached(&img_path).map_err(internal_error)?;
    Ok(())
}

async fn qrcode_decode(
    State(state): State<Arc<ServerState>>,
    base64_img: String,
) -> Result<String, (StatusCode, String)> {
    let buf = general_purpose::STANDARD
        .decode(base64_img)
        .map_err(internal_error)?;
    let mut img = rqrr::PreparedImage::prepare(
        image::load_from_memory(&buf)
            .map_err(internal_error)?
            .to_luma8(),
    );
    let text = img
        .detect_grids()
        .iter()
        .map(|grid| match grid.decode() {
            Ok((_, content)) => content,
            Err(_) => "qr decode failed".into(),
        })
        .collect::<Vec<String>>()
        .join("\n");
    state.tx.send_if_modified(|data| {
        let content = text.as_bytes();
        if data.content != content {
            data.content = content.into();
            data.source = "text".into();
            true
        } else {
            false
        }
    });
    Ok(text)
}

async fn websocket_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<ServerState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| websocket_connection(socket, state, addr))
}

async fn shutdown_server(
    State(_state): State<Arc<ServerState>>,
) -> Result<Json<&'static str>, StatusCode> {
    info!("Shutdown request received, exiting process");

    // Spawn a task to exit the process after a short delay
    // This allows us to send a response before exiting
    tokio::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        std::process::exit(0);
    });

    Ok(Json("Server shutdown initiated"))
}

async fn websocket_connection(socket: WebSocket, state: Arc<ServerState>, addr: SocketAddr) {
    let (mut sender, mut receiver) = socket.split();

    // Task to send clipboard updates to the client
    let mut clipboard_receiver = state.rx.clone();
    let mut send_task = tokio::spawn(async move {
        let addr = Arc::from(addr.to_string());
        loop {
            if clipboard_receiver.changed().await.is_err() {
                error!("Clipboard receiver closed");
                break;
            }
            if clipboard_receiver.borrow_and_update().source == addr {
                continue;
            }
            let json_str = serde_json::to_string(&*clipboard_receiver.borrow())
                .unwrap_or("serde_json failed".into());
            match sender.send(Message::text(json_str)).await {
                Ok(_) => info!("send success"),
                Err(err) => error!("send error {err}"),
            }
        }
    });

    let clipboard_sender = state.tx.clone();
    // Task to receive messages from the client
    let mut recv_task = tokio::spawn(async move {
        let addr = Arc::from(addr.to_string());
        while let Some(result) = receiver.next().await {
            match result {
                Ok(msg) => match msg {
                    Message::Text(text) => {
                        debug!("Received WebSocket message");
                        if let Ok(context) =
                            serde_json::from_str::<ClipboardData>(&text.to_string())
                        {
                            clipboard_sender.send_if_modified(|data| {
                                if data.content != context.content {
                                    data.content = context.content;
                                    data.source = Arc::clone(&addr);
                                    true
                                } else {
                                    false
                                }
                            });
                        } else {
                            warn!("Text is not ClipboardData")
                        }
                    }
                    Message::Close(frame) => {
                        if let Some(frame) = frame {
                            info!("WebSocket closed by client: {}", frame.reason);
                        } else {
                            info!("WebSocket closed by client");
                        }
                        break;
                    }
                    Message::Ping(data) => {
                        println!("{addr} send ping {data:?}");
                    }
                    _ => {}
                },
                Err(e) => {
                    error!("WebSocket error: {e}");
                    break;
                }
            }
        }
    });

    // Wait for either task to complete
    tokio::select! {
        _ = (&mut send_task) => {
            info!("Send task completed");
            recv_task.abort();
        },
        _ = (&mut recv_task) => {
            info!("Receive task completed");
            send_task.abort();
        },
    }

    info!("WebSocket connection closed");
}

/// Utility function for mapping any error into a `500 Internal Server Error`
/// response.
fn internal_error<E>(err: E) -> (StatusCode, String)
where
    E: std::error::Error,
{
    (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
}
