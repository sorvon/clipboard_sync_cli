mod client;
mod clipboard;
mod rpc_server;
mod server;

use std::ffi::OsString;
use std::net::ToSocketAddrs;
use std::sync::Arc;

use anyhow::Result;
use clap::Args;
use clap::Parser;
use clap::Subcommand;
use clipboard::ClipboardData;
use futures::future::join_all;
use service_manager::RestartPolicy;
use service_manager::ServiceLevel;
use service_manager::ServiceManager;
use tokio::io;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::sync::watch;
use tokio::task;
use tokio::task::yield_now;
use tonic::transport::Channel;

use crate::clipboard::set_clipboard;
use crate::rpc_server::EncodeType;
use crate::rpc_server::GetRequest;
use crate::rpc_server::QrDecodeRequest;
use crate::rpc_server::QrShowRequest;
use crate::rpc_server::SetRequest;
use crate::rpc_server::clipboard_sync_service_client::ClipboardSyncServiceClient;
use crate::rpc_server::qr_decode;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Cli {
    #[arg(long, short, num_args(0..=1), default_missing_value("0.0.0.0:11457"))]
    server: Option<String>,
    #[arg(long, short, num_args(0..=1), default_missing_value("localhost:11457"))]
    connect: Option<String>,
    #[arg(long, short, num_args(0..=1))]
    displays: Option<Vec<String>>,
    #[command(subcommand)]
    sub_command: Option<SubCommand>,
}

#[derive(Subcommand, Debug)]
enum SubCommand {
    Service(ServiceArgs),
    Get(RpcClientArgs),
    Set(RpcClientArgs),
    QrShow(RpcClientArgs),
    QrDecode(RpcClientArgs),
    QrDecodeLocal(QrDecodeLocalArgs),
}

#[derive(Args, Debug)]
struct ServiceArgs {
    #[arg(short, long, default_value = env!("CARGO_PKG_NAME"), help = "service name")]
    name: String,

    #[command(subcommand)]
    sub_command: ServiceSubCommand,
}
#[derive(Args, Debug)]
struct RpcClientArgs {
    #[arg(short, long, value_enum, default_value_t)]
    encode_type: EncodeType,
    #[arg(
        long,
        short,
        env = "CLIPBOARD_SYNC_CLI_SERVER",
        default_value = "127.0.0.1:11457"
    )]
    server: String,
}

#[derive(Args, Debug)]
struct QrDecodeLocalArgs {
    #[arg(short, long)]
    shot: bool,
}

#[derive(Subcommand, Debug)]
enum ServiceSubCommand {
    #[command(about = "register system service")]
    Install(InstallArgs),
    #[command(about = "unregister system service")]
    Uninstall,
    #[command(about = "check service status")]
    Status,
    #[command(about = "start service")]
    Start,
    #[command(about = "stop service")]
    Stop,
}

#[derive(Args, Debug)]
struct InstallArgs {
    #[arg(trailing_var_arg = true, allow_hyphen_values = true, help = "args")]
    args: Option<Vec<OsString>>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let mut cli = Cli::parse();
    log::debug!("{:?}", cli);
    match cli.sub_command {
        Some(SubCommand::Service(service_args)) => {
            let mut manager =
                <dyn ServiceManager>::native().expect("Failed to detect management platform");
            if let Err(e) = manager.set_level(ServiceLevel::User) {
                log::warn!("Service manager does not support user-level services: {e}");
            }
            let label = service_args.name.parse().expect("Get service label fail");
            match service_args.sub_command {
                ServiceSubCommand::Install(install_args) => {
                    manager
                        .install(service_manager::ServiceInstallCtx {
                            label,
                            program: std::env::current_exe().expect("Get current_exe fail"),
                            args: install_args.args.unwrap_or(vec![]),
                            contents: None,
                            username: None,
                            working_directory: None,
                            environment: None,
                            autostart: true,
                            restart_policy: RestartPolicy::default(),
                        })
                        .expect("Failed to install");
                    println!("Install success");
                }
                ServiceSubCommand::Uninstall => {
                    manager
                        .uninstall(service_manager::ServiceUninstallCtx { label })
                        .expect("Failed to uninstall");
                    println!("Uninstall success");
                }
                ServiceSubCommand::Status => {
                    let status = manager
                        .status(service_manager::ServiceStatusCtx { label })
                        .expect("Failed to get status");
                    println!("{status:?}");
                }
                ServiceSubCommand::Start => {
                    manager
                        .start(service_manager::ServiceStartCtx { label })
                        .expect("Failed to start");
                    println!("Start success");
                }
                ServiceSubCommand::Stop => {
                    manager
                        .stop(service_manager::ServiceStopCtx { label })
                        .expect("Failed to stop");
                    println!("Stop success");
                }
            }
        }
        Some(SubCommand::Get(arg)) => {
            log::info!("{:?}", arg);
            let mut client = get_rpc_client(arg.server).await?;
            let res = client
                .get(GetRequest {
                    encode_type: arg.encode_type.into(),
                })
                .await?
                .into_inner();
            log::info!("{:?}", &res.data);
            io::stdout().write_all(&res.data).await?;
        }
        Some(SubCommand::Set(arg)) => {
            log::info!("{:?}", arg);
            let mut client = get_rpc_client(arg.server).await?;
            let mut data = Vec::new();
            io::stdin().read_to_end(&mut data).await?;
            let res = client
                .set(SetRequest {
                    data,
                    encode_type: arg.encode_type.into(),
                })
                .await?
                .into_inner();
            log::info!("{:?}", &res);
        }
        Some(SubCommand::QrShow(arg)) => {
            log::info!("{:?}", arg);
            let mut client = get_rpc_client(arg.server).await?;
            let res = client
                .qr_show(QrShowRequest {
                    encode_type: arg.encode_type.into(),
                })
                .await?
                .into_inner();
            log::info!("{:?}", &res);
        }
        Some(SubCommand::QrDecode(arg)) => {
            log::info!("{:?}", arg);
            let mut client = get_rpc_client(arg.server).await?;
            let mut data = Vec::new();
            io::stdin().read_to_end(&mut data).await?;
            let res = client
                .qr_decode(QrDecodeRequest { data })
                .await?
                .into_inner();

            io::stdout().write_all(res.text.as_bytes()).await?;
            log::info!("{}", &res.text);
        }
        Some(SubCommand::QrDecodeLocal(args)) => {
            let text = if args.shot {
                let text = "Only windows support shot".to_string();
                #[cfg(target_os = "windows")]
                let text = {
                    use std::io::Cursor;
                    use xcap::Monitor;
                    Monitor::all()
                        .unwrap()
                        .iter()
                        .filter_map(|monitor| {
                            let img = monitor.capture_image().expect("截图失败");
                            let mut data = Vec::new();
                            img.write_to(&mut Cursor::new(&mut data), image::ImageFormat::Png)
                                .expect("截图编码失败");
                            let text = qr_decode(&data);
                            text.ok()
                        })
                        .collect::<Vec<_>>()
                        .join("\n")
                };
                text
            } else {
                let mut data = Vec::new();
                io::stdin().read_to_end(&mut data).await?;
                qr_decode(&data).unwrap_or("qr decode failed".to_string())
            };
            if !text.is_empty() {
                match arboard::Clipboard::new() {
                    Ok(mut clip) => set_clipboard(&mut clip, text.as_bytes()),
                    Err(e) => log::error!("获取剪贴板失败: {e}"),
                };
            }
            io::stdout().write_all(text.as_bytes()).await?;
            log::info!("{}", text);
        }
        None => {
            if let Some(ref mut displays) = cli.displays
                && displays.is_empty()
            {
                #[cfg(target_os = "windows")]
                displays.push("windows".into());
                #[cfg(target_os = "linux")]
                {
                    use clipboard::{DISPLAY, WAYLAND_DISPLAY};
                    if let Ok(value) = std::env::var(DISPLAY) {
                        displays.push(value);
                    }
                    if let Ok(value) = std::env::var(WAYLAND_DISPLAY) {
                        displays.push(value);
                    }
                }
            }
            println!("displays: {:?}", cli.displays);
            println!("server: {:?}", cli.server);
            println!("connect: {:?}", cli.connect);

            let (tx, rx) = watch::channel(ClipboardData::default());

            // Start server if requested
            let mut handles = vec![];
            if let Some(server_addr) = cli.server {
                let server_rx = rx.clone();
                let server_tx = tx.clone();
                let server_handle = task::spawn(async move {
                    if let Err(e) = server::start_server(
                        server_addr
                            .to_socket_addrs()
                            .expect("Invalid addr: {remote_addr}")
                            .next()
                            .expect("Invalid addr: {remote_addr}"),
                        server_rx,
                        server_tx,
                    )
                    .await
                    {
                        log::error!("Server error: {e}");
                    }
                });
                handles.push(server_handle);
            }

            // Connect to remote server if requested
            if let Some(remote_addr) = cli.connect {
                let client_tx = tx.clone();
                let client_handle = task::spawn(async move {
                    if let Err(e) = client::connect_to_server(remote_addr, client_tx).await {
                        log::error!("Client error: {e}");
                    }
                });
                handles.push(client_handle);
            }

            // Start clipboard monitoring for each display
            let clipboard_handles = cli.displays.into_iter().flatten().filter_map(|display| {
                let tx = tx.clone();
                match clipboard::Clipboard::new(tx, Arc::from(display)) {
                    Ok(clipboard_backend) => Some(task::spawn(async move {
                        tokio::join!(clipboard_backend.listen(), clipboard_backend.subscribe());
                        yield_now().await
                    })),
                    Err(e) => {
                        log::error!("获取剪贴板失败. {e}");
                        None
                    }
                }
            });
            handles.extend(clipboard_handles);
            join_all(handles).await;
        }
    };
    Ok(())
}

pub async fn get_rpc_client(
    server: String,
) -> Result<ClipboardSyncServiceClient<Channel>, tonic::transport::Error> {
    let url = if server.starts_with("http://") {
        server
    } else {
        format!("http://{}", server)
    };
    ClipboardSyncServiceClient::connect(url).await
}
