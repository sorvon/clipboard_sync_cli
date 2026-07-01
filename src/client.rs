use std::sync::Arc;

use futures::StreamExt;
use tokio::sync::{mpsc, watch};

use crate::{
    clipboard::ClipboardData,
    get_rpc_client,
    rpc_server::{SyncRequest, SyncResponse},
};

pub async fn connect_to_server(
    server: String,
    clipboard_sender: watch::Sender<ClipboardData>,
) -> anyhow::Result<()> {
    let addr0: Arc<str> = Arc::from(server.clone());
    let mut client = get_rpc_client(server).await?;
    let (sender, reciver) = mpsc::channel(1);
    let mut clipboard_receiver = clipboard_sender.subscribe();
    let addr: Arc<str> = Arc::clone(&addr0);
    let mut send_task = tokio::spawn(async move {
        let addr: Arc<str> = Arc::from(addr.to_string());
        loop {
            if clipboard_receiver.changed().await.is_err() {
                log::error!("Clipboard receiver closed");
                break;
            }
            log::debug!("{}", clipboard_receiver.borrow_and_update().source);
            log::debug!("{addr}");
            if clipboard_receiver.borrow_and_update().source == addr {
                continue;
            }
            let data = clipboard_receiver.borrow().content.clone();
            match sender.send(SyncRequest { data }).await {
                Ok(_) => log::info!("send success"),
                Err(err) => log::error!("send error {err}"),
            }
        }
    });
    let input_stream = tokio_stream::wrappers::ReceiverStream::new(reciver);
    let mut output_stream = client.sync(input_stream).await?.into_inner();
    let addr: Arc<str> = Arc::clone(&addr0);
    let mut recv_task = tokio::spawn(async move {
        let addr: Arc<str> = Arc::from(addr.to_string());
        while let Some(res) = output_stream.next().await {
            match res {
                Ok(SyncResponse { data }) => {
                    clipboard_sender.send_if_modified(|clip_data| {
                        if clip_data.content != data {
                            log::info!(
                                "同步剪贴板: {:?}",
                                String::from_utf8_lossy(&data)
                                    .chars()
                                    .take(512)
                                    .collect::<String>()
                            );
                            clip_data.content = data;
                            clip_data.source = Arc::clone(&addr);
                            true
                        } else {
                            false
                        }
                    });
                }
                Err(e) => {
                    log::error!("rpc error: {e}");
                    break;
                }
            }
        }
    });
    tokio::select! {
        _ = (&mut send_task) => {
            log::info!("Send task completed");
            recv_task.abort();
        }
        _ = (&mut recv_task) => {
            log::info!("Send task completed");
            send_task.abort();
        }
    }
    Ok(())
}
