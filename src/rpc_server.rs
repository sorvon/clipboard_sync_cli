pub mod proto {
    tonic::include_proto!("clipboard.v1");
}

use std::{
    env,
    io::{Cursor, Write},
    pin::Pin,
    sync::Arc,
};

use crate::server::ServerState;
use anyhow::anyhow;
use base64::{Engine, engine::general_purpose};
use clipboard_sync_service_server::ClipboardSyncService;
pub use clipboard_sync_service_server::ClipboardSyncServiceServer;
use futures::StreamExt;
pub use proto::*;
use qrcode::QrCode;
use tokio::sync::mpsc;
use tokio_stream::Stream;
use uuid::Uuid;

pub struct ClipboardSyncServer {
    state: Arc<ServerState>,
}

pub fn qr_decode(data: &[u8]) -> Result<String, anyhow::Error> {
    let data = image::ImageReader::new(Cursor::new(&data))
        .with_guessed_format()?
        .decode()
        .map_err(ErrorWarp::from)?
        .to_luma8();
    let mut img = rqrr::PreparedImage::prepare(data);
    let text = img
        .detect_grids()
        .iter()
        .filter_map(|grid| {
            let mut buf = Cursor::new(Vec::new());
            match grid.decode_to(&mut buf) {
                Ok(_) => Some(decode_attempt_all(buf.get_ref())),
                Err(_) => None,
            }
        })
        .collect::<Vec<String>>()
        .join("\n");
    log::debug!("qr_decode结果: {text}");
    match !text.is_empty() {
        true => Ok(text),
        false => Err(anyhow!("qr_decode结果为空")),
    }
}

impl ClipboardSyncServer {
    pub fn new(state: Arc<ServerState>) -> Self {
        Self { state }
    }
}

// type SyncStreamW = Pin<Box<dyn Stream<Item = Result<SyncResponse, tonic::Status>> + Send>>;
#[tonic::async_trait]
impl ClipboardSyncService for ClipboardSyncServer {
    async fn get(
        &self,
        request: tonic::Request<proto::GetRequest>,
    ) -> std::result::Result<tonic::Response<GetResponse>, tonic::Status> {
        let req = request.into_inner();
        log::info!("{:?}", req);
        let data = self.state.rx.borrow();
        let data = encode_by_type(&req.encode_type(), &data.content)?;
        log::info!(
            "clipboard get text = [{}]",
            str::from_utf8(&data).unwrap_or("Invaild utf-8 string")
        );
        let res = tonic::Response::new(GetResponse { data });
        Ok(res)
    }
    async fn set(
        &self,
        request: tonic::Request<SetRequest>,
    ) -> std::result::Result<tonic::Response<SetResponse>, tonic::Status> {
        let req = request.into_inner();
        log::info!("{:?}", req);
        let content = decode_by_type(&req.encode_type(), &req.data)?;
        self.state.tx.send_if_modified(move |data| {
            if data.content != content {
                data.content = content;
                data.source = "rpc".into();
                true
            } else {
                false
            }
        });
        let res = tonic::Response::new(SetResponse {});
        log::info!("{:?}", res);
        Ok(res)
    }
    async fn qr_show(
        &self,
        request: tonic::Request<QrShowRequest>,
    ) -> std::result::Result<tonic::Response<QrShowResponse>, tonic::Status> {
        let req = request.into_inner();
        log::info!("{:?}", req);
        let data = self.state.rx.borrow();
        let data = encode_by_type(&req.encode_type(), &data.content)?;
        let code = QrCode::new(&data).map_err(ErrorWarp::from)?;
        let image = code.render::<image::Rgb<u8>>().build();
        let img_path = env::temp_dir().join("clipboard_qrcode.png");
        image.save(&img_path).map_err(ErrorWarp::from)?;
        open::that_detached(&img_path).map_err(ErrorWarp::from)?;
        let res = tonic::Response::new(QrShowResponse {});
        log::info!("{:?}", res);
        Ok(res)
    }
    async fn qr_decode(
        &self,
        request: tonic::Request<QrDecodeRequest>,
    ) -> std::result::Result<tonic::Response<QrDecodeResponse>, tonic::Status> {
        let req = request.into_inner();
        log::info!("{:?}", req);
        let text = qr_decode(&req.data).map_err(ErrorWarp::from)?;
        self.state.tx.send_if_modified(|data| {
            let content = text.as_bytes();
            if data.content != content {
                data.content = content.into();
                data.source = "text".into();
                true
            } else {
                false
            }
        });
        let res = tonic::Response::new(QrDecodeResponse { text });
        log::info!("{:?}", res);
        Ok(res)
    }
    type SyncStream = Pin<Box<dyn Stream<Item = Result<SyncResponse, tonic::Status>> + Send>>;
    async fn sync(
        &self,
        request: tonic::Request<tonic::Streaming<SyncRequest>>,
    ) -> std::result::Result<tonic::Response<Self::SyncStream>, tonic::Status> {
        let addr = request.remote_addr().map_or_else(
            || Arc::from(format!("rpc_{}", Uuid::new_v4())),
            |addr| Arc::from(addr.to_string()),
        );
        let addr2 = Arc::clone(&addr);
        let mut req = request.into_inner();
        log::info!("{:?}", req);

        let tx = self.state.tx.clone();
        tokio::spawn(async move {
            log::info!("rpc server sync recv start.");
            while let Some(req) = req.next().await {
                match req {
                    Ok(SyncRequest { data }) => {
                        tx.send_if_modified(|clip_data| {
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
                        log::error!("同步流被关闭: {e}");
                        break;
                    }
                }
            }
            log::info!("rpc server sync recv end.");
        });
        let mut rx = self.state.rx.clone();
        let (sender, reciver) = mpsc::channel(1);
        tokio::spawn(async move {
            log::info!("rpc server sync send start.");
            loop {
                if rx.changed().await.is_err() {
                    log::error!("Clipboard receiver closed");
                    let _ = sender
                        .send(Err(tonic::Status::internal("Clipboard receiver closed")))
                        .await;
                    break;
                }
                if rx.borrow_and_update().source == addr2 {
                    continue;
                }
                let data = rx.borrow().content.clone();
                match sender.send(Ok(SyncResponse { data })).await {
                    Ok(_) => log::info!("send success"),
                    Err(err) => {
                        log::error!("send error {err}");
                        break;
                    }
                }
            }
            log::info!("rpc server sync send end.");
        });
        let output_stream = tokio_stream::wrappers::ReceiverStream::new(reciver);
        let res = tonic::Response::new(Box::pin(output_stream) as Self::SyncStream);
        Ok(res)
    }
    async fn shutdown(
        &self,
        request: tonic::Request<ShutdownRequest>,
    ) -> std::result::Result<tonic::Response<ShutdownResponse>, tonic::Status> {
        let req = request.into_inner();
        log::info!("{:?}", req);
        tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            std::process::exit(0);
        });
        let res = tonic::Response::new(ShutdownResponse {});
        log::info!("{:?}", res);
        Ok(res)
    }
}

fn encode_by_type(encode_type: &EncodeType, data: &[u8]) -> Result<Vec<u8>, tonic::Status> {
    let data = match &encode_type {
        EncodeType::Unspecified => data.to_vec(),
        EncodeType::Utf8 => str::from_utf8(data)
            .map_err(ErrorWarp::from)?
            .as_bytes()
            .to_vec(),
        EncodeType::Base64 => general_purpose::STANDARD.encode(data).into_bytes(),
        EncodeType::Zstd => return Err(tonic::Status::unimplemented("zstd暂未实现")),
        EncodeType::Gz => {
            let mut encoder =
                flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::best());
            encoder.write_all(data)?;
            encoder.finish()?
        }
    };
    Ok(data)
}

fn decode_by_type(encode_type: &EncodeType, data: &[u8]) -> Result<Vec<u8>, tonic::Status> {
    let data = match &encode_type {
        EncodeType::Unspecified => data.to_vec(),
        EncodeType::Utf8 => str::from_utf8(data)
            .map_err(ErrorWarp::from)?
            .as_bytes()
            .to_vec(),
        EncodeType::Base64 => general_purpose::STANDARD
            .decode(data)
            .map_err(ErrorWarp::from)?,
        EncodeType::Zstd => return Err(tonic::Status::unimplemented("zstd暂未实现")),
        EncodeType::Gz => {
            let mut decoder = flate2::write::GzDecoder::new(Vec::new());
            decoder.write_all(data)?;
            decoder.finish()?
        }
    };
    Ok(data)
}

fn decode_attempt_all(data: &[u8]) -> String {
    if let Ok(data) = decode_by_type(&EncodeType::Utf8, data) {
        log::info!("尝试utf8解码成功");
        String::from_utf8(data).unwrap_or("utf8解码异常".into())
    } else if let Ok(data) = decode_by_type(&EncodeType::Base64, data) {
        log::info!("尝试Base64解码成功");
        String::from_utf8(data).unwrap_or("Base64解码成功后不为utf8字符串".into())
    } else if let Ok(data) = decode_by_type(&EncodeType::Gz, data) {
        log::info!("尝试Gz解码成功");
        String::from_utf8(data).unwrap_or("Gz解码成功后不为utf8字符串".into())
    } else {
        log::info!("尝试解码全部失败, 返回utf8_lossy");
        String::from_utf8_lossy(data).into_owned()
    }
}

#[derive(thiserror::Error, Debug)]
enum ErrorWarp {
    #[error("IO错误: {0}")]
    Io(#[from] std::io::Error),
    #[error("UTF8转换失败: {0}")]
    FromUtf8Error(#[from] std::string::FromUtf8Error),
    #[error("UTF8转换失败: {0}")]
    Utf8Error(#[from] std::str::Utf8Error),
    #[error("BASE64解码失败: {0}")]
    Base64DecodeError(#[from] base64::DecodeError),
    #[error("qr编码失败: {0}")]
    QrError(#[from] qrcode::types::QrError),
    #[error("图片保存失败: {0}")]
    ImageError(#[from] image::ImageError),
    #[error("内部错误: {0}")]
    Anyhow(#[from] anyhow::Error),
}

impl From<ErrorWarp> for tonic::Status {
    fn from(value: ErrorWarp) -> Self {
        match value {
            ErrorWarp::Anyhow(error) => tonic::Status::internal(format!("{error}")),
            ErrorWarp::FromUtf8Error(error) => tonic::Status::invalid_argument(format!("{error}")),
            ErrorWarp::Utf8Error(error) => tonic::Status::invalid_argument(format!("{error}")),
            ErrorWarp::Base64DecodeError(error) => {
                tonic::Status::invalid_argument(format!("{error}"))
            }
            ErrorWarp::QrError(error) => tonic::Status::internal(format!("{error}")),
            ErrorWarp::ImageError(error) => tonic::Status::internal(format!("{error}")),
            ErrorWarp::Io(error) => tonic::Status::internal(format!("{error}")),
        }
    }
}
