use anyhow::anyhow;
use arboard::ImageData;
use image::{ImageReader, RgbaImage};
use serde::{Deserialize, Serialize};
use std::{io::Cursor, sync::Arc, time::Duration};
use tokio::{
    sync::{Mutex, watch::Sender},
    time::sleep,
};

#[cfg(target_os = "linux")]
pub const DISPLAY: &str = "DISPLAY";
#[cfg(target_os = "linux")]
pub const WAYLAND_DISPLAY: &str = "WAYLAND_DISPLAY";

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ClipboardData {
    pub content: Vec<u8>,
    pub source: Arc<str>,
}

impl PartialEq for ClipboardData {
    fn eq(&self, other: &Self) -> bool {
        self.content == other.content
    }
}

pub struct Clipboard {
    clipboard: Arc<Mutex<arboard::Clipboard>>,
    tx: Sender<ClipboardData>,
    display: Arc<str>,
}

impl Clipboard {
    pub fn new(tx: Sender<ClipboardData>, display: Arc<str>) -> anyhow::Result<Self> {
        #[cfg(target_os = "linux")]
        set_display_env(&display);
        Ok(Self {
            clipboard: Arc::new(Mutex::new(arboard::Clipboard::new()?)),
            tx,
            display,
        })
    }

    pub async fn listen(&self) {
        log::info!("start listen {}", self.display);
        let mut is_changed = true;
        loop {
            sleep(Duration::from_millis(300)).await;
            if let Ok(mut clip) = self.clipboard.try_lock() {
                let cur_data = match get_clipboard(&mut clip) {
                    Ok(data) => data,
                    Err(e) => {
                        if is_changed {
                            log::error!("{e}");
                            is_changed = false;
                        }
                        continue;
                    }
                };
                is_changed = self.tx.send_if_modified(|data| {
                    if cur_data != data.content {
                        log::debug!("剪贴板变更, 数据大小={} bytes", cur_data.len());
                        data.content = cur_data;
                        data.source = self.display.clone();
                        return true;
                    }
                    false
                });
            } else {
                log::warn!("listen 剪贴板被占用")
            }
        }
    }
    pub async fn subscribe(&self) {
        log::info!("start subscribe {}", self.display);
        let mut rx = self.tx.subscribe();
        loop {
            if rx.changed().await.is_err() {
                log::error!("剪贴板 watch失败display={}", self.display);
            }
            let mut clip = self.clipboard.lock().await;
            let data = rx.borrow_and_update();
            if data.source != self.display {
                set_clipboard(&mut clip, &data.content);
            }
        }
    }
}

pub fn set_clipboard(clip: &mut arboard::Clipboard, data: &[u8]) {
    if let Ok(text) = str::from_utf8(data) {
        match clip.set_text(text) {
            Ok(_) => log::debug!("剪贴板set_text成功"),
            Err(e) => log::error!("剪贴板set_text失败. {e}"),
        }
    } else if let Ok(img_reader) = ImageReader::new(Cursor::new(data)).with_guessed_format()
        && let Ok(img) = img_reader.decode()
    {
        let clip_img = ImageData {
            width: img.width() as usize,
            height: img.height() as usize,
            bytes: std::borrow::Cow::Borrowed(img.as_bytes()),
        };
        match clip.set_image(clip_img) {
            Ok(_) => log::debug!("剪贴板set_image成功"),
            Err(e) => log::error!("剪贴板set_image失败. {e}"),
        }
    } else {
        log::error!("subscribe 剪贴板数据不为utf-8字符串或图片");
    }
}

pub fn get_clipboard(clip: &mut arboard::Clipboard) -> Result<Vec<u8>, anyhow::Error> {
    let data = if let Ok(text) = clip.get_text() {
        text.into_bytes()
    } else if let Ok(img) = clip.get_image()
        && let Some(img) =
            RgbaImage::from_raw(img.width as u32, img.height as u32, img.bytes.to_vec())
    {
        let mut buffer = vec![];
        match img.write_to(&mut Cursor::new(&mut buffer), image::ImageFormat::Png) {
            Ok(_) => {}
            Err(e) => {
                return Err(anyhow!("剪贴板变更失败, 图片编码失败. {e}"));
            }
        }
        buffer
    } else {
        return Err(anyhow!("剪贴板变更, 非utf-8字符或图片"));
    };
    Ok(data)
}

#[cfg(target_os = "linux")]
fn set_display_env(display: &str) {
    match display.contains("wayland") {
        true => unsafe {
            std::env::set_var(WAYLAND_DISPLAY, display);
        },
        false => unsafe {
            std::env::set_var(DISPLAY, display);
        },
    }
}
