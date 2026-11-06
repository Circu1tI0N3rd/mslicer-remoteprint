use std::{
    net::Ipv4Addr,
    time::{Duration, Instant},
};

use anyhow::Result;
use serde_json::Value;

/// Progress of asking a printer for its camera stream (SDCP V3 command 386).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum CameraState {
    #[default]
    Idle,
    /// Asked, waiting for the printer's reply.
    Requested(Instant),
    /// The printer gave a stream address that nothing has opened yet.
    Ready(String),
    /// The stream address was handed out.
    Streaming(String),
    Failed(String),
}

/// Where a request for printer-side data (file list, history) currently stands.
#[derive(Debug, Clone, Default, PartialEq)]
pub enum Remote<T> {
    #[default]
    Idle,
    /// Asked, waiting for the printer's reply.
    Loading(Instant),
    Ready(T),
    Failed(String),
}

impl<T> Remote<T> {
    /// Turns a request the printer never answered into a failure.
    pub fn expire(self, timeout: Duration) -> Self {
        match self {
            Remote::Loading(at) if at.elapsed() > timeout => {
                Remote::Failed("The printer did not answer.".into())
            }
            other => other,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Storage {
    Internal,
    External,
}

/// One directory on the printer, as returned by SDCP command 258.
#[derive(Debug, Clone, PartialEq)]
pub struct FileListing {
    pub path: String,
    pub entries: Vec<FileEntry>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FileEntry {
    /// Full path on the printer, like `/local/part.gcode`.
    pub path: String,
    pub size: u64,
    pub storage: Storage,
    pub is_folder: bool,
}

impl FileEntry {
    pub fn from_json(value: &Value) -> Option<Self> {
        let path = value.get("name")?.as_str()?.to_owned();
        let is_folder = match value.get("type").and_then(as_u64) {
            Some(kind) => kind == 0,
            None => path.ends_with('/'),
        };

        Some(Self {
            size: value.get("usedSize").and_then(as_u64).unwrap_or(0),
            storage: match value.get("storageType").and_then(as_u64) {
                Some(1) => Storage::External,
                _ => Storage::Internal,
            },
            path,
            is_folder,
        })
    }

    /// The last path component.
    pub fn name(&self) -> &str {
        let trimmed = self.path.trim_end_matches('/');
        trimmed.rsplit('/').next().unwrap_or(trimmed)
    }

    pub fn extension(&self) -> Option<&str> {
        let (_, extension) = self.name().rsplit_once('.')?;
        Some(extension)
    }
}

/// The parent of a printer path, or `None` at the root (`/`), which lists the
/// printer's storage volumes.
pub fn parent_path(path: &str) -> Option<String> {
    let trimmed = path.trim_end_matches('/');
    let (parent, _) = trimmed.rsplit_once('/')?;
    Some(format!("{parent}/"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskStatus {
    Other,
    Completed,
    Exceptional,
    Stopped,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimelapseStatus {
    NotShot,
    Available,
    Deleted,
    Generating,
    Failed,
}

/// One finished or aborted print, from SDCP command 321.
#[derive(Debug, Clone, PartialEq)]
pub struct HistoryTask {
    pub id: String,
    pub name: String,
    pub begin_time: i64,
    pub end_time: i64,
    pub status: TaskStatus,
    pub layers_printed: u32,
    pub timelapse: TimelapseStatus,
    /// Checked to point at the printer itself, see `validate_printer_url`.
    pub timelapse_url: Option<String>,
}

impl HistoryTask {
    pub fn from_json(value: &Value, printer: Ipv4Addr) -> Option<Self> {
        let text = |key: &str| value.get(key).and_then(Value::as_str).map(str::to_owned);
        let number = |key: &str| value.get(key).and_then(as_u64);

        let timelapse_url = text("TimeLapseVideoUrl")
            .filter(|url| !url.trim().is_empty())
            .and_then(|url| validate_printer_url(&url, printer).ok());

        Some(Self {
            id: text("TaskId")?,
            name: text("TaskName").unwrap_or_default(),
            begin_time: value.get("BeginTime").and_then(Value::as_i64).unwrap_or(0),
            end_time: value.get("EndTime").and_then(Value::as_i64).unwrap_or(0),
            status: match number("TaskStatus") {
                Some(1) => TaskStatus::Completed,
                Some(2) => TaskStatus::Exceptional,
                Some(3) => TaskStatus::Stopped,
                _ => TaskStatus::Other,
            },
            layers_printed: number("AlreadyPrintLayer").unwrap_or(0) as u32,
            timelapse: match number("TimeLapseVideoStatus") {
                Some(1) => TimelapseStatus::Available,
                Some(2) => TimelapseStatus::Deleted,
                Some(3) => TimelapseStatus::Generating,
                Some(4) => TimelapseStatus::Failed,
                _ => TimelapseStatus::NotShot,
            },
            timelapse_url,
        })
    }

    pub fn has_timelapse(&self) -> bool {
        self.timelapse == TimelapseStatus::Available && self.timelapse_url.is_some()
    }
}

/// Printers are not always consistent about number types.
fn as_u64(value: &Value) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_f64().map(|x| x.max(0.0) as u64))
}

/// Addresses (camera, timelapse) are chosen by the printer and then connected
/// to, so only accept http(s) and rtsp(s) pointing at the printer itself.
/// A bare path (`/media/disk/1.mp4`) is taken to be on the printer's web
/// server, which shares the websocket's port 3030.
pub fn validate_printer_url(url: &str, printer: Ipv4Addr) -> Result<String, String> {
    // File names can hold spaces, which are not valid in a URL.
    let url = url.trim().replace(' ', "%20");
    if url.is_empty() {
        return Err("The printer sent an empty address.".into());
    }
    if url.chars().any(|c| c.is_control() || c.is_whitespace()) {
        return Err("The printer sent an address with unexpected characters.".into());
    }

    if url.starts_with('/') && !url.starts_with("//") {
        return Ok(format!("http://{printer}:3030{url}"));
    }

    let rest = ["http://", "https://", "rtsp://", "rtsps://"]
        .iter()
        .find_map(|scheme| {
            (url.get(..scheme.len()))
                .filter(|prefix| prefix.eq_ignore_ascii_case(scheme))
                .map(|_| &url[scheme.len()..])
        });
    let Some(rest) = rest else {
        return Err("The printer sent an address that is not http(s) or rtsp(s).".into());
    };

    // `user:password@host:port`, the host being whatever follows the last `@`.
    let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
    let host_port = authority.rsplit_once('@').map_or(authority, |(_, x)| x);

    let (host, port) = match host_port.rsplit_once(':') {
        Some((host, port)) => (host, Some(port)),
        None => (host_port, None),
    };
    if port.is_some_and(|port| port.parse::<u16>().is_err()) {
        return Err("The printer sent an address with an invalid port.".into());
    }

    match host.parse::<Ipv4Addr>() {
        Ok(host) if host == printer => Ok(url),
        Ok(_) => Err("The printer sent an address on a different host.".into()),
        Err(_) => Err("The printer sent an address that is not an IPv4 address.".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const PRINTER: Ipv4Addr = Ipv4Addr::new(192, 0, 2, 10);

    #[test]
    fn accepts_the_printer_itself() {
        for url in [
            "http://192.0.2.10:3031/video",
            "HTTP://192.0.2.10:3031/video",
            "http://192.0.2.10/video?x=1",
            "https://192.0.2.10:8080",
            "rtsp://192.0.2.10:554/stream",
            "rtsp://admin:hunter2@192.0.2.10/stream1",
            "RTSPS://192.0.2.10/stream",
        ] {
            assert_eq!(validate_printer_url(url, PRINTER).as_deref(), Ok(url));
        }
    }

    #[test]
    fn rejects_everything_else() {
        for url in [
            "",
            "file:///etc/passwd",
            "javascript:alert(1)",
            "ftp://192.0.2.10/video",
            "http://192.0.2.11:3031/video",
            "rtsp://192.0.2.11/stream",
            "http://example.com/video",
            "http://192.0.2.10.example.com/video",
            "http://192.0.2.10@example.com/video",
            "rtsp://admin:pw@example.com/192.0.2.10",
            "http://192.0.2.10:3031@example.com/",
            "http://192.0.2.10:notaport/video",
            "http://192.0.2.10:99999/video",
            "http://192.0.2.10:3031/vid\teo",
            "http://192.0.2.10:3031/vid\neo",
            "http://[::1]:3031/video",
            "//example.com/video.mp4",
        ] {
            assert!(validate_printer_url(url, PRINTER).is_err(), "{url:?}");
        }
    }

    #[test]
    fn trims_whitespace_and_encodes_spaces() {
        let url = " http://192.0.2.10:3031/video\n";
        let expected = "http://192.0.2.10:3031/video";
        assert_eq!(validate_printer_url(url, PRINTER).as_deref(), Ok(expected));

        assert_eq!(
            validate_printer_url("/local/my print.mp4", PRINTER).as_deref(),
            Ok("http://192.0.2.10:3030/local/my%20print.mp4")
        );
    }

    #[test]
    fn parses_file_entries() {
        let file = json!({"name": "/local/a b.gcode", "usedSize": 1234, "totalSize": 9, "storageType": 0, "type": 1});
        let file = FileEntry::from_json(&file).unwrap();
        assert!(!file.is_folder);
        assert_eq!(
            (file.name(), file.extension(), file.size),
            ("a b.gcode", Some("gcode"), 1234)
        );

        let folder = json!({"name": "/usb/prints/", "usedSize": 1.5, "storageType": 1, "type": 0});
        let folder = FileEntry::from_json(&folder).unwrap();
        assert!(folder.is_folder);
        assert_eq!(
            (folder.name(), folder.storage),
            ("prints", Storage::External)
        );

        assert!(FileEntry::from_json(&json!({"usedSize": 1})).is_none());
    }

    #[test]
    fn walks_up_paths() {
        assert_eq!(parent_path("/usb/a/b/").as_deref(), Some("/usb/a/"));
        assert_eq!(parent_path("/usb/a").as_deref(), Some("/usb/"));
        // Storage volumes are one level below the root.
        assert_eq!(parent_path("/usb/").as_deref(), Some("/"));
        assert_eq!(parent_path("/media/mmcblk0p3").as_deref(), Some("/media/"));
        assert_eq!(parent_path("/media/").as_deref(), Some("/"));
        assert_eq!(parent_path("/"), None);
    }

    #[test]
    fn parses_history() {
        let value = json!({
            "TaskId": "abc", "TaskName": "/local/x.gcode", "BeginTime": 10, "EndTime": 20,
            "TaskStatus": 1, "AlreadyPrintLayer": 7, "TimeLapseVideoStatus": 1,
            "TimeLapseVideoUrl": "http://192.0.2.10/video.mp4", "SliceInformation": {}
        });
        let task = HistoryTask::from_json(&value, PRINTER).unwrap();
        assert_eq!(task.status, TaskStatus::Completed);
        assert!(task.has_timelapse());

        // An address elsewhere is dropped rather than followed.
        let value = json!({"TaskId": "abc", "TimeLapseVideoStatus": 1, "TimeLapseVideoUrl": "http://evil.example/v.mp4"});
        let task = HistoryTask::from_json(&value, PRINTER).unwrap();
        assert!(!task.has_timelapse());

        assert!(HistoryTask::from_json(&json!({"TaskName": "no id"}), PRINTER).is_none());
    }

    /// Entries as a resin printer really sends them: the timelapse is a bare
    /// path on the printer's web port, and failed ones have no address.
    #[test]
    fn parses_real_history_entries() {
        let printer = Ipv4Addr::new(192, 0, 2, 10);
        let entry = |status: u8, url: &str| {
            json!({
                "Thumbnail": "http://192.0.2.10:3030/media/mmcblk0p1/history_image/a.bmp",
                "TaskName": "/media/mmcblk0p3/part.goo", "BeginTime": 1792918483,
                "EndTime": 1792931836, "TaskStatus": 1, "AlreadyPrintLayer": 1194,
                "SliceInformation": {"resolution_x": 8520, "layer_height": 0.05000000074505806, "volume": 0.5},
                "TaskId": "00000000-0000-4000-8000-000000000001", "MD5": "00000000",
                "CurrentLayerTalVolume": 0, "TimeLapseVideoStatus": status,
                "TimeLapseVideoUrl": url, "ErrorStatusReason": 0
            })
        };

        let task =
            HistoryTask::from_json(&entry(1, "/media/mmcblk0p2/local_1000.mp4"), printer).unwrap();
        assert!(task.has_timelapse());
        assert_eq!(
            task.timelapse_url.as_deref(),
            Some("http://192.0.2.10:3030/media/mmcblk0p2/local_1000.mp4")
        );

        let failed = HistoryTask::from_json(&entry(4, ""), printer).unwrap();
        assert!(!failed.has_timelapse());
        assert_eq!(failed.timelapse, TimelapseStatus::Failed);

        // Failed, yet with an address left behind: not offered to play.
        let stale =
            HistoryTask::from_json(&entry(4, "/media/mmcblk0p2/local_17406004.mp4"), printer)
                .unwrap();
        assert!(!stale.has_timelapse());
    }
}
