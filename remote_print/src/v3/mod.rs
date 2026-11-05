use std::{
    collections::HashMap,
    io::ErrorKind,
    mem,
    net::{Ipv4Addr, SocketAddr, TcpStream},
    sync::{
        Arc,
        mpsc::{self, Sender},
    },
    thread,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use clone_macro::clone;
use parking_lot::{Mutex, MutexGuard};
use rand::{Rng, rng};
use tracing::{info, trace, warn};
use tungstenite::Error;
use ureq::unversioned::multipart::{Form, Part};

use crate::{
    manager,
    shared::{
        CameraState, FileEntry, FileListing, FileTransferInfo, FileTransferStatus, HistoryTask,
        Remote, Response, WEB_PORT, epoch, validate_printer_url,
    },
    v3::{
        commands::{
            BATCH_DELETE_FILES, Cmd, RETRIEVE_FILE_LIST, RETRIEVE_HISTORY, RETRIEVE_TASK_DETAILS,
            SET_VIDEO_STREAM, START_PRINTING, send_command,
        },
        status::{
            AckData, Attributes, DiscoveryResponse, ErrorMessage, Message, NoticeMessage,
            ResponseMessage, Status, UploadResponse,
        },
    },
};

pub mod commands;
pub mod status;

type PrintCompletion = Arc<Mutex<Box<dyn FnMut(&manager::Client) + Send>>>;

pub struct RemotePrintV3 {
    clients: Arc<Mutex<HashMap<String, Client>>>,
    print_completion: PrintCompletion,
}

pub struct Client {
    pub attributes: Option<Attributes>,
    pub status: Option<Status>,
    pub transfer_info: FileTransferInfo,

    pub ip: Ipv4Addr,
    pub last_update: i64,
    pub pending_removal: bool,
    pub sender: Sender<Cmd>,
    pub camera: CameraState,

    /// The directory last asked for, and what the printer said about it.
    pub files_path: String,
    pub files: Remote<FileListing>,
    pub history: Remote<Vec<HistoryTask>>,
    /// The most recent thing the printer refused, for the user to read.
    pub last_error: Option<String>,

    // History arrives as a list of ids, then details for them in batches.
    history_pending: Vec<Vec<String>>,
    history_found: Vec<HistoryTask>,
    was_printing: bool,
}

/// How long to wait for the printer to answer a file or history request.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Task ids asked for per details request.
const HISTORY_BATCH: usize = 25;

impl RemotePrintV3 {
    pub(crate) fn new(
        print_completion: impl FnMut(&manager::Client) + Send + Sync + 'static,
    ) -> Self {
        Self {
            clients: Arc::new(Mutex::new(HashMap::new())),
            print_completion: Arc::new(Mutex::new(Box::new(print_completion))),
        }
    }

    pub(crate) fn connect_printer(&self, response: Response<DiscoveryResponse>) -> Result<()> {
        let machine_name = &response.data.machine_name;
        let mainboard_id = response.data.mainboard_id.clone();
        info!("Got status from `{machine_name}`",);

        if self.clients.lock().contains_key(&mainboard_id) {
            warn!("Printer `{mainboard_id}` already connected.",);
            return Ok(());
        }

        let ip = response.data.mainboard_ip.parse::<Ipv4Addr>().unwrap();
        let addr = SocketAddr::new(ip.into(), WEB_PORT);
        let stream = TcpStream::connect_timeout(&addr, Duration::from_secs(5))?;

        // Needed for elegoo-homeassistant proxy support
        let url = if response.data.proxy {
            format!("ws://{ip}:{WEB_PORT}/websocket?id={mainboard_id}")
        } else {
            format!("ws://{ip}:{WEB_PORT}/websocket")
        };

        let (mut socket, rsp) = tungstenite::client(url, stream)?;

        // note that the socket must only be configured after the websocket
        // handshake as tungstenite assumes blocking sockets. this was not fun
        // to figure out :sob:
        socket
            .get_ref()
            .set_read_timeout(Some(Duration::from_secs(1)))?;
        trace!("Websocket connected: {rsp:?}");

        let (tx, rx) = mpsc::channel();
        tx.send(Cmd::RefreshStatus).unwrap();
        tx.send(Cmd::RefreshAttributes).unwrap();

        self.clients
            .lock()
            .insert(mainboard_id.clone(), Client::new(ip, tx));

        thread::spawn(clone!(
            [{ self.clients } as clients, { self.print_completion }
                as print_completion],
            move || {
                loop {
                    while let Ok(command) = rx.try_recv() {
                        send_command(&mut socket, &mainboard_id, command);
                    }

                    {
                        let mut clients = clients.lock();
                        if clients
                            .get(&mainboard_id)
                            .map(|x| x.pending_removal)
                            .unwrap_or_default()
                        {
                            clients.remove(&mainboard_id);
                            socket.close(None).unwrap();
                        }
                    }

                    match socket.read() {
                        Ok(message) => {
                            let text = match message.to_text() {
                                Ok(x) => x,
                                Err(e) => {
                                    warn!("Failed to convert message to text: {e:?}");
                                    continue;
                                }
                            };

                            // Raw dump, enabled with the `sdcp::ws` log target.
                            trace!(target: "sdcp::ws", "<- {mainboard_id} {text}");

                            // Ignore heartbeat msgs
                            if matches!(text, "ping" | "pong") {
                                continue;
                            }

                            if let Ok(message) = serde_json::from_str::<Message>(text) {
                                trace!("message: {message:?}");
                                let mut clients = clients.lock();
                                let client = clients.get_mut(&mainboard_id).unwrap();

                                if let Some(attributes) = message.attributes {
                                    trace!("Got attributes");
                                    client.attributes = Some(attributes);
                                }

                                if let Some(status) = message.status {
                                    trace!("Got status");
                                    let is_printing = status.print_info.status.is_printing();
                                    client.status = Some(status);

                                    if mem::replace(&mut client.was_printing, is_printing)
                                        && !is_printing
                                        && let Some(client) = manager::Client::from_v3(client)
                                    {
                                        print_completion.lock()(&client)
                                    }
                                }
                            } else if let Ok(response) =
                                serde_json::from_str::<ResponseMessage>(text)
                            {
                                let (cmd, ack) = (response.data.cmd, response.data.data.ack);
                                if ack != 0 {
                                    warn!(
                                        "Printer `{mainboard_id}` rejected command {cmd} with {ack}"
                                    );
                                } else {
                                    trace!("Printer `{mainboard_id}` acknowledged command {cmd}");
                                }

                                if let Some(client) = clients.lock().get_mut(&mainboard_id) {
                                    client.on_response(cmd, &response.data.data);
                                }
                            } else if let Ok(error) = serde_json::from_str::<ErrorMessage>(text) {
                                warn!(
                                    "Printer `{mainboard_id}` reported error {:?}",
                                    error.data.data.error_code
                                );
                            } else if let Ok(notice) = serde_json::from_str::<NoticeMessage>(text) {
                                info!(
                                    "Printer `{mainboard_id}` notice (type {}): {:?}",
                                    notice.data.data.notice_type, notice.data.data.message
                                );
                            } else {
                                warn!("Failed to deserialize message: {text:?}");
                            }

                            if let Some(client) = clients.lock().get_mut(&mainboard_id) {
                                client.last_update = epoch();
                            }
                        }
                        Err(Error::Io(e))
                            if matches!(e.kind(), ErrorKind::TimedOut | ErrorKind::WouldBlock) =>
                        {
                            continue;
                        }
                        Err(Error::ConnectionClosed) => {
                            trace!("Connection to `{mainboard_id}` closed.");
                            break;
                        }
                        Err(e) => warn!("Socket error for `{mainboard_id}`: {e:?}"),
                    }
                }
            }
        ));

        Ok(())
    }

    pub fn clients(&self) -> MutexGuard<'_, HashMap<String, Client>> {
        self.clients.lock()
    }

    pub fn remove_printer(&self, mainboard: &str) -> Result<()> {
        self.clients.lock().remove(mainboard);
        Ok(())
    }

    pub fn upload(&self, mainboard: &str, data: Arc<Vec<u8>>, filename: String) -> Result<()> {
        let mut clients = self.clients.lock();
        let client = clients.get_mut(mainboard).unwrap();
        client.transfer_info = FileTransferInfo {
            status: FileTransferStatus::None,
            download_offset: 0,
            check_offset: 0,
            file_total_size: data.len() as u32,
            filename: filename.clone(),
        };
        let ip = client.ip;
        drop(clients);

        let mainboard = mainboard.to_owned();
        thread::spawn(clone!([{ self.clients } as clients], move || {
            let result = upload_chunks(&clients, &mainboard, ip, &data, &filename);

            let mut clients = clients.lock();
            if let Some(client) = clients.get_mut(&mainboard) {
                client.transfer_info.status =
                    [FileTransferStatus::Error, FileTransferStatus::Done][result.is_ok() as usize];
            }
        }));

        Ok(())
    }

    pub fn print(&self, mainboard: &str, filename: &str) -> Result<()> {
        let cmd = Cmd::StartPrinting {
            filename: filename.to_owned(),
            start_layer: 0,
        };
        self.send_cmd(mainboard, cmd)
    }

    pub fn pause_print(&self, mainboard: &str) -> Result<()> {
        self.send_cmd(mainboard, Cmd::PausePrinting)
    }

    pub fn resume_print(&self, mainboard: &str) -> Result<()> {
        self.send_cmd(mainboard, Cmd::ResumePrinting)
    }

    pub fn stop_print(&self, mainboard: &str) -> Result<()> {
        self.send_cmd(mainboard, Cmd::StopPrinting)
    }

    pub fn camera_start(&self, mainboard: &str) -> Result<()> {
        let mut clients = self.clients();
        let client = clients
            .get_mut(mainboard)
            .with_context(|| format!("Printer `{mainboard}` is not connected."))?;

        client.camera = CameraState::Requested(Instant::now());
        client
            .sender
            .send(Cmd::SetVideoStream { enable: true })
            .unwrap();
        Ok(())
    }

    /// Switches the stream off so the printer's stream slot is freed.
    pub fn camera_stop(&self, mainboard: &str) -> Result<()> {
        let mut clients = self.clients();
        let client = clients
            .get_mut(mainboard)
            .with_context(|| format!("Printer `{mainboard}` is not connected."))?;

        client.camera = CameraState::Idle;
        client
            .sender
            .send(Cmd::SetVideoStream { enable: false })
            .unwrap();
        Ok(())
    }

    /// Hands out a new stream address exactly once, for the UI to open.
    pub fn take_camera_url(&self, mainboard: &str) -> Option<String> {
        let mut clients = self.clients();
        let client = clients.get_mut(mainboard)?;

        let CameraState::Ready(url) = &client.camera else {
            return None;
        };
        let url = url.clone();
        client.camera = CameraState::Streaming(url.clone());
        Some(url)
    }

    /// Asks for the contents of a directory like `/local/` or `/usb/`.
    pub fn list_files(&self, mainboard: &str, path: &str) -> Result<()> {
        let mut clients = self.clients();
        let client = (clients.get_mut(mainboard))
            .with_context(|| format!("Printer `{mainboard}` is not connected."))?;
        client.request_files(path);
        Ok(())
    }

    /// Deletes files and folders (full printer paths), then re-lists the
    /// directory that is open.
    pub fn delete_files(
        &self,
        mainboard: &str,
        files: Vec<String>,
        folders: Vec<String>,
    ) -> Result<()> {
        self.send_cmd(mainboard, Cmd::DeleteFiles { files, folders })
    }

    pub fn list_history(&self, mainboard: &str) -> Result<()> {
        let mut clients = self.clients();
        let client = (clients.get_mut(mainboard))
            .with_context(|| format!("Printer `{mainboard}` is not connected."))?;

        client.history = Remote::Loading(Instant::now());
        client.history_pending.clear();
        client.history_found.clear();
        client.sender.send(Cmd::ListHistory).unwrap();
        Ok(())
    }

    pub fn file_listing(&self, mainboard: &str) -> Option<Remote<FileListing>> {
        let clients = self.clients();
        let client = clients.get(mainboard)?;
        Some(client.files.clone().expire(REQUEST_TIMEOUT))
    }

    pub fn history(&self, mainboard: &str) -> Option<Remote<Vec<HistoryTask>>> {
        let clients = self.clients();
        let client = clients.get(mainboard)?;
        Some(client.history.clone().expire(REQUEST_TIMEOUT))
    }

    pub fn files_path(&self, mainboard: &str) -> Option<String> {
        Some(self.clients().get(mainboard)?.files_path.clone())
    }

    /// Asks the printer for its attributes again, which carry how many video
    /// streams are in use.
    pub fn refresh_attributes(&self, mainboard: &str) -> Result<()> {
        self.send_cmd(mainboard, Cmd::RefreshAttributes)
    }

    pub fn clear_error(&self, mainboard: &str) {
        if let Some(client) = self.clients().get_mut(mainboard) {
            client.last_error = None;
        }
    }

    fn send_cmd(&self, mainboard: &str, cmd: Cmd) -> Result<()> {
        let clients = self.clients();
        let client = clients
            .get(mainboard)
            .with_context(|| format!("Printer `{mainboard}` is not connected."))?;
        client.sender.send(cmd).unwrap();
        Ok(())
    }
}

impl Client {
    fn new(ip: Ipv4Addr, sender: Sender<Cmd>) -> Self {
        Self {
            attributes: None,
            status: None,
            transfer_info: FileTransferInfo {
                status: FileTransferStatus::None,
                download_offset: 0,
                check_offset: 0,
                file_total_size: 0,
                filename: "".into(),
            },

            ip,
            last_update: 0,
            pending_removal: false,
            sender,
            camera: CameraState::Idle,
            files_path: "/".into(),
            files: Remote::Idle,
            history: Remote::Idle,
            last_error: None,
            history_pending: Vec::new(),
            history_found: Vec::new(),
            was_printing: false,
        }
    }

    fn request_files(&mut self, path: &str) {
        self.files_path = path.to_owned();
        self.files = Remote::Loading(Instant::now());
        let _ = self.sender.send(Cmd::ListFiles {
            url: path.to_owned(),
        });
    }

    /// Routes a command reply to whoever asked.
    fn on_response(&mut self, cmd: u16, data: &AckData) {
        match cmd {
            SET_VIDEO_STREAM => self.on_camera_response(data),
            RETRIEVE_FILE_LIST => self.on_file_list(data),
            BATCH_DELETE_FILES => self.on_delete(data),
            RETRIEVE_HISTORY => self.on_history_ids(data),
            RETRIEVE_TASK_DETAILS => self.on_history_details(data),
            START_PRINTING if data.ack != 0 => {
                let reason = match data.ack {
                    1 => "the printer is busy",
                    2 => "the file was not found",
                    3 => "the file failed its MD5 check",
                    4 => "the printer could not read the file",
                    5 => "the file's resolution does not match the printer",
                    6 => "the printer does not know this file format",
                    7 => "the file is for a different printer model",
                    _ => "the printer refused",
                };
                self.last_error = Some(format!("Could not start the print: {reason}."));
            }
            _ => {}
        }
    }

    fn on_file_list(&mut self, data: &AckData) {
        if !matches!(self.files, Remote::Loading(_)) {
            return;
        }

        self.files = if data.ack != 0 {
            Remote::Failed(format!("The printer refused to list files ({}).", data.ack))
        } else {
            let mut entries = (data.file_list.iter().flatten())
                .filter_map(FileEntry::from_json)
                .collect::<Vec<_>>();
            // Folders first, then by name.
            entries.sort_by_cached_key(|x| (!x.is_folder, x.name().to_lowercase()));
            Remote::Ready(FileListing {
                path: self.files_path.clone(),
                entries,
            })
        };
    }

    fn on_delete(&mut self, data: &AckData) {
        let failed = (data.err_data.iter().flatten())
            .filter_map(|x| x.as_str())
            .collect::<Vec<_>>();
        if data.ack != 0 {
            self.last_error = Some(format!("The printer refused to delete ({}).", data.ack));
        } else if !failed.is_empty() {
            self.last_error = Some(format!("Could not delete: {}.", failed.join(", ")));
        }

        // Show what is left either way.
        let path = self.files_path.clone();
        self.request_files(&path);
    }

    fn on_history_ids(&mut self, data: &AckData) {
        if !matches!(self.history, Remote::Loading(_)) {
            return;
        }

        let ids = (data.history_data.iter().flatten())
            .filter_map(|x| x.as_str().map(str::to_owned))
            .collect::<Vec<_>>();
        if data.ack != 0 {
            self.history = Remote::Failed(format!("The printer refused ({}).", data.ack));
        } else if ids.is_empty() {
            self.history = Remote::Ready(Vec::new());
        } else {
            // Reversed so `pop` hands out the batches in order.
            self.history_pending = ids.chunks(HISTORY_BATCH).rev().map(<[_]>::to_vec).collect();
            self.history_found.clear();
            self.request_next_history_batch();
        }
    }

    fn on_history_details(&mut self, data: &AckData) {
        if !matches!(self.history, Remote::Loading(_)) {
            return;
        }

        if data.ack != 0 {
            self.history = Remote::Failed(format!("The printer refused ({}).", data.ack));
            return;
        }

        let ip = self.ip;
        let found = (data.history_detail_list.iter().flatten())
            .filter_map(|x| HistoryTask::from_json(x, ip));
        self.history_found.extend(found);

        if !self.request_next_history_batch() {
            let mut tasks = mem::take(&mut self.history_found);
            tasks.sort_by_key(|x| std::cmp::Reverse(x.begin_time));
            self.history = Remote::Ready(tasks);
        }
    }

    fn request_next_history_batch(&mut self) -> bool {
        let Some(ids) = self.history_pending.pop() else {
            return false;
        };

        // A long history takes a while, the printer is answering though.
        self.history = Remote::Loading(Instant::now());
        let _ = self.sender.send(Cmd::HistoryDetails { ids });
        true
    }

    fn on_camera_response(&mut self, data: &AckData) {
        // Replies to switching the stream off, and late replies, don't matter.
        if !matches!(self.camera, CameraState::Requested(_)) {
            return;
        }

        let ack = data.ack;
        let failed = |reason: &str| CameraState::Failed(reason.to_owned());
        self.camera = match (ack, &data.video_url) {
            (0, Some(url)) => match validate_printer_url(url, self.ip) {
                Ok(url) => CameraState::Ready(url),
                Err(reason) => failed(&reason),
            },
            (0, None) => failed("The printer did not send a camera address."),
            (1, _) => failed(
                "The printer's video stream limit is reached. Close the stream in the other app and try again.",
            ),
            (2, _) => failed("The printer reports that it has no camera."),
            (3, _) => failed("The printer reported an unknown camera error."),
            (ack, _) => CameraState::Failed(format!("The printer rejected the request ({ack}).")),
        };

        // The printer may have opened a stream we won't use, free its slot.
        if matches!(self.camera, CameraState::Failed(_)) && ack == 0 {
            let _ = self.sender.send(Cmd::SetVideoStream { enable: false });
        }
    }
}

fn upload_chunks(
    clients: &Mutex<HashMap<String, Client>>,
    mainboard: &str,
    ip: Ipv4Addr,
    data: &[u8],
    filename: &str,
) -> Result<()> {
    const CHUNK_SIZE: usize = 1024 * 1024;

    let md5 = format!("{:x}", md5::compute(data));
    let uuid = uuid_v4();
    let total_size = data.len();
    let total_size_str = total_size.to_string();
    let url = format!("http://{ip}:3030/uploadFile/upload");

    let mut offset = 0;
    while offset < total_size {
        let end = (offset + CHUNK_SIZE).min(total_size);
        let chunk = &data[offset..end];
        let offset_str = offset.to_string();

        let file = Part::bytes(chunk)
            .file_name(filename)
            .mime_str("application/octet-stream")?;
        let form = Form::new()
            .text("S-File-MD5", &md5)
            .text("Check", "1")
            .text("Offset", &offset_str)
            .text("Uuid", &uuid)
            .text("TotalSize", &total_size_str)
            .part("File", file);

        let mut response = ureq::post(&url).send(form)?;
        let body = response.body_mut().read_to_string()?;
        let upload = serde_json::from_str::<UploadResponse>(&body)?;
        if !upload.success {
            let messages = &upload.messages;
            bail!("Printer rejected file chunk at offset {offset}: {messages:?}");
        }

        offset = end;
        if let Some(client) = clients.lock().get_mut(mainboard) {
            client.transfer_info.download_offset = offset as u32;
            client.transfer_info.check_offset = offset as u32;
        }
    }

    Ok(())
}

fn uuid_v4() -> String {
    let mut bytes = [0; 16];
    rng().fill_bytes(&mut bytes);

    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;

    let mut out = String::new();
    for (i, byte) in bytes.iter().enumerate() {
        matches!(i, 4 | 6 | 8 | 10).then(|| out.push('-'));
        out.push_str(&format!("{:02x}", byte));
    }

    out
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc::Receiver;

    use serde_json::json;

    use super::*;

    const IP: Ipv4Addr = Ipv4Addr::new(192, 0, 2, 10);

    fn client() -> (Client, Receiver<Cmd>) {
        let (tx, rx) = mpsc::channel();
        (Client::new(IP, tx), rx)
    }

    fn ack(value: serde_json::Value) -> AckData {
        serde_json::from_value(value).unwrap()
    }

    #[test]
    fn file_list_is_sorted_with_folders_first() {
        let (mut client, rx) = client();
        client.request_files("/usb/");
        assert!(matches!(rx.try_recv(), Ok(Cmd::ListFiles { url }) if url == "/usb/"));

        client.on_response(
            RETRIEVE_FILE_LIST,
            &ack(json!({"Ack": 0, "FileList": [
                {"name": "/usb/b.gcode", "usedSize": 5, "totalSize": 9, "storageType": 1, "type": 1},
                {"name": "/usb/Zed/", "usedSize": 0, "totalSize": 9, "storageType": 1, "type": 0},
                {"name": "/usb/a.gcode", "usedSize": 5, "totalSize": 9, "storageType": 1, "type": 1},
                {"broken": true}
            ]})),
        );

        let Remote::Ready(listing) = &client.files else {
            panic!("{:?}", client.files);
        };
        let names = listing.entries.iter().map(|x| x.name()).collect::<Vec<_>>();
        assert_eq!(names, ["Zed", "a.gcode", "b.gcode"]);
        assert_eq!(listing.path, "/usb/");
    }

    #[test]
    fn delete_reports_failures_and_relists() {
        let (mut client, rx) = client();
        client.files_path = "/local/".into();
        client.on_response(
            BATCH_DELETE_FILES,
            &ack(json!({"Ack": 0, "ErrData": ["/local/x.gcode"]})),
        );

        assert_eq!(
            client.last_error.as_deref(),
            Some("Could not delete: /local/x.gcode.")
        );
        assert!(matches!(rx.try_recv(), Ok(Cmd::ListFiles { url }) if url == "/local/"));
        assert!(matches!(client.files, Remote::Loading(_)));
    }

    #[test]
    fn history_is_fetched_in_batches_newest_first() {
        let (mut client, rx) = client();
        client.history = Remote::Loading(Instant::now());

        let ids = (0..30).map(|x| json!(format!("id{x}"))).collect::<Vec<_>>();
        client.on_response(
            RETRIEVE_HISTORY,
            &ack(json!({"Ack": 0, "HistoryData": ids})),
        );
        let Ok(Cmd::HistoryDetails { ids }) = rx.try_recv() else {
            panic!("expected a details request");
        };
        assert_eq!((ids.len(), ids[0].as_str()), (HISTORY_BATCH, "id0"));

        let task = |id: &str, begin: i64| json!({"TaskId": id, "BeginTime": begin});
        client.on_response(
            RETRIEVE_TASK_DETAILS,
            &ack(json!({"Ack": 0, "HistoryDetailList": [task("old", 1)]})),
        );
        let Ok(Cmd::HistoryDetails { ids }) = rx.try_recv() else {
            panic!("expected a second batch");
        };
        assert_eq!((ids.len(), ids[0].as_str()), (5, "id25"));
        assert!(matches!(client.history, Remote::Loading(_)));

        client.on_response(
            RETRIEVE_TASK_DETAILS,
            &ack(json!({"Ack": 0, "HistoryDetailList": [task("new", 2)]})),
        );
        let Remote::Ready(tasks) = &client.history else {
            panic!("{:?}", client.history);
        };
        assert_eq!(
            tasks.iter().map(|x| x.id.as_str()).collect::<Vec<_>>(),
            ["new", "old"]
        );
    }

    #[test]
    fn camera_address_must_be_the_printer() {
        let (mut client, _rx) = client();
        client.camera = CameraState::Requested(Instant::now());
        client.on_response(
            SET_VIDEO_STREAM,
            &ack(json!({"Ack": 0, "VideoUrl": "rtsp://192.0.2.10:554/live"})),
        );
        assert_eq!(
            client.camera,
            CameraState::Ready("rtsp://192.0.2.10:554/live".into())
        );

        client.camera = CameraState::Requested(Instant::now());
        client.on_response(
            SET_VIDEO_STREAM,
            &ack(json!({"Ack": 0, "VideoUrl": "rtsp://192.0.2.99/live"})),
        );
        assert!(matches!(client.camera, CameraState::Failed(_)));
    }

    #[test]
    fn rejected_print_is_explained() {
        let (mut client, _rx) = client();
        client.on_response(START_PRINTING, &ack(json!({"Ack": 2})));
        assert_eq!(
            client.last_error.as_deref(),
            Some("Could not start the print: the file was not found.")
        );
    }
}
