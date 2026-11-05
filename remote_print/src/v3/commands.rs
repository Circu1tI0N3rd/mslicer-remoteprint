use std::net::TcpStream;

use rand::{RngExt, rng};
use serde::Serialize;
use serde_json::json;
use tungstenite::WebSocket;

use crate::{
    shared::epoch,
    v3::status::{Command, CommandData},
};

pub const START_PRINTING: u16 = 128;
pub const RETRIEVE_FILE_LIST: u16 = 258;
pub const BATCH_DELETE_FILES: u16 = 259;
pub const RETRIEVE_HISTORY: u16 = 320;
pub const RETRIEVE_TASK_DETAILS: u16 = 321;
/// Enable or disable the camera stream.
pub const SET_VIDEO_STREAM: u16 = 386;

pub fn send_command(socket: &mut WebSocket<TcpStream>, mainboard_id: &str, cmd: Cmd) {
    let message = serde_json::to_string(&Command {
        id: mainboard_id.to_owned(),
        data: CommandData {
            cmd: cmd.cmd(),
            data: cmd.data(),
            request_id: format!("{:X}", rng().random::<u64>()),
            mainboard_id: mainboard_id.to_owned(),
            time_stamp: epoch(),
            from: 0,
        },
        topic: format!("sdcp/request/{mainboard_id}"),
    })
    .unwrap();
    tracing::trace!(target: "sdcp::ws", "-> {mainboard_id} {message}");
    socket
        .send(tungstenite::Message::Text(message.into()))
        .unwrap();
}

pub enum Cmd {
    RefreshStatus,
    RefreshAttributes,
    StartPrinting {
        filename: String,
        start_layer: u32,
    },
    PausePrinting,
    StopPrinting,
    ResumePrinting,
    SetVideoStream {
        enable: bool,
    },
    ListFiles {
        url: String,
    },
    DeleteFiles {
        files: Vec<String>,
        folders: Vec<String>,
    },
    ListHistory,
    HistoryDetails {
        ids: Vec<String>,
    },
}

impl Cmd {
    pub fn cmd(&self) -> u16 {
        match self {
            Cmd::RefreshStatus => 0,
            Cmd::RefreshAttributes => 1,
            Cmd::StartPrinting { .. } => START_PRINTING,
            Cmd::PausePrinting => 129,
            Cmd::StopPrinting => 130,
            Cmd::ResumePrinting => 131,
            Cmd::SetVideoStream { .. } => SET_VIDEO_STREAM,
            Cmd::ListFiles { .. } => RETRIEVE_FILE_LIST,
            Cmd::DeleteFiles { .. } => BATCH_DELETE_FILES,
            Cmd::ListHistory => RETRIEVE_HISTORY,
            Cmd::HistoryDetails { .. } => RETRIEVE_TASK_DETAILS,
        }
    }

    pub fn data(&self) -> impl Serialize {
        match self {
            Cmd::RefreshStatus => json!({}),
            Cmd::RefreshAttributes => json!({}),
            Cmd::StartPrinting {
                filename,
                start_layer,
            } => json!({
                "Filename": filename,
                "StartLayer": start_layer
            }),
            Cmd::PausePrinting => json!({}),
            Cmd::StopPrinting => json!({}),
            Cmd::ResumePrinting => json!({}),
            Cmd::SetVideoStream { enable } => json!({ "Enable": *enable as u8 }),
            Cmd::ListFiles { url } => json!({ "Url": url }),
            Cmd::DeleteFiles { files, folders } => {
                json!({ "FileList": files, "FolderList": folders })
            }
            Cmd::ListHistory => json!({}),
            Cmd::HistoryDetails { ids } => json!({ "Id": ids }),
        }
    }
}
