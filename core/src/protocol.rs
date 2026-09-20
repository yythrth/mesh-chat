//! The messages peers exchange inside the encrypted channel.
//!
//! Frames are one byte of kind followed by a body:
//!
//! * `0x01` – JSON-encoded [`Control`] message.
//! * `0x02` – file chunk: 16-byte transfer id, 4-byte big-endian sequence
//!   number, then raw bytes. Chunks are binary so file transfer costs no
//!   base64 overhead and shares the link with chat without blocking it.

use serde::{Deserialize, Serialize};

use crate::{Error, Result};

pub const KIND_CONTROL: u8 = 0x01;
pub const KIND_CHUNK: u8 = 0x02;

/// 32 KiB payload per chunk: comfortably inside the Noise message limit and
/// large enough that RFCOMM's small MTU is not the bottleneck.
pub const CHUNK_SIZE: usize = 32 * 1024;

pub type PeerId = String;

/// Everything one peer needs in order to reach and recognise another.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PeerInfo {
    pub id: PeerId,
    pub name: String,
    /// Dial address on the active transport (`ip:port` or `MAC:channel`).
    pub addr: String,
    pub fingerprint: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "t")]
pub enum Control {
    /// Joiner -> host: "may I join this room?"
    Hello {
        version: u32,
        room: String,
        peer: PeerInfo,
    },
    /// New peer -> already-approved peer: "the host approved me, here I am."
    MeshHello {
        version: u32,
        room: String,
        peer: PeerInfo,
    },
    /// Host -> joiner, after the operator pressed Accept. Carries the roster
    /// so the joiner can dial everybody else directly.
    Welcome {
        room: String,
        host: PeerInfo,
        peers: Vec<PeerInfo>,
    },
    /// Peer -> new peer: direct mesh link established.
    MeshAck { peer: PeerInfo },
    /// Either side refusing a link, with a reason shown in the UI.
    Rejected { reason: String },
    /// Host -> existing peers: a new peer was approved; expect their call.
    PeerJoined { peer: PeerInfo },
    /// Host -> everyone: a peer is gone.
    PeerLeft { id: PeerId, name: String },
    /// A chat line, sent directly to every peer by its author.
    Chat {
        id: String,
        from: PeerId,
        name: String,
        ts: i64,
        body: String,
    },
    /// Offer to send a file. The recipient answers accept/decline.
    FileOffer {
        transfer: String,
        name: String,
        size: u64,
        sha256: String,
    },
    FileAccept {
        transfer: String,
    },
    FileDecline {
        transfer: String,
        reason: String,
    },
    /// All chunks sent.
    FileDone {
        transfer: String,
    },
    FileAbort {
        transfer: String,
        reason: String,
    },
    /// Graceful shutdown.
    Bye,
}

pub enum Frame {
    Control(Control),
    Chunk {
        id: [u8; 16],
        seq: u32,
        data: Vec<u8>,
    },
}

pub fn encode_control(c: &Control) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(128);
    out.push(KIND_CONTROL);
    out.extend_from_slice(&serde_json::to_vec(c)?);
    Ok(out)
}

pub fn encode_chunk(id: &[u8; 16], seq: u32, data: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(21 + data.len());
    out.push(KIND_CHUNK);
    out.extend_from_slice(id);
    out.extend_from_slice(&seq.to_be_bytes());
    out.extend_from_slice(data);
    out
}

pub fn decode(buf: &[u8]) -> Result<Frame> {
    match buf.first() {
        Some(&KIND_CONTROL) => {
            let c: Control = serde_json::from_slice(&buf[1..])?;
            Ok(Frame::Control(c))
        }
        Some(&KIND_CHUNK) => {
            if buf.len() < 21 {
                return Err(Error::Protocol("truncated file chunk".into()));
            }
            let mut id = [0u8; 16];
            id.copy_from_slice(&buf[1..17]);
            let seq = u32::from_be_bytes([buf[17], buf[18], buf[19], buf[20]]);
            Ok(Frame::Chunk {
                id,
                seq,
                data: buf[21..].to_vec(),
            })
        }
        Some(other) => Err(Error::Protocol(format!("unknown frame kind {other}"))),
        None => Err(Error::Protocol("empty frame".into())),
    }
}

// --- small helpers --------------------------------------------------------

pub fn random_bytes<const N: usize>() -> [u8; N] {
    use rand::RngCore;
    let mut out = [0u8; N];
    rand::rngs::OsRng.fill_bytes(&mut out);
    out
}

pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

pub fn unhex16(s: &str) -> Result<[u8; 16]> {
    if s.len() != 32 {
        return Err(Error::Protocol("bad transfer id".into()));
    }
    let mut out = [0u8; 16];
    for i in 0..16 {
        out[i] = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16)
            .map_err(|_| Error::Protocol("bad transfer id".into()))?;
    }
    Ok(out)
}

/// Random 16-hex-character identifier, used for peer ids and message ids.
pub fn new_id() -> String {
    hex(&random_bytes::<8>())
}

pub fn now_ts() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Local-time `HH:MM:SS` for the chat view.
pub fn fmt_clock(ts: i64) -> String {
    use chrono::{Local, TimeZone};
    match Local.timestamp_opt(ts, 0).single() {
        Some(dt) => dt.format("%H:%M:%S").to_string(),
        None => "??:??:??".to_string(),
    }
}

/// Local-time `YYYY-MM-DD HH:MM:SS` for the exported transcript.
pub fn fmt_full(ts: i64) -> String {
    use chrono::{Local, TimeZone};
    match Local.timestamp_opt(ts, 0).single() {
        Some(dt) => dt.format("%Y-%m-%d %H:%M:%S").to_string(),
        None => "unknown time".to_string(),
    }
}
