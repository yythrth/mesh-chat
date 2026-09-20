//! Session transcript.
//!
//! Messages live in memory for the lifetime of the room and are written to
//! disk only when the user presses "Download Chat History" and picks a
//! location. Nothing is written anywhere else, by anyone, at any time.

use std::fs;
use std::io::Write;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::protocol::fmt_full;
use crate::Result;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RecordKind {
    /// A chat line written by a person.
    Message,
    /// Room lifecycle note (joins, leaves, transport changes).
    System,
    /// A completed file transfer.
    File,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChatRecord {
    pub ts: i64,
    pub sender: String,
    pub sender_id: String,
    pub body: String,
    pub kind: RecordKind,
}

impl ChatRecord {
    pub fn message(sender: &str, sender_id: &str, ts: i64, body: &str) -> Self {
        Self {
            ts,
            sender: sender.to_string(),
            sender_id: sender_id.to_string(),
            body: body.to_string(),
            kind: RecordKind::Message,
        }
    }

    pub fn system(ts: i64, body: &str) -> Self {
        Self {
            ts,
            sender: "system".to_string(),
            sender_id: String::new(),
            body: body.to_string(),
            kind: RecordKind::System,
        }
    }

    pub fn file(sender: &str, sender_id: &str, ts: i64, body: &str) -> Self {
        Self {
            ts,
            sender: sender.to_string(),
            sender_id: sender_id.to_string(),
            body: body.to_string(),
            kind: RecordKind::File,
        }
    }
}

/// Render the transcript as plain text: one `[timestamp] sender: body` line
/// per record.
pub fn render_text(room: &str, transport: &str, records: &[ChatRecord]) -> String {
    let mut out = String::new();
    out.push_str("Portable P2P Chat - session transcript\n");
    out.push_str(&format!("Room code : {room}\n"));
    out.push_str(&format!("Transport : {transport}\n"));
    out.push_str(&format!("Exported  : {}\n", fmt_full(crate::protocol::now_ts())));
    out.push_str(&"-".repeat(60));
    out.push('\n');
    for r in records {
        match r.kind {
            RecordKind::Message => {
                out.push_str(&format!("[{}] {}: {}\n", fmt_full(r.ts), r.sender, r.body))
            }
            RecordKind::System => out.push_str(&format!("[{}] * {}\n", fmt_full(r.ts), r.body)),
            RecordKind::File => out.push_str(&format!(
                "[{}] * {} (file) {}\n",
                fmt_full(r.ts),
                r.sender,
                r.body
            )),
        }
    }
    out
}

/// Write the transcript to `path`. A `.json` extension produces structured
/// JSON; anything else produces the plain-text rendering above.
pub fn export(path: &Path, room: &str, transport: &str, records: &[ChatRecord]) -> Result<()> {
    let is_json = path
        .extension()
        .map(|e| e.eq_ignore_ascii_case("json"))
        .unwrap_or(false);

    let bytes = if is_json {
        serde_json::to_vec_pretty(&serde_json::json!({
            "room": room,
            "transport": transport,
            "exported_at": fmt_full(crate::protocol::now_ts()),
            "messages": records,
        }))?
    } else {
        render_text(room, transport, records).into_bytes()
    };

    let mut f = fs::File::create(path)?;
    f.write_all(&bytes)?;
    f.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_export_contains_sender_and_body() {
        let records = vec![ChatRecord::message("Alice", "aa", 0, "hello there")];
        let text = render_text("L-ABCDE-FGHIJ", "LAN", &records);
        assert!(text.contains("Alice: hello there"));
        assert!(text.contains("L-ABCDE-FGHIJ"));
    }
}
