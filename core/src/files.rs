//! File transfer.
//!
//! Files ride the *same* encrypted link as chat, on every transport: an offer
//! goes out as a control message, the recipient answers accept or decline, and
//! on acceptance the bytes stream as binary chunk frames interleaved with
//! normal chat traffic. Sending to "everyone" opens one independent transfer
//! per connected peer - nothing is relayed through the host.
//!
//! Received data is written only to the path the recipient chose, and is never
//! executed or opened automatically.

use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{channel, Sender};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use sha2::{Digest, Sha256};

use crate::history::ChatRecord;
use crate::mesh::{Event, Node, Peer};
use crate::protocol::{
    encode_chunk, hex, now_ts, random_bytes, unhex16, Control, CHUNK_SIZE,
};
use crate::{Error, Result};

/// How long a sender waits for the recipient to answer an offer.
pub const OFFER_TIMEOUT: Duration = Duration::from_secs(300);
/// Minimum gap between progress events, so the UI is not flooded.
const PROGRESS_INTERVAL: Duration = Duration::from_millis(150);

enum Decision {
    Accept,
    Decline(String),
}

struct PendingOffer {
    peer_id: String,
    peer_name: String,
    name: String,
    size: u64,
    sha256: String,
}

struct ActiveIn {
    file: File,
    path: PathBuf,
    name: String,
    size: u64,
    got: u64,
    next_seq: u32,
    hasher: Sha256,
    expect_sha: String,
    peer_id: String,
    peer_name: String,
    last_emit: Instant,
}

#[derive(Default)]
pub struct Transfers {
    /// Outgoing transfers waiting for accept/decline.
    waiters: HashMap<[u8; 16], Sender<Decision>>,
    /// Which peer each outgoing transfer targets.
    out_peer: HashMap<[u8; 16], String>,
    /// Offers we received and have not answered yet.
    offers: HashMap<[u8; 16], PendingOffer>,
    /// Accepted incoming transfers currently being written.
    active: HashMap<[u8; 16], ActiveIn>,
}

/// Strip any directory component a remote peer might have put in a filename.
pub fn safe_file_name(name: &str) -> String {
    let base = name
        .rsplit(|c| c == '/' || c == '\\')
        .next()
        .unwrap_or("received.bin");
    let cleaned: String = base
        .chars()
        .filter(|c| !c.is_control() && !matches!(c, ':' | '*' | '?' | '"' | '<' | '>' | '|'))
        .take(120)
        .collect();
    let trimmed = cleaned.trim().trim_matches('.').to_string();
    if trimmed.is_empty() {
        "received.bin".to_string()
    } else {
        trimmed
    }
}

fn hash_file(path: &Path) -> Result<String> {
    let mut f = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hex(&hasher.finalize()))
}

pub fn human_size(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut v = bytes as f64;
    let mut i = 0;
    while v >= 1024.0 && i < UNITS.len() - 1 {
        v /= 1024.0;
        i += 1;
    }
    if i == 0 {
        format!("{bytes} B")
    } else {
        format!("{v:.1} {}", UNITS[i])
    }
}

impl Node {
    /// Offer a file to every connected peer.
    pub fn send_file(self: &Arc<Self>, path: PathBuf) -> Result<()> {
        let peers = self.connected_peers();
        if peers.is_empty() {
            return Err(Error::Config(
                "there is nobody connected to send a file to".into(),
            ));
        }
        let meta = std::fs::metadata(&path)?;
        if !meta.is_file() {
            return Err(Error::Config("that is not a regular file".into()));
        }
        let size = meta.len();
        let name = safe_file_name(
            &path
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_else(|| "file.bin".into()),
        );
        self.emit(Event::Status(format!(
            "Hashing {name} ({})...",
            human_size(size)
        )));
        let sha = hash_file(&path)?;

        for peer in peers {
            let node = self.clone();
            let path = path.clone();
            let name = name.clone();
            let sha = sha.clone();
            thread::spawn(move || node.send_file_to(peer, path, name, size, sha));
        }
        Ok(())
    }

    fn send_file_to(
        self: Arc<Self>,
        peer: Arc<Peer>,
        path: PathBuf,
        name: String,
        size: u64,
        sha: String,
    ) {
        let id = random_bytes::<16>();
        let id_hex = hex(&id);
        let (tx, rx) = channel::<Decision>();
        {
            let mut t = self.transfers.lock().unwrap();
            t.waiters.insert(id, tx);
            t.out_peer.insert(id, peer.info.id.clone());
        }

        let offer = Control::FileOffer {
            transfer: id_hex.clone(),
            name: name.clone(),
            size,
            sha256: sha.clone(),
        };
        if let Err(e) = peer.send(&offer) {
            self.finish_out(&id, &id_hex, &name, false, format!("could not send the offer: {e}"));
            return;
        }
        self.emit(Event::Status(format!(
            "Offered {name} to {} - waiting for a reply",
            peer.info.name
        )));

        match rx.recv_timeout(OFFER_TIMEOUT) {
            Ok(Decision::Accept) => {}
            Ok(Decision::Decline(reason)) => {
                self.finish_out(
                    &id,
                    &id_hex,
                    &name,
                    false,
                    format!("{} declined ({reason})", peer.info.name),
                );
                return;
            }
            Err(_) => {
                let _ = peer.send(&Control::FileAbort {
                    transfer: id_hex.clone(),
                    reason: "no answer in time".into(),
                });
                self.finish_out(
                    &id,
                    &id_hex,
                    &name,
                    false,
                    format!("{} did not answer in time", peer.info.name),
                );
                return;
            }
        }

        let mut file = match File::open(&path) {
            Ok(f) => f,
            Err(e) => {
                let _ = peer.send(&Control::FileAbort {
                    transfer: id_hex.clone(),
                    reason: e.to_string(),
                });
                self.finish_out(&id, &id_hex, &name, false, format!("could not read the file: {e}"));
                return;
            }
        };

        let mut buf = vec![0u8; CHUNK_SIZE];
        let mut sent: u64 = 0;
        let mut seq: u32 = 0;
        let mut last = Instant::now() - PROGRESS_INTERVAL;
        loop {
            if !self.is_active() {
                return;
            }
            let n = match file.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => n,
                Err(e) => {
                    let _ = peer.send(&Control::FileAbort {
                        transfer: id_hex.clone(),
                        reason: e.to_string(),
                    });
                    self.finish_out(&id, &id_hex, &name, false, format!("read error: {e}"));
                    return;
                }
            };
            if let Err(e) = peer.link.send(&encode_chunk(&id, seq, &buf[..n])) {
                self.finish_out(
                    &id,
                    &id_hex,
                    &name,
                    false,
                    format!("link to {} failed: {e}", peer.info.name),
                );
                return;
            }
            seq = seq.wrapping_add(1);
            sent += n as u64;
            if last.elapsed() >= PROGRESS_INTERVAL || sent == size {
                last = Instant::now();
                self.emit(Event::FileProgress {
                    transfer: id_hex.clone(),
                    name: name.clone(),
                    peer: peer.info.name.clone(),
                    done: sent,
                    total: size,
                    incoming: false,
                });
            }
        }

        let _ = peer.send(&Control::FileDone {
            transfer: id_hex.clone(),
        });
        self.record(ChatRecord::file(
            &self.me().name,
            &self.me().id,
            now_ts(),
            &format!("sent {name} ({}) to {}", human_size(size), peer.info.name),
        ));
        self.finish_out(
            &id,
            &id_hex,
            &name,
            true,
            format!("sent to {}", peer.info.name),
        );
    }

    fn finish_out(&self, id: &[u8; 16], id_hex: &str, name: &str, ok: bool, detail: String) {
        {
            let mut t = self.transfers.lock().unwrap();
            t.waiters.remove(id);
            t.out_peer.remove(id);
        }
        self.emit(Event::FileFinished {
            transfer: id_hex.to_string(),
            name: name.to_string(),
            incoming: false,
            ok,
            detail,
        });
    }

    /// Answer an incoming offer. `save_to` of `None` declines it.
    pub fn respond_offer(self: &Arc<Self>, transfer: &str, save_to: Option<PathBuf>) -> Result<()> {
        let id = unhex16(transfer)?;
        let offer = {
            let mut t = self.transfers.lock().unwrap();
            t.offers.remove(&id)
        }
        .ok_or_else(|| Error::Protocol("that file offer is no longer pending".into()))?;

        let peer = self.peer_by_id(&offer.peer_id);

        let Some(path) = save_to else {
            if let Some(p) = peer {
                let _ = p.send(&Control::FileDecline {
                    transfer: transfer.to_string(),
                    reason: "declined by the recipient".into(),
                });
            }
            self.emit(Event::FileFinished {
                transfer: transfer.to_string(),
                name: offer.name,
                incoming: true,
                ok: false,
                detail: "you declined the file".into(),
            });
            return Ok(());
        };

        let peer = peer.ok_or_else(|| Error::Closed("that peer is no longer connected".into()))?;
        let file = File::create(&path)?;
        {
            let mut t = self.transfers.lock().unwrap();
            t.active.insert(
                id,
                ActiveIn {
                    file,
                    path: path.clone(),
                    name: offer.name.clone(),
                    size: offer.size,
                    got: 0,
                    next_seq: 0,
                    hasher: Sha256::new(),
                    expect_sha: offer.sha256.clone(),
                    peer_id: offer.peer_id.clone(),
                    peer_name: offer.peer_name.clone(),
                    last_emit: Instant::now() - PROGRESS_INTERVAL,
                },
            );
        }
        peer.send(&Control::FileAccept {
            transfer: transfer.to_string(),
        })?;
        self.emit(Event::Status(format!(
            "Receiving {} into {}",
            offer.name,
            path.display()
        )));
        Ok(())
    }

    pub(crate) fn handle_file_control(self: &Arc<Self>, peer: &Arc<Peer>, c: Control) {
        match c {
            Control::FileOffer {
                transfer,
                name,
                size,
                sha256,
            } => {
                let Ok(id) = unhex16(&transfer) else { return };
                let name = safe_file_name(&name);
                {
                    let mut t = self.transfers.lock().unwrap();
                    t.offers.insert(
                        id,
                        PendingOffer {
                            peer_id: peer.info.id.clone(),
                            peer_name: peer.info.name.clone(),
                            name: name.clone(),
                            size,
                            sha256,
                        },
                    );
                }
                self.emit(Event::FileOffer {
                    transfer,
                    from: peer.info.name.clone(),
                    name,
                    size,
                });
            }
            Control::FileAccept { transfer } => {
                if let Ok(id) = unhex16(&transfer) {
                    let tx = self.transfers.lock().unwrap().waiters.get(&id).cloned();
                    if let Some(tx) = tx {
                        let _ = tx.send(Decision::Accept);
                    }
                }
            }
            Control::FileDecline { transfer, reason } => {
                if let Ok(id) = unhex16(&transfer) {
                    let tx = self.transfers.lock().unwrap().waiters.get(&id).cloned();
                    if let Some(tx) = tx {
                        let _ = tx.send(Decision::Decline(reason));
                    }
                }
            }
            Control::FileDone { transfer } => {
                if let Ok(id) = unhex16(&transfer) {
                    self.complete_incoming(&id, &transfer);
                }
            }
            Control::FileAbort { transfer, reason } => {
                if let Ok(id) = unhex16(&transfer) {
                    let (name, was) = {
                        let mut t = self.transfers.lock().unwrap();
                        t.offers.remove(&id);
                        match t.active.remove(&id) {
                            Some(a) => (a.name, true),
                            None => (String::from("file"), false),
                        }
                    };
                    if was {
                        self.emit(Event::FileFinished {
                            transfer,
                            name,
                            incoming: true,
                            ok: false,
                            detail: format!("the sender aborted: {reason}"),
                        });
                    }
                }
            }
            _ => {}
        }
    }

    pub(crate) fn handle_chunk(self: &Arc<Self>, peer: &Arc<Peer>, id: [u8; 16], seq: u32, data: &[u8]) {
        let mut problem: Option<String> = None;
        let mut progress: Option<(String, String, u64, u64)> = None;

        {
            let mut t = self.transfers.lock().unwrap();
            let Some(a) = t.active.get_mut(&id) else { return };
            if a.peer_id != peer.info.id {
                return;
            }
            if seq != a.next_seq {
                problem = Some(format!(
                    "chunks arrived out of order (expected {}, got {seq})",
                    a.next_seq
                ));
            } else if a.got + data.len() as u64 > a.size {
                problem = Some("the sender sent more data than it offered".into());
            } else if let Err(e) = a.file.write_all(data) {
                problem = Some(format!("could not write to disk: {e}"));
            } else {
                a.hasher.update(data);
                a.got += data.len() as u64;
                a.next_seq = a.next_seq.wrapping_add(1);
                if a.last_emit.elapsed() >= PROGRESS_INTERVAL || a.got == a.size {
                    a.last_emit = Instant::now();
                    progress = Some((a.name.clone(), a.peer_name.clone(), a.got, a.size));
                }
            }
        }

        if let Some(reason) = problem {
            let name = {
                let mut t = self.transfers.lock().unwrap();
                t.active.remove(&id).map(|a| a.name).unwrap_or_default()
            };
            let _ = peer.send(&Control::FileAbort {
                transfer: hex(&id),
                reason: reason.clone(),
            });
            self.emit(Event::FileFinished {
                transfer: hex(&id),
                name,
                incoming: true,
                ok: false,
                detail: reason,
            });
            return;
        }

        if let Some((name, peer_name, done, total)) = progress {
            self.emit(Event::FileProgress {
                transfer: hex(&id),
                name,
                peer: peer_name,
                done,
                total,
                incoming: true,
            });
        }
    }

    fn complete_incoming(self: &Arc<Self>, id: &[u8; 16], transfer: &str) {
        let Some(mut a) = ({
            let mut t = self.transfers.lock().unwrap();
            t.active.remove(id)
        }) else {
            return;
        };
        let _ = a.file.flush();
        let digest = hex(&a.hasher.finalize_reset());
        let ok = a.got == a.size && digest == a.expect_sha;
        let detail = if ok {
            format!("saved to {}", a.path.display())
        } else if a.got != a.size {
            format!(
                "incomplete: got {} of {}",
                human_size(a.got),
                human_size(a.size)
            )
        } else {
            "the checksum did not match - the file may be corrupt".to_string()
        };
        drop(a.file);
        if ok {
            self.record(ChatRecord::file(
                &a.peer_name,
                &a.peer_id,
                now_ts(),
                &format!(
                    "received {} ({}) -> {}",
                    a.name,
                    human_size(a.size),
                    a.path.display()
                ),
            ));
        }
        self.emit(Event::FileFinished {
            transfer: transfer.to_string(),
            name: a.name,
            incoming: true,
            ok,
            detail,
        });
    }

    /// Tear down anything in flight with a peer that just went away.
    pub(crate) fn cancel_transfers_with(self: &Arc<Self>, peer_id: &str) {
        let mut finished: Vec<(String, String)> = Vec::new();
        {
            let mut t = self.transfers.lock().unwrap();
            let dead: Vec<[u8; 16]> = t
                .active
                .iter()
                .filter(|(_, a)| a.peer_id == peer_id)
                .map(|(k, _)| *k)
                .collect();
            for k in dead {
                if let Some(a) = t.active.remove(&k) {
                    finished.push((hex(&k), a.name));
                }
            }
            let dead_offers: Vec<[u8; 16]> = t
                .offers
                .iter()
                .filter(|(_, o)| o.peer_id == peer_id)
                .map(|(k, _)| *k)
                .collect();
            for k in dead_offers {
                t.offers.remove(&k);
            }
            let dead_out: Vec<[u8; 16]> = t
                .out_peer
                .iter()
                .filter(|(_, p)| p.as_str() == peer_id)
                .map(|(k, _)| *k)
                .collect();
            for k in dead_out {
                if let Some(tx) = t.waiters.remove(&k) {
                    let _ = tx.send(Decision::Decline("the peer disconnected".into()));
                }
                t.out_peer.remove(&k);
            }
        }
        for (transfer, name) in finished {
            self.emit(Event::FileFinished {
                transfer,
                name,
                incoming: true,
                ok: false,
                detail: "the sender disconnected".into(),
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_names_are_sanitised() {
        assert_eq!(safe_file_name("..\\..\\windows\\system32\\evil.dll"), "evil.dll");
        assert_eq!(safe_file_name("/etc/passwd"), "passwd");
        assert_eq!(safe_file_name("   "), "received.bin");
    }

    #[test]
    fn sizes_are_readable() {
        assert_eq!(human_size(512), "512 B");
        assert_eq!(human_size(2048), "2.0 KB");
    }
}
