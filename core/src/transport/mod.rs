//! Transport abstraction.
//!
//! A transport gives us two things and nothing else:
//!
//! * a [`Listener`] that accepts inbound byte streams and can tell us the
//!   address other peers must dial to reach us, and
//! * a way to dial a remote address and get a [`Stream`] back.
//!
//! Everything above this layer (Noise encryption, framing, the mesh protocol,
//! chat, file transfer) is identical for every transport.
//!
//! Both implementations are *code-based only*: nothing is advertised, nothing
//! is broadcast, there is no mDNS, no SDP browsing and no discoverable
//! Bluetooth mode. A peer can only be reached by someone who was given the
//! room code out of band.

use std::io;
use std::sync::Arc;
use std::time::Duration;

pub mod lan;

#[cfg(windows)]
#[path = "bluetooth_windows.rs"]
pub mod bluetooth;

#[cfg(not(windows))]
#[path = "bluetooth_stub.rs"]
pub mod bluetooth;

/// The transports the app can run over.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Hash)]
pub enum TransportKind {
    /// Direct TCP on the local network. Room code encodes IP + port.
    Lan,
    /// Bluetooth RFCOMM. Room code encodes MAC + channel.
    Bluetooth,
}

impl TransportKind {
    pub fn as_str(self) -> &'static str {
        match self {
            TransportKind::Lan => "LAN",
            TransportKind::Bluetooth => "Bluetooth",
        }
    }

    /// Parse a user/UI supplied transport name (case-insensitive).
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "lan" | "tcp" | "local" => Some(TransportKind::Lan),
            "bluetooth" | "bt" | "rfcomm" => Some(TransportKind::Bluetooth),
            _ => None,
        }
    }

    /// One-character tag used as the first character of a room code.
    pub fn tag(self) -> char {
        match self {
            TransportKind::Lan => 'L',
            TransportKind::Bluetooth => 'B',
        }
    }

    pub fn from_tag(c: char) -> Option<Self> {
        match c.to_ascii_uppercase() {
            'L' => Some(TransportKind::Lan),
            'B' => Some(TransportKind::Bluetooth),
            _ => None,
        }
    }
}

/// A bidirectional byte stream.
///
/// Note the `&self` receivers: a single link is read by a dedicated reader
/// thread while other threads write to it. Both TCP sockets and Winsock
/// RFCOMM sockets support concurrent send/recv from different threads, so the
/// stream itself needs no interior locking; ordering of *writes* is enforced
/// one layer up, in [`crate::crypto::SecureLink`].
pub trait Stream: Send + Sync {
    fn read(&self, buf: &mut [u8]) -> io::Result<usize>;
    fn write(&self, buf: &[u8]) -> io::Result<usize>;
    /// Close both directions; safe to call more than once.
    fn close(&self);
    /// Something printable identifying the remote side (IP, MAC, ...).
    fn remote_label(&self) -> String;
    /// Apply a read timeout (used during handshakes so a stalled peer cannot
    /// pin a thread forever). `None` restores blocking behaviour.
    fn set_read_timeout(&self, dur: Option<Duration>) -> io::Result<()>;
}

/// A server endpoint other peers dial into.
pub trait Listener: Send + Sync {
    fn accept(&self) -> io::Result<Arc<dyn Stream>>;
    /// The address string that goes into our room code / `PeerInfo.addr`,
    /// e.g. `192.168.1.24:47021` or `A4B1C2D3E4F5:25`.
    fn dial_address(&self) -> String;
    fn close(&self);
}

/// Open a listening endpoint for the given transport.
pub fn listen(kind: TransportKind) -> crate::Result<Arc<dyn Listener>> {
    match kind {
        TransportKind::Lan => lan::listen(),
        TransportKind::Bluetooth => bluetooth::listen(),
    }
}

/// Dial a peer address produced by [`Listener::dial_address`].
pub fn connect(kind: TransportKind, addr: &str, timeout: Duration) -> crate::Result<Arc<dyn Stream>> {
    match kind {
        TransportKind::Lan => lan::connect(addr, timeout),
        TransportKind::Bluetooth => bluetooth::connect(addr, timeout),
    }
}
