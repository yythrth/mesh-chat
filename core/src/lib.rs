//! Core engine for the portable P2P group-chat client.
//!
//! Everything that is not user interface lives here:
//!
//! * [`transport`] – the `Transport`/`Listener`/`Stream` abstraction plus the
//!   LAN (direct TCP) and Bluetooth (RFCOMM) implementations.
//! * [`crypto`]    – Noise Protocol Framework (`Noise_XX_25519_ChaChaPoly_BLAKE2s`)
//!   session setup and framed encrypted transport.
//! * [`roomcode`]  – short human-typeable room codes (Crockford base32).
//! * [`protocol`]  – the wire messages exchanged between peers.
//! * [`mesh`]      – the node: hosting, joining, approval flow, full-mesh wiring.
//! * [`files`]     – chunked file transfer over the same encrypted channel.
//! * [`history`]   – local chat-history export.
//! * [`ratelimit`] – join-attempt throttling.
//!
//! There is no network component that stores or relays chat content: peers talk
//! to each other directly, the host only introduces them to one another.

pub mod codec;
pub mod crypto;
pub mod files;
pub mod history;
pub mod mesh;
pub mod protocol;
pub mod ratelimit;
pub mod roomcode;
pub mod transport;

use std::fmt;

/// Crate-wide result alias.
pub type Result<T> = std::result::Result<T, Error>;

/// Every failure mode the engine can report, with a human-readable message
/// (the UI shows these verbatim as the "reason" on failed connections).
#[derive(Debug)]
pub enum Error {
    /// Underlying socket / file error.
    Io(std::io::Error),
    /// Handshake or encryption failure.
    Crypto(String),
    /// A peer sent something we did not expect.
    Protocol(String),
    /// A room code could not be parsed or generated.
    RoomCode(String),
    /// The requested transport is not available on this build/platform.
    Unsupported(String),
    /// Local configuration / user error.
    Config(String),
    /// The link or room is gone.
    Closed(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "{e}"),
            Error::Crypto(m) => write!(f, "encryption error: {m}"),
            Error::Protocol(m) => write!(f, "protocol error: {m}"),
            Error::RoomCode(m) => write!(f, "room code error: {m}"),
            Error::Unsupported(m) => write!(f, "{m}"),
            Error::Config(m) => write!(f, "{m}"),
            Error::Closed(m) => write!(f, "{m}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::Protocol(e.to_string())
    }
}

/// Protocol version carried in every `Hello`. Peers with a different major
/// version are rejected with a readable reason instead of failing obscurely.
pub const PROTOCOL_VERSION: u32 = 1;
