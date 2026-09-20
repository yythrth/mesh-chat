//! End-to-end encryption for every link, on every transport.
//!
//! Pattern: `Noise_XX_25519_ChaChaPoly_BLAKE2s`.
//!
//! XX means both sides transmit a static public key inside the handshake, so
//! after the handshake each end knows the other's long-term key and can show a
//! fingerprint in the UI. The transport underneath (TCP or RFCOMM) is treated
//! as completely untrusted: it only carries opaque ciphertext frames.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use snow::{Builder, TransportState};

use crate::codec::{read_frame, write_frame};
use crate::transport::Stream;
use crate::{Error, Result};

const PATTERN: &str = "Noise_XX_25519_ChaChaPoly_BLAKE2s";

/// Largest plaintext that fits in one Noise transport message
/// (65535 - 16 bytes of Poly1305 tag).
pub const MAX_PLAINTEXT: usize = 65519;

/// Handshakes must complete inside this window or the link is dropped.
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(20);

/// A long-term static keypair. One is generated per application run; its
/// fingerprint is displayed so users can compare it out of band if they care.
pub struct Identity {
    pub private: Vec<u8>,
    pub public: Vec<u8>,
}

impl Identity {
    pub fn generate() -> Result<Self> {
        let params = PATTERN
            .parse()
            .map_err(|e| Error::Crypto(format!("bad noise pattern: {e}")))?;
        let kp = Builder::new(params)
            .generate_keypair()
            .map_err(|e| Error::Crypto(e.to_string()))?;
        Ok(Identity {
            private: kp.private,
            public: kp.public,
        })
    }

    pub fn fingerprint(&self) -> String {
        fingerprint(&self.public)
    }
}

/// Short, readable fingerprint of a static public key.
pub fn fingerprint(public: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(public);
    digest
        .iter()
        .take(6)
        .map(|b| format!("{b:02X}"))
        .collect::<Vec<_>>()
        .join(":")
}

/// An encrypted, framed link to exactly one peer.
///
/// Concurrency contract: **one** reader thread calls [`SecureLink::recv`];
/// any number of threads may call [`SecureLink::send`]. `write_gate`
/// serialises senders so that Noise's message counter and the bytes on the
/// wire stay in the same order, while the cipher mutex itself is only held for
/// the encryption step - never across a blocking socket write - so the reader
/// can always keep draining.
pub struct SecureLink {
    stream: Arc<dyn Stream>,
    cipher: Mutex<TransportState>,
    write_gate: Mutex<()>,
    remote_static: Vec<u8>,
}

impl SecureLink {
    /// Dialling side of the handshake.
    pub fn handshake_initiator(stream: Arc<dyn Stream>, identity: &Identity) -> Result<Self> {
        stream.set_read_timeout(Some(HANDSHAKE_TIMEOUT)).ok();
        let params = PATTERN
            .parse()
            .map_err(|e| Error::Crypto(format!("bad noise pattern: {e}")))?;
        let mut hs = Builder::new(params)
            .local_private_key(&identity.private)
            .build_initiator()
            .map_err(|e| Error::Crypto(e.to_string()))?;

        let mut buf = vec![0u8; 1024];
        let mut scratch = vec![0u8; 1024];

        // -> e
        let n = hs
            .write_message(&[], &mut buf)
            .map_err(|e| Error::Crypto(e.to_string()))?;
        write_frame(&*stream, &buf[..n])?;
        // <- e, ee, s, es
        let msg = read_frame(&*stream)?;
        hs.read_message(&msg, &mut scratch)
            .map_err(|e| Error::Crypto(e.to_string()))?;
        // -> s, se
        let n = hs
            .write_message(&[], &mut buf)
            .map_err(|e| Error::Crypto(e.to_string()))?;
        write_frame(&*stream, &buf[..n])?;

        Self::finish(stream, hs)
    }

    /// Listening side of the handshake.
    pub fn handshake_responder(stream: Arc<dyn Stream>, identity: &Identity) -> Result<Self> {
        stream.set_read_timeout(Some(HANDSHAKE_TIMEOUT)).ok();
        let params = PATTERN
            .parse()
            .map_err(|e| Error::Crypto(format!("bad noise pattern: {e}")))?;
        let mut hs = Builder::new(params)
            .local_private_key(&identity.private)
            .build_responder()
            .map_err(|e| Error::Crypto(e.to_string()))?;

        let mut buf = vec![0u8; 1024];
        let mut scratch = vec![0u8; 1024];

        // <- e
        let msg = read_frame(&*stream)?;
        hs.read_message(&msg, &mut scratch)
            .map_err(|e| Error::Crypto(e.to_string()))?;
        // -> e, ee, s, es
        let n = hs
            .write_message(&[], &mut buf)
            .map_err(|e| Error::Crypto(e.to_string()))?;
        write_frame(&*stream, &buf[..n])?;
        // <- s, se
        let msg = read_frame(&*stream)?;
        hs.read_message(&msg, &mut scratch)
            .map_err(|e| Error::Crypto(e.to_string()))?;

        Self::finish(stream, hs)
    }

    fn finish(stream: Arc<dyn Stream>, hs: snow::HandshakeState) -> Result<Self> {
        let remote_static = hs.get_remote_static().unwrap_or(&[]).to_vec();
        let cipher = hs
            .into_transport_mode()
            .map_err(|e| Error::Crypto(e.to_string()))?;
        // Back to blocking reads for the life of the session.
        stream.set_read_timeout(None).ok();
        Ok(SecureLink {
            stream,
            cipher: Mutex::new(cipher),
            write_gate: Mutex::new(()),
            remote_static,
        })
    }

    /// Encrypt and send one payload. Payload must fit [`MAX_PLAINTEXT`].
    pub fn send(&self, payload: &[u8]) -> Result<()> {
        if payload.len() > MAX_PLAINTEXT {
            return Err(Error::Protocol("message too large for one frame".into()));
        }
        let _gate = self
            .write_gate
            .lock()
            .map_err(|_| Error::Closed("link poisoned".into()))?;
        let mut buf = vec![0u8; payload.len() + 64];
        let n = {
            let mut cipher = self
                .cipher
                .lock()
                .map_err(|_| Error::Closed("link poisoned".into()))?;
            cipher
                .write_message(payload, &mut buf)
                .map_err(|e| Error::Crypto(e.to_string()))?
        };
        write_frame(&*self.stream, &buf[..n])?;
        Ok(())
    }

    /// Receive and decrypt one payload. Blocks. Single reader only.
    pub fn recv(&self) -> Result<Vec<u8>> {
        let ct = read_frame(&*self.stream)?;
        let mut buf = vec![0u8; ct.len() + 16];
        let n = {
            let mut cipher = self
                .cipher
                .lock()
                .map_err(|_| Error::Closed("link poisoned".into()))?;
            cipher
                .read_message(&ct, &mut buf)
                .map_err(|e| Error::Crypto(e.to_string()))?
        };
        buf.truncate(n);
        Ok(buf)
    }

    pub fn fingerprint(&self) -> String {
        fingerprint(&self.remote_static)
    }

    pub fn remote_label(&self) -> String {
        self.stream.remote_label()
    }

    pub fn close(&self) {
        self.stream.close();
    }
}
