//! Length-prefixed framing.
//!
//! Every byte that crosses a transport is inside a frame: a 4-byte big-endian
//! length followed by that many bytes. During the handshake the payload is a
//! raw Noise handshake message; afterwards it is always a Noise transport
//! message (i.e. ciphertext).

use std::io;

use crate::transport::Stream;

/// Noise transport messages are capped at 65535 bytes, so frames are too.
pub const MAX_FRAME: usize = 65535;

pub fn read_exact(s: &dyn Stream, buf: &mut [u8]) -> io::Result<()> {
    let mut off = 0usize;
    while off < buf.len() {
        match s.read(&mut buf[off..]) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "the peer closed the connection",
                ))
            }
            Ok(n) => off += n,
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

pub fn write_all(s: &dyn Stream, buf: &[u8]) -> io::Result<()> {
    let mut off = 0usize;
    while off < buf.len() {
        match s.write(&buf[off..]) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "the peer stopped accepting data",
                ))
            }
            Ok(n) => off += n,
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

pub fn write_frame(s: &dyn Stream, payload: &[u8]) -> io::Result<()> {
    if payload.len() > MAX_FRAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "frame too large",
        ));
    }
    let len = (payload.len() as u32).to_be_bytes();
    // One buffer, one write: keeps the header and body atomic per write lock.
    let mut out = Vec::with_capacity(4 + payload.len());
    out.extend_from_slice(&len);
    out.extend_from_slice(payload);
    write_all(s, &out)
}

pub fn read_frame(s: &dyn Stream) -> io::Result<Vec<u8>> {
    let mut hdr = [0u8; 4];
    read_exact(s, &mut hdr)?;
    let len = u32::from_be_bytes(hdr) as usize;
    if len > MAX_FRAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "oversized frame - not a peer speaking this protocol",
        ));
    }
    let mut buf = vec![0u8; len];
    read_exact(s, &mut buf)?;
    Ok(buf)
}
