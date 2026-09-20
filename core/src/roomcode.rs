//! Room codes.
//!
//! A room code is the *only* way to find a room: nothing is advertised,
//! broadcast or published anywhere. The code carries the host's dial address
//! for the chosen transport, encoded in Crockford base32 (no `I`, `L`, `O`,
//! `U`, so it survives being read out over the phone).
//!
//! * LAN:       `L` + base32(4-byte IPv4 + 2-byte port)   -> 11 characters
//! * Bluetooth: `B` + base32(6-byte address + 1-byte channel) -> 13 characters
//!
//! Codes are displayed in dash-separated groups; the parser ignores dashes,
//! spaces and case.

use std::net::SocketAddr;

use crate::transport::{bluetooth, TransportKind};
use crate::{Error, Result};

const ALPHABET: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";

pub fn b32_encode(data: &[u8]) -> String {
    let mut out = String::new();
    let mut acc: u32 = 0;
    let mut bits: u32 = 0;
    for &b in data {
        acc = (acc << 8) | b as u32;
        bits += 8;
        while bits >= 5 {
            bits -= 5;
            out.push(ALPHABET[((acc >> bits) & 0x1f) as usize] as char);
        }
    }
    if bits > 0 {
        out.push(ALPHABET[((acc << (5 - bits)) & 0x1f) as usize] as char);
    }
    out
}

fn b32_value(c: char) -> Result<u32> {
    let c = c.to_ascii_uppercase();
    // Crockford substitutions for characters people commonly mistype.
    let c = match c {
        'I' | 'L' => '1',
        'O' => '0',
        other => other,
    };
    ALPHABET
        .iter()
        .position(|&a| a as char == c)
        .map(|p| p as u32)
        .ok_or_else(|| Error::RoomCode(format!("'{c}' is not a valid character in a room code")))
}

pub fn b32_decode(s: &str, expected_bytes: usize) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(expected_bytes);
    let mut acc: u32 = 0;
    let mut bits: u32 = 0;
    for c in s.chars() {
        acc = (acc << 5) | b32_value(c)?;
        bits += 5;
        if bits >= 8 {
            bits -= 8;
            out.push(((acc >> bits) & 0xff) as u8);
        }
    }
    if out.len() < expected_bytes {
        return Err(Error::RoomCode("room code is too short".into()));
    }
    out.truncate(expected_bytes);
    Ok(out)
}

/// Strip formatting and return `(transport, payload_chars)`.
fn split(code: &str) -> Result<(TransportKind, String)> {
    let cleaned: String = code
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .collect::<String>()
        .to_ascii_uppercase();
    let mut chars = cleaned.chars();
    let tag = chars
        .next()
        .ok_or_else(|| Error::RoomCode("the room code is empty".into()))?;
    let kind = TransportKind::from_tag(tag).ok_or_else(|| {
        Error::RoomCode(format!(
            "'{tag}' is not a known room-code prefix (expected L for LAN or B for Bluetooth)"
        ))
    })?;
    Ok((kind, chars.collect()))
}

/// Group the payload into readable chunks: `L-ABCDE-FGHIJ`.
fn pretty(tag: char, payload: &str) -> String {
    let groups: Vec<String> = payload
        .as_bytes()
        .chunks(5)
        .map(|c| String::from_utf8_lossy(c).to_string())
        .collect();
    format!("{}-{}", tag, groups.join("-"))
}

/// Build a room code from a transport and the dial address produced by
/// [`crate::transport::Listener::dial_address`].
pub fn encode(kind: TransportKind, dial: &str) -> Result<String> {
    match kind {
        TransportKind::Lan => {
            let addr: SocketAddr = dial
                .parse()
                .map_err(|_| Error::RoomCode(format!("'{dial}' is not a valid address:port")))?;
            let v4 = match addr {
                SocketAddr::V4(v4) => v4,
                SocketAddr::V6(_) => {
                    return Err(Error::RoomCode(
                        "LAN mode needs an IPv4 address; this machine reported only IPv6".into(),
                    ))
                }
            };
            let mut bytes = Vec::with_capacity(6);
            bytes.extend_from_slice(&v4.ip().octets());
            bytes.extend_from_slice(&v4.port().to_be_bytes());
            Ok(pretty(kind.tag(), &b32_encode(&bytes)))
        }
        TransportKind::Bluetooth => {
            let (addr, channel) = bluetooth::parse_dial(dial)?;
            if channel == 0 || channel > 30 {
                return Err(Error::RoomCode(format!(
                    "RFCOMM channel {channel} is outside the encodable range 1-30"
                )));
            }
            let mut bytes = Vec::with_capacity(7);
            bytes.extend_from_slice(&addr.to_be_bytes()[2..]); // low 48 bits
            bytes.push(channel as u8);
            Ok(pretty(kind.tag(), &b32_encode(&bytes)))
        }
    }
}

/// Parse a room code back into `(transport, dial address)`.
pub fn decode(code: &str) -> Result<(TransportKind, String)> {
    let (kind, payload) = split(code)?;
    match kind {
        TransportKind::Lan => {
            let bytes = b32_decode(&payload, 6)?;
            let ip = format!("{}.{}.{}.{}", bytes[0], bytes[1], bytes[2], bytes[3]);
            let port = u16::from_be_bytes([bytes[4], bytes[5]]);
            if port == 0 {
                return Err(Error::RoomCode("room code contains an invalid port".into()));
            }
            Ok((kind, format!("{ip}:{port}")))
        }
        TransportKind::Bluetooth => {
            let bytes = b32_decode(&payload, 7)?;
            let mut addr: u64 = 0;
            for b in &bytes[..6] {
                addr = (addr << 8) | *b as u64;
            }
            let channel = bytes[6] as u32;
            if channel == 0 || channel > 30 {
                return Err(Error::RoomCode(
                    "room code contains an invalid RFCOMM channel".into(),
                ));
            }
            Ok((kind, format!("{}:{}", bluetooth::format_addr(addr), channel)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lan_roundtrip() {
        let code = encode(TransportKind::Lan, "192.168.1.24:47021").unwrap();
        let (kind, dial) = decode(&code).unwrap();
        assert_eq!(kind, TransportKind::Lan);
        assert_eq!(dial, "192.168.1.24:47021");
    }

    #[test]
    fn bluetooth_roundtrip() {
        let code = encode(TransportKind::Bluetooth, "A4B1C2D3E4F5:25").unwrap();
        let (kind, dial) = decode(&code).unwrap();
        assert_eq!(kind, TransportKind::Bluetooth);
        assert_eq!(dial, "A4B1C2D3E4F5:25");
    }

    #[test]
    fn formatting_is_ignored() {
        let code = encode(TransportKind::Lan, "10.0.0.7:5000").unwrap();
        let messy = format!("  {}  ", code.to_lowercase().replace('-', " "));
        assert_eq!(decode(&messy).unwrap().1, "10.0.0.7:5000");
    }

    #[test]
    fn bad_codes_are_rejected() {
        assert!(decode("").is_err());
        assert!(decode("Z-ABCDE").is_err());
        assert!(decode("L-AB").is_err());
    }
}
