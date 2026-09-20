//! Placeholder Bluetooth transport for non-Windows builds.
//!
//! The shipping target is Windows; this stub exists so the crate (and its test
//! suite, which only needs the LAN transport) still builds on other platforms
//! during development.

use std::sync::Arc;
use std::time::Duration;

use super::{Listener, Stream};
use crate::{Error, Result};

pub const RFCOMM_CHANNEL: u32 = 25;
pub const SERVICE_UUID: &str = "7f2d1c40-9c1e-4c4e-9b6a-2f1c4a8d5e01";

const MSG: &str = "Bluetooth mode is only available in the Windows build of this application";

pub fn format_addr(addr: u64) -> String {
    format!("{:012X}", addr & 0x0000_FFFF_FFFF_FFFF)
}

pub fn parse_addr(s: &str) -> Result<u64> {
    let hex: String = s.chars().filter(|c| c.is_ascii_hexdigit()).collect();
    if hex.len() != 12 {
        return Err(Error::RoomCode(format!(
            "'{s}' is not a 12 hex digit Bluetooth address"
        )));
    }
    u64::from_str_radix(&hex, 16).map_err(|e| Error::RoomCode(e.to_string()))
}

pub fn parse_dial(s: &str) -> Result<(u64, u32)> {
    let (mac, chan) = s
        .rsplit_once(':')
        .ok_or_else(|| Error::RoomCode(format!("'{s}' is not ADDRESS:CHANNEL")))?;
    let addr = parse_addr(mac)?;
    let chan: u32 = chan
        .parse()
        .map_err(|_| Error::RoomCode(format!("'{chan}' is not a valid RFCOMM channel")))?;
    Ok((addr, chan))
}

pub fn listen() -> Result<Arc<dyn Listener>> {
    Err(Error::Unsupported(MSG.into()))
}

pub fn connect(_addr: &str, _timeout: Duration) -> Result<Arc<dyn Stream>> {
    Err(Error::Unsupported(MSG.into()))
}
