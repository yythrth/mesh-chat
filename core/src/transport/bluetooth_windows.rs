//! Bluetooth transport for Windows: RFCOMM over Winsock (`AF_BTH`).
//!
//! Design notes, because Bluetooth has a lot of ways to accidentally become
//! "discovery":
//!
//! * We never call `BluetoothEnableDiscovery`. The local radio's
//!   discoverability is left exactly as the user configured it in Windows; a
//!   non-discoverable adapter still accepts *directed* inbound RFCOMM
//!   connections, which is all we need.
//! * We never browse or inquire. There is no `BluetoothFindFirstDevice`, no
//!   SDP service search. The joiner dials a specific address on a specific
//!   RFCOMM channel, both of which come out of the room code.
//! * The host binds the fixed RFCOMM channel [`RFCOMM_CHANNEL`] when it is
//!   free, and otherwise lets the stack assign one; either way the channel
//!   actually in use is encoded into the room code, so the joiner never has to
//!   look anything up.
//!
//! The two devices must be paired once in Windows Settings first (Windows
//! refuses unauthenticated RFCOMM connections). Pairing is a one-time,
//! user-driven action and is not discovery.

use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Once};
use std::time::Duration;

use windows_sys::Win32::Networking::WinSock as ws;

use super::{Listener, Stream};
use crate::{Error, Result};

/// Service channel we prefer. Also the value published in the room code when
/// the bind succeeds. Range 1..=30 for RFCOMM.
pub const RFCOMM_CHANNEL: u32 = 25;

/// Service class UUID for this application. Kept as the canonical identifier
/// of the "portable p2p chat" RFCOMM service; the room code carries the
/// channel directly so no SDP lookup is performed by either side.
pub const SERVICE_UUID: &str = "7f2d1c40-9c1e-4c4e-9b6a-2f1c4a8d5e01";

const AF_BTH: i32 = 32;
const SOCK_STREAM: i32 = 1;
const BTHPROTO_RFCOMM: i32 = 3;
const BT_PORT_ANY: u32 = 0xFFFF_FFFF;
const INVALID_SOCKET: ws::SOCKET = !0;
const SOCKET_ERROR: i32 = -1;
const SOL_SOCKET_: i32 = 0xffff;
const SO_RCVTIMEO_: i32 = 0x1006;
const SD_BOTH: i32 = 2;

#[repr(C)]
#[derive(Clone, Copy)]
struct SockaddrBth {
    address_family: u16,
    bt_addr: u64,
    service_class_id: [u8; 16],
    port: u32,
}

impl SockaddrBth {
    fn new(addr: u64, port: u32) -> Self {
        Self {
            address_family: AF_BTH as u16,
            bt_addr: addr,
            service_class_id: [0u8; 16],
            port,
        }
    }
}

// --- Local radio address (bthprops.lib) -----------------------------------

#[repr(C)]
struct BluetoothFindRadioParams {
    dw_size: u32,
}

#[repr(C)]
struct BluetoothRadioInfo {
    dw_size: u32,
    address: u64,
    sz_name: [u16; 248],
    ul_class_of_device: u32,
    lmp_subversion: u16,
    manufacturer: u16,
}

#[link(name = "bthprops")]
extern "system" {
    fn BluetoothFindFirstRadio(params: *const BluetoothFindRadioParams, radio: *mut isize) -> isize;
    fn BluetoothFindRadioClose(find: isize) -> i32;
    fn BluetoothGetRadioInfo(radio: isize, info: *mut BluetoothRadioInfo) -> u32;
}

#[link(name = "kernel32")]
extern "system" {
    fn CloseHandle(h: isize) -> i32;
}

/// Address of the first local Bluetooth radio, as a 48-bit value.
fn local_radio_address() -> Result<u64> {
    unsafe {
        let params = BluetoothFindRadioParams {
            dw_size: std::mem::size_of::<BluetoothFindRadioParams>() as u32,
        };
        let mut radio: isize = 0;
        let find = BluetoothFindFirstRadio(&params, &mut radio);
        if find == 0 {
            return Err(Error::Unsupported(
                "no Bluetooth radio found on this machine (is Bluetooth switched on?)".into(),
            ));
        }
        let mut info: BluetoothRadioInfo = std::mem::zeroed();
        info.dw_size = std::mem::size_of::<BluetoothRadioInfo>() as u32;
        let rc = BluetoothGetRadioInfo(radio, &mut info);
        CloseHandle(radio);
        BluetoothFindRadioClose(find);
        if rc != 0 {
            return Err(Error::Unsupported(format!(
                "could not read the local Bluetooth radio information (Windows error {rc})"
            )));
        }
        Ok(info.address & 0x0000_FFFF_FFFF_FFFF)
    }
}

// --- Winsock helpers ------------------------------------------------------

fn wsa_init() {
    static START: Once = Once::new();
    START.call_once(|| unsafe {
        let mut data: ws::WSADATA = std::mem::zeroed();
        ws::WSAStartup(0x0202, &mut data);
    });
}

fn last_error() -> io::Error {
    io::Error::from_raw_os_error(unsafe { ws::WSAGetLastError() })
}

/// Format a 48-bit address as `AABBCCDDEEFF`.
pub fn format_addr(addr: u64) -> String {
    format!("{:012X}", addr & 0x0000_FFFF_FFFF_FFFF)
}

/// Parse `AABBCCDDEEFF`, `AA:BB:CC:DD:EE:FF` or `AA-BB-...` into 48 bits.
pub fn parse_addr(s: &str) -> Result<u64> {
    let hex: String = s.chars().filter(|c| c.is_ascii_hexdigit()).collect();
    if hex.len() != 12 {
        return Err(Error::RoomCode(format!(
            "'{s}' is not a 12 hex digit Bluetooth address"
        )));
    }
    u64::from_str_radix(&hex, 16).map_err(|e| Error::RoomCode(e.to_string()))
}

/// Split a dial string `AABBCCDDEEFF:CHANNEL`.
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

// --- Stream ---------------------------------------------------------------

pub struct BtStream {
    sock: ws::SOCKET,
    label: String,
    closed: AtomicBool,
}

unsafe impl Send for BtStream {}
unsafe impl Sync for BtStream {}

impl Stream for BtStream {
    fn read(&self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let n = unsafe { ws::recv(self.sock, buf.as_mut_ptr(), buf.len() as i32, 0) };
        if n == SOCKET_ERROR {
            Err(last_error())
        } else {
            Ok(n as usize)
        }
    }

    fn write(&self, buf: &[u8]) -> io::Result<usize> {
        let n = unsafe { ws::send(self.sock, buf.as_ptr(), buf.len() as i32, 0) };
        if n == SOCKET_ERROR {
            Err(last_error())
        } else {
            Ok(n as usize)
        }
    }

    fn close(&self) {
        if !self.closed.swap(true, Ordering::SeqCst) {
            unsafe {
                ws::shutdown(self.sock, SD_BOTH);
                ws::closesocket(self.sock);
            }
        }
    }

    fn remote_label(&self) -> String {
        self.label.clone()
    }

    fn set_read_timeout(&self, dur: Option<Duration>) -> io::Result<()> {
        let ms: u32 = dur.map(|d| d.as_millis().min(u32::MAX as u128) as u32).unwrap_or(0);
        let rc = unsafe {
            ws::setsockopt(
                self.sock,
                SOL_SOCKET_,
                SO_RCVTIMEO_,
                &ms as *const u32 as *const u8,
                std::mem::size_of::<u32>() as i32,
            )
        };
        if rc == SOCKET_ERROR {
            Err(last_error())
        } else {
            Ok(())
        }
    }
}

impl Drop for BtStream {
    fn drop(&mut self) {
        self.close();
    }
}

// --- Listener -------------------------------------------------------------

pub struct BtListener {
    sock: ws::SOCKET,
    dial: String,
    closed: AtomicBool,
}

unsafe impl Send for BtListener {}
unsafe impl Sync for BtListener {}

impl Listener for BtListener {
    fn accept(&self) -> io::Result<Arc<dyn Stream>> {
        let mut addr: SockaddrBth = SockaddrBth::new(0, 0);
        let mut len = std::mem::size_of::<SockaddrBth>() as i32;
        let s = unsafe {
            ws::accept(
                self.sock,
                &mut addr as *mut SockaddrBth as *mut ws::SOCKADDR,
                &mut len,
            )
        };
        if s == INVALID_SOCKET {
            return Err(if self.closed.load(Ordering::SeqCst) {
                io::Error::new(io::ErrorKind::Other, "listener closed")
            } else {
                last_error()
            });
        }
        Ok(Arc::new(BtStream {
            sock: s,
            label: format_addr(addr.bt_addr),
            closed: AtomicBool::new(false),
        }) as Arc<dyn Stream>)
    }

    fn dial_address(&self) -> String {
        self.dial.clone()
    }

    fn close(&self) {
        if !self.closed.swap(true, Ordering::SeqCst) {
            unsafe {
                ws::closesocket(self.sock);
            }
        }
    }
}

impl Drop for BtListener {
    fn drop(&mut self) {
        self.close();
    }
}

pub fn listen() -> Result<Arc<dyn Listener>> {
    wsa_init();
    let radio = local_radio_address()?;
    unsafe {
        let sock = ws::socket(AF_BTH, SOCK_STREAM, BTHPROTO_RFCOMM);
        if sock == INVALID_SOCKET {
            return Err(Error::Unsupported(format!(
                "could not create a Bluetooth socket ({}). The Microsoft Bluetooth stack is required.",
                last_error()
            )));
        }

        // Prefer the well-known channel; fall back to whatever is free.
        let mut bound = SockaddrBth::new(0, RFCOMM_CHANNEL);
        let mut rc = ws::bind(
            sock,
            &bound as *const SockaddrBth as *const ws::SOCKADDR,
            std::mem::size_of::<SockaddrBth>() as i32,
        );
        if rc == SOCKET_ERROR {
            bound = SockaddrBth::new(0, BT_PORT_ANY);
            rc = ws::bind(
                sock,
                &bound as *const SockaddrBth as *const ws::SOCKADDR,
                std::mem::size_of::<SockaddrBth>() as i32,
            );
        }
        if rc == SOCKET_ERROR {
            let e = last_error();
            ws::closesocket(sock);
            return Err(Error::Io(e));
        }

        // Ask the stack which channel we actually got.
        let mut actual: SockaddrBth = SockaddrBth::new(0, 0);
        let mut len = std::mem::size_of::<SockaddrBth>() as i32;
        let channel = if ws::getsockname(
            sock,
            &mut actual as *mut SockaddrBth as *mut ws::SOCKADDR,
            &mut len,
        ) == 0
            && actual.port != 0
            && actual.port != BT_PORT_ANY
        {
            actual.port
        } else {
            RFCOMM_CHANNEL
        };

        if ws::listen(sock, 8) == SOCKET_ERROR {
            let e = last_error();
            ws::closesocket(sock);
            return Err(Error::Io(e));
        }

        Ok(Arc::new(BtListener {
            sock,
            dial: format!("{}:{}", format_addr(radio), channel),
            closed: AtomicBool::new(false),
        }) as Arc<dyn Listener>)
    }
}

pub fn connect(addr: &str, _timeout: Duration) -> Result<Arc<dyn Stream>> {
    wsa_init();
    let (target, channel) = parse_dial(addr)?;
    unsafe {
        let sock = ws::socket(AF_BTH, SOCK_STREAM, BTHPROTO_RFCOMM);
        if sock == INVALID_SOCKET {
            return Err(Error::Unsupported(format!(
                "could not create a Bluetooth socket ({})",
                last_error()
            )));
        }
        let sa = SockaddrBth::new(target, channel);
        let rc = ws::connect(
            sock,
            &sa as *const SockaddrBth as *const ws::SOCKADDR,
            std::mem::size_of::<SockaddrBth>() as i32,
        );
        if rc == SOCKET_ERROR {
            let e = last_error();
            ws::closesocket(sock);
            return Err(Error::Closed(format!(
                "could not reach Bluetooth device {} on channel {} ({}). Make sure the two devices are paired in Windows Settings and that the host is waiting for a joiner.",
                format_addr(target),
                channel,
                e
            )));
        }
        Ok(Arc::new(BtStream {
            sock,
            label: format_addr(target),
            closed: AtomicBool::new(false),
        }) as Arc<dyn Stream>)
    }
}
