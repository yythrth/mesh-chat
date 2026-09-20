//! LAN transport: plain, direct TCP.
//!
//! No mDNS, no UDP broadcast, no service advertising of any kind. The listener
//! binds an ephemeral port on `0.0.0.0`; the host's IPv4 address and that port
//! are what the room code encodes. A joiner decodes the code and opens a TCP
//! connection straight to it.

use std::io::{self, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream, UdpSocket};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use super::{Listener, Stream};
use crate::{Error, Result};

pub struct TcpStreamConn {
    inner: TcpStream,
    label: String,
}

impl TcpStreamConn {
    pub fn new(inner: TcpStream) -> io::Result<Self> {
        inner.set_nodelay(true).ok();
        let label = inner
            .peer_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|_| "unknown".to_string());
        Ok(Self { inner, label })
    }
}

impl Stream for TcpStreamConn {
    fn read(&self, buf: &mut [u8]) -> io::Result<usize> {
        (&self.inner).read(buf)
    }

    fn write(&self, buf: &[u8]) -> io::Result<usize> {
        (&self.inner).write(buf)
    }

    fn close(&self) {
        let _ = self.inner.shutdown(Shutdown::Both);
    }

    fn remote_label(&self) -> String {
        self.label.clone()
    }

    fn set_read_timeout(&self, dur: Option<Duration>) -> io::Result<()> {
        self.inner.set_read_timeout(dur)
    }
}

pub struct TcpListenerEndpoint {
    inner: TcpListener,
    dial: String,
    local: SocketAddr,
    closed: AtomicBool,
}

impl Listener for TcpListenerEndpoint {
    fn accept(&self) -> io::Result<Arc<dyn Stream>> {
        loop {
            let (sock, _addr) = self.inner.accept()?;
            if self.closed.load(Ordering::SeqCst) {
                let _ = sock.shutdown(Shutdown::Both);
                return Err(io::Error::new(io::ErrorKind::Other, "listener closed"));
            }
            return Ok(Arc::new(TcpStreamConn::new(sock)?) as Arc<dyn Stream>);
        }
    }

    fn dial_address(&self) -> String {
        self.dial.clone()
    }

    fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        // std has no way to interrupt a blocking accept(); poke our own port so
        // the accept loop wakes up, sees the flag and exits.
        let target = SocketAddr::new("127.0.0.1".parse().unwrap(), self.local.port());
        if let Ok(s) = TcpStream::connect_timeout(&target, Duration::from_millis(300)) {
            let _ = s.shutdown(Shutdown::Both);
        }
    }
}

/// Best-effort local IPv4 discovery.
///
/// Opening a UDP socket and "connecting" it sends no packets; it just asks the
/// OS routing table which local address would be used. Falls back to the
/// loopback address when the machine has no usable route (still fine for
/// same-host testing).
pub fn local_ipv4() -> String {
    for probe in ["8.8.8.8:80", "192.168.1.1:80", "10.0.0.1:80"] {
        if let Ok(sock) = UdpSocket::bind("0.0.0.0:0") {
            if sock.connect(probe).is_ok() {
                if let Ok(addr) = sock.local_addr() {
                    if let SocketAddr::V4(v4) = addr {
                        if !v4.ip().is_unspecified() {
                            return v4.ip().to_string();
                        }
                    }
                }
            }
        }
    }
    "127.0.0.1".to_string()
}

pub fn listen() -> Result<Arc<dyn Listener>> {
    let inner = TcpListener::bind("0.0.0.0:0")?;
    let local = inner.local_addr()?;
    let dial = format!("{}:{}", local_ipv4(), local.port());
    Ok(Arc::new(TcpListenerEndpoint {
        inner,
        dial,
        local,
        closed: AtomicBool::new(false),
    }) as Arc<dyn Listener>)
}

pub fn connect(addr: &str, timeout: Duration) -> Result<Arc<dyn Stream>> {
    let sockaddr: SocketAddr = addr
        .parse()
        .map_err(|_| Error::RoomCode(format!("'{addr}' is not a valid IPv4 address:port")))?;
    let sock = TcpStream::connect_timeout(&sockaddr, timeout).map_err(|e| {
        Error::Closed(format!(
            "could not reach {addr} over the local network ({e}) - check that both machines are on the same network and that the host's firewall allows chat.exe"
        ))
    })?;
    Ok(Arc::new(TcpStreamConn::new(sock)?) as Arc<dyn Stream>)
}
