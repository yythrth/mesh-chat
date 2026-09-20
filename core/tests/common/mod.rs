//! Helpers shared by the integration tests.
//!
//! Every test drives real `Node`s over the real LAN transport on loopback, so
//! what is exercised is the actual approval flow, the actual Noise handshakes
//! and the actual mesh wiring - not mocks.

#![allow(dead_code)]

use std::sync::mpsc::Receiver;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chat_core::mesh::{Event, Node, PeerState};
use chat_core::transport::TransportKind;

pub const T: Duration = Duration::from_secs(15);

pub fn node(name: &str) -> (Arc<Node>, Receiver<Event>) {
    Node::new(name).expect("node")
}

/// Pull events until `f` returns `Some`, or give up after `timeout`.
pub fn wait_for<T>(
    rx: &Receiver<Event>,
    timeout: Duration,
    mut f: impl FnMut(&Event) -> Option<T>,
) -> Option<T> {
    let deadline = Instant::now() + timeout;
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return None;
        }
        match rx.recv_timeout(left) {
            Ok(ev) => {
                if let Some(v) = f(&ev) {
                    return Some(v);
                }
            }
            Err(_) => return None,
        }
    }
}

pub fn wait_join_request(rx: &Receiver<Event>) -> String {
    wait_for(rx, T, |e| match e {
        Event::JoinRequest { req, .. } => Some(req.clone()),
        _ => None,
    })
    .expect("a join request should reach the host")
}

pub fn wait_connected(rx: &Receiver<Event>, name: &str) {
    wait_for(rx, T, |e| match e {
        Event::Peer {
            name: n,
            state: PeerState::Connected,
            ..
        } if n == name => Some(()),
        _ => None,
    })
    .unwrap_or_else(|| panic!("{name} never reported as connected"));
}

pub fn wait_chat(rx: &Receiver<Event>, body: &str) -> String {
    wait_for(rx, T, |e| match e {
        Event::Chat {
            body: b,
            name,
            mine: false,
            ..
        } if b == body => Some(name.clone()),
        _ => None,
    })
    .unwrap_or_else(|| panic!("message {body:?} never arrived"))
}

/// Wait until a node has `n` live links.
pub fn wait_peer_count(node: &Arc<Node>, n: usize) {
    let deadline = Instant::now() + T;
    while Instant::now() < deadline {
        if node.connected_peers().len() == n {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!(
        "expected {n} peers, found {}",
        node.connected_peers().len()
    );
}

/// Host a room and return `(node, events, code)`.
pub fn open_room(name: &str) -> (Arc<Node>, Receiver<Event>, String) {
    let (host, rx) = node(name);
    let code = host
        .host(TransportKind::Lan, Duration::from_secs(120))
        .expect("host should be able to open a LAN room");
    (host, rx, code)
}

/// Join `code` and get approved by `host`.
pub fn join_approved(
    name: &str,
    code: &str,
    host: &Arc<Node>,
    host_rx: &Receiver<Event>,
) -> (Arc<Node>, Receiver<Event>) {
    let (joiner, rx) = node(name);
    joiner
        .join(TransportKind::Lan, code)
        .expect("join request should be sent");
    let req = wait_join_request(host_rx);
    host.approve(&req, true).expect("approve");
    wait_for(&rx, T, |e| match e {
        Event::Joined { .. } => Some(()),
        _ => None,
    })
    .expect("joiner should be welcomed");
    (joiner, rx)
}
