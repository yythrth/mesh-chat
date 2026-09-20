//! Regression tests for the timing-sensitive parts: repeated join/leave churn
//! and repeated mesh formation. These caught a real race where a peer that
//! attached while a node was tearing down stayed in the map with a live
//! socket, so nobody ever observed the disconnect.

mod common;

use std::time::Duration;

use chat_core::mesh::{Event, PeerState};
use common::*;

#[test]
fn repeated_join_and_leave_always_reports_the_disconnect() {
    for i in 0..25 {
        let (host, hrx, code) = open_room("Host");
        let (alice, _arx) = join_approved("Alice", &code, &host, &hrx);
        wait_peer_count(&host, 1);

        alice.leave();

        let seen = wait_for(&hrx, Duration::from_secs(10), |e| match e {
            Event::Peer {
                name,
                state: PeerState::Disconnected,
                ..
            } if name == "Alice" => Some(()),
            _ => None,
        });
        assert!(seen.is_some(), "iteration {i}: no disconnect event");
        assert!(alice.connected_peers().is_empty());
        host.leave();
    }
}

#[test]
fn repeated_three_peer_mesh_formation_is_reliable() {
    for i in 0..10 {
        let (host, hrx, code) = open_room("Host");
        let (alice, _arx) = join_approved("Alice", &code, &host, &hrx);
        let (bob, brx) = join_approved("Bob", &code, &host, &hrx);

        wait_peer_count(&host, 2);
        wait_peer_count(&alice, 2);
        wait_peer_count(&bob, 2);

        alice.send_chat("ping").unwrap();
        assert_eq!(wait_chat(&brx, "ping"), "Alice", "iteration {i}");
        assert_eq!(wait_chat(&hrx, "ping"), "Alice", "iteration {i}");

        alice.leave();
        bob.leave();
        host.leave();
    }
}

#[test]
fn a_joiner_that_leaves_before_approval_does_not_wedge_the_host() {
    let (host, hrx, code) = open_room("Host");
    let (alice, _arx) = node("Alice");
    alice.join(chat_core::transport::TransportKind::Lan, &code).unwrap();
    let req = wait_join_request(&hrx);
    alice.leave();

    // Approving a vanished request must fail cleanly, not panic or hang.
    let _ = host.approve(&req, true);
    assert!(host.pending_requests().is_empty());

    // ...and the host still works afterwards.
    let (carol, crx) = join_approved("Carol", &code, &host, &hrx);
    wait_peer_count(&host, 1);
    host.send_chat("still alive").unwrap();
    assert_eq!(wait_chat(&crx, "still alive"), "Host");
    carol.leave();
}
