//! Deliverable 8: the host approval flow.

mod common;

use std::time::Duration;

use chat_core::mesh::{Event, Node};
use chat_core::transport::TransportKind;
use common::*;

#[test]
fn joiner_stays_pending_until_the_host_accepts() {
    let (host, hrx, code) = open_room("Host");
    let (joiner, jrx) = node("Joiner");
    joiner.join(TransportKind::Lan, &code).unwrap();

    let req = wait_join_request(&hrx);

    // Pending means pending: no link exists on either side yet.
    assert_eq!(joiner.connected_peers().len(), 0);
    assert_eq!(host.connected_peers().len(), 0);
    assert_eq!(host.pending_requests().len(), 1);

    host.approve(&req, true).unwrap();

    wait_for(&jrx, T, |e| match e {
        Event::Joined { .. } => Some(()),
        _ => None,
    })
    .expect("joiner welcomed");
    wait_connected(&hrx, "Joiner");
    wait_peer_count(&host, 1);
    wait_peer_count(&joiner, 1);
    assert!(host.pending_requests().is_empty());
}

#[test]
fn declined_joiner_is_told_why_and_gets_no_link() {
    let (host, hrx, code) = open_room("Host");
    let (joiner, jrx) = node("Rejected Randy");
    joiner.join(TransportKind::Lan, &code).unwrap();

    let req = wait_join_request(&hrx);
    host.approve(&req, false).unwrap();

    let reason = wait_for(&jrx, T, |e| match e {
        Event::RoomClosed(r) => Some(r.clone()),
        _ => None,
    })
    .expect("joiner should be told it was declined");
    assert!(reason.to_lowercase().contains("declined"), "{reason}");
    assert_eq!(joiner.connected_peers().len(), 0);
    assert_eq!(host.connected_peers().len(), 0);
}

#[test]
fn a_code_pointing_nowhere_fails_with_a_readable_reason() {
    // A structurally valid code for an address nobody is listening on.
    let dead_port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let code =
        chat_core::roomcode::encode(TransportKind::Lan, &format!("127.0.0.1:{dead_port}")).unwrap();

    let (joiner, _jrx) = node("Joiner");
    let err = joiner
        .join(TransportKind::Lan, &code)
        .expect_err("a code pointing nowhere must not connect");
    let msg = err.to_string();
    assert!(msg.contains("could not reach"), "{msg}");
    assert!(!joiner.is_active(), "a failed join must not leave the node in a room");
}

#[test]
fn a_malformed_code_is_rejected_before_any_socket_is_opened() {
    let (joiner, _jrx) = node("Joiner");
    for bad in ["", "hello", "Z-ABCDE-FGHIJ", "L-AB"] {
        assert!(
            joiner.join(TransportKind::Lan, bad).is_err(),
            "{bad:?} should be rejected"
        );
    }
}

#[test]
fn a_bluetooth_code_is_refused_in_lan_mode() {
    let bt = chat_core::roomcode::encode(
        chat_core::transport::TransportKind::Bluetooth,
        "A4B1C2D3E4F5:25",
    )
    .unwrap();
    let (joiner, _jrx) = node("Joiner");
    let err = joiner.join(TransportKind::Lan, &bt).unwrap_err();
    assert!(err.to_string().contains("Bluetooth"), "{err}");
}

#[test]
fn the_code_expires_and_can_be_reopened() {
    let (host, hrx) = node("Host");
    let code = host
        .host(TransportKind::Lan, Duration::from_millis(200))
        .unwrap();
    std::thread::sleep(Duration::from_millis(400));

    let (late, lrx) = node("Late Larry");
    late.join(TransportKind::Lan, &code).unwrap();
    let reason = wait_for(&lrx, T, |e| match e {
        Event::RoomClosed(r) => Some(r.clone()),
        _ => None,
    })
    .expect("late joiner should be refused");
    assert!(reason.contains("expired"), "{reason}");

    host.reopen_joining(Duration::from_secs(60)).unwrap();
    let (second, srx) = node("Second Try");
    second.join(TransportKind::Lan, &code).unwrap();
    let req = wait_join_request(&hrx);
    host.approve(&req, true).unwrap();
    wait_for(&srx, T, |e| match e {
        Event::Joined { .. } => Some(()),
        _ => None,
    })
    .expect("joining works again after reopening");
}

#[test]
fn a_node_can_only_be_in_one_room_at_a_time() {
    let (host, _hrx, _code) = open_room("Host");
    assert!(host.host(TransportKind::Lan, Duration::from_secs(60)).is_err());
    assert!(Node::new("x").is_ok());
}
