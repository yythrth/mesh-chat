//! Deliverable 8: full-mesh connection setup, send/receive across every peer
//! pair, and disconnect handling.

mod common;

use chat_core::mesh::{Event, PeerState};
use common::*;

#[test]
fn three_peers_form_a_full_mesh_and_every_pair_can_talk() {
    let (host, hrx, code) = open_room("Host");
    let (alice, arx) = join_approved("Alice", &code, &host, &hrx);
    let (bob, brx) = join_approved("Bob", &code, &host, &hrx);

    // Host<->Alice, Host<->Bob, Alice<->Bob: every node holds two links.
    wait_peer_count(&host, 2);
    wait_peer_count(&alice, 2);
    wait_peer_count(&bob, 2);

    // Alice and Bob linked to each other directly, not through the host.
    assert!(alice.peer_names().contains(&"Bob".to_string()));
    assert!(bob.peer_names().contains(&"Alice".to_string()));

    host.send_chat("hello from the host").unwrap();
    assert_eq!(wait_chat(&arx, "hello from the host"), "Host");
    assert_eq!(wait_chat(&brx, "hello from the host"), "Host");

    alice.send_chat("alice speaking").unwrap();
    assert_eq!(wait_chat(&hrx, "alice speaking"), "Alice");
    assert_eq!(wait_chat(&brx, "alice speaking"), "Alice");

    bob.send_chat("bob speaking").unwrap();
    assert_eq!(wait_chat(&hrx, "bob speaking"), "Bob");
    assert_eq!(wait_chat(&arx, "bob speaking"), "Bob");
}

#[test]
fn messages_still_flow_between_joiners_after_the_host_leaves() {
    let (host, hrx, code) = open_room("Host");
    let (alice, arx) = join_approved("Alice", &code, &host, &hrx);
    let (bob, brx) = join_approved("Bob", &code, &host, &hrx);
    wait_peer_count(&alice, 2);
    wait_peer_count(&bob, 2);

    host.leave();
    wait_peer_count(&alice, 1);
    wait_peer_count(&bob, 1);

    // The Alice<->Bob link was never owned by the host, so it survives.
    alice.send_chat("still here").unwrap();
    assert_eq!(wait_chat(&brx, "still here"), "Alice");
    bob.send_chat("me too").unwrap();
    assert_eq!(wait_chat(&arx, "me too"), "Bob");
}

#[test]
fn a_disconnect_is_reported_with_a_reason() {
    let (host, hrx, code) = open_room("Host");
    let (alice, _arx) = join_approved("Alice", &code, &host, &hrx);
    wait_peer_count(&host, 1);

    alice.leave();

    let detail = wait_for(&hrx, T, |e| match e {
        Event::Peer {
            name,
            state: PeerState::Disconnected,
            detail,
            ..
        } if name == "Alice" => Some(detail.clone()),
        _ => None,
    })
    .expect("the host should see Alice disconnect");
    assert!(!detail.is_empty());
    wait_peer_count(&host, 0);
}

#[test]
fn the_transcript_records_messages_from_both_sides() {
    let (host, hrx, code) = open_room("Host");
    let (alice, arx) = join_approved("Alice", &code, &host, &hrx);
    wait_peer_count(&host, 1);

    host.send_chat("one").unwrap();
    wait_chat(&arx, "one");
    alice.send_chat("two").unwrap();
    wait_chat(&hrx, "two");

    let dir = std::env::temp_dir().join(format!("chat-export-{}.txt", std::process::id()));
    host.export_history(&dir).unwrap();
    let text = std::fs::read_to_string(&dir).unwrap();
    std::fs::remove_file(&dir).ok();

    assert!(text.contains("Host: one"), "{text}");
    assert!(text.contains("Alice: two"), "{text}");
    assert!(text.contains("Transport : LAN"), "{text}");
}
