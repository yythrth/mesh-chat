//! Deliverable 8: file transfer over the encrypted channel, including the
//! accept/decline prompt, chunked streaming and progress reporting.

mod common;

use std::path::PathBuf;

use chat_core::mesh::Event;
use common::*;

fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("chatfiles-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir.join(name)
}

fn make_file(name: &str, size: usize) -> PathBuf {
    let path = scratch(name);
    // Not compressible, not all-zero: catches any chunking or ordering bug.
    let data: Vec<u8> = (0..size).map(|i| ((i * 31 + 7) % 251) as u8).collect();
    std::fs::write(&path, &data).unwrap();
    path
}

#[test]
fn an_accepted_file_arrives_byte_for_byte() {
    let (host, hrx, code) = open_room("Host");
    let (alice, arx) = join_approved("Alice", &code, &host, &hrx);
    wait_peer_count(&host, 1);

    // Several chunks plus a partial one.
    let src = make_file("payload.bin", 200 * 1024 + 123);
    host.send_file(src.clone()).unwrap();

    let (transfer, name, size) = wait_for(&arx, T, |e| match e {
        Event::FileOffer {
            transfer,
            name,
            size,
            ..
        } => Some((transfer.clone(), name.clone(), *size)),
        _ => None,
    })
    .expect("the recipient should be prompted");
    assert_eq!(name, "payload.bin");
    assert_eq!(size, 200 * 1024 + 123);

    let dest = scratch("received.bin");
    alice.respond_offer(&transfer, Some(dest.clone())).unwrap();

    let mut saw_progress = false;
    let ok = wait_for(&arx, T, |e| match e {
        Event::FileProgress { incoming: true, .. } => {
            saw_progress = true;
            None
        }
        Event::FileFinished {
            incoming: true,
            ok,
            detail,
            ..
        } => Some((*ok, detail.clone())),
        _ => None,
    })
    .expect("the transfer should finish");
    assert!(ok.0, "transfer failed: {}", ok.1);
    assert!(saw_progress, "progress should be reported to the recipient");

    wait_for(&hrx, T, |e| match e {
        Event::FileFinished {
            incoming: false,
            ok: true,
            ..
        } => Some(()),
        _ => None,
    })
    .expect("the sender should see success too");

    assert_eq!(
        std::fs::read(&src).unwrap(),
        std::fs::read(&dest).unwrap(),
        "received bytes must match the source exactly"
    );
    std::fs::remove_file(src).ok();
    std::fs::remove_file(dest).ok();
}

#[test]
fn a_declined_file_is_never_written() {
    let (host, hrx, code) = open_room("Host");
    let (alice, arx) = join_approved("Alice", &code, &host, &hrx);
    wait_peer_count(&host, 1);

    let src = make_file("nope.bin", 4096);
    host.send_file(src.clone()).unwrap();

    let transfer = wait_for(&arx, T, |e| match e {
        Event::FileOffer { transfer, .. } => Some(transfer.clone()),
        _ => None,
    })
    .expect("offer");
    alice.respond_offer(&transfer, None).unwrap();

    let detail = wait_for(&hrx, T, |e| match e {
        Event::FileFinished {
            incoming: false,
            ok: false,
            detail,
            ..
        } => Some(detail.clone()),
        _ => None,
    })
    .expect("the sender should learn it was declined");
    assert!(detail.to_lowercase().contains("declined"), "{detail}");
    std::fs::remove_file(src).ok();
}

#[test]
fn a_file_sent_to_the_room_reaches_every_peer() {
    let (host, hrx, code) = open_room("Host");
    let (alice, arx) = join_approved("Alice", &code, &host, &hrx);
    let (bob, brx) = join_approved("Bob", &code, &host, &hrx);
    wait_peer_count(&host, 2);

    let src = make_file("group.bin", 64 * 1024);
    host.send_file(src.clone()).unwrap();

    for (who, rx, node) in [
        ("alice", &arx, &alice),
        ("bob", &brx, &bob),
    ] {
        let transfer = wait_for(rx, T, |e| match e {
            Event::FileOffer { transfer, .. } => Some(transfer.clone()),
            _ => None,
        })
        .unwrap_or_else(|| panic!("{who} should be offered the file"));
        let dest = scratch(&format!("group-{who}.bin"));
        node.respond_offer(&transfer, Some(dest.clone())).unwrap();
        let ok = wait_for(rx, T, |e| match e {
            Event::FileFinished {
                incoming: true, ok, ..
            } => Some(*ok),
            _ => None,
        })
        .unwrap_or_else(|| panic!("{who} should finish the transfer"));
        assert!(ok, "{who} transfer failed");
        assert_eq!(std::fs::read(&src).unwrap(), std::fs::read(&dest).unwrap());
        std::fs::remove_file(dest).ok();
    }
    std::fs::remove_file(src).ok();
}

#[test]
fn chat_still_works_while_a_file_is_in_flight() {
    let (host, hrx, code) = open_room("Host");
    let (alice, arx) = join_approved("Alice", &code, &host, &hrx);
    wait_peer_count(&host, 1);

    let src = make_file("big.bin", 1024 * 1024);
    host.send_file(src.clone()).unwrap();
    let transfer = wait_for(&arx, T, |e| match e {
        Event::FileOffer { transfer, .. } => Some(transfer.clone()),
        _ => None,
    })
    .expect("offer");
    let dest = scratch("big-received.bin");
    alice.respond_offer(&transfer, Some(dest.clone())).unwrap();

    host.send_chat("still chatting").unwrap();
    assert_eq!(wait_chat(&arx, "still chatting"), "Host");

    wait_for(&arx, T, |e| match e {
        Event::FileFinished {
            incoming: true,
            ok: true,
            ..
        } => Some(()),
        _ => None,
    })
    .expect("transfer completes");
    assert_eq!(std::fs::read(&src).unwrap(), std::fs::read(&dest).unwrap());
    std::fs::remove_file(src).ok();
    std::fs::remove_file(dest).ok();
}
