// Portable P2P group chat - Windows front end.
//
// No console window in release builds; the debug build keeps one so that
// `println!`/panic output is visible while developing.
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use std::cell::RefCell;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chat_core::files::human_size;
use chat_core::mesh::{Event, Node, PeerState, DEFAULT_CODE_TTL};
use chat_core::protocol::fmt_clock;
use chat_core::transport::TransportKind;

use slint::{ComponentHandle, Model, ModelRc, SharedString, Timer, TimerMode, VecModel};

slint::include_modules!();

/// How often the UI thread drains the engine's event queue.
const TICK: Duration = Duration::from_millis(50);
/// Upper bound on events handled per tick, so a burst cannot freeze the frame.
const MAX_EVENTS_PER_TICK: usize = 256;
/// Transcript cap held in the list view (the full history stays in the core).
const MAX_VISIBLE_MESSAGES: usize = 2000;

/// Messages produced by the worker threads that run the blocking
/// `host()` / `join()` calls, plus local (non-engine) notices.
enum UiMsg {
    /// Starting the room failed before the engine had a chance to emit events.
    StartFailed(String),
    /// A purely local note for the status line.
    Note(String),
}

/// Everything the callbacks and the timer share.
struct App {
    node: Option<Arc<Node>>,
    rx: Option<Receiver<Event>>,
    ui_tx: Sender<UiMsg>,
    ui_rx: Receiver<UiMsg>,

    messages: Rc<VecModel<MessageRow>>,
    peers: Rc<VecModel<PeerRow>>,
    requests: Rc<VecModel<RequestRow>>,
    transfers: Rc<VecModel<TransferRow>>,

    /// Incoming file offers waiting for an Accept/Decline decision.
    offers: VecDeque<(String, String)>,
    /// When the room code stops accepting new joiners (host only).
    expires_at: Option<Instant>,
    /// Last rendered countdown, so we only touch the property when it changes.
    last_expiry: String,
}

impl App {
    fn new() -> Self {
        let (ui_tx, ui_rx) = mpsc::channel();
        App {
            node: None,
            rx: None,
            ui_tx,
            ui_rx,
            messages: Rc::new(VecModel::default()),
            peers: Rc::new(VecModel::default()),
            requests: Rc::new(VecModel::default()),
            transfers: Rc::new(VecModel::default()),
            offers: VecDeque::new(),
            expires_at: None,
            last_expiry: String::new(),
        }
    }

    fn reset_models(&mut self) {
        self.messages.set_vec(Vec::new());
        self.peers.set_vec(Vec::new());
        self.requests.set_vec(Vec::new());
        self.transfers.set_vec(Vec::new());
        self.offers.clear();
        self.expires_at = None;
        self.last_expiry.clear();
    }

    fn push_message(&self, row: MessageRow) {
        if self.messages.row_count() >= MAX_VISIBLE_MESSAGES {
            self.messages.remove(0);
        }
        self.messages.push(row);
    }

    fn push_system(&self, text: &str) {
        self.push_message(MessageRow {
            who: SharedString::from(""),
            time: SharedString::from(fmt_clock(chat_core::protocol::now_ts())),
            body: SharedString::from(text),
            mine: false,
            system: true,
        });
    }

    fn upsert_peer(&self, id: &str, name: &str, status: &str, detail: &str) {
        let row = PeerRow {
            id: SharedString::from(id),
            name: SharedString::from(name),
            status: SharedString::from(status),
            detail: SharedString::from(detail),
        };
        for i in 0..self.peers.row_count() {
            if self.peers.row_data(i).map(|r| r.id == row.id).unwrap_or(false) {
                self.peers.set_row_data(i, row);
                return;
            }
        }
        self.peers.push(row);
    }

    fn remove_request(&self, req: &str) {
        for i in 0..self.requests.row_count() {
            if self
                .requests
                .row_data(i)
                .map(|r| r.req == req)
                .unwrap_or(false)
            {
                self.requests.remove(i);
                return;
            }
        }
    }

    fn upsert_transfer(&self, id: &str, label: &str, progress: f32, state: &str) {
        let row = TransferRow {
            id: SharedString::from(id),
            label: SharedString::from(label),
            progress,
            state: SharedString::from(state),
        };
        for i in 0..self.transfers.row_count() {
            if self
                .transfers
                .row_data(i)
                .map(|r| r.id == row.id)
                .unwrap_or(false)
            {
                self.transfers.set_row_data(i, row);
                return;
            }
        }
        self.transfers.push(row);
    }
}

type Shared = Rc<RefCell<App>>;

fn main() -> Result<(), slint::PlatformError> {
    let ui = AppWindow::new()?;
    let app: Shared = Rc::new(RefCell::new(App::new()));

    {
        let a = app.borrow();
        ui.set_messages(ModelRc::from(a.messages.clone()));
        ui.set_peers(ModelRc::from(a.peers.clone()));
        ui.set_requests(ModelRc::from(a.requests.clone()));
        ui.set_transfers(ModelRc::from(a.transfers.clone()));
    }
    ui.set_status_line(SharedString::from("Pick a connection mode, then host or join a room."));

    wire_callbacks(&ui, &app);

    // Single UI-thread pump: the engine runs on its own threads and only ever
    // talks to the interface through this queue.
    let timer = Timer::default();
    {
        let weak = ui.as_weak();
        let app = app.clone();
        timer.start(TimerMode::Repeated, TICK, move || {
            if let Some(ui) = weak.upgrade() {
                pump(&ui, &app);
            }
        });
    }

    ui.run()?;
    // Keep the timer alive for the whole lifetime of the window.
    drop(timer);

    // Best-effort clean shutdown: tell every peer we are going.
    if let Some(node) = app.borrow().node.clone() {
        node.leave();
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// callbacks
// ---------------------------------------------------------------------------

fn wire_callbacks(ui: &AppWindow, app: &Shared) {
    // --- host ------------------------------------------------------------
    {
        let weak = ui.as_weak();
        let app = app.clone();
        ui.on_host_room(move || {
            let ui = match weak.upgrade() {
                Some(u) => u,
                None => return,
            };
            let kind = match mode_of(&ui) {
                Some(k) => k,
                None => return,
            };
            let node = match start_node(&ui, &app) {
                Some(n) => n,
                None => return,
            };
            ui.set_error_line(SharedString::from(""));
            ui.set_status_line(SharedString::from("Opening the room..."));
            ui.set_busy(true);
            let tx = app.borrow().ui_tx.clone();
            std::thread::spawn(move || {
                if let Err(e) = node.host(kind, DEFAULT_CODE_TTL) {
                    let _ = tx.send(UiMsg::StartFailed(e.to_string()));
                }
            });
        });
    }

    // --- join ------------------------------------------------------------
    {
        let weak = ui.as_weak();
        let app = app.clone();
        ui.on_join_room(move || {
            let ui = match weak.upgrade() {
                Some(u) => u,
                None => return,
            };
            let kind = match mode_of(&ui) {
                Some(k) => k,
                None => return,
            };
            let code = ui.get_join_code().trim().to_string();
            if code.is_empty() {
                ui.set_error_line(SharedString::from("Enter the room code the host gave you."));
                return;
            }
            let node = match start_node(&ui, &app) {
                Some(n) => n,
                None => return,
            };
            ui.set_error_line(SharedString::from(""));
            ui.set_status_line(SharedString::from("Contacting the host..."));
            ui.set_busy(true);
            let tx = app.borrow().ui_tx.clone();
            std::thread::spawn(move || {
                if let Err(e) = node.join(kind, &code) {
                    let _ = tx.send(UiMsg::StartFailed(e.to_string()));
                }
            });
        });
    }

    // --- approve / decline a pending joiner -------------------------------
    {
        let weak = ui.as_weak();
        let app = app.clone();
        ui.on_approve(move |req, accept| {
            let ui = match weak.upgrade() {
                Some(u) => u,
                None => return,
            };
            let node = match app.borrow().node.clone() {
                Some(n) => n,
                None => return,
            };
            // Take the row out immediately so the button cannot fire twice.
            app.borrow().remove_request(req.as_str());
            let req = req.to_string();
            let tx = app.borrow().ui_tx.clone();
            let _ = ui;
            std::thread::spawn(move || {
                if let Err(e) = node.approve(&req, accept) {
                    let _ = tx.send(UiMsg::Note(e.to_string()));
                }
            });
        });
    }

    // --- send a chat message ---------------------------------------------
    {
        let weak = ui.as_weak();
        let app = app.clone();
        ui.on_send_message(move || {
            let ui = match weak.upgrade() {
                Some(u) => u,
                None => return,
            };
            let body = ui.get_draft().trim().to_string();
            if body.is_empty() {
                return;
            }
            let node = match app.borrow().node.clone() {
                Some(n) => n,
                None => return,
            };
            match node.send_chat(&body) {
                Ok(()) => {
                    ui.set_draft(SharedString::from(""));
                    ui.set_error_line(SharedString::from(""));
                }
                Err(e) => ui.set_error_line(SharedString::from(e.to_string())),
            }
        });
    }

    // --- send a file to every connected peer ------------------------------
    {
        let weak = ui.as_weak();
        let app = app.clone();
        ui.on_send_file(move || {
            let ui = match weak.upgrade() {
                Some(u) => u,
                None => return,
            };
            let node = match app.borrow().node.clone() {
                Some(n) => n,
                None => return,
            };
            let picked = rfd::FileDialog::new()
                .set_title("Send a file to everyone in the room")
                .pick_file();
            let path: PathBuf = match picked {
                Some(p) => p,
                None => return,
            };
            if let Err(e) = node.send_file(path) {
                ui.set_error_line(SharedString::from(e.to_string()));
            } else {
                ui.set_error_line(SharedString::from(""));
            }
        });
    }

    // --- accept / decline an incoming file --------------------------------
    {
        let weak = ui.as_weak();
        let app = app.clone();
        ui.on_respond_offer(move |transfer, accept| {
            let ui = match weak.upgrade() {
                Some(u) => u,
                None => return,
            };
            let node = match app.borrow().node.clone() {
                Some(n) => n,
                None => return,
            };
            let transfer = transfer.to_string();

            let mut save_to: Option<PathBuf> = None;
            if accept {
                // Suggest the sender's filename, but the user always chooses
                // the folder and the final name. Nothing is ever auto-opened.
                let suggested = app
                    .borrow()
                    .offers
                    .iter()
                    .find(|(id, _)| *id == transfer)
                    .map(|(_, text)| suggested_name(text))
                    .unwrap_or_else(|| "received-file".to_string());
                save_to = rfd::FileDialog::new()
                    .set_title("Save the received file as")
                    .set_file_name(&suggested)
                    .save_file();
                if save_to.is_none() {
                    // Cancelled the save dialog: leave the offer on screen.
                    return;
                }
            }

            {
                let mut a = app.borrow_mut();
                a.offers.retain(|(id, _)| *id != transfer);
            }
            show_next_offer(&ui, &app);

            if let Err(e) = node.respond_offer(&transfer, save_to) {
                ui.set_error_line(SharedString::from(e.to_string()));
            }
        });
    }

    // --- export the transcript --------------------------------------------
    {
        let weak = ui.as_weak();
        let app = app.clone();
        ui.on_export_history(move || {
            let ui = match weak.upgrade() {
                Some(u) => u,
                None => return,
            };
            let node = match app.borrow().node.clone() {
                Some(n) => n,
                None => return,
            };
            let picked = rfd::FileDialog::new()
                .set_title("Download chat history")
                .add_filter("Text transcript", &["txt"])
                .add_filter("JSON", &["json"])
                .set_file_name("chat-history.txt")
                .save_file();
            let path = match picked {
                Some(p) => p,
                None => return,
            };
            match node.export_history(&path) {
                Ok(()) => {
                    ui.set_error_line(SharedString::from(""));
                    ui.set_status_line(SharedString::from(format!(
                        "Transcript saved to {}",
                        path.display()
                    )));
                }
                Err(e) => ui.set_error_line(SharedString::from(e.to_string())),
            }
        });
    }

    // --- copy the room code ------------------------------------------------
    {
        let weak = ui.as_weak();
        ui.on_copy_code(move || {
            let ui = match weak.upgrade() {
                Some(u) => u,
                None => return,
            };
            let code = ui.get_room_code().to_string();
            if code.is_empty() {
                return;
            }
            match arboard::Clipboard::new().and_then(|mut c| c.set_text(code)) {
                Ok(()) => ui.set_status_line(SharedString::from("Room code copied to clipboard.")),
                Err(e) => ui.set_error_line(SharedString::from(format!(
                    "Could not use the clipboard: {e}"
                ))),
            }
        });
    }

    // --- reopen joining after the code expired -----------------------------
    {
        let weak = ui.as_weak();
        let app = app.clone();
        ui.on_reopen_joining(move || {
            let ui = match weak.upgrade() {
                Some(u) => u,
                None => return,
            };
            let node = match app.borrow().node.clone() {
                Some(n) => n,
                None => return,
            };
            match node.reopen_joining(DEFAULT_CODE_TTL) {
                Ok(()) => {
                    app.borrow_mut().expires_at = Some(Instant::now() + DEFAULT_CODE_TTL);
                    ui.set_error_line(SharedString::from(""));
                }
                Err(e) => ui.set_error_line(SharedString::from(e.to_string())),
            }
        });
    }

    // --- leave the room ----------------------------------------------------
    {
        let weak = ui.as_weak();
        let app = app.clone();
        ui.on_leave_room(move || {
            let ui = match weak.upgrade() {
                Some(u) => u,
                None => return,
            };
            let node = app.borrow().node.clone();
            if let Some(node) = node {
                // The transcript stays available in the core until a new room
                // is started, so "Download chat history" still works here.
                node.leave();
            }
            {
                let mut a = app.borrow_mut();
                a.node = None;
                a.rx = None;
                a.reset_models();
            }
            ui.set_screen(SharedString::from("setup"));
            ui.set_busy(false);
            ui.set_is_host(false);
            ui.set_room_code(SharedString::from(""));
            ui.set_room_addr(SharedString::from(""));
            ui.set_expiry_note(SharedString::from(""));
            ui.set_offer_id(SharedString::from(""));
            ui.set_offer_text(SharedString::from(""));
            ui.set_error_line(SharedString::from(""));
            ui.set_status_line(SharedString::from("You left the room."));
        });
    }
}

// ---------------------------------------------------------------------------
// event pump
// ---------------------------------------------------------------------------

fn pump(ui: &AppWindow, app: &Shared) {
    // Local notices from the worker threads first.
    loop {
        let msg = {
            let a = app.borrow();
            match a.ui_rx.try_recv() {
                Ok(m) => m,
                Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => break,
            }
        };
        match msg {
            UiMsg::StartFailed(reason) => {
                {
                    let mut a = app.borrow_mut();
                    if let Some(node) = a.node.take() {
                        node.leave();
                    }
                    a.rx = None;
                    a.reset_models();
                }
                ui.set_busy(false);
                ui.set_screen(SharedString::from("setup"));
                ui.set_status_line(SharedString::from("Not connected."));
                ui.set_error_line(SharedString::from(reason));
            }
            UiMsg::Note(note) => ui.set_status_line(SharedString::from(note)),
        }
    }

    // Engine events.
    let mut new_messages = false;
    for _ in 0..MAX_EVENTS_PER_TICK {
        let ev = {
            let a = app.borrow();
            match a.rx.as_ref().map(|rx| rx.try_recv()) {
                Some(Ok(ev)) => ev,
                _ => break,
            }
        };
        if handle_event(ui, app, ev) {
            new_messages = true;
        }
    }
    if new_messages {
        // `changed _tick` inside `chat_list` (app.slint) does the actual
        // scrolling; wrapping add is fine, only change-vs-no-change matters.
        ui.set_scroll_tick(ui.get_scroll_tick().wrapping_add(1));
    }

    // Room-code countdown (host only).
    update_expiry(ui, app);
}

/// Returns true when the transcript gained a line (so we scroll to the bottom).
fn handle_event(ui: &AppWindow, app: &Shared, ev: Event) -> bool {
    match ev {
        Event::Status(text) => {
            ui.set_status_line(SharedString::from(text));
            false
        }
        Event::System(text) => {
            app.borrow().push_system(&text);
            true
        }
        Event::RoomOpened {
            code,
            transport,
            dial,
            expires_secs,
        } => {
            app.borrow_mut().expires_at = Some(Instant::now() + Duration::from_secs(expires_secs));
            ui.set_is_host(true);
            ui.set_room_code(SharedString::from(code));
            ui.set_room_addr(SharedString::from(dial));
            ui.set_transport_label(SharedString::from(transport));
            ui.set_screen(SharedString::from("room"));
            ui.set_busy(false);
            ui.set_status_line(SharedString::from(
                "Room open. Share the code; every joiner needs your approval.",
            ));
            if let Some(node) = app.borrow().node.clone() {
                ui.set_my_fingerprint(SharedString::from(node.fingerprint()));
            }
            false
        }
        Event::Joined { room, transport } => {
            ui.set_is_host(false);
            ui.set_room_code(SharedString::from(room));
            ui.set_transport_label(SharedString::from(transport));
            ui.set_screen(SharedString::from("room"));
            ui.set_busy(false);
            ui.set_expiry_note(SharedString::from(""));
            ui.set_status_line(SharedString::from("Approved - you are in the room."));
            if let Some(node) = app.borrow().node.clone() {
                ui.set_my_fingerprint(SharedString::from(node.fingerprint()));
            }
            false
        }
        Event::JoinRequest {
            req,
            name,
            addr,
            fingerprint,
        } => {
            app.borrow().requests.push(RequestRow {
                req: SharedString::from(req),
                name: SharedString::from(name.clone()),
                addr: SharedString::from(addr),
                fingerprint: SharedString::from(fingerprint),
            });
            ui.set_status_line(SharedString::from(format!(
                "{name} wants to join - accept or decline below."
            )));
            false
        }
        Event::JoinResolved { req, accepted } => {
            app.borrow().remove_request(&req);
            ui.set_status_line(SharedString::from(if accepted {
                "Joiner accepted."
            } else {
                "Joiner declined."
            }));
            false
        }
        Event::Peer {
            id,
            name,
            state,
            detail,
        } => {
            app.borrow()
                .upsert_peer(&id, &name, state.as_str(), &detail);
            if matches!(state, PeerState::Failed) && !detail.is_empty() {
                ui.set_error_line(SharedString::from(format!("{name}: {detail}")));
            }
            false
        }
        Event::Chat {
            from: _,
            name,
            ts,
            body,
            mine,
        } => {
            app.borrow().push_message(MessageRow {
                who: SharedString::from(name),
                time: SharedString::from(fmt_clock(ts)),
                body: SharedString::from(body),
                mine,
                system: false,
            });
            true
        }
        Event::FileOffer {
            transfer,
            from: _,
            name,
            size,
        } => {
            let text = format!("{name} ({})", human_size(size));
            app.borrow_mut().offers.push_back((transfer, text));
            show_next_offer(ui, app);
            false
        }
        Event::FileProgress {
            transfer,
            name,
            peer,
            done,
            total,
            incoming,
        } => {
            let frac = if total == 0 {
                0.0
            } else {
                (done as f64 / total as f64) as f32
            };
            let arrow = if incoming { "from" } else { "to" };
            app.borrow().upsert_transfer(
                &transfer,
                &format!("{name} {arrow} {peer}"),
                frac,
                &format!("{} / {}", human_size(done), human_size(total)),
            );
            false
        }
        Event::FileFinished {
            transfer,
            name,
            incoming,
            ok,
            detail,
        } => {
            let state = if ok { "Done" } else { "Failed" };
            let arrow = if incoming { "received" } else { "sent" };
            {
                let a = app.borrow();
                a.upsert_transfer(
                    &transfer,
                    &format!("{name} ({arrow})"),
                    if ok { 1.0 } else { 0.0 },
                    if detail.is_empty() { state } else { detail.as_str() },
                );
                a.push_system(&if ok {
                    format!("File {arrow}: {name}. {detail}")
                } else {
                    format!("File transfer failed: {name}. {detail}")
                });
            }
            true
        }
        Event::Error(text) => {
            ui.set_error_line(SharedString::from(text));
            false
        }
        Event::RoomClosed(reason) => {
            {
                let a = app.borrow();
                a.push_system(&format!("Room closed: {reason}"));
                a.peers.set_vec(Vec::new());
                a.requests.set_vec(Vec::new());
            }
            app.borrow_mut().expires_at = None;
            ui.set_expiry_note(SharedString::from(""));
            ui.set_status_line(SharedString::from(reason));
            true
        }
    }
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

/// Read the mode selector, showing a readable error if it is somehow unset.
fn mode_of(ui: &AppWindow) -> Option<TransportKind> {
    match TransportKind::parse(ui.get_mode().as_str()) {
        Some(k) => Some(k),
        None => {
            ui.set_error_line(SharedString::from("Choose LAN or Bluetooth first."));
            None
        }
    }
}

/// Create a fresh engine node for the name in the setup screen and install it
/// (plus its event queue) into the shared state.
fn start_node(ui: &AppWindow, app: &Shared) -> Option<Arc<Node>> {
    let name = ui.get_display_name().trim().to_string();
    if name.is_empty() {
        ui.set_error_line(SharedString::from("Enter a display name first."));
        return None;
    }
    let (node, rx) = match Node::new(&name) {
        Ok(pair) => pair,
        Err(e) => {
            ui.set_error_line(SharedString::from(e.to_string()));
            return None;
        }
    };
    {
        let mut a = app.borrow_mut();
        if let Some(old) = a.node.take() {
            old.leave();
        }
        a.reset_models();
        a.node = Some(node.clone());
        a.rx = Some(rx);
    }
    ui.set_my_fingerprint(SharedString::from(node.fingerprint()));
    ui.set_offer_id(SharedString::from(""));
    ui.set_offer_text(SharedString::from(""));
    Some(node)
}

/// Show the oldest queued file offer, or clear the prompt when none are left.
fn show_next_offer(ui: &AppWindow, app: &Shared) {
    let head = app.borrow().offers.front().cloned();
    match head {
        Some((id, text)) => {
            ui.set_offer_id(SharedString::from(id));
            ui.set_offer_text(SharedString::from(text));
        }
        None => {
            ui.set_offer_id(SharedString::from(""));
            ui.set_offer_text(SharedString::from(""));
        }
    }
}

/// `"report.pdf (1.2 MiB)"` -> `"report.pdf"`.
fn suggested_name(offer_text: &str) -> String {
    match offer_text.rfind(" (") {
        Some(i) => offer_text[..i].to_string(),
        None => offer_text.to_string(),
    }
}

/// Keep the "code expires in ..." note current without spamming the property.
fn update_expiry(ui: &AppWindow, app: &Shared) {
    let deadline = app.borrow().expires_at;
    let text = match deadline {
        None => String::new(),
        Some(t) => {
            let left = t.saturating_duration_since(Instant::now()).as_secs();
            if left == 0 {
                "Code expired - new joiners are refused. Press Reopen to accept more.".to_string()
            } else if left >= 60 {
                format!("Code accepts joiners for another {} min", (left + 59) / 60)
            } else {
                format!("Code accepts joiners for another {left}s")
            }
        }
    };
    let mut a = app.borrow_mut();
    if a.last_expiry != text {
        a.last_expiry = text.clone();
        drop(a);
        ui.set_expiry_note(SharedString::from(text));
    }
}
