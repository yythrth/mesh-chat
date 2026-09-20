//! The node: everything that makes a room work.
//!
//! Topology is a full mesh. The host owns the room and the approval decision,
//! but it never relays anything: once a joiner is approved, the host hands it
//! the connection details of every already-approved peer and tells those peers
//! that a new member is coming, and the peers wire themselves together
//! directly. Chat lines and file chunks always travel on the author's own
//! link to each recipient.
//!
//! Locking rules used throughout this module:
//!
//! * `state` is never held across socket I/O. Callers clone the `Arc<Peer>`
//!   handles they need, drop the guard, then send.
//! * Each link has exactly one reader thread ([`Node::reader_loop`]).

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use crate::crypto::{Identity, SecureLink};
use crate::files::Transfers;
use crate::history::{self, ChatRecord};
use crate::protocol::{
    encode_control, new_id, now_ts, Control, Frame, PeerId, PeerInfo,
};
use crate::ratelimit::RateLimiter;
use crate::roomcode;
use crate::transport::{self, Listener, Stream, TransportKind};
use crate::{Error, Result, PROTOCOL_VERSION};

/// How long we wait for a TCP/RFCOMM connection to come up.
pub const DIAL_TIMEOUT: Duration = Duration::from_secs(10);
/// How long a peer will hold an unrecognised mesh call while waiting for the
/// host's `PeerJoined` announcement to arrive (removes the approval race).
pub const ROSTER_GRACE: Duration = Duration::from_secs(8);
/// How long a freshly created room accepts new joiners before the code expires.
pub const DEFAULT_CODE_TTL: Duration = Duration::from_secs(10 * 60);

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Role {
    Host,
    Joiner,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PeerState {
    Connecting,
    Connected,
    Disconnected,
    Failed,
}

impl PeerState {
    pub fn as_str(self) -> &'static str {
        match self {
            PeerState::Connecting => "Connecting",
            PeerState::Connected => "Connected",
            PeerState::Disconnected => "Disconnected",
            PeerState::Failed => "Failed",
        }
    }
}

/// Everything the UI needs to know about, in the order it happened.
#[derive(Clone, Debug)]
pub enum Event {
    /// Transient progress note.
    Status(String),
    /// A line for the transcript view that is not a chat message.
    System(String),
    RoomOpened {
        code: String,
        transport: String,
        dial: String,
        expires_secs: u64,
    },
    Joined {
        room: String,
        transport: String,
    },
    JoinRequest {
        req: String,
        name: String,
        addr: String,
        fingerprint: String,
    },
    JoinResolved {
        req: String,
        accepted: bool,
    },
    Peer {
        id: String,
        name: String,
        state: PeerState,
        detail: String,
    },
    Chat {
        from: String,
        name: String,
        ts: i64,
        body: String,
        mine: bool,
    },
    FileOffer {
        transfer: String,
        from: String,
        name: String,
        size: u64,
    },
    FileProgress {
        transfer: String,
        name: String,
        peer: String,
        done: u64,
        total: u64,
        incoming: bool,
    },
    FileFinished {
        transfer: String,
        name: String,
        incoming: bool,
        ok: bool,
        detail: String,
    },
    Error(String),
    RoomClosed(String),
}

/// One connected peer and the encrypted link to it.
pub struct Peer {
    pub info: PeerInfo,
    pub link: Arc<SecureLink>,
    pub state: Mutex<PeerState>,
}

impl Peer {
    pub fn send(&self, c: &Control) -> Result<()> {
        self.link.send(&encode_control(c)?)
    }
}

struct Pending {
    info: PeerInfo,
    link: Arc<SecureLink>,
}

struct State {
    role: Option<Role>,
    kind: Option<TransportKind>,
    room: String,
    me: PeerInfo,
    listener: Option<Arc<dyn Listener>>,
    peers: HashMap<PeerId, Arc<Peer>>,
    /// Peers the host has approved (mirrored on every node so inbound mesh
    /// calls can be authorised without asking the host).
    roster: HashMap<PeerId, PeerInfo>,
    pending: HashMap<String, Pending>,
    history: Vec<ChatRecord>,
    open_until: Option<Instant>,
    limiter: RateLimiter,
}

pub struct Node {
    identity: Identity,
    tx: Sender<Event>,
    state: Mutex<State>,
    pub(crate) transfers: Mutex<Transfers>,
    running: AtomicBool,
    req_seq: AtomicU64,
}

impl Node {
    pub fn new(display_name: &str) -> Result<(Arc<Node>, Receiver<Event>)> {
        let identity = Identity::generate()?;
        let (tx, rx) = channel();
        let me = PeerInfo {
            id: new_id(),
            name: sanitize_name(display_name),
            addr: String::new(),
            fingerprint: identity.fingerprint(),
        };
        let node = Arc::new(Node {
            identity,
            tx,
            state: Mutex::new(State {
                role: None,
                kind: None,
                room: String::new(),
                me,
                listener: None,
                peers: HashMap::new(),
                roster: HashMap::new(),
                pending: HashMap::new(),
                history: Vec::new(),
                open_until: None,
                limiter: RateLimiter::default_policy(),
            }),
            transfers: Mutex::new(Transfers::default()),
            running: AtomicBool::new(false),
            req_seq: AtomicU64::new(1),
        });
        Ok((node, rx))
    }

    // --- small accessors -------------------------------------------------

    pub fn me(&self) -> PeerInfo {
        self.state.lock().unwrap().me.clone()
    }

    pub fn my_id(&self) -> PeerId {
        self.state.lock().unwrap().me.id.clone()
    }

    pub fn room(&self) -> String {
        self.state.lock().unwrap().room.clone()
    }

    pub fn kind(&self) -> Option<TransportKind> {
        self.state.lock().unwrap().kind
    }

    pub fn role(&self) -> Option<Role> {
        self.state.lock().unwrap().role
    }

    pub fn is_host(&self) -> bool {
        self.role() == Some(Role::Host)
    }

    pub fn is_active(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    pub fn fingerprint(&self) -> String {
        self.identity.fingerprint()
    }

    pub fn set_display_name(&self, name: &str) {
        let mut st = self.state.lock().unwrap();
        st.me.name = sanitize_name(name);
    }

    pub fn peer_names(&self) -> Vec<String> {
        self.state
            .lock()
            .unwrap()
            .peers
            .values()
            .map(|p| p.info.name.clone())
            .collect()
    }

    pub fn connected_peers(&self) -> Vec<Arc<Peer>> {
        self.state.lock().unwrap().peers.values().cloned().collect()
    }

    pub(crate) fn peer_by_id(&self, id: &str) -> Option<Arc<Peer>> {
        self.state.lock().unwrap().peers.get(id).cloned()
    }

    pub(crate) fn emit(&self, e: Event) {
        let _ = self.tx.send(e);
    }

    pub(crate) fn record(&self, r: ChatRecord) {
        self.state.lock().unwrap().history.push(r);
    }

    pub fn history(&self) -> Vec<ChatRecord> {
        self.state.lock().unwrap().history.clone()
    }

    /// Write the current session transcript to a file the user picked.
    pub fn export_history(&self, path: &Path) -> Result<()> {
        let (room, kind, records) = {
            let st = self.state.lock().unwrap();
            (
                st.room.clone(),
                st.kind.map(|k| k.as_str()).unwrap_or("-").to_string(),
                st.history.clone(),
            )
        };
        history::export(path, &room, &kind, &records)
    }

    fn ensure_idle(&self) -> Result<()> {
        if self.running.load(Ordering::SeqCst) {
            return Err(Error::Config(
                "you are already in a room - leave it first".into(),
            ));
        }
        Ok(())
    }

    // --- hosting ---------------------------------------------------------

    /// Open a room on `kind` and return the shareable room code.
    pub fn host(self: &Arc<Self>, kind: TransportKind, ttl: Duration) -> Result<String> {
        self.ensure_idle()?;
        let listener = transport::listen(kind)?;
        let dial = listener.dial_address();
        let code = roomcode::encode(kind, &dial)?;
        {
            let mut st = self.state.lock().unwrap();
            st.role = Some(Role::Host);
            st.kind = Some(kind);
            st.room = code.clone();
            st.me.addr = dial.clone();
            st.listener = Some(listener.clone());
            st.open_until = Some(Instant::now() + ttl);
            st.limiter = RateLimiter::default_policy();
            st.history.clear();
            st.roster.clear();
            st.peers.clear();
            st.pending.clear();
        }
        self.running.store(true, Ordering::SeqCst);
        self.spawn_accept_loop(listener);
        self.record(ChatRecord::system(
            now_ts(),
            &format!("Room opened over {} with code {}", kind.as_str(), code),
        ));
        self.emit(Event::RoomOpened {
            code: code.clone(),
            transport: kind.as_str().to_string(),
            dial,
            expires_secs: ttl.as_secs(),
        });
        Ok(code)
    }

    /// Re-open joining after the code expired (same address, same code).
    pub fn reopen_joining(self: &Arc<Self>, ttl: Duration) -> Result<()> {
        let mut st = self.state.lock().unwrap();
        if st.role != Some(Role::Host) {
            return Err(Error::Config("only the host can reopen joining".into()));
        }
        st.open_until = Some(Instant::now() + ttl);
        st.limiter = RateLimiter::default_policy();
        drop(st);
        self.emit(Event::Status("The room is accepting joiners again".into()));
        Ok(())
    }

    // --- joining ---------------------------------------------------------

    /// Dial the host named by `code` and wait (in the background) for the
    /// host operator to accept or decline.
    pub fn join(self: &Arc<Self>, kind: TransportKind, code: &str) -> Result<()> {
        self.ensure_idle()?;
        let (code_kind, dial) = roomcode::decode(code)?;
        if code_kind != kind {
            return Err(Error::Config(format!(
                "that room code is a {} code - switch the connection mode to {} and try again",
                code_kind.as_str(),
                code_kind.as_str()
            )));
        }
        // Our own inbound endpoint, so later joiners can reach us directly.
        let listener = transport::listen(kind)?;
        let my_addr = listener.dial_address();
        {
            let mut st = self.state.lock().unwrap();
            st.role = Some(Role::Joiner);
            st.kind = Some(kind);
            st.room = code.trim().to_ascii_uppercase();
            st.me.addr = my_addr;
            st.listener = Some(listener.clone());
            st.open_until = None;
            st.limiter = RateLimiter::default_policy();
            st.history.clear();
            st.roster.clear();
            st.peers.clear();
            st.pending.clear();
        }
        self.running.store(true, Ordering::SeqCst);
        self.spawn_accept_loop(listener);

        let me = self.me();
        let room = self.room();
        self.emit(Event::Status(format!(
            "Contacting the host over {}...",
            kind.as_str()
        )));

        let stream = match transport::connect(kind, &dial, DIAL_TIMEOUT) {
            Ok(s) => s,
            Err(e) => {
                self.shutdown_internal();
                return Err(e);
            }
        };
        let link = match SecureLink::handshake_initiator(stream, &self.identity) {
            Ok(l) => Arc::new(l),
            Err(e) => {
                self.shutdown_internal();
                return Err(e);
            }
        };
        if let Err(e) = link.send(&encode_control(&Control::Hello {
            version: PROTOCOL_VERSION,
            room,
            peer: me,
        })?) {
            self.shutdown_internal();
            return Err(e);
        }
        self.emit(Event::Status(
            "Request sent - waiting for the host to approve it...".into(),
        ));
        let node = self.clone();
        thread::spawn(move || node.await_welcome(link));
        Ok(())
    }

    fn await_welcome(self: Arc<Self>, link: Arc<SecureLink>) {
        let first = match link.recv() {
            Ok(b) => b,
            Err(e) => {
                self.emit(Event::RoomClosed(format!(
                    "the connection to the host was lost: {e}"
                )));
                self.shutdown_internal();
                return;
            }
        };
        match crate::protocol::decode(&first) {
            Ok(Frame::Control(Control::Welcome { room, host, peers })) => {
                {
                    let mut st = self.state.lock().unwrap();
                    st.room = room.clone();
                    st.roster.insert(host.id.clone(), host.clone());
                    for p in &peers {
                        st.roster.insert(p.id.clone(), p.clone());
                    }
                }
                let kind = self.kind().map(|k| k.as_str()).unwrap_or("-").to_string();
                self.record(ChatRecord::system(
                    now_ts(),
                    &format!("Joined room {room} over {kind}"),
                ));
                // Attach first: by the time the UI sees `Joined`, the link to
                // the host really is in place.
                self.attach_peer(host, link);
                self.emit(Event::Joined {
                    room,
                    transport: kind,
                });
                for p in peers {
                    let node = self.clone();
                    thread::spawn(move || node.dial_peer(p));
                }
            }
            Ok(Frame::Control(Control::Rejected { reason })) => {
                link.close();
                self.emit(Event::RoomClosed(reason));
                self.shutdown_internal();
            }
            Ok(_) => {
                link.close();
                self.emit(Event::RoomClosed(
                    "the host sent an unexpected reply - version mismatch?".into(),
                ));
                self.shutdown_internal();
            }
            Err(e) => {
                link.close();
                self.emit(Event::RoomClosed(format!("bad reply from the host: {e}")));
                self.shutdown_internal();
            }
        }
    }

    /// Open our own direct link to an already-approved peer (§3: no relaying).
    fn dial_peer(self: Arc<Self>, info: PeerInfo) {
        let Some(kind) = self.kind() else { return };
        self.emit(Event::Peer {
            id: info.id.clone(),
            name: info.name.clone(),
            state: PeerState::Connecting,
            detail: info.addr.clone(),
        });
        let mut last = String::from("unknown error");
        for attempt in 0..3 {
            if !self.running.load(Ordering::SeqCst) {
                return;
            }
            match self.try_dial(&info, kind) {
                Ok(()) => return,
                Err(e) => {
                    last = e.to_string();
                    if attempt < 2 {
                        thread::sleep(Duration::from_millis(600));
                    }
                }
            }
        }
        self.emit(Event::Peer {
            id: info.id.clone(),
            name: info.name.clone(),
            state: PeerState::Failed,
            detail: last,
        });
    }

    fn try_dial(self: &Arc<Self>, info: &PeerInfo, kind: TransportKind) -> Result<()> {
        let stream = transport::connect(kind, &info.addr, DIAL_TIMEOUT)?;
        let link = Arc::new(SecureLink::handshake_initiator(stream, &self.identity)?);
        link.send(&encode_control(&Control::MeshHello {
            version: PROTOCOL_VERSION,
            room: self.room(),
            peer: self.me(),
        })?)?;
        let reply = link.recv()?;
        match crate::protocol::decode(&reply)? {
            Frame::Control(Control::MeshAck { peer }) => {
                self.attach_peer(peer, link);
                Ok(())
            }
            Frame::Control(Control::Rejected { reason }) => {
                link.close();
                Err(Error::Closed(reason))
            }
            _ => {
                link.close();
                Err(Error::Protocol("unexpected reply to mesh hello".into()))
            }
        }
    }

    // --- inbound connections ---------------------------------------------

    fn spawn_accept_loop(self: &Arc<Self>, listener: Arc<dyn Listener>) {
        let node = self.clone();
        thread::spawn(move || loop {
            match listener.accept() {
                Ok(stream) => {
                    if !node.running.load(Ordering::SeqCst) {
                        stream.close();
                        break;
                    }
                    let n = node.clone();
                    thread::spawn(move || n.handle_incoming(stream));
                }
                Err(_) => {
                    if node.running.load(Ordering::SeqCst) {
                        node.emit(Event::Status(
                            "stopped listening for new connections".into(),
                        ));
                    }
                    break;
                }
            }
        });
    }

    fn handle_incoming(self: Arc<Self>, stream: Arc<dyn Stream>) {
        let label = stream.remote_label();
        let source = label.rsplit_once(':').map(|(h, _)| h.to_string()).unwrap_or(label.clone());
        {
            let mut st = self.state.lock().unwrap();
            if !st.limiter.allow(&source) {
                drop(st);
                self.emit(Event::Status(format!(
                    "ignored a connection attempt from {source} (too many attempts)"
                )));
                stream.close();
                return;
            }
        }

        let link = match SecureLink::handshake_responder(stream.clone(), &self.identity) {
            Ok(l) => Arc::new(l),
            Err(e) => {
                self.emit(Event::Status(format!(
                    "a connection from {source} failed to handshake: {e}"
                )));
                stream.close();
                return;
            }
        };

        let first = match link.recv() {
            Ok(b) => b,
            Err(_) => {
                link.close();
                return;
            }
        };
        match crate::protocol::decode(&first) {
            Ok(Frame::Control(Control::Hello {
                version,
                room,
                peer,
            })) => self.on_hello(link, version, room, peer, source),
            Ok(Frame::Control(Control::MeshHello {
                version,
                room,
                peer,
            })) => self.on_mesh_hello(link, version, room, peer),
            _ => {
                let _ = link.send(
                    &encode_control(&Control::Rejected {
                        reason: "unexpected first message".into(),
                    })
                    .unwrap_or_default(),
                );
                link.close();
            }
        }
    }

    fn reject(&self, link: &Arc<SecureLink>, reason: &str) {
        if let Ok(bytes) = encode_control(&Control::Rejected {
            reason: reason.to_string(),
        }) {
            let _ = link.send(&bytes);
        }
        link.close();
    }

    /// A joiner is asking the host for admission. Park it until the operator
    /// decides - it is connected to nobody until then (§5).
    fn on_hello(
        self: Arc<Self>,
        link: Arc<SecureLink>,
        version: u32,
        room: String,
        mut peer: PeerInfo,
        source: String,
    ) {
        if version != PROTOCOL_VERSION {
            self.reject(
                &link,
                &format!("the host runs protocol v{PROTOCOL_VERSION}, you run v{version}"),
            );
            return;
        }
        let (role, my_room, expired, known) = {
            let st = self.state.lock().unwrap();
            (
                st.role,
                st.room.clone(),
                st.open_until.map(|t| Instant::now() > t).unwrap_or(false),
                st.roster.contains_key(&peer.id) || st.me.id == peer.id,
            )
        };
        if role != Some(Role::Host) {
            self.reject(&link, "this peer is not the host of that room");
            return;
        }
        if normalize_code(&room) != normalize_code(&my_room) {
            self.reject(&link, "that room code does not match this room");
            return;
        }
        if expired {
            self.reject(
                &link,
                "this room code has expired - ask the host to reopen joining",
            );
            return;
        }
        if known {
            self.reject(&link, "a peer with that id is already in the room");
            return;
        }

        peer.name = sanitize_name(&peer.name);
        let req = self.req_seq.fetch_add(1, Ordering::SeqCst).to_string();
        let fingerprint = link.fingerprint();
        let addr = if peer.addr.is_empty() {
            source
        } else {
            peer.addr.clone()
        };
        let name = peer.name.clone();
        {
            let mut st = self.state.lock().unwrap();
            st.pending.insert(
                req.clone(),
                Pending {
                    info: peer,
                    link: link.clone(),
                },
            );
        }
        self.emit(Event::JoinRequest {
            req,
            name,
            addr,
            fingerprint,
        });
    }

    /// An approved peer is opening its direct mesh link to us.
    fn on_mesh_hello(
        self: Arc<Self>,
        link: Arc<SecureLink>,
        version: u32,
        room: String,
        mut peer: PeerInfo,
    ) {
        if version != PROTOCOL_VERSION {
            self.reject(&link, "protocol version mismatch");
            return;
        }
        if normalize_code(&room) != normalize_code(&self.room()) {
            self.reject(&link, "that room code does not match this room");
            return;
        }
        peer.name = sanitize_name(&peer.name);

        // The host's PeerJoined announcement and the newcomer's call race each
        // other; hold the call briefly rather than rejecting it outright.
        let deadline = Instant::now() + ROSTER_GRACE;
        loop {
            let approved = {
                let st = self.state.lock().unwrap();
                st.roster.contains_key(&peer.id)
            };
            if approved {
                break;
            }
            if Instant::now() >= deadline {
                self.reject(
                    &link,
                    "you are not on this room's approved list (ask the host to approve you first)",
                );
                return;
            }
            thread::sleep(Duration::from_millis(100));
        }

        if self.peer_by_id(&peer.id).is_some() {
            self.reject(&link, "already linked to that peer");
            return;
        }

        if let Ok(bytes) = encode_control(&Control::MeshAck { peer: self.me() }) {
            if link.send(&bytes).is_err() {
                link.close();
                return;
            }
        }
        self.attach_peer(peer, link);
    }

    /// Register a live link and start reading from it.
    ///
    /// The `running` check happens *inside* the state lock: `leave` clears the
    /// peer map under the same lock after flipping `running`, so a link that
    /// arrives during teardown is closed here instead of being left dangling.
    fn attach_peer(self: &Arc<Self>, info: PeerInfo, link: Arc<SecureLink>) {
        let peer = Arc::new(Peer {
            info: info.clone(),
            link,
            state: Mutex::new(PeerState::Connected),
        });
        {
            let mut st = self.state.lock().unwrap();
            if !self.running.load(Ordering::SeqCst) {
                drop(st);
                peer.link.close();
                return;
            }
            st.roster.insert(info.id.clone(), info.clone());
            st.peers.insert(info.id.clone(), peer.clone());
        }
        self.record(ChatRecord::system(
            now_ts(),
            &format!("{} connected", info.name),
        ));
        self.emit(Event::Peer {
            id: info.id.clone(),
            name: info.name.clone(),
            state: PeerState::Connected,
            detail: format!("{} · {}", info.addr, peer.link.fingerprint()),
        });
        let node = self.clone();
        thread::spawn(move || node.reader_loop(peer));
    }

    // --- host approval ----------------------------------------------------

    /// Accept or decline a pending join request.
    ///
    /// On accept the host (a) tells every existing peer about the newcomer and
    /// (b) hands the newcomer the full roster, so every pair can build its own
    /// direct link. The host does not forward anything afterwards.
    pub fn approve(self: &Arc<Self>, req: &str, accept: bool) -> Result<()> {
        let pending = {
            let mut st = self.state.lock().unwrap();
            st.pending.remove(req)
        }
        .ok_or_else(|| Error::Protocol("that join request is no longer pending".into()))?;

        if !accept {
            self.reject(&pending.link, "the host declined your request to join");
            self.record(ChatRecord::system(
                now_ts(),
                &format!("Declined a join request from {}", pending.info.name),
            ));
            self.emit(Event::System(format!(
                "Declined {}'s request",
                pending.info.name
            )));
            self.emit(Event::JoinResolved {
                req: req.to_string(),
                accepted: false,
            });
            return Ok(());
        }

        let existing = self.connected_peers();
        let roster: Vec<PeerInfo> = existing.iter().map(|p| p.info.clone()).collect();

        // Announce first so the newcomer's inbound calls are authorised.
        for p in &existing {
            if let Err(e) = p.send(&Control::PeerJoined {
                peer: pending.info.clone(),
            }) {
                self.emit(Event::Error(format!(
                    "could not tell {} about the new peer: {e}",
                    p.info.name
                )));
            }
        }
        {
            let mut st = self.state.lock().unwrap();
            st.roster.insert(pending.info.id.clone(), pending.info.clone());
        }

        let welcome = encode_control(&Control::Welcome {
            room: self.room(),
            host: self.me(),
            peers: roster,
        })?;
        pending.link.send(&welcome)?;

        self.emit(Event::JoinResolved {
            req: req.to_string(),
            accepted: true,
        });
        self.attach_peer(pending.info, pending.link);
        Ok(())
    }

    /// Pending requests the host has not answered yet, as `(req, name)`.
    pub fn pending_requests(&self) -> Vec<(String, String)> {
        self.state
            .lock()
            .unwrap()
            .pending
            .iter()
            .map(|(k, v)| (k.clone(), v.info.name.clone()))
            .collect()
    }

    // --- running links ----------------------------------------------------

    fn reader_loop(self: Arc<Self>, peer: Arc<Peer>) {
        loop {
            match peer.link.recv() {
                Ok(buf) => match crate::protocol::decode(&buf) {
                    Ok(Frame::Control(c)) => self.handle_control(&peer, c),
                    Ok(Frame::Chunk { id, seq, data }) => self.handle_chunk(&peer, id, seq, &data),
                    Err(e) => self.emit(Event::Error(format!(
                        "malformed message from {}: {e}",
                        peer.info.name
                    ))),
                },
                Err(e) => {
                    self.peer_gone(&peer, describe_disconnect(&e));
                    return;
                }
            }
        }
    }

    fn handle_control(self: &Arc<Self>, peer: &Arc<Peer>, c: Control) {
        match c {
            Control::Chat {
                from,
                name,
                ts,
                body,
                ..
            } => {
                let name = sanitize_name(&name);
                self.record(ChatRecord::message(&name, &from, ts, &body));
                self.emit(Event::Chat {
                    from,
                    name,
                    ts,
                    body,
                    mine: false,
                });
            }
            Control::PeerJoined { peer: info } => {
                {
                    let mut st = self.state.lock().unwrap();
                    st.roster.insert(info.id.clone(), info.clone());
                }
                self.emit(Event::System(format!(
                    "{} was approved by the host and will connect directly",
                    info.name
                )));
            }
            Control::PeerLeft { id, name } => {
                {
                    let mut st = self.state.lock().unwrap();
                    st.roster.remove(&id);
                }
                self.emit(Event::System(format!("{name} left the room")));
            }
            Control::Bye => {
                self.peer_gone(peer, "the peer closed the room".into());
            }
            Control::FileOffer { .. }
            | Control::FileAccept { .. }
            | Control::FileDecline { .. }
            | Control::FileDone { .. }
            | Control::FileAbort { .. } => {
                self.handle_file_control(peer, c);
            }
            Control::Rejected { reason } => {
                self.emit(Event::Error(format!("{}: {reason}", peer.info.name)));
            }
            Control::Hello { .. }
            | Control::MeshHello { .. }
            | Control::Welcome { .. }
            | Control::MeshAck { .. } => {
                // Handshake-only messages; ignore if they show up mid-session.
            }
        }
    }

    fn peer_gone(self: &Arc<Self>, peer: &Arc<Peer>, reason: String) {
        let (removed, is_host) = {
            let mut st = self.state.lock().unwrap();
            let removed = st.peers.remove(&peer.info.id).is_some();
            if removed && st.role == Some(Role::Host) {
                st.roster.remove(&peer.info.id);
            }
            (removed, st.role == Some(Role::Host))
        };
        if !removed {
            return;
        }
        *peer.state.lock().unwrap() = PeerState::Disconnected;
        peer.link.close();
        self.cancel_transfers_with(&peer.info.id);

        if is_host {
            for p in self.connected_peers() {
                let _ = p.send(&Control::PeerLeft {
                    id: peer.info.id.clone(),
                    name: peer.info.name.clone(),
                });
            }
        }
        self.record(ChatRecord::system(
            now_ts(),
            &format!("{} disconnected ({reason})", peer.info.name),
        ));
        self.emit(Event::Peer {
            id: peer.info.id.clone(),
            name: peer.info.name.clone(),
            state: PeerState::Disconnected,
            detail: reason,
        });
    }

    // --- chat -------------------------------------------------------------

    /// Send a chat line directly to every connected peer.
    pub fn send_chat(self: &Arc<Self>, body: &str) -> Result<()> {
        let body = body.trim();
        if body.is_empty() {
            return Ok(());
        }
        if body.len() > 8000 {
            return Err(Error::Config("that message is too long".into()));
        }
        let me = self.me();
        let ts = now_ts();
        let msg = Control::Chat {
            id: new_id(),
            from: me.id.clone(),
            name: me.name.clone(),
            ts,
            body: body.to_string(),
        };
        self.record(ChatRecord::message(&me.name, &me.id, ts, body));
        self.emit(Event::Chat {
            from: me.id.clone(),
            name: me.name.clone(),
            ts,
            body: body.to_string(),
            mine: true,
        });

        let peers = self.connected_peers();
        if peers.is_empty() {
            self.emit(Event::System(
                "nobody is connected yet - that message was not delivered".into(),
            ));
        }
        for p in peers {
            if let Err(e) = p.send(&msg) {
                self.emit(Event::Error(format!(
                    "could not deliver to {}: {e}",
                    p.info.name
                )));
            }
        }
        Ok(())
    }

    // --- teardown ---------------------------------------------------------

    /// Leave the room. The transcript stays in memory so it can still be
    /// exported afterwards.
    pub fn leave(self: &Arc<Self>) {
        if !self.running.swap(false, Ordering::SeqCst) {
            return;
        }
        // Take everything under one lock so a link that attaches during
        // teardown is either drained here or refused by `attach_peer`.
        let (peers, pending, listener) = {
            let mut st = self.state.lock().unwrap();
            let peers: Vec<Arc<Peer>> = st.peers.drain().map(|(_, p)| p).collect();
            let pending: Vec<Arc<SecureLink>> =
                st.pending.drain().map(|(_, p)| p.link).collect();
            st.roster.clear();
            (peers, pending, st.listener.take())
        };
        for p in peers {
            let _ = p.send(&Control::Bye);
            p.link.close();
        }
        for l in pending {
            l.close();
        }
        if let Some(l) = listener {
            l.close();
        }
        self.record(ChatRecord::system(now_ts(), "Left the room"));
        self.emit(Event::RoomClosed("you left the room".into()));
    }

    fn shutdown_internal(self: &Arc<Self>) {
        self.running.store(false, Ordering::SeqCst);
        let (peers, listener) = {
            let mut st = self.state.lock().unwrap();
            let peers: Vec<Arc<Peer>> = st.peers.drain().map(|(_, p)| p).collect();
            st.roster.clear();
            (peers, st.listener.take())
        };
        for p in peers {
            p.link.close();
        }
        if let Some(l) = listener {
            l.close();
        }
    }
}

fn normalize_code(code: &str) -> String {
    code.chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .collect::<String>()
        .to_ascii_uppercase()
}

/// Keep display names short, single-line and printable.
pub fn sanitize_name(name: &str) -> String {
    let cleaned: String = name
        .chars()
        .filter(|c| !c.is_control())
        .take(32)
        .collect::<String>()
        .trim()
        .to_string();
    if cleaned.is_empty() {
        "Anonymous".to_string()
    } else {
        cleaned
    }
}

fn describe_disconnect(e: &Error) -> String {
    match e {
        Error::Io(io) if io.kind() == std::io::ErrorKind::UnexpectedEof => {
            "the peer closed the connection".to_string()
        }
        other => other.to_string(),
    }
}
