# Mesh Chat

A single-file Windows chat client for small groups who are in the same place.
Two or more people run `chat.exe`, one of them reads a short code out loud, and
everyone else types it in. Messages and files then travel **directly between the
machines** — over the local network or over Bluetooth — encrypted end to end.

There is no server, no account, no cloud, and nothing to install. Close the
window and the conversation is gone unless you chose to save it.

```
  Alice (host)                                     Bob
  ┌────────────┐   room code: L-4KQ7M-2PX9C    ┌────────────┐
  │  chat.exe  │ ───────── spoken ──────────▶  │  chat.exe  │
  └────────────┘                               └────────────┘
         ▲                                            ▲
         └──────── encrypted TCP / RFCOMM ────────────┘
                   (no relay, no broker)
```

---

## Contents

- [What this is, and what it is not](#what-this-is-and-what-it-is-not)
- [How a room works](#how-a-room-works)
- [Security model](#security-model)
- [Building it](#building-it)
- [Running the tests](#running-the-tests)
- [Using the app](#using-the-app)
- [LAN mode notes](#lan-mode-notes)
- [Bluetooth mode notes](#bluetooth-mode-notes)
- [Troubleshooting](#troubleshooting)
- [Project layout](#project-layout)
- [Limitations and threat model](#limitations-and-threat-model)
- [Licence](#licence)

---

## What this is, and what it is not

**It is:**

- One portable executable. No installer, no runtime, no DLLs beside it. Put it
  on a USB stick and run it from there.
- Rust + [Slint](https://slint.dev) for the interface. No Electron, no .NET, no
  bundled browser.
- A **full mesh**: once you are in a room, your machine holds a separate
  encrypted link to every other machine. The host introduces people to each
  other and then gets out of the way — it never relays anyone's traffic, and a
  room keeps working after the host leaves.
- **Code-based only.** Nothing is broadcast or advertised. No mDNS, no network
  scanning, no discoverable Bluetooth, no SDP browsing. If you were not given
  the code, there is nothing on the network for you to find.

**It is not:**

- An internet chat app. There is **no internet mode**. This build deliberately
  has no WebRTC, no STUN/TURN, and no signalling server — so there is also
  nothing to host, register for, or pay for. Both machines must be on the same
  local network, or within Bluetooth range of each other.
- A mobile app. Windows only.
- A message archive. Nothing is stored anywhere unless you press *Download chat
  history* yourself.

---

## How a room works

1. **The host creates a room.** The app opens a listening socket and turns its
   own address into a short code: `L-4KQ7M-2PX9C` for LAN,
   `B-3TGWQ-8H2VB-K5` for Bluetooth. The code *is* the address — there is no
   directory to look anything up in.

2. **The host shares the code out of band.** Say it out loud, write it on a
   whiteboard, send it in a message. Dashes, spaces and letter case are all
   ignored when typing it back in, and the ambiguous characters are folded
   (`I` and `l` become `1`, `O` becomes `0`), so it survives being read aloud.

3. **A joiner dials it.** Their machine connects to the host, completes an
   encrypted handshake, sends a greeting, and then **waits**. At this point the
   joiner is connected to nobody and can see nothing.

4. **The host approves or declines.** The request appears on the host's screen
   with the joiner's name, their address, and the fingerprint of their key.
   Nothing proceeds until a human presses **Accept** or **Decline**. A decline
   sends back a readable reason and closes the link.

5. **The mesh forms.** On approval the host first tells everyone already in the
   room that someone new is arriving, then hands the newcomer the full roster.
   The newcomer dials each existing peer directly and they complete their own
   handshakes. From then on every message and every file goes straight from
   sender to receiver — the host is just another peer.

6. **The code expires.** Ten minutes after the room opens it stops accepting new
   joiners, which closes the window in which a shoulder-surfed code is useful.
   The host can press **Reopen joining** to start a fresh ten minutes. Join
   attempts are also rate limited: five per source and thirty overall per minute.

---

## Security model

Every link — LAN or Bluetooth, host-to-peer or peer-to-peer — is wrapped in the
**Noise Protocol Framework**, specifically `Noise_XX_25519_ChaChaPoly_BLAKE2s`,
via the [`snow`](https://crates.io/crates/snow) crate.

- Each instance generates a fresh X25519 static keypair on startup.
- `XX` means both sides learn each other's static key during the handshake, so
  the app can show you a **fingerprint** (the first six bytes of the SHA-256 of
  the public key, as colon-separated hex) for every peer before you approve them.
  Your own fingerprint is in the top-right of the window. If you and the person
  in front of you read out the same six bytes, there is nobody in the middle.
- Traffic is ChaChaPoly-encrypted and authenticated. A Bluetooth or Wi-Fi
  eavesdropper sees framed ciphertext and nothing else.
- Keys are never written to disk. Restart the app and you are a new identity.

File transfers ride the same encrypted links, and the received bytes are checked
against a SHA-256 hash sent with the offer, so a truncated or corrupted transfer
is reported as failed rather than silently saved.

---

## Building it

### Prerequisites

1. **Rust (stable).** Install from <https://rustup.rs>. Any recent stable
   release works; 1.85 or newer is a safe floor.
2. **The MSVC toolchain.** Rust on Windows links with Microsoft's linker, so you
   need the *Desktop development with C++* workload from
   [Visual Studio Build Tools](https://visualstudio.microsoft.com/downloads/)
   (the free Build Tools package is enough — you do not need the full IDE). That
   workload brings the MSVC linker and the Windows SDK, which is also where the
   Bluetooth headers and `bthprops.lib` come from.

Check both are present:

```powershell
rustc --version
cargo --version
```

If `rustup show` reports the `gnu` toolchain rather than `msvc`, switch:

```powershell
rustup default stable-x86_64-pc-windows-msvc
```

### Build

```powershell
git clone <your-fork-url> mesh-chat
cd mesh-chat
cargo build --release
```

The executable lands at:

```
target\release\chat.exe
```

That file is the whole application. Copy it anywhere and run it.

### Why it is a single file

`.cargo/config.toml` passes `-C target-feature=+crt-static`, which statically
links the Microsoft C runtime so the binary does not need a VC++ redistributable
on the target machine. The Slint dependency is configured with only the `winit`
backend and the **software renderer**, so there is no Skia, no Qt and no GPU
driver dependency to ship alongside. The release profile optimises for size
(`opt-level = "z"`, LTO, one codegen unit, `panic = "abort"`, symbols stripped).

If you would rather link the CRT dynamically, delete `.cargo/config.toml` or
comment out the `rustflags` line and rebuild.

### A debug build

```powershell
cargo build
.\target\debug\chat.exe
```

The debug build keeps a console window attached so panics and `println!` output
are visible. The release build hides it (`windows_subsystem = "windows"`).

---

## Running the tests

```powershell
cargo test -p chat-core
```

This exercises the engine end to end over real loopback TCP sockets — it starts
actual nodes, forms actual meshes and moves actual bytes. Twenty-seven tests
cover, among other things:

| Area | What is checked |
| --- | --- |
| Approval flow | A joiner is connected to nobody until accepted; a decline carries a readable reason |
| Room codes | Expiry, reopening, malformed codes, dead addresses, a Bluetooth code refused in LAN mode |
| Mesh | Three peers reach a full mesh and every pair can talk; the room survives the host leaving; disconnects are reported with a reason |
| Files | A transfer arrives byte for byte; a declined file is never written; a file sent to the room reaches every peer; chat keeps flowing while a 1 MB transfer is in progress |
| History | The exported transcript contains messages from both sides |
| Churn | 25 join/leave cycles and 10 mesh formations without a stuck peer or a leaked link |

The engine (`core/`) is platform independent and its tests run on Windows,
Linux and macOS. The Bluetooth transport compiles to a stub on non-Windows
platforms, which is why the automated tests drive the LAN transport; the
Bluetooth path shares every layer above the socket with it.

---

## Using the app

### Starting a room

1. Type a **display name**.
2. Pick **LAN** or **Bluetooth**.
3. Press **Create the room**.
4. Read the big code on the left out to the people joining you, or press
   **Copy code**.

### Joining a room

1. Type a display name.
2. Pick the same mode the host used. (If you pick the wrong one, the app tells
   you which mode that code belongs to instead of just failing.)
3. Type the code and press **Request to join**.
4. Wait. You are connected to nobody until the host accepts you.

### Approving someone

The host sees a card with the joiner's name, where they are connecting from,
and their key fingerprint. Compare the fingerprint with the person if you want
certainty about who it is, then press **Accept** or **Decline**.

### Sending files

**Send file** picks a file and offers it to everyone currently in the room —
each peer as an independent transfer, sent directly to them. Each recipient
sees the filename and size and chooses **Save file as...** or **Decline**.

- Nothing is written to disk until the receiver picks a location.
- The receiver always chooses the folder and the final name.
- Received files are **never opened or executed automatically**. The app will
  not launch anything for you.
- Progress bars for transfers in both directions appear above the message box.

### Saving the conversation

**Download chat history** writes the transcript to a file you choose. Name it
with a `.json` extension for structured output, or anything else for plain text:

```
Mesh Chat - session transcript
Room code : L-4KQ7M-2PX9C
Transport : LAN
Exported  : 2026-09-19 15:04:22
------------------------------------------------------------
[2026-09-19 14:58:10] * Room opened over LAN with code L-4KQ7M-2PX9C
[2026-09-19 14:59:02] Priya: are we all here?
[2026-09-19 14:59:11] Sam: yep
```

The transcript stays available after you leave a room, so you can still export
it from the setup screen path — export before starting a *new* room, which
clears it.

### Reading the peer list

Each person shows a coloured dot and a status: **Connected** (green),
**Connecting** (amber), **Disconnected** or **Failed** (red). Failures carry the
actual reason underneath — *connection refused*, *timed out*, *host declined the
request* — rather than a generic error.

The status bar at the bottom always shows which transport is carrying the
conversation and confirms it is end-to-end encrypted.

---

## LAN mode notes

- The host binds an **ephemeral TCP port on all interfaces** and encodes its
  IPv4 address plus that port into the code. The joiner decodes it and connects
  straight there.
- Both machines must be able to reach each other directly. Most home and office
  Wi-Fi is fine. **Guest networks and "client isolation" / "AP isolation" modes
  are not** — they block machine-to-machine traffic by design, and no app can
  work around that.
- The **first time you run it, Windows Defender Firewall will ask** whether to
  allow `chat.exe` to accept incoming connections. Say yes, and tick the network
  profile you are actually on (usually *Private*). If you dismiss that prompt,
  hosting silently stops working; see Troubleshooting.
- A VPN client can capture the route and make the app advertise an address the
  other machine cannot reach. If the code contains an address that looks wrong,
  disconnect the VPN and recreate the room.

---

## Bluetooth mode notes

Bluetooth mode uses **RFCOMM over Winsock** with a directed connection.

**Pair the two machines first.** Windows will not let an app open an RFCOMM
connection to a device it does not already trust, and this app deliberately does
not scan or make your adapter discoverable. So, once per pair of machines:

> Settings → Bluetooth & devices → Add device → Bluetooth → pick the other PC →
> confirm the matching PIN on both screens.

After that the room code carries the host's Bluetooth MAC address and RFCOMM
channel, and the joiner connects straight to it.

**Deliberate design choices here:**

- The adapter is **never made discoverable**. `BluetoothEnableDiscovery` is
  never called. Your machine does not appear in anyone's scan because of this
  app.
- There is **no scanning and no SDP browsing**. The channel number is carried in
  the room code instead of being looked up in a service record, which is why the
  Bluetooth code is two characters longer than the LAN one.
- The host tries to bind a fixed channel first and falls back to an
  OS-assigned one, encoding whichever it got.

**Expect it to be slower.** RFCOMM throughput is a fraction of Wi-Fi. It is
comfortable for conversation and small files, and it will move a large file
eventually, but prefer LAN mode when one is available. Range is normal Bluetooth
range — the same room, realistically.

---

## Troubleshooting

**"The room code is a Bluetooth code — switch the connection mode"**
Exactly what it says: the host used the other transport. Switch the mode
selector and try again.

**Joining times out on LAN**
Ping the host's address from the joiner (`ping 192.168.1.42`). If that fails,
the two machines are not on the same reachable network — check for guest Wi-Fi,
AP isolation, or a VPN. If ping succeeds but joining still times out, it is the
firewall on the *host*.

**The firewall prompt never appeared, or was dismissed**
Add the rule manually from an elevated PowerShell on the host:

```powershell
New-NetFirewallRule -DisplayName "Mesh Chat" `
  -Direction Inbound -Program "C:\path\to\chat.exe" `
  -Action Allow -Profile Private
```

**Bluetooth join fails immediately**
The machines are almost certainly not paired. Pair them in Windows Settings and
retry. Also confirm both adapters are switched on — a disabled radio reports as
a failure to find a local adapter.

**"That room code has expired"**
Codes stop accepting joiners after ten minutes. The host presses **Reopen
joining**.

**The host left and everything still works**
That is intended. The host only introduces peers; it does not carry their
traffic. Nobody new can join once the host is gone, because the code points at
the host's socket.

**A build error mentioning `link.exe` or `kernel32.lib`**
The MSVC build tools or the Windows SDK are missing. Install the *Desktop
development with C++* workload and open a fresh terminal.

---

## Project layout

```
mesh-chat/
├── Cargo.toml              workspace + release profile (size-optimised)
├── .cargo/config.toml      static CRT for the MSVC targets
├── core/                   the engine — platform independent, fully tested
│   ├── src/
│   │   ├── transport/
│   │   │   ├── mod.rs              Stream / Listener abstraction
│   │   │   ├── lan.rs              direct TCP
│   │   │   ├── bluetooth_windows.rs RFCOMM over Winsock
│   │   │   └── bluetooth_stub.rs   non-Windows placeholder
│   │   ├── crypto.rs       Noise XX handshake + encrypted framed links
│   │   ├── codec.rs        length-prefixed framing
│   │   ├── roomcode.rs     Crockford base32 codes
│   │   ├── protocol.rs     the wire messages
│   │   ├── mesh.rs         hosting, joining, approval, full-mesh wiring
│   │   ├── files.rs        chunked, hashed file transfer
│   │   ├── history.rs      transcript + export
│   │   └── ratelimit.rs    join-attempt throttling
│   └── tests/              approval_flow, mesh_group, file_transfer, churn
└── ui/                     the Windows front end
    ├── ui/app.slint        the entire interface
    ├── build.rs            compiles the .slint file
    └── src/main.rs         wires the engine to the interface
```

The split is deliberate: `core` knows nothing about Slint and `ui` knows nothing
about sockets. Everything in `core` is driven through one `Node` handle and one
event queue, which is what makes the engine testable without a display.

---

## Limitations and threat model

**What this protects against.** Passive eavesdropping on Wi-Fi or Bluetooth;
anyone on your network who was not given the code; a server operator reading
your messages (there is no server); a record of the conversation surviving on
someone else's infrastructure (there is none).

**What it does not protect against.**

- *An attacker who gets the code during its ten-minute window and is on your
  network.* They can reach the host — but they still have to get past a human
  pressing Accept. Check the name and fingerprint before you accept anyone.
- *A compromised machine.* Messages are in memory in plaintext on every
  participant's machine, and anyone at a participant's keyboard can export the
  transcript.
- *Traffic analysis.* An observer can see that two addresses are exchanging
  encrypted data, and roughly how much.
- *Identity across sessions.* Keys are regenerated every launch, so a
  fingerprint tells you "the same key as five minutes ago", not "the same person
  as last week". There is no persistent identity, by design.
- *Forgetting to verify.* The fingerprint check only helps if you actually do it.

**Other limits.** Windows only. Same-LAN or Bluetooth range only — there is no
internet mode in this build. New joiners cannot arrive after the host leaves.
Messages are capped at the frame size, and each file transfer is a separate
stream per peer, so sending one large file to eight people sends it eight times.

---

## Licence

MIT OR Apache-2.0, at your option.
