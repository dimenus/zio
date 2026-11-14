# Design brief: a sim network for zio (t-4086, t-4087), revision 3

Status: **phase 1 (section 12) is built, and four rounds of outside review
of it are closed (section 15). Section 15 is the acceptance record of phase
1. Phase 2 (faults and replay) has its design check (section 16) and its
API (section 5); building waits for the owner's answers in section 16.**

Revision 2 (dd55885) was reviewed by another vendor's model. This revision
takes the decisions made after that review. Section 13 lists what changed
and why.

This repo keeps no design notes (`docs/` is the user-facing mkdocs site), so
the brief sits beside the probe that found the gaps. As each phase is built,
its protocol moves into doc comments beside the code, its reasoning into the
commit message, and this file loses that section. When the last phase lands,
the file is deleted.

## 1. Goal and users

The goal: code that opens its own sockets runs under the simulator
(`-Dsim=true`), so a test of that code replays from a seed.

- **Datagram track (t-4087), for hotseat:** UDP on 127.0.0.0/8, with message
  boundaries and truncation, `net_poll`, and seeded loss, delay, reorder and
  duplication. hotseat's ENet control link and its media run over it.
- **Stream track (t-4086), for starh2:** `listen`, `connect`, `accept` and
  `getpeername` on 127.0.0.0/8, so `dst-conn` can run through the accept
  loop and admission (starh2 `tools/dst-conn/main.zig:982`).

## 2. Non-goals

- A TCP stack.
- Addresses that are not loopback, DNS, multicast, broadcast, IPv6 (to start).
- A C API exported by zio.
- The sim network on Windows. Handles there are not small integers, so the
  fd-namespace proof of section 3 does not apply, and `sim.begin` refuses a
  run that opens a sim socket there.
- Any change to a run that uses none of this (section 7, promise 1).

## 3. The seam: a logical socket interface

The seam is **logical**, not one file. A sim socket is reachable in exactly
three ways, and every other way out of zio refuses it.

1. **The Loop-level sim dispatch** (`src/ev/loop.zig`, `submitSimIo`). The
   suspending ev ops (`NetOpen`, `NetBind`, `NetRecvFrom`, `NetSendTo`,
   `NetRecvMsg`, `NetSendMsg`, `NetPoll`, `NetClose`, and today's
   `NetRecv`/`NetSend`/`NetShutdown`) go there under sim and never reach a
   backend. **This stays.** io_uring and IOCP submit ops without going through
   any `os/` wrapper, so a branch in `os/net.zig` alone could not stop them.
2. **The non-suspending socket layer**, the functions in `src/os/net.zig` a
   program calls by fd (section 3.1). Each has a sim branch for a sim fd.
3. **Nothing else.** Every other native exit inside zio that takes an fd
   refuses a sim fd: `sim: a sim fd reached <exit>`. That covers:
   - the generic file and pipe paths: `File.fromFd(...)` and then `close`,
     `read`, `write`, `readStreaming`, `stat`, ...;
   - the blocking dispatch (`ev/blocking.zig` `executeBlocking`, which a call
     with no current task takes);
   - the direct calls left in `net.zig` and `io.zig`: socket options through
     `setBoolOption`/`setIntOption` on Windows, `setNoDelay`, `getNoDelay`,
     `fileSeek*`, `fileLock*`, `isTty`, ....

   **How the exits are found:** a script lists every function in
   `src/os/*.zig` that takes an fd or handle and calls the kernel. Every
   caller of those functions in `src/` is then either the socket layer or
   gets the refusal. The list, with each exit's verdict, is in phase 1's
   commit.

### 3.1 The manifest of the non-suspending layer

In a normal build each row is the syscall. Under sim each row works on the
sim socket table, or does what the row says.

| function | datagram, under sim | normal build |
|---|---|---|
| `socket(.ipv4, .dgram)` | a sim fd | `socket` |
| `bind(fd, addr)` | port 0 takes the next ephemeral port; `AddressInUse`; any 127/8 address or 0.0.0.0 | `bind` |
| `getsockname(fd)` | the bound address | `getsockname` |
| `setNonBlocking(fd, on)` / `isNonBlocking(fd)` | stored and read back | `fcntl` |
| `setsockopt` / `getsockopt` | per option, section 8 | as asked |
| `enableDestinationAddress(fd)` | turns on `to` in `recvmsg` (stored flag) | `IP_PKTINFO` (Linux) / `IP_RECVDSTADDR` (BSD, macOS) |
| `sendto(fd, to, iov)` | one datagram; completes at once | `sendto` |
| `sendmsg(fd, to, from: ?Addr, iov)` | `from`, if given, must be an address the socket can receive on, else `error.AddressUnavailable` | `sendmsg` with `IP_PKTINFO` (Linux) / `IP_SENDSRCADDR` (BSD, macOS) |
| `recvfrom(fd, iov, .{ .dontwait, .trunc })` | one datagram, its source; `WouldBlock` when empty; `trunc` reports the full length, and the datagram is consumed | `recvfrom` |
| `recvmsg(fd, iov, flags)` | as `recvfrom`; refused on a socket that asked for destination addresses (use `recvDatagram`, which gives `to` as a value) | `recvmsg` and the control message |
| `poll(fd, events, timeout = 0)` | `{ readable, writable }` now; a closed socket or a sim pipe fd is refused for any events; a negative fd is skipped (revents 0), as the kernel does | `poll` with timeout 0 |
| `pollInterruptible(fd, events, timeout_ms)` | timeout 0 only; a positive timeout is refused under sim | `poll`; returns `.interrupted` on `EINTR` instead of retrying |
| `close(fd)` | frees the fd and the port | `close` |
| `connect`, `listen`, `accept`, `shutdown`, `send`, `recv` on a datagram fd | refused (`error.OperationNotSupported`) | as the kernel answers |

**Send errors keep the distinctions a consumer needs:** `SendError` gains
`AddressUnavailable` (`EADDRNOTAVAIL`), done in 696d880. Before, it fell into
`Unexpected`. ENet treats it as transient and returns 0.

**A source address the socket cannot use.** The kernels disagree. Every row
below was run, 3 times each, on 2026-11-14 (macOS 25.6.0 arm64; Linux 6.8.0
aarch64 in colima), through zio's `sendDatagram`:

| socket bound to | source asked for | macOS | Linux 6.8 | sim |
|---|---|---|---|---|
| 0.0.0.0 | 127.0.0.1 | sent | sent | sent |
| 0.0.0.0 | 127.0.0.2 | EADDRNOTAVAIL (lo0 has only 127.0.0.1) | sent, from 127.0.0.2 | sent (all of 127/8 is local) |
| 127.0.0.1 | 127.0.0.2 | EADDRNOTAVAIL | sent, from 127.0.0.2 | `AddressUnavailable` |
| 0.0.0.0 | 192.0.2.1 (no host has it) | EADDRNOTAVAIL | ENETUNREACH | `AddressUnavailable` |

**The sim models the strict answer, macOS's, on every host:** a source must
be an address the socket can receive on (its bound address, or any 127/8
address for a wildcard socket), else `AddressUnavailable`. Why: a peer that
replies from an address it does not own is a bug the sim should catch, and a
sim whose answers depend on the host OS would give one seed two behaviours.
Where the sim's 127/8 is wider than macOS's lo0 (the second row), it follows
Linux, so tests can use more than one loopback address. Acceptance group 7
treats this case apart (section 11).

### 3.2 Consumers, in every build

- **hotseat** owns a small C-callable bridge in its repo that implements
  ENet's socket functions (`enet_socket_create`, `_bind`, `_get_address`,
  `_set_option`, `_get_option`, `_send`, `_receive`, `_wait` and
  `_destroy`) over this layer, in production too. It returns ENet's own
  codes:
  - 0 for would-block and for `AddressUnavailable` on send;
  - -2 for truncation;
  - `ENET_SOCKET_WAIT_INTERRUPT` from `pollInterruptible`;
  - -1 for an unsupported option, which ENet ignores (`host.c:66-68`).

  hotseat's `requireNonBlocking` moves to `isNonBlocking`.
- **A consumer's bridge must not offer an `fd_set` select for sim fds.**
  ENet's unix.h makes `ENetSocketSet` an `fd_set`, which holds descriptors
  below `FD_SETSIZE` (1024 on macOS and Linux). A sim fd is `fd_base +
  index`, 100000 or more (section 3.3), so `FD_SET` on one writes past the
  set. ENet's core does not need it: it waits with `enet_socket_wait`
  (`protocol.c:1953`), which the bridge builds on `pollInterruptible`, and
  hotseat calls `enet_socketset_select` nowhere. So the bridge leaves
  `enet_socketset_select` out. A caller that needs to wait on several sim
  fds uses the layer's `poll`.
- **starh2** moves `getpeername` and `TCP_NODELAY` to the layer (stream
  track).
- A consumer keeps a grep gate in its own repo for direct libc socket calls.
  That is a second line of defence; section 3.3 is the first.

### 3.3 A sim fd is provably invalid to the kernel

A sim fd is `fd_base + index`, where `fd_base = 100000`. A process can own a
real descriptor at that number: `dup2(0, 100000)` succeeded in this shell,
whose soft limit is 1048576. So at `sim.begin`:

1. Read `RLIMIT_NOFILE`. If the soft limit is above `fd_base`, lower it to
   `fd_base`. The kernel then never hands out a descriptor at or above
   `fd_base`, and refuses `dup2` to one (`EBADF`).
2. Read it back, and check that no descriptor is open in the sim range
   (`fcntl(F_GETFD)` gives `EBADF` for each number in the table).
3. If either fails, panic at `begin`.

Every sim socket open re-reads the soft limit, so code that raised it during
the run is caught at the next open. `sim.end` restores the old soft limit.

**Rejected: lowering the hard limit.** It would make the proof permanent, but
it cannot be undone by an unprivileged process, and it would outlive the run.

## 4. Model

One sim host per run, one socket table in `sim.zig`. For each datagram socket
it holds: the local address, the non-blocking flag, the options (section 8),
a receive ring with byte and packet limits, and stats.

- **Ports.** Port 0 binds the next free port from a counter that starts at
  49152, the same in every run.
- **Binding and source address.**
  - A socket that sends before it is bound is bound to the wildcard address
    (0.0.0.0) and the next port first, as both kernels do (table below).
  - A wildcard bind receives datagrams to any 127/8 address on its port.
  - With no `from`, the source is 127.0.0.1 for a wildcard socket, or its
    bound address.
- **A send before bind.** Run on 2026-11-14, 3 times each, with a C probe on
  real sockets (macOS 25.6.0 arm64; Linux 6.8.0 aarch64 in colima); the bridge
  test `sim: enet bridge: a send before bind ...` asserts the same rows in
  both builds:

  | after an unbound socket's first send, to 127.0.0.1 | macOS | Linux 6.8 | sim |
  |---|---|---|---|
  | `getsockname` | 0.0.0.0, a new port | 0.0.0.0, a new port | 0.0.0.0, the next port |
  | its datagram's source | 127.0.0.1 | 127.0.0.1 | 127.0.0.1 |
  | a reply to 127.0.0.1 on that port | arrives | arrives | arrives |
  | a reply to 127.0.0.2 on that port | sent, never arrives (lo0 has only 127.0.0.1) | arrives, from 127.0.0.1 | arrives, from 127.0.0.1 |
  | an unbound socket's first send to 127.0.0.2 | sent, never arrives | arrives, from 127.0.0.1 | arrives, from 127.0.0.1 |
- **The wire.** A sent datagram becomes an entry `{id, flow, bytes, due_at}`.
  A **flow** is the concrete pair (source address and port, destination
  address and port).
  - With reorder off, each flow is first-in first-out, and datagrams in one
    flow due at the same time keep their send order.
  - The same-time shuffle (`takeDue`) is applied across flows only.
- **Receiving.**
  - A due datagram lands in the socket bound to its destination. If none is
    bound there, it is dropped and counted (`dropped_no_socket`).
  - If the socket's ring is over a limit (section 6), the datagram is dropped
    and counted (`dropped_rcvbuf`).
  - The sender has already succeeded either way.
- **Peers that move.** A receiver sees each datagram's real source, so a
  socket that replies to the last source follows a peer to its new port.
  ENet does that (`protocol.c:1088-1089`).

## 5. The test's API (phase 2: faults)

In `zio.sim`, in a sim build, on the sim thread, during a run. `begin` starts
a run with no rule. A test changes rules from a task, at the virtual time
that task runs; a controller task that sleeps to a time and then sets a rule
is exact.

```zig
/// null matches any value. `ip` is in 127/8; `port` is not 0.
pub const Endpoint = struct { ip: ?[4]u8 = null, port: ?u16 = null };
/// One direction: datagrams from `from` to `to`. `.{}` is every datagram.
pub const Path = struct { from: Endpoint = .{}, to: Endpoint = .{} };   // + fn reversed()

pub const Impairment = struct {
    loss_ppm: u32 = 0,         // independent loss
    burst: ?Burst = null,      // Gilbert loss: every datagram is lost in the bad state
    delay_ns: u64 = 0,         // added to wire_latency_ns
    jitter_ns: u64 = 0,        // plus a uniform 0..jitter_ns, per copy
    reorder: bool = false,     // false: never lands before an earlier datagram of its flow
    dup_ppm: u32 = 0,          // a second copy, with its own jitter
};
pub const Burst = struct { enter_ppm: u32, leave_ppm: u32 };

pub fn setImpairment(path: Path, imp: Impairment) void;
pub fn clearImpairment(path: Path) RuleStats;
pub fn setPartition(path: Path) void;     // drop everything on the path
pub fn healPartition(path: Path) RuleStats;
pub fn silence(ep: Endpoint) void;        // drop everything to or from ep
pub fn unsilence(ep: Endpoint) RuleStats;
pub fn netFaultStats() NetFaultStats;     // the run's count per cause
pub fn netFaultDraws() u64;               // promise 2
pub fn wireDigest() u64;                  // phase 1; send-time drops added
pub const RuleStats = struct { matched: u64, dropped: u64, duplicated: u64, delayed: u64 };
pub const NetFaultStats = struct {
    lost: u64, lost_burst: u64,
    partitioned_at_send: u64, partitioned_in_flight: u64,
    silenced_at_send: u64, silenced_in_flight: u64,
    duplicated: u64, delayed: u64, overtaking: u64,
};
```

A datagram meets the rules twice. The sender succeeds either way.

1. **At send**, once its source is known:
   - a silence of its source or destination, or a partition of its flow:
     dropped (`silenced_at_send`, `partitioned_at_send`; silence first). No
     draw.
   - else the **one** impairment set most recently among those that match:
     1. `burst`: the rule's state moves (good to bad with `enter_ppm`, bad to
        good with `leave_ppm`), then in the bad state it is lost
        (`lost_burst`). The state is the rule's, shared by every flow it
        matches: a link has one bad period.
     2. `loss_ppm`: lost (`lost`).
     3. `dup_ppm`: two copies (`duplicated`).
     4. each copy lands at `now + wire_latency_ns + delay_ns + jitter`.
   - unless that rule says `reorder`, a copy lands no earlier than the
     latest landing of its flow already on the wire. With no rule this
     changes nothing: phase 1's latency is fixed.
   - `overtaking` counts copies scheduled to land before an earlier
     datagram of their flow; `delayed` counts copies later than
     `wire_latency_ns`.
2. **At landing** (the first harvest at or after its time): a silence or a
   partition in force then that matches it drops it
   (`silenced_in_flight`, `partitioned_in_flight`). A link that goes down
   loses what is in it. Else phase 1: no socket, queue full, or landed.

**The draws.** A fault draw is a counter-based hash of the seed, the
datagram's send number in the run and the purpose (burst, loss, dup, jitter
of copy 0 or 1); the formula is in its doc comment. It never draws from the
run's RNG, so faults do not move the schedule's draws, and a model can
compute the same value without calling the sim. A field that cannot change
the outcome draws nothing: ppm 0 or 1_000_000, jitter 0, no burst.

**One rule per path and kind; last set wins among impairments.**
`setImpairment` on a path that has one replaces it, resets its burst to
good, and makes it the most recent. Partitions and silences are outside
"last set wins": any one that matches drops, whatever impairment was set
after it.

**Misuse panics with a message:** any call outside a run or off the sim
thread; ppm over 1_000_000; a burst with `enter_ppm` or `leave_ppm` 0 (a
burst that never starts, or never ends: that is `setPartition`);
`delay_ns + jitter_ns` over 60 s; an `Endpoint` ip outside 127/8, including
0.0.0.0 (a socket bound to the wildcard is `.{ .port = p }`); port 0; an
impairment with every field at its default (there is one way to say "no
impairment": `clearImpairment`); a clear, heal or unsilence of a rule that
is not there; a second partition or silence of an equal path; more than 64
rules.

**What is recorded.** A drop at send is a `dgram` trace event and a wire
digest record at the send time, with outcome 3 (lost), 4 (burst), 5
(partition) or 6 (silence); a drop in flight is recorded at landing as 7
(partition) or 8 (silence); a copy sets bit 0x80 of the outcome. Phase 1's
outcomes 0, 1 and 2 and their records do not change. The state digest hashes
the rule table, burst states and counts in a run that set a rule. Rule
changes are not in the trace or the wire digest: a rule that acts on nothing
leaves both unchanged (promise 2).

- **Stream track:** `setConnectOutcome(path, .accept | .refuse |
  .no_answer)` comes with that track.
- `datagramPair` stays private to zio's tests.

## 6. Receive limits

Each datagram socket has two limits, set independently:

- **bytes:** `SO_RCVBUF`, else 212992;
- **packets:** 1024.

Each datagram costs its length plus 256 bytes against the byte limit, so an
empty one costs something, and 1 against the packet limit.

**Storage:**
- It is provisioned per socket, from the runtime allocator, at open.
- A later `SO_RCVBUF` re-provisions it. Datagrams already queued stay; if
  they now exceed the new limit, nothing is dropped, but new ones are dropped
  until the queue is under the limit.
- If the allocation fails, the sim panics. `setsockopt`'s error set has no
  `SystemResources` in a build without sim, and only `AddressUnavailable` may
  change that build. A kernel never fails `SO_RCVBUF` for memory (it stores a
  number), so there is no kernel answer to copy; a harness out of memory is
  broken anyway. Running out of memory at open or at send is
  `error.SystemResources`, which those error sets have.

**Overflow is a counted receive loss (`dropped_rcvbuf`), never a panic.** A
harness asserts its own budget (`dropped_rcvbuf == 0`) separately.

## 7. Determinism: three promises, and the consumer rules

1. **Old workloads are unchanged.** Test: starh2's three sim steps keep their
   trace hashes, and zio's own sim tests pass. **Two `dst-zio` fixtures
   (`spawn_cancel_race`, `spawn_read_after_publish`) are not repeatable at
   the pin itself** (round 4: two or three values over 13 pin runs). For
   them the gate compares the *set* of values over 10 runs at the pin and
   10 on the change, not one run, until t-4092 is fixed.
2. **Inactive fault settings draw nothing** (phase 2). `netFaultDraws()`
   counts the fault code's own draws, and no fault draw touches the run's
   RNG. Test: a run with no rule, one with a rule set and cleared before any
   send, and one with a rule that matches no datagram make 0 fault draws
   and give equal traces and wire digests. A delay-only rule also draws 0.
3. **Faulted workloads repeat** (phase 2). This depends on t-4092: today
   `sim.taskId` keys tasks by pointer, and a workload that frees and spawns
   tasks can get different ids in different processes. The same seed is run in two fresh
   processes, with the heap moved in the second. The test compares the trace,
   the state digest, `wireDigest()`, and the receiver's own byte transcript,
   which it builds by itself.

**Sensitivity**, so equal digests mean something (phase 1 for the wire):

- the same run with one payload byte changed gives a different
  `wireDigest()`, while the seed, lengths and endpoints are the same;
- the receiver's transcript is compared directly, not only through the hash.

**Consumer rules (foreign code under sim):**

- Its clock goes to zio's routed clock. hotseat routes ENet's time function
  (`src/enet_patch/monotonic_time.c`, which calls `clock_gettime` today) to
  `zio.Timestamp.now`. That is hotseat's work.
- Every source of entropy in it is seeded from the sim. ENet's
  `host->randomSeed` mixes in the host pointer (`host.c:79-81`) and is sent
  in `connectID` (`host.c:215, 260`). Both peers set it from `zio.random` on
  an executor before the first connect.
- **zio's side** (phase 1):
  - `zio.randomSecure` is refused during a run, on every thread, including
    through `std.Io`'s `randomSecure`. It is **never** replaced with
    deterministic bytes.
  - `zio.random` off an executor is refused during a run.

## 8. Options: four classes

| class | under sim | examples |
|---|---|---|
| **modelled** | the option has its documented effect | `SO_RCVBUF` (byte limit), non-blocking, `enableDestinationAddress`, `TCP_NODELAY = 1` and `SO_REUSEADDR = 1` (stream track) |
| **accepted, no effect** | succeeds, as the kernel does; nothing changes | `SO_SNDBUF` |
| **unsupported, advisory** | returns `error.OptionNotSupported` (zio's existing name); nothing changes; the caller may ignore it | `SO_RCVLOWAT`, `SO_REUSEADDR` on a datagram socket, `SO_RCVTIMEO`, `SO_SNDTIMEO`, `IP_TOS`, `IP_TTL`, `IPV6_TCLASS`, and ENet's QoS option (`SO_NET_SERVICE_TYPE` on macOS, `SO_PRIORITY` on Linux), `TCP_NODELAY = 0` (stream track) |
| **misuse** | panics with the option's name | an option on a closed or unknown fd; `SO_REUSEPORT`; broadcast and multicast options; a TCP option on a datagram socket; any other option or level; a non-positive `SO_RCVBUF` |

**Reading an option** (`getsockopt`): modelled are `SO_RCVBUF` (the byte
limit), `SO_ERROR` (always 0) and the destination-address option. Reading any
other option panics: `getsockopt`'s error set is only `Unexpected` in a build
without sim, so there is no "unsupported" to return. ENet's core reads no
option, and hotseat's bridge will not; `IP_TTL` is not modelled.

- An unsupported option returns an error, so ENet's ignored -1 works
  (`host.c:66-68`).
- **Why `SO_SNDBUF` is accepted, not unsupported.** ENet sets it on every
  host (`host.c:68`), and a kernel answers 0. A sim that answers a call a
  consumer really makes with another code sends the code under test down a
  different branch: ENet ignores it, another caller may not. In the sim a
  send never waits or fails for buffer space, so any send buffer size is
  already satisfied, and 0 with no change is the true answer. Reading it
  stays not modelled (below). **If phase 2 adds a send-side limit, `SO_SNDBUF`
  must be modelled for real then.**
- **A socket with the destination-address option on cannot use `recvmsg`
  in a sim run:** not the layer's `recvmsg`, not the ev op `NetRecvMsg`,
  and not `Socket.receiveMsg`, with or without a control buffer. It panics
  and names `recvDatagram`, which returns the destination as a value. The
  kernels disagree on the case with no control buffer (Linux 6.8 sets
  `MSG_CTRUNC`, macOS 25.6 does not; measured, see 2e951b2), and the sim
  does not model a host-dependent flag. `recvDatagram` does not suspend, so
  an async typed receive (an ev op that returns the destination as a value)
  will be added when a consumer needs one. zio's own code never turns the
  option on; only a caller does.
- hotseat's Darwin tests that need a 1-byte `SO_SNDBUF` to get `EMSGSIZE`
  stay real-socket tests. Under sim the option is accepted with no effect,
  and their sends would succeed. Send pressure is not modelled (section 10).
- **TIME_WAIT is not modelled.** Stream rebinding without `SO_REUSEADDR` is
  refused (stream track). starh2's listeners set `reuse_address = true`
  (`src/edge/server.zig:1984, 2033, 2063, 2151`).

## 9. What is refused, by name

- Non-loopback addresses.
- IPv6.
- `recvmmsg`/`sendmmsg`.
- `MSG_PEEK`.
- Send flags other than `MSG_NOSIGNAL` and `MSG_DONTWAIT` (accepted with no
  effect): `MSG_OOB`, `MSG_EOR`, `MSG_DONTROUTE`, `MSG_CONFIRM`,
  `MSG_FASTOPEN`, and any flag added later.
- `connect` on a datagram socket.
- The misuse class of section 8.
- A sim fd at a native exit (section 3).
- A real fd passed to a sim op during a run.
- A positive poll timeout.
- A blocking receive on a socket not set non-blocking.
- `zio.randomSecure` and off-executor `zio.random` during a run.
- A datagram over 65507 bytes.

## 10. Fidelity limits: what is not modelled, so not tested under sim

- **Send pressure** and local send failures (`EAGAIN`, `ENOBUFS`,
  `EMSGSIZE`). `AddressUnavailable` occurs only for a bad `from`.
- **Connected UDP:** peer filtering, and ICMP-driven `ECONNREFUSED`.
- **TCP internals:** segments, Nagle, windows, retransmits, keepalive,
  TIME_WAIT, abortive close, kernel backlog overflow. Half-close is modelled.
- **MTU and fragmentation.**
- **Loopback ordering and loss under load.**
- **Interface and address changes.**
- **Network timing relative to CPU load.**
- **Memory accounting:** the receive limits are a declared model, not
  Linux's.
- **Signals interrupting a poll:** `pollInterruptible` cannot be interrupted
  under sim, because its timeout is 0.
- **Corrupted bytes.** A real UDP datagram with a bad checksum is dropped by
  the receiving kernel, so a program sees loss, never changed bytes. Loss is
  modelled; corruption is not, and never will be.
- **Bandwidth and queueing.** Delay is drawn per datagram; there is no link
  rate, no queue that grows with load, and loss does not depend on load.
- **NAT rebinding**, where the peer sees a datagram's source port change
  while the sender's socket stays the same (section 16, open point 3).

## 11. Phase 1 acceptance

Each property gets a test and a mutant that fails it. The tests are in this
repo's suite under `sim: `.

1. **Cross-interface exchange.**
   - C sends to an ev receiver, and ev sends to C.
   - Check the actual bytes, the source and destination addresses, and
     scatter/gather boundaries (two iovecs sent, one datagram received whole;
     one datagram read into two iovecs).
2. **Real bridge semantics, from C:**
   - verified non-blocking state;
   - an empty receive gives 0 (would-block);
   - a zero-length datagram arrives as a datagram of length 0;
   - truncation returns -2, and the truncated datagram is consumed (the next
     receive is the next datagram);
   - an unsupported option returns -1 and nothing changes;
   - **an ENet-shaped host creation:** socket, non-blocking, `RCVBUF`,
     `SNDBUF` (0, no effect), `enableDestinationAddress`, bind to port 0,
     `getsockname`.
3. **Addressing:**
   - port 0 gives 49152, 49153, ... in every run;
   - a bind conflict gives `AddressInUse`;
   - a wildcard receiver gets the destination address;
   - a reply with `from` set leaves from that address;
   - a bad `from` gives `AddressUnavailable`.
4. **Readiness lifecycle:**
   - `poll(…, 0)` reports readable only once a datagram has landed;
   - a `NetPoll` parked before the datagram is sent wakes when it lands,
     with no second send;
   - a parked `NetRecvFrom` cancelled gives `Canceled`, and the datagram is
     still there for the next receive;
   - rearming after a wake works;
   - `close` wakes a parked op;
   - a closed fd number is reused by the next open, and does not see the old
     socket's data.
5. **Independent limits:**
   - a small byte budget drops by bytes;
   - with a large byte budget, the packet limit drops empty datagrams;
   - raising `SO_RCVBUF` after open lets more in;
   - one socket's overflow does not drop or block another socket's
     datagrams.
6. **Isolation and replay:**
   - a real fd in a sim op is refused;
   - `File.fromFd(sim_fd).close()` and the blocking dispatch are refused;
   - a sim socket call from another thread is refused;
   - the fd namespace check of section 3.3 panics when it cannot hold;
   - two fresh processes give the same byte transcript and `wireDigest`;
   - one changed payload byte gives a different `wireDigest`;
   - starh2's three sim steps keep their hashes.
7. **Production-path parity:** the same tiny C bridge runs against the real
   layer in a normal build (real loopback sockets), with the same return
   values and the same metadata checks (destination address, source address,
   truncation, would-block).
   - **Not a parity case: a source address the socket cannot use** (section
     3.1's table). On macOS the normal build must match the sim exactly. On
     Linux the test records and asserts what the kernel does (ENETUNREACH for
     192.0.2.1; sent for 127.0.0.2, from a wildcard socket and from one bound
     to 127.0.0.1), so a kernel change is seen.

**Phase 1 passing is not approval to replace hotseat's production ENet
sockets.** hotseat's phase has its own gate.

### hotseat's acceptance gate (the consumer's, not zio's)

Before hotseat replaces ENet's socket half in production:

- old and new builds run **interleaved** (ABAB...), with representative
  **receive** traffic (a host sending media and control while keys flow);
- **exact** per-key samples (`key_wire`), not histogram buckets;
- an **explicit bound**, set before the run, on the difference of the median
  and of a high percentile;
- a **positive control:** a build that adds 10 µs per send must be detected
  by the same procedure. If it is not, the experiment is inconclusive, not a
  pass.

## 12. Build order

- **Phase 1:**
  - addressed UDP on 127/8 through the logical socket interface: the table,
    the layer's sim branch, the ev ops, `net_poll`, boundaries and
    truncation, per-flow order, limits;
  - `dgram` trace events, digest entries and `wireDigest`;
  - the fd-namespace proof and the native-exit refusals;
  - the entropy refusals;
  - `AddressUnavailable` and `pollInterruptible`;
  - the C-bridge tests in both builds.
- **Phase 2:** faults, counters, validation, and promises 2 and 3, in four
  slices, each ending at a commit with every gate of section 15:
  1. promise 1 as a test (a datagram workload's trace, state and wire
     digests pinned at 33b95ca); the rule table, validation, the draw,
     silence, partition, loss and burst, the counts, the trace and digest
     records; promise 2;
  2. delay, jitter, the flow-order clamp, reorder and duplication; a test
     of a parked op woken by a delayed landing (section 16, point 1);
  3. the op-level model test learns the faults (section 16);
  4. promise 3: a faulted workload in ten fresh processes, and one changed
     rule gives another digest.
- **Phase 3 (hotseat's):** ENet over its bridge, under sim and with the
  production gate above.
- **Stream track (starh2's):** shares the table, fds, ports, address rules,
  the layer's sim branch, the op dispatch and the trace framework. It starts
  after phase 1 has merged those.

## 13. Changes from revision 2

| finding | revision 2 | now |
|---|---|---|
| B11 | "a sim fd is above any real one" | false: `dup2(0, 100000)` succeeds here. Soft-limit proof at `begin`, re-checked at each open (3.3) |
| B12 | `os/net.zig` is the only way to a socket | a logical interface: the Loop dispatch stays, the layer branches, every other native exit refuses a sim fd, found by a script (3) |
| B13 | `SO_SNDBUF` refused with a panic, which would kill every ENet host | three option classes; unsupported means an error the caller can ignore; destination and source addresses as typed data; an ENet-shaped host creation in the acceptance list (8, 11) |
| B14 | the layer's error mapping replaces ENet's | `AddressUnavailable` on send; `pollInterruptible`; a `poll(…, 0)` row; `enet_socketset_select` named (3) |
| B15 | entropy only | the clock rule restored for consumers; `randomSecure` refused during a run, also through `std.Io`; never deterministic (7) |
| B16 | "adopt if inside the spread" | interleaved runs, receive traffic, exact samples, a bound, a positive control; it is hotseat's gate (11) |
| B17 | equal digests in two runs | one-byte sensitivity, an independent byte transcript, the packet limit isolated from the byte budget (7, 11) |
| all-off rules | normalized to no rule | stated: removal only, which does not override a wider rule (5) |
| the three questions | open | answered: keep the packet charge; refuse off-executor random; keep the rebind restriction (starh2 sets reuse) |

Rejected again, as in revision 2 (section 13 there): libc interposition, a
sim-only C shim, a proxy thread, a TCP stack, per-fd faults, random ports,
reorder by default.

## 14. Premises and open points

- hotseat calls ENet from one task, with a service timeout of 0. This was
  read: `protocol.c:1861, 1935, 1953`.
- hotseat's ENet build can drop `unix.c`'s socket half. The review judged this
  feasible; hotseat confirms it in its phase.
- Phase 1 runs on macOS and Linux. Windows sim sockets are refused (2).

## 15. State of the build (for whoever picks this up)

Updated at the end of each sitting. Reports are written outside the repo, at
`/private/tmp/claude-501/-Users-rsaunderson-Source-mine-hotseat/e4460054-682f-47b6-b854-5c24f700217b/scratchpad/zio-sim-report-N.md`.
The scripts the gates below use are beside them, in `zio-sim6-tools/` there
(set `WORK` to a scratch directory, or they work in their own folder).

### Done

- **Foreign-thread rule (t-4085).** `sim.refuseOtherThread` and
  `refuseDuringRun`, with the rule and its coverage in their doc comments
  (`src/sim.zig`). The enumeration tables are the appendices of 8498d89 and
  7ab5299.
- **The fd-namespace proof and the entropy refusals (7dbc10b).** Its tests
  run under `zig build test` since 18ec7c8 (a test child moves the sim fd base
  under the build runner's hard limit; `testMoveFdBase`).
- **t-4092 (cdba949):** each task carries its sim id (`AnyTask.sim_id`).
- **`AddressUnavailable` in `SendError` (696d880):** the one normal-build
  change; its commit shows the two functions whose machine code changed.
- **Datagram sockets through the socket layer (1dab14f):** the table, the
  wire, landing, per-flow order with a seeded interleave across flows, the
  default byte and packet limits with drop counters, `sendDatagram`,
  `recvDatagram`, `enableDestinationAddress`, `setNonBlocking`,
  `isNonBlocking`. Protocol: the block comment above `pub const Ip4` in
  `src/sim.zig`.
- **Poll and the ev ops (da0b4e1):** `poll` and `pollInterruptible` on sim
  fds; every ev op on a datagram socket goes through the same layer calls, and
  parks on the socket where a backend would wait; a real fd in a sim op
  refuses.
- **Options and limits (ec6ec37):** the option classes
  (`dgramSetOption`'s doc comment; `SO_SNDBUF` moved to its own class
  later, see below); `SO_RCVBUF` re-provisions the queue; the
  sim's allocator can fail in a test (`testSetNetAllocator`).
- **Trace and `wireDigest` (31783f6):** a `dgram` trace event per landing;
  `wireDigest()`; the datagram table in `stateDigest` only for a run that
  opened a socket. Two pipe-only probes' hashes are pinned, so promise 1 is a
  test now, not only starh2's gate.
- **The native-exit refusals (ed2ecee):** `sim.refuseSimFd(@src(), fd)` first
  in each fd-taking function of `src/os/` without a sim branch (75 lines, 7
  imports); `src/sim_exits_test.zig` fails the build when one is missing. Its
  rules and hand lists are in its header comment; the cost against upstream is
  in ed2ecee's message.
- **The C bridge tests (7833cee):** `src/enet_bridge_test.c` is an
  ENet-shaped bridge over the layer (unix.c of cgutman/enet at aca87840b57f,
  for the socket functions of section 3.2) with C tests;
  `src/enet_bridge_test.zig` exports the `zio_test_*` shims it calls and runs
  each test, in a sim run in a sim build and on real loopback sockets without
  sim. The test binary has its own root module in build.zig, so only it
  compiles the C file. Protocol: the header comments of both files.
- **SO_SNDBUF accepted with no effect, and no fd_set select (after
  158ebba):** the owner's two decisions on report 10 (sections 3.2 and 8).
  The bridge test's `enet_socketset_select` and its test are gone, and
  SO_SNDBUF sits in the bridge test's shared table.
- **Four rounds of outside review, all closed (3727c95 to d939580), and
  three model tests:** the next sections. Each fix commit holds the failing
  run before the fix, the mutants, and what was rejected.

### Phase 1 acceptance: what holds, group by group

Every test named here runs under `zig build test -Dsim=true
-Dscheduling=pinned -Dtest-filter="sim: "`. The `sim: enet bridge:` tests
also run in a build without sim, against real sockets. A test's mutants are
in the message of the commit that added it.

1. **Cross-interface exchange: holds, in both builds.** `sim: enet bridge: C
   sends two buffers to a zio task's ev receive, which replies with two
   buffers that C reads into two: bytes, sources, destination`. At the Zig
   level also `sim: dgram ev: the layer sends two buffers to an ev receiver,
   and an ev send of two buffers reaches the layer, whole, with sources`.
2. **Bridge semantics from C: holds.** All `sim: enet bridge:`:
   - `an ENet-shaped host creation: create, bind to port 0, NONBLOCK read
     back, RCVBUF, SNDBUF, get_address, get_option ERROR, then a datagram
     with its destination` (verified non-blocking state; host creation);
   - `a receive with nothing queued is 0, errno EWOULDBLOCK, and writes no
     address`;
   - `an empty datagram is received as 0 with its source written, and the
     next datagram is next`;
   - `a datagram that does not fit is -2 and is consumed`;
   - `BROADCAST and reading TTL give -1 and change nothing, and SNDBUF
     succeeds with the receive side unchanged`. The -1 is the bridge's own,
     for an option it has no case for. No option the bridge sets reaches the
     layer's unsupported class any more, so that class is proved at the Zig
     level: `sim: dgram options: an unsupported option returns
     OptionNotSupported and changes nothing`;
   - `enet_socket_wait with a zero timeout reports receive only once a
     datagram is queued, and send always`;
   - `real sockets only: a signal cuts a wait: ENET_SOCKET_WAIT_INTERRUPT
     when asked for, -1 and EINTR when not`. It skips in a sim build: a sim
     poll takes a zero timeout only, so nothing can cut it (section 10);
   - `a bind to a port in use is -1 with EADDRINUSE, destroy frees the port,
     destroy of ENET_SOCKET_NULL does nothing`.
3. **Addressing: holds.** `sim: dgram: port 0 gives 49152, 49153, 49154 in
   every run`; `sim: dgram: a bind conflict is AddressInUse, for an equal
   address and across a wildcard`; `sim: dgram: a wildcard receiver gets the
   destination address, a reply from it leaves from it, and a bad source is
   AddressUnavailable`; `sim: enet bridge: a send before bind binds the
   wildcard address and a port, and a reply to any local address on that
   port arrives` (both builds; section 4's table).
4. **Readiness lifecycle: holds.** `sim: dgram ev: poll with a zero timeout
   reports readable only once a datagram has landed, and writable always`;
   `sim: dgram ev: a poll parked before the datagram is sent wakes when it
   lands, with no second send, and rearms`; `sim: dgram ev: a parked receive
   that is canceled gives Canceled, and the next datagram is still there for
   the next receive`; `sim: dgram ev: close wakes an ev receive parked on the
   socket`; `sim: dgram: a closed fd number is reused by the next open, which
   does not see the old socket's data`; `sim: dgram ev: 64 sockets, each
   with a parked receive, all woken at one virtual time`; `sim: dgram ev: a
   negative fd in a poll set is ignored, as the kernel does, beside a sim
   socket`; the `sim: dgram ev refuses:` tests of a poll on a closed
   socket or a pipe end. Close, reuse and the harvest batch (rounds 1 and
   3): `sim: dgram ev: a receive parked on a socket that is closed and whose
   number is reused at once ends FileDescriptorNotASocket and leaves the new
   socket's datagram`; `sim: dgram ev: a receive woken by a landing whose
   socket a callback earlier in the same harvest closes and reopens ends
   FileDescriptorNotASocket`, the same for a poll; `sim: dgram ev: a task
   that a CompletionQueue callback runs inside the harvest closes and reopens
   the socket of a receive later in the batch, which ends
   FileDescriptorNotASocket`; `sim: pipe: a receive later in a batch whose
   end, or whose peer's end, a callback earlier in it closes gets the bytes
   it was woken for`; `sim: dgram ev: a race group of two sends that finish
   without deferral completes each member once`, the same for pipe sends;
   `sim: pipe: a receive with no room on a full pipe does not wake the send
   parked on it, and one with room does`. One waiter at a time (round 3):
   `sim: pipe refuses: a second ev receive on an end whose receive is woken
   and not yet harvested`, the same for a send; `sim: dgram ev refuses: a
   second ev receive on a socket whose receive is woken and not yet
   harvested, from a callback earlier in the batch`; `sim: dgram ev refuses:
   two ev ops parked on one socket`. And the op-level model test (below).
5. **Independent limits: holds.** `sim: dgram limits: a small byte budget
   drops by bytes, raising SO_RCVBUF after open lets more in, and queued
   datagrams keep their bytes`; `sim: dgram limits: with a large byte budget,
   the packet limit drops empty datagrams`; `sim: dgram limits: one socket's
   overflow does not drop or block another socket's datagrams`.
6. **Isolation and replay: holds.** `sim: dgram ev refuses: a real fd in a
   sim op`; `sim: exits refuse: a file close of a sim fd`; `sim: exits
   refuse: File.fromFd(sim fd).close() in a loop callback, through the
   blocking dispatch`; `sim: exits: every function in src/os that takes an fd
   refuses a sim fd first, or has a sim branch` (it fails on an integer
   parameter it cannot name, and on a function used as a value); `sim: exits
   refuse: an ioctl through the public fs.ioctl alias`; `sim: exits refuse:
   epoll_pwait2 on a sim fd` (Linux only); the refused send flags and
   `recvmsg` on a socket that asked for destination addresses (`sim: dgram
   refuses:`); `sim: dgram trace: the state digest sees where a datagram
   still on the wire goes and where it came from`; the `sim: dgram refuses: a
   socket ... from another thread` tests (open, bind, send, receive, close,
   setNonBlocking, enableDestinationAddress, setsockopt); the `sim: fd
   namespace:` tests; `sim: dgram trace: ten fresh processes with the heap
   moved give one transcript, wire digest, trace and state digest`; `sim:
   dgram trace: one changed payload byte gives a different wire digest, with
   the seed, lengths and endpoints the same`; one loop group per run (round
   4): `sim: refuses a loop of a second loop group polling in one run, which
   would take a datagram op submitted on the first`, the same for a pipe op,
   and `sim: refuses two Runtimes in one sim run`; `sim: dgram refuses: an
   open of a slot whose generation is at its limit`; and starh2's three sim
   steps (the gate below; its result is in the report of each sitting).
7. **Production-path parity: holds.** All `sim: enet bridge:` tests run
   against real sockets on macOS and Linux with the same expected values as
   in a sim run: one table, `expect` in `enet_bridge_test.c`, SO_SNDBUF
   included. The one exception is the one section 11 names, a source address
   the socket cannot use: `sim: enet bridge: a source address the socket
   cannot use: the sim's column in a sim run, the kernel's otherwise` (its
   `source_rows` table, a column per build).

### The outside reviews: four rounds, all closed

Another vendor's model read the code four times; its texts are
`gpt-simnet3.md` to `gpt-simnet6.md` in the reports' folder. Each finding
got a test on the code before the fix (its failing output is in the fix's
commit), the fix, and mutants.

| round | read | serious findings | closed by |
|---|---|---|---|
| 1 | 158ebba | 2 P1 (an empty datagram moved the ring head; a close-woken op ran against a reused number) and 8 more | 3727c95 to 6c2e4e0, 9ecfbc6 |
| 2 | 1126ebe | 1 P1 (a batch entry completed twice), 5 more, and three model tests | 794ce5d to 47ac04c; 4a228c9, 6299984, 1143822 |
| 3 | b9b9bd1 | 1 P1 (a landing-woken op reached a reused number from inside the batch), 3 P2 | 5c048a8, 9f48648, 0c0b061 |
| 4 | 0c0b061 | no P1; 2 P2 and 1 small | 621d29a, 72c1693, d939580 |

### Round 1 of the outside review (gpt-simnet3.md, of 158ebba)

Each finding is taken in the review's order: a test from its sequence, run
on the code before the fix (it must fail), then the fix and a mutant of it.
Where the review offers "model it or refuse it loudly", refuse loudly unless
ENet or a plain UDP program on loopback needs it. Finding 10 was about the
select adapter, which 9ecfbc6 removed.

- **1 (P1), fixed in 3727c95:** an empty datagram queued while the byte ring
  emptied moved the ring head; two later payloads shared bytes. Tests: `sim:
  dgram: an empty datagram queued while the ring empties does not make two
  later payloads share bytes`, and the model test `sim: dgram: the receive
  queue matches a list of byte strings under random sends, scatter
  receives, ...` (since sitting 16; it fails on the old line at seed 1,
  step 1364).
- **2 (P1), fixed in 1b2d7ce:** an op woken by close ran against the socket
  that reused its fd number. Test: `sim: dgram ev: a receive parked on a
  socket that is closed and whose number is reused at once ends
  FileDescriptorNotASocket and leaves the new socket's datagram`. Pipe ends
  are never reused within a run, so pipes do not have this.
- **3, fixed in 59b81fc:** a datagram on the wire now hashes its flow,
  sequence and length in `stateDigest`. The queue part did not reproduce
  (the wire digest already fixes the queue), so no code for it. Test: `sim:
  dgram trace: the state digest sees where a datagram still on the wire
  goes and where it came from`.
- **4, fixed in 0002d0e:** `ioctlPosix` (behind `pub const ioctl`),
  `ioctlWindows` and `epoll_pwait2` refuse a sim fd; `renameatx_np` is in
  `extern_fds`. The structural test fails on an integer parameter whose
  name is in neither `fd_names` nor `non_fd_ints`, and on a function used as
  a value. Its header comment has both rules.
- **5, fixed in 862d083:** a poll refuses a closed socket or a pipe end for
  any events, `NetPoll(.send)` refuses a closed socket, and a negative fd is
  skipped as the kernel does.
- **6, fixed in 81598b9:** a send before bind binds the wildcard address
  (section 4 has the measured table; the bridge test asserts it in both
  builds).
- **7, fixed in 58d40ed:** send flags other than `no_signal` and `dontwait`
  refuse, derived from `SendFlags`'s fields.
- **8, fixed in 2e951b2:** `recvmsg` refuses on a socket that asked for
  destination addresses. The kernels differ without a control buffer (Linux
  sets `MSG_CTRUNC`, macOS does not; the commit has the run).
- **9, fixed in 6c2e4e0:** the due list and the harvest buffer hold
  `max_live` entries; `pushDueAt` checks one due entry per live op.
- **The pipe siblings, checked in a336d63:** `sim: pipe: the pipe buffer
  matches a list of bytes under random sends, receives, empty slices and a
  full buffer` (5 seeds x 1000 steps; six mutants killed). No defect found.
  The pipe buffer is not a ring (`drainBuf` moves the rest to the front),
  so it has no wrap. Pipe ends are never reused within a run.
- **The owner's case after finding 2, checked in 2990bc2, and wrong:** an
  op woken by a landing, with its socket closed and its number reused near
  that time. 2990bc2 argued it cannot run against the new socket because
  `takeDue` harvests it in the same call. Round 3 (P1) showed the gap is
  between the entries of one harvest batch: an earlier entry's callback, or
  a task that a CompletionQueue callback runs (`simCoopYield`), can close
  the socket and reuse its number. Fixed with a socket generation (round 3
  below).
- **Open, not done:** t-4096, a zero-length receive on an empty sim pipe
  parks; check what the kernels do first.

### Round 2 of the outside review (gpt-simnet4.md, of 1126ebe)

Same method: a test from the review's sequence, run on the code before the
fix, then the fix and mutants. Each commit has the failing output.

- **F1 (P1), fixed in 794ce5d:** an op already copied into the harvest's
  batch could be canceled by a sibling's group callback (members that do
  not defer their finish) and then completed again. The harvest now skips
  an entry unless its exact (pointer, submit id) is live, and `endOp`
  panics on a second end. The defect is older than this work: it crashes
  the same way for pipe sends at 3417a77 (the zio starh2 pins). zio's own
  task-level waits keep members deferred, so they do not reach it.
- **F2, fixed in 7c0c22e:** a woken pipe receive whose bytes another receive
  took parks again instead of reporting end of file. Superseded in round 3:
  that second receive now refuses at submit, and the repark is gone.
- **F3, fixed in 35fda32:** pipe ev ops refuse every flag that differs from
  its default, except a receive's `dontwait`, by name from the type.
- **F4, fixed in 5500807:** the state digest hashes each datagram socket's
  parked op id and what it waits for (`DgramWait`).
- **F5, decided by the owner, 9407c06:** the `recvmsg` refusal stays;
  section 8 says what it costs.
- **F6, fixed in 47ac04c:** a sim fd in an `SCM_RIGHTS` message on a real
  socket refuses; a parameter named `control` is an fd carrier in the
  structural test, whose header now lists its known limits.
- **The op-level model test, 4a228c9:** `src/sim_op_model_test.zig`
  (`sim: op model: ...`, run in a child process). Its header comment has
  what it generates and checks, the rules that keep the model's answer
  unique, and what it does not reach. With each fix reverted in a scratch
  copy it finds round 1's finding 1 and finding 2 and round 2's F1 and F2
  at its normal length (the seeds and steps are in the commit). Its
  follow-ups (new ops submitted into a group's freed slots inside the
  harvest) kill the two "pointer only" live-check mutants; with the
  follow-ups off, both survive every model check.
- **The datagram queue model test, 6299984:** scatter receives, the packet
  limit, metadata wrap, SO_RCVBUF changes in flight and a drained tail, in
  blocks that keep the first version's mix (which round 1's finding 1
  needs).
- **The pipe model test, 1143822:** the reference calls no helper of the
  pipe code; zero to five slices.
- **Not reached by any model test yet (t-4098):** datagram flows that
  share a landing instant against an op model; a waiting pipe op with a
  deferred callback; a zero-room pipe receive (t-4096). (Pipe close,
  shutdown and reset in the steps were added in round 3.)

### Round 3 of the outside review (gpt-simnet5.md, of b9b9bd1)

F1's guard (`isLive`, `endOp`) was found sound.

- **P1, fixed in 5c048a8:** a
  receive or poll woken by a landing could reach a socket that reused its
  number, because a callback earlier in the same harvest batch, or a task
  that a CompletionQueue owner callback runs (`simCoopYield`), closed and
  reopened it. Each datagram slot now has a generation; an ev op records it
  at submit, and the harvest ends an op without a result
  FileDescriptorNotASocket when it no longer matches
  (`dgramOpOnItsSocket`). This replaces the `dgram_closed` due kind: one
  mechanism. The pipe side has no reuse; a callback that closes an end
  earlier in a batch leaves a later receive its bytes (tested).
- **Findings 2 and 3, fixed in 9f48648, by the owner's decision (no
  multiple waiters):** a
  pipe end takes one unresolved ev receive and one unresolved ev send at a
  time, and a datagram socket one unresolved ev receive or poll for
  reading. Unresolved means parked, or woken and not yet harvested. A second
  one refuses at submit, whatever it would do, and the refusal names the
  first op's state (`refuseSecondPipeWaiter`, `dgramRefuseSecondWaiter`).
  Only an ev receive takes a pipe's bytes and only an ev send fills its
  room, so the pipe reparks are gone: an empty end at a receive's harvest,
  or a full pipe at a send's, is now an invariant panic. The datagram
  repark stays, for a layer receive (`recvDatagram`) that takes the
  datagram first. starh2's gate with the rule: no refusal. A receive with no
  room no longer wakes the parked send (it freed nothing).
- **Finding 4 and the model's gaps, fixed in 0c0b061:** the op model now holds every waiting
  op to its wake: after each poll, a pipe op woken (bytes, room, close,
  shutdown, reset) must have completed `pipe_wake_ns` after its wake, and a
  datagram op as soon as its datagram landed; at the end each op still
  waiting is given what it waits for and must complete, not be cancelled.
  It also generates, inside a poll, a socket closed and reopened, a layer
  receive that takes a woken op's datagram, and a cancel; timers due with
  I/O; pipe close, shutdown and reset; and probes of a second op behind a
  woken one (receive, send, datagram). It does not reach a CompletionQueue's
  coop yield (no runtime); `sim.zig` tests that route alone.

### Round 4 of the outside review (gpt-simnet6.md, of 0c0b061)

No P1. The generation, the one-unresolved-op rule, the removed reparks and
the zero-room change were found sound.

- **Finding 1, fixed in 621d29a:** the due list is the run's, so a loop of
  another LoopGroup took an op submitted on an unrelated loop and finished
  it on itself. Older than the sim network (the same with pipe ops at
  3417a77). A sim run has one loop group by construction (one Runtime's
  executors share it; two Runtimes in a run deadlock), so a second group
  refuses (`sim.claimLoopGroup`), at a sim Runtime's init and at a loop's
  first submit or harvest. No per-group accounting.
- **Finding 2, fixed in 72c1693:** the op model's end proved readiness for
  every waiting op at once, so a race sibling's cancel could hide a lost
  registration. Now one op at a time, no random action meanwhile, and a
  proof counts only when that op completes from it.
- **Small, fixed in d939580:** a slot's generation at `maxInt(u32)` refuses
  the next open, with a message, in every build mode.

### The three model tests

Each fails when a case it exists for never happened (its counts are
required non-zero), prints its seed and step on a failure, and is killed by
the mutants in its commits.

1. **The op-level model test** (`src/sim_op_model_test.zig`, `sim: op
   model: ...`; 4a228c9, 0c0b061, 72c1693). Generates, on four datagram
   sockets and a series of pipe pairs with a bare `ev.Loop`: ev and layer
   sends and receives (scatter, truncation, empty datagrams), polls,
   cancels, race groups with deferred and non-deferred members, follow-ups
   into freed slots inside a harvest, close and reopen (number reuse),
   SO_RCVBUF, clock steps, timers due with I/O, pipe close, shutdown and
   reset, and inside a poll: closes and reopens, layer receives that take a
   woken op's datagram, and cancels. Checks every completion's result and
   bytes against a plain model, every wake against its deadline, and at the
   end proves each waiting op's registration alone; refusal probes check
   that a second op behind a woken one refuses. Finds rounds 1 and 2's four
   defects with each fix reverted (the table in 4a228c9's message). Holes:
   flows that share a landing instant and a waiting pipe op with a deferred
   callback (t-4098); the 1024-packet limit (test 2 covers it); a
   CompletionQueue's coop yield (no runtime; tested alone in `sim.zig`).
2. **The datagram queue model test** (`sim: dgram: the receive queue
   matches a list of byte strings under random sends, scatter receives,
   ...`; 3727c95, 6299984). One socket, one flow: sends left on the wire or
   landed at once, scatter receives, SO_RCVBUF changes in flight, bursts to
   the packet limit, runs of receives that wrap the metadata ring, and a
   drained tail. Hole: one flow only.
3. **The pipe model test** (`sim: pipe: the pipe buffer matches a list of
   bytes under random sends, receives, empty slices and a full buffer`;
   a336d63, 1143822). The pipe buffer alone (`sendBytes`, `drainBuf`),
   zero to five slices, with a reference that calls no pipe helper. Holes:
   storage only (test 1 covers parking and harvest); a zero-length receive
   on an empty sim pipe parks, and what the kernels do is not checked
   (t-4096).

### Next: phase 2 (faults and replay)

- **Phase 2** (section 12): faults, counters, validation, promises 2 and 3.
  The design check is section 16 (sitting 19); the owner's answers came
  with sitting 20: keep the `pushDueAt` check (a delay moves the landing,
  not the wake) and test the delayed wake; silence by endpoint; NAT
  rebinding stays t-4100, not in this phase; and one call that checks every
  rule of a run matched a datagram (`requireEveryRuleMatched`, a panic).
- **Slice 1 (sitting 20): built.** The rules (impairment with loss and
  burst, partition, silence), the draw, the counts by cause and per rule,
  the trace and wire digest records, `requireEveryRuleMatched`, and every
  misuse refusal. Protocol: the comment above `pub const Endpoint` in
  `src/sim.zig`. Tests: the filter `sim: net faults` (10 behaviour tests,
  18 refusals) and the promise 1 pin `sim: dgram trace: datagram runs with
  no fault rule keep ...` (its values measured at 33b95ca). Two of the
  tests are their own oracles: the loss test recomputes every drop from
  its own copy of the draw formula, and the records test hashes the
  records it expects and compares that with `wireDigest()`. Mutants:
  `zio-sim6-tools/mut/20-s1.json` (56, all killed).
- **Slice 2 (sitting 20): built.** Delay, jitter, the order clamp, reorder
  and duplication, with the copy bit in the records. The clamp holds after
  every rule is cleared too (a cleared rule's delayed datagrams may still
  be on the wire), so in a run that ever set a rule every send takes the
  fault path. The `pushDueAt` check stays; the delayed-wake tests run with
  it: `sim: net faults: an ev receive parked before a delayed datagram
  ...`, `... an ev poll parked ...`, `... a receive whose socket is closed
  and its number reused while a delayed datagram is in flight ...`. The
  delay model tests compute every copy's landing time and order from their
  own copy of the formula, with reorder on (218 overtaking) and off (218
  clamped). Mutants: `mut/20-s2.json` and `20-s2b.json` (28, all killed;
  one of them shows the check firing on a wake 1 ns late).
- **Slice 3 (sitting 20): built.** The op-level model test predicts every
  fault itself: its own rule table, burst states and copy of the draw
  formula, the clamp, and the drop rules again at landing. It checks the
  counts by cause and the draws after every step, each removed rule's
  returned counts, and conservation per cause at the end. Seeds 1 to 8 are
  the phase 1 mix, unchanged (each seed's counts equal those at bd82e2e);
  seeds 9 to 16 add fault steps. With faults in every seed, the shapes
  round 1's finding 1 needs fell by half, so the old mix stays a block of
  its own. With faults, a race group holds at most one datagram send:
  `Group.add` puts a member first, so the loop submits members in reverse,
  and a send's number fixes its draws (mutant R1 shows the model failing
  without this rule). Mutants: `mut/20-s3.json` (33, all killed by the
  model's own FAIL line; the address mutant needed rules on 127.0.0.2,
  which holds no socket). Full prediction held for every fault; nothing
  falls back to conservation only.
- **Slice 4 (sitting 20): built.** Promise 3: `sim: net faults: ten fresh
  processes with the heap moved give one transcript, wire digest, trace,
  state digest and fault counts for a faulted run` (every kind of fault,
  set by a controller task at virtual times; the same line on macOS and
  Linux). Sensitivity: `sim: net faults: a faulted run repeats in one
  process, and one changed rule or another seed gives another wire digest,
  trace and transcript`, where one more nanosecond of delay leaves the
  transcript and counts equal and only the wire digest differs. Mutants:
  `mut/20-s4.json` (4, all killed; a draw that reads a heap address is
  caught only by the fresh processes).
- **Phase 2 is built.** Not done in it, by decision: NAT rebinding
  (t-4100). Still open from phase 1: t-4096, t-4098 (the op model's
  holes; flows sharing a landing instant are still kept out of it).
- Open and filed: t-4096 (zero-length receive on an empty sim pipe), t-4098
  (the op model's holes above).
- **For hotseat, not zio:** ENet's receive returns 0 for an empty datagram,
  the same as would-block, so ENet's receive loop stops there for that
  service call. Later datagrams wait for the next call.

### Decided by the design's owner (2026-11-14, after reports 6-8)

- **A bad source address:** the sim stays strict and host-independent; group
  7 treats that case apart (sections 3.1 and 11).
- **Out of memory re-provisioning a queue panics; reading an option that is
  not modelled panics;** `IP_TTL` is not modelled (sections 6 and 8).
- **The native-exit refusals go inside each fd-taking function of
  `src/os/`,** one helper call first in the body, nothing else changed in
  those files, with a structural test that fails toward noise (done in
  ed2ecee).

### How to run things

- **zig:** `$(mise where zig@0.17.0)/zig`. Never `zig` by name;
  `~/.zvm/bin/zig` is 0.16.
- **The sim tests:**
  `zig build test -Dsim=true -Dscheduling=pinned -Dtest-filter="sim: "`. With
  `TEST_VERBOSE=true`, strip the colour codes first
  (`sed 's/\x1b\[[0-9;]*[mK]//g'`); then `grep -c "^sim.test.sim:"` counts
  them. Several print their own text first, so their "OK" is on the next
  line (the ten-process test, the state digest test, the pipe model test,
  the structural test): check those with `grep -A1`.
- **The normal suite:** `PATH="$(mise where zig@0.17.0):$PATH" ./check.sh --ci`.
  It runs host tests only.
- **Linux and Windows:** `check.sh` never compiles a Linux or Windows branch.
  Build `zig build test -Dtarget=aarch64-linux -Demit-test-bin=true --prefix
  <dir>` (with and without `-Dsim=true -Dscheduling=pinned`) and run it in
  colima as the memory note `linux-io-uring-tests-on-mac` says
  (`TEST_FILTER=<one substring>`). The structural test reads `src/os/` from
  the working directory, so copy `src/os` into the image beside the binary
  (`/work/src/os`). `-Dtarget=x86_64-windows` compiles the Windows test
  binary.
- **The C bridge tests in both builds:** the filter `sim: enet bridge`, once
  with `-Dsim=true -Dscheduling=pinned` (the interrupt test skips there)
  and once without sim (all run, on real sockets). On Linux, the same two
  binaries in colima with `TEST_FILTER=sim: enet bridge`.
- **Unchanged without sim:** `textcmp.sh <base commit> <src dir> <label>`
  builds the eight examples ReleaseFast from the base and from the src dir at
  one path, and compares `__TEXT,__text`. It swaps only `src/`; for a change
  to `build.zig`, `textcmp-tree.sh <base commit> <tree dir> <label>` swaps
  `build.zig`, `build.zig.zon` and `src/`. When they differ,
  `fndiff.py <a> <b>` lists the functions that differ, with only moved code
  and data normalized away. Control: the mutex's `locked` from 1 to 3.
- **The op-level model test:** the filter `sim: op model`. It runs itself
  in a child process; the parent prints the child's counts and, on a
  failure, `op model: FAIL seed S step K: ...` or the last step the child
  started. `ZIO_OP_MODEL_SEED=S` replays one seed, `ZIO_OP_MODEL_STEPS=N`
  sets the length; both pass through `zig build test`, and either skips the
  counter floors and the three probe children (a replay is a diagnostic).
  `ZIO_OP_MODEL_STEPS=0` runs the end alone (the registration proof). To run the child by
  hand (to see all its output): the test binary from `.zig-cache/o/*/test`
  with `ZIO_SIM_DEATH_CHILD=1` and `TEST_FILTER` set to the child's name.
- **The revert check:** in a `git clone --shared` of the tree,
  `mut/16-rev-edits.py <dir>` expects clones named after the four fix
  commits under `<dir>` and reverts each fix's code there (only the code:
  the tests that came with it stay); then run the filter `sim: op model` in
  each. `mut/16-run-old-test.py <list.json>` runs a mutant list against an
  older test in a clone at `mut/oldq` (`OLD_TEST`, `OLD_FILTER` name the
  test and the filter).
- **Mutants:** `mut/run.py <list.json> <out>`. A mutant counts only if it
  compiled and its named test is listed failed, or the test process crashed in
  it. Check the reason line of each kill, not just the verdict. It cuts each
  reason line at 200 characters, which can drop a C test's failing line;
  `mut/run-why.py` prints from `enet_bridge_test.c:` on. The bridge's mutant
  list is made by `mut/gen-bridge.py`. Since sitting 13, a mutant can
  carry `why` (a substring; lines holding it are printed as the reason),
  `edits` of the form `[file, old, new]` across several files, and `dump` (a
  file for the full output). The runner of sittings 1 to 12 is kept as
  `mut/run-12.py`.
- **starh2's three sim steps:** `starh2-setup.sh <zio commit> <label>` makes a
  `git archive` of starh2 429b2b3 whose two zio pins point at a `git archive`
  of that zio commit (never the live tree). `starh2-gate.sh <label> 10`, in a
  Rex tab, runs the three steps once and `dst-zio` 9 more times;
  `dstzio-cmp.py <ref> <new>` compares the value sets per line. The reference
  is 0b71a9c (`dstzio-ref.txt`, 10 runs, every line one value). The trace
  digest of a gate's full first run is, exactly,
  `grep -o 'TRACE_HASH=[0-9a-f]*' gate-<label>.txt | sort | md5 -q`; the
  reference value is c05b7576d516675ee650676f4af38437 (65 lines, the same
  for `gate-ref.txt`). Report 9's value came from another command; this one
  replaces it.

### Traps already hit (each cost a round)

- **`-Dtest-filter` is compiled in, and it is one literal substring.**
  - A `|` in it matches nothing.
  - A child process runs the same binary, so a test the filter excluded is
    not in it.
  - "10 of 10 passed" can mean only the unnamed tests ran.
- **A death test must match the panic's own line,** not any substring. The
  stack trace prints the test's source, which holds the expected text.
- **A success child must print `sim-child-done: <name>`,** and the parent
  must see exactly one, naming it.
- **Mutants that fail to compile do not count.** Remove a line with
  `and false`, not by deleting what a variable needs.
- **A mutant that changes the line count moves every later line number** in
  its failure trace: read the reason, not the line.
- **An error from inside a task stops its trace at `join`.** The test line
  (`"<test>" - <ErrorName>`) still names the error.
- **Never claim from one run of something that varies.** Round 3 did, and
  was wrong.
- **Zig's build runner lowers its own hard RLIMIT_NOFILE to 10240 on Darwin**
  (`std.process.raiseFileDescriptorLimit`), and every test binary inherits it.
- **On Linux, raw `fcntl` returns `usize`;** a negative `c_int` mask does not
  fit it.
- **macOS has only 127.0.0.1 on loopback.** A test that needs a second local
  address runs on Linux only.
- **libzio.a is not an instrument.** Its `__text` is empty in every build
  (zio exports nothing), and two builds of one commit differ in 32 bytes (the
  `builtin.zig` cache hash). The examples are what `textcmp` compares.
- **`check.sh --ci` checks formatting before it runs a test,** and zig 0.17's
  `zig fmt` rewrites `@intFromEnum` as `@backingInt`. Run `zig fmt` on new
  files first.
- **The C file builds with `-Werror`,** so a C mutant that leaves a parameter
  unused does not compile. Keep it used: `n > 1 ? 1 : n`, not `1`.
- **The mutant runner read the first "Failed tests:" block** of the output
  (to sitting 12). A death child's own summary is echoed earlier than the
  parent's, so a killed mutant could read "SURVIVED". It reads the last
  block now. Read the full output (`dump`) before you believe a survivor.
- **A mutant that crashes an earlier test is "NOT RUN"** for the test you
  named: the process died first. Read its panic line (`dump`); it still
  counts only with the reason you expected.
- **A new integer parameter in `src/os/` fails the structural test** until
  its name is in `fd_names` (an fd) or `non_fd_ints` (never an fd) in
  `src/sim_exits_test.zig`.
- **The kernels disagree on more than the source address:** a `recvmsg`
  with no control buffer on a socket asking for destination addresses sets
  `MSG_CTRUNC` on Linux 6.8 and not on macOS 25.6. Measure both before you
  write what "the kernel" does.
- **A wider random test can lose a case the narrow one hit.** The first
  wider queue mix let round 1's finding 1 survive: bursts kept the queue
  long and big limits kept the ring big, and that defect needs a short
  queue in a small ring that wraps. Keep the old mix as a block of the new
  one, and keep the old defect in the mutant list.
- **One poll moves the clock only to the next due op.** Settle by clock
  time, not by a count of polls: 40 polls once ended at 28 ms while a pipe
  send woken at 25.26 ms was due at 30.26 ms, and the end check took a
  legal wait for a lost wake.
- **Count skips by reason in a random test.** The op-level test's slot
  pool was full in 7164 steps, which silently cut its traffic in half;
  `no_free_slot` and the per-kind skip counts show it now.
- **Tool rules for agents here:** a recursive `rm` of a path built from a
  variable is refused by a hook; make a new directory instead. Restoring a
  file with `git checkout <file>` is refused too, and would drop uncommitted
  work: let the mutant runner restore it. In zsh, a word that starts with
  `=` (`echo ====`) is a command lookup and fails; quote it.

## 16. Phase 2 design check (sitting 19, 2026-11-14)

Sections 5, 7 and 12 were read against the code at 33b95ca and against the
owner's phase 2 requirements. Section 5 is now the API. What the check
found:

**Open points for the owner (building waits for these):**

1. **The `pushDueAt` check does not need to go.** Section 15 said a delayed
   landing makes it fire. It cannot. Every datagram op is pushed with the
   time `clock_ns` (`dgramClose`, `dgramOpDone`, `wakeParked`; read, and
   `grep -n "pushDueAt(" src/sim.zig` lists those three and the pipe call),
   and the check compares that same value with `clock_ns`. A fault delays
   the **landing** (the wire entry's `due_at`), and a landing wakes its op at
   the time it lands. The check would fire only if the sim delayed the
   **wake** after a landing, and the design does not do that: the queue
   would show a datagram while the op still slept. Recommendation: keep the
   check (a second guard that costs nothing), and add the test of a parked
   receive and a parked poll woken by a delayed landing, also with the
   socket closed and its number reused while the datagram is in flight.
2. **Silence is keyed by an endpoint, not by an fd.** The requirement says
   "one socket". Revision 2 rejected per-fd faults: tests think in links,
   and often do not hold the peer's fd (hotseat's ENet owns its socket). An
   exact endpoint names one socket; `.{ .port = p }` names a socket bound to
   the wildcard; `.{ .ip = a }` names a whole host that the test gave its
   own 127/8 address (the sim treats all of 127/8 as local on every host).
3. **"A path change" is half covered.** A route change (a delay step, a
   loss burst, a short partition) is a rule change at a time, which the API
   has. **NAT rebinding** (the peer sees the sender's source port change,
   and replies to the new port must reach the old socket) is not: it needs
   a NAT table, translation both ways, and an API to rebind at a time;
   about one slice of its own. ENet follows a peer's new source
   (`protocol.c:1088-1089`), so hotseat can test that by opening a new
   socket today. Does hotseat need rebinding in phase 2?

**Decided in the check (the owner may override):**

- **Drop rules act again at landing**, so a partition or a silence started
  while a delayed datagram is on the wire drops it. Rejected: deciding
  everything at send. With a 50 ms delay rule, a partition would then let
  50 ms of traffic through after it began.
- **An all-off impairment panics** instead of meaning "removal". The old
  rule needed a paragraph to say what an all-off rule under a wider rule
  does; now there is one way to remove a rule.
- **The draw is keyed by the send number in the run,** not by a per-flow
  number. Per-flow keys would keep one flow's losses fixed when unrelated
  traffic changes, but need a flow table, and a seed does not survive code
  changes anyway.
- **The burst state belongs to the rule,** shared by every flow it matches.

**Who checks what in phase 2.**

- Promise 1: today only two pipe-only probes are pinned. Slice 1 pins a
  datagram workload too, measured at 33b95ca, 3 runs.
- **The op-level model test can predict every fault in the set.** The
  model keeps its own rule table and burst states, computes each draw from
  the formula (its own code, no call into the sim), and so knows which
  datagrams land, in which socket, and when. It keeps its existing rule of
  one source per destination per landing instant (the sim orders flows that
  land at one instant by the run's RNG), now from the instants it predicts.
  No fault falls back to conservation only. Conservation (every datagram
  sent is landed, dropped with a cause, or on the wire) is checked as well,
  per cause, against `netFaultStats` and per rule.
- The requirements' rule against silent misuse also covers rules that
  never act: `clearImpairment`, `healPartition` and `unsilence` return the
  rule's counts, so a test can assert that its rule matched something.
