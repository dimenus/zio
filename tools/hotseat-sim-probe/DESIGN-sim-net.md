# Design brief: a sim network for zio (t-4086, t-4087), revision 2

Status: **for a second review before anyone builds it.** Nothing here is built.

Revision 1 (5db05ef) was reviewed by another vendor's model. This revision
takes the decisions made after that review. The main change: **one socket path
in every build.** zio's own non-suspending socket layer is the seam, and zio
exports no C. Section 13 lists what changed from revision 1 and why.

This repo keeps no design notes (`docs/` is the user-facing mkdocs site), so
the brief sits beside the probe that found the gaps. As each phase is built,
its protocol moves into doc comments beside the code, its reasoning into the
commit message, and this file loses that section. When the last phase lands,
the file is deleted.

Line numbers are for this branch (`hotseat/sim-second-user`) unless a file
names another repo.

## 1. Goal and users

The goal: code that opens its own sockets runs under the simulator
(`-Dsim=true`), so a test of that code replays from a seed.

- **Datagram track (t-4087), for hotseat:** UDP on 127.0.0.0/8, with message
  boundaries and truncation, `net_poll`, and seeded loss, delay, reorder and
  duplication. hotseat's ENet control link and its media run over it.
- **Stream track (t-4086), for starh2:** `listen`, `connect`, `accept`,
  `getpeername` on 127.0.0.0/8, so `dst-conn` can run through the accept loop
  and admission. Its scope line lists "the accept loop and admission",
  "getpeername" and "more than one connection" as unsimulated (starh2
  `tools/dst-conn/main.zig:982`).

## 2. Non-goals

- A TCP stack. A connected stream stays today's pipe: bytes in order, no
  segments, no windows, no retransmit timing.
- Addresses that are not loopback, DNS, multicast, broadcast, IPv6 (to start).
- A C API exported by zio.
- Any change to a run that uses none of this (section 7, promise 1).

## 3. The seam: one non-suspending socket layer, every build

**zio's non-suspending socket layer** is the only way a program touches a
socket by its fd, outside the suspending `zio.net` API. The functions return
at once. In a normal build each one is the syscall. Under sim, a sim fd (one
from the sim socket table) goes to the table; a real fd under sim is refused.

It starts from the wrappers that already exist in `src/os/net.zig` (`socket`
:327, `bind` :524, `listen` :585, `connect` :631, `accept` :669,
`getsockname` :746, `setsockopt` :773, `getsockopt` :801, `recvfrom` :1295,
`sendto` :1377, `recvmsg` :1463, `sendmsg` :1653, `close` :153). It adds what
is missing:

- `getpeername`;
- `setNonBlocking(fd, bool)` and `isNonBlocking(fd)`, the check hotseat makes
  today with a direct `fcntl`;
- a source address on `sendmsg` and the destination address on `recvmsg`, as
  typed values (the job `IP_PKTINFO` does in C).

**The manifest** of what the layer offers, and so of what a sim fd can do:

| function | datagram | stream (stream track) |
|---|---|---|
| `socket` | yes | yes |
| `bind` | yes (port 0: next ephemeral) | yes |
| `listen` | refused | yes |
| `connect` | refused (connected UDP is not modelled) | yes, non-blocking: in progress, then ready |
| `accept` | refused | yes |
| `getsockname` / `getpeername` | `getsockname` | both |
| `setNonBlocking` / `isNonBlocking` | yes (stored and read back) | yes |
| `setsockopt` / `getsockopt` | only the options in section 8 | same |
| `sendto` / `sendmsg` | yes; `sendmsg` may name a source address | refused (use `send`) |
| `recvfrom` / `recvmsg` | yes; destination address, truncation flag, `WouldBlock` when empty | refused |
| `send` / `recv` | refused (unconnected) | today's pipe |
| `shutdown` | refused | today's pipe |
| `close` | yes | yes |

The suspending ev ops (`NetOpen`, `NetBind`, `NetConnect`, `NetAccept`,
`NetRecvFrom`, `NetSendTo`, `NetRecvMsg`, `NetSendMsg`, `NetPoll`, and today's
`NetRecv`/`NetSend`/`NetClose`/`NetShutdown`) route a sim fd to the same table
through the same functions. **There is one sim implementation, and two ways
to reach it.**

**Consumers move every socket call to the layer, in every build:**

- **hotseat** owns a small C-callable bridge in its own repo. It implements
  ENet's socket functions (`enet_socket_create`, `_bind`, `_get_address`,
  `_set_option`, `_get_option`, `_send`, `_receive`, `_wait`, `_destroy`) by
  calling the layer, in production too. It returns ENet's own codes: 0 for
  would-block, -2 for truncation (ENet `unix.c:763-860`). It does not compile
  ENet's socket half of `unix.c`. ENet's other socket entry points (`listen`,
  `connect`, `accept`, `shutdown`, `select` in `unix.c:410,578,593,606,885`)
  get bridge versions that return -1. With no libc socket call left in the
  bridge, those paths cannot reach the kernel with a sim fd. hotseat's own
  `requireNonBlocking` (`src/enet_control.zig:1648-1653`, a direct
  `std.c.fcntl`) moves to `isNonBlocking`. Its `ev.NetPoll` wait is already
  a zio op.
- **starh2** moves `peerFromHandle`'s `std.c.getpeername`
  (`src/edge/connection.zig:630`) and `setTcpNoDelay`'s
  `posix.system.setsockopt` (`src/http1/oneshot.zig:473-477`, and its twin in
  `src/edge/server.zig`) to the layer, in every build.

**What remains possible, and how it is caught:** consumer code that still
calls libc directly with a sim fd gets `EBADF` from the kernel. A sim fd is
not a real descriptor, and it is above any real one: `fd_base` is 100000. The
consumer catches this with a gate in its own repo, a grep for direct socket
calls, as hotseat already gates names like `enet_address_set_host`. That is
the consumer's protection, not the simulator's. This brief says so plainly.

**Production cost (expected, to be measured; not measured yet):**

- zio: none in a build without sim. The sim branch inside each wrapper is
  `if (comptime sim)`, as for t-4085's checks, which left the machine code of
  seven examples unchanged (c6965cf's commit message).
- hotseat: one direct C-to-Zig call per ENet socket call. That is no extra
  syscall, and the layer's error mapping replaces ENet's `errno` checks.
  The expected cost is tens of nanoseconds, against a loopback `sendmsg` of
  a few microseconds. **This is an inference.**
- How to measure:
  1. a microbenchmark in hotseat of a million `enet_socket_send` of 100 bytes
     on loopback, with unix.c's version and with the bridge, at least 5 runs
     each, giving the median and the spread;
  2. hotseat's own key-to-wire samples (`key_wire` in `src/enet_control.zig`)
     for the same input script with each version, at least 3 runs each.

  Adopt the bridge only if the difference is inside the run-to-run spread.

## 4. Model

One sim host per run, one socket table in `sim.zig`.

```
Socket = {
  kind: datagram | stream | listener,
  local: ?Addr, peer: ?Addr,         // Addr = { ip: 127.x.y.z or 0.0.0.0, port }
  nonblocking: bool,
  options: the modelled options (section 8),
  rx: per-socket receive ring        // datagram: slots and bytes, limits in §6
  stream: ?pipe end                   // connected stream: today's End
  accept_q: connected ends not yet accepted (listener)
  stats
}
```

- **Ports.** Port 0 binds the next free port from a counter that starts at
  49152, the same in every run. Stream and datagram ports are separate
  spaces.
- **Binding and source address.** A datagram socket that sends before it is
  bound is bound first, implicitly, to 127.0.0.1 and the next port.
  A wildcard bind (0.0.0.0) receives datagrams sent to any 127.0.0.0/8
  address on its port, and nothing else. Its source address, when `sendmsg`
  names none, is 127.0.0.1. `sendmsg` may name a source address in 127/8 that
  the socket could receive on (its bound address, or any 127/8 address if it
  is wildcard-bound); any other source is refused. `recvmsg` returns the
  destination address the datagram was sent to. ENet needs this to reply
  from the right address (`unix.c:679-709` sends `IP_PKTINFO`).
- **The wire.** A datagram sent becomes an entry `{id, flow, bytes, due_at}`.
  A **flow** is the concrete pair (source address and port, destination
  address and port). With reorder off, each flow is first-in first-out: a
  datagram's due time is at least that of the previous one in its flow.
  Datagrams in one flow due at the same time keep their send order. **The
  existing same-time shuffle (`takeDue`) is applied across flows only.**
- **Receiving.** A due datagram lands in the socket bound to its destination,
  or is dropped and counted (`dropped_no_socket`). If the socket's ring is
  over either limit (section 6), it is dropped and counted (`dropped_rcvbuf`).
  The sender has already succeeded either way.
- **Peers that move.** A receiver sees each datagram's real source. A socket
  that replies to "the last source" follows a peer to its new address with no
  extra model. That is what ENet does (`protocol.c:1088-1089` copies the
  received address into the peer).
- **Streams (stream track).** `listen` makes a listener with a backlog.
  `connect` to a listener makes a `pipePair` and queues one end; the connect
  completes after the usual seeded I/O delay. `accept` takes the oldest
  queued end. Connecting where nothing listens gives `ConnectionRefused`
  after the delay. A full backlog refuses the connect (section 10 says this
  is not modelled like a kernel).

## 5. The test's API

All in `zio.sim`, sim builds only, on the sim thread (t-4085 refuses other
threads, from the round-3 checks onward).

```zig
pub const Path = struct {
    from_ip: ?[4]u8 = null, from_port: ?u16 = null,
    to_ip: ?[4]u8 = null,   to_port: ?u16 = null,
};

pub const DatagramFaults = struct {
    /// Per million datagrams on the path, dropped when sent. 0..1_000_000.
    loss_ppm: u32 = 0,
    /// Per million, delivered twice; the copy draws its own delay. 0..1_000_000.
    dup_ppm: u32 = 0,
    /// Wire delay, uniform in [min_ns, max_ns]. Null: the default (below).
    delay: ?struct { min_ns: u64, max_ns: u64 } = null,
    /// Off: each flow keeps its order (section 4). On: each datagram keeps
    /// its own drawn delay, so a range wider than the gap between sends
    /// reorders within a flow.
    reorder: bool = false,
    /// Drop everything on the path: a link broken in this direction.
    blackhole: bool = false,
};

pub fn setDatagramFaults(path: Path, faults: DatagramFaults) void;
pub fn clearDatagramFaults(path: Path) void;

pub const ConnectOutcome = enum { accept, refuse, no_answer };
pub fn setConnectOutcome(path: Path, outcome: ConnectOutcome) void;

pub const DatagramStats = struct {
    sent: u64, delivered: u64, duplicated: u64,
    dropped_loss: u64, dropped_blackhole: u64,
    dropped_rcvbuf: u64, dropped_no_socket: u64,
};
pub fn datagramStats(fd: i32) DatagramStats;

/// Bytes on the wire, for replay across processes: a hash over every
/// datagram delivered, in delivery order (id, flow, payload).
pub fn wireDigest() u64;
```

- **Rules: last set wins.**
  - `setDatagramFaults` replaces a rule with an equal `Path`, or adds one.
  - `clearDatagramFaults` removes the rule with that exact `Path`.
  - A datagram uses the most recently set rule whose `Path` matches it.
  - A rule whose knobs are all off is stored as no rule (section 7, promise 2).
  - At most 32 rules. A 33rd panics: it is a harness limit, not a network
    condition.
- **Validation at set:** a rate above 1_000_000, `min_ns > max_ns`, or a
  `Path` with an address outside 127/8 panics with the field's name.
- **The default delay**, with no rule: the seeded 1 ms or 5 ms that stream
  I/O uses today, kept in flow order.
- **No schedule API.** A test changes faults from a task, after it sleeps on
  the virtual clock.
- `datagramPair` stays private to zio's tests. No consumer needs it.

## 6. Receive limits, and losses that are not network faults

Each datagram socket has two limits:
- **bytes:** the `SO_RCVBUF` value set, else 212992 (Linux's default);
- **packets:** 1024.

Each datagram costs its length plus 256 bytes against the byte limit, so an
empty datagram still costs something, and one packet against the packet
limit. The storage for both is provisioned per socket from the runtime's
allocator when the socket is opened. So the documented overflow behaviour is
always reachable: a full ring drops the newest datagram and counts it in
`dropped_rcvbuf`. **It never panics.** ENet's 256 KiB request (`host.c:67`,
`enet.h:204`) is honoured as given.

A harness that wants "no receive loss" asserts it: `dropped_rcvbuf == 0`.
That is the harness's budget, a separate assertion from the socket's
overflow behaviour.

Fixed tables that remain, and panic when full because a full table is a
harness limit: sockets (256), wire entries in flight (4096), fault rules (32).

## 7. Determinism: three separate promises

1. **Old workloads are unchanged.** A run that opens no sim socket draws the
   same numbers, emits the same events and digests the same as before.
   - Event numbers, draw sites and pipe behaviour stay as they are.
   - New event kinds (`dgram`, `conn`) are emitted only by the new code.
   - *Test:* starh2's `dst-zio` trace hashes and `dst-conn`'s `MUTANT_FOUND`
     seeds and hashes are the same as at the pin, plus zio's own sim tests.
     This is the gate t-4085 used.
2. **Inactive fault settings draw nothing.**
   - The fault code counts its own draws (`sim.netFaultDraws()`).
   - *Test:* a datagram workload with no rule and with an all-off rule makes
     **0** fault draws, and the two give equal traces and digests.
   - An all-off rule is normalized to no rule before it is stored or
     digested, so equal configurations digest equally.
   - *Mutant:* draw for loss when `loss_ppm == 0`. The draw counter catches
     it even if both runs drew.
3. **Faulted workloads repeat.**
   - *Test:* a run with loss, duplication and a reorder range is run in two
     **fresh processes** with the same seed. The second process first makes
     an allocation of a size taken from its process id, so heap addresses
     differ between the two.
   - The test compares the trace hash, the state digest and `wireDigest()`.
     Comparing scheduler events alone would miss bytes that differ on the
     wire.

**Consumers' entropy (the rule for foreign code):** every source of entropy
in code under sim must be seeded from the sim.

- *ENet's example:* ENet seeds `host->randomSeed` from the host's address plus
  the time (`host.c:79-81`, `unix.c:219-226`). The seed makes `connectID`
  (`host.c:215`), which goes on the wire in the CONNECT command (`host.c:260`).
  hotseat must set `host->randomSeed` from a seeded source, on both peers,
  before the first connect. Patching the clock alone leaves the pointer in.
- *What the simulator offers:* `zio.random` (`src/random.zig:35`). On an
  executor under sim it is filled from that executor's generator, which is
  seeded from the run's seed (`src/runtime.zig:731`).
- **A gap to close in phase 1:** off an executor, `zio.random` falls back to
  the operating system (`src/random.zig:38-43`). Under sim, phase 1 makes
  that fallback refuse during a run, so a consumer cannot take real entropy
  by mistake.

## 8. Options: modelled, or refused

An option whose effect is not modelled is **refused**, never recorded and
ignored.

| option | effect under sim |
|---|---|
| non-blocking (`setNonBlocking`) | stored and read back; sim calls never block either way, and a blocking call shape (section 9) is refused |
| `SO_RCVBUF` | the byte limit of section 6 |
| `TCP_NODELAY = 1` | modelled: a sim stream never holds back small writes, which is what it asks for |
| `TCP_NODELAY = 0` | refused (Nagle is not modelled) |
| `SO_REUSEADDR = 1` | modelled for datagrams (no TIME_WAIT exists for UDP); for streams, see below |
| `SO_SNDBUF` | **refused**: send pressure is not modelled |
| anything else | refused |

- **Send pressure is not modelled.** A datagram send never fails with
  `EAGAIN`, `ENOBUFS` or `EMSGSIZE` under sim. hotseat's Darwin tests that
  set a 1-byte `SO_SNDBUF` to get `EMSGSIZE` (`src/enet_control.zig:2554,
  2687, 2764, 2852`) stay real-socket tests. Under sim their `SO_SNDBUF`
  call is refused, so they cannot pass by mistake.
- **Stream ports after close.** TIME_WAIT is not modelled. Binding a stream
  port that has carried a connection in this run is refused, unless the new
  socket set `SO_REUSEADDR = 1` (which, on a real kernel, also allows it).
  This brief does not claim "close frees the port" in general.

## 9. What is refused, by name

- Non-loopback addresses.
- IPv6 (to start).
- `recvmmsg`/`sendmmsg`.
- `MSG_PEEK`.
- `connect` on a datagram socket.
- Broadcast and multicast.
- The options of section 8.
- A real fd passed to a sim op or to the layer during a run.
- A blocking call shape: `recvfrom`/`recvmsg` without `dontwait` on a socket
  not set non-blocking, or a poll with a timeout.
- `zio.random` off an executor during a run (section 7).
- A datagram over 65507 bytes (the IPv4 limit). Any size up to that is
  carried; nothing is cut.

## 10. Fidelity limits: what is not modelled, so not tested under sim

- Send pressure and local send failures (`EAGAIN`, `ENOBUFS`, `EMSGSIZE`).
- Connected UDP: peer filtering, and ICMP-driven `ECONNREFUSED` on a later
  send. Refused, not simulated as loss.
- TCP: segments, Nagle, windows, retransmits, keepalive, TIME_WAIT, abortive
  close (RST on close with unread data), backlog overflow as a real kernel
  does it (SYN queue, silent drop). Half-close is modelled (today's
  `shutdown`).
- The MTU and IP fragmentation. Datagrams up to 65507 bytes arrive whole.
- Real loopback ordering under load. Sim keeps flow order unless reorder is
  set; a kernel under pressure can drop instead.
- Interface and address changes (a NAT rebinding is modelled only as a socket
  that sends from a new port).
- Timing of the network relative to CPU work: wire delays are the seeded
  values, not a function of load.

## 11. Tests and the mutants they must kill

All tests are in this repo's suite with a `sim: ` name prefix. Refusals use
the death harness, which matches the panic's own line, not a source line in
its trace (round 3).

| test | property | mutant it must kill |
|---|---|---|
| two `sendto` of 3 bytes, two `recvfrom` of 3 | boundaries | concatenate datagrams |
| `recvfrom` on an empty non-blocking socket | `WouldBlock`, nothing parked | park instead |
| a task in `net_poll(.recv)` wakes when a datagram lands | readiness | complete only on the next send |
| an undersized receive **through the test's C bridge**, written like ENet's `enet_socket_receive` | the bridge returns -2 | drop the truncation flag |
| receive-buffer overflow: `SO_RCVBUF` small, 50 datagrams sent, none read | sends succeed, the first K arrive, `dropped_rcvbuf == 50 - K` | ignore `SO_RCVBUF`; panic when full |
| packet limit with empty datagrams | the packet limit drops them | charge empty datagrams nothing |
| a peer moves: A sends from port p1, B replies to the last source; A sends from a new socket at p2 | B's next reply reaches p2 | deliver only from the first source |
| wildcard bind, `recvmsg` destination, `sendmsg` with that source | the reply's source is the address used | ignore the source address |
| per-flow order, reorder off, two flows due at the same time, seeds 1..20 | each flow in order on every seed; the flows interleave differently on some seed | shuffle within a flow |
| reorder on with a delay range | some seed delivers a flow out of order | ignore `reorder` |
| loss 50 %, 1000 datagrams, seeds 1..5 | `dropped_loss + delivered == sent` and both are non-zero on each seed | never draw loss |
| duplicate 50 % | `duplicated` > 0, each copy delivered | no second copy |
| blackhole one direction | one way dead, the other works | blackhole both ways |
| promise 2: no rule vs all-off rule | `netFaultDraws() == 0` in both; equal trace and digest | draw when a knob is off; digest the stored zero rule |
| promise 3: fresh-process replay with heap perturbation | equal trace, digest, `wireDigest()` | hash a pointer; leave payloads out of `wireDigest` |
| event contribution | a workload of N sends and M deliveries emits exactly N + M `dgram` events, counted per kind | omit `dgram` events from the trace |
| rates and ranges validated | `loss_ppm = 2_000_000` and `min > max` panic | accept them |
| refused names (section 9), one test each | each panics with its message | each check removed |
| stream track: connect with no listener, three connects and accepts, `no_answer` with a 1 s timeout, `getpeername`, two clients from 127.0.0.2 and 127.0.0.3 seen as different peers by an admission check | as named | `ConnectionRefused` never given; LIFO accept; answer anyway; return the listener's address; one peer for all |

## 12. Build order

- **Phase 1 (datagram track): addressed UDP on 127/8 through the
  non-suspending layer, no faults.**
  - The layer's sim branch for datagram sockets: `socket`, `bind`,
    `getsockname`, `setNonBlocking`/`isNonBlocking`, `sendto`/`sendmsg` with
    a source address, `recvfrom`/`recvmsg` with the destination address and
    truncation, `close`.
  - The same through the ev ops, and `net_poll` for datagram sockets.
  - Per-flow order, receive limits, `dgram` events, digest entries,
    `wireDigest`.
  - `zio.random` refusing off an executor during a run.
  - **A test that drives it from C through a tiny bridge, the way a consumer
    would:** a C file in the test calls Zig exports that call the layer, and
    sends and receives between two fixed loopback endpoints.
- **Phase 2:** faults, counters, validation, `netFaultDraws`, and the three
  promises, including fresh-process replay.
- **Phase 3 (hotseat's, briefed separately):** a real ENet exchange under sim
  through hotseat's bridge, with seeded `randomSeed` on both peers.
- **Stream track (starh2's), in parallel:** `listen`, `connect`, `accept`,
  `getpeername`, `setConnectOutcome`, the stream options of section 8.
  - It shares with the datagram track: the socket table, fd allocation, the
    port allocator, address validation and binding rules, the layer's sim
    branch and the ev op dispatch for `net_open`/`net_bind`/`net_close`, and
    the trace and digest framework.
  - So its first round should start after phase 1 has merged the table and
    the layer, or the two tracks agree on those types first.
  - Its consumer step, starh2 moving `getpeername` and `TCP_NODELAY` to the
    layer, is starh2's work.

## 13. Changes from revision 1, and alternatives rejected

| revision 1 | now | why |
|---|---|---|
| zio exports a POSIX C shim; hotseat renames ENet's calls in the sim build only | one non-suspending layer in every build; the consumer owns a C bridge that uses it in every build | the rename was two integration paths. It also missed hotseat's own `fcntl` check (review B2) and ENet's `listen`/`connect`/`accept`/`shutdown`/`select` (B4) |
| ENet deterministic once its clock is patched | consumers seed every entropy source; ENet's seed has the host pointer | review B1, `host.c:79-81` |
| due time raised to keep order, then a same-time shuffle | order per concrete flow; shuffle across flows only | review B3: the shuffle could reorder within a flow |
| shared slot table, panic when full | per-socket byte and packet limits with storage; overflow is a counted loss | review B5 |
| one "no faults vs zero faults" test | three promises, each tested, draws counted, configuration normalized | review B7 |
| `SO_SNDBUF` recorded | refused; send pressure stated as not modelled | review B8 |
| four weak tests | rewritten (section 11) | review B9 |
| phase 1: a pair with no ports | phase 1: addressed UDP through the layer, with a C bridge test | review B10 and the decision |
| two connect booleans, independent nullable delay bounds | one outcome enum, one optional range, validated rates | review |
| most specific path wins | last set wins | review answer to question 1 |

Rejected:
- **Interposing libc** (`LD_PRELOAD`, `--wrap`): platform-specific, and it
  catches every caller, wanted or not.
- **A sim-only C shim in zio:** two integration paths (above).
- **Real sockets with a fault proxy thread:** real time, not replayable.
- **A TCP stack:** large, and neither user tests TCP timing.
- **Per-fd datagram faults:** tests think in links, and often do not hold the
  peer's fd.
- **Seeded random ports:** a counter already replays.
- **Reorder on by default:** real loopback keeps order.

The answers to the review's four questions are taken as given:
- last set wins;
- no delay before a send completes;
- 127/8 is enough to start, with binding and source selection defined in
  section 4;
- `errno` is now moot. zio exports no C, and the consumer's bridge returns
  ENet's own codes.

## 14. Premises and open questions

Premises:
- hotseat calls ENet from one task, with a service timeout of 0. This was
  read: `enet_host_service` returns at `protocol.c:1935` before its only wait
  at `:1953`.
- **To check:** hotseat's ENet build can drop `unix.c`'s socket half and keep
  the rest (time and address parsing).
- **To check:** starh2's listener sets `SO_REUSEADDR`. If it does not, its
  sim runs must not rebind a used stream port (section 8).
- The production cost of the bridge is within run-to-run spread (section 3,
  to be measured).

Questions for the reviewer:
1. Is the per-datagram overhead of 256 bytes against the byte limit the
   right way to make empty datagrams cost something, or should the packet
   limit alone do that?
2. Should `zio.random` off an executor under sim draw from the run's
   generator instead of refusing? Drawing would change the trace of any run
   that does it today. Refusing would break such runs, and none is known.
3. Is refusing a stream rebind without `SO_REUSEADDR` too strict? It would
   refuse a test that restarts a server on the same port within one run
   (check whether any starh2 sim test does).
