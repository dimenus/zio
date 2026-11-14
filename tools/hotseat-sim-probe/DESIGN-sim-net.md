# Design brief: a sim network for zio (t-4086, t-4087), revision 3

Status: **phase 1 (section 12) is being built; the rest is for review.**

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
| `recvmsg(fd, iov, flags)` | as `recvfrom`, plus `to: ?Addr` (null unless enabled) | `recvmsg` and the control message |
| `poll(fd, events, timeout = 0)` | `{ readable, writable }` now | `poll` with timeout 0 |
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
  `_set_option`, `_get_option`, `_send`, `_receive`, `_wait`, `_destroy`,
  and `enet_socketset_select`) over this layer, in production too. It
  returns ENet's own codes:
  - 0 for would-block and for `AddressUnavailable` on send;
  - -2 for truncation;
  - `ENET_SOCKET_WAIT_INTERRUPT` from `pollInterruptible`;
  - -1 for an unsupported option, which ENet ignores (`host.c:66-68`).

  hotseat's `requireNonBlocking` moves to `isNonBlocking`.
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
  - A socket that sends before it is bound is bound to 127.0.0.1 and the next
    port first.
  - A wildcard bind receives datagrams to any 127/8 address on its port.
  - With no `from`, the source is 127.0.0.1 for a wildcard socket, or its
    bound address.
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

## 5. The test's API (phase 2 adds faults; phase 1 has none)

```zig
pub const Path = struct { from_ip: ?[4]u8 = null, from_port: ?u16 = null, to_ip: ?[4]u8 = null, to_port: ?u16 = null };
pub const DatagramFaults = struct {
    loss_ppm: u32 = 0,                 // 0..1_000_000, validated
    dup_ppm: u32 = 0,                  // 0..1_000_000, validated
    delay: ?struct { min_ns: u64, max_ns: u64 } = null,   // min <= max, validated
    reorder: bool = false,
    blackhole: bool = false,
};
pub fn setDatagramFaults(path: Path, faults: DatagramFaults) void;
pub fn clearDatagramFaults(path: Path) void;
pub const ConnectOutcome = enum { accept, refuse, no_answer };     // stream track
pub fn setConnectOutcome(path: Path, outcome: ConnectOutcome) void;
pub fn datagramStats(fd: i32) DatagramStats;
pub fn wireDigest() u64;      // phase 1
```

- **Last set wins.** `set` replaces the rule with an equal `Path`, or adds
  one. `clear` removes it. A datagram uses the most recently set rule that
  matches it.
- **An all-off rule means removal**, and only removal. Setting one removes any
  rule with that exact `Path`; it does **not** exempt the path from a wider
  rule. A specific all-off setting under a wildcard blackhole leaves the
  blackhole in force for that path.
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
   counts the fault code's own draws. A run with no rule and one with an
   all-off rule both make 0 such draws, and give equal traces and digests,
   because an all-off rule is removed.
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

## 8. Options: three classes

| class | under sim | examples |
|---|---|---|
| **modelled** | the option has its documented effect | `SO_RCVBUF` (byte limit), non-blocking, `enableDestinationAddress`, `TCP_NODELAY = 1` and `SO_REUSEADDR = 1` (stream track) |
| **unsupported, advisory** | returns `error.OptionNotSupported` (zio's existing name); nothing changes; the caller may ignore it | `SO_SNDBUF`, `SO_RCVLOWAT`, `SO_REUSEADDR` on a datagram socket, `SO_RCVTIMEO`, `SO_SNDTIMEO`, `IP_TOS`, `IP_TTL`, `IPV6_TCLASS`, and ENet's QoS option (`SO_NET_SERVICE_TYPE` on macOS, `SO_PRIORITY` on Linux), `TCP_NODELAY = 0` (stream track) |
| **misuse** | panics with the option's name | an option on a closed or unknown fd; `SO_REUSEPORT`; broadcast and multicast options; a TCP option on a datagram socket; any other option or level; a non-positive `SO_RCVBUF` |

**Reading an option** (`getsockopt`): modelled are `SO_RCVBUF` (the byte
limit), `SO_ERROR` (always 0) and the destination-address option. Reading any
other option panics: `getsockopt`'s error set is only `Unexpected` in a build
without sim, so there is no "unsupported" to return. ENet's core reads no
option, and hotseat's bridge will not; `IP_TTL` is not modelled.

- An unsupported option returns an error, so ENet's ignored -1 works
  (`host.c:66-68`).
- hotseat's Darwin tests that need a 1-byte `SO_SNDBUF` to get `EMSGSIZE`
  stay real-socket tests. Under sim they would see `OptionNotSupported`, and
  their sends would succeed. Send pressure is not modelled (section 10).
- **TIME_WAIT is not modelled.** Stream rebinding without `SO_REUSEADDR` is
  refused (stream track). starh2's listeners set `reuse_address = true`
  (`src/edge/server.zig:1984, 2033, 2063, 2151`).

## 9. What is refused, by name

- Non-loopback addresses.
- IPv6.
- `recvmmsg`/`sendmmsg`.
- `MSG_PEEK`.
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
     `SNDBUF` (ignored -1), `enableDestinationAddress`, bind to port 0,
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
- **Phase 2:** faults, counters, validation, and promises 2 and 3.
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
- **Options and limits (ec6ec37):** three option classes
  (`dgramSetOption`'s doc comment); `SO_RCVBUF` re-provisions the queue; the
  sim's allocator can fail in a test (`testSetNetAllocator`).
- **Trace and `wireDigest` (31783f6):** a `dgram` trace event per landing;
  `wireDigest()`; the datagram table in `stateDigest` only for a run that
  opened a socket. Two pipe-only probes' hashes are pinned, so promise 1 is a
  test now, not only starh2's gate.

### Next (phase 1, in this order)

1. **The native-exit refusals (section 3),** with the script that lists every
   fd-taking function in `src/os/` and a verdict per caller. Known gap: the
   layer's `close` on a sim **pipe** fd still reaches the kernel (EBADF is
   `unreachable` there). A proposal, not yet decided: put the refusal in each
   fd-taking function of `src/os/` itself (behind `comptime sim.compiled_in`,
   so a build without sim is unchanged), which makes every caller safe at
   once, and replace the one-off script with a test that reads `src/os/*.zig`
   through `@embedFile` and fails when a `pub fn` that takes an fd has neither
   the refusal nor a sim branch. Then a new exit cannot be added without it.
2. **The C-bridge tests in both builds, and the seven acceptance groups.**

### Decided by the design's owner (2026-11-14, after reports 6-8)

- **A bad source address:** the sim stays strict and host-independent; group
  7 treats that case apart (sections 3.1 and 11).
- **Out of memory re-provisioning a queue panics; reading an option that is
  not modelled panics;** `IP_TTL` is not modelled (sections 6 and 8).
- **The native-exit refusals go inside each fd-taking function of
  `src/os/`,** one helper call first in the body, nothing else changed in
  those files, with a structural test that fails toward noise (step 1 below).

### How to run things

- **zig:** `$(mise where zig@0.17.0)/zig`. Never `zig` by name;
  `~/.zvm/bin/zig` is 0.16.
- **The sim tests:**
  `zig build test -Dsim=true -Dscheduling=pinned -Dtest-filter="sim: "`. With
  `TEST_VERBOSE=true`, strip the colour codes first
  (`sed 's/\x1b\[[0-9;]*[mK]//g'`); then `grep -c "^sim.test.sim:"` counts
  them, and one of them prints its own text before its "OK".
- **The normal suite:** `PATH="$(mise where zig@0.17.0):$PATH" ./check.sh --ci`.
  It runs host tests only.
- **Linux and Windows:** `check.sh` never compiles a Linux or Windows branch.
  Build `zig build test -Dtarget=aarch64-linux -Demit-test-bin=true --prefix
  <dir>` (with and without `-Dsim=true -Dscheduling=pinned`) and run it in
  colima as the memory note `linux-io-uring-tests-on-mac` says
  (`TEST_FILTER=<one substring>`). `-Dtarget=x86_64-windows` compiles the
  Windows test binary.
- **Unchanged without sim:** `textcmp.sh <base commit> <src dir> <label>`
  builds the eight examples ReleaseFast from the base and from the src dir at
  one path, and compares `__TEXT,__text`. When they differ,
  `fndiff.py <a> <b>` lists the functions that differ, with only moved code
  and data normalized away. Control: the mutex's `locked` from 1 to 3.
- **Mutants:** `mut/run.py <list.json> <out>`. A mutant counts only if it
  compiled and its named test is listed failed, or the test process crashed in
  it. Check the reason line of each kill, not just the verdict.
- **starh2's three sim steps:** `starh2-setup.sh <zio commit> <label>` makes a
  `git archive` of starh2 429b2b3 whose two zio pins point at a `git archive`
  of that zio commit (never the live tree). `starh2-gate.sh <label> 10`, in a
  Rex tab, runs the three steps once and `dst-zio` 9 more times;
  `dstzio-cmp.py <ref> <new>` compares the value sets per line. The reference
  is 0b71a9c (`dstzio-ref.txt`, 10 runs, every line one value).

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
