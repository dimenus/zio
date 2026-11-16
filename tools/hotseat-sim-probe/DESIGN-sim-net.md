# Design brief: a sim network for zio (t-4086, t-4087), revision 3

Status: **phase 1 (section 12) is built, and four rounds of outside review
of it are closed (section 15). Section 15 is the acceptance record of phase
1. Phase 2 (faults and replay) is built and accepted at 4aeb580 (section
15); its API and rules are the comment above `pub const Endpoint` in
`src/sim.zig`.** **The stream track (TCP) is designed in section 17;
slices 1 to 3 are built (section 15), slice 4 (the users) is next.**

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
- **Stream track (t-4086), for starh2 and hotseat:** TCP on 127.0.0.0/8,
  so `dst-conn` can run through the accept loop and admission (starh2
  `tools/dst-conn/main.zig:982`), and hotseat's HTTPS and RTSP can run
  against its in-process test host. Its design is section 17.

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
| `connect` on a datagram fd | fixes the peer: the protocol above `dgramConnect` in `src/sim.zig` (t-4167) | `connect` |
| `listen`, `accept` on a datagram fd; `send`, `recv` on an unconnected one | refused (`error.OperationNotSupported`) | as the kernel answers |

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

Built (sitting 20). The API, the order in which a datagram meets the rules,
the draw, the counts, the records and the misuse list are the comment above
`pub const Endpoint` in `src/sim.zig`; the reasoning is in the messages of
3d35086 (the design check) and 24fcbeb to 2561a08 (the four slices).

- **Stream track:** `setConnectOutcome` is dropped; section 17.6 says
  why.
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
- On a datagram socket: a second `connect`, a disconnect, a send with an
  address on a connected socket, a layer `poll` of a socket with a pending
  error, `shutdown` of a connected socket (the list above `dgramConnect` in
  `src/sim.zig`).
- The misuse class of section 8.
- A sim fd at a native exit (section 3).
- A real fd passed to a sim op during a run.
- A positive poll timeout.
- A blocking receive on a socket not set non-blocking.
- `zio.randomSecure` and off-executor `zio.random` during a run.
- A datagram over 65507 bytes.
- A send or receive of more than IOV_MAX buffers (1024 on macOS and Linux,
  measured; both kernels answer EMSGSIZE). Up to that, any count works:
  the sim reads the caller's iovecs in place (`sim.max_iovecs`).

## 10. Fidelity limits: what is not modelled, so not tested under sim

- **Send pressure** and local send failures (`EAGAIN`, `ENOBUFS`,
  `EMSGSIZE`). `AddressUnavailable` occurs only for a bad `from`.
- **Connected UDP** is modelled (t-4167); its limits (ICMP rate limits,
  Linux's error set before the send returns, Linux's error-first order) are
  listed above `dgramConnect` in `src/sim.zig`.
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
  while the sender's socket stays the same (t-4100).

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
     of a parked op woken by a delayed landing (the `pushDueAt` check stays);
  3. the op-level model test learns the faults;
  4. promise 3: a faulted workload in ten fresh processes, and one changed
     rule gives another digest.
- **Phase 3 (hotseat's):** ENet over its bridge, under sim and with the
  production gate above.
- **Stream track:** section 17.11, slices 0 to 5.

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
- **A spin at one virtual instant panics (t-4133, 69eac70):** more than
  `sim.default_instant_poll_limit` passes of `Loop.poll` while the clock
  stands still. Design: section 16; the rule: the comment above
  `default_instant_poll_limit` in `src/sim.zig`; tests: the filter `sim:
  spin`. 69eac70's message has two wrong lines, corrected in 02ae53f's.
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
- **Connected datagram sockets (t-4167, 7ba69f0):** `connect`, the peer
  filter, sends and receives with no address, and ICMP port unreachable as
  a pending `ConnectionRefused`. Protocol: the comment above
  `dgramConnect` in `src/sim.zig`; the kernels' answers: the parity table
  in `src/sim_dgram_connect_test.zig`.
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
   `sim: pipe: a receive with room on a full pipe wakes the send parked on
   it` (a receive with no room is refused since t-4096: `sim: pipe
   refuses: a receive of 0 bytes ...`). One waiter at a time (round 3):
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
- **t-4096, closed in sitting 31:** a zero-length receive on an empty sim
  pipe parked. It is refused now, on any pipe end (`recvInto`'s doc
  comment), as on a stream socket (17.4).

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
  deferred callback. (A zero-room pipe receive is refused since t-4096.) (Pipe close,
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
   storage only (test 1 covers parking and harvest). A zero-length ev
   receive is refused since t-4096; the pipe model test calls `drainBuf`
   directly, so its empty slices still run.

### Next: phase 2 (faults and replay)

- **Phase 2** (section 12): faults, counters, validation, promises 2 and 3.
  The design check was 3d35086 (sitting 19); the owner's answers came
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
- **t-4101 (sitting 21): any buffer count up to IOV_MAX.** The sim took at
  most 16 buffers per send or receive (`sim_max_iovecs` in the socket
  layer and in the ev loop's pipe path); ENet sends up to 65. Now the
  limit is 1024, `sim.max_iovecs`, and the sim reads the iovecs in place,
  so the limit costs no stack. Tests: `sim: dgram iovecs: ...`, `sim: pipe
  iovecs: ...`, six `... of IOV_MAX + 1 buffers` refusals, `sim: enet
  bridge: a datagram gathered from ENet's most buffers, 65, ...` (both
  builds), and the op model's seeds 9 to 16, which gather one send in 16
  from many buffers (215 sends of exactly 1024). Mutants:
  `mut/21-iov.json` (17, all killed).
- **Round 5 of the outside review (gpt-simnet7.md, of 3340b0f): no P1,
  two P2, both closed (sitting 22).**
  - Finding 1, d77f351: a faulted send that failed for memory moved fault
    state (counts, matches, draws, burst). Every allocation now comes
    before the decision. Test: `sim: net faults: a send whose payload
    copy, duplicate copy or wire growth fails ...` (it failed before).
  - Finding 2, 975da92: the op model now compares the sim's wire landing
    times with its own after every step and poll, and a poll may not pass
    the model's earliest landing. The review's mutant (1 ms late) was
    already killed by the old model; the mutant "a poll's clock skips
    landings" survived it and is killed now.
  - Its generator list: added in sitting 22 a wildcard-bound socket, a
    host's silence, and rule changes inside a harvest (seeds 9 to 16; mutant
    "a wildcard socket receives nothing" killed). **Left, not built:** an
    implicit bind at a first send in the op model (the bridge test covers
    it); `requireEveryRuleMatched` at the end of each seed with the model
    predicting a panic (it needs a non-panicking count or a child per
    seed); faults that change at a timer; sends predicted to fail; source
    address overrides (`from`); more than 6 rules; `flowClash` across
    different destination ports.
- **Phase 2 accepted by the grader at 4aeb580** (report 22's review). The
  grader's own recompute: 275 of 275 sim tests in 3 of 3 runs;
  `check.sh --ci` exit 0 with 853 tests; four of the grader's mutants
  caught; the machine code of the 8 example binaries the same as 33b95ca,
  with the control differing.
- **Round 6 of the outside review (gpt-simnet8.md, of 4aeb580): no
  simulator defect; one P2 gap in the op model, closed in sitting 23.**
  `pollOnce` accepted a poll that moved the clock past its own cap and
  stopped exactly at the model's earliest landing (send at 0, landing at
  100 µs, `pollOnce(20 µs)` advancing to 100 µs). It now saves the clock
  before the poll and fails a clock that moves back or moves more than the
  poll's cap, beside the earliest-landing check. The mutant of the review's
  case, in the sim branch of `Loop.poll` (wait to the next landing when it
  comes before every timer, past the caller's cap), survived all 16 seeds
  of the old model and is killed at seed 1 step 34. A wider one (the sim
  branch sets the wait from the timers alone) let a poll capped at 1 ms
  jump 60 s with the old model green. Seeds 1 to 8 keep their counts.
- **Phase 2 is built and reviewed.** This section is its acceptance record:
  the slices above, the gates in each commit, and rounds 5 and 6. Not done in it, by decision: NAT rebinding
  (t-4100). Still open from phase 1: t-4098 (the op model's
  holes; flows sharing a landing instant are still kept out of it).
- Open and filed: t-4098 (the op model's holes above). t-4096 is closed
  (sitting 31: refused).
- **The stream track (section 17).**
  - Revisions: 1 was 6e854fc; 2 was 2d987f7, with slice 0; 3 was 4e6459f.
    The grader accepted 17.17's three points and report 25's three open
    questions as recommended. The third outside read (`gpt-simnet11.md`)
    is answered slice by slice, with no fourth full round (the grader's
    ruling).
  - **Slice 1 is built (sitting 27).** a95fe60 fixed review 3's findings
    1, 2, 8 and the accept part of 3 in section 17; d613fd1 built the
    slice; 35972fb made the Windows test binary compile again. Protocol:
    the comment above `pub const StreamProfile` in `src/sim.zig`. Tests:
    `src/sim_stream_test.zig`, under the filter `sim: stream`; the
    surface table is `sim: stream surface table`, the kernel rows `sim:
    stream parity`. Mutants: `mut/27-s1.json` beside report 27, all
    killed; d613fd1's message has their table and what was rejected.
    Report 27 has the gates' commands and output.
  - **The outside review of slice 1's code** (`gpt-simnet12.md`, of
    efa63a1): no P1; three P2 and one P3, all answered in 2ea6222. Empty
    buffer lists on a stream fd refuse in the layer's eight calls and in
    std.Io's read; each op row of the surface table has its own check;
    the rule rows have typed identities (`Rule`, `ruleSlice`), so a row
    cannot vanish; the TCP landing time and `next_conn_id` are checked,
    named panics. Mutants: `mut/28-f.json` beside report 28, all killed.
    Report 28 has the gates.
  - **Review 3's findings that blocked slice 2 are fixed in section 17**
    (c8b5570, sitting 29): the send and receive lists split into call and
    harvest lists (3); the reset before the own FIN in the send list (4,
    measured: row 39); the local reset set once, never re-armed (5, row
    40); close always counts unread bytes (6); the window release under
    `.macos` (7, row 41); the RST landing profile-neutral (9); the
    blocked-call rows backed by the kernel's account of each call (10,
    `tcpprobe4.c`); `failStream` wakes both ops and the harvest order
    picks the one that gets the error (11). Rows 39 to 41 and the probe
    are in `zio-sim29/probe4/` beside report 29.
  - **Slice 2 is built, with its op model (sittings 29 and 30), and waits
    for the grader.** Bytes, chunks (hashed by default), the window and
    paced credit, FIN, RST with data, shutdown `.send` and `.both`, close
    with unread bytes or SO_LINGER {1, 0}, the profiles' send and receive
    rules, the local reset, the window release, `getpeername`,
    `bytesReadable`, and the window and chunking settings, through the ev
    ops, the socket layer, zio.net and std.Io. Protocol: the comment above
    `pub const StreamProfile` in `src/sim.zig`. Tests:
    `src/sim_stream_bytes_test.zig` (`sim: stream bytes`, its refusals,
    and `sim: stream parity: slice 2's rows`), and the op model
    `src/sim_stream_op_model_test.zig` (`sim: stream op model`); the
    surface table is at `built_slice = 2`, with only slices 3 and 4
    pending. Commits: 6232b5f and 4d98218 (sitting 29); 59ebd24, 879fc38,
    7c72c39 and the op model's commit (sitting 30). Mutants:
    `mut/29-s2.json` beside report 29, `mut/30-*.json` beside report 30.
  - **Sitting 30, in short.**
    - The grader's two survivors: the unread buffer's compaction has a
      test; `landCredit`'s `@max` was equivalent (credits land in order,
      with growing counts) and is now an invariant panic (59ebd24).
    - The outside review of slice 2 (`gpt-simnet13.md`): its P2 false
      refusal is fixed (`.both` reserves only what leaves now,
      `creditPlan`), and its three test gaps have tests (879fc38).
    - **t-4119 is ruled (7c72c39):** the sim shows what a zio program sees
      on the kernel; the parity table, through zio, is the oracle; the C
      probes explain the kernel. A send parked when a reset arrives takes
      it (17.8, line 3a), because zio's readiness backends end a parked op
      with `SO_ERROR`. Rows 23, 25 and 41 changed under `.macos`; rows 41g,
      42, 43 and 44 are new. Linux row 41 was measured (`r41x.c`, 17.13):
      it depends on a gap smaller than one segment in the window; the sim
      has no such gap, and row 41g is a fidelity limit (17.14).
    - **The stream op model** (17.15 item 2, without faults): below.
  - **Slice 2 was accepted by the grader at 982fe5b** (its recompute agreed
    with report 30). hotseat moves its pin there, so every pushed commit
    may be pinned: keep each one green.
  - **Sitting 31, in short** (report 31 has the gates and the mutant
    lists, `zio-sim31/` beside it).
    - The grader's two survivors (f1d1b89). `chunks_landed += 2`: the op
      model now keeps its own `StreamStats` and compares every field of
      `sim.streamStats()` after every step, by reflection, so a new field
      is compared from its first run. `creditPlan`'s `.stale` changes only
      `.both`'s wire reservation (`departCredit` decides stale again by
      itself), so its one observable effect is a false limit panic; a
      bytes test makes that state (a wire one short of the limit).
    - The outside review of the op model (`gpt-simnet14.md`; 44580d4):
      stale credits counted at departure and at landing apart; wakes by
      op and by why, a shutdown by its kind; line 3a counted by the saved
      send serial; each new floor shown to fail by a mutant of its count;
      the parity rows' spawns join on every path; the parent passes replay
      overrides on; `linesWith`'s leak. t-4130: a new step `owed_close`
      and `getpeername`/`bytesReadable` checks kill S2-20 to S2-22; the
      entry limit and the woken-send window change stay with their own
      tests (closed).
    - t-4096 (75bb57e): a receive of 0 bytes on a sim pipe is refused, as
      on a stream socket. One pipe test changed: its 0-room half is a
      death test now (the commit says why).
  - **Slice 3 is built (sitting 32).** 07825f6 decided what 17.9 left
    open (17.9a); the slice is the commit after it. `.tcp` fault rules
    (delay, jitter, holding by partition and silence, release),
    `failStream`, and the connect with no answer. Protocol: the faults
    paragraph of the comment above `StreamProfile`, and `failStream`'s doc
    comment, in `src/sim.zig`. Tests: `src/sim_stream_fault_test.zig`
    (filter `sim: stream faults`; refusals `sim: stream faults refuse`;
    sensitivity `sim: stream faults: sensitivity`; parity `sim: stream
    parity: slice 3's rows`, in both builds); the op model's seeds 17 to
    32; the surface table at `built_slice = 3`, with only slice 4's row
    pending. Mutants and gates: report 32 and the slice's commit.
  - **Sitting 32, in short.** t-4133 (69eac70), t-4132 (02ae53f), the
    design fixes (07825f6), slice 3. Upstream (`origin/main` 5357759) had
    not moved.
  - **Sitting 33: the follow-up of the outside review of slice 3**
    (`gpt-simnet15.md`; report 33 has the commands, outputs and the
    mutant lists, `zio-sim33/` beside it). Each defect got a test that
    failed first; the failing output is in each commit.
    - 29d2559: two held credits of a direction coalesce to the one with
      the larger sequence, whichever was held first (17.9a point 8 was
      wrong). The op model had copied the old rule; its reference now
      keeps the larger count, compares every held and on-wire entry by
      identity (`checkHeld`), checks that no live direction is credited
      short (`checkCreditLive`) and that no send is stranded at the end.
    - 196ceef: found while looking for the same shape: an entry held at
      its landing now holds the later entries of its direction still on
      the wire at once (17.9 item 2). Before, a release could put them
      ahead of it, and an abort delivered them after a hole (the op
      model's seed 17 did, unseen). The model has a stream-offset check
      that shares nothing with the hold logic.
    - 5e81fb1: the entry limit is checked on the count after the step
      (`reserveTcp(n, freed)`), at the abort, the coalescing credit and
      `shutdown(.both)`'s release credit.
    - 6865949: the review's P3s: a real deadline-then-release case; the
      three floors count their own case; the two snapshot pairs; tests
      for held credits at abort and failure, hold-release-hold, a release
      at the time limit, a failure canceled or closed before harvest, and
      a held SYN-ACK and ACK. `Timeout` in `C-syn` is right (measured on
      Linux: a kernel's connect timeout reaches zio as `Timeout`).
    - 367db81: t-4139 answered by a test: std.Io sees
      `ConnectionResetByPeer` for both `failStream` kinds.
    - 2de1d25: `sim.datagramSocketsOpen()` and the opt-in
      `sim.refuseOpenDatagramsAtEnd()`. Not the default: 75 zio tests
      leave a datagram socket open at `end` (t-4142); starh2's steps
      leave none.
  - **Sitting 34: the outside review of cb01515**
    (`gpt-simnet16-for-seat.md`; report 34). 8730e8e: the pull left the
    direction's landing fence at a pulled entry's future time, so a
    release or an abort's RST right after a pull landed late (ran: 655000
    ns instead of 500000). The fence now resets to now after the pull;
    the rule is beside `Conn.last_due`, and in 17.9 item 5. The op model
    keeps no fence any more (it reads its own wire). **Not done:** the
    model alone does not kill the stale fence; its generator never makes
    the case (t-4148, with the steering numbers measured, and the model's
    other mirrored variables).
  - **Sitting 35 (two seats; report 35, `zio-sim36/` beside it).**
    - add1e96 (t-4148, t-4154): seeds 33 to 40 with their own weights and
      two scripted steps (`train`, `late_credit`) reach the pull cases;
      four counts with floors of 10 per profile. The op model alone now
      kills the stale fence (K-01) and t-4154's mutant, each with both
      landing times in its reason; a scenario test kills t-4154's too.
      Seeds 1 to 32 keep c557d8f's digest and no-profile lines.
    - ee61929 (t-4148): the mirrored variables (`last_credit_at`,
      `owed_at`, `next_seq`, `data_send_index`) and the second look
      (landing order, chunk draw, chunk spacing): a sim-only mutant of
      each is killed by the model; the comments beside them say which
      check, and that a rule both read wrong from the design passes.
    - t-4149: the skip is upstream's `AutoCancel: re-armed while the
      previous arm is still live on another executor`
      (`src/autocancel.zig`): it skips when no worker migrated, 1 to 3
      runs in 100 on this Mac. Not changed; filed t-4160.
    - d461504 (t-4142): `sim.end` refuses open datagram sockets by
      default; `refuseOpenDatagramsAtEnd` is gone, with no opt-out. 46
      tests and two children's tests close their sockets; every run's
      seed, events, clock, trace, wire digest and RNG are the same as at
      add1e96 (an end-of-run log compared run by run). Pipe ends are not
      the same shape: a pipe close draws from the RNG (t-4143 stays).
  - **Sitting 37 (report 37, `j37/` and `g37/` in its scratch folder).**
    - 7ba69f0 (t-4167): connected datagram sockets. Each row was run
      first on real sockets through zio (macOS 25.6; Linux 6.8 with
      io_uring and epoll; 3 runs each); the parity table holds them. The
      kernels disagree on three rows: sendTo with an address on a
      connected socket (the sim refuses), a poll with a pending error (the
      sim refuses), and a queued datagram before the error (the sim takes
      macOS's order). Promise 1: every existing run's end-of-run record is
      unchanged (0 of 1096 main and 0 of 296 child runs differ). 31
      mutants, all killed.
    - Gates on 7ba69f0: macOS sim suite 473 of 473 (2 skipped) and
      `check.sh --ci` 862 of 862 (476 skipped), 3 runs each; Linux sim
      463 of 463 (1 skipped), without sim 878 of 880 (the two DNS
      lookups), parity in io_uring and epoll, 3 runs each; the op model
      digests and the stream model's 40 seeds as at 328d798; textcmp
      equal to 0685d39, control differs; starh2's three steps with trace
      digest c05b7576d516675ee650676f4af38437.
    - Upstream: `origin/main` moved 3 commits (#923 cancel protection,
      #924 check.sh shebang, #925 `max_executors`); none touches
      sockets or the sim. Not merged.
  - **Next:**
    1. Slice 4 (17.16): the users. starh2's `dst-conn` accept step;
       hotseat's tests (17.15 item 7), after t-4110 and t-4111; then an
       outside review. Through std.Io, hotseat's "host dies" test sees
       `ConnectionResetByPeer` (t-4139's test).
  - **Open, filed:** t-4126 (macOS row 41g's answer varied in 1 run of 3),
    t-4127 (what stays open after the ruling: whether zio's readiness
    backends' `SO_ERROR` read is a defect, the parked cells no row
    measures, the gap as a fidelity limit), t-4139 (whether std.Io's
    mapping of `ConnectionTimedOut` should change: upstream's file, the
    owner's decision), t-4143 (pipe ends at `end`: a pipe close draws
    from the RNG, so closing them moves digests; it needs a decision
    first), t-4160 (upstream's AutoCancel test that skips when no worker
    migrated; whether to report it). Queued for the sim seat, not
    started: t-4106, t-4115, t-4117. Closed: t-4132, t-4133 (sitting
    32), t-4136 (sitting 33), t-4138, t-4142, t-4148, t-4149, t-4154
    (sitting 35); t-4167 (sitting 37). Filed in sitting 37: t-4176
    (hotseat: its ping-send test seam calls libc on the socket handle,
    which under sim is a sim fd), t-4177 (no zio name for EISCONN), t-4178
    (the sim's answers for send and recv on an unconnected datagram
    socket), t-4179 (a datagram OS profile, if a consumer needs Linux's
    order).
  - **What I would change, with what I know now:**
    - The parity table prints every row's answer now; a row that passes
      silently was how a measured difference could hide.
    - The surface table's checks run whole scenarios, so the table run
      takes about 9 s; a row that names its scenario test instead of
      running it again would be cheaper. Not done: running it is what
      proves the row.
- **For hotseat, not zio:** ENet's receive returns 0 for an empty datagram,
  the same as would-block, so ENet's receive loop stops there for that
  service call. Later datagrams wait for the next call.

### Upstream: merged in, never rebased (ZIOSYNC1 and 2)

- **The branch contains upstream zio 5357759 (v0.20.1) by merge commit
  f0396b1** (upstream into the sim line at 5315249; 23 conflict hunks in 5
  files, each named in its message), and the later sim work by merge
  commit 2559c02 (6e854fc into f0396b1, no conflicts). The next upstream
  catch-up is the same: `git merge origin/main`, never a rebase.
- **Gates on 2559c02:** sim tests on macOS 275 passed, 2 skipped, 250
  named `sim:` tests (3 verbose runs; 6e854fc alone gives the same);
  `check.sh --ci` 858 passed, 282 skipped (3 verbose runs); Linux in
  colima, sim build 265 passed, 1 skipped, and without sim 874 of 876, the
  two failures being the DNS lookups that fail the same way on plain
  origin/main in that container (3 runs each); the Windows test binary
  compiles; the bridge tests 12 OK without sim and 11 OK, 1 skip with it,
  on macOS and Linux; starh2's three steps exit 0, dst-zio 54 of 54 keys
  equal to the reference over 10 runs, trace digest
  c05b7576d516675ee650676f4af38437 (6e854fc alone gives the same); the
  structural test counts 129 fd-taking functions (71 refuse first, 21 sim
  branch, 2 delegate, 35 covered by callers), as before the merge.
- **Machine code without sim:** against origin/main, the eight examples
  differ in the functions the fork already differed in against its base
  c5716c4, less `LocalRunQueue` push and refill (upstream took our fix as
  #914), plus `AnyTask.releaseCoro`, `Executor.deinit` and
  `Awaitable.destroy`, whose only change is a field offset 8 bytes on (the
  fork's added `AnyTask.label` and `Executor.activity` fields, reached by
  upstream #909's stack cache). The examples' `__text` at 2559c02 equals
  that at f0396b1.
- **What an upstream merge has to watch:** upstream #918 removed the
  `allocator` parameter from 19 `src/os` path functions, each of which
  carries the refusal line; keep upstream's signature and put the line
  first. A new fd-taking function upstream fails the structural test until
  it gets the line.

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

- **The stream tests:** the filter `sim: stream` in a sim build. `sim:
  stream parity` also runs in a build without sim, on real sockets, and
  checks the host kernel's column; on Linux run it in the default
  binary (io_uring, with seccomp unconfined) and in a `-Dbackend=epoll`
  one. Before `sim.end`, a stream test closes every stream socket and
  sleeps while `sim.streamEntriesInFlight()` is not 0 (`drain` in
  `src/sim_stream_test.zig`); `end` panics on what is left.
- **Slice 2's tests:** the filter `sim: stream bytes` (its refusals are
  under `sim: stream refuses`). The parity rows of slice 2 are `sim: stream
  parity: slice 2's rows`, in both builds; on Linux run them in an
  io_uring and an epoll binary, as slice 1's. Every row prints what it
  got (`parity row N, <kernel> column, sim|kernel: ...`); a row with no
  column for this kernel, or not modelled by the sim (41g), prints that
  it was not run.
- **Slice 3's tests:** the filter `sim: stream faults`. A test that sets
  a `.tcp` partition or silence clears it before it drains: a held entry
  is released only by a clear.
- **A run that spins at one virtual instant** panics past
  `sim.default_instant_poll_limit` (t-4133). A test whose correct run
  needs more raises it with `sim.setInstantPollLimit`, and says why.
- **The stream op model:** the filter `sim: stream op model` (about
  three minutes). The parent runs seeds 1 to 40 in a child per
  profile, twice (the digest lines must be equal), then one child per
  seed with no profile, which must panic where the profiled runs' shadow
  predicted. Seeds 1 to 16 have no fault step: their digest lines are
  the check that a stream run with no fault did not change; seeds 1 to
  32 keep their digest and no-profile lines when a step is added (a new
  step gets weight 0 in the older blocks). Its header
  comment says what it generates and checks. A replay: set
  `ZIO_STREAM_MODEL_SEED=S`, `ZIO_STREAM_MODEL_STEPS=N` or
  `ZIO_STREAM_MODEL_PROFILE=linux|macos|none` for `zig build test`; the
  parent passes them to one child per profile and prints its output (no
  floors, no second process, no no-profile runs). To see a child's
  counts: the test binary with `ZIO_SIM_DEATH_CHILD=1`,
  `ZIO_STREAM_MODEL_PROFILE=...` and `TEST_FILTER` set to the child's
  name.
- **Every datagram socket is closed before `sim.end`** (t-4142): `end`
  lists each open one and panics. In a test, `defer
  dg.os_net.close(fd);` after the open, never inside a loop, a labelled
  block or a helper that returns the fd.
- **To check promise 1 for a change that should move no digest:** the
  end-of-run log in `zio-sim36/j4/` beside report 35 (`instrument.py
  <tree> <log> [nopanic]` on a scratch copy, `endlog.sh`, `attrib.py`,
  `compare.py`) logs every run's seed, events, clock, trace, wire, RNG
  and state digest at `end`, in children too, and compares two trees run
  by run.
- **The surface table needs the filter `sim: stream`:** its refusal rows
  run other tests in child processes, and a build filtered to the table
  alone does not contain them. To run the table by itself, build with
  `-Dtest-filter="sim: stream" -Demit-test-bin=true --prefix <d>` and
  run `<d>/bin/test` with `TEST_FILTER="sim: stream surface table"`.
  The same works for a mutant that the runner reports NOT RUN because
  an earlier test crashed the process.
- **The connected datagram tests:** the filter `sim: dgram connect` in a
  sim build. `sim: dgram connect parity` also runs without sim, on real
  sockets, and checks the host kernel's column; on Linux run it in an
  io_uring and an epoll binary (the gate scripts use the filter
  `parity`, which runs the stream parity too).
- **A new rule of section 17** gets a tag in `Rule` and its slice in
  `ruleSlice` in `src/sim_stream_test.zig`; the build fails until it
  has exactly one row.

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
- **The fault tests:** the filter `sim: net faults` (the refusals are
  `sim: net faults refuse`). The op model's seeds 9 to 16 carry its fault
  steps; seeds 1 to 8 are the phase 1 mix.
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
- **A random test whose sockets and rules all use one address cannot see
  an address bug.** The op model's fault rules only used 127.0.0.1 until
  the mutant "an endpoint ignores its address" survived; rules on
  127.0.0.2, which holds no socket, now catch it (sitting 20).
- **A "small" change to a drawn range is not small.** One more nanosecond
  of jitter changes the modulus of every jitter draw, so the whole run
  changes. To change only times, change a fixed delay (sitting 20).
- **A mutant's old text must be unique in the file**, and the test's own
  model copies of the fault code hold the same lines: pick text with the
  sim's own names in it (`faultDraw`, `fault_stats`).
- **A filtered test build does not analyse the tests outside its filter.**
  `-Dtest-filter` is compiled in, so a test that does not match is never
  type-checked. After a merge, a fork test that calls a function whose
  signature upstream changed compiles green under the `sim: ` filter; only
  `check.sh --ci` (no filter) finds it. In f0396b1 that was five tests in
  `src/net.zig` calling `dirDeleteFile` with the removed allocator.
- **`if (skip)` is not `if (comptime skip)`.** With a plain const, Zig
  still analyses the test body after the skip, so a POSIX body broke the
  Windows test binary (d613fd1, fixed in 35972fb). Write `if (comptime
  ...) return error.SkipZigTest;` as the sim's other tests do, and keep a
  comptime block that takes a test's function pointer off Windows.
- **zsh does not split an unquoted variable.** `o="-Dsim=true
  -Dscheduling=pinned"; zig build $o` passes one argument, and the build
  stops at its option parser. Write the flags out.
- **Ctrl-C to a Rex pane that runs a bash loop of `docker run` does not
  stop the loop.** docker passes the signal to the container and exits
  normally, so bash starts the next run. Stop the script by its exact
  PID (`ps -axo pid,command`); a container that cannot be stopped ends
  with its run, and goes in the report's cleanup list.
- **A Rex session holds at most 64 windows.** Close each tab when its
  output is saved.
- **The mutant runner edits files in place.** Point a copy of it (its
  `ROOT`) at a scratch copy of the tree (rsync without `.zig-cache` and
  `.git`); the worktree is then free for other builds while it runs.
- **A C probe is not zio.** The probes call blocking sockets in threads;
  zio parks a non-blocking op on a backend, and its kqueue backend ends a
  parked op with `SO_ERROR`, which also clears it. So rows where an op is
  parked can differ between the probe and zio on the same kernel
  (t-4119). Check a profile row through zio's public API on real sockets
  before calling it parity.
- **A read after the endpoint's own `shutdown(.both)` gives 0 at once;**
  it does not park. A test that wants a chunk to land first sleeps.
- **A connection retires as soon as both ends are dead and nothing is on
  the wire,** and `testStreamDir` then gives null. Check discards after a
  close with `streamStats().discarded`, the run's totals by cause.
- **Darwin's `std.c.T` has no `FIONREAD`;** the layer uses the BSD value
  `_IOR('f', 127, int)`, 0x4004667f.
- **The structural test accepts `sim.isSimFd(fd)`, or `sim.isDgramFd(fd)`
  with the refusal,** as a sim branch, not `sim.isStreamFd(fd)` alone. A
  new layer function with a stream branch also needs a datagram branch.
- **A floor must count its case where the case is decided,** not an
  outcome that a neighbour case also gives. Three floors of the stream op
  model did (sitting 31: stale credits, shutdown wakes, line 3a). To show
  a floor is live, zero its counting line and watch the child fail with
  its name.
- **A count written before it is counted is a guess.** 7c72c39's message
  gives `src/sim.zig` +57/-6 and the test file +178/-42; `git diff
  --numstat` gives +55/-8 and +169/-51. Run the command, then write the
  number.
- **A parity row that waits a fixed time can sit on a kernel race.** Row
  41's 200 ms wait was 5 ms under Linux's RTO and about 50 ms before the
  persist timer filled the window's last gap, so it measured a different
  state from the C probe (sitting 30). Before trusting a wait, vary it.
- **Tool rules for agents here:** a recursive `rm` of a path built from a
  variable is refused by a hook; make a new directory instead. Restoring a
  file with `git checkout <file>` is refused too, and would drop uncommitted
  work: let the mutant runner restore it. In zsh, a word that starts with
  `=` (`echo ====`) is a command lookup and fails; quote it.
- **A heredoc whose marker is not quoted runs its backticks.** `cat <<EOF`
  around a commit message with `` `code` `` in it runs each one as a
  command; a fix script failed that way and a commit went in with its
  draft message (69eac70; corrected in 02ae53f's message, as commits are
  never amended). Write `<<'EOF'`.
- **Python scripts run through `uv run --no-project`** (a hook refuses
  `python3 script.py`; inline `python3 - <<'EOF'` is allowed). The mutant
  runner is `uv run --no-project python -I run.py <list> <out>`.
- **A poll stops at the next landing, so a count of polls is not a
  time.** A release can put hundreds of entries on the wire 1 µs apart;
  the op model's connect waited 100 polls and failed with nothing wrong
  in the sim (sitting 32). Bound a wait by virtual time.
- **A reference model that copies the sim's rule is not an oracle.**
  The stream op model's credit coalescing was the sim's own rule, so the
  two agreed on a wrong answer (sitting 33). Derive the reference from
  the design text, and add a check that shares nothing with the logic
  under test (the stream offset, the credit liveness).
- **A cancel ends a parked op and a woken op the same way.** A test of
  "woken, then canceled before its harvest" must check the op was woken
  (`sim.testStreamParked`); without it the mutant "the failure wakes
  nothing" survived (sitting 33).
- **65536 one-byte TCP entries cost about 1.9 s per test** (each
  `orderedRemove` moves the rest of the list). The limit tests pay it.
- **A held list is in sequence order, not arrival order.** An entry held
  at its landing is older than one sent later and held at its send. The
  op model's first copy appended, and the new landing-time check found
  it at seed 19.
- **A sim child process gets a fresh environment** (`PATH`,
  `TEST_FILTER`, the death-child flag, the heap shift): an environment
  variable set for `zig build test` never reaches it. A probe that must
  log from children writes its path into the code (sitting 35).
- **A death child's first line of stderr starts with the runner's
  progress clear** (`\r`, `ESC [0K`). `hasPanicLine` skips it since
  d461504; before, a wanted line that was the child's first output never
  matched.
- **A closed port costs a socket slot only while it is open.** A test
  that makes a closed port (bind, read the port, close) frees slot 0, so
  the next socket is fd 100032 again: a death test's expected fd moves
  (sitting 37).
- **Linux reports an ICMP error on loopback before the send returns;**
  macOS later. A real-socket row that sends twice back to back to a
  closed port gets two different answers (row 16 of the connected
  table). Measure both before you write what "the kernel" does.
- **A random test's floor that only says "not 0" sits thin.** The pull
  cases had floors of 1 and stayed at 1 per profile; the pull seeds'
  counts have floors of 10. Steering old seeds moved their digests and
  pushed another floor to 0 each time: add a seed block instead.

## 16. A run that spins while virtual time stands still (t-4133)

**The defect.** Under sim, work takes no virtual time. A task that arms a
timer for a moment already past is woken at once, at the same instant. If
it then arms the same past moment again, the run loops at one `clock_ns`
for ever, at 100% CPU, with no message (hotseat, a mutant of its Control).
On a real clock the loop ends, because real time passes. Under sim it is a
hang, and only a wall-time alarm outside the run sees it.

**What is counted: passes of `Loop.poll` at one virtual instant.** Every
scheduler turn of every executor goes through `Loop.poll` (a full wait, a
`.zero` poll after a tick budget, a harvest in `simPollAll`), and a bare
`ev.Loop` test polls it directly. The count is reset when `clock_ns` has
moved since the last pass. Two other counters were measured and rejected:

- **Trace events at one instant** miss a main task that only yields: it
  makes no task switch and no wake, so the events stop while the polls go
  on. And a burst of datagram landings at one instant gives hundreds of
  events in a correct run (the datagram limit tests), so the bound would
  have to sit near a number that real work reaches.
- **Timer fires at one instant** miss every spin that is not a timer
  (a yield loop, two tasks that wake each other at one instant).

**The bound: `default_instant_poll_limit`, a constant in `src/sim.zig`.**
It must be far above any correct run and low enough that a spin panics in
about a second of wall time. Its commit gives both measurements and how to
repeat them: the largest count over the sim suite (every child process
included), both op models, and starh2's three steps; and the wall time of
the death tests. The bound is not a count of anything a program asks for,
so it is not derived from one; it is a margin over a measurement.

**A test can change it:** `sim.setInstantPollLimit(n)` between `begin`
and `end`; `begin` restores the default. A program that does a great deal
of zero-time work at one instant raises it. A test that wants the check to
fire early lowers it.

**What the panic prints:** the count, the virtual time, the seed (as every
sim panic does, with `TRACE_HASH` and the event count), the trace id of the
task that last ran or was woken, and the last timer that fired (its
deadline and the virtual time it fired at), or that no timer fired at this
instant. The check reads the clock and counts; it draws nothing and emits
nothing, so no digest, trace or count of a correct run changes (promise 1).

## 17. The stream track: simulated TCP on loopback (design check, revision 3)

Status: **slices 1 and 2 of 17.16 are built**, slice 2 with its op model
(slice 1: d613fd1, 35972fb; slice 2: 6232b5f and sitting 30; section 15
has the state). t-4119 is ruled (17.13). Slices 3 and 4 are design only. Revision 1 was
6e854fc, revision 2 was 2d987f7, and revision 3 is sitting 25. It answers
the second outside review, `gpt-simnet10.md`, and the grader's rulings on
report 24. The third outside review, `gpt-simnet11.md`, is answered in
place, slice by slice, with no fourth full round (the grader's ruling):
its findings 1, 2, 8 and the accept part of 3 are fixed here before slice
1 (17.5, 17.6, 17.8's RST list, 17.15 item 3). Its findings 3 (send and
receive), 4 to 7, 9, 10 and 11 are fixed before slice 2 (sitting 29), in
17.7, 17.8, 17.9 and 17.13; finding 4 and 7's rules come from rows 39 to 41,
measured in sitting 29. Each claim says how it is known: **(read)** from the code at
the file and line named, **(ran)** from the probes kept beside the
sitting's report, or **(not measured)**.

**What changed from revision 2, in short:**

- **Profiles, not mixed traces.** Where the kernels differ, the sim
  follows one declared profile, `.linux` or `.macos`, for the whole
  sequence. A differing row reached with no profile panics (17.8).
- **An endpoint's death is apart from the connection's retirement.** An
  unaccepted server endpoint has its own lifetime (17.5).
- **The op rules are decision lists, total by construction.** They cover
  one unresolved send and one unresolved receive, ops that are woken but
  not yet harvested, and every close, landing, credit, window change,
  rule change and `failStream` (17.6, 17.8).
- **Byte conservation is an invariant.** Reset is two facts, received and
  reported, each set once. Credits are frozen and paced, so they never
  travel faster than the wire. `failStream` is defined in both
  directions (17.7 to 17.9).
- **Every kind of entry has a draw key.** The data-send counter is per
  direction. The chunk bounds are corrected, and the limits are checked
  before admission (17.7, 17.10).
- **The digest lists every setting that changes the future.** The
  teardown check is separate from TIME_WAIT. Each slice's gate is scoped,
  with pending rows (17.11, 17.15, 17.16).
- **The probe is repaired** with synchronized state. Rows 16 to 35 are
  unchanged, and rows 26b and 36 to 38 are new (17.13).

### 17.1 Why, and for whom

- **hotseat.** Every App start is HTTPS, then RTSP over TCP, to its host
  (read: hotseat `src/session.zig:473-480, 524-527`). Each request uses a
  new connection; hotseat is the client; its tests run the host in the same
  process (`src/test_host.zig:328, 339`). `src/app/app.zig` imports neither
  the control socket nor the seat API (the grader's check), so the App can
  start under sim; how is hotseat's plan (its `spec/briefs/sim-enet.md`,
  part D).
- **starh2.** A server. `dst-conn` today puts one h2c connection on a
  `sim.pipePair` (read: starh2 `tools/dst-conn/main.zig:618, 982`), so the
  accept loop and admission never run under sim.

### 17.2 What exists today (read, at 6e854fc)

- **Sim pipes** (`sim.pipePair`, `src/sim.zig:667`): two ends with no
  address, 4096 bytes per direction, end of stream, injected reset and
  short writes. Each pipe op completes after `ioDelayNs()`, 1 or 5 ms
  **drawn from the run's RNG** (`:679`). Unchanged by this design.
- **A stream socket in a run** panics at `socket()`: "only IPv4 datagram
  sockets are simulated" (`src/os/net.zig:347-351`). `listen`, `accept`,
  `send`, `recv` and `shutdown` answer a datagram fd; `connect` on one
  panics; on any other sim fd they refuse as native exits. There is no
  `getpeername` in the layer.
- **The paths both users take** (read; the review traced each one, and I
  accept its table): `IpAddress.listen` is `NetOpen`, `setsockopt` for
  reuse, `NetBind` (which calls the layer's `bind` and then `getsockname`),
  `setNoDelay`, `NetListen`. `IpAddress.connect` is `NetOpen`, an optional
  bind, `NetConnect` with a timeout, `setNoDelay`, and `NetClose` on
  failure. `Server.accept` is `NetAccept`, inline-preferred without a
  deadline, retrying `ConnectionAborted`. Reads go `readBuf` → `NetRecv`;
  writes go `writeBuf` or `writeSplatHeader` → `NetSend`. std.Io's
  listen, connect, accept, read and write (also batched) use the same ev
  ops (`src/io.zig:2234, 2424, 2647, 2916, 979-998`). File sending is a
  separate path: `netWriteFileImpl` and the writer's `sendFile` →
  `NetSendFile` (`src/io.zig:2954`, `src/net.zig:1469`).
- **Native backends call the layer.** The poll backend calls the layer's
  `connect` (`src/ev/backends/poll.zig:457`); epoll and kqueue call it
  through `sockreg.submitConnect` (`src/ev/sockreg.zig:404`), and read
  `SO_ERROR` through `getSockError`. Under sim the backends are bypassed
  (`src/ev/loop.zig:1137-1139`), so these calls are not reached.

### 17.3 What the two users call (read; file and line in their repos)

hotseat pins zio 33b95ca; starh2 pins 3417a77. Both use ianic/tls.zig.
Unchanged from revision 1, which listed them with file and line; the
summary:

- **hotseat**, all through `zio.net`: `addr.connect(.{})` by IP
  (`src/gamestream.zig:181`, `src/rtsp.zig:271`); `writeAll`; `read` until
  0; `stream.reader`/`writer` under `tls.client` (`gamestream.zig:261-273`;
  `.rng` from `io.random`, `.now` from the real clock); `close`. Tests:
  `listen(.{})` on 127.0.0.1:0, `accept` with a deadline, the bound port
  from `server.socket.address`, a connect to 127.0.0.1:1 expecting
  `ConnectionRefused` (`session.zig:2761`). No `setsockopt`,
  `getpeername`, `shutdown` or `poll` on a TCP fd.
- **starh2**, through std.Io on zio's vtable and raw zio ops: listen with
  `reuse_address = true`, backlog 4096 (`src/edge/server.zig:1984`);
  accept (`:1317`); raw `setsockopt(TCP_NODELAY)` per accepted socket
  (`:2020`); raw `getpeername` (`src/edge/connection.zig:630`); raw
  `ioctl(FIONREAD)` (`src/edge/h1.zig:661`); `readBuf`,
  `writeSplatHeader`, `NetRecv`/`NetSend` on a CompletionQueue (TLS pump);
  `shutdown(.send)` (`connection.zig:6168`, `h1.zig:683`);
  `shutdown(.both)` to wake its own read pump (`connection.zig:5158-5160`);
  `close`; an outbound connect (`src/http1/oneshot.zig:316`). Tests:
  `SO_LINGER` 0, `getsockopt(TCP_NODELAY)`, raw libc clients, `::1`,
  AF_UNIX (real sockets).

### 17.4 The API surface, decided per call

**Modelled** has its documented effect. **Refused** panics with the call's
name (section 9), unless the row gives an error.

| call | under sim | who needs it |
|---|---|---|
| `NetOpen(.ipv4, .stream, .ip / .tcp)` | modelled: a sim fd | both |
| `NetBind` (127/8 or 0.0.0.0; port 0 or fixed), which also writes the bound address | modelled; TCP ports apart from UDP (17.5) | both |
| `NetListen(backlog)` | modelled; capacity in 17.6 | both |
| `NetAccept` | modelled; writes the peer address | both |
| `NetConnect` on an unbound socket, with or without a timeout | modelled (17.6) | both |
| `NetConnect` on a socket the program bound itself | refused at submit, before a SYN (17.6): neither user does it | nobody |
| `IpAddress.connect` with `local_address` | refused **in the public adapter**, before any side effect (in `src/net.zig`; on Linux it would otherwise first set `IP_BIND_ADDRESS_NO_PORT`) | nobody |
| `NetSend` with default flags (`no_signal = true`), up to IOV_MAX buffers; `writeSplatHeader` | modelled | both |
| `NetRecv` with default flags | modelled; a total buffer length of 0 is refused (t-4096's ruling) | both |
| any other send or receive flag | refused by name, derived from the flags type, as for pipes (`refuseSimPipeFlags`) | nobody |
| `NetShutdown(.send)` | modelled: half-close | starh2 |
| `NetShutdown(.both)` | modelled (17.8) | starh2 |
| `NetShutdown(.receive)` | refused: kernels differ (rows 6, 17) and nobody calls it | nobody |
| `NetClose` | modelled: FIN, or RST with unread bytes | both |
| `SO_LINGER` {1, 0}, then close | modelled: an RST at once | starh2's tests |
| `NetSendFile` (std.Io `netWriteFile`, the writer's `sendFile`) | refused by name | nobody |
| `NetPoll` on a stream fd | refused. No supported stream API path calls it; zio's own ev tests do (`src/ev/test/poll_server.zig:569`), on real sockets | nobody under sim |
| the layer's `connect`, `send`, `recv`, `poll` on a stream fd | refused: not reached by the sim dispatch (the native backends call them, 17.2) | nobody under sim |
| `getsockname` | modelled | both, through `NetBind` |
| `getpeername` | a **new** layer function, modelled; it maps `ENOTCONN` and `EINVAL` (macOS after a reset, row 9) to `SocketUnconnected` in every build (ruling 6) | starh2 |
| `FIONREAD` | a **new** layer function (`bytesReadable`): landed, unread bytes | starh2 |
| `TCP_NODELAY = 1` | modelled: stored, read back as 1, inherited by accepted sockets; no effect | both, through zio |
| `TCP_NODELAY = 0` | unsupported, advisory (section 8) | nobody |
| `SO_REUSEADDR = 1` | modelled (17.5) | starh2 |
| `SO_REUSEPORT = 1` on a stream socket | unsupported, advisory (`OptionNotSupported`), not misuse; it stays misuse on a datagram socket | zio's std.Io listen (`src/io.zig:2248`, `catch {}`) |
| `getsockopt(TCP_NODELAY)` | modelled | starh2's tests |
| `getsockopt(SO_ERROR)` | **refused** (17.13 rows 33, 34 measured it) | no sim path: only the native backends read it |
| any other option on a stream socket | misuse, panics | nobody |
| AF_UNIX, IPv6, DNS | refused (section 2) | outside sim runs |
| a consumer's raw libc call on a sim fd | not reachable by zio: the kernel answers `EBADF` (section 3.3) | t-4111 moves starh2's |

**Why `SO_ERROR` is refused, not modelled.** No sim path needs it: the sim
completes `NetConnect`, `NetSend` and `NetRecv` with their errors, and
only the native backends read `SO_ERROR`. Both kernels clear it on read
and a later read then gives 0 (rows 33, 34), so a model returning 0 would
hide a failure. If a consumer ever needs it, those rows say what it must
do.

### 17.5 Identities, lifetimes and ports

- **The socket table is shared** with datagram sockets: one fd range, one
  slot allocator, one generation per slot, recorded by each ev op at
  submit (`dgramOpOnItsSocket`). A slot's kind is datagram, stream open
  (not connected), listener, or stream endpoint. The datagram serializer
  in `stateDigest` keeps hashing slots 0 to 63 exactly as today
  (`digestNet`, `src/sim.zig:574`), so a run with no stream socket keeps
  its digests (the promise 1 pins prove it).
- **A connection** is an object in a fixed table (`max_conns` = 256). Its
  `conn_id` is assigned when `NetConnect` is submitted, in program order,
  from 1, and is never reused in a run. A connection has two
  **endpoints** and two **directions**.
- **An endpoint** has a phase: `C-open`, `C-syn`, `C-failed` (client);
  `absent`, `S-prov`, `S-queued` (server, unaccepted); `est` (owned by a
  descriptor); `dead`. An unaccepted server endpoint has **no
  descriptor**. It is owned by its listener's queue, and lives until it
  is accepted (it becomes `est` with a new slot), or until it dies.
- **The server endpoint is `absent` until a SYN makes it `S-prov`.** Only
  a landing SYN creates server state (17.6), and a SYN that lands either
  makes `S-prov`, or is answered with an RST (no listener), or is dropped
  (its client is dead). So once no SYN of a connection is on the wire or
  held, an `absent` server endpoint can never become present. **For
  retirement, `absent` counts as dead.** Two cases reach it: a connect to
  a port with no listener (the RST lands, and the wrapper closes the
  client), and a client closed in `C-syn` whose SYN is dropped at landing
  or discarded while held. Each retires once its client is dead and its
  last entry has landed (review 3, finding 1).
- **An endpoint dies** when its descriptor is closed, when its listener
  closes before it is accepted, or when an RST lands on it in `S-prov`.
  `failStream` does not kill an endpoint: it marks it failed, and the
  descriptor still owns it until close. A dead endpoint keeps the few
  flags it needs to answer late entries (17.8): whether it sent an RST,
  and whether it failed.
- **A connection retires** only when all of these hold:
  - both endpoints are dead (`absent` counts as dead, above);
  - no entry of it is on the wire or held;
  - no credit departure is owed (17.7).

  Until then, every entry lands at its endpoint, alive or dead, and is
  handled by 17.8's landing rules. After retirement, nothing of it
  remains. The `conn_id` check at landing is a guard that cannot fire by
  construction, and the op model requires its count to be 0. Revision 2
  retired an aborted connection at once; that lost the queue's reference
  and the listener's RST (review 2, findings 1 and 2).
- **Every TCP entry carries** `conn_id`, its direction, and a
  per-direction sequence number. A SYN is addressed: it also carries the
  destination, and it looks up the listener bound there when it lands.
- **Ports.** TCP has its own table and its own counter from 49152. Port 0
  takes the next free port, wraps from 65535 to 49152, and skips ports in
  use or reserved; a full range is `AddressInUse`. A connect from an
  unbound socket binds 127.0.0.1 and the next port, also for a
  destination of 127.0.0.2 (row 30, Linux).
- **TIME_WAIT is a reservation for the rest of the run.** An endpoint that
  sends its FIN before a FIN has landed on it is the active closer. Its
  local port is reserved with that socket's `SO_REUSEADDR` flag. A later
  bind to the port follows the profile (row 31):
  - `.linux`: allowed only when both the reserved socket and the new one
    set the flag;
  - `.macos`: allowed when the new socket sets it.
  - Without a profile, a bind whose answer would differ (the reserved
    socket without the flag, the new one with it) panics and names row
    31.

  A client-side active close leaves the listener's port free (row 32,
  both kernels). The teardown check counts reservations apart from leaks
  (17.15).

### 17.6 The handshake and listeners (requirement B)

**Timeline with no fault** (L = 100 µs):
- t: `NetConnect` is submitted; a SYN goes out.
- t+L: the SYN lands. The listener makes an `S-prov` endpoint and sends a
  SYN-ACK.
- t+2L: the SYN-ACK lands. The client is `est`, `NetConnect` completes,
  and an ACK goes out. The client may send at once, and its data follows
  the ACK.
- t+3L: the ACK lands. The endpoint becomes `S-queued`, and a parked
  accept wakes.

The client may close between t+2L and t+3L, and the connection is still
accepted (rows 26 and 27): queue ownership does not depend on the client.

**Capacity.** A listener holds at most `backlog` unaccepted endpoints
(`S-prov` and `S-queued` together), with `backlog` clamped to 1 to 4096. A
SYN that lands at a full listener panics: "accept queue full: backlog
overflow is not simulated". It is a fidelity limit, because neither
kernel refuses such a connect, and they disagree on how many fit (row 4).

**Decision lists.** Each event below has an ordered list. The first line
that matches decides; the last line covers every remaining state. A list
is therefore total by construction: it is the compressed form of a full
(state, event) table, in which the cells that do not depend on the state
share a line. "At the call" means due now, harvested before the clock
moves. "At landing" means in the harvest of the poll that reaches the
landing time.

**`NetConnect(fd)`:**
1. The fd is not a stream socket in `C-open`, or it was bound by the
   program: refused, as is `local_address` in the public adapter, before
   any side effect.
2. Otherwise: assign the `conn_id`; the endpoint becomes `C-syn`; a SYN
   goes out; the op parks.

**A SYN lands at address A:**
1. The client endpoint is dead (closed in `C-syn` before the SYN
   landed): dropped (`stream_dropped_dead`). No answer, and the server
   endpoint stays `absent`.
2. A listener is open at A and has room: a new `S-prov` endpoint; a
   SYN-ACK goes out. "At A" means bound to A exactly, else bound to
   0.0.0.0 on A's port.
3. A listener is open at A and full: panic (capacity).
4. Otherwise (no listener, or it closed): an RST goes out to the client.
   No server endpoint is made: it stays `absent` (17.5).

**A SYN-ACK lands at the client endpoint:**
1. The endpoint failed (`failStream` in `C-syn`): dropped, no answer. The
   server's `S-prov` endpoint then stays until its listener closes, as a
   kernel's SYN_RECV stays until its timer (a fidelity limit).
2. The endpoint is dead (closed in `C-syn`): an RST goes back. When it
   lands, the `S-prov` endpoint dies (it is never queued).
3. `C-syn`: becomes `est`; the connect completes ok, at landing; an ACK
   goes out.

**An ACK lands at the server endpoint:**
1. The endpoint is dead (its listener closed): dropped. Its RST is
   already on the way.
2. `S-prov`: becomes `S-queued` and is appended to the queue; a parked
   accept is woken with the queue head.

**An RST lands at a client in `C-syn`:** `C-failed`; the connect fails
`ConnectionRefused`, at landing. (Later RSTs: 17.8.)

**Cancel, or the deadline, of a parked `NetConnect`:** `Canceled` (zio
maps a deadline to `Timeout`), at the call. The endpoint stays `C-syn`:
canceling a wait does not abort a connect. A later SYN-ACK makes it
`est` with no op waiting. Both zio wrappers close the socket on failure.

**Close of a client socket:**
1. `C-open` or `C-failed`: the slot is freed; nothing goes on the wire.
2. `C-syn` with its SYN not yet landed (on the wire or held): the
   endpoint is dead. The SYN is dropped at landing, or discarded if
   held (`stream_dropped_dead`), and it never makes server state: the
   server endpoint stays `absent`, and the connection retires when the
   SYN is gone (17.5).
3. `C-syn` with its SYN landed: the endpoint is dead. The SYN-ACK
   answers with an RST when it lands (above).
4. `est`: 17.8.

**Submission and harvest are two lists** (review 3, finding 3). An op
that parks is evaluated twice: at the call, and again at its harvest
after a wake. The "unresolved" guard belongs to the call only: at
harvest the op being harvested is itself the unresolved one, so applying
the guard there would refuse every woken op. The harvest list starts
with the generation instead. The send and receive lists of 17.8 get the
same split before slice 2.

**`NetAccept(fd)`, at the call:**
1. The fd is not a stream listener (an open stream socket, an endpoint,
   or a closed number): refused, by name, before anything else. A
   datagram socket keeps its existing answer, `OperationNotSupported`.
2. An unresolved accept exists on this listener (parked, or woken and
   not harvested): refused.
3. The queue is not empty: the head is claimed at once. It becomes `est`
   with a new slot, and the call completes.
4. Otherwise: it parks. An ACK landing wakes it (17.6 above). A wake
   **claims nothing**: the connection stays at the head of the queue.

**`NetAccept`, at the harvest of a woken accept:**
1. The listener's generation no longer matches (it was closed, or its
   number reused): `FileDescriptorNotASocket`. Nothing is claimed.
2. Otherwise: the queue head is claimed now, becomes `est` with a new
   slot, and the accept completes with it. An empty queue here is an
   invariant panic: only an accept takes from the queue, and a second
   accept is refused while this one is unresolved.

**Cancel or deadline of an accept**, parked or woken and not harvested:
`Canceled` (zio maps a deadline to `Timeout`), at the call. Nothing was
claimed, so the connection the wake was for stays queued, and the next
accept takes it.

**`NetConnect`, at the harvest of a woken connect:** the generation
first (`FileDescriptorNotASocket`), then the result recorded at its
wake: ok at the SYN-ACK, `ConnectionRefused` at the RST. A later landing
in the same instant does not change that result; it is the next call's
to see.

**Close of a listener:**
1. Every `S-prov` and `S-queued` endpoint dies. Their unread bytes are
   discarded (cause `listener_closed`). Each sends an RST to its client,
   unless it already has an RST from that client. Row 28: the client
   reads `ECONNRESET` and then 0, and its send fails `EPIPE`.
2. Accepted connections are not affected (row 29).
3. A parked or woken accept ends `FileDescriptorNotASocket` at the next
   harvest (the generation).

**A queued endpoint keeps what lands on it** (bytes, FIN, RST) until it is
accepted or dies. So accept returns a connection whose client already
closed or reset, as rows 26, 26b and 27 measure. The first call on it
follows 17.8:
- after an orderly close, a read returns the queued bytes, then 0, and a
  send succeeds (row 27);
- after a reset, the first read or send follows the profile (rows 26 and
  26b).

**Equal times.** Entries landing at one instant are processed in
(landing time, `conn_id`, direction, sequence) order, with no draw (17.10).
Two SYNs at one instant therefore reach the listener in `conn_id` order,
which is the program order of their `NetConnect` submissions. The op
model tests both creation orders.

### 17.7 Bytes: chunks, the window, credit, conservation

**Admission.** A `NetSend` admits `min(len, room)` bytes, where room is
`window - used`, and returns that count. The admitted bytes go on the wire
as chunks, each its own entry. Before anything is admitted, the send
works out its chunk plan. If the plan would take the TCP entries (on the
wire and held, all connections) past the limit of 65536, the send panics
and names the limit; nothing is admitted.

**Chunk sizes** come from
`hash(seed, conn_id, direction, data_send_index, chunk_index, "chunk")`
(SipHash):
- if the hash's low two bits are 0, the chunk is `1 + (h >> 2) % 16`
  bytes;
- otherwise it is `512 + (h >> 2) % 3585` bytes;
- the last chunk takes the rest.

`data_send_index` is **per direction**. It moves by one each time a send
admits at least one byte, at admission, never at submission or at a
retry. So no other connection, and no UDP traffic, can shift it. Chunk k
lands at `T0 + k × 1 µs`, so each chunk lands in its own poll, and a
reader sees a prefix first.

**Order within a direction.** An entry lands no earlier than the entry
sent before it in the same direction: its landing time is
`max(its own time, the landing time of the direction's previous entry)`.
Equal times land in sequence order (17.6's equal-time order). So a FIN,
an RST or a credit sent in the same instant as a chunked send lands after
its last chunk, never between its chunks. Without this, a FIN sent with a
five-chunk send would land at `T0` and the chunks after it. Slice 1's
entries are all `clock + L` in send order, so this changes none of them.

**Chunk modes**, set with `sim.setStreamChunking(fd, mode)`, and for
connections made later with `setStreamChunkingDefault(mode)`:
- `.hashed`, the default;
- `.{ .fixed = n }`, with n from 1 to 65536 (0 panics);
- `.off`, one chunk per send.

**Bounds, corrected.** The worst case is one entry per admitted byte, so
a full 256 KiB window can need 262144 entries. That is over the limit,
and the admission check panics first. `.fixed = 1` therefore needs a
window small enough that the windows of all live connections stay under
65536 entries. The hashed mode averages about 150 entries for a full
window. The due list is not affected: it holds ops (at most `max_live`),
and chunks are wire entries. A FIN is sequenced after the last chunk of
the last admitted send.

**The window.** It is one number per direction: the bytes the receiver
will accept beyond what it has read. The sender's `used` is `admitted -
credited`. A send with no room parks.

**Credit (rulings 10 and D).** Credit is the only reverse traffic: data is
not ACKed separately.
- A read of n bytes adds n to the receiver's `read` count for that
  direction.
- **A window release** (review 3, finding 7): unread bytes that `.both`
  discards under `.macos` add to the receiver's `reclaimed` count, apart
  from `read`. They free window room exactly as a read does, so they owe a
  credit too. Without it, a sender parked on a full window whose receiver
  discards everything is never woken: no read makes credit, and a FIN
  does not wake a send. Row 41 measured it on macOS: the blocked send
  wakes, and its data draws an RST (17.13).
- A credit entry carries `read + reclaimed` at its departure. The count
  is frozen once the entry has left.
- At most one credit departs per direction per L (100 µs) of virtual
  time. A read inside that interval does not send a credit; it sets
  "owed", and the owed credit departs at `last departure + L` with the
  count at that moment. It never travels faster than the wire.
- An owed departure is processed in the harvest of the poll that reaches
  its time, after the entries that land at that instant, in (`conn_id`,
  direction) order. It is not an entry: it has no sequence number until it
  departs, and nothing is recorded until its credit lands.
- An owed credit survives its receiver's close: it departs at its time,
  and the connection does not retire until it has (17.5). A kernel's FIN
  carries the same window, so a sender parked when its peer read and then
  closed is woken, sends, and meets the RST of a closed endpoint. It is
  discarded only by `failStream` on the receiver (17.9), or when the
  direction it credits is terminal at departure time (its sender is dead,
  or has received a reset); then nothing leaves (`stale_credit`).
  (Sitting 29's first text named the receiver's own direction, which a
  closed receiver always makes terminal: that contradicted the line
  above.)
- When a credit lands, `credited = count`. A direction's credits land in
  the order they left (the order rule above, and 17.9's release keeps
  it), and a credit leaves only after a read or a release since the last
  one, so each count is larger than the one before. A count that is not
  larger is an invariant panic (sitting 30; revision 3 said `max`, which
  would hide a broken order). If room appears, a parked send is woken.
- Held credits of one direction coalesce to the newest by sequence,
  whichever reached the held list first (17.9a point 8). That is
  equivalent: both are cumulative, and they would land in order.
- A partition of the reverse direction holds the credits, so the sender
  stalls.
- Entries: at most one departure per L per direction. With fixed latency,
  that is 1 in flight. Under a delay rule it is `delay / L`, bounded by
  the entry limit.

**`sim.setStreamWindow(fd, bytes)`** sets the window of the direction
toward `fd`, and `setStreamWindowDefault(bytes)` sets it for later
connections. The default is 256 KiB; the bounds are 1 byte to 16 MiB,
and 0 or more panics. The list:
1. `fd` is not an `est` endpoint, or the sender's send is woken and not
   harvested: refused.
2. Otherwise the window is set at once, at the sender, with no wire entry
   (a stated simplification). Shrinking below `used` discards nothing;
   room is 0 until `used` falls below the window. Growing wakes a parked
   send if room appears.

**Conservation (requirement B).** For each direction, at every step:

`admitted = in_flight + held + unread + read + Σ discarded[cause]`

The causes are named:
- `abort`: the sender's held data, at its RST;
- `failed_sender`: held data when `failStream` hits the sender;
- `failed_receiver`: data that lands at a failed endpoint, plus its
  unread bytes;
- `closed_receiver`: data that lands at a closed endpoint;
- `unread_at_close`: unread bytes at any close of the descriptor,
  whatever else the close does (review 3, finding 6: also after a reset,
  when nothing goes on the wire);
- `shut_receiver`: data that lands after `.both`;
- `both_discard`: unread bytes at `.both`, under `.macos`;
- `after_reset`: data that lands after a reset was received;
- `listener_closed`.

Credit is applied exactly once per landing; counts only grow (17.7). Once a direction is
**terminal** (its sender is dead or failed, or its sender has received an
RST), its window accounting ends: no more admission is possible, and a
credit that lands later is dropped and counted (`stale_credit`). It
never wakes a send, and it never touches another connection. The op model
checks the equation after every step, and checks `credited <= read +
reclaimed` (`reclaimed` is the part of `both_discard` that freed room).

### 17.8 Endpoint rules, reset, shutdown, and the profiles (requirements A, C)

**Two facts for reset (requirement C).** An endpoint has `reset_received`
(an RST landed, or a local reset, below) and `reset_reported` (an op
returned the reset's error). Each is set once and never cleared. A second
RST that lands is dropped and counted, and it never re-arms a reported
error. An endpoint sends at most one RST in its life (`rst_sent`). So
reads go reset, then EOF, and never reset again.

**An RST landing is profile-neutral** (review 3, finding 9). It sets
`reset_received`, and records `fin_before_reset`: whether a FIN had
landed on the endpoint first. It chooses no answer. The profile is read
at the call that meets the reset, so a run with no profile that never
calls a send or a read after a FIN-then-RST needs none.

**A local reset** (review 3, finding 5). Under `.linux`, a data chunk that
lands after the endpoint's own `.both` resets it (row 18: the next read
fails). It sets the same facts as a landed RST: `reset_received`, with
`fin_before_reset` false. It is set only if `reset_received` is not
already set, which also means it was never reported. So a second chunk of
the same send (or any later one) is dropped by the "reset received" line
of the data landing list and never re-arms the error. Row 40 measured
it: 70000 bytes in one send give one `ECONNRESET`, then 0, 0.

**The profiles.** `sim.setStreamProfile(.linux | .macos)` is called before
the run's first stream socket opens, and is set once per run; a change
later panics. It is in the state digest. Each profile follows what a zio
program sees on one kernel, on every row where the kernels differ
(t-4119's ruling, sitting 30: the parity test, one test through zio on
real sockets and in the sim, is the oracle; the C probes explain the
kernel). Where no op is parked, zio sees the probe's answers. Where an op
is parked, zio's readiness backends end it on an error or hang-up event by
reading `SO_ERROR`, which reports the reset and clears it, so:

- **A send parked when a reset arrives takes it** (line 3a of the send
  evaluation, below): `ConnectionResetByPeer` with no FIN first, on both
  kernels (rows 23, 25); after a FIN, `BrokenPipe` under `.linux` (its
  `SO_ERROR` is `EPIPE`) and `ConnectionResetByPeer` under `.macos` (row
  41). It sets `reset_reported`, so reads then give 0. "Parked" means
  parked at the reset's landing; a send woken and not yet harvested then
  is evaluated as a call (not measured: the two cannot be told apart on
  loopback).

The rest of each profile, for calls:

- **`.linux`: one pending error, reported once by the first send or read
  that meets it.** The pending error exists while `reset_received` is set
  and `reset_reported` is not. Its kind comes from `fin_before_reset`:
  without a FIN first it is `ConnectionResetByPeer`, reported by a read or
  a send; after a FIN it is `pipe`, which a read does not report (it gives
  0) and a send reports as `BrokenPipe`. Reporting sets `reset_reported`.
  After that, reads give 0 and sends give `BrokenPipe`. Rows 7, 8, 17,
  18, 20, 22, 23, 25, 26, 26b, 36 and 39 to 40.
- **`.macos`: after a reset, a send that is called gives `BrokenPipe`**
  and never reports the reset (a parked send: above). The first read
  after the unread bytes gives `ConnectionResetByPeer` once, whether or
  not a FIN landed first, and sets `reset_reported`; later reads give 0.
  Rows 7, 8, 17, 18, 20, 22, 23, 25, 26, 26b, 36 and 39 to 41. (Revision
  3 said "this includes a send parked at that moment", from the probe's
  blocking send; zio on macOS gives `ECONNRESET` there.)

**Reaching a differing row with no profile panics and names the row.**
Rows where the kernels agree need no profile, so an ordinary test that
never meets one of these points needs none. The points, each at a call
or at a landing, never earlier:
- a send called after a reset with no FIN before it and not yet reported
  (rows 26b, 36, 39). After a FIN, or once a read reported it, both
  kernels give `BrokenPipe`. A send parked at a reset with no FIN first
  needs no profile: zio gives `ECONNRESET` on both (rows 23, 25);
- a send parked when a reset arrives after a FIN (row 41);
- a read that meets a reset after a FIN had landed, with no unread bytes
  (rows 8, 22). Without a FIN first both give `ConnectionResetByPeer`
  once;
- `.both` with unread bytes (row 17), and a data chunk that lands after
  `.both` (row 18, a landing);
- a rebind in the differing combination (row 31);
- two listeners on one port with different addresses (row 12);
- a connect to 127/8 other than 127.0.0.1 (rows 15 and 30).

**Rows that are not profiled, and why:**

| row | why one answer, or a refusal |
|---|---|
| 1 (poll results) | `NetPoll` on a stream fd is refused, and through `NetConnect` both kernels give `ConnectionRefused` |
| 4 (how many fit the backlog) | needs real timing: overflow panics (17.6) |
| 9 (`getpeername` after a reset) | the new layer function maps `ENOTCONN` and `EINVAL` to `SocketUnconnected` in every build, so zio's callers see one answer (ruling 6) |
| 10 (buffer sizes) | the window is declared (17.7) |
| 6, 13 (`SHUT_RD` alone, shutdown of a listener) | refused: nobody calls them |
| 12 under `.macos` | refused: which listener gets the SYN was not measured. `.linux` gives `AddressInUse` |
| 15, 30 under `.macos` | refused: macOS's answer (no answer) depends on lo0's aliases, not on TCP. `.linux` connects from 127.0.0.1 |

**The cost of profiles.** It is 6 decision points in the sim:
- a send that meets a reset;
- a read that meets a reset;
- `.both` and unread bytes (discard and release, or keep);
- `.both` and late data (local reset, or not);
- the rebind rule;
- row 12 and rows 15 and 30 (refusals under `.macos`).

What it gives: starh2 gets Linux's whole trace, hotseat on a Mac gets
macOS's, and no run mixes two kernels.

**Calls and harvests are two lists** (review 3, finding 3; 17.6 says
why). A receive or a send is checked at the call by its call list. If it
parks and is woken, its harvest list runs. Both end in the same shared
evaluation, so the call and the harvest cannot disagree about a state.

**`NetRecv(fd)` at the call:**
1. The fd is not an endpoint in `est`, or the total buffer length is 0
   (t-4096's ruling): refused.
2. A receive is unresolved (parked, or woken and not harvested): refused.
3. The receive evaluation, below. If it parks, the op parks.

**`NetRecv` at the harvest of a woken receive:**
1. The socket's generation no longer matches (closed, or the number
   reused): `FileDescriptorNotASocket`.
2. The receive evaluation. Its park line is an invariant panic here: a
   woken receive always has a result, because only a receive consumes
   bytes, and a second receive refuses while this one is unresolved.

**The receive evaluation**, in order:
1. The endpoint failed and has not reported it: the failure's error
   (`ConnectionResetByPeer` or `ConnectionTimedOut`); now reported.
2. The endpoint failed and has reported it: 0.
3. There are unread bytes: they are returned (up to the buffers), `read`
   grows, and a credit is owed (17.7).
4. A reset was received and is not reported: `.linux`: without a FIN
   first, `ConnectionResetByPeer`, now reported; after a FIN, 0 (the
   pending `pipe` stays for a send). `.macos`: `ConnectionResetByPeer`,
   now reported. No profile: as `.linux` without a FIN first; after a
   FIN, panic (row 22).
5. A FIN landed, the endpoint did `.both`, or a reset was received: 0.
6. Otherwise: park.

**`NetSend(fd)` at the call:**
1. The fd is not an endpoint in `est`: refused.
2. The total length is 0: 0, and nothing else is checked or changed (not
   measured on a kernel; zio's own writers return before they submit
   one).
3. A send is unresolved: refused.
4. The send evaluation, below. If it parks, the op parks.

**`NetSend` at the harvest of a woken send:**
1. The generation no longer matches: `FileDescriptorNotASocket`.
2. The send evaluation. Its park line is an invariant panic here: a
   woken send always has a result, because only a send uses room, a
   second send refuses, and a window change while a send is woken is
   refused (17.7).

**The send evaluation**, in order (review 3, finding 4: the reset comes
before the own FIN):
1. The endpoint failed and has not reported it: the failure's error; now
   reported.
2. The endpoint failed and has reported it: `BrokenPipe`.
3a. A reset was received and not reported, and this is the harvest of the
   send that was parked when it arrived (`reset_parked_send`): now
   reported; `ConnectionResetByPeer` with no FIN first; after a FIN,
   `.linux` `BrokenPipe`, `.macos` `ConnectionResetByPeer`, no profile
   panic (row 41). (t-4119's ruling.)
3. A reset was received: `.linux`: if it is not reported,
   `ConnectionResetByPeer` without a FIN first and `BrokenPipe` after one,
   and it is now reported; if it is reported, `BrokenPipe`. `.macos`:
   `BrokenPipe`, and nothing is reported. No profile: `BrokenPipe` after a
   FIN or once reported; otherwise panic (rows 23, 25, 26b, 36, 39).
4. Its own FIN went out (`shutdown(.send)` or `.both`): `BrokenPipe`.
5. There is room: admit (17.7), and return the count.
6. Otherwise: park.

Rows 39 and 39u fix the order of lines 3 and 4: after its own `SHUT_WR`
and a peer RST, Linux's first send gives `ECONNRESET` and consumes it,
and reads then give 0; macOS's sends give `EPIPE` and the first read
`ECONNRESET` (ran: `tcpprobe4.c`, 3 runs on each kernel). With line 4
first, Linux's read would report a reset its kernel had already given to
the send.

**What wakes a parked op.** It is then evaluated by its harvest list.

| op | woken by | when |
|---|---|---|
| receive | data, FIN or RST landing; a local reset; own `.both`; `failStream` | at landing / at the call |
| send | a credit that makes room; a window grow; an RST landing; a local reset; own `shutdown(.send / .both)`; `failStream` | at landing / at the call |
| either | cancel or deadline: `Canceled` / `Timeout`, and the op is removed | at the call |
| either | own close: `FileDescriptorNotASocket` (the generation) | next harvest |
| receive | own `shutdown(.send)`: **not** woken (row 19) | — |
| send | a FIN landing: **not** woken (half-close, row 5) | — |

Each op ends once, through the existing `isLive` and `endOp`.

**`shutdown(fd, .send)`:**
1. The fd is not an endpoint in `est`: refused.
2. A reset was received, or the endpoint failed: `SocketUnconnected`.
   This does not consume the pending error (rows 37, 38 and 39w, both
   kernels; 39w also shows it comes before line 3).
3. Its FIN already went out: ok, nothing more.
4. Otherwise: a FIN goes out behind the admitted bytes, and needs no
   room; a parked send is woken (`BrokenPipe`, row 24). If no FIN has
   landed on it, its port is reserved (TIME_WAIT).

**`shutdown(fd, .both)`:** as `.send`, and when that part fails, `.both`
stops there with its error (row 37). Then the receive side is shut:
- A parked receive is woken, and gives 0 (row 16, both kernels).
- Unread bytes: `.linux` keeps them for reading (rows 17 and 41: the
  window stays full, so a parked sender stays parked until they are
  read). `.macos` discards them (`both_discard`), adds them to
  `reclaimed`, and owes a credit: the window release of 17.7 (row 41: the
  parked sender wakes). With no profile and unread bytes, it panics (row
  17).
- A byte that lands later: see the data landing rules.

**`close(fd)`** of an `est` endpoint (other phases: 17.6):
1. Unread bytes are discarded and counted (`unread_at_close`), always
   (review 3, finding 6). Remember whether there were any.
2. The endpoint failed, or a reset was received: the descriptor is
   released; nothing goes on the wire.
3. There were unread bytes, or `SO_LINGER` {1, 0} is set: an RST goes out
   and `rst_sent` is set. The endpoint's held outbound data and FIN are
   discarded (`abort`). Its data already on the wire still lands before
   the RST, in order.
4. Otherwise: a FIN goes out, unless one already did. Its port is
   reserved if it is the active closer.

In every case the endpoint is dead. Unresolved ops end
`FileDescriptorNotASocket` at the next harvest. An owed credit still
departs (17.7).

**A data chunk lands at endpoint X:**
1. X failed: dropped (`failed_receiver`); no answer.
2. A reset was received on X (landed or local): dropped (`after_reset`);
   no answer.
3. X is dead (closed): dropped (`closed_receiver`). If X has not sent an
   RST, one goes back.
4. X did `.both`: dropped (`shut_receiver`). If X has not sent an RST,
   one goes back (rows 17, 18 and 40, both kernels). Under `.linux`, X
   gets a local reset (above), and its held outbound data and FIN are
   discarded (`abort`), as at an RST landing. With no profile, it panics
   (row 18).
5. Otherwise (`S-queued` or `est`): the bytes join the unread bytes, and
   a parked receive is woken. (`S-prov` cannot be reached here: the
   client's data follows its ACK in order.)

**A FIN lands at X:**
1. X failed, is dead, or received a reset: dropped.
2. Otherwise: the FIN is recorded, and a parked receive is woken.

**An RST lands at X:**
1. X failed, or a reset was already received: dropped and counted.
2. X is dead: dropped. A dead endpoint is in no listener's count: the
   count is the listener's live `S-prov` and `S-queued` endpoints, so a
   death leaves it by construction.
3. X is a live `S-prov` endpoint (the client closed in `C-syn` after its
   SYN landed, and its SYN-ACK was answered with this RST): X dies at
   once and leaves its listener's count, which frees one place of
   capacity. Nothing answers. It was never queued, so no accept can see
   it (review 3, finding 2).
4. X is `C-syn`: 17.6.
5. Otherwise (`S-queued` or `est`):
   - set `reset_received` and `fin_before_reset` (profile-neutral);
   - unread bytes stay, and are read first (rows 20 and 22);
   - X's held outbound data and FIN are discarded (`abort`);
   - X's sending direction is terminal;
   - a parked receive and a parked send are woken.

**A credit lands at X:**
1. X's sending direction is terminal, or X is dead or failed: dropped
   (`stale_credit`).
2. Otherwise: `credited` is set to the count, which must be larger
   (17.7), and a parked send is woken if room appears.

**Entries are handled at their landing, never by counting sends**
(requirement C). The first send after a peer closed succeeds, because
nothing has landed yet (rows 8 and 22). The sender learns only when the
RST lands.

### 17.9 Faults on streams, and failStream (rulings 7 and 8)

A rule gets a protocol: `.udp` (the default, so every phase 2 run is
unchanged) or `.tcp`.
- **Delay and jitter** are modelled.
- **Partition and silence** hold entries.
- **Loss, burst, duplication and reorder** are refused at `setRule`.

The protocol is part of each rule's record in the fault digest.

**Delay and jitter** are drawn once per entry, at its send, with the key
`(seed, conn_id, direction, sequence, purpose)`, where the purpose is
"delay" or "jitter" (17.10). Within a direction, landing times only go
forward: the clamp of 17.7 is always on, and an entry lands no earlier
than the one before it; equal times land in sequence order. (Revision 3
said "at least 1 ns after", copied from UDP's clamp, where equal times
have no sequence to order them. TCP entries have one, so equal times are
safe, and slice 1's timings stay as they are.)

**Held, defined:**
1. **When.** An entry is held if a matching **partition or silence** is
   in force at its send or at its landing. Delay and jitter rules are
   never blockers.
2. **Order.** Once a direction has a held entry, every later entry of
   that direction is held behind it, matching or not, in sequence order.
   The later entries still on the wire are held at that moment, not at
   their own landing (sitting 33). Holding them only when they landed
   broke two things, both measured: a release before their landing put
   the released (older) entries behind them, so bytes arrived out of
   order; and an abort discarded the held entry while they stayed on the
   wire, so they were delivered after a hole (the op model's seed 17 did
   this at 93b1037: 247 bytes at stream offset 269 after 258 delivered).
   So a direction that holds an entry has nothing on the wire behind it.
3. **Window.** Held payload stays charged to the window. A held credit
   stalls the sender. Held credits coalesce to the newest (17.7).
4. **Release.** When a rule is cleared, each held direction with no
   matching partition or silence left is released. Directions are
   released in (`conn_id`, direction) order. A delay rule that still
   matches does not prevent release. Its values were drawn at the send
   and are kept. (Test: a partition and a delay over one entry; clear the
   partition.)
5. **Release time.** Each entry lands at `release time + L + its extra
   time` (its chunk spacing, delay and jitter, all fixed at its send),
   clamped behind the entry before it. Nothing is redrawn (17.9a, point
   6: revision 3 left out the L). "Behind" means behind an entry of the
   direction that is on the wire, or has landed; an entry the pull of
   item 2 took off the wire is neither, so it does not hold a release or
   an RST back (sitting 34: the sim kept the pulled entries' times as the
   fence, and a release right after a pull landed up to their old times
   late; outside review of cb01515).
6. **The same instant.** A landing at T takes the rule state at the
   moment `landDue` processes it. A rule that a task clears later in the
   same instant does not undo it. A rule set by a task that ran before
   the harvest at T holds it. This is UDP's order.
7. **RST against held data.** An endpoint that aborts (close with unread
   bytes, linger 0, or an RST landing on it) discards its own held
   outbound data, FIN and credits at once (17.9a, point 9). Their bytes
   count as `abort`. Its data already on the wire lands before its RST.
   An RST obeys a partition like any entry.
8. **Dead endpoints.** Held entries that are inbound to a dead endpoint
   stay held. When they are released, 17.8's landing rules apply: data is
   dropped, and an RST goes back at most once. A connection cannot retire
   while it has held entries (17.5), so a release never meets a retired
   connection.
9. **Time.** Held entries are not on the wire, so they never make the
   clock stop. A connect's deadline is reached normally. A SYN held past
   the deadline is discarded when the wrapper closes the socket (17.6).
10. **Bounds.** Held entries count toward the 65536-entry limit, and
    connections toward `max_conns`. Both panic and name the limit. The
    limit is on the count after the step: entries the same step discards
    (an abort's held data, FIN and credits) or replaces (a held credit
    that a new held credit coalesces with) leave the count first. The
    check still runs before anything changes (sitting 33; before, it
    counted only what the step added, so an abort or a coalescing credit
    at the limit panicked: outside review of slice 3, second P2).

**`sim.failStream(fd, .reset | .timed_out)`**: a local transport death at
the virtual time of the call. It puts nothing on the wire.

| when | what happens |
|---|---|
| X in `C-syn` | the parked connect fails, at the call, with `ConnectionResetByPeer` or **`Timeout`** (`ConnectError` has no `ConnectionTimedOut`; read: `src/os/net.zig:648-650`); X becomes `C-failed` and failed; a later SYN-ACK is dropped (17.6) |
| X in `est` | X is failed. A parked receive and a parked send are woken at the call (a woken one is already due). At harvest each runs its evaluation (17.8), whose first line reports the failure: the first op harvested gets `ConnectionResetByPeer` or `ConnectionTimedOut` and sets it reported; the other then gets its line 2, 0 for a receive and `BrokenPipe` for a send. Which is first is the harvest order, a function of the seed. If none is unresolved, the next receive or send gets it once. After it is reported, receives give 0 and sends `BrokenPipe` (review 3, finding 11). |
| X's outbound data | **held** data and FIN are discarded (`failed_sender`); data **already on the wire** still lands at the peer. Owed and held credits from X are discarded. |
| X's inbound data | unread bytes are discarded (`failed_receiver`). Entries that land later are dropped, and nothing answers (17.8). |
| the peer B | B is not touched. Its parked ops stay parked until something happens to B itself: data, a FIN or an RST that lands, a cancel, a deadline, a close, or `failStream` on B. B's sends are admitted while it has room. When its window fills, the send parks, because a failed X sends no credit. |
| later close of X | the descriptor is released, and nothing goes on the wire (17.8 close, step 1) |
| X in any other phase | refused |

Why the harvest order decides: a kernel keeps one pending socket error,
so of a blocked read and a blocked send only one gets the cause, and which
one is a scheduling race. The sim makes that race the harvest order, so
it is deterministic per seed and both orders occur across seeds. Giving
both ops the error would report it twice, which no kernel does.

hotseat's case, "the host dies in the middle of the RTSP exchange", is
`failStream(client_fd, .timed_out)`. A test that also wants the host side
quiet partitions it, or closes the host's socket.

### 17.9a Slice 3's design fixes (sitting 32)

Read before the build: 17.9 to 17.12 left these points open or
ambiguous. Each is decided here; the protocol goes inline beside the code
when it is built.

1. **Where the protocol lives: `Endpoint.protocol`, `.udp` by default.**
   17.9 says "a rule gets a protocol" but not where. A TCP port 7000 and a
   UDP port 7000 are different endpoints, so the field is the endpoint's.
   One field then covers all three rule kinds (a silence takes one
   endpoint, a path two), and a rule's identity includes it with no new
   code. A path whose two endpoints name different protocols is refused.
   `.{}` is still "every datagram". Rejected: a `Path.protocol` field (a
   silence has no path, so it would need a second API); a parameter on
   each call (it changes six signatures that phase 2 tests call).
2. **The UDP code sees only `.udp` rules.** Every UDP decision skips a
   `.tcp` rule, and "a run that ever set a rule takes the fault path"
   becomes "ever set a `.udp` rule". So a stream run with `.tcp` rules
   gives its datagrams exactly what a run with no rule gives (promise 1
   for the UDP part of a mixed run). The fault digest hashes a rule's
   protocol only when it is `.tcp`, so every phase 2 digest is unchanged.
3. **What a `.tcp` rule matches.** An entry goes from its sender's local
   address to the sender's peer (a SYN: to its destination). A path
   matches on both; a silence on either. Last set wins among the `.tcp`
   impairments that match, as for UDP.
4. **A `.tcp` impairment** takes `delay_ns` and `jitter_ns`. `loss_ppm`,
   `burst`, `dup_ppm` and `reorder` are refused at `setImpairment`, by
   name (17.9). A delay is a fixed value and draws nothing. The jitter is
   drawn once per entry, at its send, from a keyed SipHash of `(seed,
   conn_id, direction, sequence, "jitter")` (the same key as the chunk
   draw), uniform in 0 to `jitter_ns`, both ends included. TCP draws are
   counted apart (`streamFaultDraws`).
5. **An entry's extra time** is its chunk spacing (17.7), plus the delay,
   plus the jitter, all fixed at its send. It lands at `send + L + extra`,
   clamped to no earlier than the entry before it in its direction.
6. **Release time (fixes 17.9 item 5).** Item 5 said "release time + the
   delay drawn at its send", with no wire latency. A released entry
   travels the wire after the path heals, like a retransmission, so it
   lands at `release + L + extra`, clamped behind the entry before it.
   This holds for an entry held at its send and for one held at its
   landing alike: one rule, and a release never lands anything at the
   instant of the release.
7. **A held entry is not on the wire.** It is in a held list, in
   (`conn_id`, direction, sequence) order. It counts in
   `streamEntriesInFlight`, blocks retirement and is in the state digest
   (17.11); it does not move the clock (17.9 item 9).
8. **Held credits coalesce (17.9 item 3):** when a credit is about to be
   held in a direction that already holds one, the two become one: the
   one with the larger sequence stays, and the other goes. Counted
   (`credits_coalesced`). The arriving credit is not always the newer one:
   a credit on the wire that is held at its landing left before a credit
   held at its send (sitting 33; the first text said the arriving one
   replaces the held one, and an older credit then replaced a newer one,
   so the sender was credited short for ever: outside review of slice 3,
   first P2). The sequence decides because every other order rule of a
   direction (the wire, the held list, the release) is the sequence. The
   count gives the same answer: a credit carries the receiver's cumulative
   `read + reclaimed` at its departure, and one leaves only after a read
   or a release since the last, so a larger sequence has a larger count;
   a newer credit whose count is not larger is an invariant panic.
9. **What an abort discards (fixes 17.9 item 7).** Item 7 named held
   data and FIN. An aborting endpoint also discards its held credits:
   its peer's direction toward it is over, so they could only land stale.
   Its held handshake entries (an ACK) stay held, and its RST is held
   behind them if its direction still holds anything. The same applies
   to `failStream` (17.9's table already names credits there).
10. **Records.** A held entry that is discarded (abort, `failStream`, a
    close in `C-syn`) is recorded in the trace and the wire digest at
    that moment with the outcome `discarded_held` (6). A hold is not
    recorded by itself: the entry's later landing record carries its
    landing time, which differs.
11. **`failStream` refusals** (completes 17.9's "any other phase"): an fd
    that is not a stream endpoint's descriptor; an endpoint in a phase
    other than `C-syn` or `est`; one that already failed; one that has
    received a reset (the transport is already over: a second death has
    no kernel meaning). `failStream` in `C-syn` with no parked connect
    (the connect was canceled; the endpoint stays `C-syn`, 17.6) makes
    it `C-failed` with nothing to report.
12. **`failStream(.timed_out)` in `est`** gives `ConnectionTimedOut` to
    the first receive or send (both error sets have it: `src/os/net.zig`
    maps `ETIMEDOUT` there); in `C-syn`, `Timeout` (17.9). `Timeout` is
    what zio gives when a kernel gives up a connect: `errnoToConnectError`
    maps `ETIMEDOUT` to it (read: `src/os/net.zig`, the POSIX branch), and
    measured in sitting 33: on Linux 6.8 in colima with
    `net.ipv4.tcp_syn_retries=1`, `zio.net`'s connect to 10.255.255.1:9
    (no answer) ended `error.Timeout` after about 3 s, with io_uring and
    with epoll, 3 runs of 3 each (a scratch probe, not a committed test:
    it needs that sysctl and a route that drops). The outside review of
    slice 3 asked whether `ConnectionTimedOut` was meant there; it is
    not, and code, design and test agree.
13. **The connect with no answer is a held SYN.** A partition or silence
    that matches the SYN holds it; the connect stays parked until its
    deadline (`Timeout`), a cancel (`Canceled`), or the release (then
    17.6 as usual). A close in `C-syn` or `C-failed` discards a held SYN
    at once (`syn_dropped_dead`, recorded `discarded_held`), so the
    connection retires at once (17.9 item 9). `C-failed` is named too: a
    client that `failStream` hit in `C-syn` is closed by zio's wrapper,
    and its held SYN would otherwise block retirement until a release.
14. **Sensitivity, for slice 3 (17.16 names it; it was not defined).**
    - 17.11's snapshot tests: two runs equal except one setting give
      different state digests: chunking `.off` against `.fixed = 1`,
      another default window, another profile, another backlog; and for
      slice 3, an entry held against the same entry on the wire, a
      failed endpoint against one not failed, and a `.tcp` rule against
      the same rule as `.udp`.
    - A faulted run repeats in one process with equal wire digest, trace
      and transcripts; one nanosecond more `.tcp` delay gives another
      wire digest with the transcripts equal; another seed under jitter
      gives another wire digest.
15. **The op model with faults** keeps the handshake out of its rules
    (17.15 item 2: "the handshake is not modelled"). Its partitions and
    delays name a connection's two ports, and its silences a client's
    port, so a new connection's handshake never matches one. The cases
    that need a fault on a handshake (a SYN held past its deadline, then
    released; a cancel at each handshake phase under a hold; the connect
    with no answer) are scenario tests, as slice 1's handshake cases are.

### 17.10 One wire, flows, draw keys and the RNG (ruling 9; requirements D, E)

- **One wire.** TCP entries go on the existing wire list, in landing
  order. `nextDueIoRemaining` and the poll's clock see them unchanged.
  Owed credit departures are sim-internal times that
  `nextDueIoRemaining` also sees.
- **Flows carry the protocol.** `sameFlow` (`src/sim.zig:2033`) gains the
  protocol, and for TCP also the `conn_id` and direction. So TCP never
  clamps UDP, and two connections never clamp each other.
- **Draw keys.** Every TCP entry kind (SYN, SYN-ACK, ACK, data chunk, FIN,
  RST, credit) has its own per-direction sequence number. All of its
  values come from a keyed hash of `(seed, conn_id, direction, sequence,
  purpose)`, with the purposes "delay", "jitter" and, for chunk sizes,
  `(…, data_send_index, chunk_index, "chunk")`. TCP fault draws are
  counted in their own counter, so UDP's `netFaultDraws()` is unchanged.
  UDP's send numbers and formula are untouched.
- **TCP landings never draw.** `landDue` lands UDP entries exactly as
  today, with its seeded choice among UDP flow heads due at one instant
  (`src/sim.zig:2058`). TCP entries due by then land in a second pass, in
  (landing time, `conn_id`, direction, sequence) order.
- **The RNG claim.** In a run with stream sockets:
  - stream code never draws from the run's RNG;
  - TCP completions take part in the harvest shuffle (`takeDue`,
    `src/sim.zig:1205`), which draws from the run's RNG, as every op does
    today;
  - the scheduler's draws are unchanged;
  - pipes keep their `ioDelayNs` draws.
- **A mixed run.** UDP fault outcomes are a function of UDP sends alone,
  and TCP never clamps UDP. But TCP changes scheduling and the harvest
  shuffle, so UDP sends can happen at other times, and UDP's outcomes are
  not those of the same UDP workload without TCP. A run with no stream
  socket is unchanged in every respect.

### 17.11 Determinism: trace and digests (requirement F)

- **Trace:** a `stream` event at each TCP landing: kind, `conn_id`,
  direction, sequence, length.
- **Wire digest:** TCP records include kind, `conn_id`, direction,
  sequence, landing time and **the payload bytes**. UDP records keep
  their encoding.
- **State digest**, a stream part for a run that opened a stream socket,
  in stable order (by `conn_id`, then slot). It covers every setting and
  state that can change the future:
  - the profile;
  - the chunking default and each connection's mode;
  - the window default and each direction's window;
  - each socket's options (`TCP_NODELAY`, `SO_REUSEADDR`, `SO_LINGER`);
  - each listener's capacity, queue and provisional endpoints;
  - each endpoint's phase, flags (FIN in and out, `.both`, reset
    received and reported, `fin_before_reset`, RST sent, failed, its
    kind and whether it was reported), and unread bytes;
  - each direction's admitted, credited, read, reclaimed and discarded
    counts, its sequence, `data_send_index` and last landing time, its
    credit pacing (last departure, owed and when);
  - every TCP entry on the wire or held;
  - the TCP port table, counter and reservations;
  - the next `conn_id`, the parked op ids and the generations.
- **Fault digest:** each rule's protocol is added.
- **Sensitivity tests:** two snapshots equal except for one setting
  (chunking `.off` against `.fixed = 1`, another default window, another
  profile, another backlog) give different digests.
- The datagram serializer is unchanged (17.5).

### 17.12 What a consumer must do (not zio's work)

- **hotseat (t-4110):** `TestHost` makes its TLS identity with
  `io.randomSecure`, which the sim refuses (read: hotseat
  `src/identity.zig:33, 374`). The sim's real clock starts at 2023-11-14
  (`src/sim.zig:405`). hotseat's sim tests on a Mac declare `.macos`; on
  Linux, `.linux`.
- **starh2 (t-4111):** move `getpeername`, `TCP_NODELAY` and `FIONREAD` to
  zio's layer. Its sim steps declare `.linux`.
- Both users' AF_UNIX sockets, hotseat's DNS connect and starh2's `::1`
  tests stay outside sim runs.

### 17.13 Kernel rows (ran)

The probes ran 3 times on macOS 25.6.0 arm64 and 3 times on Linux 6.8.0
aarch64 (colima):
- `tcpprobe.c`: rows 1 to 15 (sitting 23);
- `tcpprobe3.c`: rows 16 to 38 (sitting 25);
- `tcpprobe4.c`: rows 39 to 41, and the kernel's account of each blocked
  call (sitting 29).

The raw outputs are beside reports 23 and 25. Each kernel's three outputs
are identical, except row 10 on macOS and the random port of row 14 on
Linux.

`tcpprobe3.c` (sitting 25) replaced sitting 24's `tcpprobe2.c`. Its
blocked-call state is atomic. The worker records that it entered the
call and whether the event had started when it returned. A call that was
not inside the call at the event, or that returned before it, is
reported `REJECTED`; no run printed that. Rows 16 to 35 are byte-identical
to sitting 24's outputs on both kernels, so the repair changed no answer.

**What the blocked-call rows prove, narrowed (review 3, finding 10).**
The flags alone do not prove the call was blocked across the event: the
worker can return, pause, and read the event flag late, and `entered` is
stored before the call starts. `tcpprobe4.c` (sitting 29; rows 16 to 38
byte-identical to `tcpprobe3.c`'s outputs on both kernels, 3 runs each)
adds the kernel's own account, read just before the event: on Linux the
worker's `/proc/self/task/<tid>/syscall` named its own call (206
`sendto`, 207 `recvfrom`); on macOS `thread_info` gave run state 3,
waiting in the kernel. That held for rows 16, 19, 23, 24, 25 and 41 in
every run. So at the event each call had entered the kernel and had not
returned. **What remains uncertain:**
- on macOS the run state does not name the call. The worker makes no
  other call after `entered`, but a wait for another reason (a page
  fault, say) is not excluded by this evidence alone;
- between the sample and the event (the next statement) the call could
  still return for another cause. The only others are the 2 s guard,
  about 1.8 s away at that point, and traffic, and the probe sends none
  in that window. This is an argument from the probe's code, not a
  measurement.

Rows 39 to 41 are `tcpprobe4.c`'s new rows. The raw outputs are
`zio-sim29/probe4/` beside report 29.

| row | case | macOS | Linux | sim |
|---|---|---|---|---|
| 1 | non-blocking connect, no listener | poll `HUP`; `ECONNREFUSED` | poll `OUT/ERR/HUP`; `ECONNREFUSED` | `ConnectionRefused` (not profiled) |
| 2 | blocking connect, no listener | `ECONNREFUSED` | `ECONNREFUSED` | the same |
| 3 | non-blocking connect to a listener | in progress, then writable | the same | after a round trip |
| 4 | listen(1), 6 connects | 1 made | 2 made | panic at capacity |
| 5 | half-close | peer reads 0, sends back; own send `EPIPE` | the same | the same |
| 6 | `SHUT_RD` alone | 0, 0; peer's 2nd send `EPIPE` | 6, 5; peer fine | refused |
| 7 | close with unread bytes | peer: `ECONNRESET`, 0 | the same | the same |
| 8 | send to a closed peer, twice, then read | ok; `EPIPE`; `ECONNRESET` | ok; `EPIPE`; 0 | profile |
| 9 | linger 0; `getpeername` after | `ECONNRESET`; `EINVAL` | `ECONNRESET`; `ENOTCONN` | reset; `SocketUnconnected` |
| 10 | bytes before `EAGAIN` | 319,020 or 548,188 | 2,612,608 | the window |
| 12 | 127.0.0.1:P and 0.0.0.0:P, both reuse | allowed | `EADDRINUSE` | `.linux`: `AddressInUse`; `.macos`: refused |
| 13 | shutdown of a listener | `ENOTCONN` | ok; accept `EINVAL` | refused |
| 14 | client's local address | 127.0.0.1 | 127.0.0.1 | 127.0.0.1 |
| 15 | connect to 127.0.0.2 | no answer in 1 s | connects | `.linux`: connects; `.macos`: refused |
| 16 | `SHUT_RDWR`, read blocked in a thread | wakes, 0; later 0 | the same | the same |
| 17 | `SHUT_RDWR` with 6 unread, peer sends twice | read 0; 0; peer `EPIPE`; peer reads `ECONNRESET`, 0 | read 6; `ECONNRESET`; peer `EPIPE`; peer reads 0, 0 | profile |
| 18 | `SHUT_RDWR`, nothing unread, peer sends twice | 0; peer `EPIPE`; peer `ECONNRESET` | `ECONNRESET`; peer `EPIPE`; peer 0 | profile |
| 19 | `SHUT_WR`, read blocked in a thread | stays blocked; wakes on data | the same | the same |
| 20 | RST behind 6 unread bytes | 6; `ECONNRESET`; 0; send `EPIPE` | the same | the same |
| 21 | FIN behind 6 unread bytes | 6; 0; 0 | the same | the same |
| 22 | peer sent 5 and closed; own send; reads | ok; 5; `ECONNRESET`; 0; send `EPIPE` | ok; 5; 0; 0; send `EPIPE` | profile |
| 23 | send blocked, peer resets; then reads | `EPIPE`; `ECONNRESET`, 0 | `ECONNRESET`; 0, 0 | zio's: `ECONNRESET`; 0, 0 on both (line 3a) |
| 24 | send blocked, own `SHUT_WR` | `EPIPE` | `EPIPE` | `BrokenPipe` |
| 25 | send blocked, peer closes with unread | `EPIPE` | `ECONNRESET` | zio's: `ECONNRESET` on both (line 3a) |
| 26 | client resets before accept; read first | accept ok; `ECONNRESET`; 0 | the same | the same |
| 26b | the same; send first | send `EPIPE`, `EPIPE`; reads `ECONNRESET`, 0 | send `ECONNRESET`, `EPIPE`; reads 0, 0 | profile |
| 27 | client sends 1 and closes before accept | accept ok; 1; 0; send ok | the same | the same |
| 28 | listener closes, a connection queued | client `ECONNRESET`; 0; send `EPIPE` | the same | the same |
| 29 | accepted connection, listener closes | works | works | works |
| 30 | connect to 127.0.0.2: source | no answer | 127.0.0.1 | as row 15 |
| 31 | rebind after server close, reuse old/new 0/0, 0/1, 1/0, 1/1 | in use, ok, in use, ok | in use, in use, in use, ok | profile |
| 32 | rebind after client close, new 0, 1 | ok, ok | ok, ok | ok |
| 33 | `SO_ERROR` after a reset, twice; read | `ECONNRESET`, 0; read 0 | the same | refused |
| 34 | `SO_ERROR` after a refused connect | `ECONNREFUSED`, 0 | the same | refused |
| 35 | both shut their send side | 1 byte, 0 each | the same | the same |
| 36 | peer resets; send first, twice; reads | `EPIPE`, `EPIPE`; `ECONNRESET`, 0 | `ECONNRESET`, `EPIPE`; 0, 0 | profile |
| 37 | peer resets; `SHUT_WR`, `SHUT_RDWR` before reading; read | `ENOTCONN`, `ENOTCONN`; `ECONNRESET` | the same | `SocketUnconnected`; the reset still pending |
| 38 | peer resets; read, then `SHUT_RDWR` | `ECONNRESET`; `ENOTCONN` | the same | the same |
| 39 | own `SHUT_WR`, then peer resets (linger 0, no FIN first); send first, twice; reads | `EPIPE`, `EPIPE`; `ECONNRESET`, 0 | `ECONNRESET`, `EPIPE`; 0, 0 | profile (send line 3 before line 4) |
| 39r | the same; read first, twice; then sends | `ECONNRESET`, 0; `EPIPE`, `EPIPE` | the same | the same |
| 39u | own `SHUT_WR` after 1 byte the peer never reads; peer closes; as 39 | as 39 | as 39 | as 39 |
| 39w | own `SHUT_WR`, peer resets; `SHUT_WR`, `SHUT_RDWR`; reads | `ENOTCONN`, `ENOTCONN`; `ECONNRESET`, 0 | the same | the same (shutdown line 2 before line 3) |
| 40 | `SHUT_RDWR`, nothing unread; peer sends 70000 in one call; reads; peer sends, reads | 0, 0, 0; peer `EPIPE`, `ECONNRESET` | `ECONNRESET`, 0, 0; peer `EPIPE`, 0 | profile; one local reset, never re-armed |
| 41 | send blocked on a full window, receiver (all unread) does `SHUT_RDWR`; then sends, read; receiver reads | wakes `EPIPE`; `EPIPE`, `EPIPE`; `ECONNRESET`; receiver 0, 0 | stays blocked (the 2 s guard ends it); sends `EAGAIN`; read 0; receiver reads the bytes | profile; `.macos` zio's: the window release (17.7), the parked send `ECONNRESET`, reads 0; `.linux` stays parked (the window has no gap: 41g) |
| 41g | as 41 through zio, with the `SHUT_RDWR` 50 ms after the send starts, while the window still has a gap (Linux only) | not run (t-4126) | the parked send ends `EPIPE` about one RTO later; read 0; receiver reads the bytes | not modelled (17.14) |
| 42 | read parked, peer resets; then read, send (through zio only) | `ECONNRESET`; 0; `EPIPE` | the same | the same |
| 43 | send (full window) and read both parked on one endpoint, peer resets; then read, send (through zio only) | one op `ECONNRESET`, the other its after-reset answer (send `EPIPE`, read 0); 0; `EPIPE` | the same | the same: the harvest order picks the op (17.8, line 3a) |
| 44 | read parked, peer's FIN (through zio only) | 0; 0 | the same | the same |

**Not measured:** a listener closed while a SYN or SYN-ACK is in flight.
Loopback is too fast to catch it; the sim's rule is 17.6's.

**The ruling on t-4119 (sitting 30).** The sim shows what a zio program
sees on a kernel, not what a blocking C call sees: both users reach the
kernel only through zio. The parity table (17.15 item 5) is the oracle,
with one expected column per kernel, measured through zio. The probes
stay as the explanation of the kernel. Where an op is parked the two
differ: zio's kqueue and epoll backends end a parked op on `EV_EOF` or
`EPOLLERR`/`EPOLLHUP` by reading `SO_ERROR` (read: `handleKqueueError`,
`handleEpollError`), and a later op that finds it 0 retries its call.
Rows 23, 25 and 41 changed under `.macos` for this. Rows 42 to 44 are new
cells where an op is parked that no probe ran; each was predicted from
that mechanism and then measured through zio (sitting 30's report).

**Linux row 41, measured (sitting 30, `r41x.c` beside its report, 3 runs
per variant).** The probe's blocked send and zio's parked one differ
because the window differs at the `SHUT_RDWR`, not because one is zio:
- When a sender stalls, its window usually has a gap smaller than one
  segment (the loopback MSS is about 64 KiB; the measured `snd_wnd` was
  14464). About 250 ms later its persist timer sends data into the gap,
  and the window is 0 (`tcp_info`'s `snd_wnd`, `last_data_sent`).
- `SHUT_RDWR` after that: only zero-length window probes follow
  (`TCPWinProbe` +4), and the send stays blocked. The probe's `fill()`
  waits about 60 ms, then 200 ms more, so it is in this case; so is an
  epoll writer or a blocking loop with the event at 300 or 600 ms.
- `SHUT_RDWR` while the gap is open: about one RTO (about 206 ms) after the
  receiver's FIN reaches the sender, the persist timer sends data into the
  gap; the receiver, shut for reading, resets (`TCPAbortOnData` +1); the
  sender has the FIN, so its error is `EPIPE`. An epoll writer and a
  blocking loop both end `EPIPE` 207 to 216 ms after the event, with the
  event at 50, 100 or 200 ms. zio's parity row waited 200 ms, so it was
  in this case. `TCP_NODELAY` and a read before the event changed
  nothing.
- The kernel's mechanism (the persist timer's partial segment, the reset
  on data after `RCV_SHUTDOWN`, `EPIPE` for an RST in `CLOSE_WAIT`) is my
  reading of what the counters show, not read in the kernel's source.

The sim's window has no segment size, so it never has the gap: it is
always in the zero-window case, where the kernel keeps the send blocked.
That is row 41's `.linux` column, now measured with the event at 600 ms.
The gap case is row 41g, run on a Linux kernel only, and a fidelity limit
(17.14). Modelling it would need a segment size, the persist timer and an
RTO copied from the kernel; refusing it would also refuse the zero-window
case, which the sim gets right.

### 17.14 Fidelity limits added to section 10

- Backlog overflow panics.
- TIME_WAIT never expires in a run.
- The window is declared, and a change to it takes effect at once.
- There is no retransmission timing: a partition holds entries, it does
  not lose them.
- A failed client leaves its `S-prov` server endpoint in place until the
  listener closes.
- There is no Nagle and no delayed ACK.
- Credit is paced at one departure per L.
- There is no send buffer apart from the window: a send admits straight
  into the window and returns the count. So in row 41 under `.macos` the
  parked send is woken by the window release, admits and returns a
  count, and the next send parks until its bytes have drawn the RST,
  which it then takes (`ConnectionResetByPeer`, 17.8 line 3a). macOS's
  blocked send itself returned `EPIPE`, because its bytes waited behind a
  full send buffer. A program that loops until all is written (zio's
  `writeAll`) sees `ConnectionResetByPeer` in both: measured through zio
  on macOS (parity row 41, sittings 29 and 30).
- Chunk sizes follow a declared pattern, not an MSS.
- There is no segment size, so a window is never left with a gap smaller
  than one segment, and there is no persist timer. Under `.linux`,
  `shutdown(.both)` with unread bytes while the sender's window still has
  such a gap ends the sender's parked send `BrokenPipe` about one RTO
  later on a kernel (parity row 41g); in the sim the send stays parked, as
  on a kernel whose gap is already filled (row 41).
- `shutdown(.receive)` and `SO_ERROR` are refused.
- A passive closer's port is free at its close (a kernel holds it until
  the last ACK, well under the probe's 50 ms on loopback).
- `listen` on an unbound socket is refused (a kernel binds 0.0.0.0 and a
  port); so is a bind with SO_REUSEADDR next to a holder whose answer
  was not measured (t-4115).

### 17.15 Test plan

1. **The surface table, by reflection.** The rows cover every tag of the
   ev op union and every `net*` field of zio's std.Io vtable, both read
   with `@typeInfo`, so a new tag or field fails the test until it gets a
   row. They also cover every endpoint phase and flag combination named in
   17.6 and 17.8. Each row carries the slice that builds it. A row of a
   later slice is **pending**: it is counted and printed, and never
   counted as passed. Each slice's gate checks that the pending rows are
   exactly the rows of later slices, and the last slice requires none.
2. **An op model for streams**, in the shape of `sim_op_model_test.zig`,
   run once per profile, plus a run with no profile that must panic at
   exactly the points 17.8 names.
   - **It holds:** per direction a byte list and every conservation
     term; each endpoint's phase and flags; the landing time of every
     entry.
   - **It checks:**
     - after every step: conservation, `credited <= read`, every result
       and byte, and every wake against 17.8's lists;
     - every landing time;
     - the poll's clock (366ac57);
     - that the dead-`conn_id` guard never fires.
   - **Required cases**, each with a count that must not be 0:
     - two connects at one instant, in both creation orders;
     - a SYN held past its deadline, then released;
     - a delayed RST after the fd and port were reused;
     - a reset with held data;
     - a client reset while queued, then accepted, then read first and
       send first;
     - a listener closed with provisional and queued endpoints;
     - a reverse partition that stalls a sender;
     - a send parked when an RST lands;
     - two RSTs that land at one endpoint;
     - `.both` with a parked receive, with unread bytes, and with late
       data;
     - `failStream` with entries in flight both ways;
     - a credit that lands after its direction is terminal;
     - a partition and a delay on one entry;
     - a cancel at each handshake phase;
     - a UDP flow and a TCP connection on one address pair;
     - each chunk mode, with `.fixed = 1` at the entry limit.
   - **Built for slice 2 (sitting 30):** `src/sim_stream_op_model_test.zig`,
     without faults; its header comment has what it generates and checks.
     Of the required cases it reaches, with a count that must not be 0: a
     send parked when an RST lands, `.both` with a parked receive, with
     unread bytes (`.macos`'s release) and with late data (`.linux`'s local
     reset), a credit that lands after its direction is terminal, and each
     chunk mode. Left for slice 3 or for other tests: everything with a
     fault or a hold, `failStream`, the handshake cases (slice 1's tests),
     `.fixed = 1` at the entry limit (a bytes test), and a UDP flow beside
     a connection.
   - **Built for slice 3 (sitting 32):** the same model, seeds 17 to 32,
     with fault steps (a partition, a silence, a delay with jitter, a
     clear, `failStream`); seeds 1 to 16 are slice 2's mix, unchanged.
     It now also checks every landing time on the wire after every step.
     Of the required cases it reaches, each with its own count: a reverse
     partition that stalls a sender, a reset with held data, `failStream`
     with entries in flight both ways, and a partition and a delay on one
     entry. The handshake cases with a fault (a SYN held past its
     deadline, then released; the connect with no answer) are scenario
     tests (17.9a, point 15), as is a UDP flow beside a connection and a
     delayed RST after its fd number was reused.
3. **Teardown, apart from TIME_WAIT (finding 14).** At `sim.end`,
   **before** cleanup, the check fails, listing what remains, if any of
   these is left:
   - an unresolved op on a stream socket;
   - an occupied stream socket slot, of any kind: an open socket never
     connected, a listener (with or without an accept waiting), or an
     endpoint's descriptor (review 3, finding 8: an open socket or an
     empty listener is neither of the other two, and would pass);
   - an unretired connection: an endpoint not dead, an entry on the
     wire or held, or a credit owed.

   **How a test drains before the check.** It closes every stream
   socket, then clears its rules (a held entry is released only by
   clearing the rule that holds it), then sleeps in virtual time until
   the stream wire is empty. The sim gives the count
   (`sim.streamEntriesInFlight()`, entries on the wire, held, and credit
   departures owed); a test sleeps while it is not 0. With no fault, a
   FIN or an RST lands one L after it leaves, and an RST it answers lands
   one L later still, so `2 × L` after the last close is enough. A delay
   rule adds its delay. Paced credits (slice 2) are departures owed: they
   leave at most one L apart, so the same loop drains them. Sleeping by a
   loop on the count, not by a fixed time, is the rule: a fixed sleep
   that was long enough today is wrong the day a delay is added.

   **Reservations are expected.** They are counted and printed, then
   freed, and the check asserts that they are gone.
4. **Replay.** Ten fresh processes with the heap moved give one transcript
   per direction, wire digest, trace and state digest. One changed byte
   gives another wire digest. The sensitivity tests of 17.11 also run.
   The no-stream invariants are checked directly: in a run with no stream
   socket, the RNG position, trace and digests at the end equal those of
   the same run built without the stream code.
5. **Parity.** Every row of 17.13 that the sim models is in a Zig table
   test through zio's public API, with std.Io where the users use it.
   It runs in a sim run with a profile, and on real sockets without
   sim:
   - the `.macos` column runs on macOS (kqueue);
   - the `.linux` column runs on Linux, once with the readiness backend
     and once with io_uring.

   Rows where the kernels agree run on both hosts. A kernel change is
   then seen.
6. **Mutants** go in each slice's commit. A survivor is killed by a new
   check, or explained.
7. **The users.** starh2's three sim steps keep their hashes. A new
   `dst-conn` step (`.linux`) runs the accept loop. For hotseat (after
   t-4110):
   - a `TestHost` HTTPS request and an RTSP exchange, with chunking on;
   - a failure after a delivered prefix;
   - an accept canceled;
   - `.both` waking a blocked pump.

   Each checks the ev error and the adapter's error.

### 17.16 Build order (requirement G)

Each slice ends at a commit with the gates of section 15. **Its own gate
covers only what it builds.** Every other row of the surface table is
pending.

| slice | builds | its own gate |
|---|---|---|
| 0 | measurements (sittings 24 and 25) | 17.13 |
| 1 | the shared table, connections, `conn_id`, endpoint phases and lifetimes, retirement; ports and reservations; the profile setting; `NetOpen`, `NetBind` with `getsockname`, `NetListen`, `NetConnect` with the handshake, `NetAccept`, `NetClose` with no bytes ever sent; the listener rules of 17.6; `TCP_NODELAY`, `SO_REUSEADDR`, advisory `SO_REUSEPORT`; the `local_address` and self-bound refusals; every refusal of 17.4 | the slice 1 rows of the surface table; both users' public listen, connect and accept end to end (`zio.net`; std.Io with `reuse_address`); parity rows 2, 12, 14, 31, 32; the teardown check; promise 1 pins; mutants |
| 2 | bytes, chunks, window, credit and conservation; FIN, RST, the two reset facts, the profiles' send and receive rules; `.both`; linger 0; `getpeername`, `bytesReadable`; window and chunking settings | the op model without faults (both profiles, and the panic run with no profile); parity rows 3, 5, 7 to 9, 16 to 29, 33 to 38; mutants |
| 3 | `.tcp` rules with holding, `failStream`, the "no answer" connect | the op model with faults; replay; sensitivity |
| 4 | the users: starh2's `dst-conn` accept step; hotseat's tests | starh2's gate; hotseat's tests; an outside review |

### 17.17 Where this revision does not follow the review

- **Review finding 2 asked to "keep queued receive state and any required
  reset delivery alive".** Followed through endpoint lifetimes (17.5).
  One difference: an RST that lands on an `S-prov` endpoint kills it at
  once, because nothing can be queued for an endpoint that never reached
  the queue. That is line 3 of 17.8's RST list.
- **Review finding 9 offered "send another cumulative update when the
  outstanding entry lands".** Not used. The receiver cannot see that
  landing without a wire entry of its own. Credit is paced on the
  receiver's own clock instead (one departure per L), so its timing has a
  cause that the receiver can see.
- **The total table is written as ordered decision lists, one per event,
  not as a grid.** A grid of endpoint phases, flags and events is several
  hundred cells, most of them repeating "dropped" or "refused". A list
  covers every state by its last line, and each line names the state it
  matches. The surface table test (17.15 item 1) enumerates the grid in
  code, from the lists.
