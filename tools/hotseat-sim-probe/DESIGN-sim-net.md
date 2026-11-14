# Design brief: a sim network for zio (t-4086, t-4087), revision 3

Status: **phase 1 (section 12) is built, and four rounds of outside review
of it are closed (section 15). Section 15 is the acceptance record of phase
1. Phase 2 (faults and replay) is built and accepted at 4aeb580 (section
15); its API and rules are the comment above `pub const Endpoint` in
`src/sim.zig`.** **The stream track (TCP) is designed in section 17, not
built.**

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
- `connect` on a datagram socket.
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
  (t-4100). Still open from phase 1: t-4096, t-4098 (the op model's
  holes; flows sharing a landing instant are still kept out of it).
- Open and filed: t-4096 (zero-length receive on an empty sim pipe), t-4098
  (the op model's holes above).
- **Next: the stream track (section 17).** Revision 1 was 6e854fc
  (sitting 23); the outside review of it is `gpt-simnet9.md`. Slice 0
  (measurements) and revision 2 were done in sitting 24, not pushed until
  the grader says. Next: the grader's decision on 17.17 (the local half of
  `.both`), an outside review of revision 2, then slice 1. Filed for the users: t-4110 (hotseat's
  test host makes its TLS identity with `randomSecure`), t-4111 (starh2's
  raw `getpeername`, `TCP_NODELAY` and `FIONREAD` on accepted fds).
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
- **Tool rules for agents here:** a recursive `rm` of a path built from a
  variable is refused by a hook; make a new directory instead. Restoring a
  file with `git checkout <file>` is refused too, and would drop uncommitted
  work: let the mutant runner restore it. In zsh, a word that starts with
  `=` (`echo ====`) is a command lookup and fails; quote it.

## 17. The stream track: simulated TCP on loopback (design check, revision 2)

Status: **design only. Nothing in this section is built.** Revision 1 was
6e854fc (sitting 23). Revision 2 (sitting 24) answers the outside review
`gpt-simnet9.md` and the grader's rulings on report 23. Each claim says how
it is known: **(read)** from the code at the file and line named, **(ran)**
from the probes kept beside report 24, or **(not measured)**.

**What changed from revision 1, and why:**

- **A connection has its own identity and lifetime**, apart from its
  descriptor slot. Every wire entry carries it, so a late entry can never
  reach a replacement connection (review 2; ruling 1). 17.5.
- **The handshake has the final ACK**: a connection is in the accept queue
  only when the ACK lands. A state table covers connecting, provisional,
  cancel, timeout and every close (review 2; requirement B). 17.6.
- **Accepted bytes are delivered in chunks** with distinct landing times,
  sized by a hash, on by default. One entry per send hid framing bugs
  (review 3; requirement A). 17.7.
- **Room returns by a credit entry on the reverse wire**, at most one in
  flight per direction, so a reverse partition stalls the sender. Revision
  1 returned room at the read (ruling 10). 17.7.
- **Every way a parked op ends is listed**, with its error and its time; a
  sender learns of a close or reset when the control entry lands
  (requirement C). 17.8.
- **"Held, not lost" is defined in full**, and a test can kill a
  connection at a virtual time (`failStream`) (review 4; rulings 7, 8).
  17.9.
- **TCP flows carry protocol and connection identity**; UDP's send numbers
  and draws are unchanged; the RNG claim is stated exactly, and TCP
  landings never draw (review 1, 5; ruling 9; requirement D). 17.10.
- **Slice 0 measured 20 more rows on both kernels** (17.13, rows 16 to
  35). They settle the reset, `shutdown(.both)`, listener close,
  `SO_REUSEADDR` and `SO_ERROR` rows, and change four answers: `.both`'s
  local half follows Linux where unread or late bytes exist (17.8, where I
  disagree with ruling 4's "later reads give 0"); rebind needs
  `SO_REUSEADDR` on both sockets and returns `AddressInUse`, not a panic;
  `SO_ERROR` is refused; the client's source for 127.0.0.2 is 127.0.0.1.
- **Corrected claims:** the layer's `connect`, `send` and `recv` are called
  by the native backends, so they are "not reached by the sim dispatch",
  not "called by nobody"; `NetPoll` has TCP callers in zio's ev tests;
  default send flags are `no_signal = true`; `NetSendFile` is refused by
  name; `local_address` is refused in the public adapter (review 1).
- **Slice 1 makes both users' public listen, connect and accept work end
  to end**, with the lifetime protection, and its table test is derived
  from the op enum and the std.Io vtable (review 7; requirement E).

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
| `NetConnect` on a socket the program bound itself | refused: neither user does it | nobody |
| `IpAddress.connect` with `local_address` | refused **in the public adapter**, before any side effect (in `src/net.zig`; on Linux it would otherwise first set `IP_BIND_ADDRESS_NO_PORT`) | nobody |
| `NetSend` with default flags (`no_signal = true`), up to IOV_MAX buffers; `writeSplatHeader` | modelled | both |
| `NetRecv` with default flags | modelled | both |
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
| `getpeername` | a **new** layer function, modelled; after a reset, `SocketUnconnected` (ruling 6) | starh2 |
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
  submit (`dgramOpOnItsSocket`). A slot has a kind: datagram, stream
  unbound, listener, or stream endpoint. The datagram serializer in
  `stateDigest` keeps hashing slots 0 to 63 exactly as today
  (`digestNet`, `src/sim.zig:574`); stream state is hashed in its own
  part (17.11). A run with no stream socket therefore keeps its digests,
  and the promise 1 pins prove it.
- **A connection is its own object**, in a fixed table (`max_conns`, 256),
  with a number given in creation order (`conn_id`, from 1, never reused
  in a run). It holds two directions and two endpoint references. An
  endpoint reference is (slot, generation), or "closed" once its
  descriptor is gone.
- **Every TCP wire entry carries `conn_id`, a direction and a per-direction
  sequence number.** At landing the sim looks up the connection by
  `conn_id`, never by fd or port. A retired connection takes no entry:
  the entry is dropped and counted (`stream_dropped_retired`). So a
  delayed RST or data entry cannot reach a new connection on a reused fd
  or port.
- **A SYN is addressed.** It carries the client's `conn_id` and the
  destination address. At landing it looks up the listener bound to that
  address *then*. It never names a listener fd.
- **Retirement.** A connection is retired when both endpoints are closed
  and no entry of it is on the wire or held, or at once when it is
  aborted (17.8) and its remaining entries are discarded. Its port
  reservations follow 17.5's TIME_WAIT rule, not the connection.
- **Ports.** TCP has its own table and its own counter from 49152, apart
  from UDP. Port 0 takes the next free port; the counter wraps from 65535
  to 49152 and skips ports in use or reserved; a full range is
  `AddressInUse` (no kernel has 16384 sockets in a test). A connect from
  an unbound socket binds 127.0.0.1 and the next port, also for a
  destination of 127.0.0.2 (row 30: Linux uses 127.0.0.1 as the source).
- **TIME_WAIT, as a reservation for the rest of the run.** The endpoint
  that sent its FIN first (the active closer) leaves its local port
  reserved, with that socket's `SO_REUSEADDR` flag. A later bind to that
  port succeeds only if **both** the reserved socket and the new socket
  set `SO_REUSEADDR`; otherwise `AddressInUse` (row 31: macOS needs only
  the new socket's flag, Linux needs both; the sim takes Linux's answer,
  the stricter). A client-side active close leaves the listener's port
  free (row 32, both kernels). The reservation never expires within a
  run: a fidelity limit, since TIME_WAIT lasts 60 s on Linux.
- **Two listeners on 127.0.0.1:P and 0.0.0.0:P:** `AddressInUse` even
  with `SO_REUSEADDR` (row 12: macOS allows it, Linux refuses).

### 17.6 The handshake and the endpoint states (requirement B)

**Timeline with no fault** (L = 100 µs, `wire_latency_ns`): at t a connect
puts a SYN on the wire. At t+L the SYN lands at a listener with room; the
server side becomes **provisional** and a SYN-ACK goes back. At t+2L the
SYN-ACK lands; the client is **established**, its `NetConnect` completes,
and an ACK goes out; the client may send at once (its data follows the ACK
in the same direction). At t+3L the ACK lands; the server side is
**queued** and a parked accept wakes with it.

**Capacity.** A listener holds at most `backlog` connections, provisional
and queued together, with `backlog` clamped to 1 to 4096. A SYN that
lands when the listener is full **panics**: "accept queue full: backlog
overflow is not simulated" (ruling 3). Neither kernel refuses such a
connect, and they disagree on how many fit (row 4). This is a fidelity
limit: backlog saturation cannot be tested under this model.

**Client endpoint states:** `C-open` (opened, not connecting), `C-syn`
(SYN on the wire or held), `C-est` (established), `C-failed` (the connect
returned an error), closed.
**Server side of a connection:** `S-prov` (SYN landed, SYN-ACK out),
`S-queued` (ACK landed, in the accept queue), `S-acc` (accepted, has an
fd).
**Listener:** open, or closed.

Each cell: new state; what the op gets; wire entries. "—" means nothing
changes. Times are "at the call" (due now, harvested before the clock
moves) or "at landing" (in the harvest of the poll that reaches the
landing time).

| state | event | result |
|---|---|---|
| C-open | `NetConnect` | C-syn; the op parks; a SYN out |
| C-syn | SYN lands, listener open with room | server S-prov; a SYN-ACK out |
| C-syn | SYN lands, no listener at the address, or listener closed | an RST out to the client; no server state |
| C-syn | SYN lands, listener full | panic (capacity, above) |
| C-syn | SYN-ACK lands | C-est; `NetConnect` completes ok, at landing; an ACK out |
| C-syn | RST lands (the answer to its SYN) | C-failed; `NetConnect` fails `ConnectionRefused`, at landing |
| C-syn | the connect op is canceled, or its deadline passes | the op ends `Canceled` (zio maps a deadline to `Timeout`), at the call; the socket **stays C-syn**: canceling a wait does not abort a connect. Both zio wrappers then close the socket (next rows) |
| C-syn | close, SYN not yet landed (on the wire or held) | the connection is retired; its SYN is dropped at landing, or discarded if held (`stream_dropped_retired`); it never makes server state |
| C-syn | close, SYN landed (server S-prov, SYN-ACK in flight) | the client endpoint is closed; when the SYN-ACK lands at a closed endpoint, an RST goes back; when that lands, the server side is removed (it was never queued) |
| C-syn | a second `NetConnect` | refused: neither user does it |
| C-syn | `NetSend` or `NetRecv` | refused: a kernel answers `ENOTCONN` or waits; neither user does it |
| C-syn | `failStream` (17.9) | C-failed; the parked connect ends with the given error, at the call; the connection is retired |
| C-est | close or shutdown | as for any established endpoint (17.8) |
| C-failed | any op but close | refused: neither user reuses a failed socket |
| C-failed | close | the slot is freed; nothing on the wire |
| S-prov | ACK lands | S-queued, appended to the accept queue; a parked accept wakes with the queue head, at landing |
| S-prov | RST lands (client closed during the handshake) | removed; nothing reaches accept |
| S-prov | listener closed | removed; an RST out to the client (a client in C-est then sees the reset, as row 28) |
| S-queued | `NetAccept` | S-acc with a new slot; completes at the call (zio's inline path takes it, upstream #872) |
| S-queued | client data, FIN or RST lands | kept on the connection; accept still returns it. Rows 26 and 27: a client that reset before accept gives `ConnectionResetByPeer` at the first read, then 0; one that sent and closed gives its bytes, then 0 |
| S-queued | listener closed | the connection is aborted: an RST out to the client (row 28: the client reads `ECONNRESET`, then 0; its send fails `EPIPE`); the server side is retired |
| S-acc | listener closed | — (row 29: an accepted connection survives) |
| listener open | `NetAccept` with an empty queue | parks; one unresolved accept per listener |
| listener open | parked accept canceled | `Canceled`; queued connections stay |
| listener open | close | a parked accept ends `FileDescriptorNotASocket` at the next harvest (the generation); provisional and queued connections are aborted, as above |
| listener closed | a SYN lands at its address | an RST out (refused), as for no listener |

**Equal times.** Entries landing at the same instant are processed in
(landing time, `conn_id`, direction, sequence) order (17.10). So two SYNs
at one instant reach the listener in `conn_id` order, which is the order
of their connects; the accept queue is in ACK landing order; a parked
accept takes the queue head.

**"No answer" has no producer in slice 1.** With `setConnectOutcome`
dropped, a connect that is never answered needs a held SYN (17.9) or
`failStream`. Its check moves to slice 3.

### 17.7 Bytes: chunks, the window and credit

**Chunks (requirement A).** A send admits `min(len, room)` bytes and
returns that count. The admitted bytes go on the wire as one or more
**chunks**, each its own entry with its own landing time:

- Chunk sizes come from `h = hash(seed, tcp_send_number, chunk_index,
  "chunk")`, a keyed hash (SipHash over the four values). With h's low
  two bits 0, the chunk is `1 + (h >> 2) % 16` bytes; otherwise `512 +
  (h >> 2) % 3585` bytes (512 to 4096). The last chunk takes the rest.
  About one chunk in four is tiny, so a 5-byte TLS header or an HTTP/2
  frame header is often split. `tcp_send_number` counts TCP sends in the
  run, from 1; it is separate from UDP's send numbers.
- Chunk k of a send lands at `T0 + k × 1 µs`, where T0 is the first
  chunk's landing time. Each chunk therefore lands in its own poll, and
  a reader sees a prefix before the rest.
- **On by default.** No stream run exists yet, so nothing changes. A test
  can set a fixed chunk size, or turn chunking off (one chunk per send),
  with `sim.setStreamChunking(.{ .fixed = n } | .off | .hashed)`.
- **Never** from the run's RNG, and UDP's formula is not touched.
- **The wire and the due list.** Chunks add wire entries, not due-list
  entries: the due list holds ops (at most `max_live`), and the wire is a
  growable list. A full 256 KiB window is about 128 entries on average,
  and at most 16384 entries if every chunk were tiny. The sim declares a
  hard limit of 65536 TCP entries on the wire and held together, and
  panics above it, naming the limit. A test that sends many 1-byte
  writes sets a smaller window.

**The window.** One number per direction: the bytes the sender has
admitted and not yet seen credited. A send with no room parks; it wakes
when room appears (a credit landing, a window grow) or when the
connection ends (17.8).

**Credit (ruling 10).** When the receiver's program reads n bytes, a
**credit** entry goes on the reverse wire with the cumulative count read.
At most one credit is in flight per direction: a second read while one is
in flight updates that entry's count instead of adding one (coalesced),
and keeps its landing time. When the credit lands, the sender's used count
drops to `admitted - credited`, and a parked send with room wakes. A
partition or silence of the reverse direction holds the credit, so the
sender stalls, as in real TCP. Cost: at most one extra entry per
direction in flight. Data is not ACKed separately; credit is the only
reverse traffic.

**`sim.setStreamWindow(fd, bytes)`** sets the window of the direction
toward `fd` (what `fd` may receive); `sim.setStreamWindowDefault(bytes)`
sets it for connections made later. Default 256 KiB. Bounds: 1 byte to 16
MiB; 0 and anything above 16 MiB panic. The window is a declared bound on
admitted payload, not on the sim's memory. Shrinking below the bytes in
use discards nothing; room is 0 until use falls below the new window.
Growing wakes a parked send at once if room appears (due now). A change
takes effect at the sender at once, with no wire entry: a stated
simplification.

### 17.8 How a parked op ends, the pending error, and shutdown (requirement C)

A sender or receiver learns of the peer only when an entry lands: FIN,
RST or credit. Never by counting sends.

**What an RST landing does to the endpoint it reaches** (rows 7, 20, 22,
23):
- unread bytes stay, and are read first, on both kernels (rows 20, 22);
- a **pending reset** is set: the next `NetRecv` after the unread bytes
  ends `ConnectionResetByPeer`, once; later receives give 0 (rows 7, 20;
  ruling 5);
- a send parked at that moment ends `ConnectionResetByPeer` (Linux, rows
  23 and 25; macOS gives `EPIPE`);
- a send started after it ends `BrokenPipe` (rows 17, 18, 20, 22, both
  kernels);
- a send does not consume the receive's pending reset. This combination
  is what macOS does for the receive side and Linux does for a parked
  send: each error is one a kernel gives, and a program must handle both.
  Linux alone would give 0 at the read in row 22 (ruling 5 chose the
  error).

**An entry that lands at an endpoint whose descriptor is closed, or whose
receive half is shut** answers with an RST (rows 8, 17, 18, 22: the
sender's next send fails on both kernels). FIN and credit entries never
draw an RST.

**Every way a parked op ends:**

| op | event | result | when |
|---|---|---|---|
| receive | a data chunk lands | the bytes | at landing |
| receive | a FIN lands, nothing unread | 0 | at landing |
| receive | an RST lands | `ConnectionResetByPeer` | at landing |
| receive | own `shutdown(.both)` | 0 | at the call (row 16, both kernels) |
| receive | own `shutdown(.send)` | stays parked | (row 19, both kernels) |
| receive | own close | `FileDescriptorNotASocket` (generation) | next harvest |
| receive | cancel, deadline | `Canceled` / `Timeout` | at the call |
| receive | `failStream(.reset / .timed_out)` | `ConnectionResetByPeer` / `ConnectionTimedOut` | at the call |
| send | a credit lands and room appears | the admitted count | at landing |
| send | the window grows and room appears | the admitted count | at the call |
| send | an RST lands | `ConnectionResetByPeer` | at landing |
| send | own `shutdown(.send / .both)` | `BrokenPipe` | at the call (row 24, both kernels) |
| send | own close; cancel; deadline; `failStream` | as for a receive | as for a receive |
| send | a FIN lands | stays parked: the peer may still read (half-close, row 5) | — |
| accept, connect | see 17.6 | | |

Each wake happens once: the op is ended through the existing live check
(`isLive`, `endOp`), and a credit is applied once, at its landing.

**FIN.** `shutdown(.send)` and close put a FIN behind the bytes already
admitted, never behind bytes a parked send has not had admitted. The FIN
needs no window room. A send that is parked when its own side shuts
down ends `BrokenPipe` (above), so no admitted byte follows a FIN. Both
sides may shut down at once; each reads the other's bytes, then 0 (row
35). A connection stays alive until both directions are resolved: a FIN
or RST landed, or the connection retired.

**Close.** With nothing unread: a FIN. With unread bytes: an RST instead
(row 7). With `SO_LINGER` {1, 0}: an RST (row 9). A closed descriptor
cannot read, so its connection keeps only what is needed to deliver its
FIN and answer late entries with an RST.

**`shutdown(.both)`, measured (rows 16 to 18, 3 runs on each kernel):**

| case | macOS | Linux |
|---|---|---|
| a read blocked in another thread | wakes with 0 | wakes with 0 |
| 6 unread bytes, then read | 0 (discarded) | the 6 bytes |
| then the peer sends 5 | accepted | accepted |
| then read | 0 | `ECONNRESET` |
| the peer's next send | `EPIPE` | `EPIPE` |
| the peer then reads | `ECONNRESET`, then 0 | 0, then 0 |

The **peer-facing half is the same on both kernels**: bytes that land after
`.both` draw an RST, and the peer's next send fails. So the sim sends the
RST, and the peer follows the RST rules above. This gives the peer no
error Linux never gives: Linux's peer send fails `EPIPE` too. The only
difference is that the peer's read gives `ConnectionResetByPeer`, where
Linux gives 0, which is ruling 5's choice.

**The local half: I disagree with "later reads give 0" for two cases, on
this evidence.** Ruling 4 is right for the wake and for a quiet
connection: a parked receive completes with 0, and with nothing unread and
nothing arriving, every later read gives 0, on both kernels. But on Linux,
where starh2 runs:
- unread bytes are still delivered after `.both` (row 17: "the 6 bytes");
- a byte that lands after `.both` makes the next read fail `ECONNRESET`
  once (rows 17, 18).

starh2's read pump takes a different branch on that error: any read error
other than `EndOfStream` sets `read_failed` (read: starh2
`src/edge/wire_pump.zig:228-229`). "Later reads give 0" is macOS's answer,
and under it the sim would never show starh2 the branch Linux takes.
**Recommended:** Linux's local half. Unread bytes are delivered; a late
landing sets the pending reset (one `ConnectionResetByPeer`, then 0).
Alternative: ruling 4 as written (macOS). It is simpler, and it hides the
Linux branch. The grader decides; slice 2 builds whichever is chosen.

### 17.9 Faults on streams (rulings 7 and 8)

A rule gets a protocol: `.udp` (the default, so every phase 2 run is
unchanged) or `.tcp`. A `.tcp` rule matches a connection's entries by the
same endpoint fields as today. Delay and jitter are modelled; partition
and silence **hold** entries; loss, burst, duplication and reorder are
refused at `setRule`, because TCP hides them.

**Delay and jitter.** Drawn once per entry, at its send, from the fault
draw (never the run's RNG), keyed by `tcp_send_number` and the chunk index.
Within one direction, landing times never go backward: the clamp is always
on for TCP, and each entry lands at least 1 ns after the one before it.

**Held, defined:**
1. **When.** An entry is held if a matching partition or silence is in
   force at its send, or at its landing. A rule set while an entry is in
   flight therefore holds it at landing.
2. **Order.** Once a direction has a held entry, every later entry of that
   direction is held behind it, matching or not. Held entries keep their
   sequence order.
3. **Credit.** Held payload stays charged to the window: the sender cannot
   admit more because of it. A held credit stalls the sender (17.7).
4. **Release.** When a rule is cleared, each held direction whose entries
   no longer match any rule is released. Release only when every blocker
   is gone. Directions are released in (`conn_id`, direction) order.
5. **Release time.** Each entry keeps the delay it was given at its send
   (its planned landing minus its send time); it is not redrawn. It lands
   at `release time + that delay`, clamped behind the entry before it.
6. **The same instant.** A landing at T takes the rule state at the moment
   `landDue` processes it. A rule cleared by a task later in the same
   instant does not undo that landing; a rule set by a task that ran
   before the harvest at T does hold it. This is the order UDP uses.
7. **RST against held data.** An endpoint that aborts (close with unread
   bytes, linger 0, `failStream`) discards its own direction's held data
   and FIN at once, and releases their credit. Its data already on the
   wire still lands, ahead of the RST. The RST obeys a partition: it is
   held like any entry, so the peer learns only at release.
8. **Retired connections.** A held entry of a retired connection is
   discarded when the connection retires; a release can never revive an
   aborted connection or create server state. A SYN held past the
   connect's deadline is discarded when the program closes the socket
   (17.6); if the program keeps the socket, the release delivers it.
9. **Time.** Held entries are not on the wire, so they never make the
   clock stop "due now"; a connect deadline is reached normally.
10. **Bounds.** Held entries count toward the 65536-entry limit (17.7).
    Connections count toward `max_conns`. Both panic when exceeded,
    naming the limit. At `sim.end`, every connection, held entry and
    reservation is freed, and a run that ends with any still present
    fails the end check (17.15).

**`sim.failStream(fd, .reset | .timed_out)` (ruling 8).** A local
transport death of `fd`'s endpoint, at the virtual time of the call. A
controller task calls it, as it sets fault rules. It puts nothing on the
wire: a host that dies sends nothing. The endpoint's parked ops end with
`ConnectionResetByPeer` or `ConnectionTimedOut` at the call; its later
receives give 0 and later sends `BrokenPipe`; its unread bytes and held
entries are discarded; entries of the connection that land at it later
are dropped and counted. The peer learns nothing unless the test also
acts on the peer. hotseat's case, "the host dies in the middle of the RTSP
exchange", is `failStream(client_fd, .timed_out)` plus a partition, or
`.reset`. Both errors exist in `RecvError` and `SendError` today (read:
`src/os/net.zig:1148-1151, 1272-1278`).

### 17.10 One wire, flows, and the RNG (ruling 9; requirement D)

- **One wire.** TCP entries go on the existing wire list, in landing
  order. `nextDueIoRemaining` and the poll's clock see them unchanged.
- **Flows carry the protocol.** `sameFlow` (`src/sim.zig:2033`) compares
  only addresses and ports today. It gains the protocol, and for TCP the
  `conn_id` and direction. So a delayed TCP entry never clamps a UDP
  datagram with the same address pair, and two connections never clamp
  each other.
- **UDP's numbering and draws are unchanged.** TCP sends do not take UDP
  send numbers, and the UDP fault formula is not touched.
- **TCP landings never draw.** `landDue` lands UDP entries exactly as
  today, with its seeded choice among UDP flow heads due at one instant
  (`src/sim.zig:2058`). TCP entries due by then land in a second pass, in
  (landing time, `conn_id`, direction, sequence) order, with no draw.
- **The precise RNG claim.** In a run with stream sockets:
  - stream code never draws from the run's RNG: not for chunks, delays,
    credit, the handshake or ordering;
  - TCP completions take part in the harvest shuffle of ops due together
    (`takeDue`, `src/sim.zig:1205-1214`), which draws from the run's RNG
    as it does for every op today;
  - the scheduler's own draws (task pick, preemption) are unchanged;
  - pipes keep their `ioDelayNs` draws.
- **What a mixed run guarantees.** With TCP and UDP in one run: UDP fault
  outcomes are a function of UDP sends alone, and TCP never clamps UDP.
  But TCP changes task scheduling and the harvest shuffle, so UDP sends
  can happen at other times, and a mixed run's UDP outcomes are **not**
  equal to the same UDP workload without TCP. The wire digest of a mixed
  run includes TCP records. A run with no stream socket is unchanged in
  every respect (promise 1).

### 17.11 Determinism: trace and digests

- **Trace:** a `stream` event at each TCP landing: kind (SYN, SYN-ACK, ACK,
  data, FIN, RST, credit), `conn_id`, direction, length. Only a run that
  opens a stream socket emits one.
- **Wire digest:** TCP records go into the same running hash, with a kind
  byte, `conn_id`, direction, sequence, landing time **and the payload
  bytes**, so one changed byte gives another digest. UDP records keep
  their encoding.
- **State digest**, a stream part only for a run that opened a stream
  socket, in stable order (by `conn_id`, then slot): every connection's
  state, windows, admitted and credited counts, pending reset, FIN and RST
  flags, unread bytes; every TCP entry on the wire or held; provisional
  and queued connections per listener; the TCP port table, counter and
  TIME_WAIT reservations; the next `conn_id` and `tcp_send_number`;
  parked op ids and generations. Two runs whose futures differ cannot
  share a digest.
- **The datagram serializer is unchanged** (17.5), and empty extra slots
  are not hashed in it, so no-stream digests do not move.

### 17.12 What a consumer must do (not zio's work)

- **hotseat (t-4110):** `TestHost` makes its TLS identity with
  `io.randomSecure` (read: hotseat `src/identity.zig:33, 374`), which the
  sim refuses. The sim's real clock starts at 2023-11-14 22:13:20 UTC
  (`src/sim.zig:405`), so an identity made inside the run is valid inside
  it, and one made outside with today's date is not yet valid inside it.
- **starh2 (t-4111):** move `getpeername`, `TCP_NODELAY` and `FIONREAD` to
  zio's layer. Until then they fail silently on a sim fd.
- Both users' AF_UNIX sockets, hotseat's DNS connect and starh2's `::1`
  tests stay outside sim runs.

### 17.13 Kernel rows (ran)

Two C probes, 3 runs on macOS 25.6.0 arm64 and 3 on Linux 6.8.0 aarch64
(colima): `tcpprobe.c` (rows 1 to 15, sitting 23) and `tcpprobe2.c` (rows
16 to 35, slice 0, sitting 24), raw outputs beside report 24. Every row
was the same in 3 of 3 runs on each kernel, except row 10 on macOS
(varies by run) and the random port in row 14 on Linux. In rows 23 to 25,
the probe checks that the send was blocked before the event.

| row | case | macOS | Linux | sim |
|---|---|---|---|---|
| 1 | non-blocking connect, no listener | poll `HUP`; `ECONNREFUSED` | poll `OUT/ERR/HUP`; `ECONNREFUSED` | `ConnectionRefused` |
| 2 | blocking connect, no listener | `ECONNREFUSED` | `ECONNREFUSED` | the same |
| 3 | non-blocking connect to a listener | in progress, then writable | the same | completes after a round trip |
| 4 | listen(1), 6 connects, none accepted | 1 made | 2 made | panic at capacity |
| 5 | half-close | peer reads 0, sends back; own send `EPIPE` | the same | the same |
| 6 | `SHUT_RD`, 6 queued, peer sends | 0, 0; peer's 2nd send `EPIPE` | 6, 5; peer fine | `.receive` refused |
| 7 | close with unread bytes | peer: `ECONNRESET`, then 0 | the same | the same |
| 8 | send to a closed peer, twice, then read | ok; `EPIPE`; `ECONNRESET` | ok; `EPIPE`; 0 | ok; `BrokenPipe`; `ConnectionResetByPeer` |
| 9 | linger 0; `getpeername` after | `ECONNRESET`; `EINVAL` | `ECONNRESET`; `ENOTCONN` | reset; `SocketUnconnected` |
| 10 | bytes before `EAGAIN`, peer not reading | 319,020 or 548,188 | 2,612,608 | the window |
| 11 | rebind after server close, old without reuse | see row 31 | see row 31 | row 31 |
| 12 | 127.0.0.1:P and 0.0.0.0:P, both reuse | allowed | `EADDRINUSE` | `AddressInUse` |
| 13 | shutdown of a listener | `ENOTCONN` | ok; accept `EINVAL` | refused |
| 14 | client's local address | 127.0.0.1 | 127.0.0.1 | 127.0.0.1 |
| 15 | connect to 127.0.0.2 | no answer in 1 s | connects | connects |
| 16 | `SHUT_RDWR`, read blocked in another thread | wakes, 0; later 0 | the same | the same |
| 17 | `SHUT_RDWR` with 6 unread, peer sends twice | read 0; 0; peer `EPIPE`; peer reads `ECONNRESET`, 0 | read 6; `ECONNRESET`; peer `EPIPE`; peer reads 0, 0 | 17.8 |
| 18 | `SHUT_RDWR`, nothing unread, peer sends twice | 0; peer `EPIPE`; peer `ECONNRESET` | `ECONNRESET`; peer `EPIPE`; peer 0 | 17.8 |
| 19 | `SHUT_WR`, read blocked in another thread | stays blocked; wakes on data | the same | the same |
| 20 | RST behind 6 landed unread bytes | 6 bytes; `ECONNRESET`; 0; send `EPIPE` | the same | the same |
| 21 | FIN behind 6 landed unread bytes | 6 bytes; 0; 0 | the same | the same |
| 22 | peer sent 5 and closed; own send meets it; reads | send ok; 5 bytes; `ECONNRESET`; 0; send `EPIPE` | send ok; 5 bytes; 0; 0; send `EPIPE` | macOS's (ruling 5) |
| 23 | send blocked on a full window, peer resets | `EPIPE`; reads `ECONNRESET`, 0 | `ECONNRESET`; reads 0, 0 | `ConnectionResetByPeer`; reads: the pending reset, then 0 |
| 24 | send blocked, own `SHUT_WR` | `EPIPE` | `EPIPE` | `BrokenPipe` |
| 25 | send blocked, peer closes with unread bytes | `EPIPE` | `ECONNRESET` | `ConnectionResetByPeer` |
| 26 | client resets before accept | accept ok; `ECONNRESET`; 0 | the same | the same |
| 27 | client sends 1 and closes before accept | accept ok; 1 byte; 0; send ok | the same | the same |
| 28 | listener closes, a connection queued | client: `ECONNRESET`; 0; send `EPIPE` | the same | the same |
| 29 | accepted connection after listener close | works | works | works |
| 30 | connect to 127.0.0.2: source | no answer | client 127.0.0.1; server sees 127.0.0.1 | 127.0.0.1 |
| 31 | rebind after server close: old/new reuse 0/0, 0/1, 1/0, 1/1 | in use, ok, in use, ok | in use, in use, in use, ok | Linux's |
| 32 | rebind after client close (old reuse 1): new 0, 1 | ok, ok | ok, ok | ok |
| 33 | `SO_ERROR` after a peer reset, twice; read | `ECONNRESET`, 0; read 0 | the same | refused |
| 34 | `SO_ERROR` after a refused connect, twice | `ECONNREFUSED`, 0 | the same | refused |
| 35 | both shut their send side | each reads 1 byte, then 0 | the same | the same |

**Every row becomes a parity test** (17.15, item 4), through zio's public
API in both builds. Where the kernels differ, the test has a column per
host, so a kernel change is seen.

**Not measured:** a listener closed while a SYN or SYN-ACK is in flight
(loopback is too fast to catch it; the sim's rule is 17.6's).

### 17.14 Fidelity limits added to section 10

Backlog overflow (panic); TIME_WAIT never expires in a run; the window is
declared, and changes take effect at once; no retransmission timing (a
partition holds, it does not lose); no Nagle or delayed ACK; data is not
ACKed apart from credit; chunk sizes are a declared pattern, not an MSS;
`shutdown(.receive)` and `SO_ERROR` refused.

### 17.15 Test plan

1. **The surface table, by reflection.** For every tag of the ev op union
   and every `net*` field of zio's std.Io vtable (both read with
   `@typeInfo`, so a new op or field fails the test until it gets a row),
   and every socket state (datagram; stream open, bound, listener,
   connecting, provisional, queued, established, half-closed, reset,
   failed, closed; pipe end): the call gives its row's modelled result or
   its named refusal. The rows come from 17.4 and 17.6.
2. **An op model for streams**, in the shape of `sim_op_model_test.zig`:
   random connects (to a listener, to none, under a partition), accepts,
   sends and receives with scatter and gather, every chunking mode,
   shutdowns, closes with and without unread bytes, linger 0,
   `failStream`, window changes, cancels and deadlines, closes and
   reopens inside a harvest, `.tcp` rules set and cleared, and UDP
   traffic on the same address pairs. The model holds a byte list and a
   credit count per direction, every connection's state, and the landing
   time of every entry. It checks every result and byte, every wake
   against 17.8's table, every landing time, the poll's clock (366ac57's
   rule), and at the end: each waiting op's registration alone, no
   connection, held entry or reservation left, and every window's used
   count equal to admitted minus credited. **Required cases, each with a
   count that must be non-zero:** two connects at one instant; a SYN
   held past its deadline, then released; a delayed RST after the fd and
   port were reused; a reset with bytes held; a reverse partition that
   stalls a sender; a send parked when an RST lands; `.both` with a
   receive parked, with unread bytes, and with late data; a cancel at
   each handshake state; a UDP flow and a TCP connection on one address
   pair.
3. **Replay.** Ten fresh processes with the heap moved give one
   transcript per direction, wire digest, trace and state digest; one
   changed byte gives another wire digest. The no-stream invariants are
   checked directly: a no-stream run's RNG position, trace and digests at
   the end equal those of the same run built without the stream code.
4. **Parity, both builds.** Rows 2, 3, 5, 7, 9, 12, 14, 16 to 35 as a Zig
   table test through zio's public API (and std.Io where the users use
   it), on real loopback without sim and in a sim run with it. Without
   sim it runs on macOS (kqueue) and on Linux with the readiness backend
   and with io_uring, because they reach errors by different paths.
5. **Mutants** in each slice's commit; a survivor is killed by a new check
   or explained.
6. **The users.** starh2's three sim steps keep their hashes; a new
   `dst-conn` step runs the accept loop and admission through `connect`.
   hotseat (after t-4110): a `TestHost` HTTPS request and an RTSP
   exchange with the default chunking, a failure after a delivered prefix
   (`failStream` mid-reply), an accept canceled, and starh2's `.both`
   waking a blocked pump. Each checks the ev error and the error the
   adapter shows: std.Io maps `BrokenPipe` to `SocketUnconnected`, and a
   buffered reader keeps the cause in its `err` field.

### 17.16 Build order

Each slice ends at a commit with every gate of section 15 and its own
check.

| slice | builds | its own check |
|---|---|---|
| 0 | the measurements (done in sitting 24: rows 16 to 35) | 17.13 |
| 1 | the shared table with stream kinds; connections and `conn_id` on every entry; generations; TCP ports, TIME_WAIT reservations; `NetOpen`, `NetBind` with `getsockname`, `NetListen`, `NetConnect` with the full handshake, `NetAccept`, `NetClose`; every row of 17.6 except held SYNs; `TCP_NODELAY`, `SO_REUSEADDR`, advisory `SO_REUSEPORT`; the `local_address` refusal; every refusal of 17.4 | the reflection table (17.15 item 1); both users' public calls end to end (`zio.net` listen, connect, accept; std.Io listen with `reuse_address`, connect, accept); parity rows 2, 12, 14, 29, 31, 32; promise 1 pins; mutants |
| 2 | bytes: `NetSend`, `NetRecv`, `writeSplatHeader`; chunks; window and credit; FIN, half-close, RST, linger 0, the pending reset; `.both`; every row of 17.8; `getpeername`, `bytesReadable`; `setStreamWindow`, `setStreamChunking` | the stream op model without faults; parity rows 3, 5, 7, 9, 16 to 28, 33 to 35; mutants |
| 3 | `.tcp` rules: delay, jitter, partition and silence with holding; the refusals; `failStream`; the "no answer" connect | the op model with faults; replay (17.15 item 3) |
| 4 | the users: starh2's `dst-conn` accept step; hotseat's tests of 17.15 item 6 | starh2's gate; hotseat's tests; an outside review |

### 17.17 Where this revision does not follow the review or a ruling

- **Ruling 4, the local half of `.both`:** I recommend Linux's answer for
  unread and late bytes (17.8), with rows 17 and 18 and starh2's pump as
  the evidence. The grader decides.
- **Review 2, "keep transport state alive until its outstanding entries
  are resolved":** followed, with one exception. An aborted connection is
  retired at once, and its later entries are dropped by `conn_id` (17.5).
  Keeping it alive only to drop its entries would add a state for nothing.
- **Review 4, a "dedicated" seeded choice for equal-time TCP order:**
  not used. TCP order at one instant is fixed by `conn_id`, direction and
  sequence, with no draw. Every equal-time case then has one answer, so a
  replay needs no extra RNG stream. The cost: equal-time TCP order is not
  explored by seeds. The harvest shuffle still varies the order in which
  their completions run.
