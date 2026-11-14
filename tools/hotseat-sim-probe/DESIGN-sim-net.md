# Design brief: a sim network for zio (t-4086, t-4087)

Status: **for review before anyone builds it.** Nothing here is built.

This repo keeps no design notes (`docs/` is the user-facing mkdocs site), so
the brief sits beside the probe that found the gaps. Once a phase is built,
its protocol moves into doc comments beside the code, its reasoning into the
commit message, and this file loses that section. When the last phase lands,
the file is deleted.

Line numbers are at commit c6965cf on `hotseat/sim-second-user`.

## 1. Goal

Make the simulator (`-Dsim=true`) run code that opens its own sockets, so a
test of that code is replayable from a seed:

- **t-4086, loopback network:** `net_open` gives a sim fd. `bind`, `listen`,
  `connect` and `accept` work on loopback addresses. A connected stream is
  the existing sim pipe (`sim.pipePair`, `src/sim.zig`).
- **t-4087, datagram sockets:** `sendto`, `recvfrom`, `recvmsg`, `sendmsg`
  (one message each) and `net_poll`, with message boundaries kept, and with
  seeded loss, delay, reorder and duplicate.
- **Foreign C code** (ENet) that makes its own socket calls can be routed to
  the same sim sockets.

Users, in order of need:

1. hotseat: its app talks HTTPS and RTSP over TCP, and ENet and media over
   UDP, to a stand-in host on 127.0.0.1 (t-4082, t-4083).
2. starh2: `dst-conn` serves one connection below the accept loop on a
   hand-made pipe pair. Its scope line lists "the accept loop and
   admission", "getpeername" and "more than one connection" as unsimulated
   (starh2 `tools/dst-conn/main.zig:982`).

## 2. Non-goals

- A TCP stack. A connected stream stays a pipe: bytes in order, no segments,
  no retransmit timing, no window. Stream faults stay what `SendFaults`
  gives today (short writes, one reset at a byte), plus connect faults.
- Addresses that are not loopback. No routing, DNS, multicast, broadcast.
- Unix sockets, raw sockets, `sendfile`.
- `recvmmsg`/`sendmmsg` in the first phases (refused; see section 8).
- Any change to the behaviour or trace of a run that does not use the new
  sockets. starh2's `dst-zio` and `dst-conn` trace hashes must not move.

## 3. Today (what exists, what is refused)

- Only `net_recv`, `net_send`, `net_close`, `net_shutdown` are simulated,
  and only on fds from `sim.pipePair` (`src/ev/loop.zig` `submitSimIo`).
  Every other op panics `sim: unsimulated I/O op <op>`.
- Sim fds are `fd_base + index` (`fd_base = 100000`), in a fixed table of
  32 ends (`max_ends`). One send copies into the peer's 4096-byte buffer
  (`pipe_buf_cap`). A result is due after a seeded 1 ms or 5 ms
  (`ioDelayNs`); due results with the same time are shuffled by the seed
  (`takeDue`).
- `setsockopt`, `getsockopt` and `getsockname` are direct syscalls in
  `src/net.zig` (`Socket.setNoDelay`, `setReuseAddress`, ...), not loop ops.
  On a sim fd they reach the kernel with a number it does not own.
- tools/hotseat-sim-probe shows each refusal (`udp_bind`, `tcp_connect`,
  `udp_sendto_pipe`, `netpoll_raw_udp`).

## 4. Model

One sim **host** per run. Every sim socket lives in one table in `sim.zig`.

```
Socket = {
  kind: stream | datagram | listener,
  state: open | bound | listening | connecting | connected | closed,
  local: ?Addr,          // after bind (explicit or ephemeral)
  peer: ?Addr,           // stream: the other end; datagram: none in phase 1-2
  stream: ?pipe end      // a connected stream is today's End
  rx: datagram queue     // datagram: arrived, not yet read
  rcvbuf_bytes: u32      // datagram: the receive buffer cap (SO_RCVBUF)
  accept_q: listener's queue of connected ends not yet accepted
  options: recorded socket options (section 7)
  stats
}
Addr = { ip: 127.x.y.z (IPv4 loopback /8), port: u16 }
```

- **Ports.** `bind` to port 0 takes the next free port from a counter that
  starts at 49152, the same in every run. A port in use fails with
  `AddressInUse` (stream and datagram ports are separate spaces, as in the
  kernel). There is no `SO_REUSEPORT` sharing; the option is recorded and
  refused if a second bind relies on it.
- **Several loopback addresses.** Any 127.0.0.0/8 address is a sim address.
  A test can give the stand-in host 127.0.0.2 and the app 127.0.0.1, so a
  fault can name one side without knowing ports.
- **The wire.** A sent datagram becomes an entry `{id, from, to, bytes,
  due_at}` in a fixed list. `due_at` comes from the path's delay (section
  5). When it is due it lands in the destination socket's `rx` queue, or is
  dropped (no socket bound there; `rx` full). Wire entries join the
  existing due list's timing: the loop's park advances the clock to the
  earliest of timers, stream I/O and wire entries (`nextDueIoRemaining`
  grows a wire term).
- **Streams.** `listen` makes a listener. `connect` to a listening address
  makes a `pipePair`, puts one end on the listener's accept queue (bounded
  by backlog), and completes the connect after the usual seeded I/O delay.
  `accept` takes the oldest queued end. Connect to an address with no
  listener fails with `ConnectionRefused` after the delay.

## 5. The API a test uses

All in `zio.sim`, sim builds only, called on the sim thread (t-4085 refuses
other threads).

```zig
/// Selects datagrams or connections by direction. A null field matches
/// anything. Ports are matched only when non-null.
pub const Path = struct {
    from_ip: ?[4]u8 = null, from_port: ?u16 = null,
    to_ip: ?[4]u8 = null,   to_port: ?u16 = null,
};

pub const DatagramFaults = struct {
    /// Per million datagrams on the path, dropped when sent.
    loss_ppm: u32 = 0,
    /// Per million, delivered twice; each copy draws its own delay.
    dup_ppm: u32 = 0,
    /// Wire delay, uniform in [min, max]. Null: the default (below).
    delay_min_ns: ?u64 = null,
    delay_max_ns: ?u64 = null,
    /// Off (the default): delivery on a path keeps send order, as real
    /// loopback does; a later datagram's due time is raised to the
    /// earlier one's. On: each datagram keeps its own drawn delay, so a
    /// range wider than the gap between sends reorders.
    reorder: bool = false,
    /// Drop everything on the path: a link broken in this direction.
    blackhole: bool = false,
};

pub fn setDatagramFaults(path: Path, faults: DatagramFaults) void;
pub fn clearDatagramFaults(path: Path) void;

pub const ConnectFaults = struct {
    /// Connects on the path fail with ConnectionRefused even if a
    /// listener is there.
    refuse: bool = false,
    /// Connects on the path get no answer: they wait until the caller's
    /// timeout or cancel. (A dropped SYN.)
    no_answer: bool = false,
};
pub fn setConnectFaults(path: Path, faults: ConnectFaults) void;

/// Today's per-fd stream faults stay as they are: setSendFaults(fd, ...).

pub const DatagramStats = struct {
    sent: u64, delivered: u64, duplicated: u64,
    dropped_loss: u64, dropped_blackhole: u64,
    dropped_rcvbuf: u64, dropped_no_socket: u64,
};
pub fn datagramStats(fd: i32) DatagramStats;

/// Phase 1 only (section 11): two connected datagram fds, no ports.
pub fn datagramPair() [2]i32;
```

- **Rules, not schedules.** A test changes faults over time from a task: it
  sleeps on the virtual clock, then sets or clears a path. No fault
  timetable API.
- **Most specific path wins** when several match (more non-null fields);
  equal specificity in a fixed order (the order set). One table of at most
  32 paths; a 33rd panics.
- **The default delay** with no faults set: the seeded 1 ms or 5 ms that
  stream I/O already uses, kept in send order per path. This matches what
  stream code sees today.
- `datagramStats` is what a harness needs to say "the fault hit": a test
  that sets `loss_ppm` and sees `dropped_loss == 0` has not tested loss
  (the validity rule from t-4083's probes).

## 6. Determinism and the trace hash

- **One RNG, drawn only when a knob is on.** As `sim.Preempt` does today:
  with `loss_ppm == 0`, no draw for loss; with `dup_ppm == 0`, none for
  duplicates; with no delay range, the existing default draw. A run that
  never sets a fault draws exactly what the same code would draw with this
  feature absent. A test proves it (section 10).
- **Ids, not pointers.** Each datagram gets an id from a counter at send;
  each socket an index. The trace hashes ids and indexes, never addresses.
- **New trace events** (new `EventKind` values, so old traces are
  unchanged because they never emit them):
  `dgram` with `a = (sub << 16) | socket index`, `b = datagram id`, where
  `sub` is send, drop_loss, drop_blackhole, drop_rcvbuf, drop_no_socket,
  duplicate, deliver. `conn` for connect, accept and refusal, with the
  socket indexes. Op completions keep using `io_complete` with the submit
  id, as today.
- **State digest** (`stateDigest`) adds: the socket table (kind, state,
  local and peer address, options), each `rx` queue (ids and a hash of the
  bytes), the wire list (`due_at`, id, `to`), the port counter, the path
  fault table.
- **Same-time order.** Wire entries due at the same time go through the
  existing seeded shuffle of `takeDue`, together with stream completions.

## 7. Op semantics under sim

| op | stream socket | datagram socket | listener |
|---|---|---|---|
| `net_open` | sim fd | sim fd | (a stream fd until `listen`) |
| `net_bind` | port, `AddressInUse` | port, `AddressInUse` | - |
| `net_listen` | becomes a listener (backlog kept) | refused | - |
| `net_connect` | section 4; `ConnectFaults` | refused (phase 1-4) | refused |
| `net_accept` | refused | refused | oldest queued end; parks if none |
| `net_recv`/`net_send` | today's pipe | send: refused unconnected; recv: one datagram | refused |
| `net_recvfrom`/`net_recvmsg` | refused | one datagram, its source address; `trunc` reports the full length; `dontwait` gives `WouldBlock` | refused |
| `net_sendto`/`net_sendmsg` | refused | completes at once (UDP does not block); the datagram goes on the wire | refused |
| `net_poll(.recv)` | data or EOF in the buffer | a datagram in `rx` | a queued connection |
| `net_poll(.send)` | room in the peer buffer | always ready | refused |
| `net_close`/`net_shutdown` | today | close frees the port; shutdown refused | close frees the port, refuses queued connects |
| `setsockopt` | `TCP_NODELAY`, `SO_REUSEADDR`, `SO_KEEPALIVE`, `SO_SNDBUF` recorded | `SO_RCVBUF` (sets the cap), `SO_SNDBUF`, `SO_REUSEADDR`, `IP_PKTINFO` recorded | as stream |
| `getsockopt` | returns what was recorded | same | same |
| `getsockname`/`getpeername` | the sim addresses | `getsockname` | `getsockname` |

- `sendmsg`/`recvmsg` control data: none, except `IP_PKTINFO` on receive
  when the socket set it (ENet needs the destination address; section 8).
  Any other control message is refused.
- The direct-syscall wrappers (`os/net.zig` `setsockopt`, `getsockopt`,
  `getsockname`, `getpeername`) get a sim branch for fds at or above
  `fd_base`. That is the one place they are routed; `net.zig` does not
  change.
- A datagram larger than the cap (section 8) panics; a real kernel would
  send it, so silently cutting it would test the wrong thing.

## 8. Capacity and refusals

- Fixed tables, as `sim.zig` has today, with a panic when full: sockets 64,
  wire entries 256, datagram slots 256, path rules 32. A full table is a
  harness that needs a bigger table, not a network condition.
- **Datagram size cap: 2048 bytes** (ENet's MTU and RTP media are under it;
  check hotseat's largest datagram, see section 13). Each `rx` queue is
  also capped in bytes by `SO_RCVBUF` (default 64 KiB); overflow drops the
  new datagram and counts `dropped_rcvbuf`, which is what a kernel does.
- **Refused with a panic, by name:** a non-loopback address; IPv6 (phase
  1-4; `::1` could come later); `recvmmsg`/`sendmmsg`; `MSG_PEEK`;
  `connect` on a datagram socket; broadcast and multicast options; unknown
  socket options; any blocking C call (section 9); a real kernel fd passed
  to a sim op (it means a socket was opened outside the simulator).

## 9. Foreign C code: routing ENet's socket calls

ENet makes its socket calls in `unix.c` (cgutman/enet aca8784, read from
hotseat's dependency): `enet_socket_create` calls `socket` and `setsockopt`
(`unix.c:414-460`); `enet_socket_bind` calls `bind` (`:389`);
`enet_socket_get_address` calls `getsockname` (`:397`); `enet_socket_send`
calls `sendmsg` (`:730`); `enet_socket_receive` calls `recvmsg` (`:804`) and
reads the `IP_PKTINFO` control message for the local address; it returns 0
on `EWOULDBLOCK` and -2 on truncation. `enet_socket_wait` calls `poll`
(`:914`). `enet_socket_set_option` sets non-blocking mode with `fcntl`, or with
`ioctl(FIONBIO)` when ENet is built with `HAS_IOCTL` instead of `HAS_FCNTL`
(`unix.c:469-477`); the shim offers `zio_sim_fcntl` and refuses `ioctl`, so the
sim build must define `HAS_FCNTL`.

hotseat calls `enet_host_service(host, &ev, 0)` from one task. With a
timeout of 0 it returns (`protocol.c:1935`) before its only
`enet_socket_wait` (`protocol.c:1953`), so the app path never blocks in
ENet. hotseat waits for readability with `ev.NetPoll` on ENet's fd.

**Recommended: a POSIX-shaped C shim exported by zio, and a rename of the
calls in the sim build of the C file.**

- zio (sim builds only) exports C functions with POSIX signatures:
  `zio_sim_socket`, `zio_sim_bind`, `zio_sim_getsockname`,
  `zio_sim_setsockopt`, `zio_sim_getsockopt`, `zio_sim_fcntl`,
  `zio_sim_sendmsg`, `zio_sim_sendto`, `zio_sim_recvmsg`,
  `zio_sim_recvfrom`, `zio_sim_poll`, `zio_sim_close`. They set `errno` as
  libc would. They work on the same socket table as the ev ops, through
  synchronous entry points in `sim.zig` that the ev ops also call, so there
  is one implementation.
- They never block. `recvmsg` without `MSG_DONTWAIT` on a socket not set
  non-blocking, `poll` with a timeout other than 0, and `select` panic:
  `sim: blocking C socket call <name>`. A send completes at once; a
  receive returns a queued datagram or `EAGAIN`; `poll(…, 0)` reports
  readiness.
- **What the caller changes** (hotseat):
  1. In the sim build only, compile ENet's `unix.c` with
     `-Dsocket=zio_sim_socket -Dbind=zio_sim_bind ... -Dclose=zio_sim_close`.
     This is the same mechanism hotseat already uses for the clock
     (`-Dgettimeofday=hotseat_enet_gettimeofday`). ENet's protocol logic
     is unchanged; only its syscalls are redirected.
  2. Point the patched clock at `zio.Timestamp.now(.awake)` (t-4083 report;
     `foreign_clock` probe).
  3. Call ENet only from a task on the sim thread (t-4085 refuses other
     threads), and only with a service timeout of 0. The stand-in host's
     ENet thread, which waits in `enet_host_service(host, &ev, tick_ms)`,
     becomes a task that waits with `ev.NetPoll`, as the app's control
     task already does.
  4. Close ENet's fd through zio or the shim. hotseat already closes it
     through zio before `enet_host_destroy` (`destroyHost`).
- `IP_PKTINFO`: when the socket set it, `zio_sim_recvmsg` writes an
  `in_pktinfo` control message with the datagram's destination address.

## 10. Tests and mutants that would prove it

Tests live in the repo's suite (`src/sim.zig` or a `src/sim/` test file),
sim builds only, filtered as `sim: ` like the t-4085 tests. Refusals use the
same child-process death harness (`expectChildPanics`).

| test | what it shows | mutant it must kill |
|---|---|---|
| two `sendto` of 3 bytes, two `recvfrom` of 3 | message boundaries | concatenate datagrams in `rx` |
| `recvfrom` with `dontwait` on an empty socket | `WouldBlock`, nothing parked | park instead |
| a task parked in `net_poll(.recv)` wakes when a datagram lands | readiness | poll completes only on the next send |
| a buffer smaller than the datagram with `trunc` | full length reported, rest dropped | report the copied length |
| `loss_ppm = 500_000`, 1000 datagrams, seeds 1..5 | some dropped and some delivered on each seed, `dropped_loss + delivered == sent` | never draw loss |
| `dup_ppm`, same shape | duplicates counted and delivered | no second copy |
| delay range wider than the send gap, `reorder = true` | some seed delivers out of order | keep send order always |
| same, `reorder = false` | send order on every seed | ignore the flag |
| `blackhole` one direction | one way dead, the other works | blackhole both ways |
| a run with no faults set vs the same run with `setDatagramFaults` of all zeros | same trace hash and digest | draw for a knob that is off |
| same seed twice, faults on | same trace and digest | hash a pointer, not an id |
| two seeds, faults on | different traces | leave `dgram` events out of the hash |
| bind port 0 twice | 49152, 49153 in every run | random port |
| bind a used port | `AddressInUse` | allow it |
| connect with no listener | `ConnectionRefused` after the delay | park forever |
| three connects, three accepts | accept order is connect order | LIFO queue |
| `ConnectFaults.no_answer` with a 1 s timeout | `Timeout` at virtual 1 s | answer anyway |
| `getpeername` on an accepted stream | the connector's address | return the listener's |
| close frees the port | rebind succeeds | keep the port |
| refusals: non-loopback address, IPv6, `recvmmsg`, oversized datagram, a real fd in a sim op, a blocking C call | each panics with its message | each check removed |
| C shim: a small C file (as the probe's `foreign_clock.c`) does socket, bind, sendmsg, poll(0), recvmsg with `IP_PKTINFO` | the route works from C, `errno` included | set `errno` wrong; leave out the pktinfo message |

Existing traces: the review gate runs starh2's `dst-zio` and `dst-conn`
against the change and requires the same `dst-zio` trace hashes and the same
`MUTANT_FOUND` seeds and hashes in `dst-conn` (the method used for t-4085).
The probe's `udp_bind`, `tcp_connect`, `udp_sendto_pipe` cases flip from
refusals to results; `netpoll_raw_udp` (a real fd) stays refused.

## 11. Build order

Each phase is one round: build, review, merge, before the next starts.

1. **Datagram pair, no ports, no faults.** `sim.datagramPair()`; on its fds:
   `net_sendto`/`net_send` (the address must be the peer's), `net_recvfrom`/
   `net_recv` with boundaries, `trunc` and `dontwait`, `net_poll` for
   datagram fds and for today's pipe fds, the default delay in send order,
   `dgram` trace events, digest entries. Tests: the first four rows of the
   table, the two determinism rows, and the "no faults vs zero faults"
   row. Small on purpose: it builds the wire list and the poll plumbing
   without the address design.
2. **Datagram faults** on the pair: loss, duplicate, delay range, reorder,
   blackhole, stats. Tests: the fault rows.
3. **Sim sockets and ports:** `net_open`, `net_bind`, ephemeral ports,
   `getsockname`, the `os/net.zig` sim branch for options; datagrams by
   address; `Path` matching. `datagramPair` becomes two bound sockets.
4. **Stream loopback:** `listen`, `connect`, `accept`, `getpeername`,
   `ConnectFaults`, backlog.
5. **The C shim** and its C test.
6. **Consumers, each in its own repo:** the probe in this repo; starh2 a
   `dst-conn` variant through the accept loop (filed there); hotseat the
   ENet sim build and its first reconnect test on sim (t-4082).

## 12. Alternatives rejected

- **Interposing libc** (`LD_PRELOAD`, `DYLD_INSERT_LIBRARIES`, linker
  `--wrap`) to catch every socket call in the process: platform-specific,
  blocked on macOS for system binaries, and it catches calls from every
  thread and every library, including ones that should stay real. The
  rename is per C file and visible in the build.
- **Rewrite ENet's socket layer in Zig over `zio.net` for every build:**
  one code path, but it puts new code on hotseat's production hot path for
  a test feature, needs measuring for key-to-wire latency, and helps no
  other C library. Kept as the fallback if the renamed build ever drifts
  from production.
- **Real loopback sockets with a fault-injecting proxy thread:** real time,
  so not replayable; the t-4083 `foreign_event` probe shows what real work
  next to a virtual clock does.
- **A packet-level TCP model** (a user-space stack): large, and neither user
  tests TCP timing. Pipes give the stream semantics they test.
- **Per-fd datagram faults** (like `SendFaults`): a test thinks in links and
  directions, and often does not hold the stand-in host's fds. Paths cover
  both; stats stay per fd.
- **Seeded random ports:** a counter is already replayable; randomness adds
  nothing a test can use.
- **Reorder on by default:** real loopback keeps order, so a default that
  reorders would show every UDP test a network it never meets.
- **A separate "netsim" module or package:** the ops are dispatched in
  `loop.zig`'s sim branch and kept in `sim.zig`'s tables; a second module
  is a boundary with no second implementation behind it.

## 13. What this depends on, and questions for the reviewer

Premises (each checked or to be checked):

- hotseat calls ENet from one task, with service timeout 0 (read in
  hotseat `src/enet_control.zig`; protocol.c lines above).
- No hotseat datagram exceeds 2048 bytes. **Check** hotseat's media
  packetizer and ENet MTU before phase 2.
- The stand-in host's ENet thread can become a task (hotseat change).
- starh2's accept loop goes through `zio.net` `Server.accept` (**check**
  in starh2).

Questions:

1. Path matching by "most specific wins": is a simpler rule (last set wins)
   good enough, given tests usually set one or two paths?
2. Should `net_sendto` on a datagram socket also take the seeded 1 ms or
   5 ms before its completion, like stream sends do? The brief says no
   (UDP send does not wait for the wire), which makes ENet's send call
   cheap under sim, but it is a difference from how stream sends complete.
3. The C shim sets `errno`. Under sim, every caller is on one thread, so a
   plain global would do; using the libc `errno` keeps C callers unchanged.
   Any objection to writing libc's `errno` from Zig?
4. Is 127.0.0.0/8 enough for a test that wants two "hosts", or should a
   test be able to declare other addresses as sim-local?
