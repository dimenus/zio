// SPDX-FileCopyrightText: 2025 Lukáš Lalinský
// SPDX-License-Identifier: MIT
//
// The op-level model test of the sim's datagram sockets and pipes. The two
// storage model tests in `sim.zig` check the queues; this one checks how ops
// are submitted, parked, woken, harvested, cancelled and closed, which is
// where the outside reviews found the defects those tests missed.
//
// How it runs:
// - One sim run per seed, with a bare `ev.Loop` and no runtime, so every step
//   is exact: the test calls `Loop.add`, `Loop.cancel` and `Loop.poll` itself.
//   A timer far in the future keeps the loop from counting as done, so a poll
//   moves the clock even when no op is live.
// - Four datagram socket slots, each bound to 127.0.0.1 on its own port, and
//   one sim pipe. A random step is one of `StepKind`. ev ops live in a fixed
//   pool of slots; a race group's members live in slots too, and the group's
//   callback frees them all.
// - A group's callback that runs inside a poll may submit new ops into the
//   slots it just freed ("follow-ups"), often of the same type, so a new op
//   can sit at the address of an op that is still in the harvest's batch.
//
// The model knows only byte strings, queues, ports and which ops are
// pending. It shares no helper with the sim: it lands datagrams by the
// design's rules (section 4 and 6: wire latency, the socket bound to the
// destination port, the byte budget of length plus 256 and the 1024-packet
// limit), and it keeps each pipe direction as a list of bytes of at most
// `pipe_buf_cap`.
//
// What it checks:
// - after every op's completion: the op completed once, and its result (the
//   count, the error, the bytes in every buffer, the source port) is the
//   model's. A result decided at submit must come back unchanged, also when
//   the op was cancelled. An op that waited (parked) may end `Canceled` only
//   if a cancel was asked for it (by the test, or by a race sibling that
//   finished first); otherwise its result is computed from the model at its
//   callback.
// - after every step: each open socket's `dropped_rcvbuf`, the run's
//   no-socket drops, and the bytes buffered at each pipe end
//   (`sim.testPipeBuffered`) equal the model's.
// - after every poll: a datagram op that was due before the poll began has
//   completed, a parked receive or poll on a socket that now has a datagram
//   has completed, and a pipe op whose result was decided at submit has
//   completed 10 ms after its submit (the sim's delay is at most 5 ms).
// - at the end: every op still waiting has a reason to wait (an empty
//   queue or pipe, a full pipe); after cancelling them and draining every
//   queue, nothing is live or due, nothing is on the wire, and every
//   datagram sent was received once or counted as dropped, and every pipe
//   byte accepted was received once.
//
// Rules the generator keeps, so that the model has one answer:
// - Never two ops waiting on one socket, or two waiting receives (or sends)
//   on one pipe end at once: the sim refuses both by name.
// - Within one virtual instant, each destination port gets datagrams from
//   one source only: the sim orders flows due at one time by the seed, and
//   the model does not guess that order. Each flow is first in, first out.
// - A pipe op that will wait is never deferred: its harvest moves bytes, and
//   a deferred callback would show that later than it happened.
// - A group has members on distinct fds and at most one pipe member, so the
//   order in which the loop submits members does not matter.
// - Follow-ups only use fds with no op in flight.
// - A pipe receive always has room; a zero-room receive is t-4096.
// - A pipe receive with `dontwait` is never a group member: it can end
//   inside `Loop.add`, and a race would then cancel siblings the model has
//   already applied at submit.
//
// What it does not reach (measured with mutants, see the commit): the
// 1024-packet limit (queues here stay short; the queue model test in
// `sim.zig` fills it), pipe close, shutdown and reset during the steps (only
// a close at the end), and flows that share a landing instant.
//
// The clock step is one poll (a cap of 0 only harvests), or polls until the
// clock has moved by a fixed time. One poll stops at the next due op, and a
// pipe op is due 1 or 5 ms after submit, so single polls alone let pipe ops
// pile up until the slot pool is full.
//
// A failure prints `op model: FAIL seed S step K: ...` and panics. The test
// runs in a child process, so a crash anywhere (also outside this file)
// still names the seed and step: the child prints `op model: seed S step K`
// before each step, and the parent prints the last one. To replay one seed:
// `ZIO_OP_MODEL_SEED=S`; `ZIO_OP_MODEL_STEPS=N` changes the length.

const std = @import("std");
const builtin = @import("builtin");
const sim = @import("sim.zig");
const ev = @import("ev/root.zig");
const os_net = @import("os/net.zig");

const parent_name = "sim: op model: datagram sockets and a pipe under random sends, receives, polls, cancels, race groups, close and reopen";
const child_name = parent_name ++ ", in a child process";

const default_seeds = [_]u64{ 1, 2, 3, 4, 5, 6, 7, 8 };
const default_steps: usize = 5000;

const n_socks = 4;
const n_slots = 64;
const n_groups = 8;
const max_iov = 3;
const iov_cap = 1500;
const max_payload_part = 500;
const pipe_part_max = 1800;
const base_port: u16 = 7000;

const OpKind = enum { d_send, d_recv, d_poll_recv, d_poll_send, d_close, p_send, p_recv };

const Box = union {
    sendto: ev.NetSendTo,
    recvfrom: ev.NetRecvFrom,
    poll: ev.NetPoll,
    close: ev.NetClose,
    send: ev.NetSend,
    recv: ev.NetRecv,
};

/// A result: a count, success with no value, or an error.
const Res = union(enum) {
    n: usize,
    ok,
    err: anyerror,

    fn eql(a: Res, b: Res) bool {
        return switch (a) {
            .n => |x| b == .n and b.n == x,
            .ok => b == .ok,
            .err => |e| b == .err and b.err == e,
        };
    }
};

const Slot = struct {
    used: bool = false,
    box: Box = undefined,
    kind: OpKind = .d_send,
    serial: u64 = 0,
    fd: i32 = -1,
    /// A socket slot of `M.socks`, or a pipe end.
    target: u8 = 0,
    /// The socket's incarnation at submit.
    gen: u32 = 0,
    deferred: bool = true,
    group: ?u8 = null,
    /// The result decided at submit; null: the op waits.
    at_submit: ?Res = null,
    /// For a receive decided at submit: the datagram, or the pipe bytes taken.
    want: []const u8 = &.{},
    want_from: u16 = 0,
    lens: [max_iov]usize = @splat(0),
    n_iov: usize = 0,
    bufs: [max_iov][iov_cap]u8 = undefined,
    iov: [max_iov]os_net.iovec = undefined,
    iovc: [max_iov]os_net.iovec_const = undefined,
    parts: [max_iov][]const u8 = undefined,
    total: usize = 0,
    addr: os_net.sockaddr.in = undefined,
    addr_len: os_net.socklen_t = 0,
    dontwait: bool = false,
    cancel_req: bool = false,
    /// A close woke it while it waited: it ends FileDescriptorNotASocket.
    woken_closed: bool = false,
    completions: u32 = 0,
    /// Must be complete once `M.polls` reaches this.
    must_by_poll: ?u64 = null,
    /// Must be complete after a poll that ends at or after this clock.
    must_by_clock: ?u64 = null,
    submit_poll: u64 = 0,
    in_poll: bool = false,
    done_poll: u64 = 0,
    on_reused: bool = false,

    fn comp(s: *Slot) *ev.Completion {
        return switch (s.kind) {
            .d_send => &s.box.sendto.c,
            .d_recv => &s.box.recvfrom.c,
            .d_poll_recv, .d_poll_send => &s.box.poll.c,
            .d_close => &s.box.close.c,
            .p_send => &s.box.send.c,
            .p_recv => &s.box.recv.c,
        };
    }

    fn result(s: *Slot) Res {
        return switch (s.kind) {
            .d_send => if (s.box.sendto.getResult()) |n| .{ .n = n } else |e| .{ .err = e },
            .d_recv => if (s.box.recvfrom.getResult()) |n| .{ .n = n } else |e| .{ .err = e },
            .d_poll_recv, .d_poll_send => if (s.box.poll.getResult()) |_| .ok else |e| .{ .err = e },
            .d_close => if (s.box.close.getResult()) |_| .ok else |e| .{ .err = e },
            .p_send => if (s.box.send.getResult()) |n| .{ .n = n } else |e| .{ .err = e },
            .p_recv => if (s.box.recv.getResult()) |n| .{ .n = n } else |e| .{ .err = e },
        };
    }

    fn room(s: *const Slot) usize {
        var r: usize = 0;
        for (s.lens[0..s.n_iov]) |l| r += l;
        return r;
    }
};

const GroupFrame = struct {
    used: bool = false,
    g: ev.Group = undefined,
    members: [3]u8 = undefined,
    n: usize = 0,
    deferred: bool = true,
    cancel_req: bool = false,
    winner_seen: bool = false,
    completions: u32 = 0,
    f1_shape: bool = false,
};

const Dgm = struct { bytes: []const u8, from_port: u16 };

const MSock = struct {
    open: bool = false,
    fd: i32 = -1,
    port: u16 = 0,
    gen: u32 = 0,
    q: std.ArrayList(Dgm) = .empty,
    qh: usize = 0,
    limit: usize = sim.default_rcvbuf_bytes,
    charged: usize = 0,
    dropped_rcvbuf: u64 = 0,
    /// The op waiting on it (a receive or a poll for reading).
    waiting: ?*Slot = null,
    inflight: u32 = 0,
    reused: bool = false,
    /// Set when a receive leaves only empty datagrams; cleared when the
    /// queue empties.
    only_empty: bool = false,

    fn qlen(s: *const MSock) usize {
        return s.q.items.len - s.qh;
    }
};

const MWire = struct { due: u64, from_port: u16, to_port: u16, bytes: []const u8, to_gen: u32 };

/// One pipe end: `buf` holds the bytes sent TO it, not yet received.
const MEnd = struct {
    fd: i32 = -1,
    buf: std.ArrayList(u8) = .empty,
    waiting_recv: ?*Slot = null,
    waiting_send: ?*Slot = null,
    inflight: u32 = 0,
    accepted: u64 = 0,
    received: u64 = 0,
};

const StepKind = enum {
    ev_send,
    layer_send,
    ev_recv,
    layer_recv,
    layer_poll,
    ev_poll,
    pipe_send,
    pipe_recv,
    cancel,
    group,
    close,
    open,
    rcvbuf,
    clock,
};

const step_weights = [_]struct { StepKind, u32 }{
    .{ .ev_send, 12 },   .{ .layer_send, 5 }, .{ .ev_recv, 12 },  .{ .layer_recv, 4 },
    .{ .layer_poll, 3 }, .{ .ev_poll, 5 },    .{ .pipe_send, 9 }, .{ .pipe_recv, 9 },
    .{ .cancel, 6 },     .{ .group, 9 },      .{ .close, 3 },     .{ .open, 4 },
    .{ .rcvbuf, 3 },     .{ .clock, 22 },
};

const Counts = struct {
    steps: [std.meta.fieldNames(StepKind).len]u64 = @splat(0),
    skipped: [std.meta.fieldNames(StepKind).len]u64 = @splat(0),
    /// Steps skipped because every op slot was in use.
    no_free_slot: u64 = 0,
    ops: u64 = 0,
    parked: u64 = 0,
    canceled: u64 = 0,
    closed_hit: u64 = 0,
    reused_number_ops: u64 = 0,
    closed_then_reused: u64 = 0,
    groups: u64 = 0,
    f1_shape_groups: u64 = 0,
    followups: u64 = 0,
    followups_into_canceled_due: u64 = 0,
    f2_shape: u64 = 0,
    dgrams_sent: u64 = 0,
    empty_dgrams: u64 = 0,
    truncated: u64 = 0,
    scatter_recvs: u64 = 0,
    dropped_rcvbuf: u64 = 0,
    dropped_nosock: u64 = 0,
    discarded_at_close: u64 = 0,
    landed_in_new_socket: u64 = 0,
    /// A receive left only empty datagrams queued (no queued bytes, but
    /// entries): round 1's finding 1 moved the byte ring's head then.
    only_empty_left: u64 = 0,
    /// And a datagram with bytes then landed behind them.
    landed_behind_empty: u64 = 0,
    pipe_parked_sends: u64 = 0,
    would_block: u64 = 0,
    polls: u64 = 0,

    fn add(a: *Counts, b: Counts) void {
        inline for (comptime std.meta.fieldNames(Counts)) |name| {
            if (comptime std.mem.eql(u8, name, "steps")) {
                for (&a.steps, b.steps) |*x, y| x.* += y;
            } else if (comptime std.mem.eql(u8, name, "skipped")) {
                for (&a.skipped, b.skipped) |*x, y| x.* += y;
            } else @field(a, name) += @field(b, name);
        }
    }
};

const M = struct {
    var seed: u64 = 0;
    var step: usize = 0;
    var rng: std.Random.DefaultPrng = undefined;
    var arena: std.heap.ArenaAllocator = undefined;
    var loop: ev.Loop = undefined;
    var keepalive: ev.Timer = undefined;
    var slots: [n_slots]Slot = undefined;
    var groups: [n_groups]GroupFrame = undefined;
    var socks: [n_socks]MSock = undefined;
    var ends: [2]MEnd = undefined;
    var wire: std.ArrayList(MWire) = .empty;
    var next_gen: u32 = 1;
    var next_serial: u64 = 1;
    var polls: u64 = 0;
    var in_poll: bool = false;
    var in_add: bool = false;
    var in_cancel: bool = false;
    var used_fds: std.ArrayList(i32) = .empty;
    var c: Counts = .{};
    var dropped_nosock: u64 = 0;
    var delivered: u64 = 0;

    fn r() std.Random {
        return rng.random();
    }

    fn gpa() std.mem.Allocator {
        return arena.allocator();
    }
};

fn fail(comptime fmt: []const u8, args: anytype) noreturn {
    std.debug.print("op model: FAIL seed {d} step {d}: " ++ fmt ++ "\n", .{ M.seed, M.step } ++ args);
    @panic("op model: the sim and the model disagree");
}

fn progress() void {
    var buf: [64]u8 = undefined;
    const line = std.fmt.bufPrint(&buf, "op model: seed {d} step {d}\n", .{ M.seed, M.step }) catch unreachable;
    _ = std.c.write(2, line.ptr, line.len);
}

fn sockAddr(port: u16) os_net.sockaddr.in {
    return .{ .port = std.mem.nativeToBig(u16, port), .addr = @bitCast([4]u8{ 127, 0, 0, 1 }) };
}

// ---- the model's datagram wire ----

/// Lands every model datagram due by the sim's clock. The sim lands them at
/// the start of each harvest, and the clock only moves inside a poll just
/// before one, so between polls and inside callbacks both have landed the
/// same datagrams.
fn syncLand() void {
    const now = sim.clockNs();
    var n: usize = 0;
    while (n < M.wire.items.len and M.wire.items[n].due <= now) : (n += 1) landOne(M.wire.items[n]);
    M.wire.replaceRange(M.gpa(), 0, n, &.{}) catch @panic("oom");
}

fn landOne(w: MWire) void {
    for (&M.socks) |*s| {
        if (!s.open or s.port != w.to_port) continue;
        const cost = w.bytes.len + sim.datagram_overhead;
        if (s.qlen() == sim.rcv_packet_limit or s.charged + cost > s.limit) {
            s.dropped_rcvbuf += 1;
            M.c.dropped_rcvbuf += 1;
            return;
        }
        if (s.only_empty and w.bytes.len > 0) M.c.landed_behind_empty += 1;
        s.q.append(M.gpa(), .{ .bytes = w.bytes, .from_port = w.from_port }) catch @panic("oom");
        s.charged += cost;
        if (s.gen != w.to_gen) M.c.landed_in_new_socket += 1;
        return;
    }
    M.dropped_nosock += 1;
    M.c.dropped_nosock += 1;
}

fn sockByPort(port: u16) ?*MSock {
    for (&M.socks) |*s| {
        if (s.open and s.port == port) return s;
    }
    return null;
}

/// Whether a datagram from `from_port` to `to_port` sent now would share its
/// landing instant with another flow to the same port.
fn flowClash(from_port: u16, to_port: u16) bool {
    const due = sim.clockNs() + sim.wire_latency_ns;
    for (M.wire.items) |w| {
        if (w.due == due and w.to_port == to_port and w.from_port != from_port) return true;
    }
    return false;
}

/// Mostly an open socket's port, so queues fill and drain; otherwise any of
/// the ports a socket slot can bind, so some datagrams find no socket, and
/// some reach a socket opened after they were sent.
fn pickDestPort() u16 {
    const r = M.r();
    if (r.uintLessThan(u8, 8) != 0) {
        if (openSock(false)) |k| return M.socks[k].port;
    }
    return base_port + r.uintLessThan(u16, 2 * n_socks);
}

fn modelSend(from: *MSock, to_port: u16, bytes: []const u8) void {
    const to_gen: u32 = if (sockByPort(to_port)) |t| t.gen else 0;
    M.wire.append(M.gpa(), .{ .due = sim.clockNs() + sim.wire_latency_ns, .from_port = from.port, .to_port = to_port, .bytes = bytes, .to_gen = to_gen }) catch @panic("oom");
    M.c.dgrams_sent += 1;
    if (bytes.len == 0) M.c.empty_dgrams += 1;
}

fn takeHead(s: *MSock) Dgm {
    const d = s.q.items[s.qh];
    s.qh += 1;
    s.charged -= d.bytes.len + sim.datagram_overhead;
    M.delivered += 1;
    s.only_empty = false;
    if (s.qlen() > 0 and d.bytes.len > 0) {
        var bytes_left: usize = 0;
        for (s.q.items[s.qh..]) |x| bytes_left += x.bytes.len;
        if (bytes_left == 0) {
            s.only_empty = true;
            M.c.only_empty_left += 1;
        }
    }
    return d;
}

// ---- payloads and buffers ----

fn randomParts(s: *Slot, max_part: usize, allow_none: bool) void {
    const r = M.r();
    s.n_iov = r.intRangeAtMost(usize, 1, max_iov);
    var total: usize = 0;
    const all_empty = allow_none and r.uintLessThan(u8, 5) == 0;
    for (0..s.n_iov) |k| {
        const len: usize = if (all_empty or r.uintLessThan(u8, 4) == 0) 0 else r.intRangeAtMost(usize, 1, max_part);
        const p = M.gpa().alloc(u8, len) catch @panic("oom");
        r.bytes(p);
        s.parts[k] = p;
        s.iovc[k] = os_net.iovecConstFromSlice(p);
        total += len;
    }
    s.total = total;
}

fn gathered(s: *const Slot, n: usize) []const u8 {
    const out = M.gpa().alloc(u8, n) catch @panic("oom");
    var at: usize = 0;
    for (s.parts[0..s.n_iov]) |p| {
        if (at == n) break;
        const k = @min(p.len, n - at);
        @memcpy(out[at..][0..k], p[0..k]);
        at += k;
    }
    return out;
}

/// Receive buffers: one to three, each empty, small (truncation) or large.
fn randomBufs(s: *Slot, need_room: bool) void {
    const r = M.r();
    while (true) {
        s.n_iov = r.intRangeAtMost(usize, 1, max_iov);
        for (0..s.n_iov) |k| {
            s.lens[k] = switch (r.uintLessThan(u8, 6)) {
                0 => 0,
                1, 2 => r.intRangeAtMost(usize, 1, 24),
                else => r.intRangeAtMost(usize, 1, iov_cap),
            };
            @memset(&s.bufs[k], 0xAA);
            s.iov[k] = os_net.iovecFromSlice(s.bufs[k][0..s.lens[k]]);
        }
        if (!need_room or s.room() > 0) return;
    }
}

/// The receive's buffers hold `want[0..min(room, want.len)]`, scattered.
fn checkScatter(s: *Slot, want: []const u8, what: []const u8) void {
    const n = @min(s.room(), want.len);
    var at: usize = 0;
    for (0..s.n_iov) |k| {
        const take = @min(s.lens[k], n - at);
        if (!std.mem.eql(u8, s.bufs[k][0..take], want[at..][0..take])) {
            fail("{s} op #{d}: buffer {d} holds other bytes than the model's ({d} bytes from offset {d})", .{ what, s.serial, k, take, at });
        }
        at += take;
    }
    if (s.n_iov > 1 and n > s.lens[0]) M.c.scatter_recvs += 1;
}

// ---- slots ----

fn freeSlot() ?u8 {
    // Start at a random index so slots, and their addresses, get reused in
    // varied orders.
    const start = M.r().uintLessThan(usize, n_slots);
    for (0..n_slots) |k| {
        const i = (start + k) % n_slots;
        if (!M.slots[i].used) return @intCast(i);
    }
    M.c.no_free_slot += 1;
    return null;
}

fn freeGroup() ?u8 {
    for (&M.groups, 0..) |*g, i| {
        if (!g.used) return @intCast(i);
    }
    return null;
}

fn openSock(want_idle: bool) ?u8 {
    var cand: [n_socks]u8 = undefined;
    var n: usize = 0;
    for (&M.socks, 0..) |*s, i| {
        if (s.open and (!want_idle or s.inflight == 0)) {
            cand[n] = @intCast(i);
            n += 1;
        }
    }
    if (n == 0) return null;
    return cand[M.r().uintLessThan(usize, n)];
}

fn initSlot(i: u8, kind: OpKind) *Slot {
    const s = &M.slots[i];
    s.* = .{ .used = true, .kind = kind, .serial = M.next_serial, .submit_poll = M.polls, .in_poll = M.in_poll };
    M.next_serial += 1;
    return s;
}

// ---- building ops: each applies the model's submit-time effect and fills
// the slot; the caller adds it to the loop. Null: the rules forbid it now.

fn buildDgramSend(i: u8, from_idx: u8) ?*Slot {
    const from = &M.socks[from_idx];
    var to_port = pickDestPort();
    var tries: usize = 0;
    while (flowClash(from.port, to_port)) : (tries += 1) {
        if (tries == 3) return null;
        to_port = pickDestPort();
    }
    const s = initSlot(i, .d_send);
    s.fd = from.fd;
    s.target = from_idx;
    s.gen = from.gen;
    randomParts(s, max_payload_part, true);
    s.addr = sockAddr(to_port);
    s.box = .{ .sendto = ev.NetSendTo.init(from.fd, .{ .iovecs = s.iovc[0..s.n_iov] }, .{}, @ptrCast(&s.addr), @sizeOf(os_net.sockaddr.in)) };
    modelSend(from, to_port, gathered(s, s.total));
    s.at_submit = .{ .n = s.total };
    s.must_by_poll = M.polls + 1 + @intFromBool(M.in_poll);
    return s;
}

fn buildDgramRecv(i: u8, sock_idx: u8, allow_dontwait: bool) ?*Slot {
    const sk = &M.socks[sock_idx];
    const r = M.r();
    const dontwait = allow_dontwait and r.uintLessThan(u8, 5) == 0;
    // One unresolved ev receive or poll for reading per socket: any second
    // one refuses (`sim.dgramRefuseSecondWaiter`).
    if (sk.waiting != null) return null;
    const s = initSlot(i, .d_recv);
    s.fd = sk.fd;
    s.target = sock_idx;
    s.gen = sk.gen;
    s.dontwait = dontwait;
    randomBufs(s, false);
    s.addr = undefined;
    s.addr_len = @sizeOf(os_net.sockaddr.in);
    s.box = .{ .recvfrom = ev.NetRecvFrom.init(sk.fd, .{ .iovecs = s.iov[0..s.n_iov] }, .{ .dontwait = dontwait }, @ptrCast(&s.addr), &s.addr_len) };
    if (sk.qlen() > 0) {
        const d = takeHead(sk);
        s.want = d.bytes;
        s.want_from = d.from_port;
        s.at_submit = .{ .n = @min(s.room(), d.bytes.len) };
        if (d.bytes.len > s.room()) M.c.truncated += 1;
    } else if (dontwait) {
        s.at_submit = .{ .err = error.WouldBlock };
        M.c.would_block += 1;
    } else {
        sk.waiting = s;
        M.c.parked += 1;
    }
    if (s.at_submit != null) s.must_by_poll = M.polls + 1 + @intFromBool(M.in_poll);
    return s;
}

fn buildDgramPoll(i: u8, sock_idx: u8, for_recv: bool) ?*Slot {
    const sk = &M.socks[sock_idx];
    if (for_recv and sk.waiting != null) return null;
    const s = initSlot(i, if (for_recv) .d_poll_recv else .d_poll_send);
    s.fd = sk.fd;
    s.target = sock_idx;
    s.gen = sk.gen;
    s.box = .{ .poll = ev.NetPoll.init(sk.fd, if (for_recv) .recv else .send) };
    if (!for_recv or sk.qlen() > 0) {
        s.at_submit = .ok;
        s.must_by_poll = M.polls + 1 + @intFromBool(M.in_poll);
    } else {
        sk.waiting = s;
        M.c.parked += 1;
    }
    return s;
}

fn buildPipeSend(i: u8, e: u8) ?*Slot {
    const peer = &M.ends[1 - e];
    // One unresolved ev send per pipe end (`sim.refuseSecondPipeWaiter`).
    if (M.ends[e].waiting_send != null) return null;
    const s = initSlot(i, .p_send);
    s.fd = M.ends[e].fd;
    s.target = e;
    randomParts(s, pipe_part_max, true);
    const space = sim.pipe_buf_cap - peer.buf.items.len;
    if (s.total == 0) {
        s.at_submit = .{ .n = 0 };
    } else if (space == 0) {
        M.ends[e].waiting_send = s;
        M.c.parked += 1;
        M.c.pipe_parked_sends += 1;
    } else {
        const n = @min(s.total, space);
        peer.buf.appendSlice(M.gpa(), gathered(s, n)) catch @panic("oom");
        M.ends[e].accepted += n;
        s.at_submit = .{ .n = n };
    }
    if (s.at_submit != null) s.must_by_clock = sim.clockNs() + 10 * std.time.ns_per_ms;
    s.box = .{ .send = ev.NetSend.init(s.fd, .{ .iovecs = s.iovc[0..s.n_iov] }, .{}) };
    return s;
}

fn buildPipeRecv(i: u8, e: u8, allow_dontwait: bool) ?*Slot {
    const end = &M.ends[e];
    const r = M.r();
    const dontwait = allow_dontwait and r.uintLessThan(u8, 5) == 0;
    const level = end.buf.items.len;
    // One unresolved ev receive per pipe end.
    if (end.waiting_recv != null) return null;
    const s = initSlot(i, .p_recv);
    s.fd = end.fd;
    s.target = e;
    s.dontwait = dontwait;
    randomBufs(s, true);
    if (level > 0) {
        const n = @min(s.room(), level);
        s.want = M.gpa().dupe(u8, end.buf.items[0..n]) catch @panic("oom");
        end.buf.replaceRange(M.gpa(), 0, n, &.{}) catch @panic("oom");
        end.received += n;
        s.at_submit = .{ .n = n };
        s.must_by_clock = sim.clockNs() + 10 * std.time.ns_per_ms;
    } else if (dontwait) {
        s.at_submit = .{ .err = error.WouldBlock };
        M.c.would_block += 1;
    } else {
        end.waiting_recv = s;
        M.c.parked += 1;
    }
    s.box = .{ .recv = ev.NetRecv.init(s.fd, .{ .iovecs = s.iov[0..s.n_iov] }, .{ .dontwait = dontwait }) };
    return s;
}

/// Callbacks, deferral and bookkeeping common to every op, then the add.
fn arm(s: *Slot, deferred: bool) void {
    // A pipe op that waits moves bytes at its harvest; non-deferred, its
    // callback runs right then, so the model sees it in order.
    s.deferred = deferred and !(s.at_submit == null and (s.kind == .p_send or s.kind == .p_recv));
    const cp = s.comp();
    cp.callback = onOp;
    cp.userdata = s;
    cp.flags.defer_callback = s.deferred;
    M.c.ops += 1;
    switch (s.kind) {
        .p_send, .p_recv => M.ends[s.target].inflight += 1,
        else => {
            M.socks[s.target].inflight += 1;
            if (M.socks[s.target].reused) {
                s.on_reused = true;
                M.c.reused_number_ops += 1;
            }
        },
    }
}

fn addToLoop(cp: *ev.Completion) void {
    M.in_add = true;
    M.loop.add(cp);
    M.in_add = false;
}

/// A pipe op is allowed only when it keeps at most one waiting op of its
/// kind on its end; the builders check that.
/// Tries every socket (or both pipe ends) in a random order until the rules
/// allow the op on one. A builder that returns null has changed nothing.
fn buildAny(i: u8, kind: OpKind, idle_only: bool) ?*Slot {
    const r = M.r();
    switch (kind) {
        .d_send, .d_recv, .d_poll_recv, .d_poll_send => {
            const start = r.uintLessThan(u8, n_socks);
            for (0..n_socks) |off| {
                const k: u8 = @intCast((start + off) % n_socks);
                const sk = &M.socks[k];
                if (!sk.open or (idle_only and sk.inflight != 0)) continue;
                const s = switch (kind) {
                    .d_send => buildDgramSend(i, k),
                    .d_recv => buildDgramRecv(i, k, !idle_only),
                    else => buildDgramPoll(i, k, kind == .d_poll_recv),
                };
                if (s) |op| return op;
            }
            return null;
        },
        .p_send, .p_recv => {
            if (idle_only and (M.ends[0].inflight != 0 or M.ends[1].inflight != 0)) return null;
            const first: u8 = @intCast(r.uintLessThan(u8, 2));
            for ([2]u8{ first, 1 - first }) |e| {
                const s = if (kind == .p_send) buildPipeSend(i, e) else buildPipeRecv(i, e, !idle_only);
                if (s) |op| return op;
            }
            return null;
        },
        .d_close => unreachable,
    }
}

// ---- completions ----

fn onOp(_: *ev.Loop, cp: *ev.Completion) void {
    const s: *Slot = @ptrCast(@alignCast(cp.userdata.?));
    complete(s);
}

fn complete(s: *Slot) void {
    syncLand();
    if (!s.used) fail("a callback for a free slot (op #{d})", .{s.serial});
    s.completions += 1;
    if (s.completions > 1) fail("op #{d} ({t}) completed {d} times", .{ s.serial, s.kind, s.completions });
    const got = s.result();
    const what = @tagName(s.kind);
    if (got == .err and got.err == error.Canceled) M.c.canceled += 1;
    if (got == .err and got.err == error.FileDescriptorNotASocket) M.c.closed_hit += 1;
    if (s.at_submit) |want| {
        if (!Res.eql(want, got)) fail("{s} op #{d}: result {any}, the model's (decided at submit) {any}", .{ what, s.serial, got, want });
        if (s.kind == .d_recv and want == .n) checkDgram(s);
        if (s.kind == .p_recv and want == .n) checkScatter(s, s.want, what);
    } else if (got == .err and got.err == error.Canceled) {
        if (!s.cancel_req) fail("{s} op #{d} ended Canceled, but no cancel was asked for it", .{ what, s.serial });
        clearWaiting(s);
    } else if (s.woken_closed) {
        if (!(got == .err and got.err == error.FileDescriptorNotASocket)) fail("{s} op #{d} was waiting when its socket closed: result {any}, wanted FileDescriptorNotASocket", .{ what, s.serial, got });
        if (sockFdOpen(s.fd)) M.c.closed_then_reused += 1;
    } else harvestAction(s, got);
    finishOp(s);
}

fn sockFdOpen(fd: i32) bool {
    for (&M.socks) |*k| {
        if (k.open and k.fd == fd) return true;
    }
    return false;
}

fn checkDgram(s: *Slot) void {
    checkScatter(s, s.want, "d_recv");
    const port = std.mem.bigToNative(u16, s.addr.port);
    const ip: [4]u8 = @bitCast(s.addr.addr);
    if (port != s.want_from or !std.mem.eql(u8, &ip, &.{ 127, 0, 0, 1 })) {
        fail("d_recv op #{d}: source {any}:{d}, the model's 127.0.0.1:{d}", .{ s.serial, ip, port, s.want_from });
    }
}

fn clearWaiting(s: *Slot) void {
    switch (s.kind) {
        .p_recv => if (M.ends[s.target].waiting_recv == s) {
            M.ends[s.target].waiting_recv = null;
        },
        .p_send => if (M.ends[s.target].waiting_send == s) {
            M.ends[s.target].waiting_send = null;
        },
        else => if (M.socks[s.target].waiting == s) {
            M.socks[s.target].waiting = null;
        },
    }
}

/// The result of an op that waited, from the model's state now: its harvest
/// did the work and (non-deferred, or a datagram op) nothing else touched
/// its socket or pipe end in between.
fn harvestAction(s: *Slot, got: Res) void {
    const what = @tagName(s.kind);
    clearWaiting(s);
    switch (s.kind) {
        .d_recv, .d_poll_recv => {
            const sk = &M.socks[s.target];
            if (!sk.open or sk.gen != s.gen) fail("{s} op #{d} completed {any} after its socket closed, without being woken by the close", .{ what, s.serial, got });
            if (sk.qlen() == 0) fail("{s} op #{d} completed {any} with no datagram queued", .{ what, s.serial, got });
            if (s.kind == .d_poll_recv) {
                if (got != .ok) fail("d_poll_recv op #{d}: result {any}, wanted success", .{ s.serial, got });
                return;
            }
            const d = takeHead(sk);
            s.want = d.bytes;
            s.want_from = d.from_port;
            const want: Res = .{ .n = @min(s.room(), d.bytes.len) };
            if (!Res.eql(want, got)) fail("d_recv op #{d} (waited): result {any}, the model's {any}", .{ s.serial, got, want });
            if (d.bytes.len > s.room()) M.c.truncated += 1;
            checkDgram(s);
        },
        .p_recv => {
            const end = &M.ends[s.target];
            const level = end.buf.items.len;
            if (level == 0) fail("p_recv op #{d} (waited) completed {any} on an open pipe with nothing buffered: a kernel reader keeps waiting", .{ s.serial, got });
            const n = @min(s.room(), level);
            const want: Res = .{ .n = n };
            if (!Res.eql(want, got)) fail("p_recv op #{d} (waited): result {any}, the model's {any}", .{ s.serial, got, want });
            checkScatter(s, end.buf.items[0..n], what);
            end.buf.replaceRange(M.gpa(), 0, n, &.{}) catch @panic("oom");
            end.received += n;
        },
        .p_send => {
            const peer = &M.ends[1 - s.target];
            const space = sim.pipe_buf_cap - peer.buf.items.len;
            if (space == 0) fail("p_send op #{d} (waited) completed {any} into a full pipe", .{ s.serial, got });
            const n = @min(s.total, space);
            const want: Res = .{ .n = n };
            if (!Res.eql(want, got)) fail("p_send op #{d} (waited): result {any}, the model's {any}", .{ s.serial, got, want });
            peer.buf.appendSlice(M.gpa(), gathered(s, n)) catch @panic("oom");
            M.ends[s.target].accepted += n;
        },
        .d_send, .d_poll_send, .d_close => fail("{s} op #{d} waited, but it never waits", .{ what, s.serial }),
    }
}

fn finishOp(s: *Slot) void {
    s.done_poll = M.polls;
    switch (s.kind) {
        .p_send, .p_recv => M.ends[s.target].inflight -= 1,
        // An op that outlived its socket counts against that incarnation only.
        else => if (M.socks[s.target].gen == s.gen) {
            M.socks[s.target].inflight -= 1;
        },
    }
    if (s.group) |gi| {
        const g = &M.groups[gi];
        if (!g.winner_seen) {
            // The first member to finish wins the race: the group's own
            // callback, which runs right after this one, cancels the rest.
            g.winner_seen = true;
            for (g.members[0..g.n]) |mi| {
                if (M.slots[mi].completions == 0) M.slots[mi].cancel_req = true;
            }
        }
        // The slot stays held until the group's callback frees the frame.
    } else s.used = false;
}

fn onGroup(_: *ev.Loop, cp: *ev.Completion) void {
    const g: *GroupFrame = @ptrCast(@alignCast(cp.userdata.?));
    syncLand();
    g.completions += 1;
    if (g.completions > 1) fail("a race group completed {d} times", .{g.completions});
    for (g.members[0..g.n]) |mi| {
        if (M.slots[mi].completions != 1) fail("a race group completed while member op #{d} had completed {d} times", .{ M.slots[mi].serial, M.slots[mi].completions });
    }
    const got: Res = if (g.g.getResult()) |_| .ok else |e| .{ .err = e };
    const want: Res = if (g.cancel_req) .{ .err = error.Canceled } else .ok;
    if (!Res.eql(want, got)) fail("a race group: result {any}, wanted {any}", .{ got, want });
    var freed: [3]u8 = undefined;
    @memcpy(freed[0..g.n], g.members[0..g.n]);
    const n = g.n;
    for (freed[0..n]) |mi| M.slots[mi].used = false;
    g.used = false;
    if (M.in_poll and !M.in_add and !M.in_cancel) followUps(freed[0..n]);
}

/// Submits new ops into the slots a group just freed, inside the harvest
/// that completed it. Often of the same type, so the new op's completion is
/// at the same address as a member that may still be in the harvest's batch.
fn followUps(freed: []const u8) void {
    const r = M.r();
    for (freed) |mi| {
        if (r.boolean()) continue;
        const old = M.slots[mi];
        const kind: OpKind = if (r.uintLessThan(u8, 4) != 0) old.kind else randomOpKind();
        const s = buildFollowUp(mi, kind) orelse continue;
        M.c.followups += 1;
        if (old.at_submit != null and old.cancel_req and old.done_poll == M.polls) M.c.followups_into_canceled_due += 1;
        arm(s, r.boolean());
        addToLoop(s.comp());
    }
}

/// A follow-up uses an fd with no op in flight. A receive prefers a socket
/// with an empty queue, so it waits.
fn buildFollowUp(i: u8, kind: OpKind) ?*Slot {
    if (kind == .d_recv or kind == .d_poll_recv) {
        for (&M.socks, 0..) |*sk, k| {
            if (sk.open and sk.inflight == 0 and sk.qlen() == 0) {
                return if (kind == .d_recv) buildDgramRecv(i, @intCast(k), false) else buildDgramPoll(i, @intCast(k), true);
            }
        }
    }
    return buildAny(i, kind, true);
}

fn randomOpKind() OpKind {
    const kinds = [_]OpKind{ .d_send, .d_recv, .d_poll_recv, .d_poll_send, .p_send, .p_recv };
    return kinds[M.r().uintLessThan(usize, kinds.len)];
}

// ---- steps ----

fn pickStep() StepKind {
    var total: u32 = 0;
    for (step_weights) |w| total += w[1];
    var x = M.r().uintLessThan(u32, total);
    for (step_weights) |w| {
        if (x < w[1]) return w[0];
        x -= w[1];
    }
    unreachable;
}

fn runStep(kind: StepKind) bool {
    const r = M.r();
    switch (kind) {
        .ev_send, .ev_recv, .ev_poll, .pipe_send, .pipe_recv => {
            const i = freeSlot() orelse return false;
            const op: OpKind = switch (kind) {
                .ev_send => .d_send,
                .ev_recv => .d_recv,
                .ev_poll => if (r.boolean()) .d_poll_recv else .d_poll_send,
                .pipe_send => .p_send,
                .pipe_recv => .p_recv,
                else => unreachable,
            };
            const s = buildAny(i, op, false) orelse return false;
            arm(s, r.boolean());
            addToLoop(s.comp());
        },
        .layer_send => {
            const k = openSock(false) orelse return false;
            const from = &M.socks[k];
            const to_port = pickDestPort();
            if (flowClash(from.port, to_port)) return false;
            var tmp: Slot = .{};
            randomParts(&tmp, max_payload_part, true);
            const to = sockAddr(to_port);
            const n = os_net.sendDatagram(from.fd, tmp.iovc[0..tmp.n_iov], .{}, &to, null) catch |e| fail("layer send: {t}", .{e});
            if (n != tmp.total) fail("layer send of {d} bytes returned {d}", .{ tmp.total, n });
            modelSend(from, to_port, gathered(&tmp, tmp.total));
        },
        .layer_recv => {
            const k = openSock(false) orelse return false;
            const sk = &M.socks[k];
            var tmp: Slot = .{};
            randomBufs(&tmp, false);
            var from: os_net.sockaddr.in = undefined;
            const got = os_net.recvDatagram(sk.fd, tmp.iov[0..tmp.n_iov], .{ .dontwait = true }, &from);
            if (sk.qlen() == 0) {
                if (got) |info| fail("layer receive on an empty queue got {d} bytes", .{info.len}) else |e| {
                    if (e != error.WouldBlock) fail("layer receive on an empty queue: {t}", .{e});
                }
                M.c.would_block += 1;
            } else {
                const info = got catch |e| fail("layer receive with {d} queued: {t}", .{ sk.qlen(), e });
                const d = takeHead(sk);
                if (info.len != @min(tmp.room(), d.bytes.len) or info.truncated != (d.bytes.len > tmp.room())) {
                    fail("layer receive: len {d} truncated {any}; the model's datagram has {d} bytes, room {d}", .{ info.len, info.truncated, d.bytes.len, tmp.room() });
                }
                if (info.truncated) M.c.truncated += 1;
                tmp.addr = from;
                tmp.want = d.bytes;
                tmp.want_from = d.from_port;
                checkDgram(&tmp);
            }
        },
        .layer_poll => {
            var fds: [n_socks]os_net.pollfd = undefined;
            var idx: [n_socks]u8 = undefined;
            var n: usize = 0;
            for (&M.socks, 0..) |*sk, k| {
                if (!sk.open or r.uintLessThan(u8, 4) == 0) continue;
                fds[n] = .{ .fd = sk.fd, .events = os_net.POLL.IN | os_net.POLL.OUT, .revents = 0 };
                idx[n] = @intCast(k);
                n += 1;
            }
            if (n == 0) return false;
            _ = os_net.poll(fds[0..n], 0) catch |e| fail("layer poll: {t}", .{e});
            for (fds[0..n], idx[0..n]) |f, k| {
                const readable = f.revents & os_net.POLL.IN != 0;
                if (readable != (M.socks[k].qlen() > 0)) fail("layer poll: socket {d} readable {any}, the model has {d} queued", .{ k, readable, M.socks[k].qlen() });
                if (f.revents & os_net.POLL.OUT == 0) fail("layer poll: socket {d} not writable", .{k});
            }
        },
        .cancel => {
            var cand: [n_slots]u8 = undefined;
            var n: usize = 0;
            for (&M.slots, 0..) |*s, i| {
                if (s.used and s.completions == 0) {
                    cand[n] = @intCast(i);
                    n += 1;
                }
            }
            if (n == 0) return false;
            const s = &M.slots[cand[r.uintLessThan(usize, n)]];
            if (s.group != null and r.boolean()) {
                const g = &M.groups[s.group.?];
                g.cancel_req = true;
                for (g.members[0..g.n]) |mi| M.slots[mi].cancel_req = true;
                M.in_cancel = true;
                M.loop.cancel(&g.g.c);
                M.in_cancel = false;
            } else {
                s.cancel_req = true;
                M.in_cancel = true;
                M.loop.cancel(s.comp());
                M.in_cancel = false;
            }
        },
        .group => return runGroup(),
        .close => {
            const k = openSock(false) orelse return false;
            if (r.boolean()) {
                closeModel(k);
                os_net.close(M.socks[k].fd);
            } else {
                const i = freeSlot() orelse return false;
                const sk = &M.socks[k];
                const s = initSlot(i, .d_close);
                s.fd = sk.fd;
                s.target = k;
                s.gen = sk.gen;
                s.box = .{ .close = ev.NetClose.init(sk.fd) };
                s.at_submit = .ok;
                s.must_by_poll = M.polls + 1;
                arm(s, r.boolean());
                closeModel(k);
                addToLoop(s.comp());
            }
        },
        .open => {
            var closed: ?u8 = null;
            for (&M.socks, 0..) |*sk, k| {
                if (!sk.open) closed = @intCast(k);
            }
            const k = closed orelse return false;
            openModel(k);
        },
        .rcvbuf => {
            const k = openSock(false) orelse return false;
            const v: c_int = if (r.uintLessThan(u8, 6) == 0) @intCast(sim.default_rcvbuf_bytes) else r.intRangeAtMost(c_int, 300, 6000);
            os_net.setsockopt(M.socks[k].fd, std.posix.system.SOL.SOCKET, std.posix.system.SO.RCVBUF, std.mem.asBytes(&v)) catch |e| fail("SO_RCVBUF: {t}", .{e});
            M.socks[k].limit = @intCast(v);
        },
        .clock => {
            // One poll (a cap of 0 only harvests), or polls until the clock
            // has moved by a fixed time: one poll stops at the next due op.
            if (r.boolean()) {
                const caps = [_]u64{ 0, 20_000, 100_000, 1_000_000 };
                pollOnce(caps[r.uintLessThan(usize, caps.len)]);
            } else {
                const by = [_]u64{ 200_000, 1_000_000, 3_000_000, 10_000_000 };
                const until = sim.clockNs() + by[r.uintLessThan(usize, by.len)];
                while (sim.clockNs() < until) pollOnce(until - sim.clockNs());
            }
        },
    }
    return true;
}

fn runGroup() bool {
    const r = M.r();
    const gi = freeGroup() orelse return false;
    const g = &M.groups[gi];
    const n: usize = r.intRangeAtMost(usize, 2, 3);
    // The shape of round 2's F1: members whose result is decided at submit,
    // all finishing without deferral, so two can share one harvest and the
    // first's group callback cancels the next while it is in the batch.
    const f1 = r.uintLessThan(u8, 3) == 0;
    var used_fds: [3]i32 = .{ -2, -2, -2 };
    var pipe_used = false;
    var members: [3]*Slot = undefined;
    var got: usize = 0;
    var tries: usize = 0;
    while (got < n and tries < 12) : (tries += 1) {
        const i = freeSlot() orelse break;
        const kind = if (f1) switch (r.uintLessThan(u8, 3)) {
            0 => OpKind.d_send,
            1 => OpKind.d_recv,
            else => OpKind.d_poll_send,
        } else randomOpKind();
        const is_pipe = kind == .p_send or kind == .p_recv;
        if (is_pipe and pipe_used) continue;
        // Pick a target fd not used by another member.
        const s: ?*Slot = blk: {
            if (is_pipe) {
                const e: u8 = @intCast(r.uintLessThan(u8, 2));
                break :blk if (kind == .p_send) buildPipeSend(i, e) else buildPipeRecv(i, e, false);
            }
            var cand: [n_socks]u8 = undefined;
            var nc: usize = 0;
            for (&M.socks, 0..) |*sk, k| {
                if (!sk.open or std.mem.indexOfScalar(i32, &used_fds, sk.fd) != null) continue;
                if (f1 and kind == .d_recv and sk.qlen() == 0) continue;
                cand[nc] = @intCast(k);
                nc += 1;
            }
            if (nc == 0) break :blk null;
            const k = cand[r.uintLessThan(usize, nc)];
            break :blk switch (kind) {
                .d_send => buildDgramSend(i, k),
                .d_recv => buildDgramRecv(i, k, false),
                .d_poll_recv, .d_poll_send => buildDgramPoll(i, k, kind == .d_poll_recv),
                else => unreachable,
            };
        };
        const m = s orelse continue;
        if (is_pipe) pipe_used = true else used_fds[got] = m.fd;
        members[got] = m;
        got += 1;
    }
    if (got < 2) {
        // Not enough members: submit what was built as standalone ops, so
        // the model's submit-time effects stay true.
        for (members[0..got]) |m| {
            arm(m, r.boolean());
            addToLoop(m.comp());
        }
        return got > 0;
    }
    g.* = .{ .used = true, .n = got, .deferred = if (f1) false else r.boolean() };
    g.g = ev.Group.init(.race);
    var all_due_nondeferred: usize = 0;
    for (members[0..got], 0..) |m, k| {
        g.members[k] = @intCast((@intFromPtr(m) - @intFromPtr(&M.slots[0])) / @sizeOf(Slot));
        m.group = gi;
        arm(m, if (f1) false else r.boolean());
        g.g.add(m.comp());
        if (!m.deferred and m.at_submit != null) all_due_nondeferred += 1;
    }
    g.f1_shape = all_due_nondeferred >= 2 and !g.deferred;
    if (g.f1_shape) M.c.f1_shape_groups += 1;
    M.c.groups += 1;
    g.g.c.callback = onGroup;
    g.g.c.userdata = g;
    g.g.c.flags.defer_callback = g.deferred;
    addToLoop(&g.g.c);
    return true;
}

fn openModel(k: u8) void {
    const sk = &M.socks[k];
    const fd = os_net.socket(.ipv4, .dgram, .ip, .{ .nonblocking = true }) catch |e| fail("open: {t}", .{e});
    for (&M.socks) |*o| {
        if (o.open and o.fd == fd) fail("open returned fd {d}, which is still open", .{fd});
    }
    const reused = std.mem.indexOfScalar(i32, M.used_fds.items, fd) != null;
    if (!reused) M.used_fds.append(M.gpa(), fd) catch @panic("oom");
    const port = base_port + 2 * @as(u16, k) + M.r().uintLessThan(u16, 2);
    const a = sockAddr(port);
    os_net.bind(fd, @ptrCast(&a), @sizeOf(os_net.sockaddr.in)) catch |e| fail("bind to {d}: {t}", .{ port, e });
    sk.* = .{ .open = true, .fd = fd, .port = port, .gen = M.next_gen, .reused = reused };
    M.next_gen += 1;
}

/// The model of a close: the queue is gone, and an op waiting on the
/// socket ends FileDescriptorNotASocket at the next harvest. Ops whose
/// result was decided at submit keep it.
fn closeModel(k: u8) void {
    const sk = &M.socks[k];
    M.c.discarded_at_close += sk.qlen();
    if (sk.waiting) |w| {
        w.woken_closed = true;
        w.must_by_poll = M.polls + 1;
        sk.waiting = null;
    }
    // The dropped count is per socket; check it one last time.
    checkSockStats(sk);
    sk.open = false;
}

fn checkSockStats(sk: *MSock) void {
    const st = sim.datagramStats(sk.fd);
    if (st.dropped_rcvbuf != sk.dropped_rcvbuf) fail("socket fd {d}: {d} dropped for its byte or packet limit, the model {d}", .{ sk.fd, st.dropped_rcvbuf, sk.dropped_rcvbuf });
}

fn pollOnce(cap_ns: u64) void {
    M.in_poll = true;
    M.loop.poll(.fromNanoseconds(cap_ns)) catch |e| fail("poll: {t}", .{e});
    M.in_poll = false;
    M.polls += 1;
    M.c.polls += 1;
    syncLand();
    const now = sim.clockNs();
    for (&M.slots) |*s| {
        if (!s.used or s.completions != 0 or s.cancel_req) continue;
        if (s.must_by_poll) |p| {
            if (M.polls >= p) fail("{t} op #{d} was due before poll {d} and has not completed", .{ s.kind, s.serial, M.polls });
        }
        if (s.must_by_clock) |t| {
            if (now >= t) fail("{t} op #{d}, decided at submit, has not completed 10 ms later", .{ s.kind, s.serial });
        }
        const sk = &M.socks[s.target];
        if ((s.kind == .d_recv or s.kind == .d_poll_recv) and s.at_submit == null and !s.woken_closed and sk.open and sk.gen == s.gen and sk.qlen() > 0) {
            fail("{t} op #{d} still waits on a socket with {d} datagrams queued", .{ s.kind, s.serial, sk.qlen() });
        }
    }
}

fn checkAfterStep() void {
    for (&M.socks) |*sk| {
        if (sk.open) checkSockStats(sk);
    }
    for (&M.ends) |*e| {
        const got = sim.testPipeBuffered(e.fd);
        if (got != e.buf.items.len) fail("pipe end fd {d} holds {d} bytes, the model {d}", .{ e.fd, got, e.buf.items.len });
    }
    if (sim.datagramsDroppedNoSocket() != M.dropped_nosock) fail("{d} datagrams dropped with no socket, the model {d}", .{ sim.datagramsDroppedNoSocket(), M.dropped_nosock });
}

// ---- one seed ----

fn runSeed(seed_value: u64, steps: usize, child: bool) Counts {
    M.seed = seed_value;
    M.step = 0;
    M.rng = .init(seed_value ^ 0x6f70_6d6f_64656c);
    M.arena = .init(std.heap.page_allocator);
    defer M.arena.deinit();
    M.slots = @splat(.{});
    M.groups = @splat(.{});
    M.socks = @splat(.{});
    M.ends = @splat(.{});
    M.wire = .empty;
    M.used_fds = .empty;
    M.next_gen = 1;
    M.next_serial = 1;
    M.polls = 0;
    M.c = .{};
    M.dropped_nosock = 0;
    M.delivered = 0;

    sim.begin(seed_value);
    defer sim.end();
    M.loop.init(.{}) catch |e| fail("loop init: {t}", .{e});
    defer M.loop.deinit();
    M.keepalive = ev.Timer.init(.{ .duration = .fromSeconds(1_000_000) });
    M.loop.add(&M.keepalive.c);

    const p = sim.pipePair();
    M.ends[0].fd = p[0];
    M.ends[1].fd = p[1];
    for (0..n_socks) |k| openModel(@intCast(k));

    while (M.step < steps) : (M.step += 1) {
        if (child) progress();
        const kind = pickStep();
        if (runStep(kind)) M.c.steps[@backingInt(kind)] += 1 else M.c.skipped[@backingInt(kind)] += 1;
        checkAfterStep();
    }
    finish();
    _ = M.loop.clearTimer(&M.keepalive);
    return M.c;
}

/// Settle, check that what still waits has a reason to, cancel it, drain
/// everything, close everything, and check that nothing is left.
fn finish() void {
    // Settle by clock time, not by a count of polls: a poll moves the clock
    // only to the next due op, and a pipe op is due up to 5 ms after a wake.
    const settle_until = sim.clockNs() + 20 * std.time.ns_per_ms;
    while (sim.clockNs() < settle_until) pollOnce(2_000_000);
    checkAfterStep();
    for (&M.slots) |*s| {
        if (!s.used or s.completions != 0) continue;
        if (s.at_submit != null or s.cancel_req or s.woken_closed) fail("{t} op #{d} never completed (cancel asked: {any})", .{ s.kind, s.serial, s.cancel_req });
        const ok = switch (s.kind) {
            .d_recv, .d_poll_recv => M.socks[s.target].open and M.socks[s.target].gen == s.gen and M.socks[s.target].qlen() == 0,
            .p_recv => M.ends[s.target].buf.items.len == 0,
            .p_send => M.ends[1 - s.target].buf.items.len == sim.pipe_buf_cap,
            else => false,
        };
        if (!ok) fail("{t} op #{d} (target {d}, submitted at poll {d}, deferred {any}, group {any}) still waits with nothing to wait for; pipe levels {d} and {d}", .{ s.kind, s.serial, s.target, s.submit_poll, s.deferred, s.group, M.ends[0].buf.items.len, M.ends[1].buf.items.len });
    }
    M.step = std.math.maxInt(usize);
    for (&M.slots) |*s| {
        if (!s.used or s.completions != 0) continue;
        s.cancel_req = true;
        M.in_cancel = true;
        M.loop.cancel(s.comp());
        M.in_cancel = false;
    }
    const cancel_until = sim.clockNs() + 20 * std.time.ns_per_ms;
    while (sim.clockNs() < cancel_until) pollOnce(2_000_000);
    for (&M.slots) |*s| {
        if (s.used and s.completions == 0) fail("{t} op #{d} did not complete after its cancel", .{ s.kind, s.serial });
    }
    for (&M.groups) |*g| {
        if (g.used) fail("a race group never completed", .{});
    }
    // Drain every socket through the layer.
    for (&M.socks) |*sk| {
        if (!sk.open) continue;
        while (true) {
            var tmp: Slot = .{};
            tmp.n_iov = 1;
            tmp.lens[0] = iov_cap;
            tmp.iov[0] = os_net.iovecFromSlice(tmp.bufs[0][0..iov_cap]);
            var from: os_net.sockaddr.in = undefined;
            const got = os_net.recvDatagram(sk.fd, tmp.iov[0..1], .{ .dontwait = true }, &from);
            if (sk.qlen() == 0) {
                if (got) |info| fail("drain: a datagram of {d} bytes the model does not have", .{info.len}) else |e| {
                    if (e != error.WouldBlock) fail("drain: {t}", .{e});
                }
                break;
            }
            const info = got catch |e| fail("drain with {d} queued: {t}", .{ sk.qlen(), e });
            const d = takeHead(sk);
            if (info.len != d.bytes.len) fail("drain: {d} bytes, the model's datagram {d}", .{ info.len, d.bytes.len });
            tmp.addr = from;
            tmp.want = d.bytes;
            tmp.want_from = d.from_port;
            checkDgram(&tmp);
        }
    }
    // Drain the pipe with ev receives.
    for (0..2) |e| {
        while (M.ends[e].buf.items.len > 0) {
            const i = freeSlot().?;
            const s = buildPipeRecv(i, @intCast(e), false).?;
            arm(s, false);
            addToLoop(s.comp());
            pollOnce(10_000_000);
        }
        const i = freeSlot().?;
        const s = initSlot(i, .p_recv);
        s.fd = M.ends[e].fd;
        s.target = @intCast(e);
        s.dontwait = true;
        randomBufs(s, true);
        s.box = .{ .recv = ev.NetRecv.init(s.fd, .{ .iovecs = s.iov[0..s.n_iov] }, .{ .dontwait = true }) };
        s.at_submit = .{ .err = error.WouldBlock };
        arm(s, false);
        addToLoop(s.comp());
    }
    for (&M.socks, 0..) |*sk, k| {
        if (!sk.open) continue;
        closeModel(@intCast(k));
        os_net.close(sk.fd);
    }
    for (0..2) |e| {
        var cl = ev.NetClose.init(M.ends[e].fd);
        pipe_closed = false;
        cl.c.callback = onPipeClose;
        addToLoop(&cl.c);
        var tries: usize = 0;
        while (!pipe_closed) : (tries += 1) {
            if (tries == 10) fail("the close of pipe end {d} never completed", .{e});
            pollOnce(10_000_000);
        }
        cl.getResult() catch |err| fail("pipe close: {t}", .{err});
    }
    pollOnce(10_000_000);
    if (sim.hasLiveIo()) fail("ops still live at the end", .{});
    if (sim.hasDueIo() or sim.nextDueIoRemaining() != null) fail("I/O still due or on the wire at the end", .{});
    if (sim.hasParkedIo()) fail("a pipe op still parked at the end", .{});
    if (M.wire.items.len != 0) fail("the model still has {d} datagrams on the wire", .{M.wire.items.len});
    for (&M.slots) |*s| {
        if (s.used) fail("slot of op #{d} still held at the end", .{s.serial});
    }
    const lost = M.c.dgrams_sent - M.delivered - M.c.dropped_rcvbuf - M.c.dropped_nosock - M.c.discarded_at_close;
    if (lost != 0) fail("{d} datagrams sent, {d} received, {d} + {d} dropped, {d} discarded at close", .{ M.c.dgrams_sent, M.delivered, M.c.dropped_rcvbuf, M.c.dropped_nosock, M.c.discarded_at_close });
    for (0..2) |e| {
        if (M.ends[e].accepted != M.ends[1 - e].received) fail("pipe end {d}: {d} bytes accepted, {d} received at the other end", .{ e, M.ends[e].accepted, M.ends[1 - e].received });
    }
}

var pipe_closed = false;

fn onPipeClose(_: *ev.Loop, _: *ev.Completion) void {
    if (pipe_closed) fail("a pipe close completed twice", .{});
    pipe_closed = true;
}

fn envU64(name: [*:0]const u8) ?u64 {
    const v = std.c.getenv(name) orelse return null;
    return std.fmt.parseInt(u64, std.mem.span(v), 10) catch @panic("op model: a number was expected in the environment variable");
}

fn printCounts(c: Counts, seeds: usize, steps: usize) void {
    std.debug.print("op model counts: {d} seeds x {d} steps; steps by kind:", .{ seeds, steps });
    inline for (comptime std.meta.fieldNames(StepKind), 0..) |name, k| std.debug.print(" {s}={d}", .{ name, c.steps[k] });
    std.debug.print("\nop model counts: skipped by kind (the rules or the state forbade it then):", .{});
    inline for (comptime std.meta.fieldNames(StepKind), 0..) |name, k| std.debug.print(" {s}={d}", .{ name, c.skipped[k] });
    std.debug.print("\n", .{});
    std.debug.print("op model counts: ops={d} parked={d} canceled={d} closed_hit={d} reused_number_ops={d} closed_then_reused={d} groups={d} f1_shape_groups={d} followups={d} followups_into_canceled_due={d} f2_shape={d}\n", .{ c.ops, c.parked, c.canceled, c.closed_hit, c.reused_number_ops, c.closed_then_reused, c.groups, c.f1_shape_groups, c.followups, c.followups_into_canceled_due, c.f2_shape });
    std.debug.print("op model counts: dgrams_sent={d} empty={d} truncated={d} scatter_recvs={d} dropped_rcvbuf={d} dropped_nosock={d} discarded_at_close={d} landed_in_new_socket={d} only_empty_left={d} landed_behind_empty={d} pipe_parked_sends={d} would_block={d} polls={d} no_free_slot={d}\n", .{ c.dgrams_sent, c.empty_dgrams, c.truncated, c.scatter_recvs, c.dropped_rcvbuf, c.dropped_nosock, c.discarded_at_close, c.landed_in_new_socket, c.only_empty_left, c.landed_behind_empty, c.pipe_parked_sends, c.would_block, c.polls, c.no_free_slot });
}

test "sim: op model: datagram sockets and a pipe under random sends, receives, polls, cancels, race groups, close and reopen, in a child process" {
    if (comptime !sim.compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    if (!sim.isDeathChild()) return;
    sim.noCoreDump();
    const steps: usize = @intCast(envU64("ZIO_OP_MODEL_STEPS") orelse default_steps);
    var total: Counts = .{};
    var n_seeds: usize = 0;
    if (envU64("ZIO_OP_MODEL_SEED")) |one| {
        total.add(runSeed(one, steps, true));
        n_seeds = 1;
    } else for (default_seeds) |s| {
        total.add(runSeed(s, steps, true));
        n_seeds += 1;
    }
    printCounts(total, n_seeds, steps);
    // A kind that never happened is a hole in the instrument, not a pass.
    for (total.steps, 0..) |n, k| {
        if (n == 0) fail("no step of kind {t} ran", .{@as(StepKind, @fromBackingInt(@intCast(k)))});
    }
    inline for (.{ "parked", "canceled", "closed_hit", "reused_number_ops", "closed_then_reused", "f1_shape_groups", "followups_into_canceled_due", "empty_dgrams", "truncated", "scatter_recvs", "dropped_rcvbuf", "dropped_nosock", "discarded_at_close", "landed_in_new_socket", "pipe_parked_sends" }) |name| {
        if (@field(total, name) == 0) fail("the counter {s} is 0: that case never ran", .{name});
    }
    sim.childDone(child_name);
}

test "sim: op model: datagram sockets and a pipe under random sends, receives, polls, cancels, race groups, close and reopen" {
    if (comptime !sim.compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    if (sim.isDeathChild()) return;
    const io = std.testing.io;
    const gpa = std.testing.allocator;
    var exe_buf: [std.fs.max_path_bytes]u8 = undefined;
    const exe_len = try std.process.executablePath(io, &exe_buf);
    var env: std.process.Environ.Map = .init(gpa);
    defer env.deinit();
    if (std.c.getenv("PATH")) |path| try env.put("PATH", std.mem.span(path));
    for ([_][:0]const u8{ "ZIO_OP_MODEL_SEED", "ZIO_OP_MODEL_STEPS" }) |name| {
        if (std.c.getenv(name)) |v| try env.put(name, std.mem.span(v));
    }
    try env.put("TEST_FILTER", child_name);
    try env.put(sim.death_child_env, "1");
    const res = try std.process.run(gpa, io, .{
        .argv = &.{exe_buf[0..exe_len]},
        .environ_map = &env,
        .timeout = .{ .duration = .{ .raw = .fromSeconds(600), .clock = .awake } },
    });
    defer gpa.free(res.stdout);
    defer gpa.free(res.stderr);
    const ok = switch (res.term) {
        .exited => |code| code == 0,
        else => false,
    };
    // The last progress line names the seed and step of a crash; a model
    // failure prints its own line too. Print those, and the counts.
    var last_progress: []const u8 = "op model: no step ran";
    var it = std.mem.splitScalar(u8, res.stderr, '\n');
    while (it.next()) |line| {
        if (std.mem.indexOf(u8, line, "op model: seed ") != null) {
            last_progress = line;
        } else if (std.mem.indexOf(u8, line, "op model") != null or std.mem.indexOf(u8, line, "panic") != null) {
            std.debug.print("{s}\n", .{line});
        }
    }
    if (!ok) {
        std.debug.print("op model child: term {any}; the last step it started: {s}\n", .{ res.term, last_progress });
        return error.OpModelFailed;
    }
    try sim.checkDoneMarkers(child_name, res.stderr);
}
