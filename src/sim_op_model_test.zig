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
//   a series of sim pipe pairs (a new one when the current one is closed on
//   both ends or reset; up to 15). A random step is one of `StepKind`. ev ops
//   live in a fixed pool of slots; a race group's members live in slots too,
//   and the group's callback frees them all.
// - A group's callback that runs inside a poll may submit new ops into the
//   slots it just freed ("follow-ups"), often of the same type, so a new op
//   can sit at the address of an op that is still in the harvest's batch.
// - A callback that does not defer (an op's, or a timer's) may act inside the
//   poll as a program can (`inBatch`): close a socket and open it again (its
//   number is reused), take a datagram with a layer receive (a woken receive
//   on that socket then parks again), or cancel an op. Each can hit an op
//   later in the same harvest batch. Timers are due when a datagram lands or
//   a pipe op is harvested, so a timer and an I/O completion share a time.
// - Pipe steps also close an end, shut it down (receive, send or both), and
//   arm a write error (`sim.setSendFaults(.fail_at_byte)`) that resets the
//   pair.
// - Fault steps (seeds 9 to 16; 1 to 8 keep the phase 1 mix), between
//   polls, set or remove a datagram fault rule (design
//   protocol above `pub const Endpoint` in sim.zig): an impairment with random loss, burst, delay, jitter,
//   reorder and duplication, a partition, or a silence, on paths of the
//   sockets' ports with wildcards; at most `max_frules` in force. A removal
//   checks the counts the sim returns against the model's. The end removes
//   every rule before its proof.
//
// The model knows only byte strings, queues, ports and which ops are
// pending. It shares no helper with the sim: it lands datagrams by the
// design's rules (section 4 and 6: wire latency, the socket bound to the
// destination port, the byte budget of length plus 256 and the 1024-packet
// limit), decides every fault itself (the fault protocol above
// `pub const Endpoint` in sim.zig, with its own copy of the
// draw formula of `sim.faultDraw`'s doc comment, its own rule table and
// burst states, the order clamp, and the drop rules again at landing), and it keeps each pipe end as a list of bytes of at most
// `pipe_buf_cap` with the sim's documented rules for close, shutdown and
// reset written as its own (`pipeSendNow`, `pipeRecvNow`).
//
// What it checks:
// - after every op's completion: the op completed once, and its result (the
//   count, the error, the bytes in every buffer, the source port) is the
//   model's. A result decided at submit must come back unchanged, also when
//   the op was cancelled. An op that waited may end `Canceled` only if a
//   cancel was asked for it (by the test, or by a race sibling that finished
//   first); otherwise its result is computed from the model at its callback,
//   and a pipe op must have been woken first.
// - after every step: each open socket's `dropped_rcvbuf`, the run's
//   no-socket drops, the bytes buffered at each pipe end
//   (`sim.testPipeBuffered`), the fault counts by cause
//   (`sim.netFaultStats`) and the fault draws equal the model's.
// - after every poll, the wake obligations: a datagram op due before the
//   poll began has completed; a receive or poll waiting on a socket that now
//   has a datagram has completed; a pipe op woken (bytes, room, a close, a
//   shutdown or a reset) has completed `pipe_wake_ns` after its wake; a
//   pipe op decided at submit, 10 ms after it; a timer, once past its
//   deadline, fired exactly at it.
// - at the end: every op still waiting is given what it waits for (bytes,
//   room or a datagram) and must then complete with the model's result: an
//   op that lost its registration fails here, where a cancel would hide it.
//   Then nothing is live or due, nothing is on the wire, every datagram sent
//   and every second copy was received once or counted as dropped (by the
//   queue, no socket, a close, or a fault rule's cause), and every pipe byte accepted
//   was received once, dropped by a reset, or left at an end closed for
//   reading.
// - probes (`ZIO_OP_MODEL_PROBE`, run by the parent in their own children):
//   a second ev receive, a second ev send, and a second datagram receive
//   inside a harvest, each behind a woken op; the sim must refuse each.
//
// Rules the generator keeps, so that the model has one answer:
// - One ev receive or poll for reading per socket, and one ev receive and
//   one ev send per pipe end, until it completes: the sim refuses a second
//   (`refuseSecondPipeWaiter`, `dgramRefuseSecondWaiter`).
// - No op on a pipe end after the program closed it.
// - Within one virtual instant, each destination port gets datagrams from
//   one source only: the sim orders flows due at one time by the seed, and
//   the model does not guess that order. The instants are the ones the model
//   predicts for each copy, faults included (`flowClash`).
// - With faults (seeds 9 to 16), a race group holds at most one datagram
//   send. `ev.Group.add` puts each member first, so the loop submits
//   members in reverse order, and a send's number in the run fixes its
//   fault draws.
// - A pipe op that will wait is never deferred: its harvest moves bytes, and
//   a deferred callback would show that later than it happened.
// - A callback acts inside a poll only on a socket whose waiting op does not
//   defer: that op has not been harvested yet, or its callback would have
//   run, so the model's queue is the sim's.
// - A group has members on distinct fds and at most one pipe member, so the
//   order in which the loop submits members does not matter.
// - Follow-ups only use fds with no op in flight.
// - A pipe receive always has room; a zero-room receive is t-4096.
// - A pipe receive with `dontwait` is never a group member: it can end
//   inside `Loop.add`, and a race would then cancel siblings the model has
//   already applied at submit.
//
// What it does not reach (measured with mutants, see the commits): the
// 1024-packet limit (queues here stay short; the queue model test in
// `sim.zig` fills it), flows that share a landing instant, a waiting pipe op
// with a deferred callback (t-4098), and a CompletionQueue's coop yield (a
// bare loop has no runtime; `sim.zig` has its own test of that route).
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

/// Seeds 1 to 8 run the phase 1 mix, unchanged; seeds 9 to 16 add fault
/// steps (`faultsOn`). A wider mix loses rare shapes the narrow one hits
/// (with faults in every seed, shapes round 1's finding 1 needs fell by
/// half), so the old mix stays a block of its own.
const default_seeds = [_]u64{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 };

fn faultsOn() bool {
    return M.seed >= 9;
}
const default_steps: usize = 5000;

const n_socks = 4;
const n_slots = 64;
const n_groups = 8;
const max_iov = 3;
const iov_cap = 1500;
const max_payload_part = 500;
const pipe_part_max = 1800;
const base_port: u16 = 7000;
/// Pipe pairs in one run: the sim has 32 ends and never reuses one.
const max_pairs = 15;
const n_timers = 4;
/// The sim's largest delay from a pipe op's wake to its harvest
/// (`ioDelayNs`: 1 or 5 ms).
const pipe_wake_ns: u64 = 5 * std.time.ns_per_ms;

const OpKind = enum { d_send, d_recv, d_poll_recv, d_poll_send, d_close, p_send, p_recv, p_close, p_shutdown };

const Box = union {
    sendto: ev.NetSendTo,
    recvfrom: ev.NetRecvFrom,
    poll: ev.NetPoll,
    close: ev.NetClose,
    send: ev.NetSend,
    recv: ev.NetRecv,
    shutdown: ev.NetShutdown,
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
    /// For an op that waits: when what it waits for happened (bytes, room,
    /// a close, a shutdown, a reset). It must then complete within
    /// `pipe_wake_ns` (a pipe op); a datagram op that waits must complete in
    /// the poll whose harvest landed its datagram.
    wake_since: ?u64 = null,
    /// The end's registration proof gave this op, alone, what it waits for.
    proving: bool = false,

    fn comp(s: *Slot) *ev.Completion {
        return switch (s.kind) {
            .d_send => &s.box.sendto.c,
            .d_recv => &s.box.recvfrom.c,
            .d_poll_recv, .d_poll_send => &s.box.poll.c,
            .d_close => &s.box.close.c,
            .p_send => &s.box.send.c,
            .p_recv => &s.box.recv.c,
            .p_close => &s.box.close.c,
            .p_shutdown => &s.box.shutdown.c,
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
            .p_close => if (s.box.close.getResult()) |_| .ok else |e| .{ .err = e },
            .p_shutdown => if (s.box.shutdown.getResult()) |_| .ok else |e| .{ .err = e },
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

const MWire = struct { due: u64, from_port: u16, to_port: u16, bytes: []const u8, to_gen: u32, delayed: bool };

/// One pipe end: `buf` holds the bytes sent TO it, not yet received. The
/// flags are the sim's (`sim.End`), kept by the model from the steps.
const MEnd = struct {
    fd: i32 = -1,
    buf: std.ArrayList(u8) = .empty,
    waiting_recv: ?*Slot = null,
    waiting_send: ?*Slot = null,
    inflight: u32 = 0,
    /// Bytes this end's sends copied (the sim's `sent_bytes`).
    accepted: u64 = 0,
    received: u64 = 0,
    closed: bool = false,
    rd_shut: bool = false,
    wr_shut: bool = false,
    reset: bool = false,
    /// `sim.setSendFaults(.fail_at_byte)`: the sent-byte count at which a
    /// send resets the pipe.
    fail_at: ?u64 = null,
    /// Bytes dropped from `buf` by a reset.
    dropped: u64 = 0,
};

const TimerSlot = struct {
    used: bool = false,
    t: ev.Timer = undefined,
    deadline: u64 = 0,
    completions: u32 = 0,
    cancel_req: bool = false,
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
    pipe_close,
    pipe_shutdown,
    pipe_reset,
    timer,
    fault,
};

const step_weights = [_]struct { StepKind, u32 }{
    .{ .ev_send, 12 },   .{ .layer_send, 5 }, .{ .ev_recv, 12 },   .{ .layer_recv, 4 },
    .{ .layer_poll, 3 }, .{ .ev_poll, 5 },    .{ .pipe_send, 9 },  .{ .pipe_recv, 9 },
    .{ .cancel, 6 },     .{ .group, 9 },      .{ .close, 3 },      .{ .open, 4 },
    .{ .rcvbuf, 3 },     .{ .clock, 22 },     .{ .pipe_close, 1 }, .{ .pipe_shutdown, 1 },
    .{ .pipe_reset, 1 }, .{ .timer, 3 },      .{ .fault, 4 },
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
    /// Ops that waited, were woken, and completed (the wake obligation met).
    woken_completed: u64 = 0,
    /// Inside a poll, from a callback that does not defer (or a timer's):
    /// a socket closed and reopened; a layer receive that took the datagram
    /// a woken op was due to take (that op parks again); a cancel.
    in_batch_reopen: u64 = 0,
    in_batch_steal: u64 = 0,
    in_batch_cancel: u64 = 0,
    timers_fired: u64 = 0,
    /// A timer fired at the virtual time an op completed, in one poll.
    timer_and_io_at_one_time: u64 = 0,
    pipe_pairs: u64 = 0,
    pipe_closes: u64 = 0,
    pipe_shutdowns: u64 = 0,
    pipe_resets: u64 = 0,
    end_of_stream: u64 = 0,
    broken_pipe: u64 = 0,
    reset_errors: u64 = 0,
    /// Ops still waiting at the end that completed once given what they
    /// waited for: proof they were still registered.
    registration_proofs: u64 = 0,
    /// Race groups of waiting receives submitted at the end.
    end_groups: u64 = 0,
    /// Fault rules set and removed; datagrams the rules
    /// dropped, by cause, and copies made, delayed, clamped and overtaking;
    /// a waiting op woken by a delayed landing; removals whose returned
    /// counts the model checked.
    fault_sets: u64 = 0,
    fault_removals: u64 = 0,
    lost: u64 = 0,
    lost_burst: u64 = 0,
    partitioned_at_send: u64 = 0,
    partitioned_in_flight: u64 = 0,
    silenced_at_send: u64 = 0,
    silenced_in_flight: u64 = 0,
    duplicated: u64 = 0,
    delayed: u64 = 0,
    clamped: u64 = 0,
    overtaking: u64 = 0,
    delayed_wakes: u64 = 0,

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

/// A refusal probe (`ZIO_OP_MODEL_PROBE`): at its first chance the child
/// submits a second ev op behind a woken one, which the sim must refuse.
const Probe = enum { none, recv, send, dgram };

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
    var ends: [2 * max_pairs]MEnd = undefined;
    /// The current pipe pair: ends `2 * pair` and `2 * pair + 1`.
    var pair: u8 = 0;
    var pairs: u8 = 0;
    var timers: [n_timers]TimerSlot = undefined;
    var last_io_clock: u64 = std.math.maxInt(u64);
    var last_io_poll: u64 = 0;
    var last_timer_clock: u64 = std.math.maxInt(u64);
    var last_timer_poll: u64 = 0;
    var probe: Probe = .none;
    var in_proof: bool = false;
    var proof_done: bool = false;
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
    var frules: [max_frules]FRule = undefined;
    var frules_len: usize = 0;
    /// Distinct rule keys set in this run (the sim records at most 256).
    var fkeys: std.ArrayList(FKey) = .empty;
    var fstats: sim.NetFaultStats = .{};
    var fdraws: u64 = 0;
    /// The sim's send number of the next datagram.
    var send_no: u64 = 0;

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

// ---- the model's fault rules ----
//
// The test's own copy of the fault protocol above `pub const Endpoint` in
// sim.zig and of the draw formula in
// `sim.faultDraw`'s doc comment. It uses the API's types (`sim.Endpoint`,
// `sim.Impairment`) and nothing of the sim's fault code.

const max_frules = 6;

const FKind = enum { impairment, partition, silence };

/// A rule's identity. A silence keeps its endpoint in `from`.
const FKey = struct { kind: FKind, from: sim.Endpoint, to: sim.Endpoint };

const FRule = struct {
    key: FKey,
    imp: sim.Impairment = .{},
    bad: bool = false,
    stats: sim.RuleStats = .{},
};

fn fmix(x: u64) u64 {
    var z = x;
    z = (z ^ (z >> 30)) *% 0xbf58476d1ce4e5b9;
    z = (z ^ (z >> 27)) *% 0x94d049bb133111eb;
    return z ^ (z >> 31);
}

fn fdraw(n: u64, purpose: u64, draws: *u64) u64 {
    draws.* += 1;
    return fmix(fmix(M.seed ^ 0x6e65745f6661756c) +% (n *% 8 +% purpose) *% 0x9e3779b97f4a7c15);
}

fn fhit(ppm: u32, n: u64, purpose: u64, draws: *u64) bool {
    if (ppm == 0) return false;
    if (ppm >= 1_000_000) return true;
    return fdraw(n, purpose, draws) % 1_000_000 < ppm;
}

/// Every socket here is bound to 127.0.0.1.
fn epMatches(e: sim.Endpoint, port: u16) bool {
    if (e.ip) |ip| {
        if (!std.mem.eql(u8, &ip, &[4]u8{ 127, 0, 0, 1 })) return false;
    }
    if (e.port) |p| {
        if (p != port) return false;
    }
    return true;
}

fn ruleMatches(k: FKey, from: u16, to: u16) bool {
    return switch (k.kind) {
        .impairment, .partition => epMatches(k.from, from) and epMatches(k.to, to),
        .silence => epMatches(k.from, from) or epMatches(k.from, to),
    };
}

fn lastRule(kind: FKind, from: u16, to: u16) ?usize {
    var i = M.frules_len;
    while (i > 0) {
        i -= 1;
        if (M.frules[i].key.kind == kind and ruleMatches(M.frules[i].key, from, to)) return i;
    }
    return null;
}

const FDrop = enum { lost, lost_burst, partitioned, silenced };

const Fate = struct {
    drop: ?FDrop = null,
    copies: u8 = 1,
    due: [2]u64 = .{ 0, 0 },
    draws: u64 = 0,
};

/// What the sim will do with the next datagram from `from` to `to` sent
/// now. With `commit`, the model's counts, burst states and draws move as
/// the sim's do; without, nothing in the model changes (a generator's
/// look ahead).
fn faultFate(from: u16, to: u16, commit: bool) Fate {
    var f: Fate = .{};
    const n = M.send_no;
    if (commit) {
        for (M.frules[0..M.frules_len]) |*r| {
            if (ruleMatches(r.key, from, to)) r.stats.matched += 1;
        }
    }
    if (lastRule(.silence, from, to)) |i| {
        if (commit) {
            M.frules[i].stats.dropped += 1;
            M.fstats.silenced_at_send += 1;
        }
        f.drop = .silenced;
        return f;
    }
    if (lastRule(.partition, from, to)) |i| {
        if (commit) {
            M.frules[i].stats.dropped += 1;
            M.fstats.partitioned_at_send += 1;
        }
        f.drop = .partitioned;
        return f;
    }
    const ai = lastRule(.impairment, from, to);
    const imp: sim.Impairment = if (ai) |i| M.frules[i].imp else .{};
    if (imp.burst) |b| {
        var bad = M.frules[ai.?].bad;
        if (bad) {
            if (fhit(b.leave_ppm, n, 1, &f.draws)) bad = false;
        } else {
            if (fhit(b.enter_ppm, n, 1, &f.draws)) bad = true;
        }
        if (commit) M.frules[ai.?].bad = bad;
        if (bad) {
            if (commit) {
                M.frules[ai.?].stats.dropped += 1;
                M.fstats.lost_burst += 1;
            }
            f.drop = .lost_burst;
            return f;
        }
    }
    if (fhit(imp.loss_ppm, n, 2, &f.draws)) {
        if (commit) {
            M.frules[ai.?].stats.dropped += 1;
            M.fstats.lost += 1;
        }
        f.drop = .lost;
        return f;
    }
    if (fhit(imp.dup_ppm, n, 3, &f.draws)) {
        f.copies = 2;
        if (commit) {
            M.frules[ai.?].stats.duplicated += 1;
            M.fstats.duplicated += 1;
        }
    }
    const now = sim.clockNs();
    const base = now + sim.wire_latency_ns;
    // The flow's latest datagram still on the wire: the sim's wire holds
    // only datagrams due after now.
    var latest: ?u64 = null;
    for (M.wire.items) |w| {
        if (w.due > now and w.from_port == from and w.to_port == to) latest = @max(latest orelse w.due, w.due);
    }
    for (0..f.copies) |c| {
        var at = base + imp.delay_ns;
        if (imp.jitter_ns != 0) at += fdraw(n, 4 + c, &f.draws) % (imp.jitter_ns + 1);
        if (latest) |l| {
            if (at < l) {
                if (imp.reorder) {
                    if (commit) M.fstats.overtaking += 1;
                } else {
                    at = l;
                    if (commit) M.c.clamped += 1;
                }
            }
        }
        if (at > base and commit) {
            M.fstats.delayed += 1;
            if (ai) |i| M.frules[i].stats.delayed += 1;
        }
        f.due[c] = at;
        latest = @max(latest orelse at, at);
    }
    return f;
}

/// The drop rules at a landing; true: dropped.
fn faultInFlight(w: MWire) bool {
    for (M.frules[0..M.frules_len]) |*r| {
        if (r.key.kind != .impairment and ruleMatches(r.key, w.from_port, w.to_port)) r.stats.matched += 1;
    }
    if (lastRule(.silence, w.from_port, w.to_port)) |i| {
        M.frules[i].stats.dropped += 1;
        M.fstats.silenced_in_flight += 1;
        return true;
    }
    if (lastRule(.partition, w.from_port, w.to_port)) |i| {
        M.frules[i].stats.dropped += 1;
        M.fstats.partitioned_in_flight += 1;
        return true;
    }
    return false;
}

fn findFRule(k: FKey) ?usize {
    for (M.frules[0..M.frules_len], 0..) |r, i| {
        if (std.meta.eql(r.key, k)) return i;
    }
    return null;
}

fn removeFRule(i: usize) sim.RuleStats {
    const st = M.frules[i].stats;
    std.mem.copyForwards(FRule, M.frules[i .. M.frules_len - 1], M.frules[i + 1 .. M.frules_len]);
    M.frules_len -= 1;
    return st;
}

/// Room for one more rule key in the sim's record of the run (256).
fn keyRoom(k: FKey) bool {
    for (M.fkeys.items) |x| {
        if (std.meta.eql(x, k)) return true;
    }
    if (M.fkeys.items.len == sim.max_rule_history) return false;
    M.fkeys.append(M.gpa(), k) catch @panic("oom");
    return true;
}

fn randomEndpoint(allow_ip: bool) sim.Endpoint {
    const r = M.r();
    return switch (r.uintLessThan(u8, 6)) {
        0, 1 => .{},
        // 127.0.0.2 holds no socket here: such a rule must match nothing.
        2 => if (allow_ip) .{ .ip = .{ 127, 0, 0, 1 + r.uintLessThan(u8, 2) }, .port = base_port + r.uintLessThan(u16, 2 * n_socks) } else .{},
        else => .{ .port = base_port + r.uintLessThan(u16, 2 * n_socks) },
    };
}

fn randomImpairment() sim.Impairment {
    const r = M.r();
    const ppm = [_]u32{ 0, 0, 0, 100_000, 400_000, 1_000_000 };
    var imp: sim.Impairment = .{
        .loss_ppm = ppm[r.uintLessThan(usize, ppm.len)],
        .delay_ns = ([_]u64{ 0, 0, 50_000, 1_000_000, 3_000_000 })[r.uintLessThan(usize, 5)],
        .jitter_ns = ([_]u64{ 0, 0, 200_000, 2_000_000 })[r.uintLessThan(usize, 4)],
        .reorder = r.boolean(),
        .dup_ppm = ppm[r.uintLessThan(usize, ppm.len)],
    };
    if (r.uintLessThan(u8, 4) == 0) imp.burst = .{
        .enter_ppm = ([_]u32{ 50_000, 300_000, 1_000_000 })[r.uintLessThan(usize, 3)],
        .leave_ppm = ([_]u32{ 100_000, 500_000, 1_000_000 })[r.uintLessThan(usize, 3)],
    };
    if (std.meta.eql(imp, sim.Impairment{})) imp.delay_ns = 1_000_000;
    return imp;
}

fn checkRemoved(what: []const u8, got: sim.RuleStats, want: sim.RuleStats) void {
    if (!std.meta.eql(got, want)) fail("{s}: the sim's rule counts {any}, the model's {any}", .{ what, got, want });
    M.c.fault_removals += 1;
}

/// A fault step: set or remove one rule, between polls. Everything due by
/// now has landed in both the sim and the model first.
fn faultStep() bool {
    syncLand();
    const r = M.r();
    const choice = r.uintLessThan(u8, 12);
    if (choice < 6) {
        if (M.frules_len == max_frules) return false;
        var k: FKey = switch (choice) {
            0, 1, 2 => .{ .kind = .impairment, .from = randomEndpoint(false), .to = randomEndpoint(true) },
            3, 4 => .{ .kind = .partition, .from = randomEndpoint(false), .to = randomEndpoint(true) },
            else => .{ .kind = .silence, .from = randomEndpoint(true), .to = .{} },
        };
        // A silence of every endpoint would stop all traffic until removed.
        if (k.kind == .silence and k.from.port == null) k.from.port = base_port;
        if (k.kind != .impairment and findFRule(k) != null) return false;
        if (!keyRoom(k)) return false;
        const path: sim.Path = .{ .from = k.from, .to = k.to };
        switch (k.kind) {
            .impairment => {
                const imp = randomImpairment();
                sim.setImpairment(path, imp);
                if (findFRule(k)) |i| _ = removeFRule(i);
                M.frules[M.frules_len] = .{ .key = k, .imp = imp };
            },
            .partition => {
                sim.setPartition(path);
                M.frules[M.frules_len] = .{ .key = k };
            },
            .silence => {
                sim.silence(k.from);
                M.frules[M.frules_len] = .{ .key = k };
            },
        }
        M.frules_len += 1;
        M.c.fault_sets += 1;
        return true;
    }
    if (M.frules_len == 0) return false;
    removeRandomFRule(r.uintLessThan(usize, M.frules_len));
    return true;
}

fn removeRandomFRule(i: usize) void {
    const k = M.frules[i].key;
    const want = removeFRule(i);
    const path: sim.Path = .{ .from = k.from, .to = k.to };
    switch (k.kind) {
        .impairment => checkRemoved("clearImpairment", sim.clearImpairment(path), want),
        .partition => checkRemoved("healPartition", sim.healPartition(path), want),
        .silence => checkRemoved("unsilence", sim.unsilence(k.from), want),
    }
}

// ---- the model's datagram wire ----

/// Lands every model datagram due by the sim's clock. The sim lands them at
/// the start of each harvest, and the clock only moves inside a poll just
/// before one, so between polls and inside callbacks both have landed the
/// same datagrams.
fn syncLand() void {
    const now = sim.clockNs();
    var n: usize = 0;
    while (n < M.wire.items.len and M.wire.items[n].due <= now) : (n += 1) {
        if (M.frules_len != 0 and faultInFlight(M.wire.items[n])) continue;
        landOne(M.wire.items[n]);
    }
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
        if (w.delayed and s.waiting != null) M.c.delayed_wakes += 1;
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

/// Whether a datagram from `from_port` to `to_port` sent now would share a
/// landing instant with another flow to the same port: at the instants the
/// model predicts for its copies, faults included.
fn flowClash(from_port: u16, to_port: u16) bool {
    const f = faultFate(from_port, to_port, false);
    if (f.drop != null) return false;
    for (f.due[0..f.copies]) |due| {
        for (M.wire.items) |w| {
            if (w.due == due and w.to_port == to_port and w.from_port != from_port) return true;
        }
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
    M.c.dgrams_sent += 1;
    if (bytes.len == 0) M.c.empty_dgrams += 1;
    const f = faultFate(from.port, to_port, true);
    M.fdraws += f.draws;
    M.send_no += 1;
    if (f.drop != null) return;
    const base = sim.clockNs() + sim.wire_latency_ns;
    for (f.due[0..f.copies]) |due| {
        // After every datagram due no later, as the sim's wire.
        var pos = M.wire.items.len;
        while (pos > 0 and M.wire.items[pos - 1].due > due) pos -= 1;
        M.wire.insert(M.gpa(), pos, .{ .due = due, .from_port = from.port, .to_port = to_port, .bytes = bytes, .to_gen = to_gen, .delayed = due > base }) catch @panic("oom");
    }
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

// ---- the model's pipes ----
//
// The sim's rules (`sim.copySend`, `recvInto`, `closeFd`, `shutdownFd`,
// `resetPipe`) written as the model's own, from the design and the sim's
// doc comments: end `e` reads `ends[e].buf`; its peer is `e ^ 1`.

/// End of stream for a read on end `e`: nothing buffered, and either side
/// closed, this side shut its reads, or the peer shut its writes.
fn pipeEof(e: u8) bool {
    const E = &M.ends[e];
    const P = &M.ends[e ^ 1];
    return E.buf.items.len == 0 and (E.closed or E.rd_shut or P.closed or P.wr_shut);
}

/// What an op waiting on it waits for has happened: from now it has
/// `pipe_wake_ns` to complete.
fn wakeOp(w: ?*Slot) void {
    const op = w orelse return;
    if (op.wake_since == null) op.wake_since = sim.clockNs();
}

/// A reset (an injected write error): both ends fail every later op, unread
/// bytes are dropped, and ops waiting on either end wake.
fn resetPair(e: u8) void {
    M.c.pipe_resets += 1;
    for ([2]u8{ e, e ^ 1 }) |x| {
        const X = &M.ends[x];
        X.reset = true;
        X.dropped += X.buf.items.len;
        X.buf.clearRetainingCapacity();
        wakeOp(X.waiting_recv);
        wakeOp(X.waiting_send);
    }
}

/// A send on end `e` now: its result and its effect. Null: it waits for
/// room. At a harvest (`waited`), no room is a broken sim rule.
fn pipeSendNow(e: u8, s: *Slot, waited: bool) ?Res {
    const E = &M.ends[e];
    const P = &M.ends[e ^ 1];
    if (E.reset) return .{ .err = error.ConnectionResetByPeer };
    if (E.closed or E.wr_shut or P.closed) return .{ .err = error.BrokenPipe };
    if (s.total == 0) return .{ .n = 0 };
    var limit = s.total;
    if (E.fail_at) |at| {
        const room = at -| E.accepted;
        if (room == 0) {
            E.fail_at = null;
            resetPair(e);
            return .{ .err = error.ConnectionResetByPeer };
        }
        limit = @min(limit, room);
    }
    const space = sim.pipe_buf_cap - P.buf.items.len;
    if (space == 0) {
        if (waited) fail("p_send op #{d} (waited) completed into a full pipe", .{s.serial});
        return null;
    }
    const n = @min(limit, space);
    P.buf.appendSlice(M.gpa(), gathered(s, n)) catch @panic("oom");
    E.accepted += n;
    wakeOp(P.waiting_recv);
    return .{ .n = n };
}

/// A receive on end `e` now: its result (and `s.want`, the bytes it takes).
/// Null: it waits. At a harvest (`waited`), nothing to read and no end of
/// stream is a broken sim rule: only an ev receive takes bytes.
fn pipeRecvNow(e: u8, s: *Slot, waited: bool) ?Res {
    const E = &M.ends[e];
    if (E.reset) return .{ .err = error.ConnectionResetByPeer };
    const level = E.buf.items.len;
    if (level == 0) {
        if (pipeEof(e)) return .{ .n = 0 };
        if (waited) fail("p_recv op #{d} (waited) completed on an open pipe with nothing buffered: a kernel reader keeps waiting", .{s.serial});
        if (s.dontwait) return .{ .err = error.WouldBlock };
        return null;
    }
    const n = @min(s.room(), level);
    s.want = M.gpa().dupe(u8, E.buf.items[0..n]) catch @panic("oom");
    E.buf.replaceRange(M.gpa(), 0, n, &.{}) catch @panic("oom");
    E.received += n;
    if (n > 0) wakeOp(M.ends[e ^ 1].waiting_send);
    return .{ .n = n };
}

fn countPipeResult(r: Res) void {
    switch (r) {
        .n => |n| if (n == 0) {
            M.c.end_of_stream += 1;
        },
        .err => |e| switch (e) {
            error.BrokenPipe => M.c.broken_pipe += 1,
            error.ConnectionResetByPeer => M.c.reset_errors += 1,
            else => {},
        },
        .ok => {},
    }
}

/// Whether ops may be submitted on end `e`: a closed end's number is not
/// the program's to use any more.
fn endUsable(e: u8) bool {
    return M.ends[e].fd != -1 and !M.ends[e].closed;
}

/// A new pipe pair becomes the current one; ops on the old pair go on.
fn newPair() bool {
    if (M.pairs == max_pairs) return false;
    const p = sim.pipePair();
    const k = M.pairs;
    M.ends[2 * k] = .{ .fd = p[0] };
    M.ends[2 * k + 1] = .{ .fd = p[1] };
    M.pair = k;
    M.pairs += 1;
    M.c.pipe_pairs += 1;
    return true;
}

/// The current pair, replaced first when it is spent (both ends closed, or
/// reset) and a new one can be made.
fn currentPair() u8 {
    const a = 2 * M.pair;
    const spent = (M.ends[a].closed and M.ends[a + 1].closed) or M.ends[a].reset;
    if (spent) _ = newPair();
    return M.pair;
}

fn buildPipeSend(i: u8, e: u8) ?*Slot {
    // One unresolved ev send per pipe end (`sim.refuseSecondPipeWaiter`).
    if (!endUsable(e) or M.ends[e].waiting_send != null) return null;
    const s = initSlot(i, .p_send);
    s.fd = M.ends[e].fd;
    s.target = e;
    randomParts(s, pipe_part_max, true);
    if (pipeSendNow(e, s, false)) |res| {
        s.at_submit = res;
        countPipeResult(res);
        s.must_by_clock = sim.clockNs() + 2 * pipe_wake_ns;
    } else {
        M.ends[e].waiting_send = s;
        M.c.parked += 1;
        M.c.pipe_parked_sends += 1;
    }
    s.box = .{ .send = ev.NetSend.init(s.fd, .{ .iovecs = s.iovc[0..s.n_iov] }, .{}) };
    return s;
}

fn buildPipeRecv(i: u8, e: u8, allow_dontwait: bool) ?*Slot {
    // One unresolved ev receive per pipe end.
    if (!endUsable(e) or M.ends[e].waiting_recv != null) return null;
    const s = initSlot(i, .p_recv);
    s.fd = M.ends[e].fd;
    s.target = e;
    s.dontwait = allow_dontwait and M.r().uintLessThan(u8, 5) == 0;
    randomBufs(s, true);
    if (pipeRecvNow(e, s, false)) |res| {
        s.at_submit = res;
        countPipeResult(res);
        if (res == .err and res.err == error.WouldBlock) M.c.would_block += 1;
        s.must_by_clock = sim.clockNs() + 2 * pipe_wake_ns;
    } else {
        M.ends[e].waiting_recv = s;
        M.c.parked += 1;
    }
    s.box = .{ .recv = ev.NetRecv.init(s.fd, .{ .iovecs = s.iov[0..s.n_iov] }, .{ .dontwait = s.dontwait }) };
    return s;
}

/// An ev close of end `e`: the end is closed at submit, and ops waiting on
/// either end wake (a receive to its bytes or end of stream, a send to
/// BrokenPipe). Its own result comes after the seeded delay.
fn buildPipeClose(i: u8, e: u8) ?*Slot {
    if (!endUsable(e)) return null;
    const s = initSlot(i, .p_close);
    s.fd = M.ends[e].fd;
    s.target = e;
    s.box = .{ .close = ev.NetClose.init(s.fd) };
    s.at_submit = .ok;
    s.must_by_clock = sim.clockNs() + 2 * pipe_wake_ns;
    M.ends[e].closed = true;
    for ([2]u8{ e, e ^ 1 }) |x| {
        wakeOp(M.ends[x].waiting_recv);
        wakeOp(M.ends[x].waiting_send);
    }
    M.c.pipe_closes += 1;
    return s;
}

/// An ev shutdown of end `e`: `.receive` makes its reads end of stream once
/// empty; `.send` fails its sends and makes the peer's reads end of stream
/// once empty. It wakes the ops that now end.
fn buildPipeShutdown(i: u8, e: u8, how: os_net.ShutdownHow) ?*Slot {
    if (!endUsable(e)) return null;
    const s = initSlot(i, .p_shutdown);
    s.fd = M.ends[e].fd;
    s.target = e;
    s.box = .{ .shutdown = ev.NetShutdown.init(s.fd, how) };
    s.at_submit = .ok;
    s.must_by_clock = sim.clockNs() + 2 * pipe_wake_ns;
    const E = &M.ends[e];
    if (how != .send) {
        E.rd_shut = true;
        wakeOp(E.waiting_recv);
    }
    if (how != .receive) {
        E.wr_shut = true;
        wakeOp(E.waiting_send);
        wakeOp(M.ends[e ^ 1].waiting_recv);
    }
    M.c.pipe_shutdowns += 1;
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
        .p_send, .p_recv, .p_close, .p_shutdown => M.ends[s.target].inflight += 1,
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
            const a = 2 * currentPair();
            if (idle_only and (M.ends[a].inflight != 0 or M.ends[a + 1].inflight != 0)) return null;
            const first: u8 = a + @as(u8, @intCast(r.uintLessThan(u8, 2)));
            for ([2]u8{ first, first ^ 1 }) |e| {
                const s = if (kind == .p_send) buildPipeSend(i, e) else buildPipeRecv(i, e, !idle_only);
                if (s) |op| return op;
            }
            return null;
        },
        .d_close, .p_close, .p_shutdown => unreachable,
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
        if (s.wake_since != null) M.c.woken_completed += 1;
    } else if (got == .err and got.err == error.Canceled) {
        if (s.proving) fail("{s} op #{d} ended Canceled while it alone was given what it waits for: a cancel would hide a lost registration", .{ what, s.serial });
        if (!s.cancel_req) fail("{s} op #{d} ended Canceled, but no cancel was asked for it", .{ what, s.serial });
        clearWaiting(s);
    } else if (s.woken_closed) {
        if (!(got == .err and got.err == error.FileDescriptorNotASocket)) fail("{s} op #{d} was waiting when its socket closed: result {any}, wanted FileDescriptorNotASocket", .{ what, s.serial, got });
        if (sockFdOpen(s.fd)) M.c.closed_then_reused += 1;
    } else {
        harvestAction(s, got);
        M.c.woken_completed += 1;
    }
    if (s.proving) M.proof_done = true;
    finishOp(s);
    noteIoCompletion();
    if (!s.deferred) inBatch(s);
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
        .p_close, .p_shutdown => {},
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
            if (s.wake_since == null) fail("p_recv op #{d} completed {any} though nothing woke it", .{ s.serial, got });
            const want = pipeRecvNow(s.target, s, true).?;
            if (!Res.eql(want, got)) fail("p_recv op #{d} (waited): result {any}, the model's {any}", .{ s.serial, got, want });
            if (want == .n) checkScatter(s, s.want, what);
            countPipeResult(want);
        },
        .p_send => {
            if (s.wake_since == null) fail("p_send op #{d} completed {any} though nothing woke it", .{ s.serial, got });
            const want = pipeSendNow(s.target, s, true).?;
            if (!Res.eql(want, got)) fail("p_send op #{d} (waited): result {any}, the model's {any}", .{ s.serial, got, want });
            countPipeResult(want);
        },
        .d_send, .d_poll_send, .d_close, .p_close, .p_shutdown => fail("{s} op #{d} waited, but it never waits", .{ what, s.serial }),
    }
}

fn finishOp(s: *Slot) void {
    s.done_poll = M.polls;
    switch (s.kind) {
        .p_send, .p_recv, .p_close, .p_shutdown => M.ends[s.target].inflight -= 1,
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
    if (M.in_poll and !M.in_add and !M.in_cancel and !M.in_proof) followUps(freed[0..n]);
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
    for (step_weights) |w| total += weight(w);
    var x = M.r().uintLessThan(u32, total);
    for (step_weights) |w| {
        if (x < weight(w)) return w[0];
        x -= weight(w);
    }
    unreachable;
}

fn weight(w: @TypeOf(step_weights[0])) u32 {
    return if (w[0] == .fault and !faultsOn()) 0 else w[1];
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
        .layer_recv => layerRecv(openSock(false) orelse return false),
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
            if (r.uintLessThan(u8, 5) == 0) {
                for (&M.timers) |*t| {
                    if (!t.used or t.completions != 0) continue;
                    t.cancel_req = true;
                    M.in_cancel = true;
                    M.loop.cancel(&t.t.c);
                    M.in_cancel = false;
                    return true;
                }
            }
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
        .fault => return faultStep(),
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
        .pipe_close, .pipe_shutdown => {
            // Rare: a pair spent by closes or a reset is not reused, and a
            // run has 15.
            if (r.uintLessThan(u8, 3) != 0) return false;
            const i = freeSlot() orelse return false;
            const e = 2 * currentPair() + @as(u8, @intCast(r.uintLessThan(u8, 2)));
            const s = (if (kind == .pipe_close) buildPipeClose(i, e) else buildPipeShutdown(i, e, switch (r.uintLessThan(u8, 3)) {
                0 => .receive,
                1 => .send,
                else => .both,
            })) orelse return false;
            arm(s, r.boolean());
            addToLoop(s.comp());
        },
        .pipe_reset => {
            if (r.uintLessThan(u8, 3) != 0) return false;
            const e = 2 * currentPair() + @as(u8, @intCast(r.uintLessThan(u8, 2)));
            const E = &M.ends[e];
            if (!endUsable(e) or E.reset or E.fail_at != null) return false;
            const at = E.accepted + r.uintAtMost(u64, 3000);
            sim.setSendFaults(E.fd, .{ .fail_at_byte = at });
            E.fail_at = at;
        },
        .timer => return armTimer(),
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
    var send_used = false;
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
        if (kind == .d_send and send_used and faultsOn()) continue;
        // Pick a target fd not used by another member.
        const s: ?*Slot = blk: {
            if (is_pipe) {
                const e: u8 = 2 * currentPair() + @as(u8, @intCast(r.uintLessThan(u8, 2)));
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
        if (kind == .d_send) send_used = true;
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

/// A layer receive (`recvDatagram`, `dontwait`) on socket `k`, checked
/// against the model's queue.
fn layerRecv(k: u8) void {
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
        return;
    }
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

/// A socket on which a callback inside a poll may act: its waiting op, if
/// any, does not defer its callback, so that op has not been harvested yet
/// (its callback would have run): the model's queue is the sim's.
fn sockSafeInPoll() ?u8 {
    const start = M.r().uintLessThan(u8, n_socks);
    for (0..n_socks) |off| {
        const k: u8 = @intCast((start + off) % n_socks);
        const sk = &M.socks[k];
        if (!sk.open) continue;
        if (sk.waiting) |w| {
            if (w.deferred) continue;
        }
        return k;
    }
    return null;
}

/// A socket whose waiting op does not defer, has not been called back, and
/// has a datagram queued: woken in this batch, later in it.
fn wokenSock() ?u8 {
    for (&M.socks, 0..) |*sk, k| {
        const w = sk.waiting orelse continue;
        if (!w.deferred and w.completions == 0 and sk.open and sk.gen == w.gen and sk.qlen() > 0) return @intCast(k);
    }
    return null;
}

/// Inside a poll, from a callback that does not defer (an op's or a
/// timer's), sometimes act as a program's callback can: close a socket and
/// open it again (its number is reused), take a datagram with a layer
/// receive (a woken receive on that socket then parks again), or cancel an
/// op. Each can hit an op later in the same harvest batch.
fn inBatch(from: ?*Slot) void {
    if (!M.in_poll or M.in_add or M.in_cancel or M.in_proof) return;
    if (M.probe == .dgram) probeDgram();
    const r = M.r();
    // A woken op later in this batch is rare; take its datagram often.
    if (wokenSock()) |k| {
        if (r.boolean()) {
            M.c.in_batch_steal += 1;
            layerRecv(k);
            return;
        }
    }
    if (r.uintLessThan(u8, 6) != 0) return;
    switch (r.uintLessThan(u8, 3)) {
        0 => {
            const k = sockSafeInPoll() orelse return;
            const fd = M.socks[k].fd;
            closeModel(k);
            os_net.close(fd);
            openModel(k);
            M.c.in_batch_reopen += 1;
        },
        1 => {
            // Prefer a socket whose op is woken in this batch and not yet
            // harvested: the layer receive takes its datagram, and it must
            // park again (the datagram repark).
            const k = wokenSock() orelse sockSafeInPoll() orelse return;
            const sk = &M.socks[k];
            if (sk.waiting != null and sk.qlen() > 0) M.c.in_batch_steal += 1;
            layerRecv(k);
        },
        else => {
            var cand: [n_slots]u8 = undefined;
            var n: usize = 0;
            for (&M.slots, 0..) |*o, i| {
                if (o.used and o.completions == 0 and o != from) {
                    cand[n] = @intCast(i);
                    n += 1;
                }
            }
            if (n == 0) return;
            const o = &M.slots[cand[r.uintLessThan(usize, n)]];
            o.cancel_req = true;
            M.in_cancel = true;
            M.loop.cancel(o.comp());
            M.in_cancel = false;
            M.c.in_batch_cancel += 1;
        },
    }
}

// ---- refusal probes: a second ev op behind a woken one ----

fn probeSubmit(s: *Slot, what: []const u8, behind: *Slot) noreturn {
    std.debug.print("op model probe: seed {d} step {d}: a second ev {s} on fd {d} behind op #{d}, which is woken and not yet harvested\n", .{ M.seed, M.step, what, s.fd, behind.serial });
    arm(s, false);
    addToLoop(s.comp());
    fail("probe: the sim accepted a second ev {s} behind a woken one", .{what});
}

/// Before a step: a pipe op that waits on the current pair and was woken
/// (its callback has not run, so it is not harvested).
fn probePipe() void {
    const a = 2 * M.pair;
    for ([2]u8{ a, a + 1 }) |e| {
        const E = &M.ends[e];
        const w = (if (M.probe == .recv) E.waiting_recv else E.waiting_send) orelse continue;
        if (w.wake_since == null or w.completions != 0) continue;
        const i = freeSlot() orelse return;
        const s = initSlot(i, if (M.probe == .recv) .p_recv else .p_send);
        s.fd = E.fd;
        s.target = e;
        if (M.probe == .recv) {
            randomBufs(s, true);
            s.box = .{ .recv = ev.NetRecv.init(s.fd, .{ .iovecs = s.iov[0..s.n_iov] }, .{}) };
            probeSubmit(s, "receive", w);
        }
        randomParts(s, pipe_part_max, true);
        s.box = .{ .send = ev.NetSend.init(s.fd, .{ .iovecs = s.iovc[0..s.n_iov] }, .{}) };
        probeSubmit(s, "send", w);
    }
}

/// Inside a harvest: a socket whose receive or poll waits, does not defer,
/// has not been called back, and whose datagram has landed: it is woken and
/// later in this batch.
fn probeDgram() void {
    for (&M.socks) |*sk| {
        const w = sk.waiting orelse continue;
        if (w.deferred or w.completions != 0 or !sk.open or sk.gen != w.gen or sk.qlen() == 0) continue;
        const i = freeSlot() orelse return;
        const s = initSlot(i, .d_recv);
        s.fd = sk.fd;
        s.target = @intCast((@intFromPtr(sk) - @intFromPtr(&M.socks[0])) / @sizeOf(MSock));
        s.gen = sk.gen;
        randomBufs(s, false);
        s.addr_len = @sizeOf(os_net.sockaddr.in);
        s.box = .{ .recvfrom = ev.NetRecvFrom.init(s.fd, .{ .iovecs = s.iov[0..s.n_iov] }, .{}, @ptrCast(&s.addr), &s.addr_len) };
        probeSubmit(s, "receive", w);
    }
}

fn noteIoCompletion() void {
    if (!M.in_poll) return;
    const now = sim.clockNs();
    if (M.last_timer_clock == now and M.last_timer_poll == M.polls) M.c.timer_and_io_at_one_time += 1;
    M.last_io_clock = now;
    M.last_io_poll = M.polls;
}

fn onTimer(_: *ev.Loop, cp: *ev.Completion) void {
    const t: *TimerSlot = @ptrCast(@alignCast(cp.userdata.?));
    syncLand();
    t.completions += 1;
    if (t.completions > 1) fail("a timer completed {d} times", .{t.completions});
    const now = sim.clockNs();
    if (t.t.getResult()) |_| {
        if (now != t.deadline) fail("a timer due at {d} ns fired at {d} ns", .{ t.deadline, now });
        M.c.timers_fired += 1;
        if (M.last_io_clock == now and M.last_io_poll == M.polls) M.c.timer_and_io_at_one_time += 1;
        M.last_timer_clock = now;
        M.last_timer_poll = M.polls;
    } else |e| {
        if (e != error.Canceled or !t.cancel_req) fail("a timer ended {t}", .{e});
    }
    t.used = false;
    inBatch(null);
}

/// A timer due when a datagram lands (the wire latency), when a pipe op is
/// harvested (1 or 5 ms), or at another time; its callback does not defer,
/// and may act as `inBatch` does.
fn armTimer() bool {
    for (&M.timers) |*t| {
        if (t.used) continue;
        const offs = [_]u64{ sim.wire_latency_ns, std.time.ns_per_ms, 5 * std.time.ns_per_ms, 200_000 };
        const d = offs[M.r().uintLessThan(usize, offs.len)];
        t.* = .{ .used = true, .deadline = sim.clockNs() + d };
        t.t = ev.Timer.init(.{ .duration = .fromNanoseconds(d) });
        t.t.c.callback = onTimer;
        t.t.c.userdata = t;
        t.t.c.flags.defer_callback = false;
        addToLoop(&t.t.c);
        return true;
    }
    return false;
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
        // A close inside a poll wakes it after that harvest took its batch.
        w.must_by_poll = M.polls + 1 + @intFromBool(M.in_poll);
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
        switch (s.kind) {
            .d_recv, .d_poll_recv => {
                const sk = &M.socks[s.target];
                if (s.at_submit == null and !s.woken_closed and sk.open and sk.gen == s.gen and sk.qlen() > 0) {
                    fail("{t} op #{d} still waits on a socket with {d} datagrams queued: its wake was lost, or it lost its registration", .{ s.kind, s.serial, sk.qlen() });
                }
            },
            .p_recv, .p_send => if (s.wake_since) |t| {
                if (now >= t + pipe_wake_ns) fail("{t} op #{d} was woken at {d} ns (bytes, room, a close, a shutdown or a reset) and has not completed at {d} ns: its wake was lost, or it lost its registration", .{ s.kind, s.serial, t, now });
            },
            else => {},
        }
    }
    for (&M.timers) |*t| {
        if (t.used and t.completions == 0 and !t.cancel_req and now > t.deadline) fail("a timer due at {d} ns has not fired at {d} ns", .{ t.deadline, now });
    }
}

fn checkAfterStep() void {
    for (&M.socks) |*sk| {
        if (sk.open) checkSockStats(sk);
    }
    for (M.ends[0 .. 2 * @as(usize, M.pairs)]) |*e| {
        const got = sim.testPipeBuffered(e.fd);
        if (got != e.buf.items.len) fail("pipe end fd {d} holds {d} bytes, the model {d}", .{ e.fd, got, e.buf.items.len });
    }
    if (sim.datagramsDroppedNoSocket() != M.dropped_nosock) fail("{d} datagrams dropped with no socket, the model {d}", .{ sim.datagramsDroppedNoSocket(), M.dropped_nosock });
    const fs = sim.netFaultStats();
    if (!std.meta.eql(fs, M.fstats)) fail("fault counts: the sim's {any}, the model's {any}", .{ fs, M.fstats });
    if (sim.netFaultDraws() != M.fdraws) fail("{d} fault draws, the model {d}", .{ sim.netFaultDraws(), M.fdraws });
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
    M.pairs = 0;
    M.pair = 0;
    M.timers = @splat(.{});
    M.last_io_clock = std.math.maxInt(u64);
    M.last_timer_clock = std.math.maxInt(u64);
    M.wire = .empty;
    M.used_fds = .empty;
    M.next_gen = 1;
    M.next_serial = 1;
    M.polls = 0;
    M.c = .{};
    M.dropped_nosock = 0;
    M.delivered = 0;
    M.frules_len = 0;
    M.fkeys = .empty;
    M.fstats = .{};
    M.fdraws = 0;
    M.send_no = 0;

    sim.begin(seed_value);
    defer sim.end();
    M.loop.init(.{}) catch |e| fail("loop init: {t}", .{e});
    defer M.loop.deinit();
    M.keepalive = ev.Timer.init(.{ .duration = .fromSeconds(1_000_000) });
    M.loop.add(&M.keepalive.c);

    _ = newPair();
    for (0..n_socks) |k| openModel(@intCast(k));

    while (M.step < steps) : (M.step += 1) {
        if (child) progress();
        if (M.probe == .recv or M.probe == .send) probePipe();
        const kind = pickStep();
        if (runStep(kind)) M.c.steps[@backingInt(kind)] += 1 else M.c.skipped[@backingInt(kind)] += 1;
        checkAfterStep();
    }
    finish();
    _ = M.loop.clearTimer(&M.keepalive);
    return M.c;
}

/// Settle; then give every op that still waits what it waits for, and
/// require it to complete with the model's result: an op that lost its
/// registration fails here, where cancelling it would have hidden it. Then
/// drain and close everything, and check that nothing is left.
fn finish() void {
    // No fault acts on what the end sends to prove a registration.
    syncLand();
    while (M.frules_len > 0) removeRandomFRule(M.frules_len - 1);
    // Settle by clock time, not by a count of polls: a poll moves the clock
    // only to the next due op, a pipe op is due up to 5 ms after a wake, and
    // a datagram up to delay plus jitter (5 ms) after its send.
    settle(20 * std.time.ns_per_ms);
    checkAfterStep();
    // Empty the queues (a layer receive each, checked), so every socket
    // with nothing waiting can take part in an end group.
    for (&M.socks, 0..) |*sk, k| {
        while (sk.open and sk.qlen() > 0) layerRecv(@intCast(k));
    }
    endGroups();
    // The proof: one waiting op at a time is given what it waits for, and
    // the clock settles before the next. No close or cancel acts meanwhile
    // (`inBatch` is off). The op must complete from what it was given: not
    // stay, and not end Canceled (`complete` fails a proving op that does).
    // Giving two race members at once would let one complete and cancel
    // the other, which hides the other's lost registration (round 4,
    // finding 2). A race member cancelled by the one proven is not proven.
    M.in_proof = true;
    defer M.in_proof = false;
    var rounds: usize = 0;
    while (true) : (rounds += 1) {
        if (rounds == 4 * n_slots) fail("the registration proof did not end", .{});
        var next: ?*Slot = null;
        var woken: usize = 0;
        for (&M.slots) |*s| {
            if (!s.used or s.completions != 0) continue;
            if (s.at_submit != null or s.cancel_req or s.woken_closed) fail("{t} op #{d} never completed (decided at submit {any}, cancel asked {any}, woken by a close {any}, group {any}, deferred {any}, submitted at poll {d}, now {d} ns, must by {any} ns)", .{ s.kind, s.serial, s.at_submit, s.cancel_req, s.woken_closed, s.group, s.deferred, s.submit_poll, sim.clockNs(), s.must_by_clock });
            // Woken already (maybe late in the settle): `pollOnce` holds it
            // to its wake time.
            if (s.wake_since != null) {
                woken += 1;
            } else if (next == null) next = s;
        }
        const w = next orelse {
            if (woken == 0) break;
            settle(2 * pipe_wake_ns);
            continue;
        };
        // Recorded by the op's own completion (`complete`): its slot can be
        // free and hold another op by the time the settle ends.
        const kind = w.kind;
        const serial = w.serial;
        w.proving = true;
        M.proof_done = false;
        give(w);
        settle(2 * pipe_wake_ns);
        checkAfterStep();
        if (!M.proof_done) fail("{t} op #{d} still waits after it alone was given what it waits for: it lost its registration", .{ kind, serial });
        M.c.registration_proofs += 1;
    }
    for (&M.groups) |*g| {
        if (g.used) fail("a race group never completed", .{});
    }
    for (&M.timers) |*t| {
        if (t.used) fail("a timer due at {d} ns never fired", .{t.deadline});
    }
    M.step = std.math.maxInt(usize);
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
    // Drain every pipe end that can still be read, with ev receives. Bytes
    // at an end closed for reading stay there: `left` in the ledger.
    var left: u64 = 0;
    for (M.ends[0 .. 2 * @as(usize, M.pairs)], 0..) |*E, e| {
        while (E.buf.items.len > 0 and endUsable(@intCast(e)) and !E.reset) {
            const i = freeSlot().?;
            const s = buildPipeRecv(i, @intCast(e), false).?;
            arm(s, false);
            addToLoop(s.comp());
            settle(2 * pipe_wake_ns);
        }
        left += E.buf.items.len;
    }
    for (&M.socks, 0..) |*sk, k| {
        if (!sk.open) continue;
        closeModel(@intCast(k));
        os_net.close(sk.fd);
    }
    for (M.ends[0 .. 2 * @as(usize, M.pairs)], 0..) |*E, e| {
        if (E.closed) continue;
        const i = freeSlot().?;
        const s = buildPipeClose(i, @intCast(e)).?;
        arm(s, false);
        addToLoop(s.comp());
    }
    settle(2 * pipe_wake_ns);
    if (sim.hasLiveIo()) fail("ops still live at the end", .{});
    if (sim.hasDueIo() or sim.nextDueIoRemaining() != null) fail("I/O still due or on the wire at the end", .{});
    if (sim.hasParkedIo()) fail("a pipe op still parked at the end", .{});
    if (M.wire.items.len != 0) fail("the model still has {d} datagrams on the wire", .{M.wire.items.len});
    for (&M.slots) |*s| {
        if (s.used) fail("slot of op #{d} still held at the end", .{s.serial});
    }
    // Every datagram sent, and every second copy, was received, dropped
    // with a cause, or discarded at a close; none is left on the wire.
    const fs = M.fstats;
    const by_fault = fs.lost + fs.lost_burst + fs.partitioned_at_send + fs.partitioned_in_flight + fs.silenced_at_send + fs.silenced_in_flight;
    const lost = M.c.dgrams_sent + fs.duplicated - M.delivered - M.c.dropped_rcvbuf - M.c.dropped_nosock - M.c.discarded_at_close - by_fault;
    if (lost != 0) fail("{d} datagrams sent and {d} copies, {d} received, {d} + {d} dropped, {d} discarded at close, {d} dropped by a fault rule", .{ M.c.dgrams_sent, fs.duplicated, M.delivered, M.c.dropped_rcvbuf, M.c.dropped_nosock, M.c.discarded_at_close, by_fault });
    inline for (comptime std.meta.fieldNames(sim.NetFaultStats)) |name| @field(M.c, name) = @field(fs, name);
    var accepted: u64 = 0;
    var received: u64 = 0;
    var dropped: u64 = 0;
    for (M.ends[0 .. 2 * @as(usize, M.pairs)]) |*E| {
        accepted += E.accepted;
        received += E.received;
        dropped += E.dropped;
    }
    if (accepted != received + dropped + left) fail("pipes: {d} bytes accepted, {d} received, {d} dropped by a reset, {d} left at ends closed for reading", .{ accepted, received, dropped, left });
}

/// Race groups of two receives that wait, on sockets with nothing queued and
/// nothing waiting, submitted at the end so the registration proof has race
/// members to prove (round 4, finding 2): one member's readiness completes
/// it and the race cancels its sibling, which could hide the sibling's lost
/// registration.
fn endGroups() void {
    var free: [n_socks]u8 = undefined;
    var n: usize = 0;
    for (&M.socks, 0..) |*sk, k| {
        if (sk.open and sk.waiting == null and sk.qlen() == 0) {
            free[n] = @intCast(k);
            n += 1;
        }
    }
    var k: usize = 0;
    while (k + 2 <= n) : (k += 2) {
        const gi = freeGroup() orelse return;
        const g = &M.groups[gi];
        g.* = .{ .used = true, .n = 2, .deferred = false };
        g.g = ev.Group.init(.race);
        for (0..2) |j| {
            const i = freeSlot() orelse fail("no free slot for an end group", .{});
            const m = buildDgramRecv(i, free[k + j], false).?;
            g.members[j] = i;
            m.group = gi;
            arm(m, false);
            g.g.add(m.comp());
        }
        g.g.c.callback = onGroup;
        g.g.c.userdata = g;
        g.g.c.flags.defer_callback = false;
        addToLoop(&g.g.c);
        M.c.end_groups += 1;
    }
}

/// Polls until the clock has moved by `ns`.
fn settle(ns: u64) void {
    const until = sim.clockNs() + ns;
    while (sim.clockNs() < until) pollOnce(until - sim.clockNs());
}

/// Gives a waiting op what it waits for: bytes or room on its pipe, or a
/// datagram on its socket. Its completion is then checked as any other.
fn give(w: *Slot) void {
    switch (w.kind) {
        .p_recv => {
            const p = w.target ^ 1;
            if (!endUsable(p) or M.ends[p].waiting_send != null) fail("p_recv op #{d} waits, but its peer cannot send to it", .{w.serial});
            const i = freeSlot() orelse fail("no free slot to give op #{d} its bytes", .{w.serial});
            const s = initSlot(i, .p_send);
            s.fd = M.ends[p].fd;
            s.target = p;
            s.n_iov = 1;
            s.parts[0] = "registered";
            s.iovc[0] = os_net.iovecConstFromSlice(s.parts[0]);
            s.total = s.parts[0].len;
            s.at_submit = pipeSendNow(p, s, false) orelse fail("p_recv op #{d} waits on an empty end, but its peer's buffer is full", .{w.serial});
            s.box = .{ .send = ev.NetSend.init(s.fd, .{ .iovecs = s.iovc[0..1] }, .{}) };
            arm(s, false);
            addToLoop(s.comp());
        },
        .p_send => {
            const p = w.target ^ 1;
            const i = freeSlot() orelse fail("no free slot to give op #{d} room", .{w.serial});
            const s = buildPipeRecv(i, p, false) orelse fail("p_send op #{d} waits, but its peer cannot receive", .{w.serial});
            arm(s, false);
            addToLoop(s.comp());
        },
        .d_recv, .d_poll_recv => {
            const sk = &M.socks[w.target];
            for (&M.socks) |*from| {
                if (!from.open or flowClash(from.port, sk.port)) continue;
                var one = [1]os_net.iovec_const{os_net.iovecConstFromSlice("registered")};
                const to = sockAddr(sk.port);
                _ = os_net.sendDatagram(from.fd, &one, .{}, &to, null) catch |e| fail("give: send: {t}", .{e});
                modelSend(from, sk.port, "registered");
                return;
            }
        },
        else => fail("{t} op #{d} waits, but it never waits", .{ w.kind, w.serial }),
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
    std.debug.print("op model counts: ops={d} parked={d} canceled={d} closed_hit={d} reused_number_ops={d} closed_then_reused={d} groups={d} f1_shape_groups={d} followups={d} followups_into_canceled_due={d}\n", .{ c.ops, c.parked, c.canceled, c.closed_hit, c.reused_number_ops, c.closed_then_reused, c.groups, c.f1_shape_groups, c.followups, c.followups_into_canceled_due });
    std.debug.print("op model counts: dgrams_sent={d} empty={d} truncated={d} scatter_recvs={d} dropped_rcvbuf={d} dropped_nosock={d} discarded_at_close={d} landed_in_new_socket={d} only_empty_left={d} landed_behind_empty={d} pipe_parked_sends={d} would_block={d} polls={d} no_free_slot={d}\n", .{ c.dgrams_sent, c.empty_dgrams, c.truncated, c.scatter_recvs, c.dropped_rcvbuf, c.dropped_nosock, c.discarded_at_close, c.landed_in_new_socket, c.only_empty_left, c.landed_behind_empty, c.pipe_parked_sends, c.would_block, c.polls, c.no_free_slot });
    std.debug.print("op model counts: woken_completed={d} registration_proofs={d} end_groups={d} in_batch_reopen={d} in_batch_steal={d} in_batch_cancel={d} timers_fired={d} timer_and_io_at_one_time={d} pipe_pairs={d} pipe_closes={d} pipe_shutdowns={d} pipe_resets={d} end_of_stream={d} broken_pipe={d} reset_errors={d}\n", .{ c.woken_completed, c.registration_proofs, c.end_groups, c.in_batch_reopen, c.in_batch_steal, c.in_batch_cancel, c.timers_fired, c.timer_and_io_at_one_time, c.pipe_pairs, c.pipe_closes, c.pipe_shutdowns, c.pipe_resets, c.end_of_stream, c.broken_pipe, c.reset_errors });
    std.debug.print("op model counts: fault_sets={d} fault_removals={d} lost={d} lost_burst={d} partitioned_at_send={d} partitioned_in_flight={d} silenced_at_send={d} silenced_in_flight={d} duplicated={d} delayed={d} clamped={d} overtaking={d} delayed_wakes={d}\n", .{ c.fault_sets, c.fault_removals, c.lost, c.lost_burst, c.partitioned_at_send, c.partitioned_in_flight, c.silenced_at_send, c.silenced_in_flight, c.duplicated, c.delayed, c.clamped, c.overtaking, c.delayed_wakes });
}

test "sim: op model: datagram sockets and a pipe under random sends, receives, polls, cancels, race groups, close and reopen, in a child process" {
    if (comptime !sim.compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    if (!sim.isDeathChild()) return;
    sim.noCoreDump();
    if (std.c.getenv("ZIO_OP_MODEL_PROBE")) |pv| M.probe = std.meta.stringToEnum(Probe, std.mem.span(pv)) orelse @panic("op model: ZIO_OP_MODEL_PROBE is recv, send or dgram");
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
    if (M.probe != .none) {
        // The probe never had its chance: the parent fails on this line.
        std.debug.print("op model probe: never reached\n", .{});
        return;
    }
    printCounts(total, n_seeds, steps);
    // A replay with another length is a diagnostic, not the instrument: its
    // counts prove nothing, and are not required.
    if (std.c.getenv("ZIO_OP_MODEL_STEPS") != null) {
        sim.childDone(child_name);
        return;
    }
    // A kind that never happened is a hole in the instrument, not a pass.
    for (total.steps, 0..) |n, k| {
        if (n == 0) fail("no step of kind {t} ran", .{@as(StepKind, @fromBackingInt(@intCast(k)))});
    }
    inline for (.{ "parked", "canceled", "closed_hit", "reused_number_ops", "closed_then_reused", "f1_shape_groups", "followups_into_canceled_due", "empty_dgrams", "truncated", "scatter_recvs", "dropped_rcvbuf", "dropped_nosock", "discarded_at_close", "landed_in_new_socket", "pipe_parked_sends", "woken_completed", "registration_proofs", "end_groups", "in_batch_reopen", "in_batch_steal", "in_batch_cancel", "timers_fired", "timer_and_io_at_one_time", "pipe_closes", "pipe_shutdowns", "pipe_resets", "end_of_stream", "broken_pipe", "reset_errors", "fault_sets", "fault_removals", "lost", "lost_burst", "partitioned_at_send", "partitioned_in_flight", "silenced_at_send", "silenced_in_flight", "duplicated", "delayed", "clamped", "overtaking", "delayed_wakes" }) |name| {
        if (@field(total, name) == 0) fail("the counter {s} is 0: that case never ran", .{name});
    }
    sim.childDone(child_name);
}

/// Runs the child test with `probe` (null: the normal run); returns its
/// result, owned by `gpa`.
fn runChild(gpa: std.mem.Allocator, probe: ?[]const u8) !std.process.RunResult {
    const io = std.testing.io;
    var exe_buf: [std.fs.max_path_bytes]u8 = undefined;
    const exe_len = try std.process.executablePath(io, &exe_buf);
    var env: std.process.Environ.Map = .init(gpa);
    defer env.deinit();
    if (std.c.getenv("PATH")) |path| try env.put("PATH", std.mem.span(path));
    for ([_][:0]const u8{ "ZIO_OP_MODEL_SEED", "ZIO_OP_MODEL_STEPS" }) |name| {
        if (std.c.getenv(name)) |v| try env.put(name, std.mem.span(v));
    }
    if (probe) |p| try env.put("ZIO_OP_MODEL_PROBE", p);
    try env.put("TEST_FILTER", child_name);
    try env.put(sim.death_child_env, "1");
    return std.process.run(gpa, io, .{
        .argv = &.{exe_buf[0..exe_len]},
        .environ_map = &env,
        .timeout = .{ .duration = .{ .raw = .fromSeconds(600), .clock = .awake } },
    });
}

test "sim: op model: datagram sockets and a pipe under random sends, receives, polls, cancels, race groups, close and reopen" {
    if (comptime !sim.compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    if (sim.isDeathChild()) return;
    const gpa = std.testing.allocator;
    const res = try runChild(gpa, null);
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
    if (std.c.getenv("ZIO_OP_MODEL_SEED") != null or std.c.getenv("ZIO_OP_MODEL_STEPS") != null) return;
    // The probes: in a run, at the first chance, a second ev op behind a
    // woken one; the sim must refuse it, naming the woken one's state.
    const probes = [_]struct { []const u8, []const u8 }{
        .{ "recv", "sim: a second ev receive while one is woken and not yet harvested, on sim pipe fd " },
        .{ "send", "sim: a second ev send while one is woken and not yet harvested, on sim pipe fd " },
        .{ "dgram", "sim: a second ev receive or poll while one is woken and not yet harvested, on sim datagram socket " },
    };
    for (probes) |pr| {
        const pres = try runChild(gpa, pr[0]);
        defer gpa.free(pres.stdout);
        defer gpa.free(pres.stderr);
        var where: []const u8 = "op model probe: never reached";
        var pit = std.mem.splitScalar(u8, pres.stderr, '\n');
        while (pit.next()) |line| {
            if (std.mem.indexOf(u8, line, "op model probe: ") != null) where = line;
        }
        const died = switch (pres.term) {
            .signal => |sg| sg == .ABRT,
            else => false,
        };
        if (!died or !sim.hasPanicLine(pres.stderr, pr[1])) {
            std.debug.print("op model probe {s}: term {any}; {s}; wanted the panic \"{s}...\"\n", .{ pr[0], pres.term, where, pr[1] });
            return error.ProbeNotRefused;
        }
        std.debug.print("{s}: refused\n", .{where});
    }
}
