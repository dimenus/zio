// SPDX-FileCopyrightText: 2025 Lukáš Lalinský
// SPDX-License-Identifier: MIT
//
// The op model of the sim's stream sockets: slice 2's gate (design 17.15
// item 2, without faults). Random ops on several connections against a
// reference model, under each profile, and the points where a run with no
// profile must panic.
//
// How it runs:
// - One sim run per seed, with a bare `ev.Loop` and no runtime, so every step
//   is exact: the test calls `Loop.add`, `Loop.cancel` and `Loop.poll` itself.
//   A timer far in the future keeps the loop from counting as done.
// - One listener; up to `max_live` connections at a time and
//   `max_seed_conns` in a seed, each made by an ev connect and an ev accept, then given a random
//   window per direction and a random chunk mode. A step is one of
//   `StepKind`: an ev send or receive on a random endpoint, a poll, a
//   shutdown (.send or .both), a close (plain, or with SO_LINGER {1, 0}), a
//   window or chunk mode change, a cancel of a parked op, or a new
//   connection. A receive or a send lives in a slot until its callback.
// - The parent runs seeds 1 to 16 in one child per profile, twice, and
//   compares each seed's digest line between the two children (same seed,
//   two processes). Then, per seed, a child with no profile: it must panic
//   with the line, and at the step, that the profiled run's no-profile
//   shadow predicted (below), or finish if it predicted none.
//
// The model shares no helper with the sim. It keeps each connection's two
// endpoints (flags, unread bytes) and two directions (window, admitted,
// credited, read, reclaimed, discards by cause, credit pacing), and its own
// wire: every data chunk, FIN, RST and credit with its landing time. It cuts
// chunks with its own copy of the chunk-size draw (the doc comment of
// `chunkDraw` in sim.zig), lands an entry no earlier than the one sent
// before it in its direction, lands entries at one instant in (conn_id,
// direction, send order), departs owed credits after the landings of their
// instant in (conn_id, direction) order, and decides every call, harvest and
// landing by design 17.8's lists written as its own (`recvEval`,
// `sendEval`, `landData`, ...). The handshake is not modelled: a connection
// joins the model once its accept has completed, when nothing of it is on
// the wire.
//
// What it checks:
// - every op's result: the count, the error, and every byte a receive
//   returns (the model's own copy of what was sent). A result decided at the
//   call must come back unchanged. An op that parked must not complete
//   until the model woke it; once woken it must complete in that poll, with
//   the model's result at its harvest, in the order the loop harvests.
// - after every step: for each connection, every counter of both directions
//   (`sim.testStreamDir`, conservation included) and the flags and unread
//   bytes of both endpoints (`sim.testConnEnd`) equal the model's; a
//   connection is retired exactly when the model says it can be; the
//   entries in flight (`sim.streamEntriesInFlight`) equal the model's.
// - after every poll: the clock did not pass the model's next landing or
//   owed credit, nor the poll's cap.
// - at the end: each op still parked is given what it waits for (a byte
//   from its peer, or one more byte of window) and must complete, one at a
//   time; then every socket closes, the wire drains, and every connection
//   retires.
// - counts: how many of each step ran and were skipped (by reason), and of
//   each outcome; the child fails if a required count is 0 over its seeds
//   (`floors`), so a run where a kind never happened does not count.
//
// The no-profile shadow. Up to the first point where the kernels differ, a
// run with no profile does what a profiled run does, except one flag: under
// `.linux` a send after a FIN-then-RST consumes the pending error, which with
// no profile it does not. So the model keeps a second copy of that flag
// (`np_reported`) and, at each of 17.8's no-profile points, records the
// first step at which a run with no profile would panic and the row it would
// name. `.linux` and `.macos` must predict the same point for a seed.
//
// Rules the generator keeps, so that the model has one answer:
// - one unresolved receive and one unresolved send per endpoint;
// - no window change while the sender's send is woken and not harvested;
// - no op on an endpoint after its close; a cancel only of a parked op (not
//   woken), between polls;
// - the chunk mode `.fixed` only with windows of 16 KiB or less, so the TCP
//   entries stay far below `max_tcp_entries`;
// - the generator never reads the profile, so the runs with each profile
//   and with none take the same steps up to the first point that differs.
//
// What it does not reach: faults, holding and `failStream` (slice 3); the
// listener and handshake rules (slice 1's tests); a cancel inside a harvest
// batch (the bytes tests have it); a window change while a send is woken
// (refused; a refusal test has it).
//
// A failure prints `stream model: FAIL seed S step K: ...` and panics; the
// child prints `stream model: seed S step K` before each step, and the parent
// prints the last one. `ZIO_STREAM_MODEL_SEED=S` replays one seed,
// `ZIO_STREAM_MODEL_STEPS=N` sets the length, `ZIO_STREAM_MODEL_PROFILE` is
// `linux`, `macos` or `none`; any of them skips the floors and the parent's
// later children.

const std = @import("std");
const builtin = @import("builtin");
const sim = @import("sim.zig");
const ev = @import("ev/root.zig");
const os_net = @import("os/net.zig");

const parent_name = "sim: stream op model: connections under random sends, receives, shutdowns, closes, window and chunk changes and cancels, per profile and with none";
const child_name = parent_name ++ ", in a child process";

const L = sim.wire_latency_ns;
const us: u64 = 1000;
const default_steps = 1500;
const n_seeds = 16;
const max_live = 4;
/// Connections made in a seed; the model's table holds those not retired.
const max_seed_conns = 80;
const max_conns = 16;
const max_slots = 2 * 2 * max_live;
const buf_cap = 6000;

const StepKind = enum { send, recv, poll, settle, shut_send, shut_both, close, abort, window, chunking, cancel, connect };
const step_weights = [_]u32{ 24, 22, 18, 6, 1, 1, 2, 1, 5, 2, 4, 6 };

/// What a model op ends with.
const Want = union(enum) { n: usize, err: anyerror };

const OpKind = enum { recv, send };
const OpState = enum { decided, parked, woken, cancelled };

const Slot = struct {
    used: bool = false,
    kind: OpKind = .recv,
    state: OpState = .decided,
    serial: u32 = 0,
    ci: u8 = 0,
    side: u1 = 0,
    fd: i32 = 0,
    len: usize = 0,
    want: Want = .{ .n = 0 },
    /// A receive's expected bytes (the model's copy), when `want` is a count.
    want_bytes: [buf_cap]u8 = undefined,
    /// Why it was woken (`Counts.woken`), for the counts.
    wake: Wake = .none,
    recv: ev.NetRecv = undefined,
    send: ev.NetSend = undefined,
    buf: [buf_cap]u8 = undefined,
    riov: [1]os_net.iovec = undefined,
    siov: [1]os_net.iovec_const = undefined,
};

const Wake = enum { none, data, fin, rst, local_reset, credit, grow, shutdown, close };

const n_discard = @typeInfo(sim.Discard).@"enum".field_names.len;

const MEnd = struct {
    fd: i32 = -1,
    open: bool = false,
    fin_in: bool = false,
    fin_out: bool = false,
    rst_sent: bool = false,
    reset_received: bool = false,
    fin_before_reset: bool = false,
    reset_reported: bool = false,
    /// `reset_reported` as a run with no profile has it (the shadow).
    np_reported: bool = false,
    shut_rd: bool = false,
    linger0: bool = false,
    /// The serial of the send parked when the reset arrived.
    reset_parked_send: ?u32 = null,
    unread: std.ArrayList(u8) = .empty,
    recv: ?u8 = null,
    send: ?u8 = null,
};

const MDir = struct {
    window: u32 = sim.default_stream_window,
    admitted: u64 = 0,
    credited: u64 = 0,
    read: u64 = 0,
    reclaimed: u64 = 0,
    discarded: [n_discard]u64 = @splat(0),
    data_send_index: u64 = 0,
    last_credit_at: ?u64 = null,
    owed_at: ?u64 = null,

    fn room(d: MDir) u64 {
        const used = d.admitted - d.credited;
        return if (used >= d.window) 0 else d.window - used;
    }
};

const MConn = struct {
    used: bool = false,
    id: u32 = 0,
    ends: [2]MEnd = .{ .{}, .{} },
    dirs: [2]MDir = .{ .{}, .{} },
    chunking: sim.ChunkMode = .hashed,
    last_due: [2]u64 = .{ 0, 0 },
    next_order: u64 = 0,
};

const EKind = enum { data, fin, rst, credit };

const MEntry = struct {
    due: u64,
    conn_id: u32,
    ci: u8,
    dir: u1,
    /// Send order across the run: within a direction, the sim's sequence
    /// order.
    order: u64,
    kind: EKind,
    bytes: []const u8 = &.{},
    count: u64 = 0,
};

const Counts = struct {
    ran: [@typeInfo(StepKind).@"enum".field_names.len]u64 = @splat(0),
    skipped: [@typeInfo(StepKind).@"enum".field_names.len]u64 = @splat(0),
    /// Skips by reason.
    skip_no_endpoint: u64 = 0,
    skip_unresolved: u64 = 0,
    skip_no_parked: u64 = 0,
    skip_conn_limit: u64 = 0,
    skip_fixed_window: u64 = 0,
    send_admitted: u64 = 0,
    send_partial: u64 = 0,
    send_zero: u64 = 0,
    send_parked: u64 = 0,
    send_epipe: u64 = 0,
    send_reset: u64 = 0,
    send_reset_parked: u64 = 0,
    recv_data: u64 = 0,
    recv_eof: u64 = 0,
    recv_reset: u64 = 0,
    recv_parked: u64 = 0,
    closed_op: u64 = 0,
    cancel_recv: u64 = 0,
    cancel_send: u64 = 0,
    woken: [@typeInfo(Wake).@"enum".field_names.len]u64 = @splat(0),
    shut_unconnected: u64 = 0,
    close_fin: u64 = 0,
    close_rst: u64 = 0,
    close_quiet: u64 = 0,
    rst_answers: u64 = 0,
    local_resets: u64 = 0,
    releases: u64 = 0,
    stale_credits: u64 = 0,
    owed_departed: u64 = 0,
    credits_landed: u64 = 0,
    shrink_below_use: u64 = 0,
    shared_reset: u64 = 0,
    batches: u64 = 0,
    chunks: [3]u64 = @splat(0),
    end_proofs: u64 = 0,
    conns: u64 = 0,
    polls: u64 = 0,

    fn add(a: *Counts, b: Counts) void {
        const info = @typeInfo(Counts).@"struct";
        inline for (info.field_names, info.field_types) |name, T| {
            switch (@typeInfo(T)) {
                .int => @field(a, name) += @field(b, name),
                .array => for (&@field(a, name), @field(b, name)) |*x, y| {
                    x.* += y;
                },
                else => {},
            }
        }
    }
};

const Profile = enum { linux, macos, none };

const M = struct {
    var seed: u64 = 0;
    var step: usize = 0;
    var rng: std.Random.DefaultPrng = undefined;
    var arena: std.heap.ArenaAllocator = undefined;
    var loop: ev.Loop = undefined;
    var keepalive: ev.Timer = undefined;
    var profile: Profile = .linux;
    var conns: [max_conns]MConn = @splat(.{});
    var n_conns: usize = 0;
    var listener: i32 = -1;
    var port: u16 = 0;
    var wire: std.ArrayList(MEntry) = .empty;
    var next_order: u64 = 0;
    var slots: [max_slots]Slot = undefined;
    var next_serial: u32 = 1;
    var model_clock: u64 = 0;
    var c: Counts = .{};
    /// The run's `sim.streamStats()` as the model counts it: compared,
    /// field by field, after every step (`checkStats`).
    var st: sim.StreamStats = .{};
    var in_poll: bool = false;
    var batch: usize = 0;
    /// The no-profile shadow's first point: the step and the row text.
    var np_step: ?usize = null;
    var np_text: []const u8 = "";
    /// With no profile: the call point the model expects the sim to refuse.
    var expect_refusal: ?[]const u8 = null;
    /// A setup or end action in progress (no step's counts).
    var setup_done: [2]bool = .{ false, false };
    var keepalive_done: bool = false;
};

fn keepaliveDone(_: *ev.Loop, _: *ev.Completion) void {
    M.keepalive_done = true;
}

fn fail(comptime fmt: []const u8, args: anytype) noreturn {
    std.debug.print("stream model: FAIL seed {d} step {d}: " ++ fmt ++ "\n", .{ M.seed, M.step } ++ args);
    @panic("stream model: FAIL");
}

fn progress() void {
    var b: [64]u8 = undefined;
    const line = std.fmt.bufPrint(&b, "stream model: seed {d} step {d}\n", .{ M.seed, M.step }) catch unreachable;
    _ = std.c.write(2, line.ptr, line.len);
}

fn rand(n: usize) usize {
    return M.rng.random().uintLessThan(usize, n);
}

fn chance(pct: u32) bool {
    return M.rng.random().uintLessThan(u32, 100) < pct;
}

fn other(s: u1) u1 {
    return 1 - s;
}

fn now() u64 {
    return sim.clockNs();
}

// ---- The no-profile points (design 17.8) ----

/// A point where the kernels differ. Profiled: the shadow records it (the
/// first one only) and the profile answers. With no profile: a call point
/// is the sim's to refuse (`expect_refusal`, checked after the call); a
/// landing or harvest point means the sim should already have panicked.
fn profilePoint(comptime text: []const u8, at_call: bool) Profile {
    switch (M.profile) {
        .none => {
            if (!at_call) fail("the sim did not panic at a no-profile point it should have: {s}", .{text});
            std.debug.print("stream model: expect the sim to refuse: sim: {s}: the kernels differ here\n", .{text});
            M.expect_refusal = text;
            return .linux;
        },
        else => {
            shadowPoint(text);
            return M.profile;
        },
    }
}

fn shadowPoint(comptime text: []const u8) void {
    if (M.np_step != null) return;
    M.np_step = M.step;
    M.np_text = text;
}

fn report(e: *MEnd) void {
    e.reset_reported = true;
    e.np_reported = true;
}

// ---- The model's wire ----

fn entryBefore(a: MEntry, b: MEntry) bool {
    if (a.due != b.due) return a.due < b.due;
    if (a.conn_id != b.conn_id) return a.conn_id < b.conn_id;
    if (a.dir != b.dir) return a.dir < b.dir;
    return a.order < b.order;
}

fn sendEntry(ci: u8, from: u1, kind: EKind, extra: u64, bytes: []const u8, count: u64) void {
    const k = &M.conns[ci];
    const at = @max(now() + L + extra, k.last_due[from]);
    k.last_due[from] = at;
    const e: MEntry = .{ .due = at, .conn_id = k.id, .ci = ci, .dir = from, .order = M.next_order, .kind = kind, .bytes = bytes, .count = count };
    M.next_order += 1;
    var pos = M.wire.items.len;
    while (pos > 0 and entryBefore(e, M.wire.items[pos - 1])) pos -= 1;
    M.wire.insert(M.arena.allocator(), pos, e) catch @panic("oom");
}

/// The next time something happens on the model's wire: a landing or an
/// owed credit's departure.
fn nextEvent() ?u64 {
    var t: ?u64 = if (M.wire.items.len > 0) M.wire.items[0].due else null;
    for (M.conns) |k| {
        if (!k.used) continue;
        for (k.dirs) |d| {
            const o = d.owed_at orelse continue;
            if (t == null or o < t.?) t = o;
        }
    }
    return t;
}

/// Lands and departs everything up to `t`, time by time, as the sim's polls
/// do: the landings of an instant in order, then its owed credits.
fn advanceTo(t: u64) void {
    while (nextEvent()) |e| {
        if (e > t) break;
        if (e < M.model_clock) fail("the model's wire has an event at {d} ns, before its clock {d} ns", .{ e, M.model_clock });
        M.model_clock = e;
        while (M.wire.items.len > 0 and M.wire.items[0].due == e) {
            const w = M.wire.orderedRemove(0);
            land(w);
        }
        departOwed(e);
    }
    M.model_clock = t;
}

fn departOwed(t: u64) void {
    while (true) {
        var best: ?u8 = null;
        var best_d: u1 = 0;
        for (&M.conns, 0..) |*k, ci| {
            if (!k.used) continue;
            for (k.dirs, 0..) |d, di| {
                const o = d.owed_at orelse continue;
                if (o > t) continue;
                if (best == null or k.id < M.conns[best.?].id) {
                    best = @intCast(ci);
                    best_d = @intCast(di);
                }
            }
        }
        const ci = best orelse return;
        M.c.owed_departed += 1;
        departCredit(ci, best_d);
    }
}

fn dirTerminal(k: *const MConn, data: u1) bool {
    const x = k.ends[data];
    return !x.open or x.reset_received;
}

fn departCredit(ci: u8, data: u1) void {
    const k = &M.conns[ci];
    const d = &k.dirs[data];
    if (dirTerminal(k, data)) {
        d.owed_at = null;
        M.c.stale_credits += 1;
        M.st.stale_credit += 1;
        return;
    }
    d.owed_at = null;
    d.last_credit_at = now();
    sendEntry(ci, other(data), .credit, 0, &.{}, d.read + d.reclaimed);
}

/// A read or a release of `data` owes a credit (design 17.7).
fn creditOwed(ci: u8, data: u1) void {
    const d = &M.conns[ci].dirs[data];
    if (d.owed_at != null) return;
    if (d.last_credit_at) |t| {
        if (now() < t + L) {
            d.owed_at = t + L;
            return;
        }
    }
    departCredit(ci, data);
}

/// The chunk-size draw, the model's own copy of the formula in the doc
/// comment of `chunkDraw` in sim.zig.
fn chunkDraw(conn_id: u32, dir: u1, send_index: u64, k: u64) u64 {
    var key: [16]u8 = @splat(0);
    std.mem.writeInt(u64, key[0..8], M.seed, .little);
    var m: [26]u8 = undefined;
    std.mem.writeInt(u32, m[0..4], conn_id, .little);
    m[4] = dir;
    std.mem.writeInt(u64, m[5..13], send_index, .little);
    std.mem.writeInt(u64, m[13..21], k, .little);
    @memcpy(m[21..26], "chunk");
    return std.hash.SipHash64(2, 4).toInt(&m, &key);
}

fn admit(ci: u8, side: u1, bytes: []const u8) void {
    const k = &M.conns[ci];
    const d = &k.dirs[side];
    M.c.chunks[
        switch (k.chunking) {
            .hashed => 0,
            .fixed => 1,
            .off => 2,
        }
    ] += 1;
    var rest = bytes.len;
    var off: usize = 0;
    var idx: u64 = 0;
    while (rest > 0) : (idx += 1) {
        const want: u64 = switch (k.chunking) {
            .off => rest,
            .fixed => |n| n,
            .hashed => blk: {
                const h = chunkDraw(k.id, side, d.data_send_index, idx);
                break :blk if (h & 3 == 0) 1 + (h >> 2) % 16 else 512 + (h >> 2) % 3585;
            },
        };
        const size: usize = @intCast(@min(want, rest));
        const copy = M.arena.allocator().dupe(u8, bytes[off..][0..size]) catch @panic("oom");
        sendEntry(ci, side, .data, idx * us, copy, 0);
        off += size;
        rest -= size;
    }
    d.admitted += bytes.len;
    d.data_send_index += 1;
}

fn discard(ci: u8, data: u1, cause: sim.Discard, n: usize) void {
    M.conns[ci].dirs[data].discarded[@backingInt(cause)] += n;
    M.st.discarded[@backingInt(cause)] += n;
}

fn discardUnread(ci: u8, side: u1, cause: sim.Discard) usize {
    const e = &M.conns[ci].ends[side];
    const n = e.unread.items.len;
    discard(ci, other(side), cause, n);
    e.unread.clearRetainingCapacity();
    return n;
}

fn wakeRecv(ci: u8, side: u1, why: Wake) void {
    const e = &M.conns[ci].ends[side];
    const i = e.recv orelse return;
    if (M.slots[i].state != .parked) return;
    M.slots[i].state = .woken;
    M.slots[i].wake = why;
}

fn wakeSend(ci: u8, side: u1, why: Wake) void {
    const e = &M.conns[ci].ends[side];
    const i = e.send orelse return;
    if (M.slots[i].state != .parked) return;
    M.slots[i].state = .woken;
    M.slots[i].wake = why;
}

fn answerRst(ci: u8, at: u1) void {
    const x = &M.conns[ci].ends[at];
    if (x.rst_sent) return;
    x.rst_sent = true;
    M.c.rst_answers += 1;
    sendEntry(ci, at, .rst, 0, &.{}, 0);
}

fn parkedSendSerial(x: *const MEnd) ?u32 {
    const i = x.send orelse return null;
    return if (M.slots[i].state == .parked) M.slots[i].serial else null;
}

fn land(w: MEntry) void {
    const ci = w.ci;
    const at = other(w.dir);
    const k = &M.conns[ci];
    const x = &k.ends[at];
    switch (w.kind) {
        .data => {
            const n = w.bytes.len;
            if (x.reset_received) {
                M.st.dropped_after_reset += 1;
                return discard(ci, w.dir, .after_reset, n);
            }
            if (!x.open) {
                M.st.dropped_dead += 1;
                answerRst(ci, at);
                return discard(ci, w.dir, .closed_receiver, n);
            }
            if (x.shut_rd) {
                const p = profilePoint("row 18, a data chunk that lands after the endpoint's own shutdown(.both)", false);
                answerRst(ci, at);
                discard(ci, w.dir, .shut_receiver, n);
                if (p == .linux) {
                    x.reset_received = true;
                    x.fin_before_reset = false;
                    x.reset_parked_send = parkedSendSerial(x);
                    M.c.local_resets += 1;
                    M.st.local_resets += 1;
                    wakeRecv(ci, at, .local_reset);
                    wakeSend(ci, at, .local_reset);
                }
                return;
            }
            x.unread.appendSlice(M.arena.allocator(), w.bytes) catch @panic("oom");
            M.st.chunks_landed += 1;
            wakeRecv(ci, at, .data);
        },
        .fin => {
            // Dead first, then reset (design 17.8, the FIN landing list).
            if (!x.open) {
                M.st.dropped_dead += 1;
                return;
            }
            if (x.reset_received) {
                M.st.dropped_after_reset += 1;
                return;
            }
            x.fin_in = true;
            wakeRecv(ci, at, .fin);
        },
        .rst => {
            // Reset first, then dead (design 17.8, the RST landing list).
            if (x.reset_received) {
                M.st.dropped_after_reset += 1;
                return;
            }
            if (!x.open) {
                M.st.dropped_dead += 1;
                return;
            }
            x.reset_received = true;
            x.fin_before_reset = x.fin_in;
            x.reset_parked_send = parkedSendSerial(x);
            const both = x.recv != null and x.send != null and M.slots[x.recv.?].state == .parked and M.slots[x.send.?].state == .parked;
            if (both) M.c.shared_reset += 1;
            wakeRecv(ci, at, .rst);
            wakeSend(ci, at, .rst);
        },
        .credit => {
            if (dirTerminal(k, at)) {
                M.c.stale_credits += 1;
                M.st.stale_credit += 1;
                return;
            }
            const d = &k.dirs[at];
            if (w.count <= d.credited) fail("the model's own credit of {d} on connection {d} landed after one of {d}", .{ w.count, k.id, d.credited });
            d.credited = w.count;
            M.c.credits_landed += 1;
            M.st.credits_landed += 1;
            if (d.room() > 0) wakeSend(ci, at, .credit);
        },
    }
}

// ---- 17.8's evaluations, as the model's own ----

fn recvEval(ci: u8, side: u1, s: *Slot) ?Want {
    const k = &M.conns[ci];
    const e = &k.ends[side];
    if (e.unread.items.len > 0) {
        const n = @min(s.len, e.unread.items.len);
        @memcpy(s.want_bytes[0..n], e.unread.items[0..n]);
        e.unread.replaceRange(M.arena.allocator(), 0, n, &.{}) catch @panic("oom");
        k.dirs[other(side)].read += n;
        creditOwed(ci, other(side));
        return .{ .n = n };
    }
    if (e.reset_received) {
        // The shadow first: a run with no profile has its own flag.
        if (!e.np_reported and e.fin_before_reset) shadowPoint("rows 8 and 22, a read that meets a reset after a FIN had landed");
        if (!e.reset_reported) {
            if (!e.fin_before_reset) {
                report(e);
                return .{ .err = error.ConnectionResetByPeer };
            }
            switch (profilePointNoShadow("rows 8 and 22, a read that meets a reset after a FIN had landed", s.state == .decided)) {
                .linux, .none => {},
                .macos => {
                    report(e);
                    return .{ .err = error.ConnectionResetByPeer };
                },
            }
        }
    }
    if (e.fin_in or e.shut_rd or e.reset_received) return .{ .n = 0 };
    return null;
}

/// `profilePoint` without the shadow: for the read point, where the shadow
/// decides from its own flag, which the profiled branch does not read.
fn profilePointNoShadow(comptime text: []const u8, at_call: bool) Profile {
    if (M.profile == .none) return profilePoint(text, at_call);
    return M.profile;
}

fn sendEval(ci: u8, side: u1, s: *Slot, bytes: []const u8) ?Want {
    const k = &M.conns[ci];
    const e = &k.ends[side];
    const at_call = s.state == .decided;
    if (e.reset_received) {
        const parked = !at_call and e.reset_parked_send == s.serial;
        // The shadow.
        if (!e.np_reported) {
            if (parked) {
                if (e.fin_before_reset) shadowPoint("row 41, a send parked when a reset arrives after a FIN");
            } else if (!e.fin_before_reset) shadowPoint("rows 26b, 36 and 39, a send that meets a reset with no FIN before it");
        }
        if (e.reset_reported) return .{ .err = error.BrokenPipe };
        if (parked) {
            if (!e.fin_before_reset) {
                report(e);
                return .{ .err = error.ConnectionResetByPeer };
            }
            const p = profilePointNoShadow("row 41, a send parked when a reset arrives after a FIN", false);
            report(e);
            return .{ .err = if (p == .linux) error.BrokenPipe else error.ConnectionResetByPeer };
        }
        if (e.fin_before_reset) {
            if (M.profile == .linux) e.reset_reported = true;
            return .{ .err = error.BrokenPipe };
        }
        switch (profilePointNoShadow("rows 26b, 36 and 39, a send that meets a reset with no FIN before it", at_call)) {
            .linux, .none => {
                report(e);
                return .{ .err = error.ConnectionResetByPeer };
            },
            .macos => return .{ .err = error.BrokenPipe },
        }
    }
    if (e.fin_out) return .{ .err = error.BrokenPipe };
    const room = k.dirs[side].room();
    if (room == 0) return null;
    const n: usize = @intCast(@min(room, bytes.len));
    admit(ci, side, bytes[0..n]);
    return .{ .n = n };
}

// ---- Sim calls ----

fn sockAddr(port: u16) os_net.sockaddr.in {
    return .{ .port = std.mem.nativeToBig(u16, port), .addr = @bitCast([4]u8{ 127, 0, 0, 1 }) };
}

fn setupDone(_: *ev.Loop, _: *ev.Completion) void {
    M.setup_done[0] = true;
}

fn connectDone(_: *ev.Loop, _: *ev.Completion) void {
    M.setup_done[1] = true;
}

/// Runs a setup, shutdown or close op to its end, polling as a step does,
/// at most `max_polls` polls of `cap_ns`.
fn runOp(comp: *ev.Completion, cap_ns: u64, max_polls: usize) void {
    M.setup_done[0] = false;
    comp.callback = setupDone;
    comp.flags.defer_callback = false;
    M.loop.add(comp);
    var n: usize = 0;
    while (!M.setup_done[0]) : (n += 1) {
        if (n >= max_polls) fail("a {t} op did not complete in {d} polls", .{ comp.op, max_polls });
        pollOnce(cap_ns);
    }
}

fn openListener() void {
    var op = ev.NetOpen.init(.ipv4, .stream, .ip, .{ .nonblocking = true });
    runOp(&op.c, 50 * us, 100);
    M.listener = op.getResult() catch |e| fail("listener open: {t}", .{e});
    var a = sockAddr(0);
    var alen: os_net.socklen_t = @sizeOf(os_net.sockaddr.in);
    var b = ev.NetBind.init(M.listener, @ptrCast(&a), &alen);
    runOp(&b.c, 50 * us, 100);
    b.getResult() catch |e| fail("listener bind: {t}", .{e});
    M.port = std.mem.bigToNative(u16, a.port);
    var l = ev.NetListen.init(M.listener, 64);
    runOp(&l.c, 50 * us, 100);
    l.getResult() catch |e| fail("listen: {t}", .{e});
}

fn usedConns() usize {
    var n: usize = 0;
    for (M.conns) |k| n += @intFromBool(k.used);
    return n;
}

fn liveConns() usize {
    var n: usize = 0;
    for (M.conns) |k| {
        if (k.used and (k.ends[0].open or k.ends[1].open)) n += 1;
    }
    return n;
}

fn connect() bool {
    if (M.n_conns >= max_seed_conns or liveConns() >= max_live or usedConns() >= max_conns) {
        M.c.skip_conn_limit += 1;
        return false;
    }
    var o = ev.NetOpen.init(.ipv4, .stream, .ip, .{ .nonblocking = true });
    runOp(&o.c, 50 * us, 100);
    const cfd = o.getResult() catch |e| fail("open: {t}", .{e});
    var to = sockAddr(M.port);
    var cop = ev.NetConnect.init(cfd, @ptrCast(&to), @sizeOf(os_net.sockaddr.in));
    M.setup_done[1] = false;
    cop.c.callback = connectDone;
    cop.c.flags.defer_callback = false;
    M.loop.add(&cop.c);
    var aop = ev.NetAccept.init(M.listener, null, null);
    runOp(&aop.c, 50 * us, 100);
    const sfd = aop.getResult() catch |e| fail("accept: {t}", .{e});
    if (!M.setup_done[1]) fail("the connect did not complete before its accept", .{});
    cop.getResult() catch |e| fail("connect: {t}", .{e});
    var ci: u8 = 0;
    while (M.conns[ci].used) ci += 1;
    M.n_conns += 1;
    M.c.conns += 1;
    const k = &M.conns[ci];
    k.* = .{ .used = true, .id = @intCast(M.n_conns) };
    k.ends[0] = .{ .fd = cfd, .open = true };
    k.ends[1] = .{ .fd = sfd, .open = true };
    // Windows: tiny, small, medium or the default; the chunk mode by them.
    for (0..2) |side| {
        const w: u32 = switch (rand(4)) {
            0 => @intCast(1 + rand(16)),
            1 => @intCast(17 + rand(4096)),
            2 => @intCast(4097 + rand(12288)),
            else => sim.default_stream_window,
        };
        // The window toward `ends[side]`: data from the other side.
        sim.setStreamWindow(k.ends[side].fd, w);
        k.dirs[other(@intCast(side))].window = w;
    }
    const mode = randomChunking(ci) orelse sim.ChunkMode.hashed;
    sim.setStreamChunking(cfd, mode);
    k.chunking = mode;
    return true;
}

fn randomChunking(ci: u8) ?sim.ChunkMode {
    const k = &M.conns[ci];
    return switch (rand(3)) {
        0 => .hashed,
        1 => if (k.dirs[0].window <= 16384 and k.dirs[1].window <= 16384) .{ .fixed = @intCast(4 + rand(700)) } else null,
        else => .off,
    };
}

/// A random open endpoint, or null.
fn pickEnd() ?struct { u8, u1 } {
    var cand: [2 * max_conns]struct { u8, u1 } = undefined;
    var n: usize = 0;
    for (M.conns, 0..) |k, ci| {
        if (!k.used) continue;
        for (k.ends, 0..) |e, s| {
            if (e.open) {
                cand[n] = .{ @intCast(ci), @intCast(s) };
                n += 1;
            }
        }
    }
    if (n == 0) return null;
    return cand[rand(n)];
}

fn freeSlot() ?u8 {
    for (&M.slots, 0..) |*s, i| {
        if (!s.used) return @intCast(i);
    }
    return null;
}

fn onDone(_: *ev.Loop, comp: *ev.Completion) void {
    for (&M.slots, 0..) |*s, i| {
        if (!s.used) continue;
        const mine = switch (s.kind) {
            .recv => comp == &s.recv.c,
            .send => comp == &s.send.c,
        };
        if (mine) return complete(@intCast(i));
    }
    fail("a callback for an op the model does not have", .{});
}

fn complete(i: u8) void {
    const s = &M.slots[i];
    if (M.in_poll) {
        advanceTo(now());
        M.batch += 1;
    }
    const k = &M.conns[s.ci];
    const e = &k.ends[s.side];
    switch (s.state) {
        .parked => fail("{t} op #{d} completed though the model never woke it", .{ s.kind, s.serial }),
        .cancelled => s.want = .{ .err = error.Canceled },
        .decided => {},
        .woken => {
            M.c.woken[@backingInt(s.wake)] += 1;
            if (!e.open or e.fd != s.fd) {
                s.want = .{ .err = error.FileDescriptorNotASocket };
                M.c.closed_op += 1;
            } else {
                const w = switch (s.kind) {
                    .recv => recvEval(s.ci, s.side, s),
                    .send => sendEval(s.ci, s.side, s, s.buf[0..s.len]),
                };
                s.want = w orelse fail("woken {t} op #{d} has no result at its harvest in the model", .{ s.kind, s.serial });
                countResult(s);
            }
        },
    }
    const got: anyerror!usize = switch (s.kind) {
        .recv => s.recv.getResult(),
        .send => s.send.getResult(),
    };
    switch (s.want) {
        .n => |n| {
            const g = got catch |err| fail("{t} op #{d} on connection {d}: want {d}, got {t}", .{ s.kind, s.serial, k.id, n, err });
            if (g != n) fail("{t} op #{d} on connection {d}: want {d}, got {d}", .{ s.kind, s.serial, k.id, n, g });
            if (s.kind == .recv and !std.mem.eql(u8, s.want_bytes[0..n], s.buf[0..n])) fail("receive op #{d} on connection {d}: the bytes differ from the model's", .{ s.serial, k.id });
        },
        .err => |want_err| {
            if (got) |g| fail("{t} op #{d} on connection {d}: want {t}, got {d}", .{ s.kind, s.serial, k.id, want_err, g }) else |err| {
                if (err != want_err) fail("{t} op #{d} on connection {d}: want {t}, got {t}", .{ s.kind, s.serial, k.id, want_err, err });
            }
        },
    }
    // The endpoint's op is resolved (the fd may hold another endpoint now).
    if (e.fd == s.fd) {
        switch (s.kind) {
            .recv => if (e.recv == i) {
                e.recv = null;
            },
            .send => if (e.send == i) {
                e.send = null;
            },
        }
    }
    s.used = false;
}

fn countResult(s: *const Slot) void {
    switch (s.want) {
        .n => |n| switch (s.kind) {
            .recv => if (n == 0) {
                M.c.recv_eof += 1;
            } else {
                M.c.recv_data += 1;
            },
            .send => if (n == 0) {
                M.c.send_zero += 1;
            } else {
                M.c.send_admitted += 1;
                if (n < s.len) M.c.send_partial += 1;
            },
        },
        .err => |err| switch (s.kind) {
            .recv => if (err == error.ConnectionResetByPeer) {
                M.c.recv_reset += 1;
            },
            .send => if (err == error.BrokenPipe) {
                M.c.send_epipe += 1;
            } else if (err == error.ConnectionResetByPeer) {
                M.c.send_reset += 1;
                if (s.state == .woken and s.wake != .none) M.c.send_reset_parked += 1;
            },
        },
    }
}

/// Submits a receive or a send on (`ci`, `side`) with the model's call-time
/// result decided first.
fn submit(kind: OpKind, ci: u8, side: u1, len: usize) void {
    const i = freeSlot() orelse fail("the slot pool is full", .{});
    const s = &M.slots[i];
    const e = &M.conns[ci].ends[side];
    s.* = .{ .used = true, .kind = kind, .serial = M.next_serial, .ci = ci, .side = side, .fd = e.fd, .len = len };
    M.next_serial += 1;
    switch (kind) {
        .recv => {
            s.recv = ev.NetRecv.init(e.fd, .fromSlices(&.{s.buf[0..len]}, &s.riov), .{});
            s.recv.c.callback = onDone;
            s.recv.c.flags.defer_callback = false;
        },
        .send => {
            for (s.buf[0..len], 0..) |*b, j| b.* = @truncate(s.serial *% 131 +% j *% 7);
            s.send = ev.NetSend.init(e.fd, .fromSlices(&.{s.buf[0..len]}, &s.siov), .{});
            s.send.c.callback = onDone;
            s.send.c.flags.defer_callback = false;
        },
    }
    // The call list: a send of 0 is 0; else the evaluation.
    const w: ?Want = switch (kind) {
        .recv => recvEval(ci, side, s),
        .send => if (len == 0) Want{ .n = 0 } else sendEval(ci, side, s, s.buf[0..len]),
    };
    if (w) |r| {
        s.want = r;
        s.state = .decided;
        countResult(s);
    } else {
        s.state = .parked;
        switch (kind) {
            .recv => M.c.recv_parked += 1,
            .send => M.c.send_parked += 1,
        }
    }
    if (len > 0 or kind == .recv) switch (kind) {
        .recv => e.recv = i,
        .send => e.send = i,
    };
    M.loop.add(switch (kind) {
        .recv => &s.recv.c,
        .send => &s.send.c,
    });
    if (M.expect_refusal) |t| fail("the sim did not refuse a call it should have: {s}", .{t});
}

// ---- Steps ----

fn stepSend() bool {
    const ci, const side = pickEnd() orelse return skip(.no_endpoint);
    if (M.conns[ci].ends[side].send != null) return skip(.unresolved);
    const len: usize = switch (rand(10)) {
        0 => 0,
        1, 2, 3 => 1 + rand(16),
        4, 5, 6 => 17 + rand(1500),
        else => 1517 + rand(buf_cap - 1517),
    };
    submit(.send, ci, side, len);
    return true;
}

fn stepRecv() bool {
    const ci, const side = pickEnd() orelse return skip(.no_endpoint);
    if (M.conns[ci].ends[side].recv != null) return skip(.unresolved);
    const len: usize = switch (rand(3)) {
        0 => 1 + rand(16),
        1 => 17 + rand(2000),
        else => buf_cap,
    };
    submit(.recv, ci, side, len);
    return true;
}

const SkipWhy = enum { no_endpoint, unresolved, no_parked, fixed_window };

fn skip(why: SkipWhy) bool {
    switch (why) {
        .no_endpoint => M.c.skip_no_endpoint += 1,
        .unresolved => M.c.skip_unresolved += 1,
        .no_parked => M.c.skip_no_parked += 1,
        .fixed_window => M.c.skip_fixed_window += 1,
    }
    return false;
}

fn shutdown(how: os_net.ShutdownHow) bool {
    const ci, const side = pickEnd() orelse return skip(.no_endpoint);
    const k = &M.conns[ci];
    const e = &k.ends[side];
    // The model's list first (design 17.8, shutdown).
    var want_err = false;
    if (e.reset_received) {
        want_err = true;
        M.c.shut_unconnected += 1;
    } else {
        const shut_rd = how == .both and !e.shut_rd;
        const discards = shut_rd and e.unread.items.len > 0;
        const p: Profile = if (discards) profilePoint("row 17, shutdown(.both) with unread bytes", true) else M.profile;
        if (!e.fin_out) {
            e.fin_out = true;
            sendEntry(ci, side, .fin, 0, &.{}, 0);
            wakeSend(ci, side, .shutdown);
        }
        if (shut_rd) {
            e.shut_rd = true;
            wakeRecv(ci, side, .shutdown);
            if (discards and p == .macos) {
                const n = discardUnread(ci, side, .both_discard);
                k.dirs[other(side)].reclaimed += n;
                M.c.releases += 1;
                creditOwed(ci, other(side));
            }
        }
    }
    var op = ev.NetShutdown.init(e.fd, how);
    M.setup_done[0] = false;
    op.c.callback = setupDone;
    op.c.flags.defer_callback = false;
    M.loop.add(&op.c);
    if (M.expect_refusal) |t| fail("the sim did not refuse a call it should have: {s}", .{t});
    pollOnce(0);
    if (!M.setup_done[0]) fail("a shutdown did not complete in a poll of 0", .{});
    if (op.getResult()) |_| {
        if (want_err) fail("shutdown({t}) on connection {d}: want SocketUnconnected, got ok", .{ how, k.id });
    } else |err| {
        if (!want_err or err != error.SocketUnconnected) fail("shutdown({t}) on connection {d}: got {t}", .{ how, k.id, err });
    }
    return true;
}

fn close(abortive: bool) bool {
    const ci, const side = pickEnd() orelse return skip(.no_endpoint);
    const k = &M.conns[ci];
    const e = &k.ends[side];
    if (abortive) {
        const lg: [2]c_int = .{ 1, 0 };
        os_net.setsockopt(e.fd, os_net.SOL.SOCKET, os_net.SO.LINGER, std.mem.asBytes(&lg)) catch |err| fail("SO_LINGER: {t}", .{err});
        e.linger0 = true;
    }
    // The model's close list (design 17.8).
    const quiet = e.reset_received or e.rst_sent;
    const had_unread = e.unread.items.len > 0;
    const rst = !quiet and (had_unread or e.linger0);
    const fin = !quiet and !rst and !e.fin_out;
    _ = discardUnread(ci, side, .unread_at_close);
    if (rst) {
        e.rst_sent = true;
        M.c.close_rst += 1;
        sendEntry(ci, side, .rst, 0, &.{}, 0);
    } else if (fin) {
        e.fin_out = true;
        M.c.close_fin += 1;
        sendEntry(ci, side, .fin, 0, &.{}, 0);
    } else M.c.close_quiet += 1;
    e.open = false;
    wakeRecv(ci, side, .close);
    wakeSend(ci, side, .close);
    var op = ev.NetClose.init(e.fd);
    runOp(&op.c, 0, 1);
    op.getResult() catch |err| fail("close: {t}", .{err});
    return true;
}

fn stepWindow() bool {
    const ci, const side = pickEnd() orelse return skip(.no_endpoint);
    const k = &M.conns[ci];
    const d = &k.dirs[other(side)];
    // Refused while the sender's send is woken and not yet harvested (a
    // refusal test has that case).
    if (k.ends[other(side)].send) |i| {
        if (M.slots[i].state == .woken) return skip(.unresolved);
    }
    const w: u32 = switch (rand(4)) {
        0 => @intCast(1 + rand(16)),
        1 => @intCast(17 + rand(4096)),
        2 => @intCast(4097 + rand(12288)),
        else => if (k.chunking == .fixed) return skip(.fixed_window) else sim.default_stream_window,
    };
    sim.setStreamWindow(k.ends[side].fd, w);
    const used = d.admitted - d.credited;
    if (used > w) M.c.shrink_below_use += 1;
    d.window = w;
    if (k.ends[other(side)].open and d.room() > 0) wakeSend(ci, other(side), .grow);
    return true;
}

fn stepChunking() bool {
    const ci, const side = pickEnd() orelse return skip(.no_endpoint);
    const mode = randomChunking(ci) orelse return skip(.fixed_window);
    sim.setStreamChunking(M.conns[ci].ends[side].fd, mode);
    M.conns[ci].chunking = mode;
    return true;
}

fn stepCancel() bool {
    var cand: [max_slots]u8 = undefined;
    var n: usize = 0;
    for (&M.slots, 0..) |*s, i| {
        if (s.used and s.state == .parked) {
            cand[n] = @intCast(i);
            n += 1;
        }
    }
    if (n == 0) return skip(.no_parked);
    const i = cand[rand(n)];
    const s = &M.slots[i];
    const e = &M.conns[s.ci].ends[s.side];
    s.state = .cancelled;
    switch (s.kind) {
        .recv => {
            M.c.cancel_recv += 1;
            e.recv = null;
            M.loop.cancel(&s.recv.c);
        },
        .send => {
            M.c.cancel_send += 1;
            e.send = null;
            M.loop.cancel(&s.send.c);
        },
    }
    pollOnce(0);
    if (M.slots[i].used and M.slots[i].serial == s.serial) fail("a cancelled {t} op #{d} did not complete in a poll of 0", .{ s.kind, s.serial });
    return true;
}

/// One poll: the clock moves by at most its cap and at most to the model's
/// next event. Every woken op must complete in it.
fn pollOnce(cap_ns: u64) void {
    advanceTo(now());
    const before = now();
    const next = nextEvent();
    M.in_poll = true;
    M.batch = 0;
    M.loop.poll(.fromNanoseconds(cap_ns)) catch |e| fail("poll: {t}", .{e});
    M.in_poll = false;
    M.c.polls += 1;
    if (M.batch > 1) M.c.batches += 1;
    const after = now();
    if (after < before) fail("a poll moved the clock back, from {d} ns to {d} ns", .{ before, after });
    if (after - before > cap_ns) fail("a poll capped at {d} ns moved the clock {d} ns", .{ cap_ns, after - before });
    if (next) |t| {
        if (t >= before and after > t) fail("a poll moved the clock to {d} ns, past the model's next event at {d} ns", .{ after, t });
    }
    advanceTo(after);
    for (&M.slots) |*s| {
        if (s.used and (s.state == .woken or s.state == .decided or s.state == .cancelled)) {
            // An op decided at the call, or woken by now, is harvested in
            // this poll; one woken by a landing at `after` too.
            fail("{t} op #{d} ({t}) did not complete in the poll", .{ s.kind, s.serial, s.state });
        }
    }
}

fn settle() void {
    const t0 = now();
    const span = (1 + rand(5)) * L;
    var n: usize = 0;
    while (now() < t0 + span) : (n += 1) {
        if (n > 10000) fail("settle did not move the clock", .{});
        pollOnce(t0 + span - now());
    }
}

fn runStep(kind: StepKind) bool {
    return switch (kind) {
        .send => stepSend(),
        .recv => stepRecv(),
        .poll => blk: {
            pollOnce(switch (rand(3)) {
                0 => 0,
                1 => 1 + rand(50) * us,
                else => 100 * us + rand(200) * us,
            });
            break :blk true;
        },
        .settle => blk: {
            settle();
            break :blk true;
        },
        .shut_send => shutdown(.send),
        .shut_both => shutdown(.both),
        .close => close(false),
        .abort => close(true),
        .window => stepWindow(),
        .chunking => stepChunking(),
        .cancel => stepCancel(),
        .connect => connect(),
    };
}

fn pickStep() StepKind {
    var total: u32 = 0;
    for (step_weights) |w| total += w;
    var r = M.rng.random().uintLessThan(u32, total);
    for (step_weights, 0..) |w, i| {
        if (r < w) return @fromBackingInt(@as(u4, @intCast(i)));
        r -= w;
    }
    unreachable;
}

// ---- Checks ----

fn retirable(k: *const MConn, ci: usize) bool {
    if (k.ends[0].open or k.ends[1].open) return false;
    for (M.wire.items) |w| {
        if (w.ci == ci) return false;
    }
    return k.dirs[0].owed_at == null and k.dirs[1].owed_at == null;
}

fn checkState() void {
    var in_flight: usize = M.wire.items.len;
    for (&M.conns, 0..) |*k, ci| {
        if (!k.used) continue;
        if (retirable(k, ci)) {
            if (sim.testConnEnd(k.id, false) != null) fail("connection {d} is not retired, and the model has nothing of it left", .{k.id});
            for (&k.ends) |*e| e.unread.deinit(M.arena.allocator());
            k.used = false;
            M.st.retired += 1;
            continue;
        }
        for (k.dirs) |d| in_flight += @intFromBool(d.owed_at != null);
        for (0..2) |side| {
            const s: u1 = @intCast(side);
            const e = &k.ends[s];
            const got = sim.testConnEnd(k.id, s == 1) orelse fail("connection {d} retired, and the model still has it", .{k.id});
            const phase: sim.StreamPhase = if (e.open) .est else .dead;
            if (got.phase != phase or got.fin_in != e.fin_in or got.fin_out != e.fin_out or got.rst_sent != e.rst_sent or got.reset_received != e.reset_received or
                got.fin_before_reset != e.fin_before_reset or got.shut_rd != e.shut_rd or got.reset_reported != e.reset_reported or got.unread != e.unread.items.len)
            {
                fail("connection {d} side {d}: sim {any}, model phase {t} fin_in {} fin_out {} rst_sent {} reset_received {} fin_before_reset {} shut_rd {} reset_reported {} unread {d}", .{ k.id, s, got, phase, e.fin_in, e.fin_out, e.rst_sent, e.reset_received, e.fin_before_reset, e.shut_rd, e.reset_reported, e.unread.items.len });
            }
            const d = k.dirs[s];
            const g = sim.testStreamDir(k.id, s == 0) orelse unreachable;
            var fl: u64 = 0;
            for (M.wire.items) |w| {
                if (w.ci == ci and w.dir == s and w.kind == .data) fl += w.bytes.len;
            }
            if (g.window != d.window or g.admitted != d.admitted or g.credited != d.credited or g.read != d.read or g.reclaimed != d.reclaimed or
                !std.mem.eql(u64, &g.discarded, &d.discarded) or g.in_flight != fl or g.unread != k.ends[other(s)].unread.items.len or
                g.data_send_index != d.data_send_index or g.owed != (d.owed_at != null))
            {
                fail("connection {d} direction from side {d}: sim {any}, model window {d} admitted {d} credited {d} read {d} reclaimed {d} discarded {any} in flight {d} data_send_index {d} owed {}", .{ k.id, s, g, d.window, d.admitted, d.credited, d.read, d.reclaimed, d.discarded, fl, d.data_send_index, d.owed_at != null });
            }
            if (!g.conserved()) fail("connection {d} direction from side {d}: not conserved: {any}", .{ k.id, s, g });
            if (g.credited > g.read + g.reclaimed) fail("connection {d} direction from side {d}: credited {d} > read {d} + reclaimed {d}", .{ k.id, s, g.credited, g.read, g.reclaimed });
        }
    }
    const sim_n = sim.streamEntriesInFlight();
    if (sim_n != in_flight) fail("TCP entries in flight: sim {d}, model {d}", .{ sim_n, in_flight });
    checkStats();
}

/// Every field of `sim.streamStats()` against the model's count, by
/// reflection: a field added to `StreamStats` is compared from its first
/// run, and fails here until the model counts it. The handshake's fields
/// (`syn_dropped_dead`, `refused`) stay 0: the model only joins a
/// connection after its accept.
fn checkStats() void {
    const got = sim.streamStats();
    inline for (@typeInfo(sim.StreamStats).@"struct".field_names) |name| {
        if (!std.meta.eql(@field(got, name), @field(M.st, name))) fail("streamStats().{s}: sim {any}, model {any}", .{ name, @field(got, name), @field(M.st, name) });
    }
}

// ---- The end ----

fn endProofs() void {
    // Settle everything in flight first.
    var n: usize = 0;
    while (nextEvent() != null) : (n += 1) {
        if (n > 100000) fail("the wire did not drain", .{});
        pollOnce(10 * L);
    }
    // Each op still parked, alone: give it what it waits for.
    for (&M.slots, 0..) |*s, i| {
        if (!s.used or s.state != .parked) continue;
        const k = &M.conns[s.ci];
        const peer = &k.ends[other(s.side)];
        switch (s.kind) {
            .recv => {
                // A byte from the peer, if it can still send one.
                if (!peer.open or peer.fin_out or peer.reset_received or peer.send != null or k.dirs[other(s.side)].room() == 0) continue;
                submit(.send, s.ci, other(s.side), 1);
            },
            .send => {
                // One more byte of window, if the receiver is still there.
                if (!peer.open) continue;
                const d = &k.dirs[s.side];
                const w: u32 = @intCast(@min(d.admitted - d.credited + 1, sim.max_stream_window));
                if (w <= d.window) continue;
                sim.setStreamWindow(peer.fd, w);
                d.window = w;
                if (d.room() > 0) wakeSend(s.ci, s.side, .grow);
            },
        }
        M.c.end_proofs += 1;
        var p: usize = 0;
        while (M.slots[i].used and M.slots[i].serial == s.serial) : (p += 1) {
            if (p > 1000) fail("an end proof: parked {t} op #{d} on connection {d} never completed", .{ s.kind, s.serial, k.id });
            pollOnce(10 * L);
        }
    }
    // Close everything; ops still parked end FileDescriptorNotASocket.
    for (&M.conns) |*k| {
        if (!k.used) continue;
        for (&k.ends, 0..) |*e, side| {
            if (!e.open) continue;
            _ = side;
            closeEnd(k, e);
        }
    }
    n = 0;
    while (nextEvent() != null or anySlot()) : (n += 1) {
        if (n > 100000) fail("the end did not drain", .{});
        pollOnce(10 * L);
    }
    checkState();
    for (M.conns) |k| {
        if (k.used) fail("connection {d} did not retire at the end", .{k.id});
    }
    if (sim.streamEntriesInFlight() != 0) fail("{d} TCP entries in flight at the end", .{sim.streamEntriesInFlight()});
    var op = ev.NetClose.init(M.listener);
    runOp(&op.c, 0, 1);
}

fn anySlot() bool {
    for (M.slots) |s| {
        if (s.used) return true;
    }
    return false;
}

fn closeEnd(k: *MConn, e: *MEnd) void {
    // The same as a close step on this endpoint.
    for (&M.conns, 0..) |*kk, ci| {
        if (kk != k) continue;
        const side: u1 = if (e == &kk.ends[0]) 0 else 1;
        const quiet = e.reset_received or e.rst_sent;
        const rst = !quiet and (e.unread.items.len > 0 or e.linger0);
        const fin = !quiet and !rst and !e.fin_out;
        _ = discardUnread(@intCast(ci), side, .unread_at_close);
        if (rst) {
            e.rst_sent = true;
            sendEntry(@intCast(ci), side, .rst, 0, &.{}, 0);
        } else if (fin) {
            e.fin_out = true;
            sendEntry(@intCast(ci), side, .fin, 0, &.{}, 0);
        }
        e.open = false;
        wakeRecv(@intCast(ci), side, .close);
        wakeSend(@intCast(ci), side, .close);
        var op = ev.NetClose.init(e.fd);
        runOp(&op.c, 0, 1);
        op.getResult() catch |err| fail("close: {t}", .{err});
        return;
    }
}

// ---- A seed ----

fn runSeed(seed_value: u64, steps: usize, profile: Profile, child: bool) Counts {
    M.seed = seed_value;
    M.step = 0;
    M.profile = profile;
    M.rng = .init(seed_value ^ 0x7374_7265_616d);
    M.arena = .init(std.heap.page_allocator);
    defer M.arena.deinit();
    M.conns = @splat(.{});
    M.n_conns = 0;
    M.wire = .empty;
    M.next_order = 0;
    for (&M.slots) |*s| s.used = false;
    M.next_serial = 1;
    M.c = .{};
    M.st = .{};
    M.np_step = null;
    M.np_text = "";
    M.expect_refusal = null;
    M.in_poll = false;

    sim.begin(seed_value);
    defer sim.end();
    switch (profile) {
        .linux => sim.setStreamProfile(.linux),
        .macos => sim.setStreamProfile(.macos),
        .none => {},
    }
    M.model_clock = now();
    M.loop.init(.{}) catch |e| fail("loop init: {t}", .{e});
    defer M.loop.deinit();
    M.keepalive = ev.Timer.init(.{ .duration = .fromSeconds(1_000_000) });
    M.keepalive.c.callback = keepaliveDone;
    M.keepalive_done = false;
    M.loop.add(&M.keepalive.c);
    openListener();
    for (0..3) |_| _ = connect();
    checkState();

    while (M.step < steps) : (M.step += 1) {
        if (child) progress();
        const kind = pickStep();
        if (runStep(kind)) M.c.ran[@backingInt(kind)] += 1 else M.c.skipped[@backingInt(kind)] += 1;
        checkState();
    }
    M.step = steps;
    if (child) progress();
    endProofs();
    M.loop.cancel(&M.keepalive.c);
    var n: usize = 0;
    while (!M.keepalive_done) : (n += 1) {
        if (n > 10) fail("the keepalive timer did not end", .{});
        M.loop.poll(.fromNanoseconds(0)) catch |e| fail("poll: {t}", .{e});
    }
    // One line per seed, compared between two processes by the parent.
    std.debug.print("stream model digest: profile {t} seed {d}: trace {x:0>16} wire {x:0>16} state {x:0>16}\n", .{ profile, seed_value, sim.traceHash(), sim.wireDigest(), sim.stateDigest() });
    if (M.np_step) |k| {
        std.debug.print("stream model no-profile: seed {d}: step {d}: {s}\n", .{ seed_value, k, M.np_text });
    } else std.debug.print("stream model no-profile: seed {d}: none\n", .{seed_value});
    return M.c;
}

fn envU64(name: [*:0]const u8) ?u64 {
    const v = std.c.getenv(name) orelse return null;
    return std.fmt.parseInt(u64, std.mem.span(v), 10) catch @panic("stream model: a number was expected in the environment variable");
}

fn envProfile() Profile {
    const v = std.c.getenv("ZIO_STREAM_MODEL_PROFILE") orelse @panic("stream model: ZIO_STREAM_MODEL_PROFILE is linux, macos or none");
    return std.meta.stringToEnum(Profile, std.mem.span(v)) orelse @panic("stream model: ZIO_STREAM_MODEL_PROFILE is linux, macos or none");
}

fn printCounts(c: Counts, profile: Profile, seeds: usize, steps: usize) void {
    std.debug.print("stream model counts ({t}, {d} seeds x {d} steps):\n", .{ profile, seeds, steps });
    inline for (@typeInfo(StepKind).@"enum".field_names, 0..) |name, i| {
        std.debug.print("  step {s}: ran {d}, skipped {d}\n", .{ name, c.ran[i], c.skipped[i] });
    }
    std.debug.print("  skips: no endpoint {d}, op unresolved {d}, no parked op {d}, connection limit {d}, .fixed with a large window {d}\n", .{ c.skip_no_endpoint, c.skip_unresolved, c.skip_no_parked, c.skip_conn_limit, c.skip_fixed_window });
    const info = @typeInfo(Counts).@"struct";
    inline for (info.field_names, info.field_types) |name, T| {
        if (comptime T == u64 and !std.mem.startsWith(u8, name, "skip_")) std.debug.print("  {s}: {d}\n", .{ name, @field(c, name) });
    }
    inline for (@typeInfo(Wake).@"enum".field_names, 0..) |name, i| {
        if (i != 0) std.debug.print("  woken by {s}: {d}\n", .{ name, c.woken[i] });
    }
    std.debug.print("  sends by chunk mode: hashed {d}, fixed {d}, off {d}\n", .{ c.chunks[0], c.chunks[1], c.chunks[2] });
}

/// The counts that must not be 0 over a profile's seeds: a run where one
/// is 0 did not test what it claims.
fn floors(c: Counts, profile: Profile) void {
    const req = .{
        "send_admitted",    "send_partial",     "send_zero",    "send_parked", "send_epipe",  "send_reset",    "send_reset_parked",
        "recv_data",        "recv_eof",         "recv_reset",   "recv_parked", "closed_op",   "cancel_recv",   "cancel_send",
        "shut_unconnected", "close_fin",        "close_rst",    "close_quiet", "rst_answers", "stale_credits", "owed_departed",
        "credits_landed",   "shrink_below_use", "shared_reset", "batches",     "end_proofs",
    };
    inline for (req) |name| {
        if (@field(c, name) == 0) fail("count {s} is 0 over the seeds of profile {t}: the run did not test it", .{ name, profile });
    }
    inline for (@typeInfo(StepKind).@"enum".field_names, 0..) |name, i| {
        if (c.ran[i] == 0) fail("step {s} never ran under profile {t}", .{ name, profile });
    }
    inline for (@typeInfo(Wake).@"enum".field_names, 0..) |name, i| {
        // A local reset wakes nothing: after its own .both an endpoint's
        // receive gives 0 at once and its send BrokenPipe, so neither parks.
        const needed = i != 0 and !std.mem.eql(u8, name, "local_reset");
        if (needed and c.woken[i] == 0) fail("no op was woken by {s} under profile {t}", .{ name, profile });
    }
    for (c.chunks, 0..) |v, i| {
        if (v == 0) fail("no send in chunk mode {d} under profile {t}", .{ i, profile });
    }
    switch (profile) {
        .linux => if (c.local_resets == 0) fail("no local reset under .linux", .{}),
        .macos => if (c.releases == 0) fail("no window release under .macos", .{}),
        .none => {},
    }
}

test "sim: stream op model: connections under random sends, receives, shutdowns, closes, window and chunk changes and cancels, per profile and with none, in a child process" {
    if (comptime !sim.compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    if (!sim.isDeathChild()) return;
    sim.noCoreDump();
    const profile = envProfile();
    const steps: usize = @intCast(envU64("ZIO_STREAM_MODEL_STEPS") orelse default_steps);
    var total: Counts = .{};
    if (envU64("ZIO_STREAM_MODEL_SEED")) |one| {
        total.add(runSeed(one, steps, profile, true));
        printCounts(total, profile, 1, steps);
        sim.childDone(child_name);
        return;
    }
    for (1..n_seeds + 1) |s| total.add(runSeed(s, steps, profile, true));
    printCounts(total, profile, n_seeds, steps);
    if (std.c.getenv("ZIO_STREAM_MODEL_STEPS") == null) floors(total, profile);
    sim.childDone(child_name);
}

const Child = struct { term: std.process.Child.Term, stderr: []u8 };

fn runChild(gpa: std.mem.Allocator, profile: []const u8, seed: ?u64) !Child {
    const io = std.testing.io;
    var exe_buf: [std.fs.max_path_bytes]u8 = undefined;
    const exe_len = try std.process.executablePath(io, &exe_buf);
    var env: std.process.Environ.Map = .init(gpa);
    defer env.deinit();
    if (std.c.getenv("PATH")) |path| try env.put("PATH", std.mem.span(path));
    try env.put("TEST_FILTER", child_name);
    try env.put(sim.death_child_env, "1");
    try env.put("ZIO_STREAM_MODEL_PROFILE", profile);
    if (std.c.getenv("ZIO_STREAM_MODEL_STEPS")) |v| try env.put("ZIO_STREAM_MODEL_STEPS", std.mem.span(v));
    var sb: [24]u8 = undefined;
    if (seed) |s| try env.put("ZIO_STREAM_MODEL_SEED", try std.fmt.bufPrint(&sb, "{d}", .{s}));
    const res = try std.process.run(gpa, io, .{
        .argv = &.{exe_buf[0..exe_len]},
        .environ_map = &env,
        .stdout_limit = .limited(1 << 20),
        .stderr_limit = .limited(64 << 20),
        .timeout = .{ .duration = .{ .raw = .fromSeconds(600), .clock = .awake } },
    });
    gpa.free(res.stdout);
    return .{ .term = res.term, .stderr = res.stderr };
}

/// The lines of `stderr` that start with `prefix`, joined.
fn linesWith(gpa: std.mem.Allocator, stderr: []const u8, prefix: []const u8) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    var it = std.mem.splitScalar(u8, stderr, '\n');
    while (it.next()) |line| {
        if (std.mem.indexOf(u8, line, prefix)) |at| {
            try out.appendSlice(gpa, line[at..]);
            try out.append(gpa, '\n');
        }
    }
    return out.toOwnedSlice(gpa);
}

fn lastProgress(stderr: []const u8) []const u8 {
    var last: []const u8 = "none";
    var it = std.mem.splitScalar(u8, stderr, '\n');
    while (it.next()) |line| {
        if (std.mem.indexOf(u8, line, "stream model: seed ")) |at| last = line[at..];
    }
    return last;
}

/// A child's own output, minus the per-step progress lines.
fn printChild(stderr: []const u8) void {
    var it = std.mem.splitScalar(u8, stderr, '\n');
    while (it.next()) |line| {
        if (std.mem.indexOf(u8, line, "stream model: seed ") != null) continue;
        if (std.mem.indexOf(u8, line, "stream model") != null or std.mem.indexOf(u8, line, "panic") != null) std.debug.print("{s}\n", .{line});
    }
}

test "sim: stream op model: connections under random sends, receives, shutdowns, closes, window and chunk changes and cancels, per profile and with none" {
    if (comptime !sim.compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    if (sim.isDeathChild()) return;
    const gpa = std.testing.allocator;
    var np: [2][]u8 = undefined;
    var np_n: usize = 0;
    defer for (np[0..np_n]) |x| gpa.free(x);
    for ([_][]const u8{ "linux", "macos" }) |p| {
        var digests: [2][]u8 = undefined;
        var dn: usize = 0;
        defer for (digests[0..dn]) |x| gpa.free(x);
        for (0..2) |run| {
            const r = try runChild(gpa, p, null);
            defer gpa.free(r.stderr);
            if (run == 0) printChild(r.stderr);
            if (!r.term.success()) {
                std.debug.print("stream model child ({s}): term {any}; the last step it started: {s}\n", .{ p, r.term, lastProgress(r.stderr) });
                printChild(r.stderr);
                return error.StreamModelFailed;
            }
            try sim.checkDoneMarkers(child_name, r.stderr);
            digests[dn] = try linesWith(gpa, r.stderr, "stream model digest: ");
            dn += 1;
            if (run == 0) {
                np[np_n] = try linesWith(gpa, r.stderr, "stream model no-profile: ");
                np_n += 1;
            }
        }
        if (!std.mem.eql(u8, digests[0], digests[1])) {
            std.debug.print("two processes, profile {s}, differ:\n{s}\n----\n{s}\n", .{ p, digests[0], digests[1] });
            return error.ProcessesDiffer;
        }
        std.debug.print("stream model: profile {s}: two processes give the same {d} digest lines\n", .{ p, std.mem.count(u8, digests[0], "\n") });
    }
    if (std.c.getenv("ZIO_STREAM_MODEL_SEED") != null or std.c.getenv("ZIO_STREAM_MODEL_STEPS") != null) return;
    // The no-profile shadow: both profiles predict the same point per seed.
    if (!std.mem.eql(u8, np[0], np[1])) {
        std.debug.print("the no-profile shadows differ:\n.linux:\n{s}\n.macos:\n{s}\n", .{ np[0], np[1] });
        return error.ShadowsDiffer;
    }
    // Then a run with no profile per seed must panic there, or finish.
    var panics: usize = 0;
    var it = std.mem.splitScalar(u8, np[0], '\n');
    while (it.next()) |line| {
        if (line.len == 0) continue;
        // "stream model no-profile: seed S: step K: <text>" or "...: none".
        var parts = std.mem.splitSequence(u8, line["stream model no-profile: seed ".len..], ": ");
        const seed = try std.fmt.parseInt(u64, parts.next().?, 10);
        const rest = parts.rest();
        const r = try runChild(gpa, "none", seed);
        defer gpa.free(r.stderr);
        if (std.mem.eql(u8, rest, "none")) {
            if (!r.term.success()) {
                std.debug.print("no-profile seed {d}: predicted no panic; term {any}; last step {s}\n", .{ seed, r.term, lastProgress(r.stderr) });
                printChild(r.stderr);
                return error.NoProfileRunFailed;
            }
            try sim.checkDoneMarkers(child_name, r.stderr);
            continue;
        }
        const k_end = std.mem.indexOf(u8, rest, ": ").?;
        const step = rest["step ".len..k_end];
        const text = rest[k_end + 2 ..];
        var want_buf: [256]u8 = undefined;
        const want = try std.fmt.bufPrint(&want_buf, "sim: {s}: the kernels differ here", .{text});
        var at_buf: [64]u8 = undefined;
        const at = try std.fmt.bufPrint(&at_buf, "stream model: seed {d} step {s}", .{ seed, step });
        const died = r.term == .signal;
        if (!died or !sim.hasPanicLine(r.stderr, want) or !std.mem.eql(u8, lastProgress(r.stderr), at)) {
            std.debug.print("no-profile seed {d}: wanted the panic \"{s}\" in \"{s}\"; term {any}; last step {s}\n", .{ seed, want, at, r.term, lastProgress(r.stderr) });
            printChild(r.stderr);
            return error.NoProfileRunDiffers;
        }
        panics += 1;
        std.debug.print("no-profile seed {d}: panicked at step {s}: {s}\n", .{ seed, step, text });
    }
    if (panics == 0) return error.NoProfilePointEverReached;
}
