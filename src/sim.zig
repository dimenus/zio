// SPDX-FileCopyrightText: 2025 Lukáš Lalinský
// SPDX-License-Identifier: MIT
//
// Deterministic simulation (DST) support. Compiled in only when
// `zio_options.sim` is true. Production builds keep the option off, so these
// paths do not exist there.

const std = @import("std");
const builtin = @import("builtin");
const zio_options = @import("zio_options");

pub const compiled_in = zio_options.sim;

pub const EventKind = enum(u8) {
    task_switch = 1,
    wake = 2,
    timer_fire = 3,
    cq_pop = 4,
    io_complete = 5,
};

const TaskIdEnt = struct { ptr: usize, id: u32 };

var begun: bool = false;
var clock_routed: bool = true;
var current_seed: u64 = 0;
var clock_ns: u64 = 0;
var rng: std.Random.DefaultPrng = undefined;
var hasher: std.hash.Wyhash = undefined;
var event_count: u64 = 0;
var next_task_id: u32 = 1;
var task_ids: [2048]TaskIdEnt = undefined;
var task_id_len: u32 = 0;

const max_ends = 32;
const max_due = 32;
pub const pipe_buf_cap = 4096;
const fd_base: i32 = 100000;

const Parked = struct {
    c: *anyopaque,
    id: u32,
};

const End = struct {
    used: bool = false,
    peer: u8 = 0,
    closed: bool = false,
    buf: [pipe_buf_cap]u8 = undefined,
    buf_len: usize = 0,
    pending_recv: ?Parked = null,
    pending_send: ?Parked = null,
};

var ends: [max_ends]End = @splat(.{});

pub const DueKind = enum(u8) {
    recv = 1,
    send = 2,
    close = 3,
};

const Due = struct {
    c: *anyopaque,
    due_at: u64,
    kind: DueKind,
    id: u32,
};

pub const TakenDue = struct {
    c: *anyopaque,
    id: u32,
};

var due: [max_due]Due = undefined;
var due_len: usize = 0;
var next_io_id: u32 = 0;

pub fn isBegun() bool {
    return begun;
}

pub fn seed() u64 {
    return current_seed;
}

pub fn begin(seed_value: u64) void {
    if (comptime !compiled_in) {
        @panic("sim: not compiled in (build zio with -Dsim=true)");
    }
    if (begun) panic("sim: begin() called twice without end()", .{});
    begun = true;
    clock_routed = true;
    current_seed = seed_value;
    clock_ns = 0;
    rng = std.Random.DefaultPrng.init(seed_value);
    hasher = std.hash.Wyhash.init(0);
    event_count = 0;
    next_task_id = 1;
    task_id_len = 0;
    resetIo();
}

fn resetIo() void {
    ends = @splat(.{});
    due_len = 0;
    next_io_id = 0;
}

pub fn end() void {
    begun = false;
    clock_routed = true;
}

pub fn setClockRouted(routed: bool) void {
    clock_routed = routed;
}

/// Unix-scale offset for `.real`, matching the #717 witness: a `.real`
/// timestamp is ~1.7e18 ns and must not sit on the awake heap.
pub const real_epoch_ns: u64 = 1_700_000_000_000_000_000;

/// Logical time in nanoseconds. Panics if sim mode is on but the clock is
/// not routed here (the D4 trip).
pub fn nowNs() u64 {
    if (comptime !compiled_in) unreachable;
    if (!begun) panic("sim: clock read before begin", .{});
    if (!clock_routed) panic("sim: real clock_gettime", .{});
    return clock_ns;
}

/// Wall-clock now. `clock_idx` is `@intFromEnum(time.Clock)`. `.real` (2)
/// lives in a distinct epoch so `AutoCancel.setClock(..., .real)` is
/// load-bearing: a `.real` deadline on the awake heap never fires.
pub fn nowNsFor(clock_idx: u8) u64 {
    return nowNs() + epochNs(clock_idx);
}

pub fn epochNs(clock_idx: u8) u64 {
    return if (clock_idx == 2) real_epoch_ns else 0;
}

pub fn advanceNs(ns: u64) void {
    if (comptime !compiled_in) return;
    if (!begun) panic("sim: advance before begin", .{});
    clock_ns +|= ns;
}

pub fn pickIndex(n: usize) usize {
    if (comptime !compiled_in) unreachable;
    if (!begun) panic("sim: pick before begin", .{});
    if (n == 0) panic("sim: pickIndex(0)", .{});
    if (n == 1) return 0;
    return rng.random().uintLessThan(usize, n);
}

pub fn taskId(ptr: usize) u32 {
    if (comptime !compiled_in) return 0;
    if (!begun) return 0;
    var i: u32 = 0;
    while (i < task_id_len) : (i += 1) {
        if (task_ids[i].ptr == ptr) return task_ids[i].id;
    }
    if (task_id_len >= task_ids.len) panic("sim: task id table full", .{});
    const id = next_task_id;
    next_task_id += 1;
    task_ids[task_id_len] = .{ .ptr = ptr, .id = id };
    task_id_len += 1;
    return id;
}

pub fn emit(kind: EventKind, a: u32, b: u32) void {
    if (comptime !compiled_in) return;
    if (!begun) return;
    var word: [8]u8 = undefined;
    word[0] = @intFromEnum(kind);
    std.mem.writeInt(u24, word[1..4], @truncate(a), .little);
    std.mem.writeInt(u32, word[4..8], b, .little);
    hasher.update(&word);
    event_count += 1;
}

pub fn traceHash() u64 {
    return hasher.final();
}

pub fn eventCount() u64 {
    return event_count;
}

pub fn clockNs() u64 {
    return clock_ns;
}

/// Final-state digest: clock, event count, task ids, the trace hash, and
/// sim I/O (pipe ends and due list). Does not include the RNG stream.
pub fn stateDigest() u64 {
    var h = std.hash.Wyhash.init(1);
    var buf: [32]u8 = undefined;
    std.mem.writeInt(u64, buf[0..8], clock_ns, .little);
    std.mem.writeInt(u64, buf[8..16], event_count, .little);
    std.mem.writeInt(u32, buf[16..20], next_task_id, .little);
    std.mem.writeInt(u32, buf[20..24], task_id_len, .little);
    std.mem.writeInt(u64, buf[24..32], hasher.final(), .little);
    h.update(&buf);
    var i: u8 = 0;
    while (i < max_ends) : (i += 1) {
        const e = ends[i];
        var eb: [8]u8 = undefined;
        eb[0] = @intFromBool(e.used);
        eb[1] = @intFromBool(e.closed);
        eb[2] = e.peer;
        eb[3] = @intFromBool(e.pending_recv != null);
        eb[4] = @intFromBool(e.pending_send != null);
        std.mem.writeInt(u16, eb[5..7], @truncate(e.buf_len), .little);
        eb[7] = 0;
        h.update(&eb);
        var ids: [8]u8 = undefined;
        std.mem.writeInt(u32, ids[0..4], if (e.pending_recv) |p| p.id else 0, .little);
        std.mem.writeInt(u32, ids[4..8], if (e.pending_send) |p| p.id else 0, .little);
        h.update(&ids);
        if (e.used and e.buf_len > 0) {
            h.update(e.buf[0..e.buf_len]);
        }
    }
    std.mem.writeInt(u32, buf[0..4], next_io_id, .little);
    std.mem.writeInt(u64, buf[4..12], due_len, .little);
    h.update(buf[0..12]);
    var d: usize = 0;
    while (d < due_len) : (d += 1) {
        std.mem.writeInt(u64, buf[0..8], due[d].due_at, .little);
        std.mem.writeInt(u32, buf[8..12], due[d].id, .little);
        buf[12] = @intFromEnum(due[d].kind);
        h.update(buf[0..13]);
    }
    return h.final();
}

pub fn panic(comptime fmt: []const u8, args: anytype) noreturn {
    if (comptime compiled_in) {
        const h = if (begun) hasher.final() else 0;
        std.debug.print(
            "TRACE_HASH={x:0>16} SEED={d} events={d}\n",
            .{ h, current_seed, event_count },
        );
    }
    std.debug.panic(fmt, args);
}

pub fn protocolPanic(comptime msg: []const u8) noreturn {
    panic("{s}", .{msg});
}

pub fn deadlock() noreturn {
    panic("sim: deadlock clock_ns={d} events={d}", .{ clock_ns, event_count });
}

pub fn forbidKernelFutex() void {
    if (comptime !compiled_in) return;
    if (!begun) return;
    panic("sim: real kernel futex", .{});
}

pub fn forbidBackendPoll() noreturn {
    panic("sim: real backend.poll", .{});
}

pub fn printScope() void {
    std.debug.print("SCOPE simulated: clock, futex_park, task_pick, timer_heap, cq, executor_csprng, real_epoch, net_pipe, extra_logical_executors\n", .{});
    std.debug.print("SCOPE real: allocator, libc\n", .{});
    if (builtin.os.tag.isDarwin()) {
        std.debug.print("SCOPE unsimulated: file_io, connect_accept, extra_os_threads, dns, boot_vs_awake (boot==awake), spawn_blocking (panics), backend_kernel (poll never called; Loop.init does not open a kqueue fd)\n", .{});
    } else {
        std.debug.print("SCOPE unsimulated: file_io, connect_accept, extra_os_threads, dns, boot_vs_awake (boot==awake), spawn_blocking (panics), backend_kernel (poll never called)\n", .{});
    }
}

fn endIndex(fd: i32) ?u8 {
    if (fd < fd_base) return null;
    const i: usize = @intCast(fd - fd_base);
    if (i >= max_ends) return null;
    if (!ends[i].used) return null;
    return @intCast(i);
}

fn allocEnd() u8 {
    var i: u8 = 0;
    while (i < max_ends) : (i += 1) {
        if (!ends[i].used) {
            ends[i] = .{ .used = true };
            return i;
        }
    }
    panic("sim: pipe table full", .{});
}

/// Bidirectional pair (socketpair shape). No kernel fd.
pub fn pipePair() [2]i32 {
    if (comptime !compiled_in) unreachable;
    if (!begun) panic("sim: pipePair before begin", .{});
    const a = allocEnd();
    const b = allocEnd();
    ends[a].peer = b;
    ends[b].peer = a;
    return .{ fd_base + a, fd_base + b };
}

fn ioDelayNs() u64 {
    // Never zero: immediate due is harvested with timeout=0, which is not
    // the mid-sleep I/O window (kevent returns timed_out=false).
    return if (pickIndex(2) == 0) 1_000_000 else 5_000_000;
}

fn allocOpId() u32 {
    next_io_id += 1;
    return next_io_id;
}

fn pushDueKind(c: *anyopaque, kind: DueKind, id: u32) void {
    if (due_len >= max_due) panic("sim: I/O due list full", .{});
    due[due_len] = .{
        .c = c,
        .due_at = clock_ns + ioDelayNs(),
        .kind = kind,
        .id = id,
    };
    due_len += 1;
}

pub fn hasDueIo() bool {
    for (due[0..due_len]) |d| {
        if (d.due_at <= clock_ns) return true;
    }
    return false;
}

/// Remaining ns until the next not-yet-due I/O, or null.
pub fn nextDueIoRemaining() ?u64 {
    var min_at: ?u64 = null;
    for (due[0..due_len]) |d| {
        if (d.due_at <= clock_ns) continue;
        if (min_at == null or d.due_at < min_at.?) min_at = d.due_at;
    }
    if (min_at) |at| return at - clock_ns;
    return null;
}

pub fn hasParkedIo() bool {
    for (&ends) |e| {
        if (e.used and (e.pending_recv != null or e.pending_send != null)) return true;
    }
    return false;
}

pub const IoSubmit = union(enum) {
    due: usize,
    parked,
    would_block,
    eof,
    bad_fd,
};

/// Copy `src` into the peer's recv buffer.
pub fn sendBytes(fd: i32, src: []const u8, send_c: *anyopaque) IoSubmit {
    if (endIndex(fd) == null) return .bad_fd;
    return copySend(fd, src, send_c, allocOpId(), true);
}

/// Harvest a capacity-woken send: copy without re-queueing this completion.
/// `id` is the submit-time id from the due entry.
pub fn harvestSend(fd: i32, src: []const u8, send_c: *anyopaque, id: u32) IoSubmit {
    return copySend(fd, src, send_c, id, false);
}

fn copySend(fd: i32, src: []const u8, send_c: *anyopaque, id: u32, schedule: bool) IoSubmit {
    const i = endIndex(fd) orelse return .bad_fd;
    if (ends[i].closed) return .eof;
    const p = ends[i].peer;
    if (ends[p].closed) return .eof;
    if (src.len == 0) {
        if (schedule) pushDueKind(send_c, .send, id);
        return .{ .due = 0 };
    }
    const space = pipe_buf_cap - ends[p].buf_len;
    if (space == 0) {
        if (ends[i].pending_send != null) panic("sim: two sends parked on one fd", .{});
        ends[i].pending_send = .{ .c = send_c, .id = id };
        return .parked;
    }
    const n = @min(src.len, space);
    @memcpy(ends[p].buf[ends[p].buf_len..][0..n], src[0..n]);
    ends[p].buf_len += n;
    if (schedule) pushDueKind(send_c, .send, id);
    if (ends[p].pending_recv) |rc| {
        ends[p].pending_recv = null;
        pushDueKind(rc.c, .recv, rc.id);
    }
    return .{ .due = n };
}

pub fn recvInto(fd: i32, dst: []u8, recv_c: *anyopaque, dont_wait: bool) IoSubmit {
    const i = endIndex(fd) orelse return .bad_fd;
    const id = allocOpId();
    if (ends[i].buf_len == 0) {
        if (ends[i].closed or ends[ends[i].peer].closed) {
            pushDueKind(recv_c, .recv, id);
            return .eof;
        }
        if (dont_wait) return .would_block;
        if (ends[i].pending_recv != null) panic("sim: two recvs parked on one fd", .{});
        ends[i].pending_recv = .{ .c = recv_c, .id = id };
        return .parked;
    }
    const n = drainBuf(fd, dst);
    pushDueKind(recv_c, .recv, id);
    // A send parks on the sender end when this (recv) buffer is full.
    // Draining it must wake the peer's pending_send, not ours.
    const p = ends[i].peer;
    if (ends[p].pending_send) |sc| {
        ends[p].pending_send = null;
        pushDueKind(sc.c, .send, sc.id);
    }
    return .{ .due = n };
}

pub fn drainBuf(fd: i32, dst: []u8) usize {
    const i = endIndex(fd) orelse return 0;
    const n = @min(dst.len, ends[i].buf_len);
    if (n == 0) return 0;
    @memcpy(dst[0..n], ends[i].buf[0..n]);
    const rest = ends[i].buf_len - n;
    if (rest > 0) @memmove(ends[i].buf[0..rest], ends[i].buf[n..][0..rest]);
    ends[i].buf_len = rest;
    return n;
}

pub fn recvIsEof(fd: i32) bool {
    const i = endIndex(fd) orelse return true;
    return ends[i].buf_len == 0 and (ends[i].closed or ends[ends[i].peer].closed);
}

pub fn closeFd(fd: i32, close_c: *anyopaque) enum { due, bad_fd } {
    const i = endIndex(fd) orelse return .bad_fd;
    const id = allocOpId();
    ends[i].closed = true;
    if (ends[i].pending_recv) |rc| {
        ends[i].pending_recv = null;
        pushDueKind(rc.c, .recv, rc.id);
    }
    if (ends[i].pending_send) |sc| {
        ends[i].pending_send = null;
        pushDueKind(sc.c, .send, sc.id);
    }
    const p = ends[i].peer;
    if (ends[p].pending_recv) |rc| {
        ends[p].pending_recv = null;
        pushDueKind(rc.c, .recv, rc.id);
    }
    if (ends[p].pending_send) |sc| {
        ends[p].pending_send = null;
        pushDueKind(sc.c, .send, sc.id);
    }
    pushDueKind(close_c, .close, id);
    return .due;
}

pub fn cancelIo(c: *anyopaque) bool {
    for (&ends) |*e| {
        if (e.pending_recv) |p| {
            if (p.c == c) {
                e.pending_recv = null;
                return true;
            }
        }
        if (e.pending_send) |p| {
            if (p.c == c) {
                e.pending_send = null;
                return true;
            }
        }
    }
    var i: usize = 0;
    while (i < due_len) : (i += 1) {
        if (due[i].c == c) {
            due[i] = due[due_len - 1];
            due_len -= 1;
            return true;
        }
    }
    return false;
}

/// Pop completions whose due_at is now, shuffled when n>1. Caller harvests.
/// Future entries stay on the list.
pub fn takeDue(out: []TakenDue) usize {
    var n: usize = 0;
    var w: usize = 0;
    var i: usize = 0;
    while (i < due_len) : (i += 1) {
        if (due[i].due_at <= clock_ns) {
            if (n >= out.len) panic("sim: takeDue buffer too small", .{});
            out[n] = .{ .c = due[i].c, .id = due[i].id };
            n += 1;
        } else {
            due[w] = due[i];
            w += 1;
        }
    }
    due_len = w;
    if (n > 1) {
        var k = n;
        while (k > 1) {
            k -= 1;
            const j = pickIndex(k + 1);
            const tmp = out[k];
            out[k] = out[j];
            out[j] = tmp;
        }
    }
    return n;
}

pub fn mutantOmitTimeoutRecheck() bool {
    if (comptime !compiled_in) return false;
    return comptime std.mem.eql(u8, zio_options.sim_mutant, "omit_timeout_recheck");
}

pub fn mutantArmTimerStale() bool {
    if (comptime !compiled_in) return false;
    return comptime std.mem.eql(u8, zio_options.sim_mutant, "arm_timer_stale");
}

pub const ProbeOut = struct {
    trace: u64,
    state: u64,
    events: u64,
    clock: u64,
};

pub const WakeFirst = enum { a, b };

/// Two parked recvs submitted A then B. Wake order is `first`. Submit ids
/// must make the two orders hash differently.
pub fn runWakeOrderProbe(seed_value: u64, first: WakeFirst) ProbeOut {
    if (comptime !compiled_in) unreachable;
    begin(seed_value);
    defer end();

    const pa = pipePair();
    const pb = pipePair();
    var ca: u8 = 1;
    var cb: u8 = 2;
    var sa: u8 = 3;
    var sb: u8 = 4;
    var da: [1]u8 = undefined;
    var db: [1]u8 = undefined;

    switch (recvInto(pa[0], &da, &ca, false)) {
        .parked => {},
        else => panic("probe: recv A should park", .{}),
    }
    switch (recvInto(pb[0], &db, &cb, false)) {
        .parked => {},
        else => panic("probe: recv B should park", .{}),
    }

    const payload = "x";
    if (first == .a) {
        switch (sendBytes(pa[1], payload, &sa)) {
            .due => {},
            else => panic("probe: send A should complete", .{}),
        }
        switch (sendBytes(pb[1], payload, &sb)) {
            .due => {},
            else => panic("probe: send B should complete", .{}),
        }
    } else {
        switch (sendBytes(pb[1], payload, &sb)) {
            .due => {},
            else => panic("probe: send B should complete", .{}),
        }
        switch (sendBytes(pa[1], payload, &sa)) {
            .due => {},
            else => panic("probe: send A should complete", .{}),
        }
    }

    advanceNs(10_000_000);
    var buf: [8]TakenDue = undefined;
    const n = takeDue(&buf);
    var i: usize = 0;
    while (i < n) : (i += 1) {
        emit(.io_complete, 0, buf[i].id);
    }

    return .{
        .trace = traceHash(),
        .state = stateDigest(),
        .events = event_count,
        .clock = clock_ns,
    };
}

test "submit I/O id is stable across wake order" {
    if (comptime !compiled_in) return error.SkipZigTest;
    const ab = runWakeOrderProbe(1, .a);
    const ba = runWakeOrderProbe(1, .b);
    try std.testing.expect(ab.state != ba.state);
    try std.testing.expect(ab.trace != ba.trace);
}
