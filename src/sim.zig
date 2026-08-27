// SPDX-FileCopyrightText: 2025 Lukáš Lalinský
// SPDX-License-Identifier: MIT
//
// Deterministic simulation (DST) support. Compiled in only when
// `zio_options.sim` is true. Production builds keep the option off, so these
// paths do not exist there.

const std = @import("std");
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
const pipe_buf_cap = 4096;
const fd_base: i32 = 100000;

const End = struct {
    used: bool = false,
    peer: u8 = 0,
    closed: bool = false,
    buf: [pipe_buf_cap]u8 = undefined,
    buf_len: usize = 0,
    pending_recv: ?*anyopaque = null,
    pending_send: ?*anyopaque = null,
};

var ends: [max_ends]End = @splat(.{});
var due: [max_due]*anyopaque = undefined;
var due_len: usize = 0;

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
    if (begun) @panic("sim: begin() called twice without end()");
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
    if (!begun) @panic("sim: clock read before begin");
    if (!clock_routed) @panic("sim: real clock_gettime");
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
    if (!begun) @panic("sim: advance before begin");
    clock_ns +|= ns;
}

pub fn pickIndex(n: usize) usize {
    if (comptime !compiled_in) unreachable;
    if (!begun) @panic("sim: pick before begin");
    if (n == 0) @panic("sim: pickIndex(0)");
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
    if (task_id_len >= task_ids.len) @panic("sim: task id table full");
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

/// Final-state digest: clock, event count, assigned task ids. Does not
/// include the RNG stream.
pub fn stateDigest() u64 {
    var h = std.hash.Wyhash.init(1);
    var buf: [24]u8 = undefined;
    std.mem.writeInt(u64, buf[0..8], clock_ns, .little);
    std.mem.writeInt(u64, buf[8..16], event_count, .little);
    std.mem.writeInt(u32, buf[16..20], next_task_id, .little);
    std.mem.writeInt(u32, buf[20..24], task_id_len, .little);
    h.update(&buf);
    return h.final();
}

pub fn deadlock() noreturn {
    std.debug.panic(
        "sim: deadlock SEED={d} clock_ns={d} events={d}",
        .{ current_seed, clock_ns, event_count },
    );
}

pub fn forbidKernelFutex() void {
    if (comptime !compiled_in) return;
    if (!begun) return;
    std.debug.panic("sim: real kernel futex SEED={d}", .{current_seed});
}

pub fn forbidBackendPoll() noreturn {
    std.debug.panic("sim: real backend.poll SEED={d}", .{current_seed});
}

pub fn printScope() void {
    std.debug.print("SCOPE simulated: clock, futex_park, task_pick, timer_heap, cq, executor_csprng, real_epoch, net_pipe\n", .{});
    std.debug.print("SCOPE real: allocator, libc\n", .{});
    std.debug.print("SCOPE unsimulated: file_io, connect_accept, extra_os_threads, dns, boot_vs_awake (boot==awake), io_uring (never entered)\n", .{});
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
    @panic("sim: pipe table full");
}

/// Bidirectional pair (socketpair shape). No kernel fd.
pub fn pipePair() [2]i32 {
    if (comptime !compiled_in) unreachable;
    if (!begun) @panic("sim: pipePair before begin");
    const a = allocEnd();
    const b = allocEnd();
    ends[a].peer = b;
    ends[b].peer = a;
    return .{ fd_base + a, fd_base + b };
}

fn pushDue(c: *anyopaque) void {
    if (due_len >= max_due) @panic("sim: I/O due list full");
    due[due_len] = c;
    due_len += 1;
}

pub fn hasDueIo() bool {
    return due_len > 0;
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
    eof,
    bad_fd,
};

/// Copy `src` into the peer's recv buffer.
pub fn sendBytes(fd: i32, src: []const u8, send_c: *anyopaque) IoSubmit {
    const i = endIndex(fd) orelse return .bad_fd;
    if (ends[i].closed) return .eof;
    const p = ends[i].peer;
    if (ends[p].closed) return .eof;
    if (src.len == 0) {
        pushDue(send_c);
        return .{ .due = 0 };
    }
    const space = pipe_buf_cap - ends[p].buf_len;
    if (space == 0) {
        if (ends[i].pending_send != null) @panic("sim: two sends parked on one fd");
        ends[i].pending_send = send_c;
        return .parked;
    }
    const n = @min(src.len, space);
    @memcpy(ends[p].buf[ends[p].buf_len..][0..n], src[0..n]);
    ends[p].buf_len += n;
    pushDue(send_c);
    if (ends[p].pending_recv) |rc| {
        ends[p].pending_recv = null;
        pushDue(rc);
    }
    return .{ .due = n };
}

pub fn recvInto(fd: i32, dst: []u8, recv_c: *anyopaque) IoSubmit {
    const i = endIndex(fd) orelse return .bad_fd;
    if (ends[i].buf_len == 0) {
        if (ends[i].closed or ends[ends[i].peer].closed) {
            pushDue(recv_c);
            return .eof;
        }
        if (ends[i].pending_recv != null) @panic("sim: two recvs parked on one fd");
        ends[i].pending_recv = recv_c;
        return .parked;
    }
    const n = drainBuf(fd, dst);
    pushDue(recv_c);
    if (ends[i].pending_send) |sc| {
        ends[i].pending_send = null;
        pushDue(sc);
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
    ends[i].closed = true;
    if (ends[i].pending_recv) |rc| {
        ends[i].pending_recv = null;
        pushDue(rc);
    }
    if (ends[i].pending_send) |sc| {
        ends[i].pending_send = null;
        pushDue(sc);
    }
    const p = ends[i].peer;
    if (ends[p].pending_recv) |rc| {
        ends[p].pending_recv = null;
        pushDue(rc);
    }
    pushDue(close_c);
    return .due;
}

pub fn cancelIo(c: *anyopaque) bool {
    for (&ends) |*e| {
        if (e.pending_recv == c) {
            e.pending_recv = null;
            return true;
        }
        if (e.pending_send == c) {
            e.pending_send = null;
            return true;
        }
    }
    var i: usize = 0;
    while (i < due_len) : (i += 1) {
        if (due[i] == c) {
            due[i] = due[due_len - 1];
            due_len -= 1;
            return true;
        }
    }
    return false;
}

/// Pop every due completion, shuffled when n>1. Caller harvests.
pub fn takeDue(out: []*anyopaque) usize {
    const n = due_len;
    if (n == 0) return 0;
    if (n > out.len) @panic("sim: takeDue buffer too small");
    var i: usize = 0;
    while (i < n) : (i += 1) out[i] = due[i];
    due_len = 0;
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
