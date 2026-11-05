// SPDX-FileCopyrightText: 2025 Lukáš Lalinský
// SPDX-License-Identifier: MIT
//
// Deterministic simulation (DST) support. Compiled in only when
// `Options.sim` is true (`options.zig`). Production builds keep the option
// off, so these paths do not exist there.

const std = @import("std");
const builtin = @import("builtin");
const zio_options = @import("options.zig").options;

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
const max_live = 128;
pub const pipe_buf_cap = 4096;
const fd_base: i32 = 100000;

const Parked = struct {
    c: *anyopaque,
    id: u32,
};

/// Faults a harness arms on one end's sends (`setSendFaults`). Both are
/// seeded and recorded in the trace, so a replay hits them at the same byte.
pub const SendFaults = struct {
    /// 0: off. Otherwise each send copies at most `1 + pick(short_max)` bytes
    /// when more were offered: a short write, as a kernel may return.
    short_max: u32 = 0,
    /// The send that would carry this end's sent-byte count past this value
    /// copies only up to it; the next send fails with ConnectionResetByPeer
    /// and resets the pipe (both ends). Null: no write error.
    fail_at_byte: ?u64 = null,
};

pub const FaultStats = struct {
    short_writes: u32 = 0,
    write_errors: u32 = 0,
    /// Logical time of the first injected write error, or null.
    first_error_ns: ?u64 = null,
    sent_bytes: u64 = 0,
    /// Sends on this end that failed for any reason (the injected error, a
    /// reset pipe, a closed peer or a shut send side), and when the first
    /// did. What a harness needs to say a write failure hit a batch.
    send_failures: u32 = 0,
    first_send_failure_ns: ?u64 = null,
};

fn noteSendFailure(i: u8) void {
    ends[i].stats.send_failures += 1;
    if (ends[i].stats.first_send_failure_ns == null) ends[i].stats.first_send_failure_ns = clock_ns;
}

/// One end of a `pipePair`. `buf` holds the bytes sent TO this end.
const End = struct {
    used: bool = false,
    peer: u8 = 0,
    closed: bool = false,
    /// `shutdown(.receive)` on this end: its reads see end of stream.
    rd_shut: bool = false,
    /// `shutdown(.send)` on this end: the peer reads end of stream once the
    /// buffer drains; this end's sends fail with BrokenPipe.
    wr_shut: bool = false,
    /// The pipe was reset (an injected write error): every op on either end
    /// fails with ConnectionResetByPeer, and unread bytes are dropped.
    reset: bool = false,
    buf: [pipe_buf_cap]u8 = undefined,
    buf_len: usize = 0,
    pending_recv: ?Parked = null,
    pending_send: ?Parked = null,
    faults: SendFaults = .{},
    stats: FaultStats = .{},
};

var ends: [max_ends]End = @splat(.{});

pub const DueKind = enum(u8) {
    recv = 1,
    send = 2,
    close = 3,
    shutdown = 4,
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

const LiveOp = struct {
    c: *anyopaque,
    id: u32,
};

var live: [max_live]LiveOp = undefined;
var live_len: usize = 0;

pub fn isBegun() bool {
    return begun;
}

pub fn seed() u64 {
    return current_seed;
}

pub fn begin(seed_value: u64) void {
    if (comptime !compiled_in) {
        @panic("sim: not compiled in (declare zio_options.sim = true, or build zio with -Dsim=true)");
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
    live_len = 0;
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

/// Wall-clock now. `clock_idx` is `@backingInt(time.Clock)`. `.real` (2)
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
    word[0] = @backingInt(kind);
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
        eb[7] = @as(u8, @intFromBool(e.rd_shut)) | (@as(u8, @intFromBool(e.wr_shut)) << 1) | (@as(u8, @intFromBool(e.reset)) << 2);
        h.update(&eb);
        var sb: [8]u8 = undefined;
        std.mem.writeInt(u64, &sb, e.stats.sent_bytes, .little);
        h.update(&sb);
        var ids: [8]u8 = undefined;
        std.mem.writeInt(u32, ids[0..4], if (e.pending_recv) |p| p.id else 0, .little);
        std.mem.writeInt(u32, ids[4..8], if (e.pending_send) |p| p.id else 0, .little);
        h.update(&ids);
        if (e.used and e.buf_len > 0) {
            h.update(e.buf[0..e.buf_len]);
        }
    }
    std.mem.writeInt(u32, buf[0..4], next_io_id, .little);
    std.mem.writeInt(u32, buf[4..8], @intCast(live_len), .little);
    std.mem.writeInt(u64, buf[8..16], due_len, .little);
    h.update(buf[0..16]);
    var li: usize = 0;
    while (li < live_len) : (li += 1) {
        std.mem.writeInt(u32, buf[0..4], live[li].id, .little);
        h.update(buf[0..4]);
    }
    var d: usize = 0;
    while (d < due_len) : (d += 1) {
        std.mem.writeInt(u64, buf[0..8], due[d].due_at, .little);
        std.mem.writeInt(u32, buf[8..12], due[d].id, .little);
        buf[12] = @backingInt(due[d].kind);
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
    // Each item is a real source of nondeterminism this mode does not model.
    // work_stealing is a compile error with sim (options.zig), so migration,
    // stealing, the rescue baton and the idle mask are never exercised here.
    // loop_sleep_handshake: `Loop.poll` keeps production's order (announce
    // `sleeping`, wait, take the requests), but every executor is polled
    // with `.zero` before any loop advances the clock, so the wake bits are
    // always clear when a sleep starts; reverting the wake-before-sleep
    // check changes no fixture trace.
    const common_unsim = "file_io, connect_accept, extra_os_threads, dns, boot_vs_awake (boot==awake), spawn_blocking (panics), work_stealing (sim cannot run work_stealing), worker_os_threads (logical workers share one thread), loop_sleep_handshake (one thread: a wake never races a loop's sleep, so the sleeping bit is never seen set)";
    if (builtin.os.tag.isDarwin()) {
        std.debug.print("SCOPE unsimulated: " ++ common_unsim ++ ", backend_kernel (poll never called; Loop.init does not open a kqueue fd)\n", .{});
    } else {
        std.debug.print("SCOPE unsimulated: " ++ common_unsim ++ ", backend_kernel (poll never called)\n", .{});
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

fn allocOpId(c: *anyopaque) u32 {
    next_io_id += 1;
    if (live_len >= max_live) panic("sim: live I/O table full", .{});
    live[live_len] = .{ .c = c, .id = next_io_id };
    live_len += 1;
    return next_io_id;
}

pub fn takeOpId(c: *anyopaque) ?u32 {
    var i: usize = 0;
    while (i < live_len) : (i += 1) {
        if (live[i].c == c) {
            const id = live[i].id;
            live[i] = live[live_len - 1];
            live_len -= 1;
            return id;
        }
    }
    return null;
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

/// A submitted simulated op that has not completed yet (parked or due).
/// The sim's equivalent of a backend's in-flight count. One table serves
/// every logical loop, so with two executors it counts both loops' ops.
pub fn hasLiveIo() bool {
    if (comptime !compiled_in) unreachable;
    return live_len > 0;
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
    /// The pipe is reset: ConnectionResetByPeer.
    reset,
};

/// Arm send faults on `fd`'s sends. Replaces any earlier setting.
pub fn setSendFaults(fd: i32, faults: SendFaults) void {
    if (comptime !compiled_in) unreachable;
    const i = endIndex(fd) orelse panic("sim: setSendFaults on an unknown fd {d}", .{fd});
    ends[i].faults = faults;
}

pub fn clearSendFaults(fd: i32) void {
    setSendFaults(fd, .{});
}

/// Counts of the faults this end's sends took, and its sent bytes.
pub fn faultStats(fd: i32) FaultStats {
    if (comptime !compiled_in) unreachable;
    const i = endIndex(fd) orelse return .{};
    return ends[i].stats;
}

fn totalLen(slices: []const []const u8) usize {
    var n: usize = 0;
    for (slices) |sl| n += sl.len;
    return n;
}

/// Copy from `src` (gathered across its slices) into the peer's buffer.
pub fn sendBytes(fd: i32, src: []const []const u8, send_c: *anyopaque) IoSubmit {
    return copySend(fd, src, send_c, allocOpId(send_c), true);
}

/// Harvest a capacity-woken send: copy without re-queueing this completion.
/// `id` is the submit-time id from the due entry.
pub fn harvestSend(fd: i32, src: []const []const u8, send_c: *anyopaque, id: u32) IoSubmit {
    return copySend(fd, src, send_c, id, false);
}

/// Reset the pipe of end `i`: both ends fail every later op, unread bytes
/// are dropped, and parked ops on both ends wake to their error.
fn resetPipe(i: u8) void {
    const p = ends[i].peer;
    for ([2]u8{ i, p }) |e| {
        ends[e].reset = true;
        ends[e].buf_len = 0;
        if (ends[e].pending_recv) |rc| {
            ends[e].pending_recv = null;
            pushDueKind(rc.c, .recv, rc.id);
        }
        if (ends[e].pending_send) |sc| {
            ends[e].pending_send = null;
            pushDueKind(sc.c, .send, sc.id);
        }
    }
}

fn copySend(fd: i32, src: []const []const u8, send_c: *anyopaque, id: u32, schedule: bool) IoSubmit {
    const i = endIndex(fd) orelse {
        if (schedule) pushDueKind(send_c, .send, id);
        return .bad_fd;
    };
    if (ends[i].reset) {
        noteSendFailure(i);
        if (schedule) pushDueKind(send_c, .send, id);
        return .reset;
    }
    if (ends[i].closed or ends[i].wr_shut or ends[ends[i].peer].closed) {
        noteSendFailure(i);
        if (schedule) pushDueKind(send_c, .send, id);
        return .eof;
    }
    const p = ends[i].peer;
    const total = totalLen(src);
    if (total == 0) {
        if (schedule) pushDueKind(send_c, .send, id);
        return .{ .due = 0 };
    }
    var limit: usize = total;
    if (ends[i].faults.fail_at_byte) |at| {
        const room = at -| ends[i].stats.sent_bytes;
        if (room == 0) {
            ends[i].stats.write_errors += 1;
            if (ends[i].stats.first_error_ns == null) ends[i].stats.first_error_ns = clock_ns;
            ends[i].faults.fail_at_byte = null;
            emit(.io_complete, 0xfe, ends[i].stats.write_errors);
            noteSendFailure(i);
            resetPipe(i);
            if (schedule) pushDueKind(send_c, .send, id);
            return .reset;
        }
        limit = @min(limit, room);
    }
    const space = pipe_buf_cap - ends[p].buf_len;
    if (space == 0) {
        if (ends[i].pending_send != null) panic("sim: two sends parked on one fd", .{});
        ends[i].pending_send = .{ .c = send_c, .id = id };
        return .parked;
    }
    limit = @min(limit, space);
    if (ends[i].faults.short_max > 0 and limit > 1) {
        const cap: usize = 1 + pickIndex(@min(limit, ends[i].faults.short_max));
        if (cap < limit) {
            limit = cap;
            ends[i].stats.short_writes += 1;
            emit(.io_complete, 0xfd, @truncate(cap));
        }
    }
    var copied: usize = 0;
    for (src) |sl| {
        if (copied == limit) break;
        const k = @min(sl.len, limit - copied);
        @memcpy(ends[p].buf[ends[p].buf_len + copied ..][0..k], sl[0..k]);
        copied += k;
    }
    ends[p].buf_len += copied;
    ends[i].stats.sent_bytes += copied;
    if (schedule) pushDueKind(send_c, .send, id);
    if (ends[p].pending_recv) |rc| {
        ends[p].pending_recv = null;
        pushDueKind(rc.c, .recv, rc.id);
    }
    return .{ .due = copied };
}

/// End of stream for a read on end `i`: nothing buffered, and either side
/// closed, this side shut its reads, or the peer shut its writes.
fn atEof(i: u8) bool {
    const p = ends[i].peer;
    return ends[i].buf_len == 0 and (ends[i].closed or ends[i].rd_shut or ends[p].closed or ends[p].wr_shut);
}

pub fn recvInto(fd: i32, dst: []const []u8, recv_c: *anyopaque, dont_wait: bool) IoSubmit {
    const id = allocOpId(recv_c);
    const i = endIndex(fd) orelse {
        pushDueKind(recv_c, .recv, id);
        return .bad_fd;
    };
    if (ends[i].reset) {
        pushDueKind(recv_c, .recv, id);
        return .reset;
    }
    if (ends[i].buf_len == 0) {
        if (atEof(i)) {
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
    wakeSender(i);
    return .{ .due = n };
}

/// A send parks on the sender end when the receiver's buffer is full.
/// Draining end `i`'s buffer must wake the peer's pending send.
fn wakeSender(i: u8) void {
    const p = ends[i].peer;
    if (ends[p].pending_send) |sc| {
        ends[p].pending_send = null;
        pushDueKind(sc.c, .send, sc.id);
    }
}

/// Copy buffered bytes into `dst` (scattered across its slices).
pub fn drainBuf(fd: i32, dst: []const []u8) usize {
    const i = endIndex(fd) orelse return 0;
    var n: usize = 0;
    for (dst) |d| {
        if (n == ends[i].buf_len) break;
        const k = @min(d.len, ends[i].buf_len - n);
        @memcpy(d[0..k], ends[i].buf[n..][0..k]);
        n += k;
    }
    if (n == 0) return 0;
    const rest = ends[i].buf_len - n;
    if (rest > 0) @memmove(ends[i].buf[0..rest], ends[i].buf[n..][0..rest]);
    ends[i].buf_len = rest;
    return n;
}

/// For a harvested recv: drain, and wake a sender the drain made room for.
pub fn drainForRecv(fd: i32, dst: []const []u8) usize {
    const n = drainBuf(fd, dst);
    if (n > 0) {
        if (endIndex(fd)) |i| wakeSender(i);
    }
    return n;
}

pub fn recvIsEof(fd: i32) bool {
    const i = endIndex(fd) orelse return true;
    return atEof(i);
}

pub fn isReset(fd: i32) bool {
    const i = endIndex(fd) orelse return false;
    return ends[i].reset;
}

pub fn closeFd(fd: i32, close_c: *anyopaque) enum { due, bad_fd } {
    const id = allocOpId(close_c);
    const i = endIndex(fd) orelse {
        pushDueKind(close_c, .close, id);
        return .bad_fd;
    };
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

pub const ShutdownHow = enum { receive, send, both };

/// `shutdown(2)` on a pipe end. Wakes the reads that now see end of stream
/// and the sends that now fail.
pub fn shutdownFd(fd: i32, how: ShutdownHow, c: *anyopaque) enum { due, bad_fd } {
    const id = allocOpId(c);
    const i = endIndex(fd) orelse {
        pushDueKind(c, .shutdown, id);
        return .bad_fd;
    };
    const p = ends[i].peer;
    if (how != .send) {
        ends[i].rd_shut = true;
        if (ends[i].pending_recv) |rc| {
            ends[i].pending_recv = null;
            pushDueKind(rc.c, .recv, rc.id);
        }
    }
    if (how != .receive) {
        ends[i].wr_shut = true;
        if (ends[i].pending_send) |sc| {
            ends[i].pending_send = null;
            pushDueKind(sc.c, .send, sc.id);
        }
        if (ends[p].pending_recv) |rc| {
            ends[p].pending_recv = null;
            pushDueKind(rc.c, .recv, rc.id);
        }
    }
    pushDueKind(c, .shutdown, id);
    return .due;
}

pub fn cancelIo(c: *anyopaque) ?u32 {
    for (&ends) |*e| {
        if (e.pending_recv) |p| {
            if (p.c == c) e.pending_recv = null;
        }
        if (e.pending_send) |p| {
            if (p.c == c) e.pending_send = null;
        }
    }
    var i: usize = 0;
    while (i < due_len) : (i += 1) {
        if (due[i].c == c) {
            due[i] = due[due_len - 1];
            due_len -= 1;
            break;
        }
    }
    return takeOpId(c);
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
    return comptime zio_options.sim_mutant == .omit_timeout_recheck;
}

pub fn mutantArmTimerStale() bool {
    if (comptime !compiled_in) return false;
    return comptime zio_options.sim_mutant == .arm_timer_stale;
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

    switch (recvInto(pa[0], &.{@as([]u8, &da)}, &ca, false)) {
        .parked => {},
        else => panic("probe: recv A should park", .{}),
    }
    switch (recvInto(pb[0], &.{@as([]u8, &db)}, &cb, false)) {
        .parked => {},
        else => panic("probe: recv B should park", .{}),
    }

    const payload = "x";
    if (first == .a) {
        switch (sendBytes(pa[1], &.{@as([]const u8, payload)}, &sa)) {
            .due => {},
            else => panic("probe: send A should complete", .{}),
        }
        switch (sendBytes(pb[1], &.{@as([]const u8, payload)}, &sb)) {
            .due => {},
            else => panic("probe: send B should complete", .{}),
        }
    } else {
        switch (sendBytes(pb[1], &.{@as([]const u8, payload)}, &sb)) {
            .due => {},
            else => panic("probe: send B should complete", .{}),
        }
        switch (sendBytes(pa[1], &.{@as([]const u8, payload)}, &sa)) {
            .due => {},
            else => panic("probe: send A should complete", .{}),
        }
    }

    advanceNs(10_000_000);
    var buf: [8]TakenDue = undefined;
    const n = takeDue(&buf);
    var i: usize = 0;
    while (i < n) : (i += 1) {
        const id = takeOpId(buf[i].c) orelse buf[i].id;
        emit(.io_complete, 0, id);
    }

    return .{
        .trace = traceHash(),
        .state = stateDigest(),
        .events = event_count,
        .clock = clock_ns,
    };
}

/// Two parked recvs submitted A then B. Cancel order is `first`. Submit ids
/// must make the two orders hash differently.
pub fn runCancelOrderProbe(seed_value: u64, first: WakeFirst) ProbeOut {
    if (comptime !compiled_in) unreachable;
    begin(seed_value);
    defer end();

    const pa = pipePair();
    const pb = pipePair();
    var ca: u8 = 1;
    var cb: u8 = 2;
    var da: [1]u8 = undefined;
    var db: [1]u8 = undefined;

    switch (recvInto(pa[0], &.{@as([]u8, &da)}, &ca, false)) {
        .parked => {},
        else => panic("probe: recv A should park", .{}),
    }
    switch (recvInto(pb[0], &.{@as([]u8, &db)}, &cb, false)) {
        .parked => {},
        else => panic("probe: recv B should park", .{}),
    }

    const first_c: *anyopaque = if (first == .a) &ca else &cb;
    const second_c: *anyopaque = if (first == .a) &cb else &ca;
    const id1 = cancelIo(first_c) orelse panic("probe: missing cancel id 1", .{});
    emit(.io_complete, 1, id1);
    const id2 = cancelIo(second_c) orelse panic("probe: missing cancel id 2", .{});
    emit(.io_complete, 1, id2);

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

test "submit I/O id is stable across cancel order" {
    if (comptime !compiled_in) return error.SkipZigTest;
    const ab = runCancelOrderProbe(1, .a);
    const ba = runCancelOrderProbe(1, .b);
    try std.testing.expect(ab.state != ba.state);
    try std.testing.expect(ab.trace != ba.trace);
}
