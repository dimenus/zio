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
    preempt = 6,
    /// A datagram reached its landing time (`land`): landed or dropped.
    /// `a`: the source port, and the outcome in bits 16-23 (0 landed,
    /// 1 no socket, 2 the receive queue was full). `b`: the destination port
    /// in the high 16 bits, the length in the low 16 (at most 65507).
    dgram = 7,
};

var begun: bool = false;
var clock_routed: bool = true;
var current_seed: u64 = 0;
/// The thread that called `begin`, as `std.Thread.Id + 1`; 0 outside a run.
/// Atomic because `refuseOtherThread` reads it from other threads.
var sim_thread: std.atomic.Value(u64) = .init(0);
var clock_ns: u64 = 0;
var rng: std.Random.DefaultPrng = undefined;
var hasher: std.hash.Wyhash = undefined;
var event_count: u64 = 0;
var next_task_id: u32 = 1;

const max_ends = 32;
const max_live = 128;
/// Every due entry is an op that holds a live id, and an op is parked or
/// due, never both, so the due list can never hold more than `max_live`
/// (`pushDueAt` checks it). A harvest buffer of this size takes every op
/// that can be due at one time: 64 sockets each woken by one landing fit.
pub const max_due = max_live;
pub const pipe_buf_cap = 4096;
/// The first sim fd number in a run (section 3.3 of the sim network design).
const default_fd_base: i32 = 100000;
/// `default_fd_base`, except in a test child that moved it
/// (`testMoveFdBase`). A variable only for that reason.
var fd_base: i32 = default_fd_base;

/// Moves the sim fd range in this test process, before `begin`. The build
/// runner of `zig build test` lowers its own hard RLIMIT_NOFILE to 10240 on
/// Darwin (`std.process.raiseFileDescriptorLimit`, called from
/// lib/compiler/Maker.zig), and every test binary inherits that. No
/// descriptor can then reach 100000, so the fd-namespace checks have nothing
/// to refuse at the real base. A child that moves the base under its hard
/// limit runs the same `begin`, `pipePair` and `end` code against descriptors
/// the kernel can really create.
fn testMoveFdBase(base: i32) void {
    if (comptime !builtin.is_test) @compileError("testMoveFdBase is for tests");
    if (begun) panic("sim: testMoveFdBase during a run", .{});
    fd_base = base;
}

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
    /// Any ev op on a sim datagram socket (`dgramOpDone`, a landing or a
    /// close that wakes a parked op). An op without a result is checked
    /// against its socket's generation at the harvest (`dgramOpOnItsSocket`).
    dgram = 5,
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
    /// For an ev op on a sim datagram socket: the socket slot's generation
    /// when the op was submitted (`dgramOpStart`). 0 for every other op.
    sock_gen: u32 = 0,
};

var live: [max_live]LiveOp = undefined;
var live_len: usize = 0;

/// Seeded preemption at a non-blocking channel operation
/// (`Channel(T).tryReceive` / `trySend`, `sync/channel.zig`). On real
/// threads a task can be descheduled right after such an operation decided
/// its result, and other tasks, including ones on other executors, run
/// before it acts on that result. One thread never does that: a task runs
/// until it suspends. So when this is on, the operation's caller sleeps for
/// a seed-chosen simulated time after the result is decided, and every
/// other task may run meanwhile.
///
/// `one_in` is the rate: one operation in `one_in` preempts; 0 is off, the
/// default, and then no random number is drawn, so every trace is the same
/// as without this setting. The duration is log-uniform in
/// [`min_ns`, `max_ns`]. Kept across `begin`/`end`: a harness sets it once.
pub const Preempt = struct {
    one_in: u32,
    min_ns: u64,
    max_ns: u64,
};

var preempt: Preempt = .{ .one_in = 0, .min_ns = 0, .max_ns = 0 };

pub fn setPreempt(p: Preempt) void {
    refuseOtherThread("sim.setPreempt");
    if (p.one_in != 0 and (p.min_ns == 0 or p.max_ns < p.min_ns)) {
        panic("sim: setPreempt needs 0 < min_ns <= max_ns", .{});
    }
    preempt = p;
}

pub fn preemptSetting() Preempt {
    return preempt;
}

/// Called by a task after a non-blocking channel operation. Returns how
/// long the task sleeps, 0 for no preemption. Draws from the seed only
/// while preemption is on.
pub fn preemptNs() u64 {
    if (comptime !compiled_in) return 0;
    if (!begun or preempt.one_in == 0) return 0;
    const r = rng.random();
    if (r.uintLessThan(u32, preempt.one_in) != 0) return 0;
    // Log-uniform: as many short preemptions as long ones per decade.
    const lo: f64 = @floatFromInt(preempt.min_ns);
    const hi: f64 = @floatFromInt(preempt.max_ns);
    const ns: u64 = @intFromFloat(@exp(@log(lo) + r.float(f64) * (@log(hi) - @log(lo))));
    emit(.preempt, @truncate(ns >> 10), @truncate(ns));
    return @max(ns, 1);
}

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
    resetIo();
    establishFdNamespace();
    // Last, so a thread that sees the owner also sees the seed.
    sim_thread.store(threadKey(), .release);
}

/// The soft `RLIMIT_NOFILE` before `begin` lowered it, restored by `end`.
var saved_nofile_soft: ?std.posix.rlim_t = null;

/// A sim fd is `fd_base + index`, a plain integer the kernel could also hand
/// out: with a soft limit above `fd_base`, `dup2(real, fd_base)` succeeds
/// (round 4: it did on macOS with the soft limit at 1048576). So `begin`
/// lowers the soft limit to `fd_base`, after which the kernel never creates
/// a descriptor at or above it and refuses `dup2` to one, reads it back, and
/// checks that no descriptor is already open in the sim range. Code that
/// raises the limit again during the run is caught at the next sim fd
/// (`checkFdNamespace`). POSIX only: there are no sim fds on Windows.
fn establishFdNamespace() void {
    if (comptime builtin.os.tag == .windows) return;
    const rl = std.posix.getrlimit(.NOFILE) catch |err| panic("sim: fd namespace: getrlimit failed: {t}", .{err});
    saved_nofile_soft = null;
    if (rl.cur > fd_base) {
        std.posix.setrlimit(.NOFILE, .{ .cur = @intCast(fd_base), .max = rl.max }) catch |err|
            panic("sim: fd namespace: cannot lower the soft RLIMIT_NOFILE to {d}: {t}", .{ fd_base, err });
        saved_nofile_soft = rl.cur;
    }
    checkFdNamespace();
    var n: i32 = fd_base;
    while (n < fd_base + sim_fd_count) : (n += 1) {
        if (std.c.fcntl(n, std.c.F.GETFD) != -1) panic("sim: fd namespace: real descriptor {d} is open in the sim fd range", .{n});
    }
}

/// Panics unless the soft `RLIMIT_NOFILE` is at most `fd_base`.
fn checkFdNamespace() void {
    if (comptime builtin.os.tag == .windows) return;
    const rl = std.posix.getrlimit(.NOFILE) catch |err| panic("sim: fd namespace: getrlimit failed: {t}", .{err});
    if (rl.cur > fd_base) panic("sim: fd namespace: the soft RLIMIT_NOFILE is {d}, above the sim fd base {d}", .{ rl.cur, fd_base });
}

fn restoreFdNamespace() void {
    if (comptime builtin.os.tag == .windows) return;
    const soft = saved_nofile_soft orelse return;
    saved_nofile_soft = null;
    const rl = std.posix.getrlimit(.NOFILE) catch return;
    std.posix.setrlimit(.NOFILE, .{ .cur = soft, .max = rl.max }) catch {};
}

fn threadKey() u64 {
    return @as(u64, @intCast(std.Thread.getCurrentId())) + 1;
}

/// The rule, for a sim run (from `begin` to `end`): every public zio
/// operation that changes state a task can later see (zio's primitives, the
/// runtime and its loops, the simulator, and kernel state reached through
/// zio: files, sockets, the working directory, processes) runs on the thread
/// that called `begin`, and is checked at its entry, before its first write.
/// A change made by another thread lands at a real time the seed does not
/// choose. The check is made whether or not a task waits at that moment: a
/// refusal that fired only when one waits would itself depend on timing.
///
/// Each such operation either calls `refuseOtherThread` first, or makes its
/// first write through one that does (`RwLock.tryLock` through
/// `Mutex.tryLock`; channels, semaphores, barriers through their mutex; every
/// blocking wait, file and socket op through the waiter it creates,
/// `Waiter.Direct.init`; every spawn through `spawnTask`).
///
/// What the rule's enumeration covers, function by function (the lists are
/// in the commits for t-4085 rounds 3 and 4): every `pub fn` in src/sync/,
/// src/group.zig, task.zig, sim.zig, signal.zig, common.zig,
/// completion_queue.zig, autocancel.zig, select.zig, runtime.zig, fs.zig,
/// net.zig, dns/, the src/ev files that run under sim (loop, completion,
/// thread_pool, blocking, heap, queue, buf, sockreg), and every entry of
/// io.zig's `std.Io` vtable. Not enumerated: src/os/ (the syscall wrappers
/// those call), coro/, utils/, and the backends, which never run under sim.
///
/// Out of the rule: read-only operations; writes only to a field of a value
/// the caller owns (`TempFile.keep`, a reader's `setTimeout`), which zio does
/// not share; operations whose own contract already requires the loop's
/// thread or the select's owner in every build (`Loop.add`, `Loop.poll`,
/// `Loop.cancel`, `Waiter.beginCommit`/`finishCommit`/`abortCommit`/
/// `promotePending`); plain atomics a program shares with a thread; C code.
/// The simulator cannot see the last two. Compiled out without sim.
pub inline fn refuseOtherThread(comptime what: []const u8) void {
    if (comptime !compiled_in) return;
    checkSimThread(what);
}

/// For what a run refuses on every thread, the sim thread included: input
/// that arrives at a real time (OS signals, a pool worker's completion).
pub inline fn refuseDuringRun(comptime what: []const u8) void {
    if (comptime !compiled_in) return;
    if (sim_thread.load(.acquire) == 0) return;
    refuse("sim: " ++ what ++ " during a sim run", .{});
}

fn checkSimThread(comptime what: []const u8) void {
    const owner = sim_thread.load(.acquire);
    if (owner == 0 or owner == threadKey()) return;
    // No TRACE_HASH: the hasher belongs to the sim thread, which may be
    // updating it right now. The seed is enough to replay.
    refuse("sim: " ++ what ++ " from a thread that is not the sim thread (the one that called sim.begin)", .{});
}

/// Writes the line to fd 2 itself before panicking. A program whose
/// `std_options_debug_io` is `zio.debug_io` prints panics through zio's
/// stderr lock, which is a zio mutex, and so is refused on this thread too:
/// the panic would then print nothing at all.
fn refuse(comptime msg: []const u8, args: anytype) noreturn {
    var buf: [512]u8 = undefined;
    const line = std.fmt.bufPrint(&buf, msg ++ " SEED={d}\n", args ++ .{current_seed}) catch msg ++ "\n";
    if (comptime builtin.link_libc and builtin.os.tag != .windows) {
        _ = std.c.write(2, line.ptr, line.len);
    }
    std.debug.panic("{s}", .{line[0 .. line.len - 1]});
}

fn resetIo() void {
    ends = @splat(.{});
    due_len = 0;
    next_io_id = 0;
    live_len = 0;
    resetNet();
}

pub fn end() void {
    refuseOtherThread("sim.end");
    freeNet();
    restoreFdNamespace();
    sim_thread.store(0, .release);
    begun = false;
    clock_routed = true;
}

pub fn setClockRouted(routed: bool) void {
    refuseOtherThread("sim.setClockRouted");
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

/// The trace id of a task, given the task's own `sim_id` field. The first
/// time the trace sees a task it gets the next id, in the order the seeded
/// schedule first sees tasks, and keeps it for its life.
///
/// The id lives in the task, not in a table keyed by its address (t-4092).
/// Keyed by address, a task created where a freed one had been inherited the
/// freed task's id; whether an allocator hands back a freed address varies
/// between processes (page_allocator) or merges distinct tasks under one id
/// (the task pool), so the same seed gave different traces in different
/// processes. A run where no address was reused gets the same ids as before.
pub fn taskId(slot: *u32) u32 {
    if (comptime !compiled_in) return 0;
    if (!begun) return 0;
    if (slot.* == 0) {
        slot.* = next_task_id;
        next_task_id += 1;
    }
    return slot.*;
}

/// Tasks given an id so far in this run.
pub fn tasksSeen() u32 {
    return next_task_id - 1;
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
    // Was the address table's length: equal to this whenever no address
    // was reused, so digests of such runs are unchanged (t-4092).
    std.mem.writeInt(u32, buf[20..24], next_task_id - 1, .little);
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
    if (net_used) digestNet(&h);
    return h.final();
}

/// The datagram table, in `stateDigest`, for a run that opened a socket:
/// the wire digest, the port counter, the drop count, what is still on the
/// wire, and each socket's address, flags, limit, queue and stats.
fn digestNet(h: *std.hash.Wyhash) void {
    var b: [8]u8 = undefined;
    std.mem.writeInt(u64, &b, wireDigest(), .little);
    h.update(&b);
    std.mem.writeInt(u64, &b, (@as(u64, next_port) << 48) | (@as(u64, @intCast(wire.items.len)) << 16), .little);
    h.update(&b);
    std.mem.writeInt(u64, &b, dropped_no_socket, .little);
    h.update(&b);
    // A datagram still on the wire has not reached the wire digest, so its
    // flow is hashed here: two runs that differ only in where it goes have
    // different futures. A socket's queue needs no such entry: what landed
    // and in what order is in the wire digest, and the queue is that order
    // less the `meta_len` already received.
    for (wire.items) |w| {
        var e: [32]u8 = undefined;
        std.mem.writeInt(u64, e[0..8], w.due_at, .little);
        std.mem.writeInt(u64, e[8..16], w.seq, .little);
        @memcpy(e[16..20], &w.from.ip);
        std.mem.writeInt(u16, e[20..22], w.from.port, .little);
        @memcpy(e[22..26], &w.to.ip);
        std.mem.writeInt(u16, e[26..28], w.to.port, .little);
        std.mem.writeInt(u32, e[28..32], @intCast(w.bytes.len), .little);
        h.update(&e);
        h.update(w.bytes);
    }
    for (dgrams) |s| {
        var e: [24]u8 = undefined;
        e[0] = @intFromBool(s.used) | (@as(u8, @intFromBool(s.bound)) << 1) | (@as(u8, @intFromBool(s.nonblocking)) << 2) | (@as(u8, @intFromBool(s.dst_addr)) << 3) | (@as(u8, @intFromBool(s.parked != null)) << 4);
        @memcpy(e[1..5], &s.local.ip);
        std.mem.writeInt(u16, e[5..7], s.local.port, .little);
        std.mem.writeInt(u32, e[7..11], @intCast(s.byte_limit), .little);
        std.mem.writeInt(u32, e[11..15], @intCast(s.charged), .little);
        std.mem.writeInt(u16, e[15..17], @intCast(s.meta_len), .little);
        std.mem.writeInt(u32, e[17..21], @intCast(s.stats.received), .little);
        std.mem.writeInt(u16, e[21..23], @intCast(s.stats.dropped_rcvbuf & 0xffff), .little);
        e[23] = @truncate(s.stats.sent);
        h.update(&e);
        // Which op waits here, and for what: a datagram to this socket
        // completes it, so two runs with the ops swapped differ.
        var pk: [5]u8 = @splat(0);
        if (s.parked) |p| {
            std.mem.writeInt(u32, pk[0..4], p.id, .little);
            pk[4] = @backingInt(p.wait);
        }
        h.update(&pk);
    }
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
    // extra_os_threads: a zio wake or a primitive's state change from another
    // thread panics (`refuseOtherThread`); atomics and C code are not seen.
    const common_unsim = "file_io, connect_accept, extra_os_threads (a zio wake from one panics), dns, boot_vs_awake (boot==awake), spawn_blocking (panics), work_stealing (sim cannot run work_stealing), worker_os_threads (logical workers share one thread), loop_sleep_handshake (one thread: a wake never races a loop's sleep, so the sleeping bit is never seen set)";
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
    refuseOtherThread("sim.pipePair");
    checkFdNamespace();
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

/// Whether the op submitted as (`c`, `id`) is still live: not completed, not
/// canceled. Compares the pointer and never reads through it, so it is safe
/// for a completion whose frame may already be freed.
pub fn isLive(c: *anyopaque, id: u32) bool {
    for (live[0..live_len]) |l| {
        if (l.c == c and l.id == id) return true;
    }
    return false;
}

/// Ends the live op (`c`, `id`) at its harvest. It must be live: the caller
/// checked `isLive` first, and nothing in between may end it.
pub fn endOp(c: *anyopaque, id: u32) void {
    var i: usize = 0;
    while (i < live_len) : (i += 1) {
        if (live[i].c == c and live[i].id == id) {
            live[i] = live[live_len - 1];
            live_len -= 1;
            return;
        }
    }
    panic("sim: op {d} completed but not live: it ended twice, or was never submitted", .{id});
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
    pushDueAt(c, kind, id, clock_ns + ioDelayNs());
}

fn pushDueAt(c: *anyopaque, kind: DueKind, id: u32, at: u64) void {
    if (due_len >= max_due) panic("sim: I/O due list full", .{});
    if (due_len >= live_len) panic("sim: {d} ops due but only {d} live: an op is due twice, or due without an id", .{ due_len + 1, live_len });
    // A datagram op is due now, never later, so it is harvested before the
    // clock moves. The socket generation (`dgramOpOnItsSocket`) is what keeps
    // a closed and reopened number from reaching it; this is a second guard
    // that keeps the window to one batch. See `wakeParked`.
    if (kind == .dgram and at != clock_ns) panic("sim: a datagram op made due at {d} ns, not now ({d} ns)", .{ at, clock_ns });
    due[due_len] = .{
        .c = c,
        .due_at = at,
        .kind = kind,
        .id = id,
    };
    due_len += 1;
}

pub fn hasDueIo() bool {
    for (due[0..due_len]) |d| {
        if (d.due_at <= clock_ns) return true;
    }
    // No datagram term: the clock moves only in `Loop.waitForEvents`, and
    // every `Loop.poll` harvests right after it, which lands every datagram
    // due by then. A datagram is never due and not yet landed here.
    return false;
}

/// Remaining ns until the next not-yet-due I/O, or null. A datagram on the
/// wire counts: its landing is I/O the clock must reach.
pub fn nextDueIoRemaining() ?u64 {
    var min_at: ?u64 = null;
    for (due[0..due_len]) |d| {
        if (d.due_at <= clock_ns) continue;
        if (min_at == null or d.due_at < min_at.?) min_at = d.due_at;
    }
    // `wire` is in landing order, so the first entry not yet due is the next.
    for (wire.items) |w| {
        if (w.due_at <= clock_ns) continue;
        if (min_at == null or w.due_at < min_at.?) min_at = w.due_at;
        break;
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
    refuseOtherThread("sim.setSendFaults");
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

/// A woken pipe receive at its harvest: its bytes, or null when another
/// receive took them first and the end is open and not at end of stream. Then
/// it parks again with its submit id, as a kernel reader keeps waiting. A
/// receive with no buffer room completes with 0, as before (t-4096).
pub fn harvestRecv(fd: i32, dst: []const []u8, c: *anyopaque, id: u32) ?usize {
    const i = endIndex(fd) orelse panic("sim: a harvested receive on sim fd {d}, which is not a pipe end", .{fd});
    var room: usize = 0;
    for (dst) |d| room += d.len;
    if (ends[i].buf_len == 0 and room > 0) {
        if (ends[i].pending_recv != null) panic("sim: two recvs parked on one fd", .{});
        ends[i].pending_recv = .{ .c = c, .id = id };
        return null;
    }
    return drainForRecv(fd, dst);
}

/// For a harvested recv: drain, and wake a sender the drain made room for.
pub fn drainForRecv(fd: i32, dst: []const []u8) usize {
    const n = drainBuf(fd, dst);
    if (n > 0) {
        if (endIndex(fd)) |i| wakeSender(i);
    }
    return n;
}

/// The bytes buffered for a read on pipe end `fd`. For tests that check a
/// model of the pipe against the sim after each step.
pub fn testPipeBuffered(fd: i32) usize {
    if (comptime !builtin.is_test) @compileError("testPipeBuffered is for tests");
    const i = endIndex(fd) orelse panic("sim: testPipeBuffered on fd {d}, which is not a pipe end", .{fd});
    return ends[i].buf_len;
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
    for (&dgrams) |*d| {
        if (d.parked) |p| {
            if (p.c == c) d.parked = null;
        }
    }
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

/// Land the datagrams due by now (`landDue`), then pop completions whose
/// due_at is now, shuffled when n>1. Caller harvests. Future entries stay on
/// the list.
pub fn takeDue(out: []TakenDue) usize {
    landDue();
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

// ---- Datagram sockets (sim network design, rev 3, sections 3, 4, 6) ----
//
// The sim side of IPv4 UDP on 127.0.0.0/8. The socket layer
// (src/os/net.zig) calls these for a sim fd during a run; nothing else does.
//
// Model:
// - A datagram socket is a sim fd: `fd_base + max_ends + j` for slot `j` of
//   `dgrams`. The lowest free slot is used, so a closed fd number is reused
//   by the next open.
// - Port 0 takes the next free port from a counter that starts at 49152 in
//   every run. A socket that sends before it is bound is bound to the
//   wildcard address (0.0.0.0) and the next port first, as the kernels do.
// - A send's source is its `from` if given, else the bound address, or
//   127.0.0.1 for a wildcard socket: Linux picks 127.0.0.1 for every 127/8
//   destination (measured), and macOS has no other loopback address.
// - A sent datagram goes on the wire and lands `wire_latency_ns` later. Each
//   flow (source and destination address and port) is first-in first-out.
//   Datagrams of different flows that are due at the same time land in a
//   seeded order (`landDue`).
// - A datagram lands in the socket bound to its destination: the exact
//   address first, else a wildcard (0.0.0.0) socket on that port. With no such
//   socket it is dropped and counted (`datagramsDroppedNoSocket`). The sender
//   has already succeeded either way.
// - Each socket's receive queue has two limits: bytes (`default_rcvbuf_bytes`)
//   and packets (`rcv_packet_limit`). A datagram costs its length plus
//   `datagram_overhead` bytes, and one packet. One that does not fit is
//   dropped and counted (`DatagramStats.dropped_rcvbuf`), never a panic.
// - Socket options come in four classes (`dgramSetOption`): modelled,
//   accepted with no effect (`SO_SNDBUF`), unsupported
//   (`error.OptionNotSupported`, nothing changes, the caller may ignore it),
//   and misuse (a panic naming the option).
// - Receives are non-suspending here: an empty queue is `WouldBlock`, and a
//   receive that would have to block refuses (the sim thread would never
//   wake).
//
// Refused by name (a panic): IPv6 and non-loopback addresses (in
// src/os/net.zig and here), a datagram over `max_datagram_len`, a send to port
// 0, a bind of a bound socket, any use of a closed sim fd, and every call from
// a thread that is not the sim thread.

/// An IPv4 address and a port in host byte order.
pub const Ip4 = struct {
    ip: [4]u8,
    port: u16,
};

pub const max_dgram = 64;
/// Every sim fd number: pipe ends first, then datagram sockets.
const sim_fd_count = max_ends + max_dgram;
pub const max_datagram_len = 65507;
pub const first_ephemeral_port: u16 = 49152;
/// Phase 1 has no faults: every datagram lands this long after its send.
pub const wire_latency_ns: u64 = 100_000;
pub const default_rcvbuf_bytes: usize = 212992;
pub const rcv_packet_limit: usize = 1024;
/// What each datagram costs against the byte limit beyond its length, so an
/// empty datagram costs something too.
pub const datagram_overhead: usize = 256;

pub const DatagramStats = struct {
    /// Datagrams this socket sent.
    sent: u64 = 0,
    /// Datagrams that landed in this socket's queue.
    received: u64 = 0,
    /// Datagrams addressed to this socket that did not fit its queue.
    dropped_rcvbuf: u64 = 0,
};

const Queued = struct { off: usize, len: usize, from: Ip4, to: [4]u8 };

const Dgram = struct {
    used: bool = false,
    bound: bool = false,
    local: Ip4 = .{ .ip = @splat(0), .port = 0 },
    nonblocking: bool = false,
    dst_addr: bool = false,
    /// Payload bytes of the queued datagrams, as a ring from `ring_head`.
    ring: []u8 = &.{},
    ring_head: usize = 0,
    ring_used: usize = 0,
    /// One entry per queued datagram, as a ring from `meta_head`.
    meta: []Queued = &.{},
    meta_head: usize = 0,
    meta_len: usize = 0,
    byte_limit: usize = default_rcvbuf_bytes,
    /// Bytes charged against `byte_limit`: each queued datagram's length
    /// plus `datagram_overhead`.
    charged: usize = 0,
    stats: DatagramStats = .{},
    /// An ev op waiting for a datagram (a receive, or a poll for reading):
    /// a landing or a close makes it due now.
    parked: ?DgramParked = null,
};

const WireDatagram = struct {
    seq: u64,
    from: Ip4,
    to: Ip4,
    bytes: []u8,
    due_at: u64,
};

/// Socket queues and datagrams on the wire. Not the runtime's allocator: the
/// socket layer has no runtime handle. Addresses never enter a trace. A
/// `var` only so a test can make it fail (`testSetNetAllocator`).
var net_alloc: std.mem.Allocator = std.heap.smp_allocator;

fn testSetNetAllocator(a: std.mem.Allocator) void {
    if (comptime !builtin.is_test) @compileError("testSetNetAllocator is for tests");
    net_alloc = a;
}

var dgrams: [max_dgram]Dgram = @splat(.{});
/// Each slot's generation: how many times it was opened in this run. An ev
/// op records it at submit (`dgramOpStart`); a slot closed or opened again
/// since has another, or none open.
var dgram_gen: [max_dgram]u32 = @splat(0);
var next_port: u16 = first_ephemeral_port;
var dropped_no_socket: u64 = 0;
/// Whether this run opened a datagram socket. Only then does
/// `stateDigest` cover the datagram table, so a run with none keeps the
/// digest it had before datagram sockets existed (design section 7, promise
/// 1).
var net_used: bool = false;
/// Every datagram that reached its landing time, in landing order: when,
/// the flow, the outcome, the length and the bytes (`wireDigest`).
var wire_hasher: std.hash.Wyhash = std.hash.Wyhash.init(0);
/// Datagrams not yet landed, sorted by (due_at, seq): landing order.
var wire: std.ArrayList(WireDatagram) = .empty;
var wire_seq: u64 = 0;

fn resetNet() void {
    for (dgrams) |d| {
        if (d.used) panic("sim: a datagram socket outlived the last run", .{});
    }
    if (wire.items.len != 0) panic("sim: datagrams on the wire outlived the last run", .{});
    next_port = first_ephemeral_port;
    dgram_gen = @splat(0);
    dropped_no_socket = 0;
    wire_seq = 0;
    net_used = false;
    wire_hasher = std.hash.Wyhash.init(0);
}

/// Frees every socket and every datagram still on the wire, at `end`.
fn freeNet() void {
    for (&dgrams) |*d| {
        if (d.used) freeQueue(d);
        d.* = .{};
    }
    for (wire.items) |w| net_alloc.free(w.bytes);
    wire.clearAndFree(net_alloc);
}

fn freeQueue(d: *Dgram) void {
    net_alloc.free(d.ring);
    net_alloc.free(d.meta);
}

/// Whether `fd` is a sim fd of the current run: a pipe end or a datagram
/// socket, open or closed. During a run no real descriptor can have such a
/// number (section 3.3), so the socket layer routes these here. Outside a run
/// it is false for every fd.
pub fn isSimFd(fd: i32) bool {
    if (comptime !compiled_in) return false;
    if (sim_thread.load(.acquire) == 0) return false;
    return fd >= fd_base and fd < fd_base + sim_fd_count;
}

/// The native-exit refusal (design section 3): the first line of every
/// function in `src/os/` that takes an fd and has no sim branch of its own. A
/// sim fd is not a kernel descriptor (section 3.3), so one that reaches a
/// syscall is a bug; this panics, naming the function. `src` is the caller's
/// `@src()`. Compiled out without sim, and a no-op outside a run.
/// `sim_exits_test.zig` fails the build when an fd-taking function lacks it.
pub inline fn refuseSimFd(comptime src: std.builtin.SourceLocation, fd: anytype) void {
    if (comptime !compiled_in or builtin.os.tag == .windows) return;
    if (isSimFd(@intCast(fd))) refuse("sim: sim fd {d} reached the native exit " ++ src.file ++ " " ++ src.fn_name, .{fd});
}

/// `refuseSimFd` for a function that takes several fds, as a tuple.
pub inline fn refuseSimFds(comptime src: std.builtin.SourceLocation, fds: anytype) void {
    inline for (fds) |fd| refuseSimFd(src, fd);
}

/// Whether `fd` is in the datagram part of the sim fd range (open or not).
pub fn isDgramFd(fd: i32) bool {
    return isSimFd(fd) and fd >= fd_base + max_ends;
}

pub fn isRunning() bool {
    if (comptime !compiled_in) return false;
    return sim_thread.load(.acquire) != 0;
}

fn dgramSlot(comptime what: []const u8, fd: i32) *Dgram {
    if (fd < fd_base + max_ends or fd >= fd_base + sim_fd_count) {
        panic("sim: " ++ what ++ " on sim fd {d}, which is not a datagram socket", .{fd});
    }
    const d = &dgrams[@intCast(fd - fd_base - max_ends)];
    if (!d.used) panic("sim: " ++ what ++ " on sim fd {d}, which is closed", .{fd});
    return d;
}

fn isLoopback(ip: [4]u8) bool {
    return ip[0] == 127;
}

fn isWildcard(ip: [4]u8) bool {
    return std.mem.eql(u8, &ip, &[4]u8{ 0, 0, 0, 0 });
}

fn refuseNonLoopback(comptime what: []const u8, ip: [4]u8) void {
    if (isLoopback(ip)) return;
    panic("sim: " ++ what ++ " {d}.{d}.{d}.{d}: only 127.0.0.0/8 is simulated", .{ ip[0], ip[1], ip[2], ip[3] });
}

fn portTaken(ip: [4]u8, port: u16) bool {
    for (dgrams) |d| {
        if (!d.used or !d.bound or d.local.port != port) continue;
        if (isWildcard(ip) or isWildcard(d.local.ip) or std.mem.eql(u8, &ip, &d.local.ip)) return true;
    }
    return false;
}

fn ephemeralPort(ip: [4]u8) error{AddressInUse}!u16 {
    var tries: u32 = 0;
    while (tries < 65536 - @as(u32, first_ephemeral_port)) : (tries += 1) {
        const p = next_port;
        next_port = if (next_port == 65535) first_ephemeral_port else next_port + 1;
        if (!portTaken(ip, p)) return p;
    }
    return error.AddressInUse;
}

/// `socket(AF_INET, SOCK_DGRAM)` during a run.
pub fn dgramOpen(nonblocking: bool) error{SystemResources}!i32 {
    refuseOtherThread("sim socket open");
    if (comptime builtin.os.tag == .windows) panic("sim: sim sockets are not supported on Windows", .{});
    checkFdNamespace();
    var j: usize = 0;
    while (j < max_dgram and dgrams[j].used) j += 1;
    if (j == max_dgram) panic("sim: the datagram socket table is full ({d} sockets)", .{max_dgram});
    const ring = net_alloc.alloc(u8, default_rcvbuf_bytes) catch return error.SystemResources;
    errdefer net_alloc.free(ring);
    const meta = net_alloc.alloc(Queued, rcv_packet_limit) catch return error.SystemResources;
    dgrams[j] = .{ .used = true, .nonblocking = nonblocking, .ring = ring, .meta = meta };
    dgram_gen[j] += 1;
    net_used = true;
    return fd_base + max_ends + @as(i32, @intCast(j));
}

/// `bind`. Port 0 takes the next ephemeral port. `addr.ip` is 127/8 or
/// 0.0.0.0.
pub fn dgramBind(fd: i32, addr: Ip4) error{AddressInUse}!void {
    refuseOtherThread("sim socket bind");
    const d = dgramSlot("bind", fd);
    if (d.bound) panic("sim: bind on sim fd {d}, which is already bound", .{fd});
    if (!isWildcard(addr.ip)) refuseNonLoopback("bind to", addr.ip);
    const port = if (addr.port == 0) try ephemeralPort(addr.ip) else blk: {
        if (portTaken(addr.ip, addr.port)) return error.AddressInUse;
        break :blk addr.port;
    };
    d.bound = true;
    d.local = .{ .ip = addr.ip, .port = port };
}

/// The bound address, or 0.0.0.0:0 before a bind or a send (`getsockname`).
pub fn dgramLocal(fd: i32) Ip4 {
    return dgramSlot("getsockname", fd).local;
}

pub fn dgramSetNonBlocking(fd: i32, on: bool) void {
    refuseOtherThread("sim socket setNonBlocking");
    dgramSlot("setNonBlocking", fd).nonblocking = on;
}

pub fn dgramIsNonBlocking(fd: i32) bool {
    return dgramSlot("isNonBlocking", fd).nonblocking;
}

pub fn dgramDestinationAddressEnabled(fd: i32) bool {
    return dgramSlot("recvmsg", fd).dst_addr;
}

/// After this, a receive reports the address each datagram was sent to.
pub fn dgramEnableDestinationAddress(fd: i32) void {
    refuseOtherThread("sim socket enableDestinationAddress");
    dgramSlot("enableDestinationAddress", fd).dst_addr = true;
}

/// One datagram of `src` gathered, to `to`. With `from`, the datagram leaves
/// from that address, which must be one the socket can receive on: its bound
/// address, or for a wildcard socket any 127/8 address. Else
/// `AddressUnavailable`, as the kernel says. Completes at once; the datagram
/// lands `wire_latency_ns` later.
pub fn dgramSend(fd: i32, src: []const []const u8, to: Ip4, from: ?[4]u8) error{ AddressUnavailable, SystemResources }!usize {
    refuseOtherThread("sim socket send");
    const d = dgramSlot("send", fd);
    refuseNonLoopback("send to", to.ip);
    if (to.port == 0) panic("sim: send to port 0 from sim fd {d}", .{fd});
    const total = totalLen(src);
    if (total > max_datagram_len) panic("sim: a datagram of {d} bytes from sim fd {d}: the limit is {d}", .{ total, fd, max_datagram_len });
    if (!d.bound) {
        // As macOS and Linux do (design section 4): the wildcard address and
        // the next port, so a datagram to any local address on that port
        // lands here. The source is picked per send, below.
        const wildcard: [4]u8 = .{ 0, 0, 0, 0 };
        const port = ephemeralPort(wildcard) catch panic("sim: no free ephemeral port to bind sim fd {d} for its first send", .{fd});
        d.bound = true;
        d.local = .{ .ip = wildcard, .port = port };
    }
    var src_ip: [4]u8 = if (isWildcard(d.local.ip)) .{ 127, 0, 0, 1 } else d.local.ip;
    if (from) |f| {
        const ok = if (isWildcard(d.local.ip)) isLoopback(f) else std.mem.eql(u8, &f, &d.local.ip);
        if (!ok) return error.AddressUnavailable;
        src_ip = f;
    }
    const bytes = net_alloc.alloc(u8, total) catch return error.SystemResources;
    var at: usize = 0;
    for (src) |sl| {
        @memcpy(bytes[at..][0..sl.len], sl);
        at += sl.len;
    }
    const entry: WireDatagram = .{
        .seq = wire_seq,
        .from = .{ .ip = src_ip, .port = d.local.port },
        .to = to,
        .bytes = bytes,
        .due_at = clock_ns + wire_latency_ns,
    };
    // Keep `wire` in landing order: after every entry due no later.
    var pos = wire.items.len;
    while (pos > 0 and wire.items[pos - 1].due_at > entry.due_at) pos -= 1;
    wire.insert(net_alloc, pos, entry) catch {
        net_alloc.free(bytes);
        return error.SystemResources;
    };
    wire_seq += 1;
    d.stats.sent += 1;
    return total;
}

pub const Received = struct {
    /// Bytes copied into the buffers.
    copied: usize,
    /// The datagram's full length; more than `copied` when it was truncated.
    len: usize,
    from: Ip4,
    /// The address the datagram was sent to, if the socket asked for it
    /// (`dgramEnableDestinationAddress`); else null.
    to: ?[4]u8,
};

/// Takes the first queued datagram, scattered into `dst`. What does not fit
/// is lost: the datagram is consumed whole, as the kernel does. An empty
/// queue is `WouldBlock` when `dont_wait` or the socket is non-blocking;
/// otherwise the receive would block the sim thread for ever, so it refuses.
pub fn dgramRecv(fd: i32, dst: []const []u8, dont_wait: bool) error{WouldBlock}!Received {
    refuseOtherThread("sim socket receive");
    const d = dgramSlot("receive", fd);
    if (d.meta_len == 0) {
        if (dont_wait or d.nonblocking) return error.WouldBlock;
        panic("sim: a blocking receive on sim fd {d}, which is not non-blocking: nothing can land while the sim thread waits", .{fd});
    }
    const q = d.meta[d.meta_head];
    var copied: usize = 0;
    for (dst) |buf| {
        if (copied == q.len) break;
        const k = @min(buf.len, q.len - copied);
        ringRead(d, q.off + copied, buf[0..k]);
        copied += k;
    }
    d.meta_head = (d.meta_head + 1) % d.meta.len;
    d.meta_len -= 1;
    d.ring_used -= q.len;
    // The ring starts where the next queued datagram's bytes start. An empty
    // datagram occupies no bytes, so the ring can hold none while entries
    // remain: only an empty queue may reset the head.
    if (d.meta_len == 0) {
        if (d.ring_used != 0) panic("sim: sim fd {d} has no queued datagram but {d} queued bytes", .{ fd, d.ring_used });
        d.ring_head = 0;
    } else {
        d.ring_head = d.meta[d.meta_head].off;
    }
    d.charged -= q.len + datagram_overhead;
    return .{ .copied = copied, .len = q.len, .from = q.from, .to = if (d.dst_addr) q.to else null };
}

fn ringRead(d: *const Dgram, off: usize, out: []u8) void {
    const start = off % d.ring.len;
    const first = @min(out.len, d.ring.len - start);
    @memcpy(out[0..first], d.ring[start..][0..first]);
    @memcpy(out[first..], d.ring[0 .. out.len - first]);
}

fn ringWrite(d: *Dgram, off: usize, in: []const u8) void {
    const start = off % d.ring.len;
    const first = @min(in.len, d.ring.len - start);
    @memcpy(d.ring[start..][0..first], in[0..first]);
    @memcpy(d.ring[0 .. in.len - first], in[first..]);
}

/// `close`: frees the socket, its port and its queue, and wakes an ev op
/// parked on it. Datagrams already on the wire to its address land in
/// whatever socket holds that address then.
pub fn dgramClose(fd: i32) void {
    refuseOtherThread("sim socket close");
    const d = dgramSlot("close", fd);
    // A real backend leaves such an op waiting for ever; the sim ends it, so
    // a test of close does not hang. It completes FileDescriptorNotASocket.
    // The op's generation no longer matches the slot's, so its harvest
    // ends it whatever the fd number holds by then.
    if (d.parked) |p| {
        d.parked = null;
        pushDueAt(p.c, .dgram, p.id, clock_ns);
    }
    freeQueue(d);
    d.* = .{};
}

pub fn datagramStats(fd: i32) DatagramStats {
    if (comptime !compiled_in) unreachable;
    return dgramSlot("datagramStats", fd).stats;
}

/// Datagrams that landed where no socket was bound, in this run.
pub fn datagramsDroppedNoSocket() u64 {
    return dropped_no_socket;
}

// The ev ops on a datagram socket (`Loop.submitSimDgram`): each gets an op id
// at submit. One that has its result at once is due now, with no seeded
// delay; one that must wait for a datagram parks on the socket.

/// Starts an ev op; for one on an open datagram socket (`fd`), records the
/// socket slot's generation in the op's live entry.
pub fn dgramOpStart(c: *anyopaque, fd: ?i32) u32 {
    const id = allocOpId(c);
    if (fd) |f| {
        if (dgramIsOpen(f)) live[live_len - 1].sock_gen = dgram_gen[dgramIndex(f)];
    }
    return id;
}

fn dgramIndex(fd: i32) usize {
    return @intCast(fd - fd_base - max_ends);
}

/// Whether the live op (`c`, `id`) still has the socket it was submitted on:
/// the fd's slot is open and has the generation the op recorded. False after
/// a close, also when a new open has reused the number since. The harvest
/// asks this before it retries, completes or parks again an op that has no
/// result yet; false ends the op FileDescriptorNotASocket.
///
/// Why a generation and not "is the number open now": a callback run inside
/// the harvest, earlier in the same batch, can close the socket and open a
/// new one that gets the same number (round 3 of the outside review, P1).
/// Also a task that such a callback runs (`Executor.simCoopYield` from a
/// CompletionQueue's owner callback). Tests: `sim: dgram ev: a receive woken
/// by a landing whose socket a callback earlier in the same harvest ...` and
/// `sim: dgram ev: a task that a CompletionQueue callback runs inside the
/// harvest ...`.
pub fn dgramOpOnItsSocket(c: *anyopaque, id: u32, fd: i32) bool {
    for (live[0..live_len]) |l| {
        if (l.c == c and l.id == id) {
            return dgramIsOpen(fd) and l.sock_gen != 0 and dgram_gen[dgramIndex(fd)] == l.sock_gen;
        }
    }
    panic("sim: op {d} on sim fd {d} is not live at its harvest", .{ id, fd });
}

pub fn dgramOpDone(c: *anyopaque, id: u32) void {
    pushDueAt(c, .dgram, id, clock_ns);
}

/// What waits on a datagram socket: a receive takes the datagram, a poll
/// only learns it is there. Both are in the state digest.
pub const DgramWait = enum(u8) { recv = 1, poll = 2 };

const DgramParked = struct {
    c: *anyopaque,
    id: u32,
    wait: DgramWait,
};

pub fn dgramPark(fd: i32, c: *anyopaque, id: u32, wait: DgramWait) void {
    const d = dgramSlot("an ev receive or poll", fd);
    if (d.parked != null) panic("sim: two ev ops parked on sim datagram socket {d}", .{fd});
    d.parked = .{ .c = c, .id = id, .wait = wait };
}

pub fn dgramIsOpen(fd: i32) bool {
    if (fd < fd_base + max_ends or fd >= fd_base + sim_fd_count) return false;
    return dgrams[@intCast(fd - fd_base - max_ends)].used;
}

/// `poll` with a zero timeout: readable when a datagram is queued; a
/// datagram socket is always writable (send pressure is not modelled).
/// Refuses a pipe end and a closed socket, so a poll calls it for every
/// entry, whatever its events.
pub fn dgramReadable(fd: i32) bool {
    return dgramSlot("poll", fd).meta_len > 0;
}

// ---- Socket options (design section 8) ----

const sys = std.posix.system;
/// macOS's SO_NET_SERVICE_TYPE (sys/socket.h), which ENet's QoS option sets.
const darwin_so_net_service_type: u32 = 0x1116;

const OptionClass = enum { rcvbuf, dst_addr, so_error, sndbuf, unsupported, misuse };

fn classifyOption(level: i32, name: u32) OptionClass {
    if (level == sys.SOL.SOCKET) {
        if (name == sys.SO.RCVBUF) return .rcvbuf;
        if (name == sys.SO.ERROR) return .so_error;
        if (name == sys.SO.SNDBUF) return .sndbuf;
        if (name == sys.SO.RCVLOWAT or name == sys.SO.REUSEADDR or
            name == sys.SO.RCVTIMEO or name == sys.SO.SNDTIMEO) return .unsupported;
        if (builtin.os.tag.isDarwin() and name == darwin_so_net_service_type) return .unsupported;
        if (builtin.os.tag == .linux and name == sys.SO.PRIORITY) return .unsupported;
        return .misuse;
    }
    if (level == sys.IPPROTO.IP) {
        const dst_opt = if (builtin.os.tag == .linux) sys.IP.PKTINFO else sys.IP.RECVDSTADDR;
        if (name == dst_opt) return .dst_addr;
        if (name == sys.IP.TOS or name == sys.IP.TTL) return .unsupported;
        return .misuse;
    }
    // ENet's QoS also sets the IPv6 traffic class on every socket.
    if (level == sys.IPPROTO.IPV6 and name == sys.IPV6.TCLASS) return .unsupported;
    return .misuse;
}

fn optionName(level: i32, name: u32) []const u8 {
    if (level == sys.SOL.SOCKET) {
        if (name == sys.SO.BROADCAST) return "SO_BROADCAST";
        if (name == sys.SO.REUSEPORT) return "SO_REUSEPORT";
        return "a SOL_SOCKET option";
    }
    if (level == sys.IPPROTO.IP) {
        if (name == sys.IP.ADD_MEMBERSHIP) return "IP_ADD_MEMBERSHIP";
        if (name == sys.IP.MULTICAST_IF) return "IP_MULTICAST_IF";
        if (name == sys.IP.MULTICAST_TTL) return "IP_MULTICAST_TTL";
        if (name == sys.IP.MULTICAST_LOOP) return "IP_MULTICAST_LOOP";
        return "an IPPROTO_IP option";
    }
    if (level == sys.IPPROTO.TCP) return "a TCP option";
    return "an option of an unknown level";
}

fn optionInt(comptime what: []const u8, fd: i32, val: []const u8) c_int {
    if (val.len != @sizeOf(c_int)) panic("sim: " ++ what ++ " on sim fd {d} with a {d}-byte value (an int is {d})", .{ fd, val.len, @sizeOf(c_int) });
    return std.mem.bytesToValue(c_int, val[0..@sizeOf(c_int)]);
}

/// `setsockopt` on a sim datagram socket. Modelled: `SO_RCVBUF` (the byte
/// limit; the queue is re-provisioned), and the destination-address option
/// (`IP_PKTINFO` on Linux, `IP_RECVDSTADDR` on macOS). Accepted with no
/// effect: `SO_SNDBUF`. A sim send never waits or fails for buffer space, so
/// any send buffer size is already satisfied, and answering 0 as a kernel
/// does keeps a caller on the branch it takes in production (ENet sets it on
/// every host). A send-side limit, if one is ever added, must model it for
/// real. Unsupported: low-water mark, address reuse, timeouts, TOS, TTL,
/// ENet's QoS options; `error.OptionNotSupported` and nothing changes.
/// Anything else is misuse and refuses, naming the option: broadcast,
/// multicast, port reuse, a TCP option, an unknown option or level.
pub fn dgramSetOption(fd: i32, level: i32, name: u32, val: []const u8) error{OptionNotSupported}!void {
    refuseOtherThread("sim socket setsockopt");
    const d = dgramSlot("setsockopt", fd);
    switch (classifyOption(level, name)) {
        .rcvbuf => {
            const v = optionInt("SO_RCVBUF", fd, val);
            if (v <= 0) panic("sim: SO_RCVBUF {d} on sim fd {d}: the byte limit must be positive", .{ v, fd });
            resizeQueue(d, @intCast(v));
        },
        .dst_addr => d.dst_addr = optionInt("the destination-address option", fd, val) != 0,
        .sndbuf => _ = optionInt("SO_SNDBUF", fd, val),
        .unsupported => return error.OptionNotSupported,
        .so_error, .misuse => panic("sim: setsockopt {s} (level {d}, option {d}) on sim fd {d} is misuse here", .{ optionName(level, name), level, name, fd }),
    }
}

/// `getsockopt` on a sim datagram socket: `SO_RCVBUF` (the byte limit),
/// `SO_ERROR` (always 0: nothing is pending), and the destination-address
/// option. Every other read refuses: the layer's `GetsockoptError` has no
/// "unsupported" to return.
pub fn dgramGetOption(fd: i32, level: i32, name: u32, out: []u8) void {
    const d = dgramSlot("getsockopt", fd);
    const v: c_int = switch (classifyOption(level, name)) {
        .rcvbuf => @intCast(d.byte_limit),
        .so_error => 0,
        .dst_addr => @intFromBool(d.dst_addr),
        .sndbuf, .unsupported, .misuse => panic("sim: getsockopt {s} (level {d}, option {d}) on sim fd {d} is not simulated", .{ optionName(level, name), level, name, fd }),
    };
    if (out.len != @sizeOf(c_int)) panic("sim: getsockopt on sim fd {d} into {d} bytes (an int is {d})", .{ fd, out.len, @sizeOf(c_int) });
    @memcpy(out, std.mem.asBytes(&v));
}

/// A new byte limit. The ring is re-provisioned to hold `limit` bytes, or
/// what is queued if that is more: nothing queued is dropped, and new
/// datagrams are dropped until the queue is under the limit again. Out of
/// memory refuses: `setsockopt` has no error for it in a build without sim
/// (the design asks for SystemResources; see report 7).
fn resizeQueue(d: *Dgram, limit: usize) void {
    const cap = @max(limit, d.ring_used);
    if (cap != d.ring.len) {
        const ring = net_alloc.alloc(u8, cap) catch panic("sim: out of memory for a {d}-byte receive queue", .{cap});
        var at: usize = 0;
        var k: usize = 0;
        while (k < d.meta_len) : (k += 1) {
            const q = &d.meta[(d.meta_head + k) % d.meta.len];
            ringRead(d, q.off, ring[at..][0..q.len]);
            q.off = at;
            at += q.len;
        }
        net_alloc.free(d.ring);
        d.ring = ring;
        d.ring_head = 0;
    }
    d.byte_limit = limit;
}

fn receiverOf(to: Ip4) ?*Dgram {
    var wildcard: ?*Dgram = null;
    for (&dgrams) |*d| {
        if (!d.used or !d.bound or d.local.port != to.port) continue;
        if (std.mem.eql(u8, &d.local.ip, &to.ip)) return d;
        if (isWildcard(d.local.ip)) wildcard = d;
    }
    return wildcard;
}

const LandOutcome = enum(u8) { landed = 0, no_socket = 1, rcvbuf = 2 };

/// Records a datagram at its landing time in the trace and the wire digest.
fn recordLanding(w: WireDatagram, outcome: LandOutcome) void {
    emit(.dgram, @as(u32, w.from.port) | (@as(u32, @backingInt(outcome)) << 16), (@as(u32, w.to.port) << 16) | @as(u32, @intCast(w.bytes.len)));
    var h: [25]u8 = undefined;
    std.mem.writeInt(u64, h[0..8], w.due_at, .little);
    @memcpy(h[8..12], &w.from.ip);
    std.mem.writeInt(u16, h[12..14], w.from.port, .little);
    @memcpy(h[14..18], &w.to.ip);
    std.mem.writeInt(u16, h[18..20], w.to.port, .little);
    h[20] = @backingInt(outcome);
    std.mem.writeInt(u32, h[21..25], @intCast(w.bytes.len), .little);
    wire_hasher.update(&h);
    wire_hasher.update(w.bytes);
}

/// A hash of every datagram that reached its landing time in this run, in
/// landing order: the time, both addresses and ports, landed or dropped, the
/// length, and every payload byte. Two runs with equal digests delivered the
/// same bytes to the same places at the same times.
pub fn wireDigest() u64 {
    return wire_hasher.final();
}

fn land(w: WireDatagram) void {
    const d = receiverOf(w.to) orelse {
        dropped_no_socket += 1;
        recordLanding(w, .no_socket);
        return;
    };
    const cost = w.bytes.len + datagram_overhead;
    if (d.meta_len == d.meta.len or d.charged + cost > d.byte_limit) {
        d.stats.dropped_rcvbuf += 1;
        recordLanding(w, .rcvbuf);
        return;
    }
    recordLanding(w, .landed);
    const off = (d.ring_head + d.ring_used) % d.ring.len;
    ringWrite(d, off, w.bytes);
    d.meta[(d.meta_head + d.meta_len) % d.meta.len] = .{ .off = off, .len = w.bytes.len, .from = w.from, .to = w.to.ip };
    d.meta_len += 1;
    d.ring_used += w.bytes.len;
    d.charged += cost;
    d.stats.received += 1;
    wakeParked(d);
}

/// A landing makes the op parked on its socket due now; `takeDue` takes it in
/// the same call. Between that and the op's own harvest, earlier entries of
/// the batch run their callbacks, and one of them (or a task it runs) can
/// close the socket and reuse its number. So the harvest checks the op's
/// socket generation (`dgramOpOnItsSocket`), not the fd number. `pushDueAt`
/// still refuses a datagram op due later than now: a wait across a clock
/// step would widen that window to every task, and the generation check is
/// the guard, that refusal only a second one.
fn wakeParked(d: *Dgram) void {
    const p = d.parked orelse return;
    d.parked = null;
    pushDueAt(p.c, .dgram, p.id, clock_ns);
}

fn sameFlow(a: WireDatagram, b: WireDatagram) bool {
    return std.meta.eql(a.from, b.from) and std.meta.eql(a.to, b.to);
}

/// Lands every datagram due by now, in due order. Among datagrams due at the
/// same time, each flow's land in send order, and which flow goes next is a
/// seeded choice; one flow alone draws nothing.
fn landDue() void {
    var n: usize = 0;
    while (n < wire.items.len and wire.items[n].due_at <= clock_ns) n += 1;
    if (n == 0) return;
    const due_now = wire.items[0..n];
    var i: usize = 0;
    while (i < n) {
        var g_end = i;
        while (g_end < n and due_now[g_end].due_at == due_now[i].due_at) g_end += 1;
        // Positions i..g_end are in send order. Put a seeded pick of the
        // flow heads at position p, shifting the rest right, then land it.
        var p = i;
        while (p < g_end) : (p += 1) {
            var heads: usize = 0;
            var k = p;
            while (k < g_end) : (k += 1) {
                if (isFlowHead(due_now[p..g_end], k - p)) heads += 1;
            }
            if (heads > 1) {
                var want = pickIndex(heads);
                k = p;
                while (k < g_end) : (k += 1) {
                    if (!isFlowHead(due_now[p..g_end], k - p)) continue;
                    if (want == 0) break;
                    want -= 1;
                }
                const chosen = due_now[k];
                std.mem.copyBackwards(WireDatagram, due_now[p + 1 .. k + 1], due_now[p..k]);
                due_now[p] = chosen;
            }
            land(due_now[p]);
        }
        i = g_end;
    }
    for (due_now) |w| net_alloc.free(w.bytes);
    wire.replaceRangeAssumeCapacity(0, n, &.{});
}

/// Whether `rest[k]` is the first datagram of its flow in `rest`.
fn isFlowHead(rest: []const WireDatagram, k: usize) bool {
    for (rest[0..k]) |w| {
        if (sameFlow(w, rest[k])) return false;
    }
    return true;
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

// ---- Refusals that end the run (t-4085) ----
//
// Each test runs itself again in a child process of this test binary
// (test_runner.zig selects one test by TEST_FILTER), with `death_child_env`
// set. The child takes the path that must panic; the parent checks that it
// died of SIGABRT with the expected message on stderr. A child that returns
// instead exits 0, and a child that runs on forever hits the timeout: both
// fail the parent. The child sets RLIMIT_CORE to 0 first, so no core file.

pub const death_child_env = "ZIO_SIM_DEATH_CHILD";

pub fn isDeathChild() bool {
    return std.c.getenv(death_child_env) != null;
}

pub fn noCoreDump() void {
    std.posix.setrlimit(.CORE, .{ .cur = 0, .max = 0 }) catch {};
}

fn expectChildPanics(comptime test_name: []const u8, comptime want: []const u8) !void {
    const io = std.testing.io;
    const gpa = std.testing.allocator;
    var exe_buf: [std.fs.max_path_bytes]u8 = undefined;
    const exe_len = try std.process.executablePath(io, &exe_buf);

    var env: std.process.Environ.Map = .init(gpa);
    defer env.deinit();
    if (std.c.getenv("PATH")) |path| try env.put("PATH", std.mem.span(path));
    try env.put("TEST_FILTER", test_name);
    try env.put(death_child_env, "1");

    const res = std.process.run(gpa, io, .{
        .argv = &.{exe_buf[0..exe_len]},
        .environ_map = &env,
        // A broken deadlock check runs the clock ahead forever and prints
        // nothing; this bounds that wait.
        .timeout = .{ .duration = .{ .raw = .fromSeconds(30), .clock = .awake } },
    }) catch |err| switch (err) {
        error.Timeout => {
            std.debug.print("child of \"{s}\" ran 30 s without ending\n", .{test_name});
            return err;
        },
        else => return err,
    };
    defer gpa.free(res.stdout);
    defer gpa.free(res.stderr);
    const died = switch (res.term) {
        .signal => |s| s == .ABRT,
        else => false,
    };
    if (!died or !hasPanicLine(res.stderr, want)) {
        std.debug.print("child of \"{s}\": term {any}, wanted SIGABRT and \"{s}\"; stderr:\n{s}\n", .{ test_name, res.term, want, res.stderr });
        return error.ChildDidNotPanic;
    }
}

/// Whether a line of `stderr` is the panic itself saying `want`: the line
/// `refuse` writes, or `thread <n> panic: <want>`. A plain substring match is
/// not enough: a panic's stack trace prints source lines, and the test's own
/// line holds `want` as a literal, so any other panic would match it.
fn hasPanicLine(stderr: []const u8, want: []const u8) bool {
    var it = std.mem.splitScalar(u8, stderr, '\n');
    while (it.next()) |raw| {
        var line = raw;
        if (std.mem.startsWith(u8, line, "thread ")) {
            const at = std.mem.indexOf(u8, line, " panic: ") orelse continue;
            line = line[at + " panic: ".len ..];
        }
        if (std.mem.startsWith(u8, line, want)) return true;
    }
    return false;
}

test "sim: hasPanicLine matches the panic, not a source line in its trace" {
    const want = "sim: Event.set from a thread";
    try std.testing.expect(hasPanicLine("thread 7 panic: sim: Event.set from a thread that is not\n", want));
    try std.testing.expect(hasPanicLine("x\nsim: Event.set from a thread that is not SEED=1\n", want));
    try std.testing.expect(!hasPanicLine("thread 7 panic: sim: deadlock clock_ns=0\n    try refusal(\"t\", \"sim: Event.set from a thread\", struct {\n", want));
}

const death = struct {
    const Runtime = @import("runtime.zig").Runtime;

    fn up(executors: u8) !*Runtime {
        begin(1);
        return Runtime.init(std.heap.page_allocator, .{
            .executors = .exact(executors),
            .thread_pool = .{ .max_threads = 0 },
        });
    }

    /// Runs `func(args)` on a new OS thread and joins it. The thread is
    /// expected to panic, which ends the process before the join returns.
    fn onOtherThread(comptime func: anytype, args: anytype) !void {
        const th = try std.Thread.spawn(.{}, func, args);
        th.join();
    }

    fn noop() void {}

    fn waitForever(ev: *@import("sync/Event.zig")) void {
        ev.wait() catch {};
    }
};

test "sim: refuses a task spawn from another thread" {
    if (comptime !compiled_in) return error.SkipZigTest;
    if (!isDeathChild()) return expectChildPanics("sim: refuses a task spawn from another thread", "sim: a task spawn from a thread that is not the sim thread");
    noCoreDump();
    const rt = try death.up(1);
    try death.onOtherThread(struct {
        fn run(r: *death.Runtime) void {
            _ = r.spawn(death.noop, .{}) catch {};
        }
    }.run, .{rt});
}

test "sim: refuses a loop wake from another thread" {
    if (comptime !compiled_in) return error.SkipZigTest;
    if (!isDeathChild()) return expectChildPanics("sim: refuses a loop wake from another thread", "sim: a loop wake from a thread that is not the sim thread");
    noCoreDump();
    const rt = try death.up(1);
    try death.onOtherThread(struct {
        fn run(r: *death.Runtime) void {
            r.main_executor.loop.wake();
        }
    }.run, .{rt});
}

test "sim: refuses Event.set from another thread, with no task waiting" {
    if (comptime !compiled_in) return error.SkipZigTest;
    if (!isDeathChild()) return expectChildPanics("sim: refuses Event.set from another thread, with no task waiting", "sim: Event.set from a thread that is not the sim thread");
    noCoreDump();
    _ = try death.up(1);
    var ev: @import("sync/Event.zig") = .init;
    try death.onOtherThread(@import("sync/Event.zig").set, .{&ev});
}

test "sim: refuses Event.reset from another thread" {
    if (comptime !compiled_in) return error.SkipZigTest;
    if (!isDeathChild()) return expectChildPanics("sim: refuses Event.reset from another thread", "sim: Event.reset from a thread that is not the sim thread");
    noCoreDump();
    _ = try death.up(1);
    var ev: @import("sync/Event.zig") = .init;
    try death.onOtherThread(@import("sync/Event.zig").reset, .{&ev});
}

test "sim: refuses Mutex.tryLock from another thread, uncontended" {
    if (comptime !compiled_in) return error.SkipZigTest;
    if (!isDeathChild()) return expectChildPanics("sim: refuses Mutex.tryLock from another thread, uncontended", "sim: Mutex.tryLock from a thread that is not the sim thread");
    noCoreDump();
    _ = try death.up(1);
    var m: @import("sync/Mutex.zig") = .init;
    try death.onOtherThread(struct {
        fn run(mm: *@import("sync/Mutex.zig")) void {
            _ = mm.tryLock();
        }
    }.run, .{&m});
}

test "sim: refuses Mutex.unlock from another thread, with no task waiting" {
    if (comptime !compiled_in) return error.SkipZigTest;
    if (!isDeathChild()) return expectChildPanics("sim: refuses Mutex.unlock from another thread, with no task waiting", "sim: Mutex.unlock from a thread that is not the sim thread");
    noCoreDump();
    _ = try death.up(1);
    var m: @import("sync/Mutex.zig") = .init;
    m.lockUncancelable();
    try death.onOtherThread(@import("sync/Mutex.zig").unlock, .{&m});
}

test "sim: refuses Futex.wake from another thread, with no task waiting" {
    if (comptime !compiled_in) return error.SkipZigTest;
    if (!isDeathChild()) return expectChildPanics("sim: refuses Futex.wake from another thread, with no task waiting", "sim: Futex.wake from a thread that is not the sim thread");
    noCoreDump();
    _ = try death.up(1);
    var word: u32 = 0;
    try death.onOtherThread(struct {
        fn run(w: *u32) void {
            @import("sync/Futex.zig").wake(w, 1);
        }
    }.run, .{&word});
}

test "sim: refuses Future.set from another thread, with no task waiting" {
    if (comptime !compiled_in) return error.SkipZigTest;
    if (!isDeathChild()) return expectChildPanics("sim: refuses Future.set from another thread, with no task waiting", "sim: Future.set from a thread that is not the sim thread");
    noCoreDump();
    _ = try death.up(1);
    const F = @import("sync/future.zig").Future(u32);
    var f: F = .init;
    try death.onOtherThread(struct {
        fn run(ff: *F) void {
            ff.set(7);
        }
    }.run, .{&f});
}

test "sim: refuses ev.Async.notify from another thread, before the handle is added" {
    if (comptime !compiled_in) return error.SkipZigTest;
    if (!isDeathChild()) return expectChildPanics("sim: refuses ev.Async.notify from another thread, before the handle is added", "sim: ev.Async.notify from a thread that is not the sim thread");
    noCoreDump();
    _ = try death.up(1);
    const Async = @import("ev/completion.zig").Async;
    var a: Async = .init();
    try death.onOtherThread(Async.notify, .{&a});
}

fn deadlockChild(executors: u8) !void {
    noCoreDump();
    const rt = try death.up(executors);
    // The default stack pool keeps its shrink timer: the timer that kept the
    // clock running before t-4085.
    std.debug.assert(rt.options.stack_pool.shrink_interval.value > 0);
    var ev: @import("sync/Event.zig") = .init;
    var h = try rt.spawnInto(.{ .executor = @intCast(executors - 1) }, death.waitForever, .{&ev});
    h.join();
}

test "sim: every task parked with only the shrink timer pending is a deadlock, one executor" {
    if (comptime !compiled_in) return error.SkipZigTest;
    if (!isDeathChild()) return expectChildPanics("sim: every task parked with only the shrink timer pending is a deadlock, one executor", "sim: deadlock clock_ns=0");
    try deadlockChild(1);
}

test "sim: every task parked with only the shrink timer pending is a deadlock, two executors" {
    if (comptime !compiled_in) return error.SkipZigTest;
    if (!isDeathChild()) return expectChildPanics("sim: every task parked with only the shrink timer pending is a deadlock, two executors", "sim: deadlock clock_ns=0");
    try deadlockChild(2);
}

// ---- Runs that must end normally (round 3) ----

/// Like `expectChildPanics`, for a run that must end normally. The child
/// must exit 0 within 30 s, and must prove it ran exactly the requested case:
/// each success case ends with `childDone`, which writes
/// `sim-child-done: <name>`, and exactly one such line, naming this test,
/// must appear. The runner's filter is a substring match and exits 0 when it
/// matches nothing, so exit 0 alone proves nothing (round 4, A5).
fn expectChildSucceeds(comptime test_name: []const u8) !void {
    const io = std.testing.io;
    const gpa = std.testing.allocator;
    var exe_buf: [std.fs.max_path_bytes]u8 = undefined;
    const exe_len = try std.process.executablePath(io, &exe_buf);
    var env: std.process.Environ.Map = .init(gpa);
    defer env.deinit();
    if (std.c.getenv("PATH")) |path| try env.put("PATH", std.mem.span(path));
    try env.put("TEST_FILTER", test_name);
    try env.put(death_child_env, "1");
    const res = try std.process.run(gpa, io, .{
        .argv = &.{exe_buf[0..exe_len]},
        .environ_map = &env,
        .timeout = .{ .duration = .{ .raw = .fromSeconds(30), .clock = .awake } },
    });
    defer gpa.free(res.stdout);
    defer gpa.free(res.stderr);
    const exited_zero = switch (res.term) {
        .exited => |code| code == 0,
        else => false,
    };
    if (!exited_zero) {
        std.debug.print("child of \"{s}\": term {any}, wanted exit 0; stderr:\n{s}\n", .{ test_name, res.term, res.stderr });
        return error.ChildFailed;
    }
    return checkDoneMarkers(test_name, res.stderr);
}

const done_marker = "sim-child-done: ";

/// The end of a success case in a child: proof that this case ran to its end.
pub fn childDone(comptime test_name: []const u8) void {
    const line = done_marker ++ test_name ++ "\n";
    if (comptime builtin.link_libc and builtin.os.tag != .windows) {
        _ = std.c.write(2, line.ptr, line.len);
    } else std.debug.print("{s}", .{line});
}

pub fn checkDoneMarkers(test_name: []const u8, stderr: []const u8) error{ ChildRanNoCase, ChildRanSeveralCases, ChildRanOtherCase }!void {
    var count: usize = 0;
    var named: ?[]const u8 = null;
    var it = std.mem.splitScalar(u8, stderr, '\n');
    while (it.next()) |line| {
        // The runner's progress output can share the line (an erase-line
        // escape before it), so the marker is found anywhere in the line.
        const at = std.mem.indexOf(u8, line, done_marker) orelse continue;
        count += 1;
        named = line[at + done_marker.len ..];
    }
    if (count == 0) return error.ChildRanNoCase;
    if (count > 1) return error.ChildRanSeveralCases;
    if (!std.mem.eql(u8, named.?, test_name)) return error.ChildRanOtherCase;
}

test "sim: success helper: fixture, case" {
    if (comptime !compiled_in) return error.SkipZigTest;
    if (!isDeathChild()) return;
    childDone("sim: success helper: fixture, case");
}

test "sim: success helper: fixture, other case" {
    if (comptime !compiled_in) return error.SkipZigTest;
    if (!isDeathChild()) return;
    childDone("sim: success helper: fixture, other case");
}

// `-Dtest-filter` is applied at compile time, and the child is the same
// binary: the fixtures share this test's prefix so a filter that selects it
// also compiles them in.
test "sim: success helper: rejects a filter that runs no case, several cases, or another case" {
    if (comptime !compiled_in) return error.SkipZigTest;
    if (isDeathChild()) return;
    try expectChildSucceeds("sim: success helper: fixture, case");
    try std.testing.expectError(error.ChildRanNoCase, expectChildSucceeds("sim: success helper: fixture, none"));
    try std.testing.expectError(error.ChildRanSeveralCases, expectChildSucceeds("sim: success helper: fixture, "));
    try std.testing.expectError(error.ChildRanOtherCase, expectChildSucceeds("sim: success helper: fixture, other"));
}

/// A1 (round 3): a callback on executor 1 notifies an async handle on
/// executor 0 after `simPollAll` already polled executor 0. That handle's
/// callback releases the parked task. Nothing is ready and no task timer is
/// armed at that moment, but loop 0 has a wake pending: not a deadlock.
const cross_loop = struct {
    const Loop = @import("ev/loop.zig").Loop;
    const Completion = @import("ev/completion.zig").Completion;
    const Async = @import("ev/completion.zig").Async;
    const Timer = @import("ev/completion.zig").Timer;
    const Event = @import("sync/Event.zig");
    const rt_mod = @import("runtime.zig");

    var ev: Event = .init;
    var async_h: Async = undefined;
    var timer: Timer = undefined;

    fn onAsync(_: *Loop, _: *Completion) void {
        ev.set();
    }

    fn onTimer(_: *Loop, _: *Completion) void {
        async_h.notify();
    }

    fn waiter() void {
        async_h.c.callback = onAsync;
        rt_mod.getCurrentExecutor().loop.add(&async_h.c);
        ev.wait() catch {};
    }

    fn arm() void {
        rt_mod.sleep(.fromMilliseconds(10)) catch {};
        timer.c.callback = onTimer;
        rt_mod.getCurrentExecutor().loop.add(&timer.c);
    }

    fn runSeed(seed_value: u64, shrink_timer: bool) !void {
        begin(seed_value);
        defer end();
        ev = .init;
        async_h = Async.init();
        timer = Timer.init(.{ .duration = .zero });
        var o: rt_mod.RuntimeOptions = .{
            .executors = .exact(2),
            .thread_pool = .{ .max_threads = 0 },
        };
        // Without it no timer is armed at all, and the mux reaches its last
        // deadlock check instead of the shrink-timer one.
        if (!shrink_timer) o.stack_pool.shrink_interval = .zero;
        const rt = try rt_mod.Runtime.init(std.heap.page_allocator, o);
        defer rt.deinit();
        var w = try rt.spawnInto(.{ .executor = 0 }, waiter, .{});
        var a = try rt.spawnInto(.{ .executor = 1 }, arm, .{});
        a.join();
        w.join();
    }
};

test "sim: a wake left pending on a loop already polled is not a deadlock, two executors" {
    if (comptime !compiled_in) return error.SkipZigTest;
    if (!isDeathChild()) return expectChildSucceeds("sim: a wake left pending on a loop already polled is not a deadlock, two executors");
    noCoreDump();
    var s: u64 = 1;
    while (s <= 20) : (s += 1) try cross_loop.runSeed(s, true);
    childDone("sim: a wake left pending on a loop already polled is not a deadlock, two executors");
}

test "sim: with no timer armed, a wake left pending on a loop already polled is not a deadlock" {
    if (comptime !compiled_in) return error.SkipZigTest;
    if (!isDeathChild()) return expectChildSucceeds("sim: with no timer armed, a wake left pending on a loop already polled is not a deadlock");
    noCoreDump();
    var s: u64 = 1;
    while (s <= 20) : (s += 1) try cross_loop.runSeed(s, false);
    childDone("sim: with no timer armed, a wake left pending on a loop already polled is not a deadlock");
}

/// The same cross-loop wake with a task timer armed elsewhere (a 1 s sleep
/// on executor 1): the pending wake must be handled at the time it was made,
/// not after the clock jumps to the other timer.
const cross_loop_timed = struct {
    const rt_mod = @import("runtime.zig");
    var woke_at_ns: u64 = 0;

    fn waiter() void {
        cross_loop.waiter();
        woke_at_ns = clockNs();
    }

    fn sleeper() void {
        rt_mod.sleep(.fromSeconds(1)) catch {};
    }

    fn runSeed(seed_value: u64) !void {
        begin(seed_value);
        defer end();
        cross_loop.ev = .init;
        cross_loop.async_h = cross_loop.Async.init();
        cross_loop.timer = cross_loop.Timer.init(.{ .duration = .zero });
        woke_at_ns = 0;
        const rt = try rt_mod.Runtime.init(std.heap.page_allocator, .{
            .executors = .exact(2),
            .thread_pool = .{ .max_threads = 0 },
        });
        defer rt.deinit();
        var w = try rt.spawnInto(.{ .executor = 0 }, waiter, .{});
        var a = try rt.spawnInto(.{ .executor = 1 }, cross_loop.arm, .{});
        var s = try rt.spawnInto(.{ .executor = 1 }, sleeper, .{});
        a.join();
        w.join();
        s.join();
        if (woke_at_ns != 10 * std.time.ns_per_ms) {
            std.debug.print("seed {d}: the waiter woke at virtual {d} ns, wanted 10 ms\n", .{ seed_value, woke_at_ns });
            return error.WokeLate;
        }
    }
};

test "sim: a wake left pending on a loop already polled is handled before the clock moves, two executors" {
    if (comptime !compiled_in) return error.SkipZigTest;
    if (!isDeathChild()) return expectChildSucceeds("sim: a wake left pending on a loop already polled is handled before the clock moves, two executors");
    noCoreDump();
    var s: u64 = 1;
    while (s <= 20) : (s += 1) try cross_loop_timed.runSeed(s);
    childDone("sim: a wake left pending on a loop already polled is handled before the clock moves, two executors");
}

// ---- Round 3 refusals: one test per new check ----

const other_thread_msg = " from a thread that is not the sim thread";

/// One refusal test: the child runs `body` after starting a run, and must
/// die with `want` on stderr.
fn refusal(comptime test_name: []const u8, comptime want: []const u8, comptime body: fn () anyerror!void) !void {
    if (comptime !compiled_in) return error.SkipZigTest;
    if (!isDeathChild()) return expectChildPanics(test_name, want);
    noCoreDump();
    try body();
}

const r3 = struct {
    const RwLock = @import("sync/RwLock.zig");
    const Recursive = @import("sync/Mutex.zig").Recursive;
    const Condition = @import("sync/Condition.zig");
    const Event = @import("sync/Event.zig");
    const Group = @import("group.zig").Group;
    const Timer = @import("ev/completion.zig").Timer;
    const Waiter = @import("common.zig").Waiter;
    const Signal = @import("signal.zig").Signal;
    const rt_mod = @import("runtime.zig");

    var rw: RwLock = .init;
    var rec: Recursive = .init;
    var cond: Condition = .init;
    var group: Group = .init;
    var timer: Timer = undefined;
    var ev: Event = .init;
    var rt: *rt_mod.Runtime = undefined;

    fn up() !void {
        rt = try death.up(1);
    }

    fn other(comptime f: fn () void) !void {
        try death.onOtherThread(f, .{});
    }

    fn waitForever() void {
        ev.wait() catch {};
    }

    fn noopRet() void {}
};

test "sim: refuses RwLock.lock from another thread" {
    try refusal("sim: refuses RwLock.lock from another thread", "sim: RwLock.lock" ++ other_thread_msg, struct {
        fn body() !void {
            try r3.up();
            try r3.other(struct {
                fn f() void {
                    r3.rw.lock() catch {};
                }
            }.f);
        }
    }.body);
}

test "sim: refuses RwLock.lockUncancelable from another thread" {
    try refusal("sim: refuses RwLock.lockUncancelable from another thread", "sim: RwLock.lockUncancelable" ++ other_thread_msg, struct {
        fn body() !void {
            try r3.up();
            try r3.other(struct {
                fn f() void {
                    r3.rw.lockUncancelable();
                }
            }.f);
        }
    }.body);
}

test "sim: refuses RwLock.unlock from another thread" {
    try refusal("sim: refuses RwLock.unlock from another thread", "sim: RwLock.unlock" ++ other_thread_msg, struct {
        fn body() !void {
            try r3.up();
            r3.rw.lockUncancelable();
            try r3.other(struct {
                fn f() void {
                    r3.rw.unlock();
                }
            }.f);
        }
    }.body);
}

test "sim: refuses RwLock.tryLockShared from another thread, uncontended" {
    try refusal("sim: refuses RwLock.tryLockShared from another thread, uncontended", "sim: RwLock.tryLockShared" ++ other_thread_msg, struct {
        fn body() !void {
            try r3.up();
            try r3.other(struct {
                fn f() void {
                    _ = r3.rw.tryLockShared();
                }
            }.f);
        }
    }.body);
}

test "sim: refuses RwLock.lockShared from another thread, uncontended" {
    try refusal("sim: refuses RwLock.lockShared from another thread, uncontended", "sim: RwLock.lockShared" ++ other_thread_msg, struct {
        fn body() !void {
            try r3.up();
            try r3.other(struct {
                fn f() void {
                    r3.rw.lockShared() catch {};
                }
            }.f);
        }
    }.body);
}

test "sim: refuses RwLock.lockSharedUncancelable from another thread, uncontended" {
    try refusal("sim: refuses RwLock.lockSharedUncancelable from another thread, uncontended", "sim: RwLock.lockSharedUncancelable" ++ other_thread_msg, struct {
        fn body() !void {
            try r3.up();
            try r3.other(struct {
                fn f() void {
                    r3.rw.lockSharedUncancelable();
                }
            }.f);
        }
    }.body);
}

test "sim: refuses RwLock.unlockShared from another thread, with no writer waiting" {
    try refusal("sim: refuses RwLock.unlockShared from another thread, with no writer waiting", "sim: RwLock.unlockShared" ++ other_thread_msg, struct {
        fn body() !void {
            try r3.up();
            r3.rw.lockSharedUncancelable();
            try r3.other(struct {
                fn f() void {
                    r3.rw.unlockShared();
                }
            }.f);
        }
    }.body);
}

test "sim: refuses Mutex.Recursive.unlock from another thread" {
    try refusal("sim: refuses Mutex.Recursive.unlock from another thread", "sim: Mutex.Recursive.unlock" ++ other_thread_msg, struct {
        fn body() !void {
            try r3.up();
            r3.rec.lockUncancelable();
            r3.rec.lockUncancelable();
            try r3.other(struct {
                fn f() void {
                    r3.rec.unlock();
                }
            }.f);
        }
    }.body);
}

test "sim: refuses Condition.signal from another thread, with no task waiting" {
    try refusal("sim: refuses Condition.signal from another thread, with no task waiting", "sim: Condition.signal" ++ other_thread_msg, struct {
        fn body() !void {
            try r3.up();
            try r3.other(struct {
                fn f() void {
                    r3.cond.signal();
                }
            }.f);
        }
    }.body);
}

test "sim: refuses Condition.broadcast from another thread, with no task waiting" {
    try refusal("sim: refuses Condition.broadcast from another thread, with no task waiting", "sim: Condition.broadcast" ++ other_thread_msg, struct {
        fn body() !void {
            try r3.up();
            try r3.other(struct {
                fn f() void {
                    r3.cond.broadcast();
                }
            }.f);
        }
    }.body);
}

test "sim: refuses Loop.setTimer from another thread" {
    try refusal("sim: refuses Loop.setTimer from another thread", "sim: Loop.setTimer" ++ other_thread_msg, struct {
        fn body() !void {
            try r3.up();
            r3.timer = r3.Timer.init(.{ .duration = .fromSeconds(1) });
            try r3.other(struct {
                fn f() void {
                    r3.rt.main_executor.loop.setTimer(&r3.timer, .{ .duration = .fromSeconds(2) });
                }
            }.f);
        }
    }.body);
}

test "sim: refuses Loop.rearmTimer from another thread" {
    try refusal("sim: refuses Loop.rearmTimer from another thread", "sim: Loop.rearmTimer" ++ other_thread_msg, struct {
        fn body() !void {
            try r3.up();
            r3.timer = r3.Timer.init(.{ .duration = .fromSeconds(1) });
            try r3.other(struct {
                fn f() void {
                    _ = r3.rt.main_executor.loop.rearmTimer(&r3.timer, .{ .duration = .fromSeconds(2) });
                }
            }.f);
        }
    }.body);
}

test "sim: refuses Loop.clearTimer from another thread" {
    try refusal("sim: refuses Loop.clearTimer from another thread", "sim: Loop.clearTimer" ++ other_thread_msg, struct {
        fn body() !void {
            try r3.up();
            r3.timer = r3.Timer.init(.{ .duration = .fromSeconds(1) });
            r3.rt.main_executor.loop.setTimer(&r3.timer, .{ .duration = .fromSeconds(1) });
            try r3.other(struct {
                fn f() void {
                    _ = r3.rt.main_executor.loop.clearTimer(&r3.timer);
                }
            }.f);
        }
    }.body);
}

test "sim: refuses Timer.extend from another thread" {
    try refusal("sim: refuses Timer.extend from another thread", "sim: Timer.extend" ++ other_thread_msg, struct {
        fn body() !void {
            try r3.up();
            r3.timer = r3.Timer.init(.{ .duration = .fromSeconds(1) });
            r3.rt.main_executor.loop.setTimer(&r3.timer, .{ .duration = .fromSeconds(1) });
            try r3.other(struct {
                fn f() void {
                    _ = r3.timer.extend(.fromNanoseconds(5 * std.time.ns_per_s));
                }
            }.f);
        }
    }.body);
}

test "sim: refuses Group.setFailed from another thread" {
    try refusal("sim: refuses Group.setFailed from another thread", "sim: Group.setFailed" ++ other_thread_msg, struct {
        fn body() !void {
            try r3.up();
            try r3.other(struct {
                fn f() void {
                    r3.group.setFailed();
                }
            }.f);
        }
    }.body);
}

test "sim: refuses Group.setCanceled from another thread" {
    try refusal("sim: refuses Group.setCanceled from another thread", "sim: Group.setCanceled" ++ other_thread_msg, struct {
        fn body() !void {
            try r3.up();
            try r3.other(struct {
                fn f() void {
                    r3.group.setCanceled();
                }
            }.f);
        }
    }.body);
}

test "sim: refuses Group.setFailFast from another thread" {
    try refusal("sim: refuses Group.setFailFast from another thread", "sim: Group.setFailFast" ++ other_thread_msg, struct {
        fn body() !void {
            try r3.up();
            try r3.other(struct {
                fn f() void {
                    r3.group.setFailFast();
                }
            }.f);
        }
    }.body);
}

const r3_task = struct {
    var handle: rt_mod.JoinHandle(void) = undefined;
    const rt_mod = @import("runtime.zig");
};

test "sim: refuses a task cancel request from another thread (JoinHandle.requestCancel)" {
    try refusal("sim: refuses a task cancel request from another thread (JoinHandle.requestCancel)", "sim: AnyTask.requestCancel" ++ other_thread_msg, struct {
        fn body() !void {
            try r3.up();
            r3_task.handle = try r3.rt.spawn(r3.waitForever, .{});
            try r3.rt.yield();
            try r3.other(struct {
                fn f() void {
                    r3_task.handle.requestCancel();
                }
            }.f);
        }
    }.body);
}

test "sim: refuses a task cancel from another thread (CancelHandle.cancel)" {
    try refusal("sim: refuses a task cancel from another thread (CancelHandle.cancel)", "sim: AnyTask.cancel" ++ other_thread_msg, struct {
        fn body() !void {
            try r3.up();
            r3_task.handle = try r3.rt.spawn(r3.waitForever, .{});
            try r3.rt.yield();
            try r3.other(struct {
                fn f() void {
                    r3_task.handle.cancelHandle().cancel();
                }
            }.f);
        }
    }.body);
}

test "sim: refuses releasing a finished task's handle from another thread (JoinHandle.detach)" {
    try refusal("sim: refuses releasing a finished task's handle from another thread (JoinHandle.detach)", "sim: Awaitable.release" ++ other_thread_msg, struct {
        fn body() !void {
            try r3.up();
            r3_task.handle = try r3.rt.spawn(r3.noopRet, .{});
            while (!r3_task.handle.hasResult()) try r3.rt.yield();
            try r3.other(struct {
                fn f() void {
                    r3_task.handle.detach();
                }
            }.f);
        }
    }.body);
}

test "sim: refuses a blocking wait from another thread" {
    try refusal("sim: refuses a blocking wait from another thread", "sim: a wait" ++ other_thread_msg, struct {
        fn body() !void {
            try r3.up();
            try r3.other(struct {
                fn f() void {
                    r3.ev.wait() catch {};
                }
            }.f);
        }
    }.body);
}

test "sim: refuses Runtime.init from another thread" {
    try refusal("sim: refuses Runtime.init from another thread", "sim: Runtime.init" ++ other_thread_msg, struct {
        fn body() !void {
            try r3.up();
            try r3.other(struct {
                fn f() void {
                    _ = r3.rt_mod.Runtime.init(std.heap.page_allocator, .{ .executors = .exact(1), .thread_pool = .{ .max_threads = 0 } }) catch {};
                }
            }.f);
        }
    }.body);
}

test "sim: refuses sim.end from another thread" {
    try refusal("sim: refuses sim.end from another thread", "sim: sim.end" ++ other_thread_msg, struct {
        fn body() !void {
            try r3.up();
            try r3.other(end);
        }
    }.body);
}

test "sim: refuses sim.setPreempt from another thread" {
    try refusal("sim: refuses sim.setPreempt from another thread", "sim: sim.setPreempt" ++ other_thread_msg, struct {
        fn body() !void {
            try r3.up();
            try r3.other(struct {
                fn f() void {
                    setPreempt(.{ .one_in = 2, .min_ns = 1, .max_ns = 2 });
                }
            }.f);
        }
    }.body);
}

test "sim: refuses sim.setClockRouted from another thread" {
    try refusal("sim: refuses sim.setClockRouted from another thread", "sim: sim.setClockRouted" ++ other_thread_msg, struct {
        fn body() !void {
            try r3.up();
            try r3.other(struct {
                fn f() void {
                    setClockRouted(false);
                }
            }.f);
        }
    }.body);
}

test "sim: refuses sim.pipePair from another thread" {
    try refusal("sim: refuses sim.pipePair from another thread", "sim: sim.pipePair" ++ other_thread_msg, struct {
        fn body() !void {
            try r3.up();
            try r3.other(struct {
                fn f() void {
                    _ = pipePair();
                }
            }.f);
        }
    }.body);
}

const r3_fd = struct {
    var fd: i32 = 0;
};

test "sim: refuses sim.setSendFaults from another thread" {
    try refusal("sim: refuses sim.setSendFaults from another thread", "sim: sim.setSendFaults" ++ other_thread_msg, struct {
        fn body() !void {
            try r3.up();
            r3_fd.fd = pipePair()[0];
            try r3.other(struct {
                fn f() void {
                    setSendFaults(r3_fd.fd, .{ .short_max = 3 });
                }
            }.f);
        }
    }.body);
}

test "sim: refuses Signal.init during a run" {
    try refusal("sim: refuses Signal.init during a run", "sim: Signal.init (a real OS signal arrives at a real time) during a sim run", struct {
        fn body() !void {
            try r3.up();
            var sig = try r3.Signal.init(.user1);
            sig.deinit();
        }
    }.body);
}

const r3_sig = struct {
    var sig: r3.Signal = undefined;
};

test "sim: refuses Signal.wait during a run, for a signal registered before it" {
    try refusal("sim: refuses Signal.wait during a run, for a signal registered before it", "sim: Signal.wait (a real OS signal arrives at a real time) during a sim run", struct {
        fn body() !void {
            r3_sig.sig = try r3.Signal.init(.user1);
            try r3.up();
            r3_sig.sig.wait() catch {};
        }
    }.body);
}

test "sim: refuses Signal.waitTimeout during a run, for a signal registered before it" {
    try refusal("sim: refuses Signal.waitTimeout during a run, for a signal registered before it", "sim: Signal.waitTimeout (a real OS signal arrives at a real time) during a sim run", struct {
        fn body() !void {
            r3_sig.sig = try r3.Signal.init(.user1);
            try r3.up();
            r3_sig.sig.waitTimeout(.fromNanoseconds(std.time.ns_per_s)) catch {};
        }
    }.body);
}

test "sim: refuses Signal.asyncWait during a run, for a signal registered before it" {
    try refusal("sim: refuses Signal.asyncWait during a run, for a signal registered before it", "sim: Signal.asyncWait (a real OS signal arrives at a real time) during a sim run", struct {
        fn body() !void {
            r3_sig.sig = try r3.Signal.init(.user1);
            try r3.up();
            var w: r3.Waiter = .init();
            _ = r3_sig.sig.asyncWait(&w);
        }
    }.body);
}

test "sim: refuses thread-pool work on a worker thread during a run" {
    try refusal("sim: refuses thread-pool work on a worker thread during a run", "sim: thread-pool work on a worker thread (set thread_pool.max_threads = 0) during a sim run", struct {
        fn body() !void {
            begin(1);
            const rt = try r3.rt_mod.Runtime.init(std.heap.page_allocator, .{ .executors = .exact(1) });
            _ = rt;
            @import("common.zig").blockInPlace(r3.noopRet, .{});
        }
    }.body);
}

// ---- Round 4 (A2, A3): entry checks on the remaining public mutators ----

const r4 = struct {
    const Waiter = @import("common.zig").Waiter;
    const AsyncWaitState = @import("common.zig").AsyncWaitState;
    const select_mod = @import("select.zig");
    const AnyTask = @import("task.zig").AnyTask;
    const Work = @import("ev/completion.zig").Work;

    /// A custom future that keeps the waiter zio gives it, as the Future
    /// protocol allows, so another thread can reach that waiter.
    const Stash = struct {
        stashed: ?*Waiter = null,
        pub const Result = void;
        pub fn asyncWait(self: *Stash, waiter: *Waiter) AsyncWaitState {
            self.stashed = waiter;
            return .queued;
        }
        pub fn asyncCancelWait(self: *Stash, waiter: *Waiter) bool {
            _ = waiter;
            self.stashed = null;
            return true;
        }
        pub fn getResult(self: *Stash) void {
            _ = self;
        }
    };

    var a: Stash = .{};
    var b: Stash = .{};
    var work: Work = undefined;

    fn waitDirect() void {
        _ = select_mod.wait(&a) catch {};
    }

    fn waitSelect() void {
        _ = select_mod.select(.{ .a = &a, .b = &b }) catch {};
    }

    fn untilStashed() !void {
        while (a.stashed == null) try r3.rt.yield();
    }

    fn noWork(_: *Work) void {}
};

test "sim: refuses Waiter.signal from another thread, on a direct waiter a custom future kept" {
    try refusal("sim: refuses Waiter.signal from another thread, on a direct waiter a custom future kept", "sim: Waiter.signal" ++ other_thread_msg, struct {
        fn body() !void {
            try r3.up();
            r3_task.handle = try r3.rt.spawn(r4.waitDirect, .{});
            try r4.untilStashed();
            try r3.other(struct {
                fn f() void {
                    r4.a.stashed.?.signal();
                }
            }.f);
        }
    }.body);
}

test "sim: refuses Waiter.signal from another thread, on a select waiter a custom future kept" {
    try refusal("sim: refuses Waiter.signal from another thread, on a select waiter a custom future kept", "sim: Waiter.signal" ++ other_thread_msg, struct {
        fn body() !void {
            try r3.up();
            r3_task.handle = try r3.rt.spawn(r4.waitSelect, .{});
            try r4.untilStashed();
            try r3.other(struct {
                fn f() void {
                    r4.a.stashed.?.signal();
                }
            }.f);
        }
    }.body);
}

test "sim: refuses Waiter.tryClaim from another thread, on a select waiter a custom future kept" {
    try refusal("sim: refuses Waiter.tryClaim from another thread, on a select waiter a custom future kept", "sim: Waiter.tryClaim" ++ other_thread_msg, struct {
        fn body() !void {
            try r3.up();
            r3_task.handle = try r3.rt.spawn(r4.waitSelect, .{});
            try r4.untilStashed();
            try r3.other(struct {
                fn f() void {
                    _ = r4.a.stashed.?.tryClaim();
                }
            }.f);
        }
    }.body);
}

test "sim: refuses AnyTask.setCanceled from another thread" {
    try refusal("sim: refuses AnyTask.setCanceled from another thread", "sim: AnyTask.setCanceled" ++ other_thread_msg, struct {
        fn body() !void {
            try r3.up();
            r3_task.handle = try r3.rt.spawn(r3.waitForever, .{});
            try r3.rt.yield();
            try r3.other(struct {
                fn f() void {
                    _ = r4.AnyTask.fromAwaitable(r3_task.handle.awaitable.?).setCanceled(.user);
                }
            }.f);
        }
    }.body);
}

test "sim: refuses ThreadPool.submit from another thread, inline pool" {
    try refusal("sim: refuses ThreadPool.submit from another thread, inline pool", "sim: ThreadPool.submit" ++ other_thread_msg, struct {
        fn body() !void {
            try r3.up();
            r4.work = r4.Work.init(r4.noWork, null);
            try r3.other(struct {
                fn f() void {
                    r3.rt.thread_pool.submit(&r4.work);
                }
            }.f);
        }
    }.body);
}

// ---- Round 4 (A4): io.zig, net.zig, spawn and DNS entry points ----

test "sim: refuses a task wake from another thread (AnyTask.wake)" {
    try refusal("sim: refuses a task wake from another thread (AnyTask.wake)", "sim: a task wake or spawn" ++ other_thread_msg, struct {
        fn body() !void {
            try r3.up();
            r3_task.handle = try r3.rt.spawn(r3.waitForever, .{});
            try r3.rt.yield();
            try r3.other(struct {
                fn f() void {
                    r4.AnyTask.fromAwaitable(r3_task.handle.awaitable.?).wake();
                }
            }.f);
        }
    }.body);
}

test "sim: refuses Socket.bind from another thread" {
    try refusal("sim: refuses Socket.bind from another thread", "sim: Socket.bind" ++ other_thread_msg, struct {
        fn body() !void {
            try r3.up();

            try r3.other(struct {
                fn f() void {
                    var sock: @import("net.zig").Socket = .{ .handle = 0, .address = undefined };
                    sock.bind(.{ .ip = @import("net.zig").IpAddress.parseIp4("127.0.0.1", 0) catch unreachable }) catch {};
                }
            }.f);
        }
    }.body);
}

test "sim: refuses Socket.connect from another thread" {
    try refusal("sim: refuses Socket.connect from another thread", "sim: Socket.connect" ++ other_thread_msg, struct {
        fn body() !void {
            try r3.up();

            try r3.other(struct {
                fn f() void {
                    var sock: @import("net.zig").Socket = .{ .handle = 0, .address = undefined };
                    sock.connect(.{ .ip = @import("net.zig").IpAddress.parseIp4("127.0.0.1", 9) catch unreachable }, .{}) catch {};
                }
            }.f);
        }
    }.body);
}

test "sim: refuses a boolean socket option from another thread (Socket.setReuseAddress)" {
    try refusal("sim: refuses a boolean socket option from another thread (Socket.setReuseAddress)", "sim: Socket option (setBoolOption)" ++ other_thread_msg, struct {
        fn body() !void {
            try r3.up();

            try r3.other(struct {
                fn f() void {
                    const sock: @import("net.zig").Socket = .{ .handle = 0, .address = undefined };
                    sock.setReuseAddress(true) catch {};
                }
            }.f);
        }
    }.body);
}

test "sim: refuses an integer socket option from another thread (Socket.setSendBufferSize)" {
    try refusal("sim: refuses an integer socket option from another thread (Socket.setSendBufferSize)", "sim: Socket option (setIntOption)" ++ other_thread_msg, struct {
        fn body() !void {
            try r3.up();

            try r3.other(struct {
                fn f() void {
                    const sock: @import("net.zig").Socket = .{ .handle = 0, .address = undefined };
                    sock.setSendBufferSize(4096) catch {};
                }
            }.f);
        }
    }.body);
}

test "sim: refuses Socket.setNoDelay from another thread" {
    try refusal("sim: refuses Socket.setNoDelay from another thread", "sim: Socket.setNoDelay" ++ other_thread_msg, struct {
        fn body() !void {
            try r3.up();

            try r3.other(struct {
                fn f() void {
                    const sock: @import("net.zig").Socket = .{ .handle = 0, .address = undefined };
                    sock.setNoDelay(true) catch {};
                }
            }.f);
        }
    }.body);
}

test "sim: refuses std.Io fileSeekBy from another thread" {
    try refusal("sim: refuses std.Io fileSeekBy from another thread", "sim: std.Io fileSeekBy" ++ other_thread_msg, struct {
        fn body() !void {
            try r3.up();

            try r3.other(struct {
                fn f() void {
                    const io = r3.rt.io();
                    const file: std.Io.File = .{ .handle = 0, .flags = .{ .nonblocking = false } };
                    io.vtable.fileSeekBy(io.userdata, file, 1) catch {};
                }
            }.f);
        }
    }.body);
}

test "sim: refuses std.Io fileSeekTo from another thread" {
    try refusal("sim: refuses std.Io fileSeekTo from another thread", "sim: std.Io fileSeekTo" ++ other_thread_msg, struct {
        fn body() !void {
            try r3.up();

            try r3.other(struct {
                fn f() void {
                    const io = r3.rt.io();
                    const file: std.Io.File = .{ .handle = 0, .flags = .{ .nonblocking = false } };
                    io.vtable.fileSeekTo(io.userdata, file, 0) catch {};
                }
            }.f);
        }
    }.body);
}

test "sim: refuses std.Io fileTryLock from another thread" {
    try refusal("sim: refuses std.Io fileTryLock from another thread", "sim: std.Io fileTryLock" ++ other_thread_msg, struct {
        fn body() !void {
            try r3.up();

            try r3.other(struct {
                fn f() void {
                    const io = r3.rt.io();
                    const file: std.Io.File = .{ .handle = 0, .flags = .{ .nonblocking = false } };
                    _ = io.vtable.fileTryLock(io.userdata, file, .shared) catch {};
                }
            }.f);
        }
    }.body);
}

test "sim: refuses std.Io fileUnlock from another thread" {
    try refusal("sim: refuses std.Io fileUnlock from another thread", "sim: std.Io fileUnlock" ++ other_thread_msg, struct {
        fn body() !void {
            try r3.up();

            try r3.other(struct {
                fn f() void {
                    const io = r3.rt.io();
                    const file: std.Io.File = .{ .handle = 0, .flags = .{ .nonblocking = false } };
                    io.vtable.fileUnlock(io.userdata, file);
                }
            }.f);
        }
    }.body);
}

test "sim: refuses std.Io fileDowngradeLock from another thread" {
    try refusal("sim: refuses std.Io fileDowngradeLock from another thread", "sim: std.Io fileDowngradeLock" ++ other_thread_msg, struct {
        fn body() !void {
            try r3.up();

            try r3.other(struct {
                fn f() void {
                    const io = r3.rt.io();
                    const file: std.Io.File = .{ .handle = 0, .flags = .{ .nonblocking = false } };
                    io.vtable.fileDowngradeLock(io.userdata, file) catch {};
                }
            }.f);
        }
    }.body);
}

test "sim: refuses std.Io fileEnableAnsiEscapeCodes from another thread" {
    try refusal("sim: refuses std.Io fileEnableAnsiEscapeCodes from another thread", "sim: std.Io fileEnableAnsiEscapeCodes" ++ other_thread_msg, struct {
        fn body() !void {
            try r3.up();

            try r3.other(struct {
                fn f() void {
                    const io = r3.rt.io();
                    const file: std.Io.File = .{ .handle = 0, .flags = .{ .nonblocking = false } };
                    io.vtable.fileEnableAnsiEscapeCodes(io.userdata, file) catch {};
                }
            }.f);
        }
    }.body);
}

test "sim: refuses std.Io processSetCurrentDir from another thread" {
    try refusal("sim: refuses std.Io processSetCurrentDir from another thread", "sim: std.Io processSetCurrentDir" ++ other_thread_msg, struct {
        fn body() !void {
            try r3.up();

            try r3.other(struct {
                fn f() void {
                    const io = r3.rt.io();
                    io.vtable.processSetCurrentDir(io.userdata, .{ .handle = std.posix.AT.FDCWD }) catch {};
                }
            }.f);
        }
    }.body);
}

test "sim: refuses std.Io processSetCurrentPath from another thread" {
    try refusal("sim: refuses std.Io processSetCurrentPath from another thread", "sim: std.Io processSetCurrentPath" ++ other_thread_msg, struct {
        fn body() !void {
            try r3.up();

            try r3.other(struct {
                fn f() void {
                    const io = r3.rt.io();
                    io.vtable.processSetCurrentPath(io.userdata, ".") catch {};
                }
            }.f);
        }
    }.body);
}

test "sim: refuses std.Io processReplace from another thread" {
    try refusal("sim: refuses std.Io processReplace from another thread", "sim: std.Io processReplace" ++ other_thread_msg, struct {
        fn body() !void {
            try r3.up();

            try r3.other(struct {
                fn f() void {
                    const io = r3.rt.io();
                    io.vtable.processReplace(io.userdata, .{ .argv = &.{"/nonexistent-zio-sim-probe"} }) catch {};
                }
            }.f);
        }
    }.body);
}

test "sim: refuses std.Io processSpawn from another thread" {
    try refusal("sim: refuses std.Io processSpawn from another thread", "sim: std.Io processSpawn" ++ other_thread_msg, struct {
        fn body() !void {
            try r3.up();

            try r3.other(struct {
                fn f() void {
                    const io = r3.rt.io();
                    _ = io.vtable.processSpawn(io.userdata, .{ .argv = &.{"/nonexistent-zio-sim-probe"} }) catch {};
                }
            }.f);
        }
    }.body);
}

test "sim: refuses std.Io childKill from another thread" {
    try refusal("sim: refuses std.Io childKill from another thread", "sim: std.Io childKill" ++ other_thread_msg, struct {
        fn body() !void {
            try r3.up();

            try r3.other(struct {
                fn f() void {
                    const io = r3.rt.io();
                    var child: std.process.Child = .{ .id = null, .thread_handle = {}, .stdin = null, .stdout = null, .stderr = null, .request_resource_usage_statistics = false };
                    io.vtable.childKill(io.userdata, &child);
                }
            }.f);
        }
    }.body);
}

test "sim: refuses std.Io inheritParentDir from another thread" {
    try refusal("sim: refuses std.Io inheritParentDir from another thread", "sim: std.Io inheritParentDir" ++ other_thread_msg, struct {
        fn body() !void {
            try r3.up();

            try r3.other(struct {
                fn f() void {
                    const io = r3.rt.io();
                    _ = io.vtable.inheritParentDir(io.userdata, std.posix.AT.FDCWD) catch {};
                }
            }.f);
        }
    }.body);
}

test "sim: refuses std.Io inheritParentFile from another thread" {
    try refusal("sim: refuses std.Io inheritParentFile from another thread", "sim: std.Io inheritParentFile" ++ other_thread_msg, struct {
        fn body() !void {
            try r3.up();

            try r3.other(struct {
                fn f() void {
                    const io = r3.rt.io();
                    _ = io.vtable.inheritParentFile(io.userdata, 0, .{ .nonblocking = false }) catch {};
                }
            }.f);
        }
    }.body);
}

test "sim: refuses std.Io netSocketCreatePair from another thread" {
    try refusal("sim: refuses std.Io netSocketCreatePair from another thread", "sim: std.Io netSocketCreatePair" ++ other_thread_msg, struct {
        fn body() !void {
            try r3.up();

            try r3.other(struct {
                fn f() void {
                    const io = r3.rt.io();
                    _ = io.vtable.netSocketCreatePair(io.userdata, .{}) catch {};
                }
            }.f);
        }
    }.body);
}

test "sim: refuses std.Io batchAwaitConcurrent from another thread" {
    try refusal("sim: refuses std.Io batchAwaitConcurrent from another thread", "sim: std.Io batchAwaitConcurrent" ++ other_thread_msg, struct {
        fn body() !void {
            try r3.up();

            try r3.other(struct {
                fn f() void {
                    const io = r3.rt.io();
                    var storage: [1]std.Io.Operation.Storage = undefined;
                    var batch: std.Io.Batch = .init(&storage);
                    io.vtable.batchAwaitConcurrent(io.userdata, &batch, .none) catch {};
                }
            }.f);
        }
    }.body);
}

test "sim: refuses std.Io batchCancel from another thread" {
    try refusal("sim: refuses std.Io batchCancel from another thread", "sim: std.Io batchCancel" ++ other_thread_msg, struct {
        fn body() !void {
            try r3.up();

            try r3.other(struct {
                fn f() void {
                    const io = r3.rt.io();
                    var storage: [1]std.Io.Operation.Storage = undefined;
                    var batch: std.Io.Batch = .init(&storage);
                    io.vtable.batchCancel(io.userdata, &batch);
                }
            }.f);
        }
    }.body);
}

test "sim: refuses a DNS lookup during a run" {
    try refusal("sim: refuses a DNS lookup during a run", "sim: a DNS lookup during a sim run", struct {
        fn body() !void {
            try r3.up();
            var storage: [4]@import("dns/root.zig").LookupResult = undefined;
            const host: @import("net.zig").HostName = .{ .bytes = "localhost" };
            _ = host.lookup(&storage, .{ .port = 80 }) catch {};
        }
    }.body);
}

// ---- Phase 1 (design rev 3, sections 3.3 and 7): fd namespace, entropy ----

const p1 = struct {
    const random_mod = @import("random.zig");

    /// The sim fd base these tests move to (`testMoveFdBase`), and the soft
    /// limit they raise to above it. Both sit under 10240, the hard limit
    /// every `zig build test` child has on Darwin, so the kernel can really
    /// put a descriptor in the moved range and the checks have something to
    /// refuse. Nothing here skips: a hard limit too low to set this up fails
    /// the test (`error.HardLimitTooLow`).
    const moved_base: i32 = 1000;
    const raised_soft: std.posix.rlim_t = 2000;

    fn moveBase() !void {
        const rl = try std.posix.getrlimit(.NOFILE);
        if (rl.max < raised_soft) {
            std.debug.print("hard RLIMIT_NOFILE {d} is below {d}: cannot set up an fd-namespace test\n", .{ rl.max, raised_soft });
            return error.HardLimitTooLow;
        }
        testMoveFdBase(moved_base);
    }

    /// Raises this child's soft fd limit above the moved base, the state a
    /// process is often in at the real base (the shell this was written in
    /// had a soft limit of 1048576, above 100000).
    fn raiseSoftLimit() !void {
        const rl = try std.posix.getrlimit(.NOFILE);
        try std.posix.setrlimit(.NOFILE, .{ .cur = raised_soft, .max = rl.max });
    }
};

test "sim: fd namespace: begin lowers the soft fd limit to the sim base, a real descriptor cannot take a sim number, and end restores the limit" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    if (!isDeathChild()) return expectChildSucceeds("sim: fd namespace: begin lowers the soft fd limit to the sim base, a real descriptor cannot take a sim number, and end restores the limit");
    noCoreDump();
    try p1.moveBase();
    try p1.raiseSoftLimit();
    // The control: before the run the kernel does put a descriptor at a sim
    // number, so the refusals below are the limit's doing.
    try std.testing.expectEqual(@as(c_int, p1.moved_base + 7), std.c.dup2(0, p1.moved_base + 7));
    _ = std.c.close(p1.moved_base + 7);
    begin(1);
    try std.testing.expectEqual(@as(std.posix.rlim_t, @intCast(p1.moved_base)), (try std.posix.getrlimit(.NOFILE)).cur);
    try std.testing.expectEqual(@as(c_int, -1), std.c.dup2(0, p1.moved_base));
    try std.testing.expectEqual(@as(c_int, -1), std.c.dup2(0, p1.moved_base + 5));
    end();
    try std.testing.expectEqual(p1.raised_soft, (try std.posix.getrlimit(.NOFILE)).cur);
    childDone("sim: fd namespace: begin lowers the soft fd limit to the sim base, a real descriptor cannot take a sim number, and end restores the limit");
}

test "sim: fd namespace: begin refuses when a real descriptor holds a sim fd number" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: fd namespace: begin refuses when a real descriptor holds a sim fd number", "sim: fd namespace: real descriptor 1003 is open in the sim fd range", struct {
        fn body() !void {
            try p1.moveBase();
            try p1.raiseSoftLimit();
            if (std.c.dup2(0, p1.moved_base + 3) != p1.moved_base + 3) return error.DupFailed;
            begin(1);
        }
    }.body);
}

test "sim: fd namespace: a sim fd made after the soft limit was raised again is refused" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: fd namespace: a sim fd made after the soft limit was raised again is refused", "sim: fd namespace: the soft RLIMIT_NOFILE is 2000, above the sim fd base 1000", struct {
        fn body() !void {
            try p1.moveBase();
            begin(1);
            try p1.raiseSoftLimit();
            _ = pipePair();
        }
    }.body);
}

// At the real base and a hard limit below it, which is every `zig build
// test` child on Darwin, there is nothing to lower: `begin` must leave the
// soft limit as it was, and `end` must too. Also pins the real base.
test "sim: fd namespace: at the real base with the hard limit below it, begin and end leave the soft limit alone" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    if (!isDeathChild()) return expectChildSucceeds("sim: fd namespace: at the real base with the hard limit below it, begin and end leave the soft limit alone");
    noCoreDump();
    try std.testing.expectEqual(@as(i32, 100000), fd_base);
    const rl = try std.posix.getrlimit(.NOFILE);
    if (rl.max >= fd_base) {
        // From a shell with a high hard limit: lower it in this child, which
        // is the state this test is about.
        try std.posix.setrlimit(.NOFILE, .{ .cur = @min(rl.cur, 4096), .max = 4096 });
    }
    const before = (try std.posix.getrlimit(.NOFILE)).cur;
    begin(1);
    try std.testing.expectEqual(before, (try std.posix.getrlimit(.NOFILE)).cur);
    _ = pipePair();
    end();
    try std.testing.expectEqual(before, (try std.posix.getrlimit(.NOFILE)).cur);
    childDone("sim: fd namespace: at the real base with the hard limit below it, begin and end leave the soft limit alone");
}

test "sim: refuses zio.randomSecure during a run" {
    try refusal("sim: refuses zio.randomSecure during a run", "sim: zio.randomSecure (real entropy) during a sim run", struct {
        fn body() !void {
            try r3.up();
            var buf: [8]u8 = undefined;
            p1.random_mod.randomSecure(&buf) catch {};
        }
    }.body);
}

test "sim: refuses std.Io randomSecure during a run" {
    try refusal("sim: refuses std.Io randomSecure during a run", "sim: zio.randomSecure (real entropy) during a sim run", struct {
        fn body() !void {
            try r3.up();
            const io = r3.rt.io();
            var buf: [8]u8 = undefined;
            io.vtable.randomSecure(io.userdata, &buf) catch {};
        }
    }.body);
}

test "sim: refuses zio.random off an executor during a run" {
    try refusal("sim: refuses zio.random off an executor during a run", "sim: zio.random off an executor (real entropy) during a sim run", struct {
        fn body() !void {
            begin(1);
            var buf: [8]u8 = undefined;
            p1.random_mod.random(&buf);
        }
    }.body);
}

// ---- t-4092: a task's trace id is the task's, not its address's ----

const t4092 = struct {
    const rt_mod = @import("runtime.zig");
    const Allocator = std.mem.Allocator;
    const Alignment = std.mem.Alignment;

    /// Larger than a task-pool item, so each task comes from the runtime's
    /// allocator (as in starh2's dst-zio spawn fixtures).
    const Big = struct { pad: [640]u8 = @splat(0) };
    fn quick(b: Big) void {
        std.mem.doNotOptimizeAway(b.pad[0]);
    }
    const spawned = 8;

    const Out = struct { trace: u64, seen: u32 };

    /// Spawns `spawned` tasks one after the other, each freed before the next
    /// is made, so an allocator that reuses memory puts the next at the
    /// freed one's address.
    fn workload(alloc: Allocator, seed_value: u64) !Out {
        begin(seed_value);
        defer end();
        const rt = try rt_mod.Runtime.init(alloc, .{
            .executors = .exact(1),
            .thread_pool = .{ .max_threads = 0 },
            .stack_pool = .{ .maximum_size = 8 * 1024 * 1024, .committed_size = 256 * 1024, .shrink_interval = .zero },
        });
        defer rt.deinit();
        var i: usize = 0;
        while (i < spawned) : (i += 1) {
            var h = try rt.spawn(quick, .{.{}});
            h.join();
        }
        return .{ .trace = traceHash(), .seen = tasksSeen() };
    }

    /// Never hands back a freed address (frees are dropped; a test leak).
    const NeverReuse = struct {
        fn alloc(_: *anyopaque, len: usize, a: Alignment, ra: usize) ?[*]u8 {
            return std.heap.page_allocator.rawAlloc(len, a, ra);
        }
        fn resize(_: *anyopaque, _: []u8, _: Alignment, _: usize, _: usize) bool {
            return false;
        }
        fn remap(_: *anyopaque, _: []u8, _: Alignment, _: usize, _: usize) ?[*]u8 {
            return null;
        }
        fn free(_: *anyopaque, _: []u8, _: Alignment, _: usize) void {}
        const vtable: Allocator.VTable = .{ .alloc = alloc, .resize = resize, .remap = remap, .free = free };
        fn allocator() Allocator {
            return .{ .ptr = undefined, .vtable = &vtable };
        }
    };

    /// Always hands back the last freed block when the size and alignment
    /// match, the way a pool or a kernel that reuses a mapping does.
    const ReuseLast = struct {
        var cached: ?[]u8 = null;
        var cached_align: Alignment = .@"1";
        fn alloc(_: *anyopaque, len: usize, a: Alignment, ra: usize) ?[*]u8 {
            if (cached) |c| {
                if (c.len == len and cached_align == a) {
                    cached = null;
                    return c.ptr;
                }
            }
            return std.heap.page_allocator.rawAlloc(len, a, ra);
        }
        fn resize(_: *anyopaque, _: []u8, _: Alignment, _: usize, _: usize) bool {
            return false;
        }
        fn remap(_: *anyopaque, _: []u8, _: Alignment, _: usize, _: usize) ?[*]u8 {
            return null;
        }
        fn free(_: *anyopaque, m: []u8, a: Alignment, ra: usize) void {
            if (cached) |c| std.heap.page_allocator.rawFree(c, cached_align, ra);
            cached = m;
            cached_align = a;
        }
        const vtable: Allocator.VTable = .{ .alloc = alloc, .resize = resize, .remap = remap, .free = free };
        fn allocator() Allocator {
            return .{ .ptr = undefined, .vtable = &vtable };
        }
    };
};

test "sim: a task's trace id does not depend on whether the allocator reuses freed task memory" {
    if (comptime !compiled_in) return error.SkipZigTest;
    const fresh = try t4092.workload(t4092.NeverReuse.allocator(), 7);
    const reused = try t4092.workload(t4092.ReuseLast.allocator(), 7);
    // Every spawned task is a distinct task, plus at most the main task.
    try std.testing.expect(fresh.seen >= t4092.spawned and fresh.seen <= t4092.spawned + 1);
    try std.testing.expectEqual(fresh.seen, reused.seen);
    try std.testing.expectEqual(fresh.trace, reused.trace);
}

test "sim: twenty fresh processes with the heap moved give one trace for a workload that frees and spawns tasks" {
    if (comptime !compiled_in) return error.SkipZigTest;
    if (isDeathChild()) {
        noCoreDump();
        // The heap perturbation of design rev 3, section 7: each child first
        // maps a different number of pages, so later addresses differ.
        const shift = std.fmt.parseInt(usize, std.mem.span(std.c.getenv("ZIO_SIM_HEAP_SHIFT") orelse "0"), 10) catch 0;
        if (shift > 0) _ = try std.heap.page_allocator.alloc(u8, shift * 4096 + 7);
        const out = try t4092.workload(std.heap.page_allocator, 7);
        // Each spawned task is its own task, plus the main task. Equal
        // traces alone are not enough: before t-4092's fix every child here
        // gave the same trace with the 8 tasks merged under one id.
        if (out.seen != t4092.spawned + 1) {
            std.debug.print("seen {d} tasks, wanted {d}\n", .{ out.seen, t4092.spawned + 1 });
            return error.TasksMerged;
        }
        var line_buf: [64]u8 = undefined;
        const line = try std.fmt.bufPrint(&line_buf, "sim-trace: {x:0>16} seen={d}\n", .{ out.trace, out.seen });
        _ = std.c.write(2, line.ptr, line.len);
        childDone("sim: twenty fresh processes with the heap moved give one trace for a workload that frees and spawns tasks");
        return;
    }
    const io = std.testing.io;
    const gpa = std.testing.allocator;
    var exe_buf: [std.fs.max_path_bytes]u8 = undefined;
    const exe_len = try std.process.executablePath(io, &exe_buf);
    var first: ?[]const u8 = null;
    var first_buf: [64]u8 = undefined;
    var k: usize = 1;
    while (k <= 20) : (k += 1) {
        var env: std.process.Environ.Map = .init(gpa);
        defer env.deinit();
        if (std.c.getenv("PATH")) |path| try env.put("PATH", std.mem.span(path));
        try env.put("TEST_FILTER", "sim: twenty fresh processes with the heap moved");
        try env.put(death_child_env, "1");
        var shift_buf: [16]u8 = undefined;
        try env.put("ZIO_SIM_HEAP_SHIFT", try std.fmt.bufPrint(&shift_buf, "{d}", .{k * 13}));
        const res = try std.process.run(gpa, io, .{
            .argv = &.{exe_buf[0..exe_len]},
            .environ_map = &env,
            .timeout = .{ .duration = .{ .raw = .fromSeconds(30), .clock = .awake } },
        });
        defer gpa.free(res.stdout);
        defer gpa.free(res.stderr);
        try checkDoneMarkers("sim: twenty fresh processes with the heap moved give one trace for a workload that frees and spawns tasks", res.stderr);
        const at = std.mem.indexOf(u8, res.stderr, "sim-trace: ") orelse return error.NoTrace;
        const end_at = std.mem.indexOfScalarPos(u8, res.stderr, at, '\n') orelse return error.NoTrace;
        const got = res.stderr[at..end_at];
        if (first) |f| {
            if (!std.mem.eql(u8, f, got)) {
                std.debug.print("child {d}: {s}, child 1: {s}\n", .{ k, got, f });
                return error.TraceDiffers;
            }
        } else {
            @memcpy(first_buf[0..got.len], got);
            first = first_buf[0..got.len];
        }
    }
    std.debug.print("20 children: {s}\n", .{first.?});
}

// ---- Phase 1: datagram sockets through the socket layer ----

const dg = struct {
    const rt_mod = @import("runtime.zig");
    const os_net = @import("os/net.zig");
    const Duration = @import("time.zig").Duration;

    /// Runs `body` as the one task of a fresh sim run with seed `seed_value`.
    fn run(seed_value: u64, comptime body: fn () anyerror!void) !void {
        begin(seed_value);
        defer end();
        var o: rt_mod.RuntimeOptions = .{
            .executors = .exact(1),
            .thread_pool = .{ .max_threads = 0 },
        };
        o.stack_pool.shrink_interval = .zero;
        const rt = try rt_mod.Runtime.init(std.heap.page_allocator, o);
        defer rt.deinit();
        runtime = rt;
        var h = try rt.spawn(body, .{});
        try h.join();
    }

    /// The runtime of the run in progress, for a body that spawns tasks.
    var runtime: *rt_mod.Runtime = undefined;

    const ev = @import("ev/root.zig");
    const io_common = @import("common.zig");

    fn setInt(fd: i32, level: i32, name: u32, v: c_int) !void {
        try os_net.setsockopt(fd, level, name, std.mem.asBytes(&v));
    }

    fn getInt(fd: i32, level: i32, name: u32) !c_int {
        var v: c_int = -1;
        try os_net.getsockopt(fd, level, name, std.mem.asBytes(&v));
        return v;
    }

    fn sendN(fd: i32, to: os_net.sockaddr.in, n: usize, payload: []const u8) !void {
        var k: usize = 0;
        while (k < n) : (k += 1) _ = try sendFrom(fd, to, null, payload);
    }

    /// zio's own receive (`net.Socket.receiveFrom`'s ev op), on a sim fd.
    fn evRecvFrom(fd: i32, buf: []u8) !struct { len: usize, from: Ip4 } {
        var iov: [1]os_net.iovec = undefined;
        var from: os_net.sockaddr.in = undefined;
        var from_len: os_net.socklen_t = @sizeOf(os_net.sockaddr.in);
        var op = ev.NetRecvFrom.init(fd, .fromSlice(buf, &iov), .{}, @ptrCast(&from), &from_len);
        try io_common.waitForIo(&op.c);
        const n = try op.getResult();
        return .{ .len = n, .from = .{ .ip = @bitCast(from.addr), .port = std.mem.bigToNative(u16, from.port) } };
    }

    /// zio's own send of two buffers as one datagram (`NetSendTo`).
    fn evSendTo2(fd: i32, to: os_net.sockaddr.in, a: []const u8, b: []const u8) !usize {
        var iov = [2]os_net.iovec_const{ os_net.iovecConstFromSlice(a), os_net.iovecConstFromSlice(b) };
        var op = ev.NetSendTo.init(fd, .{ .iovecs = &iov }, .{}, @ptrCast(&to), @sizeOf(os_net.sockaddr.in));
        try io_common.waitForIo(&op.c);
        return op.getResult();
    }

    fn evPoll(fd: i32, event: ev.NetPoll.Event) !void {
        var op = ev.NetPoll.init(fd, event);
        try io_common.waitForIo(&op.c);
        try op.getResult();
    }

    fn evOpen() !i32 {
        var op = ev.NetOpen.init(.ipv4, .dgram, .ip, .{ .nonblocking = true });
        try io_common.waitForIo(&op.c);
        return op.getResult();
    }

    fn evBind(fd: i32, a: os_net.sockaddr.in) !Ip4 {
        var sa = a;
        var len: os_net.socklen_t = @sizeOf(os_net.sockaddr.in);
        var op = ev.NetBind.init(fd, @ptrCast(&sa), &len);
        try io_common.waitForIo(&op.c);
        try op.getResult();
        return .{ .ip = @bitCast(sa.addr), .port = std.mem.bigToNative(u16, sa.port) };
    }

    fn evClose(fd: i32) void {
        var op = ev.NetClose.init(fd);
        io_common.waitForIoUncancelable(&op.c);
        op.getResult() catch {};
    }

    /// `poll(fd, events, 0)` through the socket layer: the revents.
    fn pollNow(fd: i32, events: i16) !i16 {
        var fds = [1]os_net.pollfd{.{ .fd = fd, .events = events, .revents = 0 }};
        _ = try os_net.poll(&fds, 0);
        return fds[0].revents;
    }

    fn open() !i32 {
        return os_net.socket(.ipv4, .dgram, .ip, .{ .nonblocking = true });
    }

    fn addr(a: u8, b: u8, c: u8, d: u8, port: u16) os_net.sockaddr.in {
        return .{ .port = std.mem.nativeToBig(u16, port), .addr = @bitCast([4]u8{ a, b, c, d }) };
    }

    fn bindTo(fd: i32, a: os_net.sockaddr.in) !void {
        try os_net.bind(fd, @ptrCast(&a), @sizeOf(os_net.sockaddr.in));
    }

    fn local(fd: i32) !Ip4 {
        var sa: os_net.sockaddr.in = undefined;
        var len: os_net.socklen_t = @sizeOf(os_net.sockaddr.in);
        try os_net.getsockname(fd, @ptrCast(&sa), &len);
        return .{ .ip = @bitCast(sa.addr), .port = std.mem.bigToNative(u16, sa.port) };
    }

    fn sendTo(fd: i32, to: os_net.sockaddr.in, parts: []const []const u8) !usize {
        var iov: [4]os_net.iovec_const = undefined;
        for (parts, 0..) |p, i| iov[i] = os_net.iovecConstFromSlice(p);
        return os_net.sendto(fd, iov[0..parts.len], .{}, @ptrCast(&to), @sizeOf(os_net.sockaddr.in));
    }

    fn sendFrom(fd: i32, to: os_net.sockaddr.in, from: ?[4]u8, data: []const u8) !usize {
        var iov = [1]os_net.iovec_const{os_net.iovecConstFromSlice(data)};
        return os_net.sendDatagram(fd, &iov, .{}, &to, from);
    }

    const Got = struct { info: os_net.DatagramInfo, from: Ip4 };

    fn recvInto(fd: i32, bufs: []const []u8) !Got {
        var iov: [4]os_net.iovec = undefined;
        for (bufs, 0..) |b, i| iov[i] = os_net.iovecFromSlice(b);
        var from: os_net.sockaddr.in = undefined;
        const info = try os_net.recvDatagram(fd, iov[0..bufs.len], .{ .dontwait = true }, &from);
        return .{ .info = info, .from = .{ .ip = @bitCast(from.addr), .port = std.mem.bigToNative(u16, from.port) } };
    }

    /// Lets the wire deliver: a sleep longer than `wire_latency_ns`.
    fn settle() !void {
        try rt_mod.sleep(.fromNanoseconds(2 * wire_latency_ns));
    }
};

test "sim: dgram: a datagram sent through the socket layer lands after the wire latency, whole, with its source" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    try dg.run(1, struct {
        fn body() !void {
            const rx = try dg.open();
            const tx = try dg.open();
            try dg.bindTo(rx, dg.addr(127, 0, 0, 1, 0));
            const rx_addr = try dg.local(rx);
            try std.testing.expectEqual(Ip4{ .ip = .{ 127, 0, 0, 1 }, .port = 49152 }, rx_addr);
            // Two buffers sent, one datagram.
            try std.testing.expectEqual(@as(usize, 5), try dg.sendTo(tx, dg.addr(127, 0, 0, 1, 49152), &.{ "ab", "cde" }));
            // The sender was bound on its first send: the wildcard address
            // and the next port, as the kernels do.
            try std.testing.expectEqual(Ip4{ .ip = .{ 0, 0, 0, 0 }, .port = 49153 }, try dg.local(tx));
            var b1: [3]u8 = undefined;
            var b2: [10]u8 = undefined;
            // Not landed yet after half the latency, though the loop has
            // polled and harvested since the send.
            try dg.rt_mod.sleep(.fromNanoseconds(wire_latency_ns / 2));
            try std.testing.expectError(error.WouldBlock, dg.recvInto(rx, &.{ &b1, &b2 }));
            try dg.settle();
            // One datagram read into two buffers.
            const got = try dg.recvInto(rx, &.{ &b1, &b2 });
            try std.testing.expectEqual(@as(usize, 5), got.info.len);
            try std.testing.expect(!got.info.truncated);
            try std.testing.expectEqualStrings("abc", &b1);
            try std.testing.expectEqualStrings("de", b2[0..2]);
            try std.testing.expectEqual(Ip4{ .ip = .{ 127, 0, 0, 1 }, .port = 49153 }, got.from);
            try std.testing.expectEqual(@as(?[4]u8, null), got.info.to);
            try std.testing.expectError(error.WouldBlock, dg.recvInto(rx, &.{&b2}));
            dg.os_net.close(rx);
            dg.os_net.close(tx);
        }
    }.body);
}

test "sim: dgram: port 0 gives 49152, 49153, 49154 in every run" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    const T = struct {
        var ports: [3]u16 = undefined;
        fn body() !void {
            for (&ports) |*p| {
                const fd = try dg.open();
                try dg.bindTo(fd, dg.addr(127, 0, 0, 1, 0));
                p.* = (try dg.local(fd)).port;
            }
        }
    };
    var seed_value: u64 = 1;
    while (seed_value <= 3) : (seed_value += 1) {
        T.ports = @splat(0);
        try dg.run(seed_value, T.body);
        try std.testing.expectEqual([3]u16{ 49152, 49153, 49154 }, T.ports);
    }
}

test "sim: dgram: a bind conflict is AddressInUse, for an equal address and across a wildcard" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    try dg.run(1, struct {
        fn body() !void {
            const a = try dg.open();
            const b = try dg.open();
            const c = try dg.open();
            try dg.bindTo(a, dg.addr(127, 0, 0, 1, 5000));
            try std.testing.expectError(error.AddressInUse, dg.bindTo(b, dg.addr(127, 0, 0, 1, 5000)));
            try std.testing.expectError(error.AddressInUse, dg.bindTo(b, dg.addr(0, 0, 0, 0, 5000)));
            // Another loopback address on the same port is free.
            try dg.bindTo(b, dg.addr(127, 0, 0, 2, 5000));
            try dg.bindTo(c, dg.addr(0, 0, 0, 0, 6000));
            const d = try dg.open();
            try std.testing.expectError(error.AddressInUse, dg.bindTo(d, dg.addr(127, 0, 0, 9, 6000)));
            // Closing frees the port.
            dg.os_net.close(a);
            try dg.bindTo(d, dg.addr(127, 0, 0, 1, 5000));
            // Port 0 skips a port that is taken.
            const e = try dg.open();
            try dg.bindTo(e, dg.addr(127, 0, 0, 1, 49152));
            const f = try dg.open();
            try dg.bindTo(f, dg.addr(127, 0, 0, 1, 0));
            try std.testing.expectEqual(@as(u16, 49153), (try dg.local(f)).port);
        }
    }.body);
}

test "sim: dgram: a wildcard receiver gets the destination address, a reply from it leaves from it, and a bad source is AddressUnavailable" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    try dg.run(1, struct {
        fn body() !void {
            const server = try dg.open();
            const client = try dg.open();
            try dg.bindTo(server, dg.addr(0, 0, 0, 0, 7000));
            try dg.os_net.enableDestinationAddress(server);
            try dg.bindTo(client, dg.addr(127, 0, 0, 1, 0));
            _ = try dg.sendFrom(client, dg.addr(127, 0, 0, 5, 7000), null, "ping");
            try dg.settle();
            var buf: [8]u8 = undefined;
            const got = try dg.recvInto(server, &.{&buf});
            try std.testing.expectEqual(@as(?[4]u8, .{ 127, 0, 0, 5 }), got.info.to);
            try std.testing.expectEqual(Ip4{ .ip = .{ 127, 0, 0, 1 }, .port = 49152 }, got.from);
            // Reply from the address the request was sent to.
            _ = try dg.sendFrom(server, dg.addr(127, 0, 0, 1, got.from.port), got.info.to, "pong");
            try dg.settle();
            const back = try dg.recvInto(client, &.{&buf});
            try std.testing.expectEqualStrings("pong", buf[0..back.info.len]);
            try std.testing.expectEqual(Ip4{ .ip = .{ 127, 0, 0, 5 }, .port = 7000 }, back.from);
            // Without a source address, a wildcard socket sends from 127.0.0.1.
            _ = try dg.sendFrom(server, dg.addr(127, 0, 0, 1, got.from.port), null, "x");
            try dg.settle();
            try std.testing.expectEqual(Ip4{ .ip = .{ 127, 0, 0, 1 }, .port = 7000 }, (try dg.recvInto(client, &.{&buf})).from);
            // A socket bound to 127.0.0.1 cannot send from 127.0.0.2, and a
            // wildcard one cannot send from outside 127/8.
            try std.testing.expectError(error.AddressUnavailable, dg.sendFrom(client, dg.addr(127, 0, 0, 1, 7000), .{ 127, 0, 0, 2 }, "x"));
            try std.testing.expectError(error.AddressUnavailable, dg.sendFrom(server, dg.addr(127, 0, 0, 1, 49152), .{ 10, 0, 0, 1 }, "x"));
            // Without enableDestinationAddress there is no destination.
            try dg.settle();
            _ = try dg.sendFrom(server, dg.addr(127, 0, 0, 1, 49152), null, "y");
            try dg.settle();
            try std.testing.expectEqual(@as(?[4]u8, null), (try dg.recvInto(client, &.{&buf})).info.to);
        }
    }.body);
}

test "sim: dgram: non-blocking state is stored and read back, an empty receive would block, an empty datagram arrives, and truncation consumes the datagram" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    try dg.run(1, struct {
        fn body() !void {
            const blocking = try dg.os_net.socket(.ipv4, .dgram, .udp, .{ .nonblocking = false });
            try std.testing.expect(!try dg.os_net.isNonBlocking(blocking));
            try dg.os_net.setNonBlocking(blocking, true);
            try std.testing.expect(try dg.os_net.isNonBlocking(blocking));
            try dg.os_net.setNonBlocking(blocking, false);
            try std.testing.expect(!try dg.os_net.isNonBlocking(blocking));
            const rx = try dg.open();
            try std.testing.expect(try dg.os_net.isNonBlocking(rx));
            const tx = try dg.open();
            try dg.bindTo(rx, dg.addr(127, 0, 0, 1, 0));
            var buf: [4]u8 = undefined;
            // Empty: WouldBlock, from the socket's mode alone (no dontwait).
            var iov = [1]dg.os_net.iovec{dg.os_net.iovecFromSlice(&buf)};
            var from: dg.os_net.sockaddr.in = undefined;
            try std.testing.expectError(error.WouldBlock, dg.os_net.recvDatagram(rx, &iov, .{}, &from));
            const to = dg.addr(127, 0, 0, 1, 49152);
            _ = try dg.sendFrom(tx, to, null, "");
            _ = try dg.sendFrom(tx, to, null, "0123456789");
            _ = try dg.sendFrom(tx, to, null, "next");
            try dg.settle();
            const empty = try dg.recvInto(rx, &.{&buf});
            try std.testing.expectEqual(@as(usize, 0), empty.info.len);
            try std.testing.expect(!empty.info.truncated);
            const trunc = try dg.os_net.recvDatagram(rx, &iov, .{ .trunc = true }, &from);
            try std.testing.expect(trunc.truncated);
            try std.testing.expectEqual(@as(usize, if (builtin.os.tag == .linux) 10 else 4), trunc.len);
            try std.testing.expectEqualStrings("0123", &buf);
            const next = try dg.recvInto(rx, &.{&buf});
            try std.testing.expectEqualStrings("next", buf[0..next.info.len]);
            // recvfrom and recvmsg report the same.
            _ = try dg.sendFrom(tx, to, null, "0123456789");
            _ = try dg.sendFrom(tx, to, null, "0123456789");
            try dg.settle();
            var flen: dg.os_net.socklen_t = @sizeOf(dg.os_net.sockaddr.in);
            try std.testing.expectEqual(@as(usize, 4), try dg.os_net.recvfrom(rx, &iov, .{}, @ptrCast(&from), &flen));
            const m = try dg.os_net.recvmsg(rx, &iov, .{}, null, null, null);
            try std.testing.expectEqual(@as(usize, 4), m.len);
            try std.testing.expect(m.flags & std.c.MSG.TRUNC != 0);
            try std.testing.expectError(error.WouldBlock, dg.recvInto(rx, &.{&buf}));
        }
    }.body);
}

test "sim: dgram: the default byte budget drops by bytes, counted, and a datagram to no socket is counted" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    try dg.run(1, struct {
        fn body() !void {
            const rx = try dg.open();
            const tx = try dg.open();
            try dg.bindTo(rx, dg.addr(127, 0, 0, 1, 0));
            // Each empty datagram costs `datagram_overhead` bytes: the budget
            // holds 832 of them, under the 1024-packet limit.
            const fits = default_rcvbuf_bytes / datagram_overhead;
            try std.testing.expectEqual(@as(usize, 832), fits);
            var k: usize = 0;
            while (k < fits + 3) : (k += 1) _ = try dg.sendFrom(tx, dg.addr(127, 0, 0, 1, 49152), null, "");
            _ = try dg.sendFrom(tx, dg.addr(127, 0, 0, 1, 40000), null, "nobody");
            try dg.settle();
            const st = datagramStats(rx);
            try std.testing.expectEqual(@as(u64, fits), st.received);
            try std.testing.expectEqual(@as(u64, 3), st.dropped_rcvbuf);
            try std.testing.expectEqual(@as(u64, fits + 4), datagramStats(tx).sent);
            try std.testing.expectEqual(@as(u64, 1), datagramsDroppedNoSocket());
            // Draining makes room again.
            var buf: [1]u8 = undefined;
            k = 0;
            while (k < fits) : (k += 1) _ = try dg.recvInto(rx, &.{&buf});
            try std.testing.expectError(error.WouldBlock, dg.recvInto(rx, &.{&buf}));
            _ = try dg.sendFrom(tx, dg.addr(127, 0, 0, 1, 49152), null, "again");
            try dg.settle();
            try std.testing.expectEqualStrings("a", (buf[0..(try dg.recvInto(rx, &.{&buf})).info.len]));
        }
    }.body);
}

const dg_order = struct {
    var log: [12]u8 = undefined;
    var log_len: usize = 0;

    /// Two senders each send three datagrams to one receiver at the same
    /// logical time; the receiver logs the payloads in arrival order.
    fn body() !void {
        const rx = try dg.open();
        const a = try dg.open();
        const b = try dg.open();
        try dg.bindTo(rx, dg.addr(127, 0, 0, 1, 0));
        const to = dg.addr(127, 0, 0, 1, 49152);
        for ([_][]const u8{ "a", "b", "c" }) |p| _ = try dg.sendFrom(a, to, null, p);
        for ([_][]const u8{ "1", "2", "3" }) |p| _ = try dg.sendFrom(b, to, null, p);
        try dg.settle();
        log_len = 0;
        var buf: [1]u8 = undefined;
        while (dg.recvInto(rx, &.{&buf})) |_| {
            log[log_len] = buf[0];
            log_len += 1;
        } else |err| if (err != error.WouldBlock) return err;
    }

    fn orderFor(seed_value: u64) ![6]u8 {
        try dg.run(seed_value, body);
        if (log_len != 6) return error.LostDatagram;
        return log[0..6].*;
    }

    fn inFlowOrder(o: [6]u8) bool {
        var letters: [3]u8 = undefined;
        var digits: [3]u8 = undefined;
        var nl: usize = 0;
        var nd: usize = 0;
        for (o) |ch| {
            if (ch >= 'a') {
                letters[nl] = ch;
                nl += 1;
            } else {
                digits[nd] = ch;
                nd += 1;
            }
        }
        return std.mem.eql(u8, &letters, "abc") and std.mem.eql(u8, &digits, "123");
    }
};

test "sim: dgram: each flow is first in, first out; flows due at the same time interleave by the seed, the same way for the same seed" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    var distinct: [20][6]u8 = undefined;
    var n_distinct: usize = 0;
    var seed_value: u64 = 1;
    while (seed_value <= 20) : (seed_value += 1) {
        const o = try dg_order.orderFor(seed_value);
        try std.testing.expect(dg_order.inFlowOrder(o));
        try std.testing.expectEqual(o, try dg_order.orderFor(seed_value));
        var seen = false;
        for (distinct[0..n_distinct]) |d| {
            if (std.mem.eql(u8, &d, &o)) seen = true;
        }
        if (!seen) {
            distinct[n_distinct] = o;
            n_distinct += 1;
        }
    }
    // The seeded order is not one fixed order: 20 seeds give several.
    try std.testing.expect(n_distinct >= 3);
}

// The outside review's sequence for finding 1 (review of 158ebba): an empty
// datagram left queued while the byte ring empties must not move where the
// next payload is written.
test "sim: dgram: an empty datagram queued while the ring empties does not make two later payloads share bytes" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    try dg.run(1, struct {
        fn body() !void {
            const rx = try dg.open();
            const tx = try dg.open();
            try dg.bindTo(rx, dg.addr(127, 0, 0, 1, 0));
            const to = dg.addr(127, 0, 0, 1, (try dg.local(rx)).port);
            var buf: [4]u8 = undefined;
            _ = try dg.sendFrom(tx, to, null, "A");
            _ = try dg.sendFrom(tx, to, null, "");
            try dg.settle();
            try std.testing.expectEqualStrings("A", buf[0..(try dg.recvInto(rx, &.{&buf})).info.len]);
            _ = try dg.sendFrom(tx, to, null, "B");
            try dg.settle();
            try std.testing.expectEqual(@as(usize, 0), (try dg.recvInto(rx, &.{&buf})).info.len);
            _ = try dg.sendFrom(tx, to, null, "C");
            try dg.settle();
            try std.testing.expectEqualStrings("B", buf[0..(try dg.recvInto(rx, &.{&buf})).info.len]);
            _ = try dg.sendFrom(tx, to, null, "D");
            try dg.settle();
            try std.testing.expectEqualStrings("C", buf[0..(try dg.recvInto(rx, &.{&buf})).info.len]);
            try std.testing.expectEqualStrings("D", buf[0..(try dg.recvInto(rx, &.{&buf})).info.len]);
        }
    }.body);
}

/// The reference for the datagram queue's model test: a list of byte strings
/// and the two limits, with the landing and receive rules of design section 6
/// written plainly. `wire` holds what was sent and has not landed yet, so the
/// limit that decides a landing is the one in force when it lands.
const QueueModel = struct {
    wire: std.ArrayList([]const u8) = .empty,
    queued: std.ArrayList([]const u8) = .empty,
    head: usize = 0,
    limit: usize = default_rcvbuf_bytes,
    charged: usize = 0,
    dropped: u64 = 0,
    /// Drops where the queue already held `rcv_packet_limit` datagrams.
    dropped_for_packets: u64 = 0,
    landed: u64 = 0,
    taken: u64 = 0,

    fn send(m: *QueueModel, gpa: std.mem.Allocator, bytes: []const u8) !void {
        try m.wire.append(gpa, bytes);
    }

    /// Lands everything on the wire, in send order: one flow.
    fn settle(m: *QueueModel, gpa: std.mem.Allocator) !void {
        for (m.wire.items) |bytes| try m.land(gpa, bytes);
        m.wire.clearRetainingCapacity();
    }

    fn land(m: *QueueModel, gpa: std.mem.Allocator, bytes: []const u8) !void {
        const cost = bytes.len + datagram_overhead;
        if (m.len() == rcv_packet_limit) {
            m.dropped += 1;
            m.dropped_for_packets += 1;
            return;
        }
        if (m.charged + cost > m.limit) {
            m.dropped += 1;
            return;
        }
        try m.queued.append(gpa, bytes);
        m.charged += cost;
        m.landed += 1;
    }

    fn len(m: *const QueueModel) usize {
        return m.queued.items.len - m.head;
    }

    fn take(m: *QueueModel) ?[]const u8 {
        if (m.head == m.queued.items.len) return null;
        const b = m.queued.items[m.head];
        m.head += 1;
        m.taken += 1;
        m.charged -= b.len + datagram_overhead;
        return b;
    }
};

const queue_model = struct {
    const seeds = [_]u64{ 1, 2, 3, 4, 5 };
    const steps = 1500;

    var model_seed: u64 = 0;
    var step: usize = 0;
    /// Receive buffers: static, a task's stack is small.
    var bufs: [3][8100]u8 = undefined;
    var c: struct {
        sent: u64 = 0,
        large: u64 = 0,
        empty: u64 = 0,
        truncated: u64 = 0,
        scatter: u64 = 0,
        resizes_in_flight: u64 = 0,
        dropped_for_bytes: u64 = 0,
        dropped_for_packets: u64 = 0,
        max_queued: usize = 0,
        meta_wraps: u64 = 0,
        tail: u64 = 0,
    } = .{};

    fn sendOne(tx: i32, to: dg.os_net.sockaddr.in, gpa: std.mem.Allocator, model: *QueueModel, len: usize, r: std.Random) !void {
        const bytes = try gpa.alloc(u8, len);
        r.bytes(bytes);
        _ = try dg.sendFrom(tx, to, null, bytes);
        try model.send(gpa, bytes);
        c.sent += 1;
        if (len == 0) c.empty += 1;
        if (len > 1500) c.large += 1;
    }

    /// One receive into one to three buffers whose total room is near the
    /// datagram's length, checked against the model's head.
    fn recvOne(rx: i32, model: *QueueModel, r: std.Random) !void {
        const want = model.take();
        const wlen = if (want) |w| w.len else 8;
        var room = r.uintAtMost(usize, wlen + 2);
        const n = r.intRangeAtMost(usize, 1, 3);
        var lens: [3]usize = .{ 0, 0, 0 };
        for (0..n) |k| {
            lens[k] = if (k == n - 1) room else r.uintAtMost(usize, room);
            room -= lens[k];
        }
        var dst: [3][]u8 = undefined;
        for (0..n) |k| {
            @memset(&bufs[k], 0xAA);
            dst[k] = bufs[k][0..lens[k]];
        }
        const w = want orelse {
            try std.testing.expectError(error.WouldBlock, dg.recvInto(rx, dst[0..n]));
            return;
        };
        var total: usize = 0;
        for (lens[0..n]) |l| total += l;
        const got = try dg.recvInto(rx, dst[0..n]);
        const copied = @min(total, w.len);
        try std.testing.expectEqual(copied, got.info.len);
        try std.testing.expectEqual(w.len > total, got.info.truncated);
        var at: usize = 0;
        for (0..n) |k| {
            const part = @min(lens[k], copied - at);
            try std.testing.expectEqualSlices(u8, w[at..][0..part], bufs[k][0..part]);
            // What did not fit leaves the rest of the buffer alone.
            for (bufs[k][part..lens[k]]) |b| try std.testing.expectEqual(@as(u8, 0xAA), b);
            at += part;
        }
        if (w.len > total) c.truncated += 1;
        if (n > 1 and copied > lens[0]) c.scatter += 1;
    }

    /// One to three datagrams: a quarter empty, one in twenty large.
    fn sendSome(tx: i32, to: dg.os_net.sockaddr.in, gpa: std.mem.Allocator, model: *QueueModel, r: std.Random) !void {
        for (0..r.intRangeAtMost(usize, 1, 3)) |_| {
            const len: usize = switch (r.uintLessThan(u8, 20)) {
                0...4 => 0,
                5 => r.intRangeAtMost(usize, 1501, 8000),
                else => r.intRangeAtMost(usize, 1, 700),
            };
            try sendOne(tx, to, gpa, model, len, r);
        }
    }

    fn setRcvbuf(rx: i32, model: *QueueModel, v: c_int) !void {
        if (model.wire.items.len > 0) c.resizes_in_flight += 1;
        try dg.setInt(rx, sys.SOL.SOCKET, sys.SO.RCVBUF, v);
        model.limit = @intCast(v);
    }

    fn check(rx: i32, model: *const QueueModel) !void {
        const st = datagramStats(rx);
        try std.testing.expectEqual(model.dropped, st.dropped_rcvbuf);
        try std.testing.expectEqual(model.landed, st.received);
        const readable = (try dg.pollNow(rx, dg.os_net.POLL.IN)) & dg.os_net.POLL.IN != 0;
        try std.testing.expectEqual(model.len() > 0, readable);
        c.max_queued = @max(c.max_queued, model.len());
    }

    fn body() !void {
        var arena: std.heap.ArenaAllocator = .init(std.heap.page_allocator);
        defer arena.deinit();
        const gpa = arena.allocator();
        var prng: std.Random.DefaultPrng = .init(model_seed);
        const r = prng.random();
        var model: QueueModel = .{};
        const rx = try dg.open();
        const tx = try dg.open();
        try dg.bindTo(rx, dg.addr(127, 0, 0, 1, 0));
        const to = dg.addr(127, 0, 0, 1, (try dg.local(rx)).port);
        step = 0;
        while (step < steps) : (step += 1) {
            // Blocks of 300 steps. Even blocks keep the queue short with
            // small byte limits: a send lands at once, so landings and
            // receives alternate one at a time and the ring wraps often.
            // Round 1's finding 1 needs that shape (the mix of the first
            // version of this test). Odd blocks fill the queue to the
            // packet limit, drain it in runs, and change SO_RCVBUF while
            // datagrams are on the wire.
            if ((step / 300) % 2 == 0) switch (r.uintLessThan(u8, 10)) {
                0...3 => {
                    try sendSome(tx, to, gpa, &model, r);
                    try dg.settle();
                    try model.settle(gpa);
                },
                4...7 => try recvOne(rx, &model, r),
                else => try setRcvbuf(rx, &model, r.intRangeAtMost(c_int, 300, 3000)),
            } else switch (r.uintLessThan(u8, 12)) {
                // Left on the wire until a settle step.
                0...2 => try sendSome(tx, to, gpa, &model, r),
                3 => {
                    try dg.settle();
                    try model.settle(gpa);
                },
                4...6 => try recvOne(rx, &model, r),
                // A large limit lets the packet limit, not the bytes, fill
                // the queue.
                7, 8 => try setRcvbuf(rx, &model, switch (r.uintLessThan(u8, 3)) {
                    0 => 400_000,
                    1 => @intCast(default_rcvbuf_bytes),
                    else => r.intRangeAtMost(c_int, 300, 3000),
                }),
                // A burst of tiny datagrams, landed at once.
                9 => {
                    for (0..r.intRangeAtMost(usize, 100, 700)) |_| try sendOne(tx, to, gpa, &model, r.uintAtMost(usize, 2), r);
                    try dg.settle();
                    try model.settle(gpa);
                },
                // A run of receives: it moves the metadata ring's head far.
                else => for (0..r.intRangeAtMost(usize, 20, 300)) |_| try recvOne(rx, &model, r),
            }
            try check(rx, &model);
        }
        // The tail: land what is left, receive all of it, then nothing.
        try dg.settle();
        try model.settle(gpa);
        try check(rx, &model);
        while (model.len() > 0) {
            const w = model.take().?;
            const got = try dg.recvInto(rx, &.{bufs[0][0..w.len]});
            try std.testing.expectEqual(w.len, got.info.len);
            try std.testing.expect(!got.info.truncated);
            try std.testing.expectEqualSlices(u8, w, bufs[0][0..w.len]);
            c.tail += 1;
        }
        try check(rx, &model);
        try std.testing.expectError(error.WouldBlock, dg.recvInto(rx, &.{bufs[0][0..8]}));
        c.dropped_for_packets += model.dropped_for_packets;
        c.dropped_for_bytes += model.dropped - model.dropped_for_packets;
        // The metadata ring holds `rcv_packet_limit` entries; every
        // receive moves its head one entry.
        c.meta_wraps += model.taken / rcv_packet_limit;
        dg.os_net.close(rx);
        dg.os_net.close(tx);
    }
};

// Random steps against `QueueModel`: sends of one to three datagrams (a
// quarter empty, one in twenty from 1501 to 8000 bytes) that stay on the wire
// until a settle step, so SO_RCVBUF can change between a send and its
// landing; receives into one to three buffers (scatter), with room near the
// datagram's length so some are truncated; bursts of 100 to 700 tiny
// datagrams, so with a large SO_RCVBUF the 1024-packet limit drops; runs of
// 20 to 300 receives, so the metadata ring wraps. After every step the drop
// and landing counts and readiness must equal the model's; at the end the
// whole tail is received and compared. A failure prints the seed and step.
test "sim: dgram: the receive queue matches a list of byte strings under random sends, scatter receives, truncation, SO_RCVBUF changes in flight, the packet limit and metadata wrap, and a drained tail" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    queue_model.c = .{};
    for (queue_model.seeds) |s| {
        queue_model.model_seed = s;
        dg.run(s, queue_model.body) catch |err| {
            std.debug.print("datagram queue model test: seed {d}, step {d}: {t}\n", .{ s, queue_model.step, err });
            return err;
        };
    }
    const c = queue_model.c;
    std.debug.print("datagram queue model test: {d} seeds x {d} steps: {d} sent ({d} empty, {d} over 1500 bytes), {d} truncated, {d} scattered, {d} SO_RCVBUF changes in flight, {d} dropped for bytes, {d} for packets, at most {d} queued, {d} metadata ring wraps, {d} in the tail\n", .{ queue_model.seeds.len, queue_model.steps, c.sent, c.empty, c.large, c.truncated, c.scatter, c.resizes_in_flight, c.dropped_for_bytes, c.dropped_for_packets, c.max_queued, c.meta_wraps, c.tail });
    // Each case this test is for must have happened, or it tested nothing.
    inline for (.{ "empty", "large", "truncated", "scatter", "resizes_in_flight", "dropped_for_bytes", "dropped_for_packets", "meta_wraps", "tail" }) |name| {
        if (@field(c, name) == 0) {
            std.debug.print("datagram queue model test: {s} is 0\n", .{name});
            return error.CaseNeverRan;
        }
    }
    try std.testing.expectEqual(@as(usize, rcv_packet_limit), c.max_queued);
}

/// What one direction of a sim pipe should hold: the bytes sent and not yet
/// received, in order, under `pipe_buf_cap`. It calls no helper of the code
/// under test (not `totalLen`): a helper both sides share can be wrong for
/// both in the same way, and the comparison then passes.
const PipeModel = struct {
    bytes: std.ArrayList(u8) = .empty,

    /// A send copies what fits, gathered in slice order. A full buffer parks
    /// a send of one byte or more; a send of nothing returns 0 at once.
    fn send(m: *PipeModel, gpa: std.mem.Allocator, src: []const []const u8) !?usize {
        var total: usize = 0;
        for (src) |sl| total += sl.len;
        if (total == 0) return 0;
        const space = pipe_buf_cap - m.bytes.items.len;
        if (space == 0) return null;
        const n = @min(total, space);
        var left = n;
        for (src) |sl| {
            const k = @min(sl.len, left);
            try m.bytes.appendSlice(gpa, sl[0..k]);
            left -= k;
        }
        return n;
    }

    /// A receive takes the oldest bytes, scattered in slice order.
    fn recv(m: *PipeModel, gpa: std.mem.Allocator, cap: usize) ![]const u8 {
        const n = @min(cap, m.bytes.items.len);
        const out = try gpa.dupe(u8, m.bytes.items[0..n]);
        try m.bytes.replaceRange(gpa, 0, n, &.{});
        return out;
    }
};

// One pipe, both directions, driven through the sim's own byte handling
// (`copySend`, `drainBuf`): sends of zero to five slices (a quarter of them
// empty) big enough to fill the 4096-byte buffer often, a send to a full
// buffer that parks and is cancelled, and receives into zero to five slices
// (a quarter of them empty). Parking, waking and harvest are the op-level
// model test's (`sim_op_model_test.zig`). Each end's buffer is compared with
// `PipeModel` after every step. A failure prints the seed and the step.
test "sim: pipe: the pipe buffer matches a list of bytes under random sends, receives, empty slices and a full buffer" {
    if (comptime !compiled_in) return error.SkipZigTest;
    var arena: std.heap.ArenaAllocator = .init(std.heap.page_allocator);
    defer arena.deinit();
    const gpa = arena.allocator();
    var parks: usize = 0;
    var empty_sends: usize = 0;
    var no_slice_sends: usize = 0;
    var wide: usize = 0;
    for ([_]u64{ 1, 2, 3, 4, 5 }) |model_seed| {
        begin(model_seed);
        defer end();
        var prng: std.Random.DefaultPrng = .init(model_seed);
        const r = prng.random();
        const fds = pipePair();
        var models: [2]PipeModel = .{ .{}, .{} };
        var dummy: u8 = 0;
        var step: usize = 0;
        errdefer std.debug.print("pipe model test: seed {d}, step {d}\n", .{ model_seed, step });
        while (step < 1000) : (step += 1) {
            // `from` sends into the buffer of the other end, `models[1 - from]`.
            const from = r.uintLessThan(usize, 2);
            if (r.boolean()) {
                var parts: [5][]u8 = undefined;
                const n = r.uintAtMost(usize, 5);
                var total: usize = 0;
                for (parts[0..n]) |*part| {
                    const len: usize = if (r.uintLessThan(u8, 4) == 0) 0 else r.intRangeAtMost(usize, 1, 1500);
                    part.* = try gpa.alloc(u8, len);
                    r.bytes(part.*);
                    total += len;
                }
                var src: [5][]const u8 = undefined;
                for (parts[0..n], 0..) |part, k| src[k] = part;
                if (total == 0) empty_sends += 1;
                if (n == 0) no_slice_sends += 1;
                if (n >= 4) wide += 1;
                const want = try models[1 - from].send(gpa, src[0..n]);
                switch (copySend(fds[from], src[0..n], &dummy, 0, false)) {
                    .due => |got| try std.testing.expectEqual(want, got),
                    .parked => {
                        try std.testing.expectEqual(@as(?usize, null), want);
                        parks += 1;
                        _ = cancelIo(&dummy);
                    },
                    else => |o| {
                        std.debug.print("send: {t}\n", .{o});
                        return error.UnexpectedSend;
                    },
                }
            } else {
                var bufs: [5][2000]u8 = undefined;
                var dst: [5][]u8 = undefined;
                const n = r.uintAtMost(usize, 5);
                if (n >= 4) wide += 1;
                var cap: usize = 0;
                for (dst[0..n], 0..) |*d, k| {
                    const len: usize = if (r.uintLessThan(u8, 4) == 0) 0 else r.intRangeAtMost(usize, 1, 2000);
                    d.* = bufs[k][0..len];
                    cap += len;
                }
                const want = try models[from].recv(gpa, cap);
                const got = drainBuf(fds[from], dst[0..n]);
                try std.testing.expectEqual(want.len, got);
                var at: usize = 0;
                for (dst[0..n]) |d| {
                    const k = @min(d.len, got - at);
                    try std.testing.expectEqualSlices(u8, want[at..][0..k], d[0..k]);
                    at += k;
                }
            }
            for (fds, 0..) |fd, k| {
                const e = ends[endIndex(fd).?];
                try std.testing.expectEqualSlices(u8, models[k].bytes.items, e.buf[0..e.buf_len]);
            }
        }
    }
    // The run did reach the cases it is for.
    std.debug.print("pipe model test: 5000 steps, {d} sends parked on a full buffer, {d} sends of nothing ({d} with no slice), {d} sends or receives of four or five slices\n", .{ parks, empty_sends, no_slice_sends, wide });
    try std.testing.expect(parks > 20);
    try std.testing.expect(empty_sends > 20);
    try std.testing.expect(no_slice_sends > 20);
    try std.testing.expect(wide > 20);
}

// The outside review's sequence for finding 2 (review of 158ebba).
test "sim: dgram ev: a receive parked on a socket that is closed and whose number is reused at once ends FileDescriptorNotASocket and leaves the new socket's datagram" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    try dg.run(1, struct {
        fn parkedRecv(fd: i32) !usize {
            var buf: [8]u8 = undefined;
            return (try dg.evRecvFrom(fd, &buf)).len;
        }
        fn body() !void {
            const rx = try dg.open();
            try dg.bindTo(rx, dg.addr(127, 0, 0, 1, 0));
            var h = try dg.runtime.spawn(parkedRecv, .{rx});
            // Let the receive park.
            try dg.rt_mod.sleep(.fromNanoseconds(1));
            dg.os_net.close(rx);
            const nx = try dg.open();
            try std.testing.expectEqual(rx, nx);
            try dg.bindTo(nx, dg.addr(127, 0, 0, 1, 0));
            const tx = try dg.open();
            _ = try dg.sendFrom(tx, dg.addr(127, 0, 0, 1, (try dg.local(nx)).port), null, "new");
            try dg.settle();
            try std.testing.expectError(error.FileDescriptorNotASocket, h.join());
            var buf: [8]u8 = undefined;
            try std.testing.expectEqualStrings("new", buf[0..(try dg.recvInto(nx, &.{&buf})).info.len]);
        }
    }.body);
}

test "sim: dgram: a closed fd number is reused by the next open, which does not see the old socket's data" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    try dg.run(1, struct {
        fn body() !void {
            const rx = try dg.open();
            const tx = try dg.open();
            try dg.bindTo(rx, dg.addr(127, 0, 0, 1, 0));
            _ = try dg.sendFrom(tx, dg.addr(127, 0, 0, 1, 49152), null, "old");
            try dg.settle();
            dg.os_net.close(rx);
            const again = try dg.open();
            try std.testing.expectEqual(rx, again);
            try std.testing.expectEqual(Ip4{ .ip = .{ 0, 0, 0, 0 }, .port = 0 }, try dg.local(again));
            try dg.bindTo(again, dg.addr(127, 0, 0, 1, 49152));
            var buf: [8]u8 = undefined;
            try std.testing.expectError(error.WouldBlock, dg.recvInto(again, &.{&buf}));
            try std.testing.expectEqual(@as(u64, 0), datagramStats(again).received);
        }
    }.body);
}

test "sim: dgram: end frees every socket and every datagram on the wire, so the next run starts empty" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    const T = struct {
        var first_fd: i32 = 0;
        fn leave() !void {
            const rx = try dg.open();
            const tx = try dg.open();
            first_fd = rx;
            try dg.bindTo(rx, dg.addr(127, 0, 0, 1, 0));
            _ = try dg.sendFrom(tx, dg.addr(127, 0, 0, 1, 49152), null, "queued");
            try dg.settle();
            // Still on the wire at end:
            _ = try dg.sendFrom(tx, dg.addr(127, 0, 0, 1, 49152), null, "in flight");
        }
        fn fresh() !void {
            const fd = try dg.open();
            try std.testing.expectEqual(first_fd, fd);
            try dg.bindTo(fd, dg.addr(127, 0, 0, 1, 0));
            try std.testing.expectEqual(@as(u16, 49152), (try dg.local(fd)).port);
            try dg.settle();
            var buf: [16]u8 = undefined;
            try std.testing.expectError(error.WouldBlock, dg.recvInto(fd, &.{&buf}));
        }
    };
    try dg.run(1, T.leave);
    try dg.run(1, T.fresh);
}

// ---- Phase 1 refusals: datagram sockets ----

const dg_thread = struct {
    var fd: i32 = 0;
    var buf: [4]u8 = undefined;
};

test "sim: dgram refuses: a blocking receive on a socket that is not non-blocking" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: dgram refuses: a blocking receive on a socket that is not non-blocking", "sim: a blocking receive on sim fd 100032, which is not non-blocking", struct {
        fn body() !void {
            try r3.up();
            const fd = try dg.os_net.socket(.ipv4, .dgram, .ip, .{ .nonblocking = false });
            var buf: [4]u8 = undefined;
            var iov = [1]dg.os_net.iovec{dg.os_net.iovecFromSlice(&buf)};
            _ = try dg.os_net.recvfrom(fd, &iov, .{}, null, null);
        }
    }.body);
}

test "sim: dgram refuses: sendto with MSG_OOB" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: dgram refuses: sendto with MSG_OOB", "sim: send flag oob on sim fd 100032 is not simulated", struct {
        fn body() !void {
            try r3.up();
            const fd = try dg.open();
            var iov = [1]dg.os_net.iovec_const{dg.os_net.iovecConstFromSlice("x")};
            const to = dg.addr(127, 0, 0, 1, 7000);
            _ = try dg.os_net.sendto(fd, &iov, .{ .oob = true }, @ptrCast(&to), @sizeOf(dg.os_net.sockaddr.in));
        }
    }.body);
}

test "sim: dgram refuses: sendto with MSG_FASTOPEN" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: dgram refuses: sendto with MSG_FASTOPEN", "sim: send flag fastopen on sim fd 100032 is not simulated", struct {
        fn body() !void {
            try r3.up();
            const fd = try dg.open();
            var iov = [1]dg.os_net.iovec_const{dg.os_net.iovecConstFromSlice("x")};
            const to = dg.addr(127, 0, 0, 1, 7000);
            _ = try dg.os_net.sendto(fd, &iov, .{ .fastopen = true }, @ptrCast(&to), @sizeOf(dg.os_net.sockaddr.in));
        }
    }.body);
}

test "sim: dgram refuses: sendmsg with MSG_EOR" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: dgram refuses: sendmsg with MSG_EOR", "sim: send flag eor on sim fd 100032 is not simulated", struct {
        fn body() !void {
            try r3.up();
            const fd = try dg.open();
            var iov = [1]dg.os_net.iovec_const{dg.os_net.iovecConstFromSlice("x")};
            const to = dg.addr(127, 0, 0, 1, 7000);
            _ = try dg.os_net.sendmsg(fd, &iov, .{ .eor = true }, @ptrCast(&to), @sizeOf(dg.os_net.sockaddr.in), null);
        }
    }.body);
}

test "sim: dgram refuses: sendDatagram with MSG_DONTROUTE" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: dgram refuses: sendDatagram with MSG_DONTROUTE", "sim: send flag dont_route on sim fd 100032 is not simulated", struct {
        fn body() !void {
            try r3.up();
            const fd = try dg.open();
            var iov = [1]dg.os_net.iovec_const{dg.os_net.iovecConstFromSlice("x")};
            const to = dg.addr(127, 0, 0, 1, 7000);
            _ = try dg.os_net.sendDatagram(fd, &iov, .{ .dont_route = true }, &to, null);
        }
    }.body);
}

test "sim: dgram refuses: sendDatagram with MSG_CONFIRM" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: dgram refuses: sendDatagram with MSG_CONFIRM", "sim: send flag confirm on sim fd 100032 is not simulated", struct {
        fn body() !void {
            try r3.up();
            const fd = try dg.open();
            var iov = [1]dg.os_net.iovec_const{dg.os_net.iovecConstFromSlice("x")};
            const to = dg.addr(127, 0, 0, 1, 7000);
            _ = try dg.os_net.sendDatagram(fd, &iov, .{ .confirm = true }, &to, null);
        }
    }.body);
}

test "sim: dgram: a send with MSG_NOSIGNAL or MSG_DONTWAIT is accepted through sendto, sendmsg and sendDatagram, and lands" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    try dg.run(1, struct {
        fn body() !void {
            const rx = try dg.open();
            try dg.bindTo(rx, dg.addr(127, 0, 0, 1, 7000));
            const tx = try dg.open();
            const to = dg.addr(127, 0, 0, 1, 7000);
            var iov = [1]dg.os_net.iovec_const{dg.os_net.iovecConstFromSlice("x")};
            const flags = [_]dg.os_net.SendFlags{ .{ .no_signal = true, .dontwait = true }, .{ .no_signal = false }, .{ .dontwait = true } };
            for (flags) |f| {
                _ = try dg.os_net.sendto(tx, &iov, f, @ptrCast(&to), @sizeOf(dg.os_net.sockaddr.in));
                _ = try dg.os_net.sendmsg(tx, &iov, f, @ptrCast(&to), @sizeOf(dg.os_net.sockaddr.in), null);
                _ = try dg.os_net.sendDatagram(tx, &iov, f, &to, null);
            }
            try dg.settle();
            var n: usize = 0;
            var buf: [4]u8 = undefined;
            while (dg.recvInto(rx, &.{&buf})) |_| n += 1 else |err| if (err != error.WouldBlock) return err;
            try std.testing.expectEqual(@as(usize, 9), n);
            dg.os_net.close(rx);
            dg.os_net.close(tx);
        }
    }.body);
}

test "sim: dgram refuses: recvmsg without a control buffer on a socket that asked for destination addresses" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: dgram refuses: recvmsg without a control buffer on a socket that asked for destination addresses", "sim: recvmsg on sim fd 100032, which asked for destination addresses, is not simulated: use recvDatagram", struct {
        fn body() !void {
            try dg.run(1, struct {
                fn inner() !void {
                    const rx = try dg.open();
                    try dg.bindTo(rx, dg.addr(127, 0, 0, 1, 7000));
                    try dg.os_net.enableDestinationAddress(rx);
                    _ = try dg.sendFrom(rx, dg.addr(127, 0, 0, 1, 7000), null, "x");
                    try dg.settle();
                    var buf: [4]u8 = undefined;
                    var iov = [1]dg.os_net.iovec{dg.os_net.iovecFromSlice(&buf)};
                    const r = try dg.os_net.recvmsg(rx, &iov, .{}, null, null, null);
                    std.debug.print("recvmsg returned len {d}, flags 0x{x}, controllen {d}\n", .{ r.len, r.flags, r.controllen });
                }
            }.inner);
        }
    }.body);
}

test "sim: dgram refuses: recvmsg with a control buffer on a socket that asked for destination addresses" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: dgram refuses: recvmsg with a control buffer on a socket that asked for destination addresses", "sim: recvmsg on sim fd 100032, which asked for destination addresses, is not simulated: use recvDatagram", struct {
        fn body() !void {
            try r3.up();
            const rx = try dg.open();
            try dg.os_net.enableDestinationAddress(rx);
            var buf: [4]u8 = undefined;
            var ctl: [64]u8 = undefined;
            var iov = [1]dg.os_net.iovec{dg.os_net.iovecFromSlice(&buf)};
            _ = try dg.os_net.recvmsg(rx, &iov, .{ .dontwait = true }, null, null, &ctl);
        }
    }.body);
}

test "sim: dgram refuses: an IPv6 socket during a run" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: dgram refuses: an IPv6 socket during a run", "sim: socket(ipv6, dgram, ip) during a sim run", struct {
        fn body() !void {
            try r3.up();
            _ = try dg.os_net.socket(.ipv6, .dgram, .ip, .{ .nonblocking = true });
        }
    }.body);
}

test "sim: dgram refuses: a stream socket during a run" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: dgram refuses: a stream socket during a run", "sim: socket(ipv4, stream, tcp) during a sim run", struct {
        fn body() !void {
            try r3.up();
            _ = try dg.os_net.socket(.ipv4, .stream, .tcp, .{ .nonblocking = true });
        }
    }.body);
}

test "sim: dgram refuses: a send to a non-loopback address" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: dgram refuses: a send to a non-loopback address", "sim: send to 10.0.0.1: only 127.0.0.0/8 is simulated", struct {
        fn body() !void {
            try r3.up();
            const fd = try dg.open();
            _ = try dg.sendFrom(fd, dg.addr(10, 0, 0, 1, 9), null, "x");
        }
    }.body);
}

test "sim: dgram refuses: a bind to a non-loopback address" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: dgram refuses: a bind to a non-loopback address", "sim: bind to 192.168.1.1: only 127.0.0.0/8 is simulated", struct {
        fn body() !void {
            try r3.up();
            const fd = try dg.open();
            try dg.bindTo(fd, dg.addr(192, 168, 1, 1, 9));
        }
    }.body);
}

test "sim: dgram refuses: a datagram over 65507 bytes" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: dgram refuses: a datagram over 65507 bytes", "sim: a datagram of 65508 bytes from sim fd 100032: the limit is 65507", struct {
        fn body() !void {
            try r3.up();
            const fd = try dg.open();
            const big = try std.heap.page_allocator.alloc(u8, 65508);
            _ = try dg.sendFrom(fd, dg.addr(127, 0, 0, 1, 9), null, big);
        }
    }.body);
}

test "sim: dgram refuses: a send to port 0" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: dgram refuses: a send to port 0", "sim: send to port 0 from sim fd 100032", struct {
        fn body() !void {
            try r3.up();
            const fd = try dg.open();
            _ = try dg.sendFrom(fd, dg.addr(127, 0, 0, 1, 0), null, "x");
        }
    }.body);
}

test "sim: dgram refuses: a bind of a bound socket" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: dgram refuses: a bind of a bound socket", "sim: bind on sim fd 100032, which is already bound", struct {
        fn body() !void {
            try r3.up();
            const fd = try dg.open();
            try dg.bindTo(fd, dg.addr(127, 0, 0, 1, 0));
            try dg.bindTo(fd, dg.addr(127, 0, 0, 1, 0));
        }
    }.body);
}

test "sim: dgram refuses: a send on a closed socket" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: dgram refuses: a send on a closed socket", "sim: send on sim fd 100032, which is closed", struct {
        fn body() !void {
            try r3.up();
            const fd = try dg.open();
            dg.os_net.close(fd);
            _ = try dg.sendFrom(fd, dg.addr(127, 0, 0, 1, 9), null, "x");
        }
    }.body);
}

test "sim: dgram refuses: connect on a datagram socket" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: dgram refuses: connect on a datagram socket", "sim: connect on sim datagram socket 100032: connected UDP is not simulated", struct {
        fn body() !void {
            try r3.up();
            const fd = try dg.open();
            const a = dg.addr(127, 0, 0, 1, 9);
            try dg.os_net.connect(fd, @ptrCast(&a), @sizeOf(dg.os_net.sockaddr.in));
        }
    }.body);
}

test "sim: dgram refuses: an IPv6 destination" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: dgram refuses: an IPv6 destination", "sim: sendto with address family", struct {
        fn body() !void {
            try r3.up();
            const fd = try dg.open();
            var a6: std.c.sockaddr.in6 = .{ .port = 0, .flowinfo = 0, .addr = @splat(0), .scope_id = 0 };
            var iov = [1]dg.os_net.iovec_const{dg.os_net.iovecConstFromSlice("x")};
            _ = try dg.os_net.sendto(fd, &iov, .{}, @ptrCast(&a6), @sizeOf(std.c.sockaddr.in6));
        }
    }.body);
}

test "sim: dgram refuses: MSG_PEEK" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: dgram refuses: MSG_PEEK", "sim: MSG_PEEK on sim fd 100032 is not simulated", struct {
        fn body() !void {
            try r3.up();
            const fd = try dg.open();
            var buf: [4]u8 = undefined;
            var iov = [1]dg.os_net.iovec{dg.os_net.iovecFromSlice(&buf)};
            _ = try dg.os_net.recvfrom(fd, &iov, .{ .peek = true }, null, null);
        }
    }.body);
}

test "sim: dgram refuses: a socket open from another thread" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: dgram refuses: a socket open from another thread", "sim: sim socket open" ++ other_thread_msg, struct {
        fn body() !void {
            try r3.up();
            try r3.other(struct {
                fn f() void {
                    _ = dg.open() catch {};
                }
            }.f);
        }
    }.body);
}

test "sim: dgram refuses: a socket bind from another thread" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: dgram refuses: a socket bind from another thread", "sim: sim socket bind" ++ other_thread_msg, struct {
        fn body() !void {
            try r3.up();
            dg_thread.fd = try dg.open();
            try r3.other(struct {
                fn f() void {
                    dg.bindTo(dg_thread.fd, dg.addr(127, 0, 0, 1, 0)) catch {};
                }
            }.f);
        }
    }.body);
}

test "sim: dgram refuses: a socket send from another thread" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: dgram refuses: a socket send from another thread", "sim: sim socket send" ++ other_thread_msg, struct {
        fn body() !void {
            try r3.up();
            dg_thread.fd = try dg.open();
            try r3.other(struct {
                fn f() void {
                    _ = dg.sendFrom(dg_thread.fd, dg.addr(127, 0, 0, 1, 9), null, "x") catch {};
                }
            }.f);
        }
    }.body);
}

test "sim: dgram refuses: a socket receive from another thread" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: dgram refuses: a socket receive from another thread", "sim: sim socket receive" ++ other_thread_msg, struct {
        fn body() !void {
            try r3.up();
            dg_thread.fd = try dg.open();
            try r3.other(struct {
                fn f() void {
                    _ = dg.recvInto(dg_thread.fd, &.{&dg_thread.buf}) catch {};
                }
            }.f);
        }
    }.body);
}

test "sim: dgram refuses: a socket close from another thread" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: dgram refuses: a socket close from another thread", "sim: sim socket close" ++ other_thread_msg, struct {
        fn body() !void {
            try r3.up();
            dg_thread.fd = try dg.open();
            try r3.other(struct {
                fn f() void {
                    dg.os_net.close(dg_thread.fd);
                }
            }.f);
        }
    }.body);
}

test "sim: dgram refuses: a socket setNonBlocking from another thread" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: dgram refuses: a socket setNonBlocking from another thread", "sim: sim socket setNonBlocking" ++ other_thread_msg, struct {
        fn body() !void {
            try r3.up();
            dg_thread.fd = try dg.open();
            try r3.other(struct {
                fn f() void {
                    dg.os_net.setNonBlocking(dg_thread.fd, false) catch {};
                }
            }.f);
        }
    }.body);
}

test "sim: dgram refuses: a socket enableDestinationAddress from another thread" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: dgram refuses: a socket enableDestinationAddress from another thread", "sim: sim socket enableDestinationAddress" ++ other_thread_msg, struct {
        fn body() !void {
            try r3.up();
            dg_thread.fd = try dg.open();
            try r3.other(struct {
                fn f() void {
                    dg.os_net.enableDestinationAddress(dg_thread.fd) catch {};
                }
            }.f);
        }
    }.body);
}

test "sim: dgram: a datagram in flight is I/O still to come: the clock reaches its landing before the run is a deadlock" {
    try refusal("sim: dgram: a datagram in flight is I/O still to come: the clock reaches its landing before the run is a deadlock", "sim: deadlock clock_ns=100000", struct {
        fn body() !void {
            try r3.up();
            const fd = try dg.open();
            _ = try dg.sendFrom(fd, dg.addr(127, 0, 0, 1, 9), null, "x");
            var ev: @import("sync/Event.zig") = .init;
            try ev.wait();
        }
    }.body);
}

test "sim: fd namespace: begin refuses a real descriptor in the datagram part of the sim fd range" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: fd namespace: begin refuses a real descriptor in the datagram part of the sim fd range", "sim: fd namespace: real descriptor 1037 is open in the sim fd range", struct {
        fn body() !void {
            try p1.moveBase();
            try p1.raiseSoftLimit();
            const n = p1.moved_base + max_ends + 5;
            if (std.c.dup2(0, n) != n) return error.DupFailed;
            begin(1);
        }
    }.body);
}

test "sim: fd namespace: a datagram socket opened after the soft limit was raised again is refused" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: fd namespace: a datagram socket opened after the soft limit was raised again is refused", "sim: fd namespace: the soft RLIMIT_NOFILE is 2000, above the sim fd base 1000", struct {
        fn body() !void {
            try p1.moveBase();
            begin(1);
            try p1.raiseSoftLimit();
            _ = try dgramOpen(true);
        }
    }.body);
}

// ---- Phase 1, slice 2: readiness and the ev ops on datagram sockets ----

test "sim: dgram ev: the layer sends two buffers to an ev receiver, and an ev send of two buffers reaches the layer, whole, with sources" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    try dg.run(1, struct {
        fn body() !void {
            const ev_fd = try dg.evOpen();
            const ev_addr = try dg.evBind(ev_fd, dg.addr(127, 0, 0, 1, 0));
            try std.testing.expectEqual(Ip4{ .ip = .{ 127, 0, 0, 1 }, .port = 49152 }, ev_addr);
            const c_fd = try dg.open();
            try dg.bindTo(c_fd, dg.addr(127, 0, 0, 1, 0));
            // Layer to ev.
            _ = try dg.sendTo(c_fd, dg.addr(127, 0, 0, 1, 49152), &.{ "hel", "lo" });
            var buf: [16]u8 = undefined;
            const got = try dg.evRecvFrom(ev_fd, &buf);
            try std.testing.expectEqualStrings("hello", buf[0..got.len]);
            try std.testing.expectEqual(Ip4{ .ip = .{ 127, 0, 0, 1 }, .port = 49153 }, got.from);
            // Ev to layer.
            try std.testing.expectEqual(@as(usize, 6), try dg.evSendTo2(ev_fd, dg.addr(127, 0, 0, 1, 49153), "wor", "ld!"));
            try dg.settle();
            var b1: [2]u8 = undefined;
            var b2: [8]u8 = undefined;
            const back = try dg.recvInto(c_fd, &.{ &b1, &b2 });
            try std.testing.expectEqual(@as(usize, 6), back.info.len);
            try std.testing.expectEqualStrings("wo", &b1);
            try std.testing.expectEqualStrings("rld!", b2[0..4]);
            try std.testing.expectEqual(Ip4{ .ip = .{ 127, 0, 0, 1 }, .port = 49152 }, back.from);
            dg.evClose(ev_fd);
        }
    }.body);
}

test "sim: dgram ev: poll with a zero timeout reports readable only once a datagram has landed, and writable always" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    try dg.run(1, struct {
        fn body() !void {
            const rx = try dg.open();
            const tx = try dg.open();
            try dg.bindTo(rx, dg.addr(127, 0, 0, 1, 0));
            const IN = dg.os_net.POLL.IN;
            const OUT = dg.os_net.POLL.OUT;
            try std.testing.expectEqual(@as(i16, OUT), try dg.pollNow(rx, IN | OUT));
            _ = try dg.sendFrom(tx, dg.addr(127, 0, 0, 1, 49152), null, "x");
            try std.testing.expectEqual(@as(i16, 0), try dg.pollNow(rx, IN));
            try dg.settle();
            try std.testing.expectEqual(@as(i16, IN), try dg.pollNow(rx, IN));
            try std.testing.expectEqual(dg.os_net.PollOutcome{ .ready = IN }, try dg.os_net.pollInterruptible(rx, IN, 0));
            var buf: [1]u8 = undefined;
            _ = try dg.recvInto(rx, &.{&buf});
            try std.testing.expectEqual(@as(i16, 0), try dg.pollNow(rx, IN));
            try std.testing.expectEqual(dg.os_net.PollOutcome.timed_out, try dg.os_net.pollInterruptible(rx, IN, 0));
        }
    }.body);
}

const dg_park = struct {
    var rx: i32 = 0;
    var tx: i32 = 0;
    var woke_at: u64 = 0;
    var sent_at: u64 = 0;
    var result: anyerror!void = {};
    var got: [8]u8 = undefined;
    var got_len: usize = 0;

    fn pollThenRecord() void {
        result = dg.evPoll(rx, .recv);
        woke_at = clockNs();
    }

    fn recvThenRecord() void {
        if (dg.evRecvFrom(rx, &got)) |r| {
            got_len = r.len;
            result = {};
        } else |err| result = err;
        woke_at = clockNs();
    }
};

test "sim: dgram ev: a poll parked before the datagram is sent wakes when it lands, with no second send, and rearms" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    try dg.run(1, struct {
        fn body() !void {
            dg_park.rx = try dg.open();
            dg_park.tx = try dg.open();
            try dg.bindTo(dg_park.rx, dg.addr(127, 0, 0, 1, 0));
            var round: usize = 0;
            while (round < 2) : (round += 1) {
                dg_park.woke_at = 0;
                var h = try dg.runtime.spawn(dg_park.pollThenRecord, .{});
                // Let the poll park: a sleep of 1 ms with nothing on the wire.
                try dg.rt_mod.sleep(.fromMilliseconds(1));
                try std.testing.expectEqual(@as(u64, 0), dg_park.woke_at);
                dg_park.sent_at = clockNs();
                _ = try dg.sendFrom(dg_park.tx, dg.addr(127, 0, 0, 1, 49152), null, "x");
                h.join();
                try dg_park.result;
                try std.testing.expectEqual(dg_park.sent_at + wire_latency_ns, dg_park.woke_at);
                var buf: [1]u8 = undefined;
                _ = try dg.recvInto(dg_park.rx, &.{&buf});
            }
        }
    }.body);
}

test "sim: dgram ev: a parked receive that is canceled gives Canceled, and the next datagram is still there for the next receive" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    try dg.run(1, struct {
        fn body() !void {
            dg_park.rx = try dg.open();
            dg_park.tx = try dg.open();
            try dg.bindTo(dg_park.rx, dg.addr(127, 0, 0, 1, 0));
            var h = try dg.runtime.spawn(dg_park.recvThenRecord, .{});
            try dg.rt_mod.sleep(.fromMilliseconds(1));
            h.cancel();
            try std.testing.expectError(error.Canceled, dg_park.result);
            _ = try dg.sendFrom(dg_park.tx, dg.addr(127, 0, 0, 1, 49152), null, "kept");
            var buf: [8]u8 = undefined;
            const got = try dg.evRecvFrom(dg_park.rx, &buf);
            try std.testing.expectEqualStrings("kept", buf[0..got.len]);
        }
    }.body);
}

const dg_wide = struct {
    var fds: [max_dgram]i32 = undefined;
    var got: [max_dgram]u8 = undefined;

    fn recvOne(i: usize) !void {
        var buf: [4]u8 = undefined;
        const r = try dg.evRecvFrom(fds[i], &buf);
        if (r.len != 1) return error.WrongLength;
        got[i] = buf[0];
    }

    /// Every datagram slot open, an ev receive parked on each, and one of
    /// them sends a datagram to every one (itself too) at one virtual time,
    /// so all of them land, and wake their receive, together.
    fn body() !void {
        for (&fds, 0..) |*fd, i| {
            fd.* = try dg.open();
            try dg.bindTo(fd.*, dg.addr(127, 0, 0, 1, @intCast(7000 + i)));
        }
        @memset(&got, 0xff);
        var hs: [max_dgram]@TypeOf(try dg.runtime.spawn(recvOne, .{0})) = undefined;
        hs[0] = try dg.runtime.spawn(recvOne, .{0});
        for (hs[1..], 1..) |*h, i| h.* = try dg.runtime.spawn(recvOne, .{i});
        try dg.rt_mod.sleep(.fromMilliseconds(1));
        for (0..max_dgram) |i| {
            const b = [1]u8{@intCast(i)};
            _ = try dg.sendFrom(fds[0], dg.addr(127, 0, 0, 1, @intCast(7000 + i)), null, &b);
        }
        for (&hs) |*h| try h.join();
        for (got, 0..) |g, i| try std.testing.expectEqual(@as(u8, @intCast(i)), g);
        for (fds) |fd| dg.os_net.close(fd);
    }
};

test "sim: dgram ev: 64 sockets, each with a parked receive, all woken at one virtual time" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    try dg.run(3, dg_wide.body);
}

/// The owner's case after finding 2: an op parked on socket A is woken by a
/// datagram landing, and a task closes A, maybe opens a socket that takes
/// A's number, and maybe sends that one a datagram, all near the landing's
/// virtual time. The old op must end with A's datagram, or
/// FileDescriptorNotASocket, soon; never with the new socket's datagram, and
/// never by waiting on the new socket. (A task here runs only between polls.
/// Inside a harvest it takes a callback or a CompletionQueue's coop yield:
/// the `dg_batch_close` and `dg_coop_close` tests.)
const dg_land_close = struct {
    const Kind = enum { recv, poll };
    const Outcome = enum { got_old, not_a_socket };
    var kind: Kind = .recv;
    /// When the closer acts, from the send of A's datagram.
    var close_after_ns: u64 = 0;
    var reuse = true;
    var send_new = true;
    var a: i32 = 0;
    var tx: i32 = 0;
    var outcome: ?Outcome = null;
    var done_at: u64 = 0;

    fn waiter() !void {
        var buf: [8]u8 = undefined;
        switch (kind) {
            .recv => if (dg.evRecvFrom(a, &buf)) |r| {
                if (!std.mem.eql(u8, buf[0..r.len], "old")) {
                    std.debug.print("the receive got \"{s}\"\n", .{buf[0..r.len]});
                    return error.GotAnotherSocketsDatagram;
                }
                outcome = .got_old;
            } else |err| switch (err) {
                error.FileDescriptorNotASocket => outcome = .not_a_socket,
                else => return err,
            },
            .poll => if (dg.evPoll(a, .recv)) |_| {
                outcome = .got_old;
            } else |err| switch (err) {
                error.FileDescriptorNotASocket => outcome = .not_a_socket,
                else => return err,
            },
        }
        done_at = clock_ns;
    }

    fn closer() !void {
        try dg.rt_mod.sleep(.fromNanoseconds(close_after_ns));
        dg.os_net.close(a);
        if (!reuse) return;
        const b = try dg.open();
        if (b != a) return error.NumberNotReused;
        try dg.bindTo(b, dg.addr(127, 0, 0, 1, 7000));
    }

    fn body() !void {
        outcome = null;
        a = try dg.open();
        try dg.bindTo(a, dg.addr(127, 0, 0, 1, 7000));
        tx = try dg.open();
        try dg.bindTo(tx, dg.addr(127, 0, 0, 1, 7100));
        var w = try dg.runtime.spawn(waiter, .{});
        try dg.rt_mod.sleep(.fromMilliseconds(1));
        const landing = clock_ns + wire_latency_ns;
        _ = try dg.sendFrom(tx, dg.addr(127, 0, 0, 1, 7000), null, "old");
        var c = try dg.runtime.spawn(closer, .{});
        // Late: the new socket's datagram lands well after A's.
        try dg.rt_mod.sleep(.fromMilliseconds(5));
        const late = clock_ns;
        if (send_new and reuse) _ = try dg.sendFrom(tx, dg.addr(127, 0, 0, 1, 7000), null, "new");
        try dg.rt_mod.sleep(.fromMilliseconds(1));
        try c.join();
        // An op wrongly left parked on the new socket ends here, late.
        if (reuse) dg.os_net.close(a);
        try w.join();
        dg.os_net.close(tx);
        if (done_at >= late) {
            std.debug.print("the old op ended at {d} ns, after the late mark {d} (A's datagram landed at {d})\n", .{ done_at, late, landing });
            return error.OldOpOutlivedTheClose;
        }
        // Each op ended once, and nothing is left.
        try std.testing.expectEqual(@as(usize, 0), live_len);
        try std.testing.expectEqual(@as(usize, 0), due_len);
    }
};

test "sim: dgram ev: an op woken by a landing, with its socket closed and its number reused near that time, ends with its own datagram or FileDescriptorNotASocket" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    const T = dg_land_close;
    var seen: [2][2]usize = .{ .{ 0, 0 }, .{ 0, 0 } };
    var runs: usize = 0;
    for ([_]T.Kind{ .recv, .poll }) |k| {
        for ([_]bool{ true, false }) |reuse| {
            for ([_]bool{ true, false }) |send_new| {
                if (!reuse and send_new) continue;
                for ([_]u64{ wire_latency_ns - 1, wire_latency_ns, wire_latency_ns + 1 }) |after| {
                    for (1..11) |s| {
                        T.kind = k;
                        T.reuse = reuse;
                        T.send_new = send_new;
                        T.close_after_ns = after;
                        dg.run(s, T.body) catch |err| {
                            std.debug.print("{t}, reuse {}, new datagram {}, close {d} ns after the send, seed {d}: {t}\n", .{ k, reuse, send_new, after, s, err });
                            return err;
                        };
                        seen[@backingInt(k)][@backingInt(T.outcome.?)] += 1;
                        runs += 1;
                    }
                }
            }
        }
    }
    std.debug.print("{d} runs; receive: {d} got A's datagram, {d} FileDescriptorNotASocket; poll: {d} readable, {d} FileDescriptorNotASocket\n", .{ runs, seen[0][0], seen[0][1], seen[1][0], seen[1][1] });
    // Both sides of the close were reached, for both kinds of op.
    for (seen) |row| for (row) |n| try std.testing.expect(n > 0);
}

/// Round 2 of the outside review, F1: two ops of a race group that complete
/// in one harvest, each finishing without deferral. The first one's group
/// callback cancels the second, which then completes through the cancel; the
/// harvest must not complete it again from its batch.
/// Round 3 of the outside review, P1, the callback route: two receives woken
/// by landings at one virtual time are in one harvest batch. The first one's
/// callback (not deferred, so it runs inside the harvest) closes the other's
/// socket, opens a socket that gets the same fd number, binds it to the same
/// port and sends "new" to it. The second receive must end
/// FileDescriptorNotASocket: its socket is gone, whatever the number holds.
/// With `use_poll`, both ops are polls for reading instead of receives.
const dg_batch_close = struct {
    var use_poll = false;
    var polls: [2]dg.ev.NetPoll = undefined;
    var fds: [2]i32 = undefined;
    var ports: [2]u16 = undefined;
    var ops: [2]dg.ev.NetRecvFrom = undefined;
    var bufs: [2][8]u8 = undefined;
    var iovs: [2][1]dg.os_net.iovec = undefined;
    var froms: [2]dg.os_net.sockaddr.in = undefined;
    var from_lens: [2]dg.os_net.socklen_t = undefined;
    var calls: [2]u32 = .{ 0, 0 };
    var first: ?usize = null;
    var reopened: i32 = -1;
    var tx: i32 = -1;

    fn onRecv(_: *dg.ev.Loop, c: *dg.ev.Completion) void {
        const k: usize = @intFromPtr(c.userdata.?) - 1;
        calls[k] += 1;
        if (first != null) return;
        first = k;
        // Close the other receive's socket and take its number and port.
        const other = 1 - k;
        dg.os_net.close(fds[other]);
        reopened = dg.open() catch @panic("open");
        dg.bindTo(reopened, dg.addr(127, 0, 0, 1, ports[other])) catch @panic("bind");
        _ = dg.sendFrom(tx, dg.addr(127, 0, 0, 1, ports[other]), null, "new") catch @panic("send");
    }

    fn body() !void {
        calls = .{ 0, 0 };
        first = null;
        reopened = -1;
        tx = try dg.open();
        const loop = &dg.rt_mod.getCurrentExecutor().loop;
        for (0..2) |k| {
            fds[k] = try dg.open();
            try dg.bindTo(fds[k], dg.addr(127, 0, 0, 1, 0));
            ports[k] = (try dg.local(fds[k])).port;
            from_lens[k] = @sizeOf(dg.os_net.sockaddr.in);
            ops[k] = dg.ev.NetRecvFrom.init(fds[k], .fromSlice(&bufs[k], &iovs[k]), .{}, @ptrCast(&froms[k]), &from_lens[k]);
            polls[k] = dg.ev.NetPoll.init(fds[k], .recv);
            const c = if (use_poll) &polls[k].c else &ops[k].c;
            c.flags.defer_callback = false;
            c.callback = onRecv;
            c.userdata = @ptrFromInt(k + 1);
            // Parks at once: the queue is empty.
            loop.add(c);
        }
        // Both datagrams land at one virtual time; both receives wake.
        for (0..2) |k| _ = try dg.sendFrom(tx, dg.addr(127, 0, 0, 1, ports[k]), null, "x");
        // Long enough for "new" to land too.
        try dg.rt_mod.sleep(.fromMilliseconds(1));
        try std.testing.expectEqual([2]u32{ 1, 1 }, calls);
        const k = first.?;
        const other = 1 - k;
        try std.testing.expectEqual(fds[other], reopened);
        if (use_poll) {
            try polls[k].getResult();
            try std.testing.expectError(error.FileDescriptorNotASocket, polls[other].getResult());
            // The replacement still holds both its datagrams: the old
            // socket's never landed there, "new" did.
            var buf: [8]u8 = undefined;
            try std.testing.expectEqualStrings("new", buf[0..(try dg.recvInto(reopened, &.{&buf})).info.len]);
            try std.testing.expectError(error.WouldBlock, dg.recvInto(reopened, &.{&buf}));
            for ([_]i32{ reopened, fds[k], tx }) |fd| dg.os_net.close(fd);
            return;
        }
        try std.testing.expectEqual(@as(usize, 1), try ops[k].getResult());
        _ = ops[other].getResult() catch |err| {
            try std.testing.expectEqual(error.FileDescriptorNotASocket, err);
            // The replacement still holds its own datagram.
            var buf: [8]u8 = undefined;
            try std.testing.expectEqualStrings("new", buf[0..(try dg.recvInto(reopened, &.{&buf})).info.len]);
            dg.os_net.close(reopened);
            dg.os_net.close(fds[k]);
            dg.os_net.close(tx);
            return;
        };
        std.debug.print("seed {d}: the receive whose socket was closed in the batch got {d} bytes: \"{s}\"\n", .{ seed(), try ops[other].getResult(), bufs[other][0..try ops[other].getResult()] });
        return error.ReceiveReachedTheReplacementSocket;
    }
};

test "sim: dgram ev: a receive woken by a landing whose socket a callback earlier in the same harvest closes and reopens ends FileDescriptorNotASocket" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    dg_batch_close.use_poll = false;
    for (1..17) |s| try dg.run(s, dg_batch_close.body);
}

test "sim: dgram ev: a poll woken by a landing whose socket a callback earlier in the same harvest closes and reopens ends FileDescriptorNotASocket" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    dg_batch_close.use_poll = true;
    defer dg_batch_close.use_poll = false;
    for (1..17) |s| try dg.run(s, dg_batch_close.body);
}

/// Round 3 of the outside review, P1, the task route: a CompletionQueue's
/// owner callback calls `simCoopYield` when no driver waits on the queue
/// (src/completion_queue.zig `ownerCallback`), and that runs a ready task of
/// another executor inside the harvest. Receive A is in the queue and does
/// not defer its finish; receive B is a task's wait on another socket; both
/// are woken by landings at one virtual time; B is an op of this test's own
/// on executor 0's loop, so its state can be read. Task X waits on an Event.
/// Receive C, woken by a landing at the same time, sets that Event from its
/// callback (not deferred). When the batch harvests C, then A, then B, C's
/// callback makes X ready and A's owner callback runs it, inside the harvest.
/// (A task's own I/O wait cannot be the trigger: its wake is delivered after
/// the poll, through the loop's dispatched list.) X then closes B's socket, opens one that gets the same
/// number and port, and sends "new" to it. B must end FileDescriptorNotASocket or with its own
/// datagram, never with "new". `reached` counts the runs in which X ran
/// while A was complete and B was not: inside the batch.
const dg_coop_close = struct {
    const CompletionQueue = @import("completion_queue.zig").CompletionQueue;
    var sa: i32 = -1;
    var sb: i32 = -1;
    var sc: i32 = -1;
    var port_b: u16 = 0;
    var tx: i32 = -1;
    var a: dg.ev.NetRecvFrom = undefined;
    var a_buf: [8]u8 = undefined;
    var a_iov: [1]dg.os_net.iovec = undefined;
    var b: dg.ev.NetRecvFrom = undefined;
    var b_buf: [8]u8 = undefined;
    var b_iov: [1]dg.os_net.iovec = undefined;
    var b_calls: u32 = 0;
    var x_closed: bool = false;
    var go: @import("sync/Event.zig") = .init;
    var c_op: dg.ev.NetRecvFrom = undefined;
    var c_buf: [8]u8 = undefined;
    var c_iov: [1]dg.os_net.iovec = undefined;
    var reached: usize = 0;
    var reopened: i32 = -1;

    fn onB(_: *dg.ev.Loop, _: *dg.ev.Completion) void {
        b_calls += 1;
    }

    fn onC(_: *dg.ev.Loop, _: *dg.ev.Completion) void {
        go.set();
    }

    fn closer() void {
        go.wait() catch return;
        if (b.c.has_result) return;
        if (a.c.has_result) reached += 1;
        x_closed = true;
        dg.os_net.close(sb);
        reopened = dg.open() catch @panic("open");
        dg.bindTo(reopened, dg.addr(127, 0, 0, 1, port_b)) catch @panic("bind");
        _ = dg.sendFrom(tx, dg.addr(127, 0, 0, 1, port_b), null, "new") catch @panic("send");
    }

    fn driver(x_exec: u6) !void {
        b_calls = 0;
        x_closed = false;
        reopened = -1;
        sa = try dg.open();
        sb = try dg.open();
        sc = try dg.open();
        tx = try dg.open();
        try dg.bindTo(sa, dg.addr(127, 0, 0, 1, 0));
        try dg.bindTo(sb, dg.addr(127, 0, 0, 1, 0));
        try dg.bindTo(sc, dg.addr(127, 0, 0, 1, 0));
        port_b = (try dg.local(sb)).port;
        var cq = CompletionQueue.init();
        a = dg.ev.NetRecvFrom.init(sa, .fromSlice(&a_buf, &a_iov), .{}, null, null);
        a.c.flags.defer_callback = false;
        try cq.submit(&a.c);
        b = dg.ev.NetRecvFrom.init(sb, .fromSlice(&b_buf, &b_iov), .{}, null, null);
        b.c.callback = onB;
        // Parks at once: the queue is empty.
        dg.rt_mod.getCurrentExecutor().loop.add(&b.c);
        go = .init;
        c_op = dg.ev.NetRecvFrom.init(sc, .fromSlice(&c_buf, &c_iov), .{}, null, null);
        c_op.c.flags.defer_callback = false;
        c_op.c.callback = onC;
        dg.rt_mod.getCurrentExecutor().loop.add(&c_op.c);
        var x = try dg.runtime.spawnInto(.{ .executor = x_exec }, closer, .{});
        // Let X park on the Event.
        try dg.rt_mod.sleep(.fromNanoseconds(1));
        _ = try dg.sendFrom(tx, dg.addr(127, 0, 0, 1, (try dg.local(sc)).port), null, "c");
        _ = try dg.sendFrom(tx, dg.addr(127, 0, 0, 1, (try dg.local(sa)).port), null, "a");
        _ = try dg.sendFrom(tx, dg.addr(127, 0, 0, 1, port_b), null, "b");
        try dg.rt_mod.sleep(.fromMilliseconds(1));
        x.join();
        try std.testing.expectEqual(@as(u32, 1), b_calls);
        try std.testing.expectEqual(&a.c, try cq.wait());
        try std.testing.expectEqual(@as(usize, 1), try a.getResult());
        if (b.getResult()) |n| {
            if (!std.mem.eql(u8, b_buf[0..n], "b")) {
                std.debug.print("seed {d}: receive B got \"{s}\" after its socket was closed and its number reused\n", .{ seed(), b_buf[0..n] });
                return error.ReceiveReachedTheReplacementSocket;
            }
        } else |err| {
            if (!x_closed) return err;
            try std.testing.expectEqual(error.FileDescriptorNotASocket, err);
        }
        dg.os_net.close(sa);
        dg.os_net.close(sc);
        dg.os_net.close(if (x_closed) reopened else sb);
        dg.os_net.close(tx);
    }

    fn run(seed_value: u64, x_exec: u6) !void {
        begin(seed_value);
        defer end();
        var o: dg.rt_mod.RuntimeOptions = .{ .executors = .exact(2), .thread_pool = .{ .max_threads = 0 } };
        o.stack_pool.shrink_interval = .zero;
        const rt = try dg.rt_mod.Runtime.init(std.heap.page_allocator, o);
        defer rt.deinit();
        dg.runtime = rt;
        var h = try rt.spawnInto(.{ .executor = 0 }, driver, .{x_exec});
        try h.join();
    }
};

test "sim: dgram ev: a task that a CompletionQueue callback runs inside the harvest closes and reopens the socket of a receive later in the batch, which ends FileDescriptorNotASocket" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    dg_coop_close.reached = 0;
    for (1..65) |s| {
        for ([_]u6{ 0, 1 }) |x_exec| {
            dg_coop_close.run(s, x_exec) catch |err| {
                std.debug.print("seed {d}, closer on executor {d}: {t}\n", .{ s, x_exec, err });
                return err;
            };
        }
    }
    std.debug.print("128 runs; the closer ran inside the harvest batch in {d}\n", .{dg_coop_close.reached});
    try std.testing.expect(dg_coop_close.reached > 0);
}

/// Round 3, P1, the pipe side: pipe ends are never reused within a run, so
/// no replacement can take an op's number; but a callback earlier in a batch
/// can close the end of an op later in it. Receive R parks on end 0. Send S on
/// end 1 copies "data" into end 0 and wakes R; both are due after the seeded
/// delay, and when the delays are equal they share a batch. S's callback (not
/// deferred) closes end `close_end` with an ev close. R must then get the
/// bytes it was woken for, as a kernel reader blocked when another thread
/// closes the descriptor does: the bytes are in the buffer, and the close is
/// end of stream only after them.
const pipe_batch_close = struct {
    var fds: [2]i32 = undefined;
    var close_end: usize = 0;
    var r: dg.ev.NetRecv = undefined;
    var r_buf: [16]u8 = undefined;
    var r_iov: [1]dg.os_net.iovec = undefined;
    var s_op: dg.ev.NetSend = undefined;
    var s_iov: [1]dg.os_net.iovec_const = undefined;
    var cl: dg.ev.NetClose = undefined;
    var r_calls: u32 = 0;
    var reached: usize = 0;
    var r_clock: u64 = 0;
    var s_clock: u64 = 0;
    var r_after_s: bool = false;

    fn onR(_: *dg.ev.Loop, _: *dg.ev.Completion) void {
        r_calls += 1;
        r_clock = clockNs();
    }

    fn onClose(_: *dg.ev.Loop, _: *dg.ev.Completion) void {}

    fn onS(loop: *dg.ev.Loop, _: *dg.ev.Completion) void {
        // R is later in this batch when it has no result yet here and
        // completes at this same virtual time (checked in `body`).
        r_after_s = !r.c.has_result;
        s_clock = clockNs();
        cl = dg.ev.NetClose.init(fds[close_end]);
        // A completion with no callback is a task wake (the loop's
        // dispatched list); this one belongs to no task.
        cl.c.callback = onClose;
        loop.add(&cl.c);
    }

    fn body() !void {
        fds = pipePair();
        r_calls = 0;
        const loop = &dg.rt_mod.getCurrentExecutor().loop;
        r = dg.ev.NetRecv.init(fds[0], .fromSlice(&r_buf, &r_iov), .{});
        r.c.flags.defer_callback = false;
        r.c.callback = onR;
        loop.add(&r.c);
        s_op = dg.ev.NetSend.init(fds[1], .fromSlice("data", &s_iov), .{});
        s_op.c.flags.defer_callback = false;
        s_op.c.callback = onS;
        loop.add(&s_op.c);
        try dg.rt_mod.sleep(.fromMilliseconds(20));
        try std.testing.expectEqual(@as(u32, 1), r_calls);
        if (r_after_s and r_clock == s_clock) reached += 1;
        try std.testing.expectEqual(@as(usize, 4), try s_op.getResult());
        try std.testing.expectEqualStrings("data", r_buf[0..try r.getResult()]);
        try cl.getResult();
        var other = dg.ev.NetClose.init(fds[1 - close_end]);
        try dg.io_common.waitForIo(&other.c);
        try other.getResult();
    }
};

test "sim: pipe: a receive later in a batch whose end, or whose peer's end, a callback earlier in it closes gets the bytes it was woken for" {
    if (comptime !compiled_in) return error.SkipZigTest;
    pipe_batch_close.reached = 0;
    for ([_]usize{ 0, 1 }) |e| {
        pipe_batch_close.close_end = e;
        for (1..33) |s| {
            dg.run(s, pipe_batch_close.body) catch |err| {
                std.debug.print("seed {d}, closing end {d}: {t}\n", .{ s, e, err });
                return err;
            };
        }
    }
    std.debug.print("64 runs; the close came before the receive's harvest in the same batch in {d}\n", .{pipe_batch_close.reached});
    try std.testing.expect(pipe_batch_close.reached > 0);
}

const dg_race = struct {
    var calls: [2]u32 = .{ 0, 0 };

    fn counted(_: *dg.ev.Loop, c: *dg.ev.Completion) void {
        const i: usize = @intFromPtr(c.userdata.?) - 1;
        calls[i] += 1;
    }

    fn body() !void {
        const tx = try dg.open();
        const rx = try dg.open();
        try dg.bindTo(rx, dg.addr(127, 0, 0, 1, 7000));
        const to = dg.addr(127, 0, 0, 1, 7000);
        calls = .{ 0, 0 };
        var iov_a = [1]dg.os_net.iovec_const{dg.os_net.iovecConstFromSlice("a")};
        var iov_b = [1]dg.os_net.iovec_const{dg.os_net.iovecConstFromSlice("b")};
        var a = dg.ev.NetSendTo.init(tx, .{ .iovecs = &iov_a }, .{}, @ptrCast(&to), @sizeOf(dg.os_net.sockaddr.in));
        var b = dg.ev.NetSendTo.init(tx, .{ .iovecs = &iov_b }, .{}, @ptrCast(&to), @sizeOf(dg.os_net.sockaddr.in));
        for ([_]*dg.ev.Completion{ &a.c, &b.c }, 1..) |c, k| {
            c.flags.defer_callback = false;
            c.callback = counted;
            c.userdata = @ptrFromInt(k);
        }
        var g = dg.ev.Group.init(.race);
        g.add(&a.c);
        g.add(&b.c);
        try dg.io_common.waitForIo(&g.c);
        try std.testing.expectEqual([2]u32{ 1, 1 }, calls);
        try std.testing.expectEqual(@as(usize, 0), live_len);
        try std.testing.expectEqual(@as(usize, 0), due_len);
        dg.os_net.close(tx);
        dg.os_net.close(rx);
    }
};

/// F1 of round 2 on pipe ops, which share the harvest. A pipe send is due
/// after a seeded 1 or 5 ms, so some seeds put both sends in one harvest.
fn pipeRaceBody() !void {
    const pa = pipePair();
    const pb = pipePair();
    dg_race.calls = .{ 0, 0 };
    var ia = [1]dg.os_net.iovec_const{dg.os_net.iovecConstFromSlice("a")};
    var ib = [1]dg.os_net.iovec_const{dg.os_net.iovecConstFromSlice("b")};
    var a = dg.ev.NetSend.init(pa[0], .{ .iovecs = &ia }, .{});
    var b = dg.ev.NetSend.init(pb[0], .{ .iovecs = &ib }, .{});
    for ([_]*dg.ev.Completion{ &a.c, &b.c }, 1..) |c, k| {
        c.flags.defer_callback = false;
        c.callback = dg_race.counted;
        c.userdata = @ptrFromInt(k);
    }
    var g = dg.ev.Group.init(.race);
    g.add(&a.c);
    g.add(&b.c);
    try dg.io_common.waitForIo(&g.c);
    try std.testing.expectEqual([2]u32{ 1, 1 }, dg_race.calls);
    try std.testing.expectEqual(@as(usize, 0), live_len);
    try std.testing.expectEqual(@as(usize, 0), due_len);
}

test "sim: pipe: a race group of two sends that finish without deferral completes each member once" {
    if (comptime !compiled_in) return error.SkipZigTest;
    for (1..9) |s| try dg.run(s, pipeRaceBody);
}

test "sim: dgram ev: a race group of two sends that finish without deferral completes each member once" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    try dg.run(1, dg_race.body);
}

/// Round 2 of the outside review, F2: a pipe receive R1 parked on an empty
/// end is woken by a send, but a second receive R2 takes the bytes before
/// R1's wake is harvested. R1 must wait for more bytes, not report end of
/// file on an open pipe.
const pipe_stale = struct {
    var fds: [2]i32 = undefined;
    var got: [2][16]u8 = undefined;
    var len: [2]usize = .{ 0, 0 };
    var r2_first: bool = false;

    fn recvOne(k: usize) !void {
        var iov: [1]dg.os_net.iovec = undefined;
        var op = dg.ev.NetRecv.init(fds[0], .fromSlice(&got[k], &iov), .{});
        try dg.io_common.waitForIo(&op.c);
        len[k] = try op.getResult();
    }

    fn send(bytes: []const u8) !void {
        var iov = [1]dg.os_net.iovec_const{dg.os_net.iovecConstFromSlice(bytes)};
        var op = dg.ev.NetSend.init(fds[1], .{ .iovecs = &iov }, .{});
        try dg.io_common.waitForIo(&op.c);
        if (try op.getResult() != bytes.len) return error.ShortSend;
    }

    fn body() !void {
        fds = pipePair();
        len = .{ 0, 0 };
        var r1 = try dg.runtime.spawn(recvOne, .{0});
        try dg.rt_mod.sleep(.fromMilliseconds(1));
        try send("first");
        var r2 = try dg.runtime.spawn(recvOne, .{1});
        try dg.rt_mod.sleep(.fromMilliseconds(20));
        r2_first = len[1] == 5;
        try send("second");
        try r1.join();
        try r2.join();
        for (len, 0..) |n, k| {
            if (n == 0) {
                std.debug.print("receive {d} returned 0 (end of file) on an open pipe\n", .{k + 1});
                return error.FalseEndOfFile;
            }
        }
        var all: [32]u8 = undefined;
        const first: usize = if (r2_first) 1 else 0;
        @memcpy(all[0..len[first]], got[first][0..len[first]]);
        @memcpy(all[len[first]..][0..len[1 - first]], got[1 - first][0..len[1 - first]]);
        try std.testing.expectEqualStrings("firstsecond", all[0 .. len[0] + len[1]]);
    }
};

test "sim: pipe: a receive woken by bytes another receive took waits for more, and never reports end of file on an open pipe" {
    if (comptime !compiled_in) return error.SkipZigTest;
    var stale_cases: usize = 0;
    for (1..17) |s| {
        dg.run(s, pipe_stale.body) catch |err| {
            std.debug.print("seed {d}: {t}\n", .{ s, err });
            return err;
        };
        if (pipe_stale.r2_first) stale_cases += 1;
    }
    std.debug.print("16 seeds; in {d} the second receive took the first bytes\n", .{stale_cases});
    try std.testing.expect(stale_cases > 0);
}

test "sim: dgram ev: close wakes an ev receive parked on the socket" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    try dg.run(1, struct {
        fn body() !void {
            dg_park.rx = try dg.open();
            try dg.bindTo(dg_park.rx, dg.addr(127, 0, 0, 1, 0));
            dg_park.woke_at = 0;
            var h = try dg.runtime.spawn(dg_park.recvThenRecord, .{});
            try dg.rt_mod.sleep(.fromMilliseconds(1));
            try std.testing.expectEqual(@as(u64, 0), dg_park.woke_at);
            const closed_at = clockNs();
            dg.evClose(dg_park.rx);
            h.join();
            try std.testing.expectError(error.FileDescriptorNotASocket, dg_park.result);
            try std.testing.expectEqual(closed_at, dg_park.woke_at);
        }
    }.body);
}

test "sim: dgram ev: an ev receive with a datagram already queued and a poll for writing complete at once, and a receive with dontwait and none queued is WouldBlock" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    try dg.run(1, struct {
        fn body() !void {
            const rx = try dg.evOpen();
            _ = try dg.evBind(rx, dg.addr(127, 0, 0, 1, 0));
            var buf: [4]u8 = undefined;
            var iov: [1]dg.os_net.iovec = undefined;
            var op = dg.ev.NetRecvFrom.init(rx, .fromSlice(&buf, &iov), .{ .dontwait = true }, null, null);
            try dg.io_common.waitForIo(&op.c);
            try std.testing.expectError(error.WouldBlock, op.getResult());
            const tx = try dg.open();
            _ = try dg.sendFrom(tx, dg.addr(127, 0, 0, 1, 49152), null, "now");
            try dg.settle();
            const before = clockNs();
            const got = try dg.evRecvFrom(rx, &buf);
            try std.testing.expectEqualStrings("now", buf[0..got.len]);
            try std.testing.expectEqual(before, clockNs());
            // A poll for writing completes at once: a datagram socket is
            // always writable here.
            try dg.evPoll(rx, .send);
            try std.testing.expectEqual(before, clockNs());
        }
    }.body);
}

test "sim: dgram ev refuses: a real fd in a sim op" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: dgram ev refuses: a real fd in a sim op", "sim: a real fd 0 reached sim op net_recvfrom during a run", struct {
        fn body() !void {
            try r3.up();
            var buf: [4]u8 = undefined;
            _ = try dg.evRecvFrom(0, &buf);
        }
    }.body);
}

test "sim: dgram ev refuses: two ev ops parked on one socket" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: dgram ev refuses: two ev ops parked on one socket", "sim: two ev ops parked on sim datagram socket 100032", struct {
        fn body() !void {
            try r3.up();
            dg_park.rx = try dg.open();
            try dg.bindTo(dg_park.rx, dg.addr(127, 0, 0, 1, 0));
            _ = try r3.rt.spawn(dg_park.recvThenRecord, .{});
            _ = try r3.rt.spawn(dg_park.pollThenRecord, .{});
            try r3.rt.sleep(.fromMilliseconds(1));
        }
    }.body);
}

test "sim: dgram ev refuses: a poll with a positive timeout on a sim fd" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: dgram ev refuses: a poll with a positive timeout on a sim fd", "sim: a poll with a timeout of 10 ms on sim fds: only a zero timeout is allowed", struct {
        fn body() !void {
            try r3.up();
            const fd = try dg.open();
            var fds = [1]dg.os_net.pollfd{.{ .fd = fd, .events = dg.os_net.POLL.IN, .revents = 0 }};
            _ = try dg.os_net.poll(&fds, 10);
        }
    }.body);
}

test "sim: dgram ev refuses: a poll that mixes sim and real fds" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: dgram ev refuses: a poll that mixes sim and real fds", "sim: a poll of 1 sim fds and 1 real fds: they cannot be mixed", struct {
        fn body() !void {
            try r3.up();
            const fd = try dg.open();
            var fds = [2]dg.os_net.pollfd{ .{ .fd = fd, .events = dg.os_net.POLL.IN, .revents = 0 }, .{ .fd = 0, .events = dg.os_net.POLL.IN, .revents = 0 } };
            _ = try dg.os_net.poll(&fds, 0);
        }
    }.body);
}

test "sim: dgram ev refuses: a poll for writing on a closed sim socket" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: dgram ev refuses: a poll for writing on a closed sim socket", "sim: poll on sim fd 100032, which is closed", struct {
        fn body() !void {
            try r3.up();
            const fd = try dg.open();
            dg.os_net.close(fd);
            _ = try dg.pollNow(fd, dg.os_net.POLL.OUT);
        }
    }.body);
}

test "sim: dgram ev refuses: a poll for writing on a sim pipe fd" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: dgram ev refuses: a poll for writing on a sim pipe fd", "sim: poll on sim fd 100000, which is not a datagram socket", struct {
        fn body() !void {
            try r3.up();
            const fd = pipePair()[0];
            _ = try dg.pollNow(fd, dg.os_net.POLL.OUT);
        }
    }.body);
}

test "sim: dgram ev refuses: an ev poll for sending on a closed sim socket" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: dgram ev refuses: an ev poll for sending on a closed sim socket", "sim: poll on sim fd 100032, which is closed", struct {
        fn body() !void {
            try r3.up();
            const fd = try dg.open();
            dg.os_net.close(fd);
            try dg.evPoll(fd, .send);
        }
    }.body);
}

test "sim: dgram ev: a negative fd in a poll set is ignored, as the kernel does, beside a sim socket" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    try dg.run(1, struct {
        fn body() !void {
            const fd = try dg.open();
            const P = dg.os_net.POLL;
            var fds = [3]dg.os_net.pollfd{
                .{ .fd = -1, .events = P.IN | P.OUT, .revents = 0x7 },
                .{ .fd = fd, .events = P.OUT, .revents = 0 },
                .{ .fd = -5, .events = P.OUT, .revents = 0 },
            };
            try std.testing.expectEqual(@as(usize, 1), try dg.os_net.poll(&fds, 0));
            try std.testing.expectEqual(@as(i16, 0), fds[0].revents);
            try std.testing.expectEqual(@as(i16, P.OUT), fds[1].revents);
            try std.testing.expectEqual(@as(i16, 0), fds[2].revents);
            dg.os_net.close(fd);
        }
    }.body);
}

test "sim: dgram ev refuses: pollInterruptible with a positive timeout on a sim fd" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: dgram ev refuses: pollInterruptible with a positive timeout on a sim fd", "sim: a poll with a timeout of 5 ms on sim fds: only a zero timeout is allowed", struct {
        fn body() !void {
            try r3.up();
            const fd = try dg.open();
            _ = try dg.os_net.pollInterruptible(fd, dg.os_net.POLL.IN, 5);
        }
    }.body);
}

// ---- Phase 1, slice 3: socket options and receive limits (sections 6, 8) ----

test "sim: dgram options: an ENet-shaped host creation: socket, non-blocking, RCVBUF, SNDBUF accepted with no effect, destination addresses, bind to port 0, getsockname" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    try dg.run(1, struct {
        fn body() !void {
            const fd = try dg.os_net.socket(.ipv4, .dgram, .udp, .{ .nonblocking = false });
            try dg.os_net.setNonBlocking(fd, true);
            try std.testing.expect(try dg.os_net.isNonBlocking(fd));
            try dg.setInt(fd, sys.SOL.SOCKET, sys.SO.RCVBUF, 256 * 1024);
            // Accepted as a kernel does; the byte limit stays RCVBUF's (below).
            try dg.setInt(fd, sys.SOL.SOCKET, sys.SO.SNDBUF, 256 * 1024);
            try dg.os_net.enableDestinationAddress(fd);
            try dg.bindTo(fd, dg.addr(0, 0, 0, 0, 0));
            try std.testing.expectEqual(Ip4{ .ip = .{ 0, 0, 0, 0 }, .port = 49152 }, try dg.local(fd));
            try std.testing.expectEqual(@as(c_int, 256 * 1024), try dg.getInt(fd, sys.SOL.SOCKET, sys.SO.RCVBUF));
            try std.testing.expectEqual(@as(c_int, 0), try dg.getInt(fd, sys.SOL.SOCKET, sys.SO.ERROR));
            const dst_opt = if (builtin.os.tag == .linux) sys.IP.PKTINFO else sys.IP.RECVDSTADDR;
            try std.testing.expectEqual(@as(c_int, 1), try dg.getInt(fd, sys.IPPROTO.IP, dst_opt));
            try dg.setInt(fd, sys.IPPROTO.IP, dst_opt, 0);
            try std.testing.expectEqual(@as(c_int, 0), try dg.getInt(fd, sys.IPPROTO.IP, dst_opt));
            try dg.setInt(fd, sys.IPPROTO.IP, dst_opt, 1);
            // It works: a datagram arrives with its destination address.
            const tx = try dg.open();
            _ = try dg.sendFrom(tx, dg.addr(127, 0, 0, 1, 49152), null, "host");
            try dg.settle();
            var buf: [8]u8 = undefined;
            try std.testing.expectEqual(@as(?[4]u8, .{ 127, 0, 0, 1 }), (try dg.recvInto(fd, &.{&buf})).info.to);
        }
    }.body);
}

test "sim: dgram options: an unsupported option returns OptionNotSupported and changes nothing" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    try dg.run(1, struct {
        fn body() !void {
            const fd = try dg.open();
            const before = try dg.getInt(fd, sys.SOL.SOCKET, sys.SO.RCVBUF);
            const unsupported = [_][2]i64{
                .{ sys.SOL.SOCKET, sys.SO.RCVLOWAT },
                .{ sys.SOL.SOCKET, sys.SO.REUSEADDR },
                .{ sys.IPPROTO.IP, sys.IP.TOS },
                .{ sys.IPPROTO.IP, sys.IP.TTL },
                .{ sys.IPPROTO.IPV6, sys.IPV6.TCLASS },
                .{ sys.SOL.SOCKET, if (builtin.os.tag.isDarwin()) darwin_so_net_service_type else sys.SO.PRIORITY },
            };
            for (unsupported) |o| {
                try std.testing.expectError(error.OptionNotSupported, dg.setInt(fd, @intCast(o[0]), @intCast(o[1]), 1));
            }
            try std.testing.expectEqual(before, try dg.getInt(fd, sys.SOL.SOCKET, sys.SO.RCVBUF));
            try dg.bindTo(fd, dg.addr(127, 0, 0, 1, 0));
            const tx = try dg.open();
            _ = try dg.sendFrom(tx, dg.addr(127, 0, 0, 1, 49152), null, "still");
            try dg.settle();
            var buf: [8]u8 = undefined;
            try std.testing.expectEqualStrings("still", buf[0..(try dg.recvInto(fd, &.{&buf})).info.len]);
        }
    }.body);
}

test "sim: dgram limits: with a large byte budget, the packet limit drops empty datagrams" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    try dg.run(1, struct {
        fn body() !void {
            const rx = try dg.open();
            const tx = try dg.open();
            try dg.setInt(rx, sys.SOL.SOCKET, sys.SO.RCVBUF, 1 << 20);
            try dg.bindTo(rx, dg.addr(127, 0, 0, 1, 0));
            try dg.sendN(tx, dg.addr(127, 0, 0, 1, 49152), rcv_packet_limit + 6, "");
            try dg.settle();
            try std.testing.expectEqual(@as(u64, rcv_packet_limit), datagramStats(rx).received);
            try std.testing.expectEqual(@as(u64, 6), datagramStats(rx).dropped_rcvbuf);
        }
    }.body);
}

test "sim: dgram limits: a small byte budget drops by bytes, raising SO_RCVBUF after open lets more in, and queued datagrams keep their bytes" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    try dg.run(1, struct {
        fn body() !void {
            const rx = try dg.open();
            const tx = try dg.open();
            // 1000 bytes: two 150-byte datagrams (406 each) fit, a third does not.
            try dg.setInt(rx, sys.SOL.SOCKET, sys.SO.RCVBUF, 1000);
            try dg.bindTo(rx, dg.addr(127, 0, 0, 1, 0));
            const to = dg.addr(127, 0, 0, 1, 49152);
            var p: [150]u8 = undefined;
            for (&p, 0..) |*b, i| b.* = @intCast(i);
            var buf: [150]u8 = undefined;
            // One datagram stays queued while six more go through, so each
            // lands 150 bytes further on: the sixth starts at 900 and
            // straddles the end of the 1000-byte ring.
            _ = try dg.sendFrom(tx, to, null, &p);
            try dg.settle();
            var k: usize = 0;
            while (k < 6) : (k += 1) {
                p[0] = if (k == 5) 'W' else 0;
                _ = try dg.sendFrom(tx, to, null, &p);
                try dg.settle();
                _ = try dg.recvInto(rx, &.{&buf});
            }
            p[0] = 'A';
            try dg.sendN(tx, to, 2, &p);
            try dg.settle();
            try std.testing.expectEqual(@as(u64, 8), datagramStats(rx).received);
            try std.testing.expectEqual(@as(u64, 1), datagramStats(rx).dropped_rcvbuf);
            try dg.setInt(rx, sys.SOL.SOCKET, sys.SO.RCVBUF, 4000);
            p[0] = 'B';
            try dg.sendN(tx, to, 3, &p);
            try dg.settle();
            try std.testing.expectEqual(@as(u64, 11), datagramStats(rx).received);
            try std.testing.expectEqual(@as(u64, 1), datagramStats(rx).dropped_rcvbuf);
            // Every queued datagram came through the re-provision whole,
            // the one that straddled the old ring's end ('W') included.
            for ("WABBB") |w| {
                const got = try dg.recvInto(rx, &.{&buf});
                try std.testing.expectEqual(@as(usize, 150), got.info.len);
                try std.testing.expectEqual(w, buf[0]);
                for (buf[1..], 1..) |b, i| try std.testing.expectEqual(@as(u8, @intCast(i)), b);
            }
            try std.testing.expectError(error.WouldBlock, dg.recvInto(rx, &.{&buf}));
        }
    }.body);
}

test "sim: dgram limits: lowering SO_RCVBUF below what is queued drops nothing queued, and drops new datagrams until the queue is under the limit" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    try dg.run(1, struct {
        fn body() !void {
            const rx = try dg.open();
            const tx = try dg.open();
            try dg.bindTo(rx, dg.addr(127, 0, 0, 1, 0));
            const to = dg.addr(127, 0, 0, 1, 49152);
            var p: [100]u8 = @splat(7);
            try dg.sendN(tx, to, 4, &p);
            try dg.settle();
            // Below the 400 payload bytes queued: the ring must keep them.
            try dg.setInt(rx, sys.SOL.SOCKET, sys.SO.RCVBUF, 380);
            try dg.sendN(tx, to, 1, &p);
            try dg.settle();
            try std.testing.expectEqual(@as(u64, 4), datagramStats(rx).received);
            try std.testing.expectEqual(@as(u64, 1), datagramStats(rx).dropped_rcvbuf);
            var buf: [100]u8 = undefined;
            var k: usize = 0;
            while (k < 4) : (k += 1) try std.testing.expectEqual(@as(usize, 100), (try dg.recvInto(rx, &.{&buf})).info.len);
            // Under the limit again: one 100-byte datagram (356) fits in 380.
            try dg.sendN(tx, to, 2, &p);
            try dg.settle();
            try std.testing.expectEqual(@as(u64, 5), datagramStats(rx).received);
            try std.testing.expectEqual(@as(u64, 2), datagramStats(rx).dropped_rcvbuf);
        }
    }.body);
}

test "sim: dgram limits: one socket's overflow does not drop or block another socket's datagrams" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    try dg.run(1, struct {
        fn body() !void {
            const small = try dg.open();
            const big = try dg.open();
            const tx = try dg.open();
            try dg.setInt(small, sys.SOL.SOCKET, sys.SO.RCVBUF, 300);
            try dg.bindTo(small, dg.addr(127, 0, 0, 1, 0));
            try dg.bindTo(big, dg.addr(127, 0, 0, 1, 0));
            var k: usize = 0;
            while (k < 20) : (k += 1) {
                _ = try dg.sendFrom(tx, dg.addr(127, 0, 0, 1, 49152), null, "x");
                _ = try dg.sendFrom(tx, dg.addr(127, 0, 0, 1, 49153), null, "y");
            }
            try dg.settle();
            try std.testing.expectEqual(@as(u64, 1), datagramStats(small).received);
            try std.testing.expectEqual(@as(u64, 19), datagramStats(small).dropped_rcvbuf);
            try std.testing.expectEqual(@as(u64, 20), datagramStats(big).received);
            try std.testing.expectEqual(@as(u64, 0), datagramStats(big).dropped_rcvbuf);
        }
    }.body);
}

test "sim: dgram limits: out of memory at open and at send is SystemResources, and the run goes on" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    try dg.run(1, struct {
        fn body() !void {
            const ok_fd = try dg.open();
            var failing = std.testing.FailingAllocator.init(std.heap.smp_allocator, .{ .fail_index = 0 });
            testSetNetAllocator(failing.allocator());
            defer testSetNetAllocator(std.heap.smp_allocator);
            try std.testing.expectError(error.SystemResources, dg.open());
            try std.testing.expectError(error.SystemResources, dg.sendFrom(ok_fd, dg.addr(127, 0, 0, 1, 9), null, "x"));
            // The second allocation of an open fails: the first is freed.
            failing = std.testing.FailingAllocator.init(std.heap.smp_allocator, .{ .fail_index = 1 });
            testSetNetAllocator(failing.allocator());
            try std.testing.expectError(error.SystemResources, dg.open());
            try std.testing.expectEqual(failing.allocated_bytes, failing.freed_bytes);
            testSetNetAllocator(std.heap.smp_allocator);
            _ = try dg.sendFrom(ok_fd, dg.addr(127, 0, 0, 1, 9), null, "x");
        }
    }.body);
}

test "sim: dgram options refuse: SO_BROADCAST" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: dgram options refuse: SO_BROADCAST", "sim: setsockopt SO_BROADCAST", struct {
        fn body() !void {
            try r3.up();
            const fd = try dg.open();
            try dg.setInt(fd, sys.SOL.SOCKET, sys.SO.BROADCAST, 1);
        }
    }.body);
}

test "sim: dgram options refuse: SO_REUSEPORT" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: dgram options refuse: SO_REUSEPORT", "sim: setsockopt SO_REUSEPORT", struct {
        fn body() !void {
            try r3.up();
            const fd = try dg.open();
            try dg.setInt(fd, sys.SOL.SOCKET, sys.SO.REUSEPORT, 1);
        }
    }.body);
}

test "sim: dgram options refuse: a multicast membership" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: dgram options refuse: a multicast membership", "sim: setsockopt IP_ADD_MEMBERSHIP", struct {
        fn body() !void {
            try r3.up();
            const fd = try dg.open();
            var mreq: [8]u8 = @splat(0);
            try dg.os_net.setsockopt(fd, sys.IPPROTO.IP, sys.IP.ADD_MEMBERSHIP, &mreq);
        }
    }.body);
}

test "sim: dgram options refuse: a TCP option on a datagram socket" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: dgram options refuse: a TCP option on a datagram socket", "sim: setsockopt a TCP option", struct {
        fn body() !void {
            try r3.up();
            const fd = try dg.open();
            try dg.setInt(fd, sys.IPPROTO.TCP, 1, 1);
        }
    }.body);
}

test "sim: dgram options refuse: SO_RCVBUF of zero" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: dgram options refuse: SO_RCVBUF of zero", "sim: SO_RCVBUF 0 on sim fd 100032: the byte limit must be positive", struct {
        fn body() !void {
            try r3.up();
            const fd = try dg.open();
            try dg.setInt(fd, sys.SOL.SOCKET, sys.SO.RCVBUF, 0);
        }
    }.body);
}

test "sim: dgram options refuse: a read of an option that is not simulated" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: dgram options refuse: a read of an option that is not simulated", "sim: getsockopt a SOL_SOCKET option", struct {
        fn body() !void {
            try r3.up();
            const fd = try dg.open();
            _ = try dg.getInt(fd, sys.SOL.SOCKET, sys.SO.SNDBUF);
        }
    }.body);
}

test "sim: dgram options refuse: an option on a closed socket" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: dgram options refuse: an option on a closed socket", "sim: setsockopt on sim fd 100032, which is closed", struct {
        fn body() !void {
            try r3.up();
            const fd = try dg.open();
            dg.os_net.close(fd);
            try dg.setInt(fd, sys.SOL.SOCKET, sys.SO.RCVBUF, 1000);
        }
    }.body);
}

test "sim: dgram options refuse: out of memory re-provisioning the receive queue" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: dgram options refuse: out of memory re-provisioning the receive queue", "sim: out of memory for a 300000-byte receive queue", struct {
        fn body() !void {
            try r3.up();
            const fd = try dg.open();
            var failing = std.testing.FailingAllocator.init(std.heap.smp_allocator, .{ .fail_index = 0 });
            testSetNetAllocator(failing.allocator());
            try dg.setInt(fd, sys.SOL.SOCKET, sys.SO.RCVBUF, 300000);
        }
    }.body);
}

test "sim: dgram refuses: a socket setsockopt from another thread" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: dgram refuses: a socket setsockopt from another thread", "sim: sim socket setsockopt" ++ other_thread_msg, struct {
        fn body() !void {
            try r3.up();
            dg_thread.fd = try dg.open();
            try r3.other(struct {
                fn f() void {
                    dg.setInt(dg_thread.fd, sys.SOL.SOCKET, sys.SO.RCVBUF, 1000) catch {};
                }
            }.f);
        }
    }.body);
}

// ---- Phase 1, slice 4: datagrams in the trace, and the wire digest (section 7) ----

// Measured at ec6ec37, before datagrams entered the trace or the state digest
// (3 of 3 runs gave these). A run that opens no datagram socket must keep
// them: promise 1 of design section 7.
test "sim: dgram trace: a run with no datagram socket keeps the trace and state digest it had before" {
    if (comptime !compiled_in) return error.SkipZigTest;
    const a = runWakeOrderProbe(1, .a);
    const c = runCancelOrderProbe(3, .b);
    try std.testing.expectEqual(@as(u64, 0x4095dc9a3d186e85), a.trace);
    try std.testing.expectEqual(@as(u64, 0x90bbdaa79a2b64f7), a.state);
    try std.testing.expectEqual(@as(u64, 0xabf05a246f5166cb), c.trace);
    try std.testing.expectEqual(@as(u64, 0x77327e95ccaf1e7a), c.state);
}

const dg_wire = struct {
    var flip = false;
    var transcript: [256]u8 = undefined;
    var transcript_len: usize = 0;
    const Out = struct { wire: u64, trace: u64, state: u64 };
    var out: Out = undefined;

    /// Three senders, four datagrams each, to one wildcard receiver, sent
    /// at one time so their flows interleave by the seed. One ev receive
    /// waits first and takes the first to land. The receiver writes its own
    /// transcript: for each datagram, the source port's low byte, the
    /// length, and the bytes.
    fn body() !void {
        const rx = try dg.open();
        try dg.bindTo(rx, dg.addr(0, 0, 0, 0, 7000));
        try dg.os_net.enableDestinationAddress(rx);
        var senders: [3]i32 = undefined;
        for (&senders) |*fd| fd.* = try dg.open();
        transcript_len = 0;
        var h = try dg.runtime.spawn(evFirst, .{rx});
        try dg.rt_mod.sleep(.fromMilliseconds(1));
        for (0..4) |k| {
            for (senders, 0..) |fd, i| {
                var p: [6]u8 = .{ 'd', @intCast('0' + i), @intCast('0' + k), 'x', 'y', 'z' };
                if (flip and i == 1 and k == 2) p[4] ^= 1;
                _ = try dg.sendFrom(fd, dg.addr(127, 0, 0, @intCast(1 + i), 7000), null, p[0 .. 3 + k]);
            }
        }
        try h.join();
        try dg.settle();
        var buf: [16]u8 = undefined;
        while (dg.recvInto(rx, &.{&buf})) |got| {
            note(@truncate(got.from.port), buf[0..got.info.len]);
        } else |err| if (err != error.WouldBlock) return err;
        out = .{ .wire = wireDigest(), .trace = traceHash(), .state = stateDigest() };
    }

    fn evFirst(rx: i32) !void {
        var buf: [16]u8 = undefined;
        const got = try dg.evRecvFrom(rx, &buf);
        note(@truncate(got.from.port), buf[0..got.len]);
    }

    fn note(port_low: u8, bytes: []const u8) void {
        transcript[transcript_len] = port_low;
        transcript[transcript_len + 1] = @intCast(bytes.len);
        @memcpy(transcript[transcript_len + 2 ..][0..bytes.len], bytes);
        transcript_len += 2 + bytes.len;
    }

    fn run(seed_value: u64, flip_byte: bool) !Out {
        flip = flip_byte;
        try dg.run(seed_value, body);
        return out;
    }
};

test "sim: dgram trace: one changed payload byte gives a different wire digest, with the seed, lengths and endpoints the same" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    const base = try dg_wire.run(5, false);
    var base_buf: [256]u8 = undefined;
    const base_transcript = base_buf[0..dg_wire.transcript_len];
    @memcpy(base_transcript, dg_wire.transcript[0..dg_wire.transcript_len]);
    // 12 datagrams, each its source byte, its length and 3 to 6 bytes.
    try std.testing.expectEqual(@as(usize, 78), base_transcript.len);
    const again = try dg_wire.run(5, false);
    try std.testing.expectEqual(base, again);
    const flipped = try dg_wire.run(5, true);
    try std.testing.expect(base.wire != flipped.wire);
    // The state digest covers the datagram table, the wire digest with it.
    try std.testing.expect(base.state != flipped.state);
    // The trace and the transcript's shape do not see payload bytes: the
    // digest is what catches the change.
    try std.testing.expectEqual(base.trace, flipped.trace);
    try std.testing.expectEqual(base_transcript.len, dg_wire.transcript_len);
    try std.testing.expect(!std.mem.eql(u8, base_transcript, dg_wire.transcript[0..dg_wire.transcript_len]));
}

test "sim: dgram trace: a landing is a trace event: interleaves that differ give different traces, and the same seed the same" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    const Seen = struct { order: [6]u8, trace: u64 };
    var seen: [12]Seen = undefined;
    var seed_value: u64 = 1;
    while (seed_value <= 12) : (seed_value += 1) {
        const o = try dg_order.orderFor(seed_value);
        const t = traceHash();
        _ = try dg_order.orderFor(seed_value);
        try std.testing.expectEqual(t, traceHash());
        seen[seed_value - 1] = .{ .order = o, .trace = t };
    }
    var pairs_with_different_orders: usize = 0;
    for (seen, 0..) |a, i| {
        for (seen[i + 1 ..]) |b| {
            if (std.mem.eql(u8, &a.order, &b.order)) continue;
            pairs_with_different_orders += 1;
            try std.testing.expect(a.trace != b.trace);
        }
    }
    try std.testing.expect(pairs_with_different_orders > 0);
}

test "sim: dgram trace: ten fresh processes with the heap moved give one transcript, wire digest, trace and state digest" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    const name = "sim: dgram trace: ten fresh processes with the heap moved give one transcript, wire digest, trace and state digest";
    if (isDeathChild()) {
        noCoreDump();
        const shift = std.fmt.parseInt(usize, std.mem.span(std.c.getenv("ZIO_SIM_HEAP_SHIFT") orelse "0"), 10) catch 0;
        if (shift > 0) _ = try std.heap.page_allocator.alloc(u8, shift * 4096 + 7);
        const o = try dg_wire.run(9, false);
        var line_buf: [1024]u8 = undefined;
        var w: std.Io.Writer = .fixed(&line_buf);
        try w.print("sim-dgram: wire={x:0>16} trace={x:0>16} state={x:0>16} transcript=", .{ o.wire, o.trace, o.state });
        for (dg_wire.transcript[0..dg_wire.transcript_len]) |b| try w.print("{x:0>2}", .{b});
        try w.writeByte('\n');
        _ = std.c.write(2, w.buffered().ptr, w.buffered().len);
        childDone(name);
        return;
    }
    const io = std.testing.io;
    const gpa = std.testing.allocator;
    var exe_buf: [std.fs.max_path_bytes]u8 = undefined;
    const exe_len = try std.process.executablePath(io, &exe_buf);
    var first: ?[]u8 = null;
    defer if (first) |f| gpa.free(f);
    var k: usize = 1;
    while (k <= 10) : (k += 1) {
        var env: std.process.Environ.Map = .init(gpa);
        defer env.deinit();
        if (std.c.getenv("PATH")) |path| try env.put("PATH", std.mem.span(path));
        try env.put("TEST_FILTER", name);
        try env.put(death_child_env, "1");
        var shift_buf: [16]u8 = undefined;
        try env.put("ZIO_SIM_HEAP_SHIFT", try std.fmt.bufPrint(&shift_buf, "{d}", .{k * 13}));
        const res = try std.process.run(gpa, io, .{
            .argv = &.{exe_buf[0..exe_len]},
            .environ_map = &env,
            .timeout = .{ .duration = .{ .raw = .fromSeconds(30), .clock = .awake } },
        });
        defer gpa.free(res.stdout);
        defer gpa.free(res.stderr);
        try checkDoneMarkers(name, res.stderr);
        const at = std.mem.indexOf(u8, res.stderr, "sim-dgram: ") orelse return error.NoLine;
        const end_at = std.mem.indexOfScalarPos(u8, res.stderr, at, '\n') orelse return error.NoLine;
        const got = res.stderr[at..end_at];
        if (first) |f| {
            if (!std.mem.eql(u8, f, got)) {
                std.debug.print("child {d}: {s}\nchild 1: {s}\n", .{ k, got, f });
                return error.ChildrenDiffer;
            }
        } else first = try gpa.dupe(u8, got);
    }
    // The transcript holds all 12 datagrams: 78 bytes, 156 hex digits.
    const t_at = (std.mem.indexOf(u8, first.?, "transcript=") orelse return error.NoTranscript) + "transcript=".len;
    try std.testing.expectEqual(@as(usize, 156), first.?.len - t_at);
    std.debug.print("10 children: {s}\n", .{first.?[0..@min(first.?.len, 90)]});
}

const dg_inflight = struct {
    const Variant = enum { base, other_port, other_ip, other_source, queue_swapped };
    var variant: Variant = .base;
    var state: u64 = undefined;
    var wire_d: u64 = undefined;
    var trace: u64 = undefined;

    /// A wildcard receiver on 7000 and a sender on wildcard:7100. Two
    /// datagrams land in the receiver's queue first, then one more is sent
    /// and the digests are read while it is still on the wire. Each variant
    /// changes one thing and keeps every length, time and payload byte.
    fn body() !void {
        const rx = try dg.open();
        try dg.bindTo(rx, dg.addr(0, 0, 0, 0, 7000));
        _ = try dg.open(); // a second socket on 7001 would hide a bad port
        const tx = try dg.open();
        try dg.bindTo(tx, dg.addr(0, 0, 0, 0, 7100));
        const first: []const u8 = if (variant == .queue_swapped) "b" else "a";
        const second: []const u8 = if (variant == .queue_swapped) "a" else "b";
        _ = try dg.sendFrom(tx, dg.addr(127, 0, 0, 1, 7000), null, first);
        _ = try dg.sendFrom(tx, dg.addr(127, 0, 0, 1, 7000), null, second);
        try dg.settle();
        const to = switch (variant) {
            .other_port => dg.addr(127, 0, 0, 1, 7001),
            .other_ip => dg.addr(127, 0, 0, 2, 7000),
            else => dg.addr(127, 0, 0, 1, 7000),
        };
        const from: ?[4]u8 = if (variant == .other_source) .{ 127, 0, 0, 3 } else null;
        _ = try dg.sendFrom(tx, to, from, "x");
        state = stateDigest();
        wire_d = wireDigest();
        trace = traceHash();
        try dg.settle();
    }

    fn run(v: Variant) !struct { state: u64, wire: u64, trace: u64 } {
        variant = v;
        try dg.run(4, body);
        return .{ .state = state, .wire = wire_d, .trace = trace };
    }
};

/// Round 2 of the outside review, F4: two receivers, an op parked on each.
/// The variants swap which op parks on which socket, or make the first op a
/// poll instead of a receive. The live-id list and each socket's "parked"
/// bit are the same in all of them.
const dg_parked_digest = struct {
    const Variant = enum { base, swapped, first_is_poll };
    var variant: Variant = .base;
    var fds: [2]i32 = undefined;
    var state: u64 = 0;

    fn first() void {
        const fd = if (variant == .swapped) fds[1] else fds[0];
        if (variant == .first_is_poll) {
            dg.evPoll(fd, .recv) catch {};
        } else {
            var buf: [4]u8 = undefined;
            _ = dg.evRecvFrom(fd, &buf) catch {};
        }
    }

    fn second() void {
        const fd = if (variant == .swapped) fds[0] else fds[1];
        var buf: [4]u8 = undefined;
        _ = dg.evRecvFrom(fd, &buf) catch {};
    }

    fn body() !void {
        for (&fds, 0..) |*fd, i| {
            fd.* = try dg.open();
            try dg.bindTo(fd.*, dg.addr(127, 0, 0, 1, @intCast(7000 + i)));
        }
        var a = try dg.runtime.spawn(first, .{});
        var b = try dg.runtime.spawn(second, .{});
        try dg.rt_mod.sleep(.fromMilliseconds(1));
        state = stateDigest();
        for (fds) |fd| dg.os_net.close(fd);
        a.join();
        b.join();
    }

    fn run(v: Variant) !u64 {
        variant = v;
        try dg.run(2, body);
        return state;
    }
};

test "sim: dgram trace: the state digest sees which op is parked on which socket, and its kind" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    const T = dg_parked_digest;
    const base = try T.run(.base);
    try std.testing.expectEqual(base, try T.run(.base));
    var blind: usize = 0;
    for ([_]T.Variant{ .swapped, .first_is_poll }) |v| {
        const d = try T.run(v);
        if (d == base) {
            std.debug.print("variant {t}: state digest {x:0>16}, the same as the base run's\n", .{ v, d });
            blind += 1;
        }
    }
    if (blind != 0) return error.StateDigestBlind;
}

test "sim: dgram trace: the state digest sees where a datagram still on the wire goes and where it came from" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    const base = try dg_inflight.run(.base);
    try std.testing.expectEqual(base, try dg_inflight.run(.base));
    var blind: usize = 0;
    inline for (.{ .other_port, .other_ip, .other_source, .queue_swapped }) |v| {
        const o = try dg_inflight.run(v);
        // Nothing has landed differently yet: the wire digest and the trace
        // cannot tell the runs apart, so the state digest must.
        if (v != .queue_swapped) {
            try std.testing.expectEqual(base.wire, o.wire);
            try std.testing.expectEqual(base.trace, o.trace);
        }
        if (base.state == o.state) {
            std.debug.print("variant {s}: state digest {x:0>16}, the same as the base run's\n", .{ @tagName(v), o.state });
            blind += 1;
        }
    }
    if (blind != 0) return error.StateDigestBlind;
    std.debug.print("base run, one datagram on the wire: state digest {x:0>16}\n", .{base.state});
}

test {
    _ = @import("sim_exits_test.zig");
    _ = @import("sim_op_model_test.zig");
}

// ---- Phase 1: the native-exit refusals, one test per class of exit ----

test "sim: pipe refuses: a send with MSG_OOB" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: pipe refuses: a send with MSG_OOB", "sim: send flag oob on sim pipe fd 100001 is not simulated", struct {
        fn body() !void {
            try r3.up();
            const p = pipePair();
            var iov = [1]dg.os_net.iovec_const{dg.os_net.iovecConstFromSlice("x")};
            var op = dg.ev.NetSend.init(p[1], .{ .iovecs = &iov }, .{ .oob = true });
            try dg.io_common.waitForIo(&op.c);
            _ = try op.getResult();
        }
    }.body);
}

test "sim: pipe refuses: a send with MSG_DONTWAIT" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: pipe refuses: a send with MSG_DONTWAIT", "sim: send flag dontwait on sim pipe fd 100001 is not simulated", struct {
        fn body() !void {
            try r3.up();
            const p = pipePair();
            var iov = [1]dg.os_net.iovec_const{dg.os_net.iovecConstFromSlice("x")};
            var op = dg.ev.NetSend.init(p[1], .{ .iovecs = &iov }, .{ .dontwait = true });
            try dg.io_common.waitForIo(&op.c);
            _ = try op.getResult();
        }
    }.body);
}

test "sim: pipe refuses: a send without MSG_NOSIGNAL" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: pipe refuses: a send without MSG_NOSIGNAL", "sim: send flag no_signal on sim pipe fd 100001 is not simulated", struct {
        fn body() !void {
            try r3.up();
            const p = pipePair();
            var iov = [1]dg.os_net.iovec_const{dg.os_net.iovecConstFromSlice("x")};
            var op = dg.ev.NetSend.init(p[1], .{ .iovecs = &iov }, .{ .no_signal = false });
            try dg.io_common.waitForIo(&op.c);
            _ = try op.getResult();
        }
    }.body);
}

test "sim: pipe refuses: a receive with MSG_PEEK" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: pipe refuses: a receive with MSG_PEEK", "sim: receive flag peek on sim pipe fd 100000 is not simulated", struct {
        fn body() !void {
            try r3.up();
            const p = pipePair();
            {
                var iov = [1]dg.os_net.iovec_const{dg.os_net.iovecConstFromSlice("x")};
                var op = dg.ev.NetSend.init(p[1], .{ .iovecs = &iov }, .{});
                try dg.io_common.waitForIo(&op.c);
                _ = try op.getResult();
            }
            var buf: [4]u8 = undefined;
            var riov: [1]dg.os_net.iovec = undefined;
            var op = dg.ev.NetRecv.init(p[0], .fromSlice(&buf, &riov), .{ .peek = true });
            try dg.io_common.waitForIo(&op.c);
            std.debug.print("received {d} bytes\n", .{try op.getResult()});
        }
    }.body);
}

test "sim: pipe refuses: a receive with MSG_TRUNC" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: pipe refuses: a receive with MSG_TRUNC", "sim: receive flag trunc on sim pipe fd 100000 is not simulated", struct {
        fn body() !void {
            try r3.up();
            const p = pipePair();
            {
                var iov = [1]dg.os_net.iovec_const{dg.os_net.iovecConstFromSlice("x")};
                var op = dg.ev.NetSend.init(p[1], .{ .iovecs = &iov }, .{});
                try dg.io_common.waitForIo(&op.c);
                _ = try op.getResult();
            }
            var buf: [4]u8 = undefined;
            var riov: [1]dg.os_net.iovec = undefined;
            var op = dg.ev.NetRecv.init(p[0], .fromSlice(&buf, &riov), .{ .trunc = true });
            try dg.io_common.waitForIo(&op.c);
            std.debug.print("received {d} bytes\n", .{try op.getResult()});
        }
    }.body);
}

// Round 2 of the outside review, F6: a real socket made before the run
// is not a sim fd, so the layer's sendmsg takes the native path; an
// SCM_RIGHTS message in it must not carry a sim fd to the kernel.
const scm_probe = struct {
    var pair: [2]c_int = .{ -1, -1 };
    fn body() !void {
        const p = pipePair();
        const sc = std.posix.system;
        var control: [64]u8 align(@alignOf(sc.cmsghdr)) = @splat(0);
        const len = dg.os_net.cmsg_data_offset + @sizeOf(i32);
        const hdr: sc.cmsghdr = .{ .len = @intCast(len), .level = sc.SOL.SOCKET, .type = sc.SCM.RIGHTS };
        @memcpy(control[0..@sizeOf(sc.cmsghdr)], std.mem.asBytes(&hdr));
        std.mem.writeInt(i32, control[dg.os_net.cmsg_data_offset..][0..4], p[0], builtin.cpu.arch.endian());
        var iov = [1]dg.os_net.iovec_const{dg.os_net.iovecConstFromSlice("x")};
        const n = dg.os_net.sendmsg(pair[0], &iov, .{}, null, 0, control[0..std.mem.alignForward(usize, len, dg.os_net.cmsg_align)]);
        std.debug.print("sendmsg on the real socket returned {any}\n", .{n});
    }
};

test "sim: exits refuse: a sim fd in an SCM_RIGHTS message on a real socket" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    if (comptime !compiled_in) return error.SkipZigTest;
    const name = "sim: exits refuse: a sim fd in an SCM_RIGHTS message on a real socket";
    if (!isDeathChild()) return expectChildPanics(name, "sim: sim fd 100000 reached the native exit os/net.zig sendmsg");
    noCoreDump();
    if (std.c.socketpair(std.c.AF.UNIX, std.c.SOCK.DGRAM, 0, &scm_probe.pair) != 0) return error.SocketPair;
    try dg.run(1, scm_probe.body);
}

test "sim: exits refuse: a read of a sim fd" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: exits refuse: a read of a sim fd", "sim: sim fd 100000 reached the native exit os/fs.zig read", struct {
        fn body() !void {
            try r3.up();
            const fd = pipePair()[0];
            var buf: [4]u8 = undefined;
            _ = try @import("os/fs.zig").read(fd, &buf);
        }
    }.body);
}

test "sim: exits refuse: a socket call on a sim pipe fd" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: exits refuse: a socket call on a sim pipe fd", "sim: sim fd 100000 reached the native exit os/net.zig listen", struct {
        fn body() !void {
            try r3.up();
            const fd = pipePair()[0];
            try @import("os/net.zig").listen(fd, 8);
        }
    }.body);
}

test "sim: exits refuse: fcntl on a sim fd" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: exits refuse: fcntl on a sim fd", "sim: sim fd 100000 reached the native exit os/posix.zig setNonblocking", struct {
        fn body() !void {
            try r3.up();
            const fd = pipePair()[0];
            try @import("os/posix.zig").setNonblocking(fd);
        }
    }.body);
}

test "sim: exits refuse: an ioctl on a sim fd" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: exits refuse: an ioctl on a sim fd", "sim: sim fd 100000 reached the native exit os/fs.zig isTty", struct {
        fn body() !void {
            try r3.up();
            const fd = pipePair()[0];
            _ = @import("os/fs.zig").isTty(fd);
        }
    }.body);
}

test "sim: exits refuse: an ioctl through the public fs.ioctl alias" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: exits refuse: an ioctl through the public fs.ioctl alias", "sim: sim fd 100000 reached the native exit os/fs.zig ioctlPosix", struct {
        fn body() !void {
            try r3.up();
            const fd = pipePair()[0];
            _ = @import("os/fs.zig").ioctl(fd, 0, null);
        }
    }.body);
}

test "sim: exits refuse: epoll_pwait2 on a sim fd" {
    if (comptime builtin.os.tag != .linux) return error.SkipZigTest;
    try refusal("sim: exits refuse: epoll_pwait2 on a sim fd", "sim: sim fd 100000 reached the native exit os/linux.zig epoll_pwait2", struct {
        fn body() !void {
            try r3.up();
            const fd = pipePair()[0];
            var evs: [1]std.os.linux.epoll_event = undefined;
            _ = @import("os/linux.zig").epoll_pwait2(fd, &evs, 1, null);
        }
    }.body);
}

test "sim: exits refuse: a poll set of sim pipe fds" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: exits refuse: a poll set of sim pipe fds", "sim: poll on sim fd 100000, which is not a datagram socket", struct {
        fn body() !void {
            try r3.up();
            const fd = pipePair()[0];
            var fds = [1]dg.os_net.pollfd{.{ .fd = fd, .events = dg.os_net.POLL.IN, .revents = 0 }};
            _ = try dg.os_net.poll(&fds, 0);
        }
    }.body);
}

test "sim: exits refuse: a file close of a sim fd" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: exits refuse: a file close of a sim fd", "sim: sim fd 100000 reached the native exit os/fs.zig close", struct {
        fn body() !void {
            try r3.up();
            const fd = pipePair()[0];
            try @import("os/fs.zig").close(fd);
        }
    }.body);
}

test "sim: exits refuse: a posix close of a sim fd" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: exits refuse: a posix close of a sim fd", "sim: sim fd 100000 reached the native exit os/posix.zig close", struct {
        fn body() !void {
            try r3.up();
            const fd = pipePair()[0];
            @import("os/posix.zig").close(fd);
        }
    }.body);
}

test "sim: exits refuse: a socket close of a sim pipe fd" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: exits refuse: a socket close of a sim pipe fd", "sim: sim fd 100000 reached the native exit os/net.zig close", struct {
        fn body() !void {
            try r3.up();
            const fd = pipePair()[0];
            dg.os_net.close(fd);
        }
    }.body);
}

const exits_cb = struct {
    const Timer = @import("ev/completion.zig").Timer;
    const Loop = @import("ev/loop.zig").Loop;
    const Completion = @import("ev/completion.zig").Completion;
    var timer: Timer = undefined;
    var fd: i32 = 0;

    /// A loop callback has no task, so a file close there takes the
    /// blocking dispatch (`ev/blocking.zig` `executeBlocking`).
    fn onTimer(_: *Loop, _: *Completion) void {
        @import("fs.zig").File.fromFd(fd).close();
    }
};

test "sim: exits refuse: File.fromFd(sim fd).close() in a loop callback, through the blocking dispatch" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: exits refuse: File.fromFd(sim fd).close() in a loop callback, through the blocking dispatch", "sim: sim fd 100000 reached the native exit os/fs.zig close", struct {
        fn body() !void {
            try r3.up();
            exits_cb.fd = pipePair()[0];
            exits_cb.timer = exits_cb.Timer.init(.{ .duration = .fromMilliseconds(1) });
            exits_cb.timer.c.callback = exits_cb.onTimer;
            r3.rt.main_executor.loop.add(&exits_cb.timer.c);
            try r3.rt.sleep(.fromMilliseconds(5));
        }
    }.body);
}
