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
};

const TaskIdEnt = struct { ptr: usize, id: u32 };

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
    task_id_len = 0;
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
        std.posix.setrlimit(.NOFILE, .{ .cur = fd_base, .max = rl.max }) catch |err|
            panic("sim: fd namespace: cannot lower the soft RLIMIT_NOFILE to {d}: {t}", .{ fd_base, err });
        saved_nofile_soft = rl.cur;
    }
    checkFdNamespace();
    var n: i32 = fd_base;
    while (n < fd_base + max_ends) : (n += 1) {
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
}

pub fn end() void {
    refuseOtherThread("sim.end");
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

// ---- Refusals that end the run (t-4085) ----
//
// Each test runs itself again in a child process of this test binary
// (test_runner.zig selects one test by TEST_FILTER), with `death_child_env`
// set. The child takes the path that must panic; the parent checks that it
// died of SIGABRT with the expected message on stderr. A child that returns
// instead exits 0, and a child that runs on forever hits the timeout: both
// fail the parent. The child sets RLIMIT_CORE to 0 first, so no core file.

const death_child_env = "ZIO_SIM_DEATH_CHILD";

fn isDeathChild() bool {
    return std.c.getenv(death_child_env) != null;
}

fn noCoreDump() void {
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
fn childDone(comptime test_name: []const u8) void {
    const line = done_marker ++ test_name ++ "\n";
    if (comptime builtin.link_libc and builtin.os.tag != .windows) {
        _ = std.c.write(2, line.ptr, line.len);
    } else std.debug.print("{s}", .{line});
}

fn checkDoneMarkers(test_name: []const u8, stderr: []const u8) error{ ChildRanNoCase, ChildRanSeveralCases, ChildRanOtherCase }!void {
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

    /// Whether this process could hold a real descriptor at a sim fd number.
    /// Under `zig build test` on macOS the hard limit was 10240 (round 4), so
    /// the kernel cannot reach the sim range at all and these tests have
    /// nothing to set up: they skip there, and run when the test binary is
    /// started from a shell whose hard limit is higher.
    fn simRangeReachable() bool {
        const rl = std.posix.getrlimit(.NOFILE) catch return false;
        return rl.max >= fd_base + max_ends;
    }

    /// Raises this child's soft fd limit above `fd_base`, the state a
    /// process is often in (it was 1048576 in the shell this was written in).
    fn raiseSoftLimit() !void {
        const rl = try std.posix.getrlimit(.NOFILE);
        const want: std.posix.rlim_t = 200000;
        if (rl.max < want) return error.SkipZigTest;
        try std.posix.setrlimit(.NOFILE, .{ .cur = want, .max = rl.max });
    }
};

test "sim: fd namespace: begin lowers the soft fd limit to the sim base, a real descriptor cannot take a sim number, and end restores the limit" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    if (!isDeathChild() and !p1.simRangeReachable()) return error.SkipZigTest;
    if (!isDeathChild()) return expectChildSucceeds("sim: fd namespace: begin lowers the soft fd limit to the sim base, a real descriptor cannot take a sim number, and end restores the limit");
    noCoreDump();
    try p1.raiseSoftLimit();
    begin(1);
    try std.testing.expect((try std.posix.getrlimit(.NOFILE)).cur <= fd_base);
    try std.testing.expectEqual(@as(c_int, -1), std.c.dup2(0, fd_base));
    try std.testing.expectEqual(@as(c_int, -1), std.c.dup2(0, fd_base + 5));
    end();
    try std.testing.expectEqual(@as(std.posix.rlim_t, 200000), (try std.posix.getrlimit(.NOFILE)).cur);
    childDone("sim: fd namespace: begin lowers the soft fd limit to the sim base, a real descriptor cannot take a sim number, and end restores the limit");
}

test "sim: fd namespace: begin refuses when a real descriptor holds a sim fd number" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    if (!isDeathChild() and !p1.simRangeReachable()) return error.SkipZigTest;
    try refusal("sim: fd namespace: begin refuses when a real descriptor holds a sim fd number", "sim: fd namespace: real descriptor 100003 is open in the sim fd range", struct {
        fn body() !void {
            try p1.raiseSoftLimit();
            if (std.c.dup2(0, fd_base + 3) != fd_base + 3) return error.DupFailed;
            begin(1);
        }
    }.body);
}

test "sim: fd namespace: a sim fd made after the soft limit was raised again is refused" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    if (!isDeathChild() and !p1.simRangeReachable()) return error.SkipZigTest;
    try refusal("sim: fd namespace: a sim fd made after the soft limit was raised again is refused", "sim: fd namespace: the soft RLIMIT_NOFILE is 200000, above the sim fd base 100000", struct {
        fn body() !void {
            begin(1);
            try p1.raiseSoftLimit();
            _ = pipePair();
        }
    }.body);
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
