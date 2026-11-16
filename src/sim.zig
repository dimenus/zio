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
    /// A datagram reached its landing time (`land`), landed or dropped, or
    /// a fault rule dropped it at its send. `a`: the source port, and the
    /// outcome (`LandOutcome`) in bits 16-23. `b`: the destination port in
    /// the high 16 bits, the length in the low 16 (at most 65507).
    dgram = 7,
    /// A TCP entry reached its landing time (`landStream`). `a`: the
    /// `conn_id`'s low 16 bits, the kind (`TcpKind`) in bits 16-19, the
    /// direction in bit 20, the outcome (`StreamOutcome`) in bits 21-23.
    /// `b`: the sequence's low 16 bits in the high 16, the payload length in
    /// the low 16 (0 until slice 2).
    stream = 8,
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
    /// The ev receive (send) on this end that has no result yet: parked, or
    /// woken and not yet harvested. One at a time (`refuseSecondPipeWaiter`).
    unresolved_recv: ?Parked = null,
    unresolved_send: ?Parked = null,
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
    /// Any ev op on a sim stream socket: due now, like `dgram`, and checked
    /// against its socket's generation at the harvest when it has no result
    /// (`streamOpOnItsSocket`).
    stream = 6,
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

/// The loop group of this run's loops: claimed by the first loop that
/// submits or harvests sim I/O (`claimLoopGroup`), cleared at `begin`.
var run_loop_group: ?*const anyopaque = null;

/// A sim run has one loop group. The live table and the due list are the
/// run's, not a loop's: every loop's harvest takes every due op and finishes
/// it through its own state, so a loop of a second group would take ops
/// submitted on the first, and their completions would never reach the loop
/// that submitted them (round 4 of the outside review, finding 1; older than
/// the sim network: it happens with pipe ops at 3417a77). One Runtime has one
/// group for all its executors (`Runtime.loop_group`), and a sim runtime is
/// one or two executors with the main one; a second group comes only from a
/// second Runtime, or an `ev.Loop` of its own, in the same run. Neither
/// works today (two runtimes deadlock before a datagram reaches a task), so
/// a second group refuses here instead of scoping the tables per group: at
/// a sim Runtime's init (`Runtime.initStatic`), and at a loop's first submit
/// or harvest.
pub fn claimLoopGroup(group: *const anyopaque, comptime what: []const u8) void {
    const g = run_loop_group orelse {
        run_loop_group = group;
        return;
    };
    if (g != group) panic("sim: a sim run has one loop group: " ++ what ++ " (a second Runtime, or an ev.Loop of its own, in one run); a second group's harvest would take the first group's ops and finish them on the wrong loop", .{});
}

const LiveOp = struct {
    c: *anyopaque,
    id: u32,
    /// For an ev op on a sim datagram socket: the socket slot's generation
    /// when the op was submitted (`dgramOpStart`). 0 for every other op.
    sock_gen: u32 = 0,
    /// An ev op on a sim stream socket (`streamOpStart`): the harvest
    /// checks it with `streamOpOnItsSocket`, and `end`'s teardown check
    /// lists it if it is still live.
    on_stream: bool = false,
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
    resetInstant();
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
    run_loop_group = null;
    ends = @splat(.{});
    due_len = 0;
    next_io_id = 0;
    live_len = 0;
    resetNet();
}

pub fn end() void {
    refuseOtherThread("sim.end");
    // Open datagram sockets are listed first, so a run that also leaves
    // stream objects shows both before either check panics.
    const dgrams_open = dgramTeardownList();
    if (stream_used) streamTeardownCheck();
    if (dgrams_open != 0) panic("sim: datagram teardown: {d} datagram sockets are open at sim.end (listed above): close every datagram socket before sim.end", .{dgrams_open});
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

// ---- A spin at one virtual instant (t-4133) ----
//
// The rule: a run whose loops pass through `Loop.poll` more than
// `instant_poll_limit` times while `clock_ns` stands still panics, and the
// line names the spin. Under sim, work takes no virtual time. A task that
// keeps arming a timer for a moment already past is woken at once, at the
// same instant, and a task that yields until the clock moves never sees it
// move; either loops at one instant for ever, at full CPU and with no
// message. On a real clock the same loop ends when real time passes, so
// under sim it is a hang that only a wall-time alarm outside the run sees.
//
// What is counted: each pass of `Loop.poll` (`notePoll`, first in it once
// the loop is known not done). Every scheduler turn of every executor goes
// through it: a full wait, the `.zero` poll after a spent tick budget, each
// loop of `simPollAll`, and a bare `ev.Loop` test's own polls. The count
// starts again when the clock has moved since the last pass. Trace events
// were measured and rejected: a main task that only yields makes no task
// switch and no wake, so they stop while the polls go on, and a burst of
// datagram landings at one instant gives hundreds of them in a correct run.
//
// The bound is a margin over a measurement, not a count of anything a
// program asks for: the commit that added it has the largest count seen in
// a correct run and the wall time a spin takes to reach the bound.
// `setInstantPollLimit` changes it for one run.
//
// The check reads the clock and counts. It draws nothing and emits nothing,
// so a run that stays under the bound keeps every trace and digest.

/// Loop polls allowed at one virtual instant before the run panics (t-4133).
pub const default_instant_poll_limit: u64 = 100_000;
var instant_poll_limit: u64 = default_instant_poll_limit;
/// The virtual time the counts below belong to.
var instant_ns: u64 = 0;
var instant_polls: u64 = 0;
var instant_timer_fires: u64 = 0;
/// For the panic line: the trace id of the task that last ran or was woken,
/// and the last timer that fired (its clock and deadline, and when).
var last_task_trace_id: u32 = 0;
var last_timer: ?struct { clock: u8, deadline_ns: u64, fired_ns: u64 } = null;

fn resetInstant() void {
    instant_poll_limit = default_instant_poll_limit;
    instant_ns = 0;
    instant_polls = 0;
    instant_timer_fires = 0;
    last_task_trace_id = 0;
    last_timer = null;
}

fn rollInstant() void {
    if (clock_ns == instant_ns) return;
    instant_ns = clock_ns;
    instant_polls = 0;
    instant_timer_fires = 0;
}

/// Sets the bound of the rule above for this run; `begin` restores
/// `default_instant_poll_limit`. A run that does a great deal of zero-time
/// work at one instant raises it; a test that wants a spin caught early
/// lowers it. 0 is refused: no run can poll less than once.
pub fn setInstantPollLimit(n: u64) void {
    if (comptime !compiled_in) return;
    refuseOtherThread("sim.setInstantPollLimit");
    if (!begun) panic("sim: setInstantPollLimit before begin", .{});
    if (n == 0) panic("sim: setInstantPollLimit(0): a run polls at least once", .{});
    instant_poll_limit = n;
}

/// Called by `Loop.poll` under sim, once per pass (the rule above).
pub fn notePoll() void {
    if (comptime !compiled_in) return;
    if (!begun) return;
    rollInstant();
    instant_polls += 1;
    if (instant_polls > instant_poll_limit) spinPanic();
}

/// Called by `Loop.checkTimers` under sim for each timer it fires.
pub fn noteTimerFire(clock_idx: u8, deadline_ns: u64) void {
    if (comptime !compiled_in) return;
    if (!begun) return;
    rollInstant();
    instant_timer_fires += 1;
    last_timer = .{ .clock = clock_idx, .deadline_ns = deadline_ns, .fired_ns = clock_ns };
}

fn spinPanic() noreturn {
    var task_buf: [64]u8 = undefined;
    // Trace id 0: no task has been switched to or woken yet, which is a
    // main task that only yields.
    const task = if (last_task_trace_id == 0)
        "no task has been switched to or woken in this run (the main task?)"
    else
        std.fmt.bufPrint(&task_buf, "the last task run or woken has trace id {d}", .{last_task_trace_id}) catch unreachable;
    const head = "sim: {d} loop polls at virtual time {d} ns with the clock not moving (the limit is {d}, sim.setInstantPollLimit): a task spins without virtual time passing (t-4133); {s}; ";
    const lt = last_timer orelse panic(head ++ "no timer has fired in this run", .{ instant_polls, clock_ns, instant_poll_limit, task });
    if (lt.fired_ns != clock_ns) panic(head ++ "no timer fired at this instant (the last fired at {d} ns)", .{ instant_polls, clock_ns, instant_poll_limit, task, lt.fired_ns });
    panic(head ++ "{d} timers fired at this instant, the last with deadline {d} ns on clock {d}", .{ instant_polls, clock_ns, instant_poll_limit, task, instant_timer_fires, lt.deadline_ns, lt.clock });
}

pub fn emit(kind: EventKind, a: u32, b: u32) void {
    if (comptime !compiled_in) return;
    if (!begun) return;
    if (kind == .task_switch or kind == .wake) last_task_trace_id = a;
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
    if (rule_history_len != 0) digestFaults(&h);
    if (stream_used) digestStreams(&h);
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
        std.mem.writeInt(u64, e[8..16], w.seq | (@as(u64, @intFromBool(w.copy)) << 63), .little);
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
    // Only for a run that connected a socket (a run without keeps its
    // digests): the ICMP count, and each socket's peer, pending error,
    // filter drops and refusals.
    if (dgram_connect_used) {
        std.mem.writeInt(u64, &b, icmp_seq, .little);
        h.update(&b);
        // The profile decides what a later call does. The ghosts need no
        // entry: each is a death the wire digest records, alive until a
        // time its record and the clock give.
        h.update(&.{if (dgram_profile) |pr| @backingInt(pr) else 0});
        for (dgrams) |s| {
            var e: [24]u8 = @splat(0);
            if (s.peer) |p| {
                e[0] = 1;
                @memcpy(e[1..5], &p.ip);
                std.mem.writeInt(u16, e[5..7], p.port, .little);
            }
            e[7] = @intFromBool(s.refused);
            std.mem.writeInt(u64, e[8..16], s.stats.dropped_not_peer, .little);
            std.mem.writeInt(u64, e[16..24], s.stats.refusals, .little);
            h.update(&e);
        }
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
            resolve(c, id);
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
            resolve(c, id);
            return id;
        }
    }
    return null;
}

/// The op (`c`, `id`) has its result or was cancelled: it no longer holds
/// its pipe end's or socket's one unresolved slot.
fn resolve(c: *anyopaque, id: u32) void {
    for (&ends) |*e| {
        if (e.unresolved_recv) |u| {
            if (u.c == c and u.id == id) e.unresolved_recv = null;
        }
        if (e.unresolved_send) |u| {
            if (u.c == c and u.id == id) e.unresolved_send = null;
        }
    }
    for (&dgrams) |*d| {
        if (d.unresolved) |u| {
            if (u.c == c and u.id == id) d.unresolved = null;
        }
    }
    for (&streams) |*t| {
        if (t.unresolved) |u| {
            if (u.c == c and u.id == id) t.unresolved = null;
        }
        if (t.unresolved_recv) |u| {
            if (u.c == c and u.id == id) t.unresolved_recv = null;
        }
        if (t.unresolved_send) |u| {
            if (u.c == c and u.id == id) t.unresolved_send = null;
        }
    }
}

/// A sim pipe end takes one unresolved ev receive and one unresolved ev
/// send at a time; a second refuses at submit, whatever it would do (take
/// bytes, park, or would-block). A kernel lets several readers or writers
/// wait on one descriptor; the sim does not model that (owner's decision,
/// round 3): an op woken and not yet harvested would otherwise find its
/// bytes taken, or its room filled, and a newer op parked in its slot.
fn refuseSecondPipeWaiter(comptime what: []const u8, fd: i32, old: Parked, parked: bool, new_id: u32) noreturn {
    panic("sim: a second ev " ++ what ++ " while one is {s}, on sim pipe fd {d} (op {d}; the new op {d}): a sim pipe end takes one unresolved ev " ++ what ++ " at a time", .{ if (parked) "parked" else "woken and not yet harvested", fd, old.id, new_id });
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
    if (kind == .stream and at != clock_ns) panic("sim: a stream op made due at {d} ns, not now ({d} ns)", .{ at, clock_ns });
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
    // The TCP wire too: it is sorted by landing time first.
    for (tcp_wire.items) |w| {
        if (w.due_at <= clock_ns) continue;
        if (min_at == null or w.due_at < min_at.?) min_at = w.due_at;
        break;
    }
    // And owed credit departures (design 17.10: sim-internal times the
    // poll's clock must stop at).
    if (stream_used) {
        if (nextOwedCredit()) |t| {
            if (min_at == null or t < min_at.?) min_at = t;
        }
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

/// The most buffers one send or receive on a sim fd takes: IOV_MAX, 1024 on
/// macOS 25.6 and Linux 6.8 (measured with `sysconf(_SC_IOV_MAX)`, 3 runs
/// each, sitting 21). Both kernels send and receive with 1024 and answer
/// EMSGSIZE for 1025. The sim refuses above the limit instead, by name: a
/// program that passes more is broken on every host, and local send errors
/// are not modelled (design section 10). ENet builds a datagram from up to
/// 65 (`ENET_BUFFER_MAXIMUM`, 1 + 2 * 32 commands).
///
/// The sim reads the caller's list in place (`bufBytes`), never copying it
/// into an array, so the limit costs no stack: a copy would take 16 bytes
/// per entry, 16 KiB at this limit.
pub const max_iovecs = 1024;

fn checkIovecs(comptime what: []const u8, fd: i32, n: usize) void {
    if (n > max_iovecs) panic("sim: " ++ what ++ " of {d} buffers on sim fd {d}: the limit is IOV_MAX, {d} (the kernel answers EMSGSIZE)", .{ n, fd, max_iovecs });
}

/// One entry of a buffer list as a slice. The sim takes a list of slices
/// (its own tests) or of the socket layer's iovecs (`base`, `len`).
inline fn bufBytes(b: anytype) BufBytes(@TypeOf(b)) {
    if (comptime @typeInfo(@TypeOf(b)) == .pointer) return b;
    return b.base[0..b.len];
}

fn BufBytes(comptime T: type) type {
    if (@typeInfo(T) == .pointer) return T;
    return if (@FieldType(T, "base") == [*]const u8) []const u8 else []u8;
}

fn totalLen(bufs: anytype) usize {
    var n: usize = 0;
    for (bufs) |b| n += bufBytes(b).len;
    return n;
}

/// Copy from `src` (gathered across its slices) into the peer's buffer.
pub fn sendBytes(fd: i32, src: anytype, send_c: *anyopaque) IoSubmit {
    return copySend(fd, src, send_c, allocOpId(send_c), true);
}

/// Harvest a capacity-woken send: copy without re-queueing this completion.
/// `id` is the submit-time id from the due entry.
pub fn harvestSend(fd: i32, src: anytype, send_c: *anyopaque, id: u32) IoSubmit {
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

fn copySend(fd: i32, src: anytype, send_c: *anyopaque, id: u32, schedule: bool) IoSubmit {
    checkIovecs("a send", fd, src.len);
    const i = endIndex(fd) orelse {
        if (schedule) pushDueKind(send_c, .send, id);
        return .bad_fd;
    };
    if (schedule) {
        if (ends[i].unresolved_send) |u| refuseSecondPipeWaiter("send", fd, u, ends[i].pending_send != null, id);
    }
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
        // At a harvest the send was woken by a drain, and only another ev
        // send on this end fills the room again, which the sim refuses
        // while this one is unresolved.
        if (!schedule) panic("sim: a woken ev send on sim pipe fd {d} found the pipe full: only another ev send fills it, and that refuses while this one is unresolved", .{fd});
        ends[i].pending_send = .{ .c = send_c, .id = id };
        ends[i].unresolved_send = ends[i].pending_send;
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
    for (src) |b| {
        const sl = bufBytes(b);
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

/// An ev receive on a sim pipe end, at its submit.
///
/// A receive with no buffer room (total length 0) is refused, before any
/// side effect (t-4096, decided yes; design 17.4 refuses the same on a
/// stream socket). The sim parked it on an empty pipe until bytes or end of
/// stream came, and gave 0 at once on a pipe holding bytes; a kernel's
/// `recv(fd, buf, 0)` was not measured, and on Linux it is expected to give
/// 0 at once in both cases. So the sim's answer could differ from the
/// kernel's, silently. No user of the sim submits one.
pub fn recvInto(fd: i32, dst: anytype, recv_c: *anyopaque, dont_wait: bool) IoSubmit {
    checkIovecs("a receive", fd, dst.len);
    if (totalLen(dst) == 0) panic("sim: a receive of 0 bytes on sim pipe fd {d} is refused (t-4096): what a kernel answers is not measured, and the sim would park it on an empty pipe", .{fd});
    const id = allocOpId(recv_c);
    const i = endIndex(fd) orelse {
        pushDueKind(recv_c, .recv, id);
        return .bad_fd;
    };
    if (ends[i].unresolved_recv) |u| refuseSecondPipeWaiter("receive", fd, u, ends[i].pending_recv != null, id);
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
        ends[i].pending_recv = .{ .c = recv_c, .id = id };
        ends[i].unresolved_recv = ends[i].pending_recv;
        return .parked;
    }
    const n = drainBuf(fd, dst);
    pushDueKind(recv_c, .recv, id);
    // It has room (above) and the pipe has bytes, so it took some.
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
pub fn drainBuf(fd: i32, dst: anytype) usize {
    const i = endIndex(fd) orelse return 0;
    var n: usize = 0;
    for (dst) |b| {
        const d = bufBytes(b);
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

/// A woken pipe receive at its harvest, not reset and not at end of stream:
/// its bytes. It has room: `recvInto` refuses one without (t-4096).
///
/// Its bytes are still there: only an ev receive takes a pipe's bytes (the
/// socket layer refuses a sim pipe fd), and a second one on this end refuses
/// at submit while this one is unresolved. Before that rule (round 3,
/// findings 2 and 3) another receive could take them, and this one parked
/// again (7c0c22e); now an empty end here is a broken invariant.
pub fn harvestRecv(fd: i32, dst: anytype) usize {
    const i = endIndex(fd) orelse panic("sim: a harvested receive on sim fd {d}, which is not a pipe end", .{fd});
    if (ends[i].buf_len == 0) panic("sim: a woken ev receive on sim pipe fd {d} found no bytes, no end of stream and no reset: only another ev receive takes bytes, and that refuses while this one is unresolved", .{fd});
    return drainForRecv(fd, dst);
}

/// For a harvested recv: drain, and wake a sender the drain made room for.
pub fn drainForRecv(fd: i32, dst: anytype) usize {
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
    for (&streams) |*t| {
        if (t.parked) |p| {
            if (p.c == c) t.parked = null;
        }
        if (t.parked_recv) |p| {
            if (p.c == c) t.parked_recv = null;
        }
        if (t.parked_send) |p| {
            if (p.c == c) t.parked_send = null;
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
    landStreamDue();
    if (stream_used) departOwedCredits();
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
    /// Datagrams addressed to this connected socket from another source:
    /// dropped at the landing (the connected-socket protocol below).
    dropped_not_peer: u64 = 0,
    /// ICMP port unreachables that reached this connected socket. Each sets
    /// the one pending error; a second before a call takes it is counted
    /// here and still leaves one error.
    refusals: u64 = 0,
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
    /// That op until it has its result: parked, or woken and not yet
    /// harvested. One at a time (`dgramRefuseSecondWaiter`).
    unresolved: ?DgramParked = null,
    /// The peer `connect` fixed (`dgramConnect`); null for an unconnected
    /// socket.
    peer: ?Ip4 = null,
    /// The pending error of a connected socket: an ICMP port unreachable
    /// landed, and the next send or receive takes it (`ConnectionRefused`).
    /// One slot, as both kernels keep it (`so_error`, `sk_err`).
    refused: bool = false,
};

const WireDatagram = struct {
    seq: u64,
    from: Ip4,
    to: Ip4,
    bytes: []u8,
    due_at: u64,
    /// The second copy of a duplicated datagram (`dup_ppm`). Its records
    /// set bit 0x80 of the outcome.
    copy: bool = false,
    /// An ICMP port unreachable, not a datagram: `from` is the closed
    /// address, `to` the sender of the datagram that died there, and it has
    /// no bytes. Its records set bit 0x40 of the outcome.
    icmp: bool = false,
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
/// since has another, or none open. 0 means "no socket" and is never a
/// generation. A slot opened `maxInt(u32)` times in one run refuses its next
/// open (`dgramOpen`): a wrap would give a new socket an old one's
/// generation, and an op of the old one could act on it. Nothing else in a
/// slot bounds its opens, so this is a refusal, not a trap of a safety
/// check, and holds in every build mode.
var dgram_gen: [max_dgram]u32 = @splat(0);

pub fn testSetWireSeq(n: u64) void {
    if (comptime !builtin.is_test) @compileError("testSetWireSeq is for tests");
    wire_seq = n;
}

fn testSetDgramGen(slot: usize, gen: u32) void {
    if (comptime !builtin.is_test) @compileError("testSetDgramGen is for tests");
    dgram_gen[slot] = gen;
}
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
/// Whether this run connected a datagram socket. Only then does
/// `stateDigest` cover the connected state and an ICMP go on the wire, so a
/// run with no connect keeps every digest it had before (promise 1).
var dgram_connect_used: bool = false;
/// ICMP port unreachables made in this run; the fault draws of the next
/// one use `icmp_send_base + icmp_seq` as its send number.
var icmp_seq: u64 = 0;
/// An ICMP's send number for the fault draws is in its own range, so it
/// never draws what a datagram draws, and making one moves no datagram's
/// fate. `faultDraw` multiplies the number by 8 (mod 2^64): bit 60 is the
/// highest bit that survives that, so the ranges stay apart below 2^60 sends.
/// A run that reaches 2^60 datagram sends or ICMPs refuses (`dgramSend`,
/// `sendIcmpAt`) before the ranges could meet.
const icmp_send_base: u64 = 1 << 60;

/// ICMPs the sim did not make: datagrams from `sender` died at `dead`
/// while no socket bound at `sender` was connected to `dead`, so nothing
/// could take their error then. A kernel's ICMP would still be on its way
/// back until `arrival`, and a socket that holds that four-tuple when it
/// arrives takes the error (rows 18, 19 and 22 of the in-flight table, ran on
/// Linux with the change made between the death and the arrival).
/// `dgramConnect` makes those ICMPs then (`takeGhosts`). Kept only until
/// `arrival`; never in a digest: a ghost is a function of the death records
/// in the wire digest (each death at its `due_at`) and the clock.
const Ghost = struct {
    sender: Ip4,
    dead: Ip4,
    /// `exact`: when its ICMPs land, `death + wire_latency_ns`. Else an upper
    /// bound of the latest one's arrival (saturating).
    arrival: u64,
    /// Deaths merged here: an exact ghost makes this many ICMPs.
    count: u32,
    /// No fault rule was set at any of its deaths, and all of them were at
    /// one instant. Else its ICMPs' fates (loss, delay, jitter, order) or
    /// their separate times are not known, and a connect that makes its
    /// four-tuple before `arrival` refuses.
    exact: bool,
};
pub const max_ghosts = 256;
/// The most deaths an exact ghost counts (`testSetGhostCountMax`).
var ghost_count_max: u32 = std.math.maxInt(u32);

pub fn testSetGhostCountMax(n: u32) void {
    if (comptime !builtin.is_test) @compileError("testSetGhostCountMax is for tests");
    ghost_count_max = n;
}
var ghosts: [max_ghosts]Ghost = undefined;
var ghosts_len: usize = 0;
/// When the table is full even after merging each four-tuple into one
/// entry, a death goes to the bucket of its four-tuple's hash, which keeps
/// the latest arrival among the deaths it took. A connect refuses only while
/// its own four-tuple's bucket is live, so a connect whose four-tuple was
/// never dropped is refused only if it shares a bucket with one that was.
pub const ghost_buckets = 1024;
var ghost_bucket_until: [ghost_buckets]u64 = @splat(0);

fn resetNet() void {
    for (dgrams) |d| {
        if (d.used) panic("sim: a datagram socket outlived the last run", .{});
    }
    if (wire.items.len != 0) panic("sim: datagrams on the wire outlived the last run", .{});
    resetStreams();
    next_port = first_ephemeral_port;
    dgram_gen = @splat(0);
    dropped_no_socket = 0;
    wire_seq = 0;
    net_used = false;
    dgram_connect_used = false;
    dgram_profile = null;
    icmp_seq = 0;
    ghosts_len = 0;
    ghost_bucket_until = @splat(0);
    wire_hasher = std.hash.Wyhash.init(0);
    rules_len = 0;
    rule_history_len = 0;
    udp_rule_since = no_rule;
    tcp_rule_set = false;
    fault_stats = .{};
    fault_draws = 0;
}

/// Datagram sockets open now in this run (t-4136): a consumer can check
/// them before `end` and word its own failure. 0 outside a run.
///
/// `end` refuses a run that leaves one open (t-4142): it lists each one's
/// fd (`sim: datagram teardown: sim fd N is an open datagram socket`) and
/// panics, as it does for stream objects, so a program that forgets a
/// close (for example, destroys an ENet host without closing its socket)
/// fails its test instead of passing. Close each socket before `end`. The
/// layer's close (`os_net.close`, which calls `dgramClose`) draws nothing
/// from the run's RNG and does not move the clock, and on a socket with no
/// parked op it records nothing in the trace or the wire digest (a parked
/// op would complete, which the trace records). So a test that closes its
/// idle sockets after its last digest read keeps every digest it records.
pub fn datagramSocketsOpen() usize {
    if (comptime !compiled_in) return 0;
    if (!begun) return 0;
    var n: usize = 0;
    for (dgrams) |d| n += @intFromBool(d.used);
    return n;
}

/// Prints one line per datagram socket still open, and returns how many.
fn dgramTeardownList() usize {
    var n: usize = 0;
    for (dgrams, 0..) |d, i| {
        if (!d.used) continue;
        n += 1;
        std.debug.print("sim: datagram teardown: sim fd {d} is an open datagram socket\n", .{fd_base + max_ends + @as(i32, @intCast(i))});
    }
    return n;
}

/// Frees every socket and every datagram still on the wire, at `end`.
fn freeNet() void {
    for (&dgrams) |*d| {
        if (d.used) freeQueue(d);
        d.* = .{};
    }
    for (wire.items) |w| net_alloc.free(w.bytes);
    wire.clearAndFree(net_alloc);
    freeStreams();
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
    if (streams[@intCast(fd - fd_base - max_ends)].used) panic("sim: " ++ what ++ " on sim fd {d}, which is a stream socket: not simulated on a stream socket (the protocol above `pub const StreamProfile` in sim.zig)", .{fd});
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
    while (j < max_dgram and (dgrams[j].used or streams[j].used)) j += 1;
    if (j == max_dgram) panic("sim: the datagram socket table is full ({d} sockets)", .{max_dgram});
    const gen = std.math.add(u32, dgram_gen[j], 1) catch panic("sim: datagram socket slot {d} was opened {d} times in this run: its generation would wrap, and an op of an old socket could act on a new one", .{ j, dgram_gen[j] });
    const ring = net_alloc.alloc(u8, default_rcvbuf_bytes) catch return error.SystemResources;
    errdefer net_alloc.free(ring);
    const meta = net_alloc.alloc(Queued, rcv_packet_limit) catch return error.SystemResources;
    dgram_gen[j] = gen;
    dgrams[j] = .{ .used = true, .nonblocking = nonblocking, .ring = ring, .meta = meta };
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

// ---- Connected datagram sockets (t-4167) ----
//
// What `connect` on a sim datagram socket does, and what the sim refuses.
// Every row that cites a number is a row of the parity table in
// `sim_dgram_connect_test.zig` (rows 1 to 17), or of its in-flight table
// (rows 18 to 24). The parity table runs through zio's API on real loopback
// sockets in a build without sim (macOS, and Linux with io_uring and with
// epoll) and in sim runs in a sim build, and checks each against its
// column. The in-flight rows need a delay on loopback: they ran on Linux
// with netem; macOS's lookup at the ICMP's arrival was read in XNU's
// `udp_ctlinput`, not run (dummynet needs root).
//
// How to use it: `zio.net.Socket.connect` (or std.Io's `connect` with
// `.dgram`, or the layer's `connect`), then `Socket.send` and
// `Socket.receive` (the ev ops NetSend and NetRecv), the layer's `send`,
// `recv`, `sendmsg` with no address, or any receive that also gives the
// source (`recvfrom`, `recvmsg`, `recvDatagram`, NetRecvFrom, NetRecvMsg).
// A run whose program can meet a row where the kernels differ sets a
// datagram profile first (`setDatagramProfile(.linux or .macos)`).
//
// - **The peer.** `connect` fixes it (row 1). A socket bound to the wildcard
//   address now has 127.0.0.1 as its local address, with its port; an
//   unbound socket is bound to 127.0.0.1 and the next port (rows 1 and 2).
//   `getpeername` reads the peer back; on an unconnected socket it is
//   `SocketUnconnected`. Datagrams already queued stay, from whatever source
//   (row 14: neither kernel drops them).
// - **The filter.** A datagram that lands at a connected socket from any
//   other address or port is dropped and counted in
//   `DatagramStats.dropped_not_peer`, recorded as `LandOutcome.not_peer` in
//   the trace and the wire digest (row 3). To the kernel that datagram found
//   no socket, so it is a dead landing like one to a closed port (row 17).
// - **Sends and receives with no address** go to and come from the peer
//   (row 4). On an unconnected socket, and on a closed fd, the layer's
//   `send` and `recv` and a NetRecv keep their answers from before
//   (`OperationNotSupported`), and so does `shutdown` (`SocketUnconnected`):
//   every new path is taken only for an open, connected socket.
// - **ICMP port unreachable.** A datagram that dies at its landing (no
//   socket bound to its destination, or the filter above) makes an ICMP
//   back to its sender's address when a socket bound there is connected to
//   that destination. It meets the fault rules as a datagram on that
//   reversed path does: silence, partition, loss, burst, delay, jitter, the
//   order clamp and duplication, with its own range of send numbers for the
//   draws (`icmp_send_base`), and the rules' counts include it. So it lands
//   one path latency after the death, a full round trip after the send. At
//   its landing, the socket that holds the four-tuple then (bound to the
//   sender's address, connected to the ICMP's source) takes it: its pending
//   error is set (`Dgram.refused`, `DatagramStats.refusals`) and an ev op
//   parked on it is woken. That socket can be another than the sender: a
//   new socket bound to the same address and connected to the same peer
//   while the ICMP is in flight (rows 18, 22). Recorded with bit 0x40 of
//   the outcome: `landed` when it set an error, `no_socket` when no socket
//   held the four-tuple. No datagram that dies otherwise (a full queue, a
//   fault) makes one, as no kernel does.
// - **A death no socket could take yet** (no socket at the sender's address
//   was connected to the destination) makes no ICMP, so a run that never
//   connects keeps every record. It is kept as a `Ghost` until its ICMP
//   would have landed (the death is at the datagram's `due_at`; it was
//   under a fault rule only if one was set before then); a `connect` that
//   makes its four-tuple before then makes the ICMP, due at that time
//   (row 19). An exact ghost whose count would overflow becomes inexact. A connect to another peer (row 20), from another
//   address (row 23), or after that time (row 24) makes none. Ghosts of one
//   four-tuple are merged: deaths at one instant into one entry with a
//   count; with a fault rule set, or when the table is full, into one
//   inexact entry with the latest bound. A connect that makes an inexact
//   ghost's four-tuple before its bound refuses (the ICMPs' fates or times
//   are not known). A table full even then puts the death in its
//   four-tuple's overflow bucket, and only a connect whose four-tuple hashes
//   to a live bucket refuses.
// - **A late harvest** (a test jumped the clock with `advanceNs` past a wire
//   entry and polled later; the loop's own clock never passes one): a late
//   death whose sender is not connected to where it died is exact, as
//   above. A late death whose sender is connected there, or a late ICMP
//   that a connected socket would take, panics, naming the jump and the
//   remedy (poll right after it): what the socket gets would depend on how
//   late the harvest is.
// - **The pending error** is one slot (rows 16 and 21: two ICMPs give one
//   error on both kernels). The next send takes it and does not send its
//   datagram (row 7), also a send with an address (row 25, both kernels;
//   before row 5's profile question arises); a receive takes it when the queue is empty (rows 6,
//   9). A parked receive is woken with it (row 9). A parked NetPoll for
//   reading is woken and completes with success, and the error stays (row
//   10). `getsockopt` `SO_ERROR` returns ECONNREFUSED and clears it (row 8).
//   `recvmsg` on a socket with destination addresses on takes it too; it
//   still refuses a call that would return a datagram or would block.
//   An unconnected socket never has one (row 12).
// - **The datagram profile** (`DatagramProfile`), where the kernels differ:
//   - row 5, a send with an address on a connected socket: `.macos` fails it
//     `Unexpected` (macOS's EISCONN, as zio maps it: `SendError` has no name
//     for it, t-4177), `.linux` sends to that address from the socket's
//     local address;
//   - row 6, a layer `poll` with an error pending: `.macos` reports POLLIN
//     (if asked), `.linux` reports POLLERR (always, as the kernel does);
//   - row 11, a receive with a datagram queued and an error pending:
//     `.macos` gives the datagram first, `.linux` the error first.
//   With no profile each of the three panics, as a stream row does with no
//   `StreamProfile`; a run that never meets one of them needs none. A run
//   that connects nothing can meet none (each needs a connected socket or a
//   pending error).
// - **The promise to a run that never connects a datagram socket**: if it
//   completed before t-4167, it completes the same, with the same events,
//   clock, draws and digests (sitting 37's and later end-of-run
//   comparisons against 328d798). A run that panicked before may panic with
//   another text or get an answer (`getpeername` on an unconnected socket
//   now answers `SocketUnconnected`; `sendmsg` with no address panics with
//   another text, after the send flags and the fd are checked).
//
// Refused by name (a panic): a second connect, to any peer (both kernels
// allow it and replace the peer, row 13: not simulated); a disconnect
// (`AF_UNSPEC`); a connect to a port 0, a non-loopback or a wildcard
// address; a connect that makes an inexact `Ghost`'s four-tuple (or any
// ghost's, in a run that set a fault rule) before its bound, or whose
// four-tuple's overflow bucket is live; a late harvest that reaches a
// connected socket; `shutdown` of a connected socket;
// the three profile rows with no profile; in a run that connected, its
// 2^60th datagram send or ICMP, and a connect after 2^60 sends.
//
// Fidelity limits: ICMP rate limits (Linux allows 1000 a second with a
// burst of 50, macOS's `icmplim`) are not modelled. Linux usually sets the
// error on loopback before the send returns (row 16: its second
// back-to-back send is refused), because the sending task runs the receive
// softirq itself; that is a schedule, not a guarantee (RPS, or softirq work
// handed to ksoftirqd, defers it). The sim always takes a round trip, under
// either profile: a datagram lands only at a harvest, `wire_latency_ns`
// after its send, and that order is what the per-flow FIFO and the fault
// rules rest on (t-4179 has the rest).

/// Where the kernels differ for a connected datagram socket (the
/// connected-socket protocol above). One per run, set before its first
/// datagram socket opens; none by default.
pub const DatagramProfile = enum(u8) { linux = 1, macos = 2 };

var dgram_profile: ?DatagramProfile = null;

pub fn setDatagramProfile(p: DatagramProfile) void {
    if (comptime !compiled_in) unreachable;
    refuseOtherThread("sim.setDatagramProfile");
    if (!begun) panic("sim: setDatagramProfile outside a run", .{});
    if (net_used) panic("sim: setDatagramProfile({t}) after the run's first datagram socket opened: set it before", .{p});
    if (dgram_profile) |old| panic("sim: setDatagramProfile({t}) after setDatagramProfile({t}): a run has one profile", .{ p, old });
    dgram_profile = p;
}

pub fn datagramProfile() ?DatagramProfile {
    return dgram_profile;
}

/// The profile, at a row where the kernels differ. With none, panics and
/// names the row.
fn needDgramProfile(comptime row: []const u8, fd: i32) DatagramProfile {
    return dgram_profile orelse panic("sim: " ++ row ++ " on sim datagram socket {d}: the kernels differ here, and this run set no datagram profile (sim.setDatagramProfile(.linux or .macos), before its first datagram socket)", .{fd});
}

/// `connect` on a datagram socket: fixes the peer (the connected-socket
/// protocol above). `to` is the address the caller gave.
pub fn dgramConnect(fd: i32, to: Ip4) void {
    refuseOtherThread("sim socket connect");
    const d = dgramSlot("connect", fd);
    if (d.peer != null) panic("sim: a second connect on sim datagram socket {d}: both kernels allow it and replace the peer; not simulated", .{fd});
    refuseNonLoopback("connect to", to.ip);
    if (to.port == 0) panic("sim: connect of sim datagram socket {d} to port 0", .{fd});
    const port = if (d.bound) d.local.port else ephemeralPort(.{ 0, 0, 0, 0 }) catch panic("sim: no free ephemeral port to bind sim fd {d} for its connect", .{fd});
    // The local address the kernels choose (rows 1 and 2): the wildcard
    // becomes 127.0.0.1, the address a wildcard socket sends from here.
    const local: Ip4 = .{ .ip = if (isWildcard(d.local.ip)) .{ 127, 0, 0, 1 } else d.local.ip, .port = port };
    if (wire_seq >= icmp_send_base) panic("sim: connect of sim datagram socket {d} after {d} datagram sends: from 2^60 on, an ICMP's fault draws could meet a send's (`icmp_send_base`)", .{ fd, wire_seq });
    takeGhosts(fd, local, to);
    d.bound = true;
    d.local = local;
    d.peer = to;
    dgram_connect_used = true;
}

fn ghostBucket(sender: Ip4, dead: Ip4) usize {
    const key = (@as(u64, std.mem.readInt(u32, &sender.ip, .big)) << 32 | std.mem.readInt(u32, &dead.ip, .big)) ^ (@as(u64, sender.port) << 16 | dead.port) *% 0x9e3779b97f4a7c15;
    return @intCast(mix64(key) % ghost_buckets);
}

/// Makes the ICMPs of every ghost whose four-tuple this connect makes and
/// whose arrival is still to come, due at that arrival (row 19); refuses
/// where their fate or time is not known, or where the four-tuple's
/// overflow bucket is live.
fn takeGhosts(fd: i32, local: Ip4, to: Ip4) void {
    if (ghost_bucket_until[ghostBucket(local, to)] > clock_ns) panic("sim: connect of sim datagram socket {d} to {d}.{d}.{d}.{d}:{d}: the ghost table was full, and a datagram whose four-tuple shares this one's overflow bucket died with no ICMP made, whose ICMP could still be on its way; not simulated", .{ fd, to.ip[0], to.ip[1], to.ip[2], to.ip[3], to.port });
    var i: usize = 0;
    while (i < ghosts_len) {
        const g = ghosts[i];
        if (g.arrival <= clock_ns or !std.meta.eql(g.sender, local) or !std.meta.eql(g.dead, to)) {
            i += 1;
            continue;
        }
        if (!g.exact or udp_rule_since != no_rule) panic("sim: connect of sim datagram socket {d} to {d}.{d}.{d}.{d}:{d}, where a datagram from its address died with no ICMP made, whose ICMP could still be on its way, in a run that set a fault rule or with deaths merged: its fate or time (loss, delay, jitter, order) is not known; not simulated", .{ fd, to.ip[0], to.ip[1], to.ip[2], to.ip[3], to.port });
        std.mem.copyForwards(Ghost, ghosts[i .. ghosts_len - 1], ghosts[i + 1 .. ghosts_len]);
        ghosts_len -= 1;
        wire.ensureUnusedCapacity(net_alloc, g.count) catch panic("sim: out of memory for {d} ICMP entries at a connect", .{g.count});
        for (0..g.count) |_| sendIcmpAt(g.dead, g.sender, g.arrival);
    }
}

/// Remembers a death at `death_at` (its `due_at`) that made no ICMP:
/// merged into a ghost of the same four-tuple where it can be, else a new
/// entry, else (the table full even after `compactGhosts`) its four-tuple's
/// overflow bucket. Drops ghosts already past their arrival first.
fn addGhost(sender: Ip4, dead: Ip4, death_at: u64) void {
    const faulted = udp_rule_since < death_at;
    const arrival = death_at +| wire_latency_ns +| if (faulted) max_fault_delay_ns else 0;
    var w: usize = 0;
    for (ghosts[0..ghosts_len]) |x| {
        if (x.arrival <= clock_ns) continue;
        ghosts[w] = x;
        w += 1;
    }
    ghosts_len = w;
    for (ghosts[0..ghosts_len]) |*e| {
        if (!std.meta.eql(e.sender, sender) or !std.meta.eql(e.dead, dead)) continue;
        if (e.exact and !faulted and e.arrival == arrival) {
            // A count that would overflow is no longer exact: a connect refuses.
            if (e.count == ghost_count_max) e.exact = false else e.count += 1;
            return;
        }
        if (!e.exact or faulted) {
            e.exact = false;
            e.arrival = @max(e.arrival, arrival);
            e.count +|= 1;
            return;
        }
        // An exact ghost of the same four-tuple at another time stays apart,
        // so each keeps its time.
    }
    if (ghosts_len == max_ghosts) compactGhosts();
    if (ghosts_len == max_ghosts) {
        const b = &ghost_bucket_until[ghostBucket(sender, dead)];
        b.* = @max(b.*, arrival);
        return;
    }
    ghosts[ghosts_len] = .{ .sender = sender, .dead = dead, .arrival = arrival, .count = 1, .exact = !faulted };
    ghosts_len += 1;
}

/// Merges the ghosts of each four-tuple into one inexact entry (the latest
/// arrival, the deaths summed), so the table holds one entry per four-tuple.
fn compactGhosts() void {
    var w: usize = 0;
    for (ghosts[0..ghosts_len]) |x| {
        for (ghosts[0..w]) |*e| {
            if (std.meta.eql(e.sender, x.sender) and std.meta.eql(e.dead, x.dead)) {
                e.exact = false;
                e.arrival = @max(e.arrival, x.arrival);
                e.count +|= x.count;
                break;
            }
        } else {
            ghosts[w] = x;
            w += 1;
        }
    }
    ghosts_len = w;
}

/// The connected peer (`getpeername`), or null.
pub fn dgramPeer(fd: i32) ?Ip4 {
    return dgramSlot("getpeername", fd).peer;
}

/// Whether `fd` is an open, connected sim datagram socket. False for a
/// closed one, so the layer's new connected paths leave a closed fd's old
/// answers alone.
pub fn dgramIsConnected(fd: i32) bool {
    if (!dgramIsOpen(fd)) return false;
    return dgrams[dgramIndex(fd)].peer != null;
}

/// One datagram of `src` gathered, to `to`. With `from`, the datagram leaves
/// from that address, which must be one the socket can receive on: its bound
/// address, or for a wildcard socket any 127/8 address. Else
/// `AddressUnavailable`, as the kernel says. Completes at once; the datagram
/// lands `wire_latency_ns` later.
///
/// `to_addr` null is a send with no address (`send`, `sendmsg` without
/// one): to the peer of a connected socket. The layer passes null only for
/// a connected socket. An address on a connected socket is row 5 of the
/// profile (the connected-socket protocol above `dgramConnect`). A connected
/// socket with a pending error takes it here instead of sending:
/// `ConnectionRefused`, and the datagram is not sent, as both kernels do
/// (row 7 of the parity table in `sim_dgram_connect_test.zig`).
pub fn dgramSend(fd: i32, src: anytype, to_addr: ?Ip4, from: ?[4]u8) error{ AddressUnavailable, SystemResources, ConnectionRefused, Unexpected }!usize {
    refuseOtherThread("sim socket send");
    checkIovecs("a send", fd, src.len);
    const d = dgramSlot("send", fd);
    if (dgram_connect_used and wire_seq >= icmp_send_base) panic("sim: datagram send number {d} on sim fd {d}: from 2^60 on, a send's fault draws could meet an ICMP's (`icmp_send_base`)", .{ wire_seq, fd });
    // A pending error is taken by any send, with an address or not, before
    // anything else: row 25, measured on both kernels.
    if (d.refused) {
        d.refused = false;
        return error.ConnectionRefused;
    }
    const to = if (to_addr) |a| blk: {
        if (d.peer != null) switch (needDgramProfile("a send with an address on a connected socket (row 5)", fd)) {
            .macos => return error.Unexpected,
            .linux => {},
        };
        break :blk a;
    } else d.peer orelse panic("sim: a send with no address on unconnected sim datagram socket {d}", .{fd});
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
    for (src) |b| {
        const sl = bufBytes(b);
        @memcpy(bytes[at..][0..sl.len], sl);
        at += sl.len;
    }
    if (udp_rule_since != no_rule) {
        const src_addr: Ip4 = .{ .ip = src_ip, .port = d.local.port };
        // Every allocation this send could need comes before any fault
        // decision: a decision moves counts, matches, draws and a burst
        // state, and a send that then failed for memory would leave them
        // moved, so its retry would get another fate (round 5 of the
        // outside review, finding 1). A duplicate's copy is made whether or
        // not the rule duplicates, and freed if it does not.
        const second = net_alloc.dupe(u8, bytes) catch {
            net_alloc.free(bytes);
            return error.SystemResources;
        };
        wire.ensureUnusedCapacity(net_alloc, 2) catch {
            net_alloc.free(bytes);
            net_alloc.free(second);
            return error.SystemResources;
        };
        switch (faultAtSend(wire_seq, src_addr, to)) {
            .dropped => |outcome| {
                // The sender succeeded; the datagram is recorded at its
                // send time, bytes and all, and never reaches the wire.
                recordLanding(.{ .seq = wire_seq, .from = src_addr, .to = to, .bytes = bytes, .due_at = clock_ns }, outcome);
                net_alloc.free(bytes);
                net_alloc.free(second);
            },
            .sent => |s| {
                wireInsert(.{ .seq = wire_seq, .from = src_addr, .to = to, .bytes = bytes, .due_at = s.due[0] });
                if (s.copies == 2) {
                    wireInsert(.{ .seq = wire_seq, .from = src_addr, .to = to, .bytes = second, .due_at = s.due[1], .copy = true });
                } else net_alloc.free(second);
            },
        }
        wire_seq += 1;
        d.stats.sent += 1;
        return total;
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
///
/// A connected socket's pending error is taken by a receive that finds the
/// queue empty: `ConnectionRefused`. A queued datagram comes first, as macOS
/// orders it; Linux returns the error first (row 11 of the parity table in
/// `sim_dgram_connect_test.zig`). The sim has no datagram profile and takes
/// macOS's order on every host (the connected-socket protocol above
/// `dgramConnect` says why).
pub fn dgramRecv(fd: i32, dst: anytype, dont_wait: bool) error{ WouldBlock, ConnectionRefused }!Received {
    refuseOtherThread("sim socket receive");
    checkIovecs("a receive", fd, dst.len);
    const d = dgramSlot("receive", fd);
    if (dgramTakeRefusal(fd)) return error.ConnectionRefused;
    if (d.meta_len == 0) {
        if (dont_wait or d.nonblocking) return error.WouldBlock;
        panic("sim: a blocking receive on sim fd {d}, which is not non-blocking: nothing can land while the sim thread waits", .{fd});
    }
    const q = d.meta[d.meta_head];
    var copied: usize = 0;
    for (dst) |b| {
        const buf = bufBytes(b);
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

/// Takes `fd`'s pending error if a receive would now: queue empty, or row 11.
pub fn dgramTakeRefusal(fd: i32) bool {
    const d = dgramSlot("receive", fd);
    if (!d.refused or (d.meta_len != 0 and needDgramProfile("a receive with a datagram queued and an error pending (row 11)", fd) == .macos)) return false;
    d.refused = false;
    return true;
}

/// Puts `w` on the wire after every entry due no later (capacity reserved).
fn wireInsert(w: WireDatagram) void {
    var pos = wire.items.len;
    while (pos > 0 and wire.items[pos - 1].due_at > w.due_at) pos -= 1;
    wire.insertAssumeCapacity(pos, w);
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

/// ICMP port unreachables made in this run (the connected-socket protocol
/// above `dgramConnect`): one per datagram that died with its sender
/// connected there. A copy made by `dup_ppm` is not counted again.
pub fn icmpsMade() u64 {
    return icmp_seq;
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
    if (d.unresolved) |u| {
        if (u.c != c or u.id != id) panic("sim: op {d} parks on sim datagram socket {d}, whose unresolved op is {d}", .{ id, fd, u.id });
    }
    d.parked = .{ .c = c, .id = id, .wait = wait };
    d.unresolved = d.parked;
}

/// A sim datagram socket takes one unresolved ev receive or poll for
/// reading at a time (parked, or woken and not yet harvested); a second
/// refuses at submit, whatever it would do. Between a landing and a woken
/// op's harvest, a callback earlier in the batch could otherwise submit one
/// that takes the datagram and another that parks, and the woken op's
/// harvest would find its slot taken (round 3, the datagram side of findings
/// 2 and 3). A layer receive (`recvDatagram`) can still take the datagram;
/// then the woken op parks again.
pub fn dgramRefuseSecondWaiter(fd: i32, new_id: u32) void {
    if (!dgramIsOpen(fd)) return;
    const d = &dgrams[dgramIndex(fd)];
    const u = d.unresolved orelse return;
    panic("sim: a second ev receive or poll while one is {s}, on sim datagram socket {d} (op {d}; the new op {d}): a sim datagram socket takes one unresolved ev receive or poll for reading at a time", .{ if (d.parked != null) "parked" else "woken and not yet harvested", fd, u.id, new_id });
}

pub fn dgramIsOpen(fd: i32) bool {
    if (fd < fd_base + max_ends or fd >= fd_base + sim_fd_count) return false;
    return dgrams[@intCast(fd - fd_base - max_ends)].used;
}

/// `poll` with a zero timeout: readable when a datagram is queued; a
/// datagram socket is always writable (send pressure is not modelled).
/// Refuses a pipe end and a closed socket, so a poll calls it for every
/// entry, whatever its events.
/// The layer's `poll` of one sim datagram socket: its revents for
/// `events`. POLLIN when a datagram is queued; POLLOUT always (send
/// pressure is not modelled). With an error pending, row 6 of the profile:
/// `.macos` adds POLLIN, `.linux` adds POLLERR whatever was asked.
pub fn dgramPollRevents(fd: i32, events: i16) i16 {
    const d = dgramSlot("poll", fd);
    var readable = d.meta_len > 0;
    var r: i16 = 0;
    if (d.refused) switch (needDgramProfile("a poll with an error pending (row 6)", fd)) {
        .macos => readable = true,
        .linux => r |= sys.POLL.ERR,
    };
    if (events & sys.POLL.IN != 0 and readable) r |= sys.POLL.IN;
    if (events & sys.POLL.OUT != 0) r |= sys.POLL.OUT;
    return r;
}

/// What a NetPoll for reading waits for: a queued datagram, or a pending
/// error (both kernels wake it and complete it with success; the error
/// stays for the next call: row 10 of the parity table).
pub fn dgramPollReady(fd: i32) bool {
    const d = dgramSlot("poll", fd);
    return d.meta_len > 0 or d.refused;
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
/// `SO_ERROR` (ECONNREFUSED for a connected socket's pending error, which
/// the read clears, as both kernels do; else 0), and the
/// destination-address option. Every other read refuses: the layer's
/// `GetsockoptError` has no "unsupported" to return.
pub fn dgramGetOption(fd: i32, level: i32, name: u32, out: []u8) void {
    const d = dgramSlot("getsockopt", fd);
    const v: c_int = switch (classifyOption(level, name)) {
        .rcvbuf => @intCast(d.byte_limit),
        .so_error => blk: {
            if (!d.refused) break :blk 0;
            d.refused = false;
            break :blk @intCast(@backingInt(std.posix.E.CONNREFUSED));
        },
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

/// What happened to a datagram, in the trace and the wire digest. 0 to 2
/// are phase 1's, recorded at landing. 3 to 6 are drops by a fault rule at
/// the send, recorded at the send time; 7 and 8 are drops by a rule in
/// force at the landing, recorded at the landing (the fault protocol above
/// `pub const Endpoint`).
const LandOutcome = enum(u8) {
    landed = 0,
    no_socket = 1,
    rcvbuf = 2,
    lost = 3,
    lost_burst = 4,
    partitioned = 5,
    silenced = 6,
    partitioned_in_flight = 7,
    silenced_in_flight = 8,
    /// Landed at a connected socket from a source that is not its peer
    /// (the connected-socket protocol above `dgramConnect`).
    not_peer = 9,
};

/// Records a datagram at its landing time in the trace and the wire digest.
fn recordLanding(w: WireDatagram, outcome: LandOutcome) void {
    const code: u8 = @backingInt(outcome) | (@as(u8, @intFromBool(w.icmp)) << 6) | (@as(u8, @intFromBool(w.copy)) << 7);
    emit(.dgram, @as(u32, w.from.port) | (@as(u32, code) << 16), (@as(u32, w.to.port) << 16) | @as(u32, @intCast(w.bytes.len)));
    var h: [25]u8 = undefined;
    std.mem.writeInt(u64, h[0..8], w.due_at, .little);
    @memcpy(h[8..12], &w.from.ip);
    std.mem.writeInt(u16, h[12..14], w.from.port, .little);
    @memcpy(h[14..18], &w.to.ip);
    std.mem.writeInt(u16, h[18..20], w.to.port, .little);
    h[20] = code;
    std.mem.writeInt(u32, h[21..25], @intCast(w.bytes.len), .little);
    wire_hasher.update(&h);
    wire_hasher.update(w.bytes);
}

/// The landing times of the datagrams on the wire, in landing order, so a
/// test's model can compare them with its own (round 5 of the outside
/// review, finding 2).
pub fn testWireDues(out: []u64) usize {
    if (comptime !builtin.is_test) @compileError("testWireDues is for tests");
    if (wire.items.len > out.len) panic("sim: testWireDues: {d} datagrams on the wire, room for {d}", .{ wire.items.len, out.len });
    for (wire.items, 0..) |w, i| out[i] = w.due_at;
    return wire.items.len;
}

/// A hash of every datagram that reached its landing time in this run, in
/// landing order: the time, both addresses and ports, landed or dropped, the
/// length, and every payload byte. Two runs with equal digests delivered the
/// same bytes to the same places at the same times.
pub fn wireDigest() u64 {
    return wire_hasher.final();
}

fn land(w: WireDatagram) void {
    if (rules_len != 0) {
        if (faultInFlight(w.from, w.to)) |outcome| {
            recordLanding(w, outcome);
            return;
        }
    }
    if (w.icmp) return landIcmp(w);
    const d = receiverOf(w.to) orelse {
        dropped_no_socket += 1;
        recordLanding(w, .no_socket);
        diedAtLanding(w);
        return;
    };
    if (d.peer) |p| {
        if (!std.meta.eql(p, w.from)) {
            d.stats.dropped_not_peer += 1;
            recordLanding(w, .not_peer);
            diedAtLanding(w);
            return;
        }
    }
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

/// A datagram found no socket at its landing (none bound there, or a
/// connected one whose peer it is not). If a socket bound at its sender's
/// address is connected to where it died, the kernel's ICMP port
/// unreachable goes back (`sendIcmpAt`). Otherwise nothing can take one now,
/// and it is kept as a `Ghost` for a connect before its arrival.
fn diedAtLanding(w: WireDatagram) void {
    const s = receiverOf(w.from) orelse return addGhost(w.from, w.to, w.due_at);
    const p = s.peer orelse return addGhost(w.from, w.to, w.due_at);
    if (!std.meta.eql(p, w.to)) return addGhost(w.from, w.to, w.due_at);
    lateHarvest("its sender is connected to where it died", w);
    sendIcmpAt(w.to, w.from, clock_ns + wire_latency_ns);
}

/// A wire entry processed after its `due_at` (a test's `advanceNs` jumped
/// past it; the loop's clock never does) refuses to reach a connected socket.
fn lateHarvest(comptime what: []const u8, w: WireDatagram) void {
    if (w.due_at >= clock_ns) return;
    panic("sim: a datagram from port {d} to port {d} due at {d} ns was harvested at {d} ns, after a clock jump (sim.advanceNs) with no poll between; " ++ what ++ ", and what a connected socket gets would depend on how late the harvest is: not simulated. Poll right after the jump (for example sleep 1 ns)", .{ w.from.port, w.to.port, w.due_at, clock_ns });
}

/// Puts an ICMP port unreachable from `from` (the address where a datagram
/// died) to `to` (its sender) on the wire, due at `due_at` with no fault rule
/// in the run, else through the rules as a datagram on that path (sent now).
/// The caller reserved the room (`landDue`, `takeGhosts`).
fn sendIcmpAt(from: Ip4, to: Ip4, due_at: u64) void {
    // Below 2^60 always: each ICMP is a death, each death a send, and a run
    // that connected refuses its 2^60th send (`dgramSend`, `dgramConnect`).
    const send_no = icmp_send_base + icmp_seq;
    icmp_seq += 1;
    const base: WireDatagram = .{ .seq = send_no, .from = from, .to = to, .bytes = &.{}, .due_at = due_at, .icmp = true };
    if (udp_rule_since == no_rule) return wireInsert(base);
    switch (faultAtSend(send_no, from, to)) {
        .dropped => |outcome| {
            var x = base;
            x.due_at = clock_ns;
            recordLanding(x, outcome);
        },
        .sent => |s| {
            var a = base;
            a.due_at = s.due[0];
            wireInsert(a);
            if (s.copies == 2) {
                var b = base;
                b.due_at = s.due[1];
                b.copy = true;
                wireInsert(b);
            }
        },
    }
}

/// An ICMP landed at the address of the datagram's sender: the socket bound
/// there takes the error if it is connected to the ICMP's source.
fn landIcmp(w: WireDatagram) void {
    const d = receiverOf(w.to) orelse return recordLanding(w, .no_socket);
    const p = d.peer orelse return recordLanding(w, .no_socket);
    if (!std.meta.eql(p, w.from)) return recordLanding(w, .no_socket);
    lateHarvest("it is an ICMP that a connected socket would take", w);
    recordLanding(w, .landed);
    d.refused = true;
    d.stats.refusals += 1;
    wakeParked(d);
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
    // Each landing can make one ICMP, in two copies at most (`sendIcmp`),
    // inserted after these n (it is due later than now). Reserved first, so
    // `due_now` stays valid.
    if (dgram_connect_used) wire.ensureUnusedCapacity(net_alloc, 2 * n) catch panic("sim: out of memory for the ICMP entries of {d} landings", .{n});
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

// ---- Stream sockets (design section 17; slices 1 and 2 of 17.16) ----
//
// The sim side of IPv4 TCP on 127.0.0.0/8. Slice 1 builds sockets, ports,
// the handshake, listeners, accept and close; slice 2 builds bytes, FIN,
// RST with data, shutdown and the profiles' send and receive rules. The
// socket layer
// (src/os/net.zig) and the ev loop's sim dispatch (`Loop.submitSimStream`)
// call these; nothing else does.
//
// How a test uses stream sockets under sim:
// - Through zio's public API, as a program does: `zio.net.IpAddress.listen`
//   and `connect`, `Server.accept`, `close`; or std.Io's `listen`,
//   `connect`, `accept` and `close` on zio's vtable. Each is the ev ops
//   NetOpen, NetBind, NetListen, NetConnect, NetAccept and NetClose, plus
//   `setsockopt` for TCP_NODELAY, SO_REUSEADDR and SO_REUSEPORT.
// - Bytes go through the same API: `Stream.read`, `write`, `writeAll`,
//   `readVec`, `writeVec`, `shutdown`, and their std.Io forms (`netRead`,
//   `netWrite`, `netShutdown`), each the ev op NetRecv, NetSend or
//   NetShutdown with the default flags. `os.net.getpeername` and
//   `os.net.bytesReadable` (FIONREAD) are the layer's own, and SO_LINGER
//   {1, 0} makes a close send an RST.
// - A run that meets a row where the kernels differ needs a profile:
//   `sim.setStreamProfile(.linux | .macos)`, called before the run's first
//   stream socket opens, once per run. Without one, such a row panics and
//   names it: a bind with SO_REUSEADDR next to a listener on another
//   address of the port (row 12), a rebind over a TIME_WAIT reservation
//   that did not set SO_REUSEADDR (row 31), a connect to 127/8 other than
//   127.0.0.1 (rows 15 and 30), a send called after a reset with no FIN
//   before it (rows 26b, 36, 39), a send parked when a reset arrives after
//   a FIN (row 41), a read that meets a reset after a FIN (rows 8, 22),
//   shutdown(.both) with unread bytes (row 17), and data that lands after
//   shutdown(.both) (row 18).
// - What a profile follows (t-4119's ruling): what a zio program sees on
//   that kernel, which is not always what a blocking C call sees. The two
//   differ where an op is parked: zio's readiness backends (kqueue, epoll)
//   end a parked op on an error or hang-up event by reading SO_ERROR, which
//   reports the reset and clears it. So a send parked when a reset arrives
//   takes it: ECONNRESET with no FIN first, on both kernels (rows 23, 25);
//   after a FIN, ECONNRESET on macOS and EPIPE on Linux (row 41). Reads then
//   give 0. A send called after the reset follows the kernel's send: EPIPE
//   on macOS, the pending error once on Linux. The parity table in
//   `sim_stream_bytes_test.zig` runs each row through zio on the kernel and
//   in a sim run, with one expected column per kernel.
// - Not modelled (design 17.14): under `.linux`, shutdown(.both) with unread
//   bytes while the sender's window still has a gap smaller than one
//   segment. The kernel's persist timer then sends data into the gap, the
//   shut receiver resets, and the parked send ends EPIPE (parity row 41g).
//   The sim's window has no segment size, so it never has that gap: it is
//   always in the state the kernel reaches once the gap is filled, where the
//   send stays parked (row 41).
// - Settings, each in a run, on the sim thread: `setStreamWindow(fd, n)`
//   (the window toward `fd`, 1 byte to 16 MiB, default 256 KiB, refused
//   while the peer's send is woken and not harvested) and
//   `setStreamWindowDefault(n)` for later connections;
//   `setStreamChunking(fd, mode)` (`.hashed`, the default; `.{ .fixed = n
//   }`, n from 1 to 65536; `.off`) for `fd`'s connection, and
//   `setStreamChunkingDefault(mode)`.
// - Before `sim.end`: close every stream socket, then sleep in virtual time
//   while `streamEntriesInFlight()` is not 0 (TCP entries on the wire and
//   credits owed). `end` checks, before it frees
//   anything, that no stream socket is open, no op on one is unresolved and
//   every connection is retired, and panics listing what remains (design
//   17.15 item 3). TIME_WAIT reservations are expected: `end` counts them,
//   prints the count and frees them.
//
// Model:
// - A stream socket takes a slot of the datagram table's range: one fd
//   range, one lowest-free slot allocator, one generation per slot
//   (`dgram_gen`), whichever kind opened it. A slot is a datagram socket
//   (`dgrams[j].used`) or a stream socket (`streams[j].used`), never both.
//   A stream socket is open (never connected), a listener, or an endpoint's
//   descriptor (`SockKind`).
// - A connection (`Conn`) is made by `NetConnect`, which gives it the next
//   `conn_id` (from 1, never reused in a run). It has two endpoints, client
//   and server (`StreamEnd`), whose phase is `StreamPhase`, and two
//   directions; direction d is from side d.
// - Each TCP entry (SYN, SYN-ACK, ACK, FIN, RST) carries its `conn_id`, its
//   direction and a sequence number of that direction, and lands
//   `wire_latency_ns` after it leaves. `takeDue` lands the datagrams due
//   first (`landDue`, unchanged), then the TCP entries due, in (landing
//   time, `conn_id`, direction, sequence) order (`landStreamDue`). No TCP
//   landing draws from the run's RNG. A landing goes to its connection's
//   endpoint, alive or dead, never to whatever socket holds a number or a
//   port now; a SYN alone looks up a listener, at its landing.
// - The handshake: the SYN lands at a listener, which makes an `S-prov`
//   endpoint and sends a SYN-ACK; at its landing the connect completes and
//   an ACK leaves; at the ACK's landing the endpoint is queued (`S-queued`)
//   and an accept waiting on the listener is woken. A wake claims nothing:
//   the accept takes the queue head at its harvest, so a cancel before then
//   leaves the connection queued (17.6). A SYN with no listener at its
//   address is answered with an RST: `ConnectionRefused`.
// - A listener holds at most `backlog` (1 to 4096) live `S-prov` and
//   `S-queued` endpoints; a SYN that lands at a full one panics (backlog
//   overflow is not simulated). Closing a listener kills them; each sends
//   an RST to its client. Accepted connections are not affected.
// - Close of an established endpoint sends a FIN (none after an RST
//   landed on it). The endpoint that sends its FIN before one has landed on
//   it is the active closer, and its address and port are reserved for the
//   rest of the run (TIME_WAIT), with its SO_REUSEADDR flag. A passive
//   closer's port is free at its close.
// - A connection retires (its table entry is freed) when both endpoints are
//   dead (a server endpoint never made, `absent`, counts as dead) and no
//   entry of it is on the wire.
//
// Ports: TCP has its own port table and its own counter from 49152 (wrapping
// to 49152 after 65535). Port 0 takes the next port that no stream socket
// holds and no reservation covers. A connect from an unbound socket binds
// 127.0.0.1 and the next port. A bind to a fixed port, against each stream
// socket holding that port on an overlapping address (equal, or either is
// 0.0.0.0), and each reservation:
// - the new socket did not set SO_REUSEADDR: `AddressInUse`;
// - a listener at the equal address: `AddressInUse`;
// - a listener at another address, both with SO_REUSEADDR: row 12, by
//   profile: `.linux` `AddressInUse`; `.macos` refused (which listener gets
//   a SYN was not measured);
// - a reservation with SO_REUSEADDR: no conflict;
// - a reservation without it: row 31, by profile: `.linux` `AddressInUse`,
//   `.macos` no conflict;
// - any other holder (a listener without SO_REUSEADDR, a bound socket, an
//   endpoint) with the new socket's SO_REUSEADDR set: refused, not
//   measured.
//
// Options on a stream socket: TCP_NODELAY 1 is stored, read back, inherited
// by accepted sockets, and has no effect; TCP_NODELAY 0 is
// `OptionNotSupported` (Nagle is not modelled). SO_REUSEADDR is stored and
// used by bind and TIME_WAIT. SO_REUSEPORT is `OptionNotSupported`
// (advisory: zio's std.Io listen sets it and ignores the answer); it stays
// misuse on a datagram socket. `getsockopt(SO_ERROR)` is refused (no sim
// path needs it, and both kernels clear it on read: design rows 33, 34).
// Any other option panics, naming it.
//
// Bytes (design 17.7 and 17.8):
// - A send admits `min(length, room)` bytes at once, where room is the
//   window less what the peer has not credited, and returns that count; with
//   no room it parks. A send of length 0 returns 0. The admitted bytes go out
//   as chunks (`ChunkMode`): chunk k lands `wire_latency_ns + k µs` after the
//   send, and no entry lands before the one sent before it in its direction.
// - A read takes the unread bytes, up to its buffers, and owes a credit. At
//   most one credit departs per direction per `wire_latency_ns`; one owed
//   departs `wire_latency_ns` after the last, carrying the count at that
//   time. A credit that lands raises what the sender may use and wakes its
//   parked send. A receive with nothing unread parks, and gives 0 after a
//   FIN, after its own shutdown(.both), and after a reset is reported.
// - The decision lists of design 17.8 decide each call and each harvest;
//   `streamRecv`, `streamSend`, `streamShutdown`, `closeEndpoint` and the
//   `land*` functions are those lists, line by line.
// - `testStreamDir` gives a test each direction's counters, so it can check
//   `admitted = in flight + held + unread + read + discarded`.
//
// Faults (design 17.9 and 17.9a):
// - A fault rule whose endpoints say `.protocol = .tcp` acts on TCP entries
//   (SYN, SYN-ACK, ACK, data, FIN, RST, credit), never on datagrams; a
//   `.udp` rule (the default) never on TCP entries. An entry goes from its
//   sender's local address to its peer (a SYN to its destination); a path
//   matches both, a silence either. The calls are the datagram ones:
//   `setImpairment`, `setPartition`, `silence`, and their clears.
// - A `.tcp` impairment takes `delay_ns` and `jitter_ns` (the last set
//   that matches acts). Both are fixed at the entry's send, the jitter by
//   `tcpJitterDraw`; the entry lands `wire_latency_ns` + its chunk spacing
//   + delay + jitter after its send, never before the entry before it in
//   its direction. `loss_ppm`, `burst`, `dup_ppm` and `reorder` refuse.
// - A `.tcp` partition or silence holds entries, never loses them. An entry
//   is held if one matches at its send or at its landing, or if its
//   direction already holds one (order). When one is held at its landing,
//   the later entries of its direction still on the wire are held behind
//   it at once (`held_behind`). Held data stays charged to the
//   window, so a held credit stalls the sender. Two held credits of one
//   direction become one: the one with the larger sequence, whichever was
//   held first (`holdTcp`). When a clear leaves a held direction
//   with no matching partition or silence, it is released, in (`conn_id`,
//   direction) order: each entry lands `wire_latency_ns` + its extra time
//   after the release, in order. Held entries never stop the clock, count
//   in `streamEntriesInFlight` and toward `max_tcp_entries`, and keep a
//   connection from retiring. A test that drains before `sim.end` clears
//   its rules first.
// - An abort (close with unread bytes or SO_LINGER {1, 0}, an RST landing,
//   a local reset) discards the endpoint's held data, FIN and credits;
//   its RST follows. A close in `C-syn` or `C-failed` discards a held SYN.
//   Each discard is recorded (`StreamOutcome.discarded_held`).
// - The connect with no answer is a held SYN: the connect stays parked
//   until its deadline (`Timeout`), a cancel, a close, or the release.
// - `failStream(fd, .reset | .timed_out)`: its doc comment.
// - Counts: `streamStats()` (`held_at_send`, `held_at_landing`,
//   `held_behind`, `released`,
//   `credits_coalesced`, `discarded_held`, `delayed`, `dropped_failed`),
//   each rule's `matched` and `delayed`, `streamFaultDraws()`.
//
// Refused by name (a panic): poll and the datagram calls on a stream
// socket; the socket layer's own recv, send and the rest (only the native
// backends call them); a receive of 0 bytes; a second receive or send while
// one is unresolved; a send or receive flag other than the defaults;
// shutdown(.receive); SO_LINGER with a linger time; `failStream` misuse (its
// doc comment); a `.tcp` impairment with loss, burst, duplication or
// reorder; a path whose ends name two protocols; `connect` on a socket
// the program bound, on a listener or on an endpoint; `local_address` in
// `IpAddress.connect` (refused in src/net.zig before any side effect);
// `listen` on an unbound socket, a listener or an endpoint; `accept` on
// anything but a listener (a datagram socket keeps OperationNotSupported); a
// second accept on a listener while one is unresolved; a bind of a bound or
// connected socket; the layer's own `connect` and `accept` (only the native
// backends call them); IPv6, non-loopback addresses, and port 0 as a
// destination; more than `max_conns` connections at once or
// `max_tcp_entries` on the wire; and every call from a thread that is not
// the sim thread.

pub const StreamProfile = enum(u8) { linux = 1, macos = 2 };

/// Connections at one time (design 17.5). A retired connection frees its
/// entry; its `conn_id` is never reused.
pub const max_conns = 256;
/// TCP entries on the wire at one time, all connections (design 17.7).
pub const max_tcp_entries = 65536;
/// The largest listen backlog; `listen` clamps to 1..max_backlog.
pub const max_backlog = 4096;

var stream_profile: ?StreamProfile = null;
/// Whether this run opened a stream socket. Only then does `stateDigest`
/// cover the stream tables and `end` run the teardown check, so a run with
/// none keeps every digest it had before stream sockets existed (design 17.5
/// and section 7, promise 1).
var stream_used: bool = false;

const SockKind = enum(u8) { open = 1, listener = 2, endpoint = 3 };
const Side = enum(u1) { client = 0, server = 1 };

fn otherSide(s: Side) Side {
    return if (s == .client) .server else .client;
}

const StreamParked = struct { c: *anyopaque, id: u32 };

const StreamSock = struct {
    used: bool = false,
    kind: SockKind = .open,
    nonblocking: bool = false,
    /// The program bound it (`streamBind`). A connect from it is refused.
    bound: bool = false,
    /// The bound address; an endpoint's local address.
    local: Ip4 = .{ .ip = @splat(0), .port = 0 },
    nodelay: bool = false,
    reuseaddr: bool = false,
    /// SO_LINGER {1, 0}: a close sends an RST (design 17.8, close 3).
    linger0: bool = false,
    /// A listener's capacity (`listen`'s backlog, clamped).
    backlog: u16 = 0,
    /// A listener's next queue position: ACK landing order.
    next_queue_pos: u64 = 0,
    /// An endpoint's connection (index into `conns`) and side.
    conn: u16 = 0,
    side: Side = .client,
    /// The connect or accept waiting on this socket: a landing or a close
    /// makes it due now.
    parked: ?StreamParked = null,
    /// That op until it has its result: parked, or woken and not yet
    /// harvested. One at a time (a second accept refuses).
    unresolved: ?StreamParked = null,
    /// An endpoint's receive and send, each the same way: parked until a
    /// landing, a setting or a call wakes it; unresolved until it has its
    /// result. One of each at a time (design 17.8, the call lists).
    parked_recv: ?StreamParked = null,
    unresolved_recv: ?StreamParked = null,
    parked_send: ?StreamParked = null,
    unresolved_send: ?StreamParked = null,
};

var streams: [max_dgram]StreamSock = @splat(.{});

/// An endpoint's phase (design 17.5). `absent`: a server endpoint no SYN
/// has made (it counts as dead for retirement). `c_syn`: a connect's SYN
/// left and no answer landed. `c_failed`: an RST answered the SYN.
/// `s_prov`: a SYN landed at a listener. `s_queued`: its ACK landed, it waits
/// for an accept. `est`: owned by a descriptor. `dead`: closed, or killed by
/// its listener's close or an RST in `s_prov`.
pub const StreamPhase = enum(u8) { absent = 0, c_syn = 1, c_failed = 2, s_prov = 3, s_queued = 4, est = 5, dead = 6 };

const StreamEnd = struct {
    phase: StreamPhase = .absent,
    /// The descriptor's slot, in `c_syn`, `c_failed` and `est`.
    slot: u8 = 0,
    local: Ip4 = .{ .ip = @splat(0), .port = 0 },
    peer: Ip4 = .{ .ip = @splat(0), .port = 0 },
    fin_in: bool = false,
    fin_out: bool = false,
    /// It sent an RST; it sends at most one in its life.
    rst_sent: bool = false,
    reset_received: bool = false,
    /// Whether a FIN had landed when the RST did. Kept profile-neutral:
    /// slice 2's profiles decide from it what the reset reports (review 3,
    /// finding 9).
    fin_before_reset: bool = false,
    /// An unaccepted server endpoint's listener: its slot and generation.
    listener_slot: u8 = 0,
    listener_gen: u32 = 0,
    queue_pos: u64 = 0,
    /// The listener's options when the SYN landed, for the accepted socket.
    nodelay: bool = false,
    reuseaddr: bool = false,
    linger0: bool = false,
    /// It did `shutdown(.both)`: its receive side is shut.
    shut_rd: bool = false,
    /// An op returned the reset's error (design 17.8, the two facts).
    reset_reported: bool = false,
    /// `failStream` hit it (design 17.9): how, and whether an op has
    /// returned its error. Kept after it dies, to drop late entries.
    failed: ?StreamFailure = null,
    fail_reported: bool = false,
    /// The op id of the send that was parked when the reset was received,
    /// if one was. Set with `reset_received`, profile-neutral; the send
    /// evaluation reads it at that op's harvest (design 17.8: a parked send
    /// takes the reset, as zio's readiness backends end it with SO_ERROR).
    reset_parked_send: ?u32 = null,
    /// Bytes landed and not read: `unread.items[unread_head..]`. Freed when
    /// the connection retires.
    unread: std.ArrayList(u8) = .empty,
    unread_head: usize = 0,

    fn unreadLen(e: StreamEnd) usize {
        return e.unread.items.len - e.unread_head;
    }
};

/// Why bytes were discarded (design 17.7, conservation). `abort`: held
/// data at its sender's abort; `failed_sender` and `failed_receiver`:
/// `failStream`'s (held outbound, and unread or landing later).
pub const Discard = enum(u8) { abort, failed_sender, failed_receiver, closed_receiver, unread_at_close, shut_receiver, both_discard, after_reset, listener_closed };

/// One direction of a connection: data from side `d` to the other side,
/// with its window and credit (design 17.7). The credit for it travels the
/// other way.
const StreamDir = struct {
    /// The bytes the receiver accepts beyond `read + reclaimed`.
    window: u32 = default_stream_window,
    admitted: u64 = 0,
    /// The highest `read + reclaimed` count a credit has brought back.
    credited: u64 = 0,
    read: u64 = 0,
    /// Unread bytes `.both` discarded under `.macos`: room freed without a
    /// read (the window release, design 17.7).
    reclaimed: u64 = 0,
    discarded: [@typeInfo(Discard).@"enum".field_names.len]u64 = @splat(0),
    /// Sends that admitted at least one byte: the chunk draws' key.
    data_send_index: u64 = 0,
    /// Credit pacing: when the last credit departed (none yet: null), and
    /// when an owed one departs.
    last_credit_at: ?u64 = null,
    owed_at: ?u64 = null,

    fn room(d: StreamDir) u64 {
        const used = d.admitted - d.credited;
        return if (used >= d.window) 0 else d.window - used;
    }
};

/// How a send's admitted bytes are cut into chunks (design 17.7).
pub const ChunkMode = union(enum) {
    /// Drawn from a keyed hash: mostly 512 to 4096 bytes, one in four 1 to
    /// 16 bytes.
    hashed,
    /// Every chunk `n` bytes (1 to 65536), the last one the rest.
    fixed: u32,
    /// One chunk per send.
    off,
};

pub const default_stream_window: u32 = 256 * 1024;
pub const max_stream_window: u32 = 16 * 1024 * 1024;
/// Chunk k of a send lands `k` times this after chunk 0 (design 17.7).
pub const chunk_spacing_ns: u64 = 1000;

const Conn = struct {
    used: bool = false,
    id: u32 = 0,
    ends: [2]StreamEnd = .{ .{}, .{} },
    /// The next sequence number of each direction, across entry kinds.
    next_seq: [2]u64 = .{ 0, 0 },
    /// Each direction's fence: no entry put on the wire lands before it
    /// (design 17.7: not before the entry sent before it). It is the landing
    /// time of the direction's last entry put on the wire, and it must stay
    /// a time of an entry that is on the wire or has landed, never of one
    /// taken off it:
    /// - `putOnWire` (a send, a release) moves it to the entry's time;
    /// - a landing leaves it, at a time not after now;
    /// - a hold at landing takes every later entry of the direction off the
    ///   wire (the pull), so nothing of the direction is on the wire after
    ///   it, and the fence becomes now (sitting 34; it kept the pulled
    ///   entries' future times before, and a release or an abort's RST
    ///   right after the pull was clamped behind them: outside review of
    ///   cb01515). Now never constrains: every new time is at least
    ///   now + `wire_latency_ns`.
    /// No other path takes an entry off the wire (a discard at an abort,
    /// `failStream` or a close takes held entries only). A release of
    /// entries held at their send, with older entries of the direction
    /// still on the wire, lands behind those: the fence is theirs.
    last_due: [2]u64 = .{ 0, 0 },
    syn_landed: bool = false,
    dirs: [2]StreamDir = .{ .{}, .{} },
    chunking: ChunkMode = .hashed,
    /// Entries of each direction in `tcp_held` (design 17.9).
    held: [2]u32 = .{ 0, 0 },
};

var conns: [max_conns]Conn = @splat(.{});
var next_conn_id: u32 = 1;

/// The kinds of TCP entry slice 1 sends. Data chunks and credits come with
/// slice 2.
pub const TcpKind = enum(u8) { syn = 1, syn_ack = 2, ack = 3, fin = 4, rst = 5, data = 6, credit = 7 };

const TcpEntry = struct {
    due_at: u64,
    conn_id: u32,
    /// Index into `conns`; `conn_id` guards it at landing.
    conn: u16,
    dir: Side,
    seq: u64,
    kind: TcpKind,
    /// A SYN's destination: it looks up the listener bound there when it
    /// lands. Unused by other kinds.
    to: Ip4,
    /// A data chunk's bytes, owned by the entry until it lands (`net_alloc`).
    bytes: []u8 = &.{},
    /// A credit's count: the receiver's `read + reclaimed` at departure.
    count: u64 = 0,
    /// Its time after `wire_latency_ns`, fixed at its send: the chunk
    /// spacing, a `.tcp` delay and its jitter. A release lands it this long
    /// after `release + wire_latency_ns` (design 17.9a, point 6).
    extra_ns: u64 = 0,
};

/// TCP entries a `.tcp` partition or silence holds, in (`conn_id`,
/// direction, sequence) order (design 17.9). Not on the wire: they never
/// make the clock stop.
var tcp_held: std.ArrayList(TcpEntry) = .empty;
/// Jitter draws of `.tcp` rules (`streamFaultDraws`).
var stream_fault_draws: u64 = 0;

/// TCP entries not yet landed, sorted by (due_at, conn_id, dir, seq):
/// landing order. Apart from the datagram `wire` so that `landDue` stays
/// as it was: TCP lands in a second pass and never draws (design 17.10).
var tcp_wire: std.ArrayList(TcpEntry) = .empty;

/// What happened to a TCP entry at its landing, in the trace and the wire
/// digest.
const StreamOutcome = enum(u8) {
    landed = 0,
    /// A SYN whose client closed before it landed (`stream_dropped_dead`).
    dropped_dead_client = 1,
    /// Landed at a dead endpoint.
    dropped_dead = 2,
    /// Landed at an endpoint that had received an RST.
    dropped_reset = 3,
    /// Data that landed after the endpoint's own `.both`.
    dropped_shut = 4,
    /// A credit for a terminal direction, or at a dead endpoint.
    dropped_stale = 5,
    /// A held entry discarded: by an abort or `failStream` of its sender,
    /// or a held SYN at its client's close (design 17.9a, point 10).
    discarded_held = 6,
    /// Landed at an endpoint `failStream` hit (design 17.8, line 1 of each
    /// landing list).
    dropped_failed = 7,
};

pub const StreamStats = struct {
    /// SYNs dropped because their client closed before they landed.
    syn_dropped_dead: u64 = 0,
    /// SYNs answered with an RST: no listener at their address.
    refused: u64 = 0,
    /// Entries that landed at a dead endpoint.
    dropped_dead: u64 = 0,
    /// Data chunks, FINs and RSTs that landed at an endpoint that had
    /// received a reset (landed or local).
    dropped_after_reset: u64 = 0,
    /// Connections retired.
    retired: u64 = 0,
    /// Credits dropped: at departure or landing, for a direction that is
    /// terminal, or at a dead endpoint (`stale_credit`); and a credit owed
    /// by an endpoint that `failStream` hit.
    stale_credit: u64 = 0,
    /// Local resets: data that landed after `.both` under `.linux`.
    local_resets: u64 = 0,
    /// Data chunks that joined an endpoint's unread bytes.
    chunks_landed: u64 = 0,
    /// Credits that raised a sender's `credited`.
    credits_landed: u64 = 0,
    /// Entries a `.tcp` partition or silence held at their send, and at
    /// their landing; entries released when the last rule holding their
    /// direction was cleared (design 17.9).
    held_at_send: u64 = 0,
    held_at_landing: u64 = 0,
    /// Entries taken off the wire because an earlier entry of their
    /// direction was held at its landing (design 17.9 item 2).
    held_behind: u64 = 0,
    released: u64 = 0,
    /// Held credits replaced by a newer one of their direction.
    credits_coalesced: u64 = 0,
    /// Held entries discarded (`StreamOutcome.discarded_held`).
    discarded_held: u64 = 0,
    /// Entries a `.tcp` impairment gave a delay or a jitter above 0.
    delayed: u64 = 0,
    /// Entries that landed at an endpoint `failStream` hit, credits apart
    /// (those are `stale_credit`).
    dropped_failed: u64 = 0,
    /// Bytes discarded in this run, by cause: the sum over every direction,
    /// kept after a connection retires.
    discarded: [@typeInfo(Discard).@"enum".field_names.len]u64 = @splat(0),
};

/// Counts `n` bytes of direction `d` as discarded for `cause`, there and in
/// the run's totals.
fn discardBytes(d: *StreamDir, cause: Discard, n: usize) void {
    d.discarded[@backingInt(cause)] += n;
    stream_stats.discarded[@backingInt(cause)] += n;
}

var stream_stats: StreamStats = .{};

/// TIME_WAIT: an active closer's address and port, reserved for the rest of
/// the run, with its socket's SO_REUSEADDR flag (design 17.5).
const Reservation = struct { ip: [4]u8, port: u16, reuse: bool };
var reservations: std.ArrayList(Reservation) = .empty;
var tcp_next_port: u16 = first_ephemeral_port;
/// The window and chunk mode a new connection starts with
/// (`setStreamWindowDefault`, `setStreamChunkingDefault`).
var window_default: u32 = default_stream_window;
var chunking_default: ChunkMode = .hashed;

fn resetStreams() void {
    for (streams) |s| {
        if (s.used) panic("sim: a stream socket outlived the last run", .{});
    }
    for (conns) |k| {
        if (k.used) panic("sim: a connection outlived the last run", .{});
    }
    if (tcp_wire.items.len != 0 or tcp_held.items.len != 0 or reservations.items.len != 0) panic("sim: TCP entries or reservations outlived the last run", .{});
    stream_fault_draws = 0;
    stream_profile = null;
    stream_used = false;
    next_conn_id = 1;
    tcp_next_port = first_ephemeral_port;
    stream_stats = .{};
    window_default = default_stream_window;
    chunking_default = .hashed;
}

fn freeStreams() void {
    for (tcp_wire.items) |w| net_alloc.free(w.bytes);
    for (tcp_held.items) |w| net_alloc.free(w.bytes);
    tcp_held.clearAndFree(net_alloc);
    for (&conns) |*k| {
        for (&k.ends) |*e| e.unread.deinit(net_alloc);
    }
    tcp_wire.clearAndFree(net_alloc);
    reservations.clearAndFree(net_alloc);
    streams = @splat(.{});
    conns = @splat(.{});
}

/// Sets the run's stream profile: which kernel the sim follows where the
/// kernels differ (design 17.8). Before the run's first stream socket, once.
pub fn setStreamProfile(p: StreamProfile) void {
    if (comptime !compiled_in) unreachable;
    refuseOtherThread("sim.setStreamProfile");
    if (!begun) panic("sim: setStreamProfile outside a run", .{});
    if (stream_used) panic("sim: setStreamProfile({t}) after the run's first stream socket opened: set it before", .{p});
    if (stream_profile) |old| panic("sim: setStreamProfile({t}) after setStreamProfile({t}): a run has one profile", .{ p, old });
    stream_profile = p;
}

pub fn streamProfile() ?StreamProfile {
    return stream_profile;
}

/// The profile, at a point where the kernels differ. With none, panics and
/// names the row.
fn needProfile(comptime row: []const u8) StreamProfile {
    return stream_profile orelse panic("sim: " ++ row ++ ": the kernels differ here, and this run set no stream profile (sim.setStreamProfile(.linux or .macos), before its first stream socket)", .{});
}

fn sockIndex(fd: i32) usize {
    return @intCast(fd - fd_base - max_ends);
}

/// Whether `fd` is an open stream socket of this run.
pub fn isStreamFd(fd: i32) bool {
    if (!isDgramFd(fd)) return false;
    return streams[sockIndex(fd)].used;
}

fn streamSlot(comptime what: []const u8, fd: i32) *StreamSock {
    if (!isStreamFd(fd)) panic("sim: " ++ what ++ " on sim fd {d}, which is not an open stream socket", .{fd});
    return &streams[sockIndex(fd)];
}

fn kindName(k: SockKind) []const u8 {
    return switch (k) {
        .open => "stream socket that is not connected",
        .listener => "listener",
        .endpoint => "connected stream socket",
    };
}

/// The lowest slot free in both tables, with its generation moved on.
fn allocSockSlot() usize {
    var j: usize = 0;
    while (j < max_dgram and (dgrams[j].used or streams[j].used)) j += 1;
    if (j == max_dgram) panic("sim: the socket table is full ({d} sockets)", .{max_dgram});
    dgram_gen[j] = std.math.add(u32, dgram_gen[j], 1) catch panic("sim: socket slot {d} was opened {d} times in this run: its generation would wrap, and an op of an old socket could act on a new one", .{ j, dgram_gen[j] });
    return j;
}

/// `socket(AF_INET, SOCK_STREAM)` during a run.
pub fn streamOpen(nonblocking: bool) i32 {
    refuseOtherThread("sim socket open");
    if (comptime builtin.os.tag == .windows) panic("sim: sim sockets are not supported on Windows", .{});
    checkFdNamespace();
    const j = allocSockSlot();
    streams[j] = .{ .used = true, .nonblocking = nonblocking };
    stream_used = true;
    return fd_base + max_ends + @as(i32, @intCast(j));
}

fn overlaps(a: [4]u8, b: [4]u8) bool {
    return isWildcard(a) or isWildcard(b) or std.mem.eql(u8, &a, &b);
}

fn holdsPort(s: StreamSock) bool {
    return s.used and (s.bound or s.kind == .endpoint);
}

fn tcpPortFree(ip: [4]u8, port: u16) bool {
    for (streams) |s| {
        if (holdsPort(s) and s.local.port == port and overlaps(ip, s.local.ip)) return false;
    }
    for (reservations.items) |r| {
        if (r.port == port and overlaps(ip, r.ip)) return false;
    }
    return true;
}

fn tcpEphemeral(ip: [4]u8) error{AddressInUse}!u16 {
    var tries: u32 = 0;
    while (tries < 65536 - @as(u32, first_ephemeral_port)) : (tries += 1) {
        const p = tcp_next_port;
        tcp_next_port = if (tcp_next_port == 65535) first_ephemeral_port else tcp_next_port + 1;
        if (tcpPortFree(ip, p)) return p;
    }
    return error.AddressInUse;
}

/// The bind rules for a fixed port: the table in the protocol comment above.
fn tcpBindCheck(fd: i32, addr: Ip4, reuse: bool) error{AddressInUse}!void {
    for (streams, 0..) |s, j| {
        if (!holdsPort(s) or s.local.port != addr.port or !overlaps(addr.ip, s.local.ip)) continue;
        if (!reuse) return error.AddressInUse;
        const holder = fd_base + max_ends + @as(i32, @intCast(j));
        if (s.kind == .listener and s.reuseaddr) {
            if (std.mem.eql(u8, &s.local.ip, &addr.ip)) return error.AddressInUse;
            switch (needProfile("row 12, a bind with SO_REUSEADDR to a port a listener holds on another address")) {
                .linux => return error.AddressInUse,
                .macos => panic("sim: a bind of sim fd {d} with SO_REUSEADDR to port {d}, which listener {d} holds on another address: refused under .macos (row 12: macOS allows it, and which listener gets a SYN was not measured)", .{ fd, addr.port, holder }),
            }
        }
        panic("sim: a bind of sim fd {d} with SO_REUSEADDR to port {d}, held by sim fd {d} (a {s}{s}): not measured, so not simulated", .{ fd, addr.port, holder, kindName(s.kind), if (s.kind == .listener) " without SO_REUSEADDR" else "" });
    }
    for (reservations.items) |r| {
        if (r.port != addr.port or !overlaps(addr.ip, r.ip)) continue;
        if (!reuse) return error.AddressInUse;
        if (r.reuse) continue;
        switch (needProfile("row 31, a bind with SO_REUSEADDR over a TIME_WAIT reservation that did not set it")) {
            .linux => return error.AddressInUse,
            .macos => {},
        }
    }
}

/// `bind` of a stream socket. Port 0 takes the next free TCP port.
pub fn streamBind(fd: i32, addr: Ip4) error{AddressInUse}!void {
    refuseOtherThread("sim socket bind");
    const s = streamSlot("bind", fd);
    if (s.kind != .open or s.bound) panic("sim: bind on sim fd {d}, which is already bound or connected (a {s}): not simulated", .{ fd, kindName(s.kind) });
    if (!isWildcard(addr.ip)) refuseNonLoopback("bind to", addr.ip);
    const port = if (addr.port == 0) try tcpEphemeral(addr.ip) else blk: {
        try tcpBindCheck(fd, addr, s.reuseaddr);
        break :blk addr.port;
    };
    s.bound = true;
    s.local = .{ .ip = addr.ip, .port = port };
}

/// `getsockname` of a stream socket: 0.0.0.0:0 before a bind or a connect.
pub fn streamLocal(fd: i32) Ip4 {
    return streamSlot("getsockname", fd).local;
}

pub fn streamListen(fd: i32, backlog: u31) void {
    refuseOtherThread("sim socket listen");
    const s = streamSlot("listen", fd);
    if (s.kind != .open) panic("sim: listen on sim fd {d}, a {s}: not simulated", .{ fd, kindName(s.kind) });
    if (!s.bound) panic("sim: listen on sim fd {d}, which is not bound: the kernel would bind 0.0.0.0 and a port; not simulated (neither user does it)", .{fd});
    s.kind = .listener;
    s.backlog = @intCast(std.math.clamp(backlog, 1, max_backlog));
}

pub fn streamSetNonBlocking(fd: i32, on: bool) void {
    refuseOtherThread("sim socket setNonBlocking");
    streamSlot("setNonBlocking", fd).nonblocking = on;
}

pub fn streamIsNonBlocking(fd: i32) bool {
    return streamSlot("isNonBlocking", fd).nonblocking;
}

/// `setsockopt` on a stream socket: the option list in the protocol comment
/// above.
pub fn streamSetOption(fd: i32, level: i32, name: u32, val: []const u8) error{OptionNotSupported}!void {
    refuseOtherThread("sim socket setsockopt");
    const s = streamSlot("setsockopt", fd);
    if (level == sys.IPPROTO.TCP and name == sys.TCP.NODELAY) {
        if (optionInt("TCP_NODELAY", fd, val) == 0) return error.OptionNotSupported;
        s.nodelay = true;
        return;
    }
    if (level == sys.SOL.SOCKET and name == sys.SO.REUSEADDR) {
        s.reuseaddr = optionInt("SO_REUSEADDR", fd, val) != 0;
        return;
    }
    if (level == sys.SOL.SOCKET and name == sys.SO.REUSEPORT) {
        _ = optionInt("SO_REUSEPORT", fd, val);
        return error.OptionNotSupported;
    }
    if (level == sys.SOL.SOCKET and name == sys.SO.LINGER) {
        // {1, 0}: a close sends an RST; {0, any}: the default. A positive
        // linger time is not simulated (nobody sets one).
        if (val.len != @sizeOf(sys.linger)) panic("sim: setsockopt SO_LINGER on sim fd {d} with {d} bytes (a struct linger is {d})", .{ fd, val.len, @sizeOf(sys.linger) });
        const l = std.mem.bytesToValue(sys.linger, val);
        if (l.onoff != 0 and l.linger != 0) panic("sim: setsockopt SO_LINGER {{1, {d}}} on sim stream socket {d}: a linger time is not simulated; only {{1, 0}} and off", .{ l.linger, fd });
        s.linger0 = l.onoff != 0;
        return;
    }
    panic("sim: setsockopt {s} (level {d}, option {d}) on sim stream socket {d} is misuse here", .{ optionName(level, name), level, name, fd });
}

/// `getsockopt` on a stream socket: TCP_NODELAY only.
pub fn streamGetOption(fd: i32, level: i32, name: u32, out: []u8) void {
    const s = streamSlot("getsockopt", fd);
    if (level == sys.SOL.SOCKET and name == sys.SO.ERROR) panic("sim: getsockopt SO_ERROR on sim stream socket {d} is refused: no sim path needs it, and both kernels clear it on read (design 17.4, rows 33 and 34)", .{fd});
    if (level != sys.IPPROTO.TCP or name != sys.TCP.NODELAY) panic("sim: getsockopt {s} (level {d}, option {d}) on sim stream socket {d} is not simulated", .{ optionName(level, name), level, name, fd });
    if (out.len != @sizeOf(c_int)) panic("sim: getsockopt on sim fd {d} into {d} bytes (an int is {d})", .{ fd, out.len, @sizeOf(c_int) });
    const v: c_int = @intFromBool(s.nodelay);
    @memcpy(out, std.mem.asBytes(&v));
}

/// `IpAddress.connect` with `local_address`, during a run: refused before
/// any side effect (design 17.4). Neither user does it. Compiled out without
/// sim.
pub inline fn refuseConnectLocalAddress() void {
    if (comptime !compiled_in) return;
    if (isRunning()) panic("sim: IpAddress.connect with local_address during a sim run: a connect from a bound stream socket is not simulated", .{});
}

/// Refuses, by name, a call on a stream socket that no sim path reaches:
/// the socket layer's own byte calls (only the native backends call them;
/// zio's stream ops are the sim's own) and the datagram calls.
pub fn refuseStream(comptime what: []const u8, fd: i32) noreturn {
    panic("sim: " ++ what ++ " on sim stream socket {d} is not simulated (design 17.4: zio's stream ops do not call it under sim)", .{fd});
}

fn tcpBefore(a: TcpEntry, b: TcpEntry) bool {
    if (a.due_at != b.due_at) return a.due_at < b.due_at;
    if (a.conn_id != b.conn_id) return a.conn_id < b.conn_id;
    if (a.dir != b.dir) return @backingInt(a.dir) < @backingInt(b.dir);
    return a.seq < b.seq;
}

/// Puts an entry from side `from` of connection `ci` on the TCP wire, due
/// `wire_latency_ns` from now. Capacity was reserved (`reserveTcp`).
fn sendTcp(ci: u16, from: Side, kind: TcpKind, to: Ip4) void {
    sendTcpAt(ci, from, kind, to, 0, &.{}, 0);
}

/// `sendTcp` for an entry due `extra_ns` after the wire latency (a chunk's
/// spacing), with a data chunk's bytes (owned by the entry from here) or a
/// credit's count. It lands no earlier than the entry sent before it in its
/// direction (design 17.7, order within a direction); equal times land in
/// sequence order.
fn sendTcpAt(ci: u16, from: Side, kind: TcpKind, to: Ip4, extra_ns: u64, bytes: []u8, count: u64) void {
    const k = &conns[ci];
    const d = @backingInt(from);
    var e: TcpEntry = .{ .due_at = 0, .conn_id = k.id, .conn = ci, .dir = from, .seq = k.next_seq[d], .kind = kind, .to = to, .bytes = bytes, .count = count, .extra_ns = extra_ns };
    k.next_seq[d] += 1;
    // A run that never set a `.tcp` rule takes none of this: its entries
    // land exactly as before slice 3 (promise 1).
    if (tcp_rule_set) {
        const from_addr, const to_addr = tcpPath(k, from);
        for (rules[0..rules_len]) |*r| {
            if (r.key.protocol() == .tcp and r.key.matches(from_addr, to_addr)) countMatched(r);
        }
        if (dropRule(.impairment, .tcp, from_addr, to_addr)) |r| {
            var add = r.imp.delay_ns;
            if (r.imp.jitter_ns != 0) {
                add += tcpJitterDraw(k.id, from, e.seq) % (r.imp.jitter_ns + 1);
                stream_fault_draws += 1;
            }
            if (add > 0) {
                e.extra_ns += add;
                stream_stats.delayed += 1;
                r.stats.delayed += 1;
            }
        }
        if (heldAtSend(k, from)) {
            stream_stats.held_at_send += 1;
            holdTcp(e);
            return;
        }
    }
    putOnWire(e);
}

/// Whether an entry that side `from` of `k` sends now is held at its send
/// (design 17.9 items 1 and 2): its direction already holds one, or a
/// `.tcp` partition or silence matches its path. Only in a run that set a
/// `.tcp` rule.
fn heldAtSend(k: *const Conn, from: Side) bool {
    if (!tcp_rule_set) return false;
    if (k.held[@backingInt(from)] > 0) return true;
    const from_addr, const to_addr = tcpPath(k, from);
    return tcpBlocked(from_addr, to_addr);
}

/// Whether a held credit of direction `dir` of `ci` is there to be
/// replaced by a credit held now (`holdTcp`).
fn heldCredit(ci: u16, dir: Side) bool {
    if (conns[ci].held[@backingInt(dir)] == 0) return false;
    for (tcp_held.items) |h| {
        if (h.conn == ci and h.dir == dir and h.kind == .credit) return true;
    }
    return false;
}

/// Side `side`'s held entries that an abort or `failStream` discards
/// (`discardHeldOutbound`): its data, FIN and credits.
fn heldOutbound(ci: u16, side: Side) usize {
    if (conns[ci].held[@backingInt(side)] == 0) return 0;
    var n: usize = 0;
    for (tcp_held.items) |h| n += @intFromBool(h.conn == ci and h.dir == side and abortDiscards(h.kind));
    return n;
}

fn abortDiscards(kind: TcpKind) bool {
    return kind == .data or kind == .fin or kind == .credit;
}

/// Puts `e` on the wire, due `wire_latency_ns + e.extra_ns` from now and no
/// earlier than the entry before it in its direction. Capacity was reserved.
fn putOnWire(entry: TcpEntry) void {
    var e = entry;
    const k = &conns[e.conn];
    const d = @backingInt(e.dir);
    e.due_at = @max(tcpLandingAfter(e.extra_ns), k.last_due[d]);
    k.last_due[d] = e.due_at;
    var pos = tcp_wire.items.len;
    while (pos > 0 and tcpBefore(e, tcp_wire.items[pos - 1])) pos -= 1;
    tcp_wire.insertAssumeCapacity(pos, e);
}

/// The addresses an entry of direction `from` travels between, for the
/// `.tcp` rules (design 17.9a, point 3): the sender's local address and its
/// peer. A server side that no SYN made (`absent`: the RST that refuses a
/// SYN) sends from the address the SYN went to.
fn tcpPath(k: *const Conn, from: Side) struct { Ip4, Ip4 } {
    const x = k.ends[@backingInt(from)];
    const y = k.ends[@backingInt(otherSide(from))];
    if (x.phase == .absent) return .{ y.peer, y.local };
    return .{ x.local, x.peer };
}

/// Whether a `.tcp` partition or silence in force matches the path.
fn tcpBlocked(from: Ip4, to: Ip4) bool {
    return dropRule(.silence, .tcp, from, to) != null or dropRule(.partition, .tcp, from, to) != null;
}

/// The jitter draw of a `.tcp` rule: a keyed SipHash of (seed, conn_id,
/// direction, sequence, "jitter"), with `chunkDraw`'s key: the run's seed
/// in the key's first 8 bytes and zeros after; the message is those fields,
/// little endian, then the 6 bytes "jitter". The jitter is this modulo
/// `jitter_ns + 1`. It never touches the run's RNG.
fn tcpJitterDraw(conn_id: u32, dir: Side, seq: u64) u64 {
    var key: [16]u8 = @splat(0);
    std.mem.writeInt(u64, key[0..8], current_seed, .little);
    var m: [19]u8 = undefined;
    std.mem.writeInt(u32, m[0..4], conn_id, .little);
    m[4] = @backingInt(dir);
    std.mem.writeInt(u64, m[5..13], seq, .little);
    @memcpy(m[13..19], "jitter");
    return std.hash.SipHash64(2, 4).toInt(&m, &key);
}

fn heldBefore(a: TcpEntry, b: TcpEntry) bool {
    if (a.conn_id != b.conn_id) return a.conn_id < b.conn_id;
    if (a.dir != b.dir) return @backingInt(a.dir) < @backingInt(b.dir);
    return a.seq < b.seq;
}

/// Holds `e` (design 17.9 items 2 and 3). Two held credits of one
/// direction coalesce to the one with the larger sequence, whichever came
/// to the list first: an entry held at its landing left before one held at
/// its send, so arriving later does not make a credit newer (outside review
/// of slice 3, first P2). The newer one's count includes the older's,
/// because a credit carries the receiver's cumulative `read + reclaimed`
/// at its departure; a newer credit whose count is not larger is an
/// invariant panic. Capacity was reserved.
fn holdTcp(e: TcpEntry) void {
    const k = &conns[e.conn];
    const d = @backingInt(e.dir);
    if (e.kind == .credit) {
        for (tcp_held.items, 0..) |h, i| {
            if (h.conn != e.conn or h.dir != e.dir or h.kind != .credit) continue;
            const newer, const older = if (e.seq > h.seq) .{ e, h } else .{ h, e };
            if (newer.count <= older.count) panic("sim: held credits of connection {d}: sequence {d} carries {d}, not more than sequence {d}'s {d} (an invariant: counts grow with the sequence)", .{ k.id, newer.seq, newer.count, older.seq, older.count });
            stream_stats.credits_coalesced += 1;
            // The held one is newer: the arriving one is dropped.
            if (h.seq > e.seq) return;
            _ = tcp_held.orderedRemove(i);
            k.held[d] -= 1;
            break;
        }
    }
    var pos = tcp_held.items.len;
    while (pos > 0 and heldBefore(e, tcp_held.items[pos - 1])) pos -= 1;
    tcp_held.insertAssumeCapacity(pos, e);
    k.held[d] += 1;
}

/// Releases every held direction that no `.tcp` partition or silence
/// matches any more, in (`conn_id`, direction) order (design 17.9 item 4).
/// Each entry lands `wire_latency_ns + extra_ns` after now, behind the
/// entry before it in its direction.
fn releaseHeld() void {
    var order: [max_conns]u16 = undefined;
    var n: usize = 0;
    for (conns, 0..) |k, ci| {
        if (!k.used or k.held[0] + k.held[1] == 0) continue;
        var pos = n;
        while (pos > 0 and conns[order[pos - 1]].id > k.id) : (pos -= 1) order[pos] = order[pos - 1];
        order[pos] = @intCast(ci);
        n += 1;
    }
    for (order[0..n]) |ci| {
        const k = &conns[ci];
        for ([_]Side{ .client, .server }) |dir| {
            const d = @backingInt(dir);
            if (k.held[d] == 0) continue;
            const from_addr, const to_addr = tcpPath(k, dir);
            if (tcpBlocked(from_addr, to_addr)) continue;
            for (tcp_wire.items) |w| {
                if (w.conn == ci and w.dir == dir) {
                    for (tcp_held.items) |h| {
                        if (h.conn == ci and h.dir == dir and h.seq < w.seq) panic("sim: connection {d} releases sequence {d} while sequence {d} of its direction is on the wire (an invariant: a held entry has nothing behind it on the wire)", .{ k.id, h.seq, w.seq });
                    }
                }
            }
            _ = tcpLandingTime();
            tcp_wire.ensureUnusedCapacity(net_alloc, k.held[d]) catch panic("sim: out of memory for a TCP wire entry", .{});
            var i: usize = 0;
            while (i < tcp_held.items.len) {
                const h = tcp_held.items[i];
                if (h.conn != ci or h.dir != dir) {
                    i += 1;
                    continue;
                }
                _ = tcp_held.orderedRemove(i);
                putOnWire(h);
                stream_stats.released += 1;
            }
            k.held[d] = 0;
        }
    }
}

/// Discards side `side`'s held data, FIN and credits: an abort (`cause`
/// `.abort`) or `failStream` (`.failed_sender`) of that side (design 17.9
/// item 7 and 17.9a point 9). Its held handshake entries stay.
fn discardHeldOutbound(ci: u16, side: Side, cause: Discard) void {
    const k = &conns[ci];
    const d = @backingInt(side);
    var i: usize = 0;
    while (k.held[d] > 0 and i < tcp_held.items.len) {
        const h = tcp_held.items[i];
        if (h.conn != ci or h.dir != side or !abortDiscards(h.kind)) {
            i += 1;
            continue;
        }
        _ = tcp_held.orderedRemove(i);
        k.held[d] -= 1;
        if (h.kind == .data) discardBytes(&k.dirs[d], cause, h.bytes.len);
        discardHeld(h);
    }
}

/// Records a held entry discarded now, and frees it.
fn discardHeld(entry: TcpEntry) void {
    var e = entry;
    e.due_at = clock_ns;
    stream_stats.discarded_held += 1;
    recordStream(e, .discarded_held);
    net_alloc.free(e.bytes);
}

/// `tcpLandingTime` plus `extra_ns`, under the same named limit.
fn tcpLandingAfter(extra_ns: u64) u64 {
    return std.math.add(u64, tcpLandingTime(), extra_ns) catch panic("sim: a TCP entry sent at {d} ns would land after maxInt(u64) ns: the sim's time limit", .{clock_ns});
}

/// When a TCP entry sent now lands. The clock can reach `maxInt(u64)`
/// (`advanceNs` saturates), and a landing time past it would wrap to the
/// past: that is the sim's time limit, a named panic, never a wrapped or
/// saturated time (outside review 5, finding 4).
fn tcpLandingTime() u64 {
    return std.math.add(u64, clock_ns, wire_latency_ns) catch panic("sim: a TCP entry sent at {d} ns would land after maxInt(u64) ns: the sim's time limit", .{clock_ns});
}

/// Room for `n` more TCP entries, and a landing time for them: every path
/// that sends calls this before it changes any state, so a limit panics
/// with nothing half done. `freed` is the entries the same step removes
/// before it adds its own (an abort's held entries, a held credit that the
/// new one replaces): the limit is on the count after the step, so a step
/// that does not raise the count past the limit is never refused (outside
/// review of slice 3, second P2). Out of memory panics: a landing that
/// must answer has no error to return.
fn reserveTcp(n: usize, freed: usize) void {
    _ = tcpLandingTime();
    // Held entries count toward the limit (design 17.9 item 10). Room is
    // made in both lists: a send goes to one or the other.
    const now_n = tcp_wire.items.len + tcp_held.items.len;
    if (freed > now_n) panic("sim: a step frees {d} TCP entries of the {d} there are (an invariant)", .{ freed, now_n });
    if (now_n - freed + n > max_tcp_entries) panic("sim: {d} TCP entries on the wire and held: the limit is max_tcp_entries, {d}", .{ now_n - freed + n, max_tcp_entries });
    tcp_wire.ensureUnusedCapacity(net_alloc, n) catch panic("sim: out of memory for a TCP wire entry", .{});
    tcp_held.ensureUnusedCapacity(net_alloc, n) catch panic("sim: out of memory for a held TCP entry", .{});
}

fn wakeStream(slot: usize) void {
    const p = streams[slot].parked orelse return;
    streams[slot].parked = null;
    pushDueAt(p.c, .stream, p.id, clock_ns);
}

fn wakeRecv(slot: usize) void {
    const p = streams[slot].parked_recv orelse return;
    streams[slot].parked_recv = null;
    pushDueAt(p.c, .stream, p.id, clock_ns);
}

fn wakeSend(slot: usize) void {
    const p = streams[slot].parked_send orelse return;
    streams[slot].parked_send = null;
    pushDueAt(p.c, .stream, p.id, clock_ns);
}

/// TCP entries on the wire or held, and credit departures owed. A test
/// sleeps while it is not 0 before `sim.end`, after it cleared the rules
/// that hold entries (design 17.15 item 3).
pub fn streamEntriesInFlight() usize {
    var n = tcp_wire.items.len + tcp_held.items.len;
    for (conns) |k| {
        if (!k.used) continue;
        for (k.dirs) |d| n += @intFromBool(d.owed_at != null);
    }
    return n;
}

/// The earliest owed credit departure after now, for the poll's clock.
fn nextOwedCredit() ?u64 {
    var min_at: ?u64 = null;
    for (conns) |k| {
        if (!k.used) continue;
        for (k.dirs) |d| {
            const t = d.owed_at orelse continue;
            if (t <= clock_ns) continue;
            if (min_at == null or t < min_at.?) min_at = t;
        }
    }
    return min_at;
}

pub fn streamStats() StreamStats {
    return stream_stats;
}

/// TIME_WAIT reservations so far in this run.
pub fn streamReservations() usize {
    return reservations.items.len;
}

/// `NetConnect` on `fd` to `to`. Parks the op (`c`, `id`) on the socket
/// after the SYN leaves; the SYN-ACK's or the RST's landing wakes it
/// (`streamHarvestConnect`). The checks before the SYN are design 17.6's
/// `NetConnect` list, line 1.
pub fn streamConnect(fd: i32, to: Ip4, c: *anyopaque, id: u32) error{AddressInUse}!void {
    refuseOtherThread("sim socket connect");
    const s = streamSlot("connect", fd);
    if (s.kind != .open) panic("sim: connect on sim fd {d}, a {s}: not simulated", .{ fd, kindName(s.kind) });
    if (s.bound) panic("sim: connect on sim fd {d}, which the program bound: a connect from a bound stream socket is not simulated (neither user does it)", .{fd});
    refuseNonLoopback("connect to", to.ip);
    if (to.port == 0) panic("sim: connect to port 0 from sim fd {d}", .{fd});
    if (!std.mem.eql(u8, &to.ip, &[4]u8{ 127, 0, 0, 1 })) {
        switch (needProfile("rows 15 and 30, a connect to 127/8 other than 127.0.0.1")) {
            .linux => {},
            .macos => panic("sim: connect from sim fd {d} to {d}.{d}.{d}.{d}: refused under .macos (rows 15 and 30: macOS's answer depends on lo0's aliases)", .{ fd, to.ip[0], to.ip[1], to.ip[2], to.ip[3] }),
        }
    }
    var ci: usize = 0;
    while (ci < max_conns and conns[ci].used) ci += 1;
    if (ci == max_conns) panic("sim: {d} connections at once: the limit is max_conns", .{max_conns});
    // A conn_id is never reused in a run, so the run's last u32 cannot be
    // handed out: the connect after it would need a successor that wraps
    // (outside review 5, finding 4). Checked, with the time limit, before
    // the port counter or any table moves.
    const after_id = std.math.add(u32, next_conn_id, 1) catch panic("sim: conn_id {d} is the last a u32 holds: conn_ids are never reused in a run, so a run makes at most {d} connections", .{ next_conn_id, std.math.maxInt(u32) - 1 });
    reserveTcp(1, 0);
    const here: [4]u8 = .{ 127, 0, 0, 1 };
    const port = try tcpEphemeral(here);
    const local: Ip4 = .{ .ip = here, .port = port };
    conns[ci] = .{ .used = true, .id = next_conn_id, .dirs = .{ .{ .window = window_default }, .{ .window = window_default } }, .chunking = chunking_default };
    next_conn_id = after_id;
    conns[ci].ends[@backingInt(Side.client)] = .{ .phase = .c_syn, .slot = @intCast(sockIndex(fd)), .local = local, .peer = to };
    s.kind = .endpoint;
    s.conn = @intCast(ci);
    s.side = .client;
    s.local = local;
    sendTcp(@intCast(ci), .client, .syn, to);
    s.parked = .{ .c = c, .id = id };
    s.unresolved = s.parked;
}

/// A woken connect at its harvest, its socket checked first
/// (`streamOpOnItsSocket`): ok if the SYN-ACK landed, `ConnectionRefused`
/// if an RST did. A later landing in the same instant does not change
/// that: it is the next call's to see.
pub fn streamHarvestConnect(fd: i32) error{ ConnectionRefused, ConnectionResetByPeer, Timeout }!void {
    const s = streamSlot("a harvested connect", fd);
    const e = conns[s.conn].ends[@backingInt(s.side)];
    // `failStream` in `C-syn` (17.9): `ConnectError` has no
    // `ConnectionTimedOut`, so a timed-out death is `Timeout`.
    if (e.failed) |how| return switch (how) {
        .reset => error.ConnectionResetByPeer,
        .timed_out => error.Timeout,
    };
    return switch (e.phase) {
        .est => {},
        .c_failed => error.ConnectionRefused,
        else => panic("sim: a woken connect on sim fd {d} found its endpoint {t}: only a SYN-ACK or an RST wakes it", .{ fd, e.phase }),
    };
}

pub const Accepted = struct { fd: i32, peer: Ip4 };

fn queueHead(slot: usize) ?usize {
    var best: ?usize = null;
    for (conns, 0..) |k, ci| {
        if (!k.used) continue;
        const e = k.ends[@backingInt(Side.server)];
        if (e.phase != .s_queued or e.listener_slot != slot or e.listener_gen != dgram_gen[slot]) continue;
        if (best == null or e.queue_pos < conns[best.?].ends[@backingInt(Side.server)].queue_pos) best = ci;
    }
    return best;
}

fn listenerCount(slot: usize) usize {
    var n: usize = 0;
    for (conns) |k| {
        if (!k.used) continue;
        const e = k.ends[@backingInt(Side.server)];
        if ((e.phase == .s_prov or e.phase == .s_queued) and e.listener_slot == slot and e.listener_gen == dgram_gen[slot]) n += 1;
    }
    return n;
}

/// Takes the queued connection `ci` into a new descriptor.
fn claim(ci: usize, nonblocking: bool) Accepted {
    const e = &conns[ci].ends[@backingInt(Side.server)];
    const j = allocSockSlot();
    streams[j] = .{ .used = true, .kind = .endpoint, .nonblocking = nonblocking, .local = e.local, .nodelay = e.nodelay, .reuseaddr = e.reuseaddr, .linger0 = e.linger0, .conn = @intCast(ci), .side = .server };
    e.phase = .est;
    e.slot = @intCast(j);
    return .{ .fd = fd_base + max_ends + @as(i32, @intCast(j)), .peer = e.peer };
}

/// `NetAccept` on `fd`, at the call (design 17.6). null: the op (`c`, `id`)
/// parked on the listener.
pub fn streamAccept(fd: i32, c: *anyopaque, id: u32, nonblocking: bool) ?Accepted {
    refuseOtherThread("sim socket accept");
    const s = streamSlot("accept", fd);
    if (s.kind != .listener) panic("sim: accept on sim fd {d}, a {s}: not simulated", .{ fd, kindName(s.kind) });
    if (s.unresolved) |u| panic("sim: a second accept while one is {s}, on sim listener {d} (op {d}; the new op {d}): a listener takes one unresolved accept at a time", .{ if (s.parked != null) "parked" else "woken and not yet harvested", fd, u.id, id });
    if (queueHead(sockIndex(fd))) |ci| return claim(ci, nonblocking);
    s.parked = .{ .c = c, .id = id };
    s.unresolved = s.parked;
    return null;
}

/// A woken accept at its harvest, its listener checked first
/// (`streamOpOnItsSocket`): claims the queue head now.
pub fn streamHarvestAccept(fd: i32, nonblocking: bool) Accepted {
    const ci = queueHead(sockIndex(fd)) orelse panic("sim: a woken accept on sim listener {d} found its queue empty: only an accept takes from it, and a second refuses while this one is unresolved", .{fd});
    return claim(ci, nonblocking);
}

// ---- Slice 2: bytes (design 17.7 and 17.8) ----

/// What a receive or a send evaluation gives: a count, an error, or "park".
pub const StreamResult = union(enum) { n: usize, err: StreamError, park };
pub const StreamError = error{ ConnectionResetByPeer, ConnectionTimedOut, BrokenPipe };

fn failError(how: StreamFailure) StreamError {
    return switch (how) {
        .reset => error.ConnectionResetByPeer,
        .timed_out => error.ConnectionTimedOut,
    };
}

fn endOf(s: *const StreamSock) *StreamEnd {
    return &conns[s.conn].ends[@backingInt(s.side)];
}

/// The socket behind `fd` for a byte call: an endpoint's descriptor whose
/// endpoint is `est`. Anything else is design 17.8's line 1: refused.
fn estSlot(comptime what: []const u8, fd: i32) *StreamSock {
    const s = streamSlot(what, fd);
    if (s.kind != .endpoint) panic("sim: " ++ what ++ " on sim fd {d}, a {s}: not simulated", .{ fd, kindName(s.kind) });
    const e = endOf(s);
    if (e.phase != .est) panic("sim: " ++ what ++ " on sim fd {d}, whose connection is in phase {t}, not established: not simulated", .{ fd, e.phase });
    return s;
}

/// Whether data direction `data` (from side `data`) has ended its window
/// accounting: its sender is dead, failed, or has received a reset (design
/// 17.7).
fn dirTerminal(k: *const Conn, data: Side) bool {
    const x = k.ends[@backingInt(data)];
    return x.phase == .dead or x.phase == .absent or x.reset_received or x.failed != null;
}

/// Moves the unread bytes of side `side` to a discard cause.
fn discardUnread(ci: u16, side: Side, cause: Discard) usize {
    const e = &conns[ci].ends[@backingInt(side)];
    const n = e.unreadLen();
    discardBytes(&conns[ci].dirs[@backingInt(otherSide(side))], cause, n);
    e.unread.clearRetainingCapacity();
    e.unread_head = 0;
    return n;
}

/// Copies unread bytes of `e` into `bufs`, as many as fit.
fn copyOutUnread(e: *StreamEnd, bufs: anytype) usize {
    var n: usize = 0;
    for (bufs) |b| {
        const dst = bufBytes(b);
        const src = e.unread.items[e.unread_head..];
        const m = @min(dst.len, src.len);
        @memcpy(dst[0..m], src[0..m]);
        e.unread_head += m;
        n += m;
        if (e.unread_head == e.unread.items.len) break;
    }
    if (e.unread_head == e.unread.items.len) {
        e.unread.clearRetainingCapacity();
        e.unread_head = 0;
    } else if (e.unread_head >= 4096 and e.unread_head * 2 >= e.unread.items.len) {
        const rest = e.unread.items.len - e.unread_head;
        std.mem.copyForwards(u8, e.unread.items[0..rest], e.unread.items[e.unread_head..]);
        e.unread.shrinkRetainingCapacity(rest);
        e.unread_head = 0;
    }
    return n;
}

/// Design 17.8, the receive evaluation.
fn recvEval(s: *StreamSock, bufs: anytype) StreamResult {
    const ci = s.conn;
    const e = endOf(s);
    // Lines 1 and 2: `failStream`. One op reports it; which one is the
    // harvest order (17.9).
    if (e.failed) |how| {
        if (e.fail_reported) return .{ .n = 0 };
        e.fail_reported = true;
        return .{ .err = failError(how) };
    }
    if (e.unreadLen() > 0) {
        const n = copyOutUnread(e, bufs);
        conns[ci].dirs[@backingInt(otherSide(s.side))].read += n;
        creditOwed(ci, otherSide(s.side));
        return .{ .n = n };
    }
    if (e.reset_received and !e.reset_reported) {
        if (!e.fin_before_reset) {
            // Both kernels report it once (rows 20, 23, 26, 36, 39).
            e.reset_reported = true;
            return .{ .err = error.ConnectionResetByPeer };
        }
        switch (needProfile("rows 8 and 22, a read that meets a reset after a FIN had landed")) {
            // The pending error is `pipe`: a read gives 0 and leaves it.
            .linux => {},
            .macos => {
                e.reset_reported = true;
                return .{ .err = error.ConnectionResetByPeer };
            },
        }
    }
    if (e.fin_in or e.shut_rd or e.reset_received) return .{ .n = 0 };
    return .park;
}

/// Design 17.8, the send evaluation (lines 3 to 6). The reset comes before
/// the own FIN (review 3, finding 4; row 39).
fn sendEval(s: *StreamSock, bufs: anytype) StreamResult {
    const ci = s.conn;
    const e = endOf(s);
    // Lines 1 and 2: `failStream`.
    if (e.failed) |how| {
        if (e.fail_reported) return .{ .err = error.BrokenPipe };
        e.fail_reported = true;
        return .{ .err = failError(how) };
    }
    if (e.reset_received) {
        if (e.reset_reported) return .{ .err = error.BrokenPipe };
        // Line 3a: the send that was parked when the reset arrived, at its
        // harvest (it is the endpoint's one unresolved send then; at a
        // call there is none). zio's readiness backends end a parked op
        // with SO_ERROR, which reports the reset and clears it (t-4119's
        // ruling: the sim follows zio on the kernel). With no FIN first,
        // both kernels then give ECONNRESET (rows 23, 25). After a FIN,
        // Linux's SO_ERROR is EPIPE and macOS's ECONNRESET (row 41).
        const parked_at_reset = if (s.unresolved_send) |u| e.reset_parked_send == u.id else false;
        if (parked_at_reset) {
            e.reset_reported = true;
            if (!e.fin_before_reset) return .{ .err = error.ConnectionResetByPeer };
            return switch (needProfile("row 41, a send parked when a reset arrives after a FIN")) {
                .linux => .{ .err = error.BrokenPipe },
                .macos => .{ .err = error.ConnectionResetByPeer },
            };
        }
        if (e.fin_before_reset) {
            // Both kernels give EPIPE; Linux consumes its pending `pipe`.
            if (stream_profile == .linux) e.reset_reported = true;
            return .{ .err = error.BrokenPipe };
        }
        switch (needProfile("rows 26b, 36 and 39, a send that meets a reset with no FIN before it")) {
            .linux => {
                e.reset_reported = true;
                return .{ .err = error.ConnectionResetByPeer };
            },
            .macos => return .{ .err = error.BrokenPipe },
        }
    }
    if (e.fin_out) return .{ .err = error.BrokenPipe };
    const room = conns[ci].dirs[@backingInt(s.side)].room();
    if (room == 0) return .park;
    const n: usize = @intCast(@min(room, totalLen(bufs)));
    admit(ci, s.side, bufs, n);
    return .{ .n = n };
}

/// The chunk-size draw: a keyed SipHash of (seed, conn_id, direction,
/// data_send_index, chunk_index, "chunk"). The key is the run's seed in
/// its first 8 bytes and zeros after; the message is those fields, little
/// endian, then the 5 bytes "chunk". It never touches the run's RNG.
fn chunkDraw(conn_id: u32, dir: Side, send_index: u64, k: u64) u64 {
    var key: [16]u8 = @splat(0);
    std.mem.writeInt(u64, key[0..8], current_seed, .little);
    var m: [26]u8 = undefined;
    std.mem.writeInt(u32, m[0..4], conn_id, .little);
    m[4] = @backingInt(dir);
    std.mem.writeInt(u64, m[5..13], send_index, .little);
    std.mem.writeInt(u64, m[13..21], k, .little);
    @memcpy(m[21..26], "chunk");
    return std.hash.SipHash64(2, 4).toInt(&m, &key);
}

/// Chunk `k`'s size, with `rest` bytes still to cut (design 17.7).
fn chunkSize(k: *const Conn, dir: Side, idx: u64, rest: usize) usize {
    const want: u64 = switch (k.chunking) {
        .off => rest,
        .fixed => |n| n,
        .hashed => blk: {
            const h = chunkDraw(k.id, dir, k.dirs[@backingInt(dir)].data_send_index, idx);
            break :blk if (h & 3 == 0) 1 + (h >> 2) % 16 else 512 + (h >> 2) % 3585;
        },
    };
    return @intCast(@min(want, rest));
}

/// A position in a list of buffers, for gathering a send into chunks
/// without walking the list from its start for each chunk.
const BufCursor = struct {
    i: usize = 0,
    off: usize = 0,

    fn take(c: *BufCursor, bufs: anytype, dst: []u8) void {
        var got: usize = 0;
        while (got < dst.len) {
            const src = bufBytes(bufs[c.i])[c.off..];
            const m = @min(src.len, dst.len - got);
            @memcpy(dst[got..][0..m], src[0..m]);
            got += m;
            c.off += m;
            if (c.off == bufBytes(bufs[c.i]).len) {
                c.i += 1;
                c.off = 0;
            }
        }
    }
};

/// Admits `n` bytes of `bufs` from side `side`: the chunk plan first, its
/// entry count and the last chunk's landing time checked before anything
/// moves (design 17.7, admission), then one entry per chunk.
fn admit(ci: u16, side: Side, bufs: anytype, n: usize) void {
    const k = &conns[ci];
    var chunks: usize = 0;
    var rest = n;
    while (rest > 0) : (chunks += 1) rest -= chunkSize(k, side, chunks, rest);
    if (tcp_wire.items.len + chunks > max_tcp_entries) panic("sim: a send of {d} bytes on connection {d} needs {d} chunks: with {d} TCP entries on the wire that is past max_tcp_entries, {d} (design 17.7: a smaller window or a larger chunk mode)", .{ n, k.id, chunks, tcp_wire.items.len, max_tcp_entries });
    _ = tcpLandingAfter((chunks - 1) * chunk_spacing_ns);
    reserveTcp(chunks, 0);
    const peer = k.ends[@backingInt(side)].peer;
    var cur: BufCursor = .{};
    rest = n;
    var idx: usize = 0;
    while (rest > 0) : (idx += 1) {
        const size = chunkSize(k, side, idx, rest);
        const bytes = net_alloc.alloc(u8, size) catch panic("sim: out of memory for a TCP data chunk of {d} bytes", .{size});
        cur.take(bufs, bytes);
        sendTcpAt(ci, side, .data, peer, idx * chunk_spacing_ns, bytes, 0);
        rest -= size;
    }
    const d = &k.dirs[@backingInt(side)];
    d.admitted += n;
    d.data_send_index += 1;
}

/// A read (or a window release) of data direction `data` owes a credit:
/// it departs now if none departed in the last L, else at `last + L`,
/// carrying the count at that moment (design 17.7).
fn creditOwed(ci: u16, data: Side) void {
    const d = &conns[ci].dirs[@backingInt(data)];
    switch (creditPlan(ci, data)) {
        .joins_owed => {},
        .owe => d.owed_at = d.last_credit_at.? +| wire_latency_ns,
        .departs, .stale => departCredit(ci, data),
    }
}

/// What a read or a release of data direction `data` does with its credit
/// now (design 17.7). Only `.departs` puts an entry on the wire now, so a
/// caller that reserves wire room before it changes state reserves for that
/// alone (outside review of slice 2, P2: `.both` reserved for a credit that
/// was already owed).
const CreditPlan = enum {
    /// A credit is already owed: it departs at its time with the newer count.
    joins_owed,
    /// One departed less than L ago: this one is owed for L after it.
    owe,
    /// It leaves now.
    departs,
    /// The direction is terminal: nothing leaves (`stale_credit`).
    stale,
};

fn creditPlan(ci: u16, data: Side) CreditPlan {
    const d = conns[ci].dirs[@backingInt(data)];
    if (d.owed_at != null) return .joins_owed;
    if (d.last_credit_at) |t| {
        if (clock_ns < t +| wire_latency_ns) return .owe;
    }
    return if (dirTerminal(&conns[ci], data)) .stale else .departs;
}

/// A credit for data direction `data` leaves its receiver now, unless the
/// direction is terminal: then nothing leaves (`stale_credit`).
fn departCredit(ci: u16, data: Side) void {
    const k = &conns[ci];
    const d = &k.dirs[@backingInt(data)];
    if (dirTerminal(k, data)) {
        d.owed_at = null;
        stream_stats.stale_credit += 1;
        return;
    }
    const r = otherSide(data);
    // A credit held now replaces a held one of its direction: one in, one
    // out (`holdTcp`; it is the newest, so it is the one kept).
    reserveTcp(1, @intFromBool(heldAtSend(k, r) and heldCredit(ci, r)));
    d.owed_at = null;
    d.last_credit_at = clock_ns;
    sendTcpAt(ci, r, .credit, k.ends[@backingInt(r)].peer, 0, &.{}, d.read + d.reclaimed);
}

/// The owed credits due by now, in (`conn_id`, direction) order, after the
/// entries that land at this instant (design 17.7).
fn departOwedCredits() void {
    while (true) {
        var best: ?usize = null;
        var best_d: u1 = 0;
        for (conns, 0..) |k, ci| {
            if (!k.used) continue;
            for (k.dirs, 0..) |d, di| {
                const t = d.owed_at orelse continue;
                if (t > clock_ns) continue;
                if (best == null or k.id < conns[best.?].id) {
                    best = ci;
                    best_d = @intCast(di);
                }
            }
        }
        const ci = best orelse return;
        departCredit(@intCast(ci), @fromBackingInt(@intCast(best_d)));
        maybeRetire(ci);
    }
}

/// `NetRecv` on stream socket `fd`, at the call (design 17.8). `.park`: the
/// op (`c`, `id`) parked on the socket.
pub fn streamRecv(fd: i32, bufs: anytype, c: *anyopaque, id: u32) StreamResult {
    refuseOtherThread("sim socket receive");
    const s = estSlot("NetRecv", fd);
    checkIovecs("a receive", fd, bufs.len);
    if (totalLen(bufs) == 0) panic("sim: a receive of 0 bytes on sim stream socket {d} is refused (t-4096's ruling; design 17.8, NetRecv line 1)", .{fd});
    if (s.unresolved_recv) |u| panic("sim: a second receive while one is {s}, on sim stream socket {d} (op {d}; the new op {d}): an endpoint takes one unresolved receive at a time", .{ if (s.parked_recv != null) "parked" else "woken and not yet harvested", fd, u.id, id });
    const r = recvEval(s, bufs);
    if (r == .park) {
        s.parked_recv = .{ .c = c, .id = id };
        s.unresolved_recv = s.parked_recv;
    }
    return r;
}

/// A woken receive at its harvest, its socket checked first
/// (`streamOpOnItsSocket`).
pub fn streamHarvestRecv(fd: i32, bufs: anytype) StreamResult {
    const s = streamSlot("a harvested receive", fd);
    const r = recvEval(s, bufs);
    if (r == .park) panic("sim: a woken receive on sim stream socket {d} has no result: only a receive takes bytes, and a second refuses while this one is unresolved (an invariant)", .{fd});
    return r;
}

/// `NetSend` on stream socket `fd`, at the call (design 17.8).
pub fn streamSend(fd: i32, bufs: anytype, c: *anyopaque, id: u32) StreamResult {
    refuseOtherThread("sim socket send");
    const s = estSlot("NetSend", fd);
    checkIovecs("a send", fd, bufs.len);
    if (totalLen(bufs) == 0) return .{ .n = 0 };
    if (s.unresolved_send) |u| panic("sim: a second send while one is {s}, on sim stream socket {d} (op {d}; the new op {d}): an endpoint takes one unresolved send at a time", .{ if (s.parked_send != null) "parked" else "woken and not yet harvested", fd, u.id, id });
    const r = sendEval(s, bufs);
    if (r == .park) {
        s.parked_send = .{ .c = c, .id = id };
        s.unresolved_send = s.parked_send;
    }
    return r;
}

/// A woken send at its harvest, its socket checked first.
pub fn streamHarvestSend(fd: i32, bufs: anytype) StreamResult {
    const s = streamSlot("a harvested send", fd);
    const r = sendEval(s, bufs);
    if (r == .park) panic("sim: a woken send on sim stream socket {d} has no room: only a send uses room, and a window change while it is woken refuses (an invariant)", .{fd});
    return r;
}

/// `shutdown` of a stream socket (design 17.8). `.receive` is refused.
pub fn streamShutdown(fd: i32, how: ShutdownHow) error{SocketUnconnected}!void {
    refuseOtherThread("sim socket shutdown");
    const s = estSlot("shutdown", fd);
    if (how == .receive) panic("sim: shutdown(.receive) on sim stream socket {d} is refused: the kernels differ (rows 6 and 17) and nobody calls it (design 17.4)", .{fd});
    const ci = s.conn;
    const e = endOf(s);
    // Line 2, before line 3 (rows 37, 38 and 39w). `.both` stops here too.
    if (e.reset_received or e.failed != null) return error.SocketUnconnected;
    const shut_rd = how == .both and !e.shut_rd;
    const discards = shut_rd and e.unreadLen() > 0;
    // Before anything moves: the profile, and room on the wire.
    const p: ?StreamProfile = if (discards) needProfile("row 17, shutdown(.both) with unread bytes") else stream_profile;
    const releases = discards and p == .macos;
    // Only what leaves now: the FIN, and the release's credit if it
    // departs now rather than joining one already owed or paced.
    const credit_now = releases and creditPlan(ci, otherSide(s.side)) == .departs;
    const leaving = @as(usize, @intFromBool(!e.fin_out)) + @intFromBool(credit_now);
    // The FIN and the credit go the same way: the credit is held exactly
    // when the FIN is (`heldAtSend` before either), and then it replaces a
    // held credit of that direction, which leaves the count.
    if (leaving > 0) reserveTcp(leaving, @intFromBool(credit_now and heldAtSend(&conns[ci], s.side) and heldCredit(ci, s.side)));
    if (!e.fin_out) {
        e.fin_out = true;
        sendTcp(ci, s.side, .fin, e.peer);
        if (!e.fin_in) reservations.append(net_alloc, .{ .ip = e.local.ip, .port = e.local.port, .reuse = s.reuseaddr }) catch panic("sim: out of memory for a TIME_WAIT reservation", .{});
        wakeSend(sockIndex(fd));
    }
    if (!shut_rd) return;
    e.shut_rd = true;
    wakeRecv(sockIndex(fd));
    if (releases) {
        // The window release (design 17.7): room freed without a read.
        const n = discardUnread(ci, s.side, .both_discard);
        conns[ci].dirs[@backingInt(otherSide(s.side))].reclaimed += n;
        creditOwed(ci, otherSide(s.side));
    }
}

/// `getpeername` of a stream socket: the peer while established and not
/// reset; null where a kernel answers ENOTCONN or EINVAL (row 9), which the
/// layer maps to `SocketUnconnected` in every build.
pub fn streamPeer(fd: i32) ?Ip4 {
    const s = streamSlot("getpeername", fd);
    if (s.kind != .endpoint) return null;
    const e = endOf(s);
    if (e.phase != .est or e.reset_received or e.failed != null) return null;
    return e.peer;
}

/// `FIONREAD` of a stream socket: its unread bytes (design 17.4).
pub fn streamBytesReadable(fd: i32) usize {
    return endOf(estSlot("bytesReadable (FIONREAD)", fd)).unreadLen();
}

fn checkWindow(bytes: u32) void {
    if (bytes == 0 or bytes > max_stream_window) panic("sim: a stream window of {d} bytes: the bounds are 1 to {d} (design 17.7)", .{ bytes, max_stream_window });
}

fn checkChunkMode(mode: ChunkMode) void {
    switch (mode) {
        .fixed => |n| if (n == 0 or n > 65536) panic("sim: chunk mode .fixed = {d}: the bounds are 1 to 65536 (design 17.7)", .{n}),
        else => {},
    }
}

/// The window of connections made from now on, each direction.
pub fn setStreamWindowDefault(bytes: u32) void {
    refuseOtherThread("sim.setStreamWindowDefault");
    if (!begun) panic("sim: setStreamWindowDefault outside a run", .{});
    checkWindow(bytes);
    window_default = bytes;
}

/// The window of the direction toward `fd`, at once and with no wire
/// entry (design 17.7). Refused while the sender's send is woken and not
/// harvested. A grow wakes a parked send if room appears; a shrink below
/// what is in use discards nothing.
pub fn setStreamWindow(fd: i32, bytes: u32) void {
    refuseOtherThread("sim.setStreamWindow");
    const s = estSlot("setStreamWindow", fd);
    checkWindow(bytes);
    const k = &conns[s.conn];
    const sender = otherSide(s.side);
    const x = k.ends[@backingInt(sender)];
    const live_sender = x.phase == .est;
    if (live_sender) {
        const ps = streams[x.slot];
        if (ps.unresolved_send != null and ps.parked_send == null) panic("sim: setStreamWindow on sim fd {d} while its peer's send is woken and not yet harvested: refused (design 17.7)", .{fd});
    }
    const d = &k.dirs[@backingInt(sender)];
    d.window = bytes;
    if (live_sender and d.room() > 0) wakeSend(x.slot);
}

/// The chunk mode of connections made from now on.
pub fn setStreamChunkingDefault(mode: ChunkMode) void {
    refuseOtherThread("sim.setStreamChunkingDefault");
    if (!begun) panic("sim: setStreamChunkingDefault outside a run", .{});
    checkChunkMode(mode);
    chunking_default = mode;
}

/// The chunk mode of `fd`'s connection, both directions, for sends from
/// now on.
pub fn setStreamChunking(fd: i32, mode: ChunkMode) void {
    refuseOtherThread("sim.setStreamChunking");
    const s = estSlot("setStreamChunking", fd);
    checkChunkMode(mode);
    conns[s.conn].chunking = mode;
}

/// Starts an ev op on stream socket `fd`; records the slot's generation.
pub fn streamOpStart(c: *anyopaque, fd: i32) u32 {
    const id = allocOpId(c);
    live[live_len - 1].on_stream = true;
    if (isStreamFd(fd)) live[live_len - 1].sock_gen = dgram_gen[sockIndex(fd)];
    return id;
}

/// Whether the live op (`c`, `id`) still has the stream socket it was
/// submitted on: open, and of the generation it recorded. False after a
/// close, also when a new socket reused the number since (in the same
/// harvest batch, as for datagram sockets: `dgramOpOnItsSocket`).
pub fn streamOpOnItsSocket(c: *anyopaque, id: u32, fd: i32) bool {
    for (live[0..live_len]) |l| {
        if (l.c == c and l.id == id) {
            return isStreamFd(fd) and l.sock_gen != 0 and dgram_gen[sockIndex(fd)] == l.sock_gen;
        }
    }
    panic("sim: op {d} on sim fd {d} is not live at its harvest", .{ id, fd });
}

pub fn streamOpDone(c: *anyopaque, id: u32) void {
    pushDueAt(c, .stream, id, clock_ns);
}

/// Whether the live op (`c`, `id`) was submitted on a stream socket.
pub fn isStreamOp(c: *anyopaque, id: u32) bool {
    for (live[0..live_len]) |l| {
        if (l.c == c and l.id == id) return l.on_stream;
    }
    return false;
}

/// `close` of a stream socket (design 17.6 and 17.8). Wakes an op waiting
/// on it: its harvest ends it `FileDescriptorNotASocket` (the generation).
/// How `failStream` kills an endpoint's transport.
pub const StreamFailure = enum(u8) { reset = 1, timed_out = 2 };

/// A local transport death of the endpoint behind `fd`, at the virtual time
/// of the call: the host on the other side vanished, or the kernel gave up
/// (design 17.9). Nothing goes on the wire, and the peer is not told.
/// - In `C-syn`: the parked connect (if any) fails now,
///   `ConnectionResetByPeer` for `.reset` and `Timeout` for `.timed_out`
///   (`ConnectError` has no `ConnectionTimedOut`); the endpoint is
///   `C-failed`, and a SYN-ACK that lands later gets no answer.
/// - In `est`: a parked receive and a parked send are woken now. The first
///   one harvested (the seed's harvest order) gets `ConnectionResetByPeer`
///   or `ConnectionTimedOut`; after that, receives give 0 and sends
///   `BrokenPipe`. Its held data, FIN and credits are discarded
///   (`failed_sender`), its unread bytes too (`failed_receiver`), and a
///   credit it owed never leaves (`stale_credit`). Its entries already on
///   the wire still land at the peer. Entries that land on it later are
///   dropped, with no answer. `shutdown` gives `SocketUnconnected`, and a
///   close puts nothing on the wire.
/// Refused: an fd that is not a connection's endpoint; any other phase; an
/// endpoint that already failed, or that received a reset (its transport is
/// already over).
pub fn failStream(fd: i32, how: StreamFailure) void {
    if (comptime !compiled_in) unreachable;
    refuseOtherThread("sim.failStream");
    if (!begun) panic("sim: failStream outside a sim run", .{});
    const s = streamSlot("failStream", fd);
    if (s.kind != .endpoint) panic("sim: failStream on sim fd {d}, a {s}: only a connection's endpoint can fail", .{ fd, kindName(s.kind) });
    const ci = s.conn;
    const e = endOf(s);
    if (e.failed != null) panic("sim: failStream on sim fd {d}, which already failed", .{fd});
    if (e.reset_received) panic("sim: failStream on sim fd {d}, which has received a reset: its transport is already over", .{fd});
    const j = sockIndex(fd);
    switch (e.phase) {
        .c_syn => {
            e.phase = .c_failed;
            e.failed = how;
            wakeStream(j);
        },
        .est => {
            e.failed = how;
            discardHeldOutbound(ci, s.side, .failed_sender);
            const din = &conns[ci].dirs[@backingInt(otherSide(s.side))];
            if (din.owed_at != null) {
                din.owed_at = null;
                stream_stats.stale_credit += 1;
            }
            _ = discardUnread(ci, s.side, .failed_receiver);
            wakeRecv(j);
            wakeSend(j);
        },
        else => panic("sim: failStream on sim fd {d}, whose connection is in phase {t}: only C-syn and est are simulated", .{ fd, e.phase }),
    }
}

/// A client's SYN that is still held, discarded at its close (design 17.9
/// item 9): it never makes server state, and the connection can retire.
fn discardHeldSyn(ci: u16) void {
    const k = &conns[ci];
    if (k.held[@backingInt(Side.client)] == 0) return;
    for (tcp_held.items, 0..) |h, i| {
        if (h.conn != ci or h.dir != .client or h.kind != .syn) continue;
        _ = tcp_held.orderedRemove(i);
        k.held[@backingInt(Side.client)] -= 1;
        stream_stats.syn_dropped_dead += 1;
        discardHeld(h);
        return;
    }
}

pub fn streamClose(fd: i32) void {
    refuseOtherThread("sim socket close");
    const s = streamSlot("close", fd);
    const j = sockIndex(fd);
    wakeStream(j);
    wakeRecv(j);
    wakeSend(j);
    switch (s.kind) {
        .open => {},
        .listener => closeListener(j),
        .endpoint => closeEndpoint(s.conn, s.side, s.reuseaddr, s.linger0),
    }
    streams[j] = .{};
}

fn closeListener(j: usize) void {
    for (&conns, 0..) |*k, ci| {
        if (!k.used) continue;
        const e = &k.ends[@backingInt(Side.server)];
        if (e.phase != .s_prov and e.phase != .s_queued) continue;
        if (e.listener_slot != j or e.listener_gen != dgram_gen[j]) continue;
        const answer = !e.reset_received and !e.rst_sent;
        if (answer) reserveTcp(1, 0);
        _ = discardUnread(@intCast(ci), .server, .listener_closed);
        e.phase = .dead;
        if (answer) {
            e.rst_sent = true;
            sendTcp(@intCast(ci), .server, .rst, e.peer);
        }
        maybeRetire(ci);
    }
}

/// Close of an endpoint's descriptor (design 17.8, close; 17.6 for the
/// client phases).
fn closeEndpoint(ci: u16, side: Side, reuse: bool, linger0: bool) void {
    const e = &conns[ci].ends[@backingInt(side)];
    switch (e.phase) {
        // The SYN is dropped at its landing if it is on the wire, and
        // discarded now if it is held (design 17.9 item 9); if it landed,
        // the SYN-ACK is answered with an RST at its landing.
        .c_syn, .c_failed => discardHeldSyn(ci),
        .est => {
            // Line 2: after a reset or a failure nothing goes on the wire;
            // an endpoint sends at most one RST in its life.
            const quiet = e.reset_received or e.rst_sent or e.failed != null;
            const rst = !quiet and (e.unreadLen() > 0 or linger0);
            const fin = !quiet and !rst and !e.fin_out;
            // An RST first discards the held data, FIN and credits it
            // goes behind (line 3 below): those leave the count.
            if (rst or fin) reserveTcp(1, if (rst) heldOutbound(ci, side) else 0);
            // Line 1, always (review 3, finding 6).
            _ = discardUnread(ci, side, .unread_at_close);
            if (rst) {
                // Line 3: its held data, FIN and credits go first, so its
                // RST is not held behind them.
                discardHeldOutbound(ci, side, .abort);
                e.rst_sent = true;
                sendTcp(ci, side, .rst, e.peer);
            } else if (fin) {
                e.fin_out = true;
                sendTcp(ci, side, .fin, e.peer);
                if (!e.fin_in) {
                    reservations.append(net_alloc, .{ .ip = e.local.ip, .port = e.local.port, .reuse = reuse }) catch panic("sim: out of memory for a TIME_WAIT reservation", .{});
                }
            }
        },
        else => panic("sim: close of an endpoint in phase {t}", .{e.phase}),
    }
    e.phase = .dead;
    maybeRetire(ci);
}

fn maybeRetire(ci: usize) void {
    const k = &conns[ci];
    if (!k.used) return;
    for (k.ends) |e| {
        if (e.phase != .dead and e.phase != .absent) return;
    }
    for (tcp_wire.items) |w| {
        if (w.conn == ci) return;
    }
    if (k.held[0] + k.held[1] != 0) return;
    for (k.dirs) |d| {
        if (d.owed_at != null) return;
    }
    for (&k.ends) |*e| e.unread.deinit(net_alloc);
    k.* = .{};
    stream_stats.retired += 1;
}

/// The listener a SYN to `to` reaches: bound to `to` exactly, else to
/// 0.0.0.0 on its port.
fn listenerAt(to: Ip4) ?usize {
    var wildcard: ?usize = null;
    for (streams, 0..) |s, j| {
        if (!s.used or s.kind != .listener or s.local.port != to.port) continue;
        if (std.mem.eql(u8, &s.local.ip, &to.ip)) return j;
        if (isWildcard(s.local.ip)) wildcard = j;
    }
    return wildcard;
}

/// Lands every TCP entry due by now, in (landing time, `conn_id`,
/// direction, sequence) order. An answer a landing sends is due later, so it
/// is not in this pass.
fn landStreamDue() void {
    while (tcp_wire.items.len > 0 and tcp_wire.items[0].due_at <= clock_ns) {
        const e = tcp_wire.orderedRemove(0);
        if (tcp_rule_set and holdAtLanding(e)) continue;
        landStream(e);
    }
}

/// Holds `e` at its landing if a `.tcp` partition or silence matches it
/// now, or its direction already holds an entry (design 17.9 items 1, 2
/// and 6). A drop rule that matches it now was set after its send (one in
/// force then would have held it), so it counts as matched here.
fn holdAtLanding(e: TcpEntry) bool {
    const k = &conns[e.conn];
    if (!k.used or k.id != e.conn_id) panic("sim: a TCP entry of connection {d} landed after the connection retired (the guard that cannot fire by construction)", .{e.conn_id});
    const from_addr, const to_addr = tcpPath(k, e.dir);
    var matched = false;
    for (rules[0..rules_len]) |*r| {
        if (r.key != .impairment and r.key.protocol() == .tcp and r.key.matches(from_addr, to_addr)) {
            countMatched(r);
            matched = true;
        }
    }
    if (!matched and k.held[@backingInt(e.dir)] == 0) return false;
    // The later entries of its direction that are still on the wire are
    // held behind it now (design 17.9 item 2), not at their own landing: a
    // release before they land would put them ahead of it, and an abort
    // would discard it and leave them to be delivered after a hole (sitting
    // 33). So a direction that holds an entry has none on the wire behind
    // it (`releaseHeld` checks this).
    var later: usize = 0;
    for (tcp_wire.items) |w| later += @intFromBool(w.conn == e.conn and w.dir == e.dir);
    tcp_held.ensureUnusedCapacity(net_alloc, 1 + later) catch panic("sim: out of memory for a held TCP entry", .{});
    stream_stats.held_at_landing += 1;
    holdTcp(e);
    var i: usize = 0;
    while (later > 0 and i < tcp_wire.items.len) {
        const w = tcp_wire.items[i];
        if (w.conn != e.conn or w.dir != e.dir) {
            i += 1;
            continue;
        }
        _ = tcp_wire.orderedRemove(i);
        later -= 1;
        stream_stats.held_behind += 1;
        holdTcp(w);
    }
    // Nothing of the direction is on the wire now: its fence must not be
    // a pulled entry's time (`Conn.last_due`).
    for (tcp_wire.items) |w| {
        if (w.conn == e.conn and w.dir == e.dir) panic("sim: connection {d}: sequence {d} of a direction that holds an entry is still on the wire after the pull (an invariant)", .{ k.id, w.seq });
    }
    k.last_due[@backingInt(e.dir)] = clock_ns;
    return true;
}

fn landStream(e: TcpEntry) void {
    const k = &conns[e.conn];
    if (!k.used or k.id != e.conn_id) panic("sim: a TCP entry of connection {d} landed after the connection retired (the guard that cannot fire by construction)", .{e.conn_id});
    const at = otherSide(e.dir);
    const outcome: StreamOutcome = switch (e.kind) {
        .syn => landSyn(e),
        .syn_ack => landSynAck(e.conn),
        .ack => landAck(e.conn),
        .fin => landFin(e.conn, at),
        .rst => landRst(e.conn, at),
        .data => landData(e),
        .credit => landCredit(e.conn, at, e.count),
    };
    switch (outcome) {
        .landed, .dropped_shut, .dropped_stale => {},
        .dropped_dead_client => stream_stats.syn_dropped_dead += 1,
        .dropped_dead => stream_stats.dropped_dead += 1,
        .dropped_reset => stream_stats.dropped_after_reset += 1,
        .dropped_failed => stream_stats.dropped_failed += 1,
        .discarded_held => unreachable,
    }
    recordStream(e, outcome);
    net_alloc.free(e.bytes);
    maybeRetire(e.conn);
}

/// Records a TCP entry at its landing in the trace and the wire digest.
fn recordStream(e: TcpEntry, outcome: StreamOutcome) void {
    // Slice 1's kinds keep their records exactly: length 0, nothing after.
    const a: u32 = (e.conn_id & 0xffff) | (@as(u32, @backingInt(e.kind)) << 16) | (@as(u32, @backingInt(e.dir)) << 20) | (@as(u32, @backingInt(outcome)) << 21);
    emit(.stream, a, (@as(u32, @truncate(e.seq & 0xffff)) << 16) | @as(u32, @intCast(@min(e.bytes.len, 0xffff))));
    var h: [27]u8 = undefined;
    std.mem.writeInt(u64, h[0..8], e.due_at, .little);
    std.mem.writeInt(u32, h[8..12], e.conn_id, .little);
    h[12] = @backingInt(e.dir);
    h[13] = @backingInt(e.kind);
    h[14] = @backingInt(outcome);
    std.mem.writeInt(u64, h[15..23], e.seq, .little);
    std.mem.writeInt(u32, h[23..27], @intCast(e.bytes.len), .little);
    wire_hasher.update(&h);
    switch (e.kind) {
        .data => wire_hasher.update(e.bytes),
        .credit => {
            var c: [8]u8 = undefined;
            std.mem.writeInt(u64, &c, e.count, .little);
            wire_hasher.update(&c);
        },
        else => {},
    }
}

/// A SYN lands at its address (design 17.6).
fn landSyn(e: TcpEntry) StreamOutcome {
    const k = &conns[e.conn];
    const cl = &k.ends[@backingInt(Side.client)];
    if (cl.phase != .dead) reserveTcp(1, 0);
    k.syn_landed = true;
    if (cl.phase == .dead) return .dropped_dead_client;
    const j = listenerAt(e.to) orelse {
        stream_stats.refused += 1;
        sendTcp(e.conn, .server, .rst, cl.local);
        return .landed;
    };
    const l = streams[j];
    if (listenerCount(j) >= l.backlog) panic("sim: a SYN to port {d} landed at a full listener (backlog {d}): accept queue full: backlog overflow is not simulated", .{ e.to.port, l.backlog });
    k.ends[@backingInt(Side.server)] = .{ .phase = .s_prov, .local = e.to, .peer = cl.local, .listener_slot = @intCast(j), .listener_gen = dgram_gen[j], .nodelay = l.nodelay, .reuseaddr = l.reuseaddr, .linger0 = l.linger0 };
    sendTcp(e.conn, .server, .syn_ack, cl.local);
    return .landed;
}

/// A SYN-ACK lands at the client endpoint (design 17.6).
fn landSynAck(ci: u16) StreamOutcome {
    const cl = &conns[ci].ends[@backingInt(Side.client)];
    // Line 1: a failed client answers nothing, closed or not (17.6).
    if (cl.failed != null) return .dropped_failed;
    switch (cl.phase) {
        .dead => {
            if (!cl.rst_sent) {
                reserveTcp(1, 0);
                cl.rst_sent = true;
                sendTcp(ci, .client, .rst, cl.peer);
            }
            return .dropped_dead;
        },
        .c_syn => {
            reserveTcp(1, 0);
            cl.phase = .est;
            wakeStream(cl.slot);
            sendTcp(ci, .client, .ack, cl.peer);
            return .landed;
        },
        else => panic("sim: a SYN-ACK landed at a client endpoint in phase {t}", .{cl.phase}),
    }
}

/// An ACK lands at the server endpoint (design 17.6).
fn landAck(ci: u16) StreamOutcome {
    const sv = &conns[ci].ends[@backingInt(Side.server)];
    switch (sv.phase) {
        // Its listener closed: its RST is already on the way.
        .dead => return .dropped_dead,
        .s_prov => {
            const l = &streams[sv.listener_slot];
            sv.phase = .s_queued;
            sv.queue_pos = l.next_queue_pos;
            l.next_queue_pos += 1;
            wakeStream(sv.listener_slot);
            return .landed;
        },
        else => panic("sim: an ACK landed at a server endpoint in phase {t}", .{sv.phase}),
    }
}

/// A FIN lands at side `at` (design 17.8).
fn landFin(ci: u16, at: Side) StreamOutcome {
    const x = &conns[ci].ends[@backingInt(at)];
    if (x.failed != null) return .dropped_failed;
    switch (x.phase) {
        .dead => return .dropped_dead,
        .s_queued, .est => {
            if (x.reset_received) return .dropped_reset;
            x.fin_in = true;
            if (x.phase == .est) wakeRecv(x.slot);
            return .landed;
        },
        else => panic("sim: a FIN landed at an endpoint in phase {t}", .{x.phase}),
    }
}

/// An RST lands at side `at` (design 17.8's RST list; line 3 is review 3's
/// finding 2).
fn landRst(ci: u16, at: Side) StreamOutcome {
    const x = &conns[ci].ends[@backingInt(at)];
    if (x.failed != null) return .dropped_failed;
    if (x.reset_received) return .dropped_reset;
    switch (x.phase) {
        .dead => return .dropped_dead,
        // Its listener's count is its live endpoints, so it leaves the
        // count by dying: one place of capacity is free again.
        .s_prov => x.phase = .dead,
        .c_syn => {
            x.phase = .c_failed;
            x.reset_received = true;
            wakeStream(x.slot);
        },
        .s_queued, .est => {
            // Profile-neutral (review 3, finding 9): the call that meets it
            // reads the profile. Unread bytes stay, and are read first.
            x.reset_received = true;
            x.fin_before_reset = x.fin_in;
            discardHeldOutbound(ci, at, .abort);
            if (x.phase == .est) {
                x.reset_parked_send = if (streams[x.slot].parked_send) |p| p.id else null;
                wakeRecv(x.slot);
                wakeSend(x.slot);
            }
        },
        else => panic("sim: an RST landed at an endpoint in phase {t}", .{x.phase}),
    }
    return .landed;
}

/// Answers from side `at` with its one RST, if it has not sent one.
fn answerRst(ci: u16, at: Side) void {
    const x = &conns[ci].ends[@backingInt(at)];
    if (x.rst_sent) return;
    reserveTcp(1, 0);
    x.rst_sent = true;
    sendTcp(ci, at, .rst, x.peer);
}

/// A data chunk lands (design 17.8, the data landing list).
fn landData(e: TcpEntry) StreamOutcome {
    const k = &conns[e.conn];
    const at = otherSide(e.dir);
    const x = &k.ends[@backingInt(at)];
    const d = &k.dirs[@backingInt(e.dir)];
    const n = e.bytes.len;
    if (x.failed != null) {
        discardBytes(d, .failed_receiver, n);
        return .dropped_failed;
    }
    if (x.reset_received) {
        discardBytes(d, .after_reset, n);
        return .dropped_reset;
    }
    switch (x.phase) {
        .dead => {
            answerRst(e.conn, at);
            discardBytes(d, .closed_receiver, n);
            return .dropped_dead;
        },
        .s_queued, .est => {},
        else => panic("sim: a data chunk landed at an endpoint in phase {t}", .{x.phase}),
    }
    if (x.shut_rd) {
        const p = needProfile("row 18, a data chunk that lands after the endpoint's own shutdown(.both)");
        answerRst(e.conn, at);
        discardBytes(d, .shut_receiver, n);
        if (p == .linux) {
            // A local reset (review 3, finding 5): the same facts as a
            // landed RST, set once; a later chunk takes line 2 above.
            x.reset_received = true;
            x.fin_before_reset = false;
            x.reset_parked_send = if (streams[x.slot].parked_send) |ps| ps.id else null;
            discardHeldOutbound(e.conn, at, .abort);
            stream_stats.local_resets += 1;
            wakeRecv(x.slot);
            wakeSend(x.slot);
        }
        return .dropped_shut;
    }
    x.unread.appendSlice(net_alloc, e.bytes) catch panic("sim: out of memory for {d} unread bytes", .{n});
    stream_stats.chunks_landed += 1;
    if (x.phase == .est) wakeRecv(x.slot);
    return .landed;
}

/// A credit lands at side `at`, the sender of the direction it credits
/// (design 17.8, the credit landing list).
fn landCredit(ci: u16, at: Side, count: u64) StreamOutcome {
    const k = &conns[ci];
    const x = k.ends[@backingInt(at)];
    if (dirTerminal(k, at)) {
        stream_stats.stale_credit += 1;
        return .dropped_stale;
    }
    const d = &k.dirs[@backingInt(at)];
    if (count > d.admitted) panic("sim: a credit of {d} on connection {d} is past the {d} bytes admitted (an invariant)", .{ count, k.id, d.admitted });
    // A direction's credits land in the order they left: no entry lands
    // before the one sent before it in its direction (design 17.7), and a
    // release of held entries keeps that order (17.9). A credit leaves only
    // after a read or a window release of at least one byte since the last
    // one left, so each carries a larger count. A count that is not larger
    // means that order broke: a panic, not a silent max.
    if (count <= d.credited) panic("sim: a credit of {d} on connection {d} landed after one of {d}: credits land in the order they left, each with a larger count (an invariant)", .{ count, k.id, d.credited });
    d.credited = count;
    stream_stats.credits_landed += 1;
    if (x.phase == .est and d.room() > 0) wakeSend(x.slot);
    return .landed;
}

/// What a test can see of one endpoint (`testStreamEnd`, `testConnEnd`).
pub const TestStreamEnd = struct {
    conn_id: u32,
    phase: StreamPhase,
    fin_in: bool,
    fin_out: bool,
    rst_sent: bool,
    reset_received: bool,
    fin_before_reset: bool,
    local: Ip4,
    peer: Ip4,
    shut_rd: bool,
    reset_reported: bool,
    unread: usize,
    failed: ?StreamFailure,
    fail_reported: bool,
};

fn endView(ci: usize, side: Side) TestStreamEnd {
    const e = conns[ci].ends[@backingInt(side)];
    return .{ .conn_id = conns[ci].id, .phase = e.phase, .fin_in = e.fin_in, .fin_out = e.fin_out, .rst_sent = e.rst_sent, .reset_received = e.reset_received, .fin_before_reset = e.fin_before_reset, .local = e.local, .peer = e.peer, .shut_rd = e.shut_rd, .reset_reported = e.reset_reported, .unread = e.unreadLen(), .failed = e.failed, .fail_reported = e.fail_reported };
}

/// The endpoint behind stream socket `fd`, which must be an endpoint's
/// descriptor.
pub fn testStreamEnd(fd: i32) TestStreamEnd {
    if (comptime !builtin.is_test) @compileError("testStreamEnd is for tests");
    const s = streamSlot("testStreamEnd", fd);
    if (s.kind != .endpoint) panic("sim: testStreamEnd on sim fd {d}, a {s}", .{ fd, kindName(s.kind) });
    return endView(s.conn, s.side);
}

/// Side `server` (true) or client of connection `conn_id`; null once it
/// retired.
pub fn testConnEnd(conn_id: u32, server: bool) ?TestStreamEnd {
    if (comptime !builtin.is_test) @compileError("testConnEnd is for tests");
    for (conns, 0..) |k, ci| {
        if (k.used and k.id == conn_id) return endView(ci, if (server) .server else .client);
    }
    return null;
}

/// What a test can see of one data direction (`testStreamDir`): its
/// counters, the bytes of its chunks on the wire, and its receiver's unread
/// bytes, so a test can check conservation (design 17.7).
pub const TestStreamDir = struct {
    window: u32,
    admitted: u64,
    credited: u64,
    read: u64,
    reclaimed: u64,
    discarded: [@typeInfo(Discard).@"enum".field_names.len]u64,
    in_flight: u64,
    /// Data bytes held by a `.tcp` partition or silence, and the entries of
    /// every kind this direction holds (design 17.9).
    held: u64,
    held_entries: u32,
    unread: u64,
    data_send_index: u64,
    owed: bool,

    pub fn discardedTotal(d: TestStreamDir) u64 {
        var n: u64 = 0;
        for (d.discarded) |v| n += v;
        return n;
    }

    /// `admitted = in_flight + held + unread + read + discarded` (design
    /// 17.7).
    pub fn conserved(d: TestStreamDir) bool {
        return d.admitted == d.in_flight + d.held + d.unread + d.read + d.discardedTotal();
    }
};

/// The data direction of connection `conn_id` from its client (`from_client`)
/// or from its server; null once it retired.
pub fn testStreamDir(conn_id: u32, from_client: bool) ?TestStreamDir {
    if (comptime !builtin.is_test) @compileError("testStreamDir is for tests");
    for (conns, 0..) |k, ci| {
        if (!k.used or k.id != conn_id) continue;
        const side: Side = if (from_client) .client else .server;
        const d = k.dirs[@backingInt(side)];
        var in_flight: u64 = 0;
        for (tcp_wire.items) |w| {
            if (w.conn == ci and w.kind == .data and w.dir == side) in_flight += w.bytes.len;
        }
        var held: u64 = 0;
        for (tcp_held.items) |w| {
            if (w.conn == ci and w.kind == .data and w.dir == side) held += w.bytes.len;
        }
        return .{ .window = d.window, .admitted = d.admitted, .credited = d.credited, .read = d.read, .reclaimed = d.reclaimed, .discarded = d.discarded, .in_flight = in_flight, .held = held, .held_entries = k.held[@backingInt(side)], .unread = k.ends[@backingInt(otherSide(side))].unreadLen(), .data_send_index = d.data_send_index, .owed = d.owed_at != null };
    }
    return null;
}

/// The landing times of the TCP entries on the wire, in landing order.
pub fn testTcpWireDues(out: []u64) usize {
    if (comptime !builtin.is_test) @compileError("testTcpWireDues is for tests");
    if (tcp_wire.items.len > out.len) panic("sim: testTcpWireDues: {d} entries on the wire, room for {d}", .{ tcp_wire.items.len, out.len });
    for (tcp_wire.items, 0..) |w, i| out[i] = w.due_at;
    return tcp_wire.items.len;
}

/// Whether a receive and a send are parked on stream socket `fd`: waiting
/// on it, not yet woken. A woken op is due and no longer parked, so a test
/// can tell "woken, not harvested" from "still waiting".
pub fn testStreamParked(fd: i32) struct { recv: bool, send: bool } {
    if (comptime !builtin.is_test) @compileError("testStreamParked is for tests");
    const s = streams[sockIndex(fd)];
    return .{ .recv = s.parked_recv != null, .send = s.parked_send != null };
}

/// One TCP entry as a test sees it (`testTcpEntries`).
pub const TestTcpEntry = struct {
    conn_id: u32,
    from_client: bool,
    seq: u64,
    kind: TcpKind,
    /// A credit's count; 0 for other kinds.
    count: u64,
    /// A data chunk's length; 0 for other kinds.
    len: usize,
    /// Its landing time on the wire; 0 for a held entry (it has none until
    /// it is released).
    due_at: u64,
};

/// The TCP entries on the wire (in landing order) or held (in (`conn_id`,
/// direction, sequence) order), for a test that keeps its own copy of
/// them: it can name the entry that differs, not only a time or a count.
pub fn testTcpEntries(held: bool, out: []TestTcpEntry) usize {
    if (comptime !builtin.is_test) @compileError("testTcpEntries is for tests");
    const list = if (held) tcp_held.items else tcp_wire.items;
    if (list.len > out.len) panic("sim: testTcpEntries: {d} entries, room for {d}", .{ list.len, out.len });
    for (list, 0..) |w, i| out[i] = .{ .conn_id = w.conn_id, .from_client = w.dir == .client, .seq = w.seq, .kind = w.kind, .count = w.count, .len = w.bytes.len, .due_at = if (held) 0 else w.due_at };
    return list.len;
}

/// Moves the next `conn_id` of this run, to reach its limit in a test.
pub fn testSetNextConnId(id: u32) void {
    if (comptime !builtin.is_test) @compileError("testSetNextConnId is for tests");
    if (!begun) panic("sim: testSetNextConnId outside a run", .{});
    next_conn_id = id;
}

/// Moves the TCP port counter of this run, to reach the top of the range in
/// a test.
pub fn testSetNextTcpPort(port: u16) void {
    if (comptime !builtin.is_test) @compileError("testSetNextTcpPort is for tests");
    if (!begun) panic("sim: testSetNextTcpPort outside a run", .{});
    if (port < first_ephemeral_port) panic("sim: testSetNextTcpPort({d}): below the ephemeral range", .{port});
    tcp_next_port = port;
}

/// Live `S-prov` and `S-queued` endpoints of listener `fd`.
pub fn testListenerCount(fd: i32) usize {
    if (comptime !builtin.is_test) @compileError("testListenerCount is for tests");
    const s = streamSlot("testListenerCount", fd);
    if (s.kind != .listener) panic("sim: testListenerCount on sim fd {d}, a {s}", .{ fd, kindName(s.kind) });
    return listenerCount(sockIndex(fd));
}

/// Connections in the table now (not retired).
pub fn testConnsLive() usize {
    if (comptime !builtin.is_test) @compileError("testConnsLive is for tests");
    var n: usize = 0;
    for (conns) |k| n += @intFromBool(k.used);
    return n;
}

/// The teardown check at `sim.end`, before anything is freed (design 17.15
/// item 3).
fn streamTeardownCheck() void {
    var left: usize = 0;
    for (streams, 0..) |s, j| {
        if (!s.used) continue;
        left += 1;
        std.debug.print("sim: stream teardown: sim fd {d} is open, a {s}{s}\n", .{ fd_base + max_ends + @as(i32, @intCast(j)), kindName(s.kind), if (s.unresolved != null or s.unresolved_recv != null or s.unresolved_send != null) ", with an op unresolved" else "" });
    }
    for (live[0..live_len]) |l| {
        if (!l.on_stream) continue;
        left += 1;
        std.debug.print("sim: stream teardown: op {d} on a stream socket is unresolved\n", .{l.id});
    }
    for (conns) |k| {
        if (!k.used) continue;
        left += 1;
        var on_wire: usize = 0;
        for (tcp_wire.items) |w| {
            if (w.conn_id == k.id) on_wire += 1;
        }
        std.debug.print("sim: stream teardown: connection {d} is not retired: client {t}, server {t}, {d} entries on the wire, {d} held, credits owed {}/{}\n", .{ k.id, k.ends[0].phase, k.ends[1].phase, on_wire, k.held[0] + k.held[1], k.dirs[0].owed_at != null, k.dirs[1].owed_at != null });
    }
    if (reservations.items.len != 0) {
        std.debug.print("sim: stream teardown: {d} TIME_WAIT reservations, expected, freed\n", .{reservations.items.len});
        reservations.clearRetainingCapacity();
    }
    if (reservations.items.len != 0) panic("sim: stream teardown: reservations remain after they were freed", .{});
    if (left != 0) panic("sim: stream teardown: {d} stream objects remain at sim.end (listed above): close every stream socket, then sleep while sim.streamEntriesInFlight() is not 0", .{left});
}

fn digestChunkMode(h: *std.hash.Wyhash, m: ChunkMode) void {
    var b: [8]u8 = undefined;
    std.mem.writeInt(u64, &b, switch (m) {
        .hashed => 1 << 32,
        .off => 2 << 32,
        .fixed => |n| (3 << 32) | @as(u64, n),
    }, .little);
    h.update(&b);
}

/// The stream part of `stateDigest`, for a run that opened a stream socket:
/// every setting and state that can change the future (design 17.11, as
/// far as slice 1 has them).
fn digestStreams(h: *std.hash.Wyhash) void {
    var b: [8]u8 = undefined;
    std.mem.writeInt(u64, &b, (@as(u64, if (stream_profile) |p| @backingInt(p) else 0) << 56) | (@as(u64, tcp_next_port) << 32) | next_conn_id, .little);
    h.update(&b);
    std.mem.writeInt(u64, &b, window_default, .little);
    h.update(&b);
    digestChunkMode(h, chunking_default);
    for (dgram_gen) |g| {
        std.mem.writeInt(u32, b[0..4], g, .little);
        h.update(b[0..4]);
    }
    for (reservations.items) |r| {
        var e: [7]u8 = undefined;
        @memcpy(e[0..4], &r.ip);
        std.mem.writeInt(u16, e[4..6], r.port, .little);
        e[6] = @intFromBool(r.reuse);
        h.update(&e);
    }
    for (tcp_wire.items) |w| {
        var e: [32]u8 = undefined;
        std.mem.writeInt(u64, e[0..8], w.due_at, .little);
        std.mem.writeInt(u32, e[8..12], w.conn_id, .little);
        e[12] = @backingInt(w.dir);
        e[13] = @backingInt(w.kind);
        std.mem.writeInt(u64, e[14..22], w.seq, .little);
        @memcpy(e[22..26], &w.to.ip);
        std.mem.writeInt(u16, e[26..28], w.to.port, .little);
        std.mem.writeInt(u32, e[28..32], @intCast(w.bytes.len), .little);
        h.update(&e);
        h.update(w.bytes);
        std.mem.writeInt(u64, &b, w.count, .little);
        h.update(&b);
    }
    for (streams) |s| {
        var e: [36]u8 = undefined;
        e[0] = @intFromBool(s.used) | (@as(u8, @intFromBool(s.bound)) << 1) | (@as(u8, @intFromBool(s.nonblocking)) << 2) | (@as(u8, @intFromBool(s.nodelay)) << 3) | (@as(u8, @intFromBool(s.reuseaddr)) << 4) | (@as(u8, @backingInt(s.side)) << 5);
        e[1] = @backingInt(s.kind);
        @memcpy(e[2..6], &s.local.ip);
        std.mem.writeInt(u16, e[6..8], s.local.port, .little);
        std.mem.writeInt(u16, e[8..10], s.backlog, .little);
        std.mem.writeInt(u64, e[10..18], s.next_queue_pos, .little);
        std.mem.writeInt(u16, e[18..20], s.conn, .little);
        std.mem.writeInt(u32, e[20..24], if (s.parked) |p| p.id else 0, .little);
        std.mem.writeInt(u32, e[24..28], if (s.unresolved) |p| p.id else 0, .little);
        e[28] = @intFromBool(s.linger0);
        @memset(e[29..36], 0);
        h.update(&e);
        var q: [16]u8 = undefined;
        std.mem.writeInt(u32, q[0..4], if (s.parked_recv) |x| x.id else 0, .little);
        std.mem.writeInt(u32, q[4..8], if (s.unresolved_recv) |x| x.id else 0, .little);
        std.mem.writeInt(u32, q[8..12], if (s.parked_send) |x| x.id else 0, .little);
        std.mem.writeInt(u32, q[12..16], if (s.unresolved_send) |x| x.id else 0, .little);
        h.update(&q);
    }
    for (conns) |k| {
        if (!k.used) continue;
        var e: [21]u8 = undefined;
        std.mem.writeInt(u32, e[0..4], k.id, .little);
        std.mem.writeInt(u64, e[4..12], k.next_seq[0], .little);
        std.mem.writeInt(u64, e[12..20], k.next_seq[1], .little);
        e[20] = @intFromBool(k.syn_landed);
        h.update(&e);
        for (k.ends) |x| {
            var f: [32]u8 = undefined;
            f[0] = @backingInt(x.phase);
            f[1] = x.slot;
            f[2] = @intFromBool(x.fin_in) | (@as(u8, @intFromBool(x.fin_out)) << 1) | (@as(u8, @intFromBool(x.rst_sent)) << 2) | (@as(u8, @intFromBool(x.reset_received)) << 3) | (@as(u8, @intFromBool(x.fin_before_reset)) << 4) | (@as(u8, @intFromBool(x.nodelay)) << 5) | (@as(u8, @intFromBool(x.reuseaddr)) << 6);
            @memcpy(f[3..7], &x.local.ip);
            std.mem.writeInt(u16, f[7..9], x.local.port, .little);
            @memcpy(f[9..13], &x.peer.ip);
            std.mem.writeInt(u16, f[13..15], x.peer.port, .little);
            f[15] = x.listener_slot;
            std.mem.writeInt(u32, f[16..20], x.listener_gen, .little);
            std.mem.writeInt(u64, f[20..28], x.queue_pos, .little);
            f[28] = @intFromBool(x.linger0) | (@as(u8, @intFromBool(x.shut_rd)) << 1) | (@as(u8, @intFromBool(x.reset_reported)) << 2) | (@as(u8, @intFromBool(x.reset_parked_send != null)) << 3);
            @memset(f[29..32], 0);
            h.update(&f);
            // Hashed only when set, so a run that never sets it keeps its
            // digests.
            if (x.reset_parked_send) |id| {
                var g: [4]u8 = undefined;
                std.mem.writeInt(u32, &g, id, .little);
                h.update(&g);
            }
            // The same for a failure (slice 3).
            if (x.failed) |how| h.update(&.{ @backingInt(how), @intFromBool(x.fail_reported) });
            std.mem.writeInt(u64, &b, x.unreadLen(), .little);
            h.update(&b);
            h.update(x.unread.items[x.unread_head..]);
        }
        for (k.last_due) |t| {
            std.mem.writeInt(u64, &b, t, .little);
            h.update(&b);
        }
        digestChunkMode(h, k.chunking);
        for (k.dirs) |d| {
            for ([_]u64{ d.window, d.admitted, d.credited, d.read, d.reclaimed, d.data_send_index, d.last_credit_at orelse std.math.maxInt(u64), d.owed_at orelse std.math.maxInt(u64) }) |v| {
                std.mem.writeInt(u64, &b, v, .little);
                h.update(&b);
            }
            for (d.discarded) |v| {
                std.mem.writeInt(u64, &b, v, .little);
                h.update(&b);
            }
        }
    }
    for ([_]u64{ stream_stats.stale_credit, stream_stats.local_resets, stream_stats.chunks_landed, stream_stats.credits_landed }) |v| {
        std.mem.writeInt(u64, &b, v, .little);
        h.update(&b);
    }
    std.mem.writeInt(u64, &b, stream_stats.retired, .little);
    h.update(&b);
    // Slice 3, only for a run that set a `.tcp` rule (a run without one
    // keeps its digests): every held entry, each one's extra time, the
    // held counts per direction, the draws and the fault counts.
    if (tcp_rule_set) {
        for (tcp_held.items) |w| {
            var e: [40]u8 = undefined;
            std.mem.writeInt(u32, e[0..4], w.conn_id, .little);
            e[4] = @backingInt(w.dir);
            e[5] = @backingInt(w.kind);
            std.mem.writeInt(u64, e[6..14], w.seq, .little);
            std.mem.writeInt(u64, e[14..22], w.extra_ns, .little);
            std.mem.writeInt(u64, e[22..30], w.count, .little);
            std.mem.writeInt(u32, e[30..34], @intCast(w.bytes.len), .little);
            @memcpy(e[34..38], &w.to.ip);
            std.mem.writeInt(u16, e[38..40], w.to.port, .little);
            h.update(&e);
            h.update(w.bytes);
        }
        for (tcp_wire.items) |w| {
            std.mem.writeInt(u64, &b, w.extra_ns, .little);
            h.update(&b);
        }
        for (conns) |k| {
            if (!k.used) continue;
            std.mem.writeInt(u64, &b, (@as(u64, k.held[0]) << 32) | k.held[1], .little);
            h.update(&b);
        }
        for ([_]u64{ stream_fault_draws, stream_stats.held_at_send, stream_stats.held_at_landing, stream_stats.held_behind, stream_stats.released, stream_stats.credits_coalesced, stream_stats.discarded_held, stream_stats.delayed, stream_stats.dropped_failed }) |v| {
            std.mem.writeInt(u64, &b, v, .little);
            h.update(&b);
        }
    }
}

/// Jitter draws of `.tcp` rules in this run (design 17.9a, point 4).
pub fn streamFaultDraws() u64 {
    return stream_fault_draws;
}

// ---- Datagram faults (phase 2 of the sim network) ----
//
// A test sets fault rules during a run, from a task, at the virtual time
// that task runs; they act on datagrams sent (and landing) from then on.
// `begin` starts a run with no rule. Every call is on the sim thread, during
// a run. The sender of a faulted datagram succeeds, as a kernel's does.
// This block is about rules whose endpoints say `.protocol = .udp` (the
// default): only those act on datagrams, and "a run that set a rule" below
// means a `.udp` rule. A `.tcp` rule acts on stream sockets instead (the
// faults paragraph of the comment above `StreamProfile`).
//
// Rules, of three kinds:
// - An impairment (`setImpairment`) on a `Path`: loss, burst loss, delay,
//   jitter, reorder and duplication (`Impairment`). Among
//   the impairments whose path matches a datagram, only the one set most
//   recently acts ("last set wins"). Setting one on a path that has one
//   replaces it, resets its burst to the good state, and makes it the most
//   recent.
// - A partition (`setPartition`) drops every datagram on its path, one
//   direction (`Path.reversed` gives the other).
// - A silence (`silence`) drops every datagram from or to its endpoint:
//   one socket (an exact address and port), a socket bound to the wildcard
//   (`.{ .port = p }`), or a host that the test gave its own 127/8 address
//   (`.{ .ip = a }`).
// Partitions and silences are outside "last set wins": any one that matches
// drops, whatever impairment was set after it.
//
// A datagram meets the rules twice:
// 1. At its send (`faultAtSend`), once its source is known: a silence of its
//    source or destination drops it (`silenced_at_send`), else a partition of
//    its flow (`partitioned_at_send`), else the acting impairment: its burst
//    state moves (good to bad with `enter_ppm`, bad to good with
//    `leave_ppm`) and in the bad state the datagram is lost (`lost_burst`);
//    else `loss_ppm` loses it (`lost`). The burst state is the rule's, shared
//    by every flow it matches: a link has one bad period. Else `dup_ppm`
//    makes a second copy (`duplicated`), and each copy lands at
//    `now + wire_latency_ns + delay_ns + jitter`, jitter drawn per copy in
//    0..jitter_ns. Then, unless the acting rule says `reorder`, a copy lands
//    no earlier than the latest datagram of its flow already on the wire
//    (the first copy counts as on the wire for the second): the order
//    clamp, which also holds with no rule in force once a rule was set in
//    the run, since datagrams delayed by a cleared rule may still be on the
//    wire. Under `reorder`, a copy due before such a datagram counts as
//    `overtaking`. A copy that lands later than `now + wire_latency_ns` for
//    any of these reasons counts as `delayed`. Copies of one flow due at one
//    instant land in the order they went on the wire; flows due at one
//    instant land in a seeded order (`landDue`), as in phase 1.
// 2. At its landing (`faultInFlight`, the first harvest at or after its
//    time): a silence or partition in force then that matches it drops it
//    (`silenced_in_flight`, `partitioned_in_flight`). A link that goes down
//    loses what is in it.
// When several drop rules match, a silence acts before a partition, and of
// one kind the most recent acts. Each drop is in the trace and the wire
// digest (`LandOutcome` 3 to 8; a second copy sets bit 0x80 of the outcome,
// landed or dropped). A delay needs no record of its own: a landing's
// record holds its time. Rule changes are not recorded: a rule that acts on
// nothing leaves both unchanged (promise 2). The state digest covers the
// rules, the counts and the draws of a run that set a rule.
//
// Counts: `netFaultStats` per cause for the run; per rule, `matched` (sends
// whose flow its path matched while it was in force, acting or not; for a
// drop rule also datagrams it matched at landing) and `dropped` (what it
// dropped). `clearImpairment`, `healPartition` and `unsilence` return the
// rule's counts; `requireEveryRuleMatched` checks every rule of the run.
//
// Draws (`faultDraw`) never touch the run's RNG, and a field that cannot
// change the outcome draws nothing (`netFaultDraws` counts them).
//
// Misuse panics, naming the call: a call outside a run or off the sim
// thread; ppm over 1_000_000; `delay_ns + jitter_ns` over
// `max_fault_delay_ns`; a burst whose `enter_ppm` or `leave_ppm` is 0
// (a burst that never starts or never ends: that is `setPartition`); an
// endpoint address outside 127/8, 0.0.0.0 included, or port 0; an
// impairment with every field at its default (`clearImpairment` removes);
// clearing, healing or unsilencing a rule that is not there; a second
// partition or silence of an equal path or endpoint; more than `max_rules`
// rules in force, or `max_rule_history` distinct rules in a run.

/// Matches a datagram's source or destination. null matches any value. `ip`
/// is in 127/8; `port` is not 0.
pub const Endpoint = struct {
    ip: ?[4]u8 = null,
    port: ?u16 = null,
    /// What the rule acts on: datagrams (`.udp`) or a stream's TCP entries
    /// (`.tcp`). A path's two endpoints name one protocol.
    protocol: Protocol = .udp,

    fn matches(e: Endpoint, a: Ip4) bool {
        if (e.ip) |ip| {
            if (!std.mem.eql(u8, &ip, &a.ip)) return false;
        }
        if (e.port) |p| {
            if (p != a.port) return false;
        }
        return true;
    }
};

pub const Protocol = enum(u8) { udp = 1, tcp = 2 };

/// One direction: datagrams from `from` to `to`. `.{}` matches every
/// datagram.
pub const Path = struct {
    from: Endpoint = .{},
    to: Endpoint = .{},

    pub fn reversed(p: Path) Path {
        return .{ .from = p.to, .to = p.from };
    }

    fn matches(p: Path, from: Ip4, to: Ip4) bool {
        return p.from.matches(from) and p.to.matches(to);
    }
};

/// Gilbert loss: in the bad state every datagram is lost. Mean burst length
/// is `1_000_000 / leave_ppm` datagrams that the rule acts on.
pub const Burst = struct { enter_ppm: u32, leave_ppm: u32 };

pub const Impairment = struct {
    /// Independent loss, per million datagrams.
    loss_ppm: u32 = 0,
    burst: ?Burst = null,
    /// Added to `wire_latency_ns`.
    delay_ns: u64 = 0,
    /// Plus a uniform draw in 0..jitter_ns (both ends included), per copy.
    jitter_ns: u64 = 0,
    /// false: a copy never lands before a datagram of its flow that is
    /// already on the wire (the order clamp). true: jitter and a change of
    /// rule may reorder the flow.
    reorder: bool = false,
    /// A second copy, per million datagrams, with its own jitter.
    dup_ppm: u32 = 0,
};

/// The most `delay_ns + jitter_ns` may be.
pub const max_fault_delay_ns: u64 = 60 * std.time.ns_per_s;

pub const RuleStats = struct {
    matched: u64 = 0,
    dropped: u64 = 0,
    /// Second copies this rule made.
    duplicated: u64 = 0,
    /// Copies of datagrams it acted on that land later than
    /// `wire_latency_ns` after their send, for any reason.
    delayed: u64 = 0,
};

/// The run's faulted datagrams, by cause.
pub const NetFaultStats = struct {
    lost: u64 = 0,
    lost_burst: u64 = 0,
    partitioned_at_send: u64 = 0,
    partitioned_in_flight: u64 = 0,
    silenced_at_send: u64 = 0,
    silenced_in_flight: u64 = 0,
    /// Second copies made.
    duplicated: u64 = 0,
    /// Copies that land later than `wire_latency_ns` after their send: a
    /// delay, jitter, or the order clamp behind a delayed datagram.
    delayed: u64 = 0,
    /// Copies scheduled to land before a datagram of their flow that was
    /// already on the wire (only under a rule with `reorder`).
    overtaking: u64 = 0,
};

pub const max_rules = 64;
pub const max_rule_history = 256;

const RuleKind = enum(u8) { impairment = 1, partition = 2, silence = 3 };

/// A rule's identity: its kind and what it matches. One rule per key.
const RuleKey = union(RuleKind) {
    impairment: Path,
    partition: Path,
    silence: Endpoint,

    fn eql(a: RuleKey, b: RuleKey) bool {
        return std.meta.eql(a, b);
    }

    fn matches(k: RuleKey, from: Ip4, to: Ip4) bool {
        return switch (k) {
            .impairment, .partition => |p| p.matches(from, to),
            .silence => |e| e.matches(from) or e.matches(to),
        };
    }

    fn protocol(k: RuleKey) Protocol {
        return switch (k) {
            .impairment, .partition => |p| p.from.protocol,
            .silence => |e| e.protocol,
        };
    }
};

const Rule = struct {
    key: RuleKey,
    /// The impairment's settings, and whether its burst is in the bad state.
    /// Only an impairment's are read; a drop rule keeps the defaults.
    imp: Impairment = .{},
    bad: bool = false,
    stats: RuleStats = .{},
    /// Its entry in `rule_history`.
    hist: usize,
};

/// Rules in force, oldest first: the last matching impairment acts.
var rules: [max_rules]Rule = undefined;
var rules_len: usize = 0;

/// Every distinct rule key set in this run, with what it matched across all
/// the times it was in force (`requireEveryRuleMatched`).
const HistoryEntry = struct { key: RuleKey, matched: u64, first_set_ns: u64 };
var rule_history: [max_rule_history]HistoryEntry = undefined;
var rule_history_len: usize = 0;
/// Whether this run ever set a `.udp` rule (its datagrams take the fault
/// path from then on), or a `.tcp` one (its TCP entries do).
/// When this run first set a `.udp` rule: `no_rule` before. A death after
/// it was under a rule, wherever it is harvested (`addGhost`).
var udp_rule_since: u64 = no_rule;
const no_rule = std.math.maxInt(u64);
var tcp_rule_set: bool = false;

var fault_stats: NetFaultStats = .{};
var fault_draws: u64 = 0;

pub fn netFaultStats() NetFaultStats {
    return fault_stats;
}

/// How many fault draws this run made (design section 7, promise 2).
pub fn netFaultDraws() u64 {
    return fault_draws;
}

/// The purposes of the draws for one datagram: burst, loss, dup, then the
/// jitter of the first and of the second copy.
const DrawPurpose = enum(u64) { burst = 1, loss = 2, dup = 3, jitter0 = 4, jitter1 = 5 };

/// SplitMix64's output function.
fn mix64(x: u64) u64 {
    var z = x;
    z = (z ^ (z >> 30)) *% 0xbf58476d1ce4e5b9;
    z = (z ^ (z >> 27)) *% 0x94d049bb133111eb;
    return z ^ (z >> 31);
}

/// One fault draw, for the datagram with send number `send_no` (the run's
/// count of datagrams sent before it, from 0) and a purpose:
///
///     base = mix64(seed ^ 0x6e65745f6661756c)
///     draw = mix64(base +% (send_no *% 8 +% purpose) *% 0x9e3779b97f4a7c15)
///
/// That is SplitMix64's output number `8 * send_no + purpose` from the state
/// `base`. It reads no state but the seed, so the run's RNG is never drawn
/// for a fault, and a model can compute the same value with its own code.
fn faultDraw(send_no: u64, purpose: DrawPurpose) u64 {
    fault_draws += 1;
    const base = mix64(current_seed ^ 0x6e65745f6661756c);
    return mix64(base +% (send_no *% 8 +% @backingInt(purpose)) *% 0x9e3779b97f4a7c15);
}

/// Whether an event of `ppm` per million happens. Draws only when the answer
/// is not fixed: 0 never happens, 1_000_000 always does.
fn happens(ppm: u32, send_no: u64, purpose: DrawPurpose) bool {
    if (ppm == 0) return false;
    if (ppm >= 1_000_000) return true;
    return faultDraw(send_no, purpose) % 1_000_000 < ppm;
}

fn countMatched(r: *Rule) void {
    r.stats.matched += 1;
    rule_history[r.hist].matched += 1;
}

/// The most recent rule of `kind` and `proto` in force that matches the
/// flow.
fn dropRule(kind: RuleKind, proto: Protocol, from: Ip4, to: Ip4) ?*Rule {
    var i = rules_len;
    while (i > 0) {
        i -= 1;
        if (rules[i].key == kind and rules[i].key.protocol() == proto and rules[i].key.matches(from, to)) return &rules[i];
    }
    return null;
}

/// What a datagram's send becomes: dropped, or one or two copies with their
/// landing times.
const SendFate = union(enum) {
    dropped: LandOutcome,
    sent: struct { copies: u8, due: [2]u64 },
};

/// The latest landing time of a datagram of this flow still on the wire.
/// `wire` is in landing order, so the last one found from the end is it.
fn flowLatest(from: Ip4, to: Ip4) ?u64 {
    var i = wire.items.len;
    while (i > 0) {
        i -= 1;
        const w = wire.items[i];
        if (std.meta.eql(w.from, from) and std.meta.eql(w.to, to)) return w.due_at;
    }
    return null;
}

/// The fault rules at a datagram's send, in a run that has set a rule
/// (the order clamp applies after every rule is cleared too: datagrams
/// delayed by a rule may still be on the wire).
fn faultAtSend(send_no: u64, from: Ip4, to: Ip4) SendFate {
    if (faultDrop(send_no, from, to)) |outcome| return .{ .dropped = outcome };
    const r = dropRule(.impairment, .udp, from, to);
    const imp: Impairment = if (r) |x| x.imp else .{};
    const base = clock_ns + wire_latency_ns;
    var fate: SendFate = .{ .sent = .{ .copies = 1, .due = .{ base, base } } };
    const s = &fate.sent;
    if (happens(imp.dup_ppm, send_no, .dup)) {
        s.copies = 2;
        fault_stats.duplicated += 1;
        r.?.stats.duplicated += 1;
    }
    // A copy already decided is on the wire before the next one.
    var latest = flowLatest(from, to);
    for (0..s.copies) |c| {
        var at = base + imp.delay_ns;
        if (imp.jitter_ns != 0) {
            at += faultDraw(send_no, if (c == 0) .jitter0 else .jitter1) % (imp.jitter_ns + 1);
        }
        if (latest) |l| {
            if (at < l) {
                if (imp.reorder) fault_stats.overtaking += 1 else at = l;
            }
        }
        if (at > base) {
            fault_stats.delayed += 1;
            if (r) |x| x.stats.delayed += 1;
        }
        s.due[c] = at;
        latest = @max(latest orelse at, at);
    }
    return fate;
}

/// The drop decisions at a datagram's send. Null: it goes on the wire.
fn faultDrop(send_no: u64, from: Ip4, to: Ip4) ?LandOutcome {
    for (rules[0..rules_len]) |*r| {
        if (r.key.protocol() == .udp and r.key.matches(from, to)) countMatched(r);
    }
    if (dropRule(.silence, .udp, from, to)) |r| {
        r.stats.dropped += 1;
        fault_stats.silenced_at_send += 1;
        return .silenced;
    }
    if (dropRule(.partition, .udp, from, to)) |r| {
        r.stats.dropped += 1;
        fault_stats.partitioned_at_send += 1;
        return .partitioned;
    }
    const r = dropRule(.impairment, .udp, from, to) orelse return null;
    if (r.imp.burst) |b| {
        if (r.bad) {
            if (happens(b.leave_ppm, send_no, .burst)) r.bad = false;
        } else {
            if (happens(b.enter_ppm, send_no, .burst)) r.bad = true;
        }
        if (r.bad) {
            r.stats.dropped += 1;
            fault_stats.lost_burst += 1;
            return .lost_burst;
        }
    }
    if (happens(r.imp.loss_ppm, send_no, .loss)) {
        r.stats.dropped += 1;
        fault_stats.lost += 1;
        return .lost;
    }
    return null;
}

/// The drop rules at a datagram's landing. Every drop rule that matches it
/// was set after its send (one in force then would have dropped it), so
/// counting it as matched here counts no datagram twice for one rule.
fn faultInFlight(from: Ip4, to: Ip4) ?LandOutcome {
    for (rules[0..rules_len]) |*r| {
        if (r.key != .impairment and r.key.protocol() == .udp and r.key.matches(from, to)) countMatched(r);
    }
    if (dropRule(.silence, .udp, from, to)) |r| {
        r.stats.dropped += 1;
        fault_stats.silenced_in_flight += 1;
        return .silenced_in_flight;
    }
    if (dropRule(.partition, .udp, from, to)) |r| {
        r.stats.dropped += 1;
        fault_stats.partitioned_in_flight += 1;
        return .partitioned_in_flight;
    }
    return null;
}

fn faultCall(comptime what: []const u8) void {
    refuseOtherThread("sim." ++ what);
    if (!begun) panic("sim: " ++ what ++ " outside a sim run", .{});
}

fn checkEndpoint(comptime what: []const u8, e: Endpoint) void {
    if (e.ip) |ip| {
        if (ip[0] != 127) panic("sim: " ++ what ++ ": the address {d}.{d}.{d}.{d} is not in 127.0.0.0/8 (a socket bound to 0.0.0.0 is matched by its port alone)", .{ ip[0], ip[1], ip[2], ip[3] });
    }
    if (e.port) |p| {
        if (p == 0) panic("sim: " ++ what ++ ": port 0 matches no datagram", .{});
    }
}

fn checkPath(comptime what: []const u8, p: Path) void {
    checkEndpoint(what, p.from);
    checkEndpoint(what, p.to);
    if (p.from.protocol != p.to.protocol) panic("sim: " ++ what ++ ": a path from a {t} endpoint to a {t} one: both ends name one protocol", .{ p.from.protocol, p.to.protocol });
}

fn checkPpm(comptime what: []const u8, comptime field: []const u8, ppm: u32) void {
    if (ppm > 1_000_000) panic("sim: " ++ what ++ ": " ++ field ++ " {d} is over 1000000", .{ppm});
}

fn findRule(key: RuleKey) ?usize {
    for (rules[0..rules_len], 0..) |r, i| {
        if (r.key.eql(key)) return i;
    }
    return null;
}

fn removeRule(i: usize) RuleStats {
    const st = rules[i].stats;
    std.mem.copyForwards(Rule, rules[i .. rules_len - 1], rules[i + 1 .. rules_len]);
    rules_len -= 1;
    return st;
}

fn addRule(comptime what: []const u8, key: RuleKey, imp: Impairment) void {
    if (rules_len == max_rules) panic("sim: " ++ what ++ ": {d} fault rules are in force, the most a run holds", .{max_rules});
    const hist = for (rule_history[0..rule_history_len], 0..) |h, i| {
        if (h.key.eql(key)) break i;
    } else blk: {
        if (rule_history_len == max_rule_history) panic("sim: " ++ what ++ ": {d} distinct fault rules were set in this run, the most it records", .{max_rule_history});
        rule_history[rule_history_len] = .{ .key = key, .matched = 0, .first_set_ns = clock_ns };
        rule_history_len += 1;
        break :blk rule_history_len - 1;
    };
    rules[rules_len] = .{ .key = key, .imp = imp, .hist = hist };
    rules_len += 1;
    switch (key.protocol()) {
        .udp => udp_rule_since = @min(udp_rule_since, clock_ns),
        .tcp => tcp_rule_set = true,
    }
}

/// Sets the impairment of `path`, replacing one already there (its burst
/// starts good again) and making it the most recently set.
pub fn setImpairment(path: Path, imp: Impairment) void {
    faultCall("setImpairment");
    checkPath("setImpairment", path);
    checkPpm("setImpairment", "loss_ppm", imp.loss_ppm);
    if (imp.burst) |b| {
        checkPpm("setImpairment", "burst.enter_ppm", b.enter_ppm);
        checkPpm("setImpairment", "burst.leave_ppm", b.leave_ppm);
        if (b.enter_ppm == 0) panic("sim: setImpairment: a burst with enter_ppm 0 never starts", .{});
        if (b.leave_ppm == 0) panic("sim: setImpairment: a burst with leave_ppm 0 never ends (that is setPartition)", .{});
    }
    checkPpm("setImpairment", "dup_ppm", imp.dup_ppm);
    if (imp.delay_ns > max_fault_delay_ns or imp.jitter_ns > max_fault_delay_ns - imp.delay_ns) {
        panic("sim: setImpairment: delay_ns {d} plus jitter_ns {d} is over {d} ns", .{ imp.delay_ns, imp.jitter_ns, max_fault_delay_ns });
    }
    if (std.meta.eql(imp, Impairment{})) panic("sim: setImpairment with every field at its default: clearImpairment removes an impairment", .{});
    if (path.from.protocol == .tcp) {
        // Design 17.9: TCP does not lose, copy or reorder; a partition or
        // silence holds entries instead.
        if (imp.loss_ppm != 0) panic("sim: setImpairment: loss_ppm on a .tcp path is refused: a stream loses nothing (a partition or silence holds entries)", .{});
        if (imp.burst != null) panic("sim: setImpairment: burst on a .tcp path is refused: a stream loses nothing (a partition or silence holds entries)", .{});
        if (imp.dup_ppm != 0) panic("sim: setImpairment: dup_ppm on a .tcp path is refused: a stream delivers each byte once", .{});
        if (imp.reorder) panic("sim: setImpairment: reorder on a .tcp path is refused: a stream delivers in order", .{});
    }
    if (findRule(.{ .impairment = path })) |i| _ = removeRule(i);
    addRule("setImpairment", .{ .impairment = path }, imp);
}

/// Removes the impairment of `path` and returns its counts.
pub fn clearImpairment(path: Path) RuleStats {
    faultCall("clearImpairment");
    const i = findRule(.{ .impairment = path }) orelse panic("sim: clearImpairment of a path that has no impairment", .{});
    return removeRule(i);
}

/// Drops every datagram on `path` (one direction), at send and in flight.
pub fn setPartition(path: Path) void {
    faultCall("setPartition");
    checkPath("setPartition", path);
    if (findRule(.{ .partition = path }) != null) panic("sim: setPartition of a path that is already partitioned", .{});
    addRule("setPartition", .{ .partition = path }, .{});
}

pub fn healPartition(path: Path) RuleStats {
    faultCall("healPartition");
    const i = findRule(.{ .partition = path }) orelse panic("sim: healPartition of a path that is not partitioned", .{});
    const st = removeRule(i);
    if (path.from.protocol == .tcp) releaseHeld();
    return st;
}

/// Drops every datagram from or to `ep`, at send and in flight.
pub fn silence(ep: Endpoint) void {
    faultCall("silence");
    checkEndpoint("silence", ep);
    if (findRule(.{ .silence = ep }) != null) panic("sim: silence of an endpoint that is already silenced", .{});
    addRule("silence", .{ .silence = ep }, .{});
}

pub fn unsilence(ep: Endpoint) RuleStats {
    faultCall("unsilence");
    const i = findRule(.{ .silence = ep }) orelse panic("sim: unsilence of an endpoint that is not silenced", .{});
    const st = removeRule(i);
    if (ep.protocol == .tcp) releaseHeld();
    return st;
}

fn writeEndpoint(w: *std.Io.Writer, e: Endpoint) !void {
    if (e.ip) |ip| try w.print("{d}.{d}.{d}.{d}", .{ ip[0], ip[1], ip[2], ip[3] }) else try w.writeByte('*');
    if (e.port) |p| try w.print(":{d}", .{p}) else try w.writeAll(":*");
    if (e.protocol == .tcp) try w.writeAll("/tcp");
}

/// Panics, naming each one, unless every fault rule set in this run matched
/// at least one datagram while it was in force (counted over every time its
/// path was set, cleared or not). A rule that matched nothing is usually a
/// wrong address in the test. A panic, not a returned list: a caller that
/// forgets to check a list would pass, and this exists to make that loud.
pub fn requireEveryRuleMatched() void {
    faultCall("requireEveryRuleMatched");
    var buf: [1024]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    var n: usize = 0;
    for (rule_history[0..rule_history_len]) |h| {
        if (h.matched != 0) continue;
        n += 1;
        // A full buffer cuts the list; the count before it stays exact.
        describeRule(&w, h, n == 1) catch {};
    }
    if (n != 0) panic("sim: {d} fault rules matched no datagram in this run: {s}", .{ n, w.buffered() });
}

fn describeRule(w: *std.Io.Writer, h: HistoryEntry, first: bool) !void {
    if (!first) try w.writeAll("; ");
    switch (h.key) {
        .impairment, .partition => |p| {
            try w.print("{t} from ", .{h.key});
            try writeEndpoint(w, p.from);
            try w.writeAll(" to ");
            try writeEndpoint(w, p.to);
        },
        .silence => |e| {
            try w.writeAll("silence of ");
            try writeEndpoint(w, e);
        },
    }
    try w.print(" (first set at {d} ns)", .{h.first_set_ns});
}

fn hashEndpoint(h: *std.hash.Wyhash, e: Endpoint) void {
    var b: [7]u8 = @splat(0);
    // The protocol's bit is set only for `.tcp`, so a `.udp` rule's record
    // is what it was before slice 3 (design 17.9a, point 2).
    b[0] = @as(u8, @intFromBool(e.ip != null)) | (@as(u8, @intFromBool(e.port != null)) << 1) | (@as(u8, @intFromBool(e.protocol == .tcp)) << 2);
    if (e.ip) |ip| @memcpy(b[1..5], &ip);
    if (e.port) |p| std.mem.writeInt(u16, b[5..7], p, .little);
    h.update(&b);
}

fn hashRuleKey(h: *std.hash.Wyhash, k: RuleKey) void {
    h.update(&.{@backingInt(k)});
    switch (k) {
        .impairment, .partition => |p| {
            hashEndpoint(h, p.from);
            hashEndpoint(h, p.to);
        },
        .silence => |e| hashEndpoint(h, e),
    }
}

/// The fault rules in `stateDigest`, for a run that set one: the rules in
/// force in their order with their settings, burst states and counts; every
/// rule of the run with what it matched; the counts by cause; the draws.
fn digestFaults(h: *std.hash.Wyhash) void {
    var b: [8]u8 = undefined;
    for (rules[0..rules_len]) |r| {
        hashRuleKey(h, r.key);
        var e: [67]u8 = @splat(0);
        std.mem.writeInt(u32, e[0..4], r.imp.loss_ppm, .little);
        if (r.imp.burst) |bu| {
            e[4] = 1;
            std.mem.writeInt(u32, e[5..9], bu.enter_ppm, .little);
            std.mem.writeInt(u32, e[9..13], bu.leave_ppm, .little);
        }
        e[13] = @intFromBool(r.bad);
        std.mem.writeInt(u64, e[14..22], r.stats.matched, .little);
        std.mem.writeInt(u64, e[22..30], r.stats.dropped, .little);
        std.mem.writeInt(u64, e[30..38], r.imp.delay_ns, .little);
        std.mem.writeInt(u64, e[38..46], r.imp.jitter_ns, .little);
        std.mem.writeInt(u32, e[46..50], r.imp.dup_ppm, .little);
        e[50] = @intFromBool(r.imp.reorder);
        std.mem.writeInt(u64, e[51..59], r.stats.duplicated, .little);
        std.mem.writeInt(u64, e[59..67], r.stats.delayed, .little);
        h.update(&e);
    }
    for (rule_history[0..rule_history_len]) |hist| {
        hashRuleKey(h, hist.key);
        std.mem.writeInt(u64, &b, hist.matched, .little);
        h.update(&b);
        std.mem.writeInt(u64, &b, hist.first_set_ns, .little);
        h.update(&b);
    }
    inline for (comptime std.meta.fieldNames(NetFaultStats)) |name| {
        std.mem.writeInt(u64, &b, @field(fault_stats, name), .little);
        h.update(&b);
    }
    std.mem.writeInt(u64, &b, fault_draws, .little);
    h.update(&b);
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

    switch (recvInto(pa[0], &[_][]u8{&da}, &ca, false)) {
        .parked => {},
        else => panic("probe: recv A should park", .{}),
    }
    switch (recvInto(pb[0], &[_][]u8{&db}, &cb, false)) {
        .parked => {},
        else => panic("probe: recv B should park", .{}),
    }

    const payload = "x";
    if (first == .a) {
        switch (sendBytes(pa[1], &[_][]const u8{payload}, &sa)) {
            .due => {},
            else => panic("probe: send A should complete", .{}),
        }
        switch (sendBytes(pb[1], &[_][]const u8{payload}, &sb)) {
            .due => {},
            else => panic("probe: send B should complete", .{}),
        }
    } else {
        switch (sendBytes(pb[1], &[_][]const u8{payload}, &sb)) {
            .due => {},
            else => panic("probe: send B should complete", .{}),
        }
        switch (sendBytes(pa[1], &[_][]const u8{payload}, &sa)) {
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

    switch (recvInto(pa[0], &[_][]u8{&da}, &ca, false)) {
        .parked => {},
        else => panic("probe: recv A should park", .{}),
    }
    switch (recvInto(pb[0], &[_][]u8{&db}, &cb, false)) {
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

pub fn expectChildPanics(comptime test_name: []const u8, comptime want: []const u8) !void {
    _ = try runPanicChild(test_name, want);
}

/// `expectChildPanics`, and the child must end within `max_ms` of wall
/// time. Prints how long it took.
fn expectChildPanicsWithin(comptime test_name: []const u8, comptime want: []const u8, max_ms: u64) !void {
    const ns = try runPanicChild(test_name, want);
    std.debug.print("child of \"{s}\" panicked after {d} ms of wall time (at most {d})\n", .{ test_name, ns / std.time.ns_per_ms, max_ms });
    if (ns > max_ms * std.time.ns_per_ms) return error.ChildTooSlow;
}

/// Runs the test `test_name` in a child process and checks it panics with
/// `want`; returns the child's wall time in ns.
fn runPanicChild(comptime test_name: []const u8, comptime want: []const u8) !u64 {
    const io = std.testing.io;
    const gpa = std.testing.allocator;
    var exe_buf: [std.fs.max_path_bytes]u8 = undefined;
    const exe_len = try std.process.executablePath(io, &exe_buf);

    var env: std.process.Environ.Map = .init(gpa);
    defer env.deinit();
    if (std.c.getenv("PATH")) |path| try env.put("PATH", std.mem.span(path));
    try env.put("TEST_FILTER", test_name);
    try env.put(death_child_env, "1");

    const started = std.Io.Timestamp.now(io, .awake);
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
    return @intCast(started.durationTo(std.Io.Timestamp.now(io, .awake)).nanoseconds);
}

/// Whether a line of `stderr` is the panic itself saying `want`: the line
/// `refuse` writes, or `thread <n> panic: <want>`. A plain substring match is
/// not enough: a panic's stack trace prints source lines, and the test's own
/// line holds `want` as a literal, so any other panic would match it. A line
/// may start with the test runner's progress clear (`\r` and `ESC [0K`),
/// which the child's first line of stderr carries; it is skipped.
pub fn hasPanicLine(stderr: []const u8, want: []const u8) bool {
    var it = std.mem.splitScalar(u8, stderr, '\n');
    while (it.next()) |raw| {
        var line = raw;
        while (true) {
            if (std.mem.startsWith(u8, line, "\r")) {
                line = line[1..];
            } else if (std.mem.startsWith(u8, line, "\x1b[0K")) {
                line = line[4..];
            } else break;
        }
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
    // The child's first line, after the runner's progress clear.
    try std.testing.expect(hasPanicLine("\r\x1b[0Ksim: Event.set from a thread that is not\n", want));
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

// ---- A spin at one virtual instant (t-4133) ----

const spin = struct {
    const Waiter = @import("common.zig").Waiter;
    const rt_mod = @import("runtime.zig");

    /// t-4133's shape: a task arms a timer for a moment already past, is
    /// woken at once at the same instant, and arms it again.
    fn pastTimer() void {
        while (true) {
            var w: Waiter = .init();
            defer w.deinit();
            w.timedWait(1, .{ .deadline = .fromNanoseconds(0) }, .allow_cancel) catch {};
        }
    }

    /// Waits for the clock to move by yielding. A main task's yield makes
    /// no task switch and no wake, so the trace stops while the polls go on.
    fn yieldUntilClockMoves() void {
        const start = clock_ns;
        while (clock_ns == start) rt_mod.yield() catch {};
    }

    fn spawnedPastTimer(executors: u8) !void {
        noCoreDump();
        const rt = try death.up(executors);
        var h = try rt.spawnInto(.{ .executor = @intCast(executors - 1) }, pastTimer, .{});
        h.join();
    }
};

test "sim: spin: a task that keeps arming a timer for a moment already past panics at one virtual instant, naming the task and the timer, within seconds of wall time" {
    if (comptime !compiled_in) return error.SkipZigTest;
    if (!isDeathChild()) return expectChildPanicsWithin("sim: spin: a task that keeps arming a timer for a moment already past panics at one virtual instant, naming the task and the timer, within seconds of wall time", "sim: 100001 loop polls at virtual time 0 ns with the clock not moving (the limit is 100000, sim.setInstantPollLimit): a task spins without virtual time passing (t-4133); the last task run or woken has trace id 1; 100000 timers fired at this instant, the last with deadline 0 ns on clock 0", 5000);
    try spin.spawnedPastTimer(1);
}

test "sim: spin: the same with the task on the second of two executors" {
    if (comptime !compiled_in) return error.SkipZigTest;
    if (!isDeathChild()) return expectChildPanicsWithin("sim: spin: the same with the task on the second of two executors", "sim: 100001 loop polls at virtual time 0 ns with the clock not moving (the limit is 100000, sim.setInstantPollLimit): a task spins without virtual time passing (t-4133); the last task run or woken has trace id 1; 33319 timers fired at this instant, the last with deadline 0 ns on clock 0", 5000);
    try spin.spawnedPastTimer(2);
}

test "sim: spin: a main task that yields until the clock moves panics, with no timer to name" {
    if (comptime !compiled_in) return error.SkipZigTest;
    if (!isDeathChild()) return expectChildPanicsWithin("sim: spin: a main task that yields until the clock moves panics, with no timer to name", "sim: 100001 loop polls at virtual time 0 ns with the clock not moving (the limit is 100000, sim.setInstantPollLimit): a task spins without virtual time passing (t-4133); no task has been switched to or woken in this run (the main task?); no timer has fired in this run", 5000);
    noCoreDump();
    _ = try death.up(1);
    spin.yieldUntilClockMoves();
}

test "sim: spin: a lowered limit panics at exactly one poll past it, at the virtual time of the spin" {
    if (comptime !compiled_in) return error.SkipZigTest;
    if (!isDeathChild()) return expectChildPanics("sim: spin: a lowered limit panics at exactly one poll past it, at the virtual time of the spin", "sim: 51 loop polls at virtual time 3000000 ns with the clock not moving (the limit is 50, sim.setInstantPollLimit): a task spins without virtual time passing (t-4133); the last task run or woken has trace id 1; 51 timers fired at this instant, the last with deadline 0 ns on clock 0");
    noCoreDump();
    const rt = try death.up(1);
    setInstantPollLimit(50);
    // Timers fire at 1 and 2 ms too: the line counts only this instant's.
    for (0..3) |_| try spin.rt_mod.sleep(.fromMilliseconds(1));
    var h = try rt.spawn(spin.pastTimer, .{});
    h.join();
}

test "sim: spin refuses: setInstantPollLimit(0)" {
    if (comptime !compiled_in) return error.SkipZigTest;
    if (!isDeathChild()) return expectChildPanics("sim: spin refuses: setInstantPollLimit(0)", "sim: setInstantPollLimit(0): a run polls at least once");
    noCoreDump();
    _ = try death.up(1);
    setInstantPollLimit(0);
}

test "sim: spin: the count starts again when the clock moves: 2000 sleeps of 1 ms under a limit of 10 end normally, and begin restores the default" {
    if (comptime !compiled_in) return error.SkipZigTest;
    const Runtime = @import("runtime.zig").Runtime;
    begin(1);
    defer end();
    const rt = try Runtime.init(std.testing.allocator, .{ .executors = .exact(1), .thread_pool = .{ .max_threads = 0 } });
    defer rt.deinit();
    setInstantPollLimit(10);
    for (0..2000) |_| try spin.rt_mod.sleep(.fromMilliseconds(1));
    try std.testing.expectEqual(@as(u64, 2_000_000_000), clock_ns);
    try std.testing.expectEqual(@as(u64, 10), instant_poll_limit);
    end();
    begin(1);
    try std.testing.expectEqual(default_instant_poll_limit, instant_poll_limit);
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
            // All three stay open to the end: a closed one would free its
            // port for the next.
            var fds: [3]i32 = undefined;
            for (&ports, &fds) |*p, *fd| {
                fd.* = try dg.open();
                try dg.bindTo(fd.*, dg.addr(127, 0, 0, 1, 0));
                p.* = (try dg.local(fd.*)).port;
            }
            for (fds) |fd| dg.os_net.close(fd);
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
            defer dg.os_net.close(b);
            const c = try dg.open();
            defer dg.os_net.close(c);
            try dg.bindTo(a, dg.addr(127, 0, 0, 1, 5000));
            try std.testing.expectError(error.AddressInUse, dg.bindTo(b, dg.addr(127, 0, 0, 1, 5000)));
            try std.testing.expectError(error.AddressInUse, dg.bindTo(b, dg.addr(0, 0, 0, 0, 5000)));
            // Another loopback address on the same port is free.
            try dg.bindTo(b, dg.addr(127, 0, 0, 2, 5000));
            try dg.bindTo(c, dg.addr(0, 0, 0, 0, 6000));
            const d = try dg.open();
            defer dg.os_net.close(d);
            try std.testing.expectError(error.AddressInUse, dg.bindTo(d, dg.addr(127, 0, 0, 9, 6000)));
            // Closing frees the port.
            dg.os_net.close(a);
            try dg.bindTo(d, dg.addr(127, 0, 0, 1, 5000));
            // Port 0 skips a port that is taken.
            const e = try dg.open();
            defer dg.os_net.close(e);
            try dg.bindTo(e, dg.addr(127, 0, 0, 1, 49152));
            const f = try dg.open();
            defer dg.os_net.close(f);
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
            defer dg.os_net.close(server);
            const client = try dg.open();
            defer dg.os_net.close(client);
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
            defer dg.os_net.close(blocking);
            try std.testing.expect(!try dg.os_net.isNonBlocking(blocking));
            try dg.os_net.setNonBlocking(blocking, true);
            try std.testing.expect(try dg.os_net.isNonBlocking(blocking));
            try dg.os_net.setNonBlocking(blocking, false);
            try std.testing.expect(!try dg.os_net.isNonBlocking(blocking));
            const rx = try dg.open();
            defer dg.os_net.close(rx);
            try std.testing.expect(try dg.os_net.isNonBlocking(rx));
            const tx = try dg.open();
            defer dg.os_net.close(tx);
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
            defer dg.os_net.close(rx);
            const tx = try dg.open();
            defer dg.os_net.close(tx);
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
        defer dg.os_net.close(rx);
        const a = try dg.open();
        defer dg.os_net.close(a);
        const b = try dg.open();
        defer dg.os_net.close(b);
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
            defer dg.os_net.close(rx);
            const tx = try dg.open();
            defer dg.os_net.close(tx);
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
                // A submit, as an ev send makes it; the cancel ends the op
                // (due or parked) so the next send is the end's only one.
                const outcome = sendBytes(fds[from], src[0..n], &dummy);
                _ = cancelIo(&dummy);
                switch (outcome) {
                    .due => |got| try std.testing.expectEqual(want, got),
                    .parked => {
                        try std.testing.expectEqual(@as(?usize, null), want);
                        parks += 1;
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
            defer dg.os_net.close(nx);
            try std.testing.expectEqual(rx, nx);
            try dg.bindTo(nx, dg.addr(127, 0, 0, 1, 0));
            const tx = try dg.open();
            defer dg.os_net.close(tx);
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
            defer dg.os_net.close(tx);
            try dg.bindTo(rx, dg.addr(127, 0, 0, 1, 0));
            _ = try dg.sendFrom(tx, dg.addr(127, 0, 0, 1, 49152), null, "old");
            try dg.settle();
            dg.os_net.close(rx);
            const again = try dg.open();
            defer dg.os_net.close(again);
            try std.testing.expectEqual(rx, again);
            try std.testing.expectEqual(Ip4{ .ip = .{ 0, 0, 0, 0 }, .port = 0 }, try dg.local(again));
            try dg.bindTo(again, dg.addr(127, 0, 0, 1, 49152));
            var buf: [8]u8 = undefined;
            try std.testing.expectError(error.WouldBlock, dg.recvInto(again, &.{&buf}));
            try std.testing.expectEqual(@as(u64, 0), datagramStats(again).received);
        }
    }.body);
}

test "sim: dgram: end frees every datagram still on the wire, so the next run starts empty" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    const T = struct {
        var first_fd: i32 = 0;
        // The sockets close at the end of the run's body, as `end` requires
        // (t-4142); the close drops the queued datagram, and `end` frees the
        // one still on the wire.
        fn leave() !void {
            const rx = try dg.open();
            defer dg.os_net.close(rx);
            const tx = try dg.open();
            defer dg.os_net.close(tx);
            first_fd = rx;
            try dg.bindTo(rx, dg.addr(127, 0, 0, 1, 0));
            _ = try dg.sendFrom(tx, dg.addr(127, 0, 0, 1, 49152), null, "queued");
            try dg.settle();
            // Still on the wire at end:
            _ = try dg.sendFrom(tx, dg.addr(127, 0, 0, 1, 49152), null, "in flight");
        }
        fn fresh() !void {
            const fd = try dg.open();
            defer dg.os_net.close(fd);
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
            defer dg.os_net.close(fd);
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
            defer dg.os_net.close(fd);
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
            defer dg.os_net.close(fd);
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
            defer dg.os_net.close(fd);
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
            defer dg.os_net.close(fd);
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
                    defer dg.os_net.close(rx);
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
            defer dg.os_net.close(rx);
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

// Until slice 1 of the stream track this refused `socket(ipv4, stream,
// tcp)`; that socket is modelled now (design 17.4; `sim: stream:` tests
// open it through std.Io with protocol `.tcp`). A stream socket of any
// other protocol still refuses.
test "sim: dgram refuses: a stream socket that is not TCP during a run" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: dgram refuses: a stream socket that is not TCP during a run", "sim: socket(ipv4, stream, udp) during a sim run", struct {
        fn body() !void {
            try r3.up();
            _ = try dg.os_net.socket(.ipv4, .stream, .udp, .{ .nonblocking = true });
        }
    }.body);
}

test "sim: dgram refuses: a send to a non-loopback address" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: dgram refuses: a send to a non-loopback address", "sim: send to 10.0.0.1: only 127.0.0.0/8 is simulated", struct {
        fn body() !void {
            try r3.up();
            const fd = try dg.open();
            defer dg.os_net.close(fd);
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
            defer dg.os_net.close(fd);
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
            defer dg.os_net.close(fd);
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
            defer dg.os_net.close(fd);
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
            defer dg.os_net.close(fd);
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

// Until t-4167 every connect on a datagram socket refused, and this test
// checked that. A connect is simulated now (`sim_dgram_connect_test.zig`);
// one to an address outside 127/8 still refuses, as a send there does.
test "sim: dgram refuses: connect on a datagram socket to an address outside 127/8" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: dgram refuses: connect on a datagram socket to an address outside 127/8", "sim: connect to 10.0.0.1: only 127.0.0.0/8 is simulated", struct {
        fn body() !void {
            try r3.up();
            const fd = try dg.open();
            defer dg.os_net.close(fd);
            const a = dg.addr(10, 0, 0, 1, 9);
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
            defer dg.os_net.close(fd);
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
            defer dg.os_net.close(fd);
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
            defer dg.os_net.close(fd);
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
            defer dg.os_net.close(c_fd);
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
            defer dg.os_net.close(rx);
            const tx = try dg.open();
            defer dg.os_net.close(tx);
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
            defer dg.os_net.close(dg_park.rx);
            dg_park.tx = try dg.open();
            defer dg.os_net.close(dg_park.tx);
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
            defer dg.os_net.close(dg_park.rx);
            dg_park.tx = try dg.open();
            defer dg.os_net.close(dg_park.tx);
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
            defer dg.os_net.close(rx);
            _ = try dg.evBind(rx, dg.addr(127, 0, 0, 1, 0));
            var buf: [4]u8 = undefined;
            var iov: [1]dg.os_net.iovec = undefined;
            var op = dg.ev.NetRecvFrom.init(rx, .fromSlice(&buf, &iov), .{ .dontwait = true }, null, null);
            try dg.io_common.waitForIo(&op.c);
            try std.testing.expectError(error.WouldBlock, op.getResult());
            const tx = try dg.open();
            defer dg.os_net.close(tx);
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

/// Round 3 of the outside review, findings 2 and 3: a second ev op behind a
/// woken one. Raw ops on the test task's loop, each with a callback (a
/// completion with none would be read as a task wake).
const second_waiter = struct {
    const Raw = union { recv: dg.ev.NetRecv, send: dg.ev.NetSend, recvfrom: dg.ev.NetRecvFrom };
    var ops: [6]Raw = undefined;
    var iovs: [6][1]dg.os_net.iovec = undefined;
    var ciovs: [6][1]dg.os_net.iovec_const = undefined;
    var bufs: [6][4096]u8 = undefined;
    var fds: [2]i32 = undefined;
    var first: bool = true;

    fn noop(_: *dg.ev.Loop, _: *dg.ev.Completion) void {}

    fn recv(k: usize, fd: i32, room: usize) *dg.ev.Completion {
        ops[k] = .{ .recv = dg.ev.NetRecv.init(fd, .fromSlice(bufs[k][0..room], &iovs[k]), .{}) };
        ops[k].recv.c.callback = noop;
        return &ops[k].recv.c;
    }

    fn send(k: usize, fd: i32, len: usize) *dg.ev.Completion {
        ops[k] = .{ .send = dg.ev.NetSend.init(fd, .fromSlice(bufs[k][0..len], &ciovs[k]), .{}) };
        ops[k].send.c.callback = noop;
        return &ops[k].send.c;
    }

    /// R1 parks on end 0; S copies into it and wakes R1, due after the
    /// seeded delay. R2 is submitted before R1's harvest. Before the rule,
    /// R2 took the bytes, R3 parked, and R1's harvest found R3 in its slot.
    fn receives() !void {
        fds = pipePair();
        const loop = &dg.rt_mod.getCurrentExecutor().loop;
        loop.add(recv(0, fds[0], 16));
        loop.add(send(1, fds[1], 4));
        loop.add(recv(2, fds[0], 16));
        loop.add(recv(3, fds[0], 16));
        try dg.rt_mod.sleep(.fromMilliseconds(20));
    }

    /// S0 fills end 0's buffer; S1 parks on end 1; R drains 100 bytes and
    /// wakes S1. S2 is submitted before S1's harvest. Before the rule, S2
    /// took the room, S3 parked, and S1's harvest found S3 in its slot.
    fn sends() !void {
        fds = pipePair();
        const loop = &dg.rt_mod.getCurrentExecutor().loop;
        loop.add(send(0, fds[1], pipe_buf_cap));
        loop.add(send(1, fds[1], 10));
        loop.add(recv(2, fds[0], 100));
        loop.add(send(3, fds[1], 100));
        loop.add(send(4, fds[1], 10));
        try dg.rt_mod.sleep(.fromMilliseconds(20));
    }

    /// Receives on sockets X and Y woken by landings at one time. The first
    /// one's callback submits two receives on the other's socket, whose
    /// receive is woken and later in the batch. Before the rule, the first
    /// took the datagram, the second parked, and the woken receive's harvest
    /// found it in its slot.
    fn onDgram(loop: *dg.ev.Loop, c: *dg.ev.Completion) void {
        if (!first) return;
        first = false;
        const k: usize = @intFromPtr(c.userdata.?) - 1;
        for (2..4) |j| {
            ops[j] = .{ .recvfrom = dg.ev.NetRecvFrom.init(fds[1 - k], .fromSlice(&bufs[j], &iovs[j]), .{}, null, null) };
            ops[j].recvfrom.c.callback = noop;
            loop.add(&ops[j].recvfrom.c);
        }
    }

    fn dgrams() !void {
        first = true;
        const loop = &dg.rt_mod.getCurrentExecutor().loop;
        const tx = try dg.open();
        defer dg.os_net.close(tx);
        for (0..2) |k| {
            fds[k] = try dg.open();
            try dg.bindTo(fds[k], dg.addr(127, 0, 0, 1, 0));
            ops[k] = .{ .recvfrom = dg.ev.NetRecvFrom.init(fds[k], .fromSlice(&bufs[k], &iovs[k]), .{}, null, null) };
            ops[k].recvfrom.c.flags.defer_callback = false;
            ops[k].recvfrom.c.callback = onDgram;
            ops[k].recvfrom.c.userdata = @ptrFromInt(k + 1);
            loop.add(&ops[k].recvfrom.c);
        }
        for (0..2) |k| _ = try dg.sendFrom(tx, dg.addr(127, 0, 0, 1, (try dg.local(fds[k])).port), null, "x");
        try dg.rt_mod.sleep(.fromMilliseconds(1));
    }
};

/// A receive with room on a full pipe frees room, so it wakes the send
/// parked on the other end. (Until t-4096 this test also gave a receive with
/// no room first, which took nothing and must not wake the send. Such a
/// receive is refused now: the two tests after this one.)
const pipe_zero_room = struct {
    fn fill() !void {
        const w = second_waiter;
        w.fds = pipePair();
        const loop = &dg.rt_mod.getCurrentExecutor().loop;
        loop.add(w.send(0, w.fds[1], pipe_buf_cap));
        loop.add(w.send(1, w.fds[1], 10));
        try dg.rt_mod.sleep(.fromMilliseconds(10));
        // S0 is done, S1 parked on the full pipe.
        try std.testing.expectEqual(@as(usize, pipe_buf_cap), try w.ops[0].send.getResult());
        try std.testing.expect(!w.ops[1].send.c.has_result);
    }

    fn body() !void {
        try fill();
        const w = second_waiter;
        const loop = &dg.rt_mod.getCurrentExecutor().loop;
        loop.add(w.recv(3, w.fds[0], 100));
        try dg.rt_mod.sleep(.fromMilliseconds(10));
        try std.testing.expectEqual(@as(usize, 100), try w.ops[3].recv.getResult());
        try std.testing.expectEqual(@as(usize, 10), try w.ops[1].send.getResult());
    }

    /// t-4096's case: a receive of 0 bytes on an empty pipe. Before the
    /// refusal it parked until bytes or end of stream came.
    fn emptyPipe() !void {
        const w = second_waiter;
        w.fds = pipePair();
        const loop = &dg.rt_mod.getCurrentExecutor().loop;
        loop.add(w.recv(0, w.fds[0], 0));
        try dg.rt_mod.sleep(.fromMilliseconds(10));
        std.debug.print("a receive of 0 bytes on an empty pipe: has_result {}\n", .{w.ops[0].recv.c.has_result});
    }

    /// The case this test's first half had: a receive of 0 bytes on a
    /// full pipe with a send parked on it. Before the refusal it gave 0.
    fn fullPipe() !void {
        try fill();
        const w = second_waiter;
        const loop = &dg.rt_mod.getCurrentExecutor().loop;
        loop.add(w.recv(2, w.fds[0], 0));
        try dg.rt_mod.sleep(.fromMilliseconds(10));
        std.debug.print("a receive of 0 bytes on a full pipe: {any}\n", .{w.ops[2].recv.getResult()});
    }
};

test "sim: pipe: a receive with room on a full pipe wakes the send parked on it" {
    if (comptime !compiled_in) return error.SkipZigTest;
    for (1..9) |s| try dg.run(s, pipe_zero_room.body);
}

test "sim: pipe refuses: a receive of 0 bytes on an empty pipe (t-4096)" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: pipe refuses: a receive of 0 bytes on an empty pipe (t-4096)", "sim: a receive of 0 bytes on sim pipe fd 100000 is refused (t-4096)", struct {
        fn body() !void {
            try dg.run(1, pipe_zero_room.emptyPipe);
        }
    }.body);
}

test "sim: pipe refuses: a receive of 0 bytes on a full pipe with a send parked on it (t-4096)" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: pipe refuses: a receive of 0 bytes on a full pipe with a send parked on it (t-4096)", "sim: a receive of 0 bytes on sim pipe fd 100000 is refused (t-4096)", struct {
        fn body() !void {
            try dg.run(1, pipe_zero_room.fullPipe);
        }
    }.body);
}

/// Round 4 of the outside review, finding 1: the due list is the sim's, not a
/// loop's, so a loop's harvest takes every due op, also one submitted on a
/// loop of another LoopGroup, and finishes it through its own state. Two
/// bare loops A and B (each its own group): an op submitted on B with no
/// callback, then A polled. Before the refusal, A took the completion into
/// its own dispatched queue and B never saw it.
const two_groups = struct {
    const Loop = @import("ev/loop.zig").Loop;

    fn run(comptime pipe: bool) !void {
        begin(1);
        defer end();
        var a: Loop = undefined;
        var b: Loop = undefined;
        try a.init(.{});
        defer {
            a.bindThread();
            a.deinit();
        }
        try b.init(.{});
        defer {
            b.bindThread();
            b.deinit();
        }
        var poll_op: dg.ev.NetPoll = undefined;
        var send_op: dg.ev.NetSend = undefined;
        var iov: [1]dg.os_net.iovec_const = undefined;
        const c = if (pipe) blk: {
            const fds = pipePair();
            send_op = dg.ev.NetSend.init(fds[1], .fromSlice("x", &iov), .{});
            break :blk &send_op.c;
        } else blk: {
            const fd = try dg.open();
            poll_op = dg.ev.NetPoll.init(fd, .send);
            break :blk &poll_op.c;
        };
        b.bindThread();
        b.add(c);
        a.bindThread();
        try a.poll(.fromMilliseconds(10));
        const took = a.nextDispatched();
        b.bindThread();
        try b.poll(.fromMilliseconds(10));
        const got = b.nextDispatched();
        std.debug.print("loop A's dispatched queue: {s}; loop B's: {s}\n", .{ if (took == c) "the op submitted on B" else "nothing", if (got == c) "its own op" else "nothing" });
        return error.NoRefusal;
    }
};

test "sim: refuses a loop of a second loop group polling in one run, which would take a datagram op submitted on the first" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: refuses a loop of a second loop group polling in one run, which would take a datagram op submitted on the first", "sim: a sim run has one loop group", struct {
        fn body() !void {
            try two_groups.run(false);
        }
    }.body);
}

test "sim: refuses a loop of a second loop group polling in one run, which would take a pipe op submitted on the first" {
    try refusal("sim: refuses a loop of a second loop group polling in one run, which would take a pipe op submitted on the first", "sim: a sim run has one loop group", struct {
        fn body() !void {
            try two_groups.run(true);
        }
    }.body);
}

test "sim: dgram: datagramSocketsOpen counts this run's open datagram sockets, 0 outside a run (t-4136)" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    try std.testing.expectEqual(@as(usize, 0), datagramSocketsOpen());
    try dg.run(1, struct {
        fn body() !void {
            try std.testing.expectEqual(@as(usize, 0), datagramSocketsOpen());
            const a = try dg.open();
            const b = try dg.open();
            try std.testing.expectEqual(@as(usize, 2), datagramSocketsOpen());
            dg.os_net.close(a);
            try std.testing.expectEqual(@as(usize, 1), datagramSocketsOpen());
            dg.os_net.close(b);
            try std.testing.expectEqual(@as(usize, 0), datagramSocketsOpen());
        }
    }.body);
    try std.testing.expectEqual(@as(usize, 0), datagramSocketsOpen());
}

test "sim: dgram: a run that closed its datagram sockets ends normally, and the next run starts with none open (t-4142)" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    try dg.run(1, struct {
        fn body() !void {
            const a = try dg.open();
            dg.os_net.close(a);
        }
    }.body);
    try dg.run(1, struct {
        fn body() !void {
            try std.testing.expectEqual(@as(usize, 0), datagramSocketsOpen());
        }
    }.body);
}

test "sim: dgram refuses: sim.end with a datagram socket open (t-4142)" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: dgram refuses: sim.end with a datagram socket open", "sim: datagram teardown: 1 datagram sockets are open at sim.end (listed above): close every datagram socket before sim.end", struct {
        fn body() !void {
            try dg.run(1, struct {
                fn inner() !void {
                    const a = try dg.open();
                    _ = try dg.open();
                    dg.os_net.close(a);
                }
            }.inner);
        }
    }.body);
}

// The list line, before the panic: the open socket by its own fd (the
// second open, 100033), not the closed one (100032).
test "sim: dgram refuses: sim.end lists each open datagram socket by its fd before it panics (t-4142)" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: dgram refuses: sim.end lists each open datagram socket by its fd", "sim: datagram teardown: sim fd 100033 is an open datagram socket", struct {
        fn body() !void {
            try dg.run(1, struct {
                fn inner() !void {
                    const a = try dg.open();
                    _ = try dg.open();
                    dg.os_net.close(a);
                }
            }.inner);
        }
    }.body);
}

test "sim: dgram refuses: an open of a slot whose generation is at its limit" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: dgram refuses: an open of a slot whose generation is at its limit", "sim: datagram socket slot 0 was opened 4294967295 times in this run", struct {
        fn body() !void {
            begin(1);
            testSetDgramGen(0, std.math.maxInt(u32));
            _ = try dg.open();
        }
    }.body);
}

test "sim: refuses two Runtimes in one sim run" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: refuses two Runtimes in one sim run", "sim: a sim run has one loop group", struct {
        var got: usize = 0;
        fn exchange() !void {
            const rx = try dg.open();
            defer dg.os_net.close(rx);
            try dg.bindTo(rx, dg.addr(127, 0, 0, 1, 7100));
            const tx = try dg.open();
            defer dg.os_net.close(tx);
            _ = try dg.sendFrom(tx, dg.addr(127, 0, 0, 1, 7100), null, "hi");
            var buf: [8]u8 = undefined;
            got = (try dg.evRecvFrom(rx, &buf)).len;
        }
        fn body() !void {
            begin(1);
            var o: dg.rt_mod.RuntimeOptions = .{ .executors = .exact(1), .thread_pool = .{ .max_threads = 0 } };
            o.stack_pool.shrink_interval = .zero;
            const rt1 = try dg.rt_mod.Runtime.init(std.heap.page_allocator, o);
            const rt2 = try dg.rt_mod.Runtime.init(std.heap.page_allocator, o);
            _ = rt2;
            var h = try rt1.spawn(exchange, .{});
            try h.join();
        }
    }.body);
}

test "sim: pipe refuses: a second ev receive on an end whose receive is woken and not yet harvested" {
    try refusal("sim: pipe refuses: a second ev receive on an end whose receive is woken and not yet harvested", "sim: a second ev receive while one is woken and not yet harvested, on sim pipe fd 100000", struct {
        fn body() !void {
            try dg.run(1, second_waiter.receives);
        }
    }.body);
}

test "sim: pipe refuses: a second ev send on an end whose send is woken and not yet harvested" {
    try refusal("sim: pipe refuses: a second ev send on an end whose send is woken and not yet harvested", "sim: a second ev send while one is woken and not yet harvested, on sim pipe fd 100001", struct {
        fn body() !void {
            try dg.run(1, second_waiter.sends);
        }
    }.body);
}

test "sim: dgram ev refuses: a second ev receive on a socket whose receive is woken and not yet harvested, from a callback earlier in the batch" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: dgram ev refuses: a second ev receive on a socket whose receive is woken and not yet harvested, from a callback earlier in the batch", "sim: a second ev receive or poll while one is woken and not yet harvested, on sim datagram socket", struct {
        fn body() !void {
            try dg.run(1, second_waiter.dgrams);
        }
    }.body);
}

test "sim: dgram ev refuses: two ev ops parked on one socket" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: dgram ev refuses: two ev ops parked on one socket", "sim: a second ev receive or poll while one is parked, on sim datagram socket 100032", struct {
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
            defer dg.os_net.close(fd);
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
            defer dg.os_net.close(fd);
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
            defer dg.os_net.close(tx);
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
            defer dg.os_net.close(fd);
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
            defer dg.os_net.close(tx);
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
            defer dg.os_net.close(rx);
            const tx = try dg.open();
            defer dg.os_net.close(tx);
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
            defer dg.os_net.close(rx);
            const tx = try dg.open();
            defer dg.os_net.close(tx);
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
            defer dg.os_net.close(rx);
            const tx = try dg.open();
            defer dg.os_net.close(tx);
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
            defer dg.os_net.close(small);
            const big = try dg.open();
            defer dg.os_net.close(big);
            const tx = try dg.open();
            defer dg.os_net.close(tx);
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
            defer dg.os_net.close(ok_fd);
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
            defer dg.os_net.close(fd);
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
            defer dg.os_net.close(fd);
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
            defer dg.os_net.close(fd);
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
            defer dg.os_net.close(fd);
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
            defer dg.os_net.close(fd);
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
            defer dg.os_net.close(fd);
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
            defer dg.os_net.close(fd);
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
    /// Fault rules set before the sends, for promise 2. None of them acts.
    const Rules = enum { none, set_clear, unmatched };
    var rules_before: Rules = .none;
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
        defer dg.os_net.close(rx);
        try dg.bindTo(rx, dg.addr(0, 0, 0, 0, 7000));
        try dg.os_net.enableDestinationAddress(rx);
        var senders: [3]i32 = undefined;
        for (&senders) |*fd| fd.* = try dg.open();
        defer for (senders) |fd| dg.os_net.close(fd);
        transcript_len = 0;
        var h = try dg.runtime.spawn(evFirst, .{rx});
        try dg.rt_mod.sleep(.fromMilliseconds(1));
        switch (rules_before) {
            .none => {},
            .set_clear => {
                setImpairment(.{}, .{ .loss_ppm = 500_000, .burst = .{ .enter_ppm = 10, .leave_ppm = 10 } });
                setPartition(.{});
                silence(.{});
                _ = clearImpairment(.{});
                _ = healPartition(.{});
                _ = unsilence(.{});
            },
            .unmatched => {
                setImpairment(.{ .to = .{ .port = 7999 } }, .{ .loss_ppm = 500_000 });
                setPartition(.{ .from = .{ .port = 7998 } });
                silence(.{ .ip = .{ 127, 0, 0, 99 } });
            },
        }
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
        rules_before = .none;
        try dg.run(seed_value, body);
        return out;
    }

    fn runRules(seed_value: u64, r: Rules) !Out {
        flip = false;
        rules_before = r;
        defer rules_before = .none;
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

/// A datagram workload whose landings take every phase 1 outcome: two land,
/// two find the receive queue full, one finds no socket. Pinned below.
const dg_drops = struct {
    const Out = struct { wire: u64, trace: u64, state: u64 };
    var out: Out = undefined;

    fn body() !void {
        const rx = try dg.open();
        defer dg.os_net.close(rx);
        try dg.bindTo(rx, dg.addr(127, 0, 0, 1, 7000));
        // Two 10-byte datagrams cost 2 * (10 + 256) = 532 bytes; a third does not fit.
        try dg.setInt(rx, sys.SOL.SOCKET, sys.SO.RCVBUF, 600);
        const tx = try dg.open();
        defer dg.os_net.close(tx);
        try dg.bindTo(tx, dg.addr(127, 0, 0, 1, 7100));
        for (0..4) |k| {
            const p: [10]u8 = @splat(@intCast('a' + k));
            _ = try dg.sendFrom(tx, dg.addr(127, 0, 0, 1, 7000), null, &p);
        }
        _ = try dg.sendFrom(tx, dg.addr(127, 0, 0, 1, 7999), null, "nobody");
        try dg.settle();
        var buf: [16]u8 = undefined;
        var got: usize = 0;
        while (dg.recvInto(rx, &.{&buf})) |_| got += 1 else |err| if (err != error.WouldBlock) return err;
        if (got != 2 or datagramStats(rx).dropped_rcvbuf != 2 or datagramsDroppedNoSocket() != 1) return error.WrongOutcomes;
        out = .{ .wire = wireDigest(), .trace = traceHash(), .state = stateDigest() };
    }

    fn run(seed_value: u64) !Out {
        try dg.run(seed_value, body);
        return out;
    }
};

// Measured at 33b95ca, before any fault code existed (3 of 3 runs gave
// these). A run that sets no fault rule must keep them: promise 1 of design
// section 7, for datagram runs. The pipe-only pins are in "a run with no
// datagram socket keeps ...".
test "sim: dgram trace: datagram runs with no fault rule keep the trace, state and wire digests they had at 33b95ca" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    const w = try dg_wire.run(9, false);
    const d = try dg_drops.run(2);
    try std.testing.expectEqual(@as(u64, 0xb95f842cbcbc9534), w.wire);
    try std.testing.expectEqual(@as(u64, 0x4e7eaa6bc33a0140), w.trace);
    try std.testing.expectEqual(@as(u64, 0xe99b17b275d61903), w.state);
    try std.testing.expectEqual(@as(u64, 0x5bd00146b45c77d0), d.wire);
    try std.testing.expectEqual(@as(u64, 0xcc75045db683d758), d.trace);
    try std.testing.expectEqual(@as(u64, 0xfd06f5dc797433e9), d.state);
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
        defer dg.os_net.close(rx);
        try dg.bindTo(rx, dg.addr(0, 0, 0, 0, 7000));
        const spare = try dg.open(); // a second socket on 7001 would hide a bad port
        defer dg.os_net.close(spare);
        const tx = try dg.open();
        defer dg.os_net.close(tx);
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
    _ = @import("sim_stream_test.zig");
    _ = @import("sim_stream_bytes_test.zig");
    _ = @import("sim_stream_fault_test.zig");
    _ = @import("sim_stream_op_model_test.zig");
    _ = @import("sim_dgram_connect_test.zig");
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

// ---- Phase 2, slice 1: fault rules, drops, counts and records ----

const nf = struct {
    fn ep(d: u8, port: u16) Endpoint {
        return .{ .ip = .{ 127, 0, 0, d }, .port = port };
    }

    fn sock(d: u8, port: u16) !i32 {
        const fd = try dg.open();
        try dg.bindTo(fd, dg.addr(127, 0, 0, d, port));
        return fd;
    }

    /// Sends datagram number `k` (4 bytes) from `fd` to 127.0.0.`d`:`port`.
    fn send(fd: i32, d: u8, port: u16, k: u32) !void {
        var p: [4]u8 = undefined;
        std.mem.writeInt(u32, &p, k, .little);
        _ = try dg.sendFrom(fd, dg.addr(127, 0, 0, d, port), null, &p);
    }

    /// Takes everything queued on `fd`: the numbers, in order, into `out`.
    fn drain(fd: i32, out: []u32) !usize {
        var n: usize = 0;
        var buf: [8]u8 = undefined;
        while (dg.recvInto(fd, &.{&buf})) |got| {
            if (got.info.len != 4) return error.NotANumberedDatagram;
            out[n] = std.mem.readInt(u32, buf[0..4], .little);
            n += 1;
        } else |err| if (err != error.WouldBlock) return err;
        return n;
    }

    fn count(fd: i32) !usize {
        var out: [1100]u32 = undefined;
        return drain(fd, &out);
    }

    fn noOtherCause(st: NetFaultStats, comptime keep: []const []const u8) !void {
        inline for (comptime std.meta.fieldNames(NetFaultStats)) |name| {
            const kept = comptime for (keep) |k| {
                if (std.mem.eql(u8, k, name)) break true;
            } else false;
            if (!kept and @field(st, name) != 0) {
                std.debug.print("{s} = {d}, expected 0\n", .{ name, @field(st, name) });
                return error.UnexpectedCause;
            }
        }
    }
};

const nf_silence = struct {
    fn body() !void {
        const a = try nf.sock(1, 7000);
        defer dg.os_net.close(a);
        const b = try nf.sock(1, 7001);
        defer dg.os_net.close(b);
        const c = try nf.sock(1, 7002);
        defer dg.os_net.close(c);
        silence(nf.ep(1, 7001));
        for (0..3) |k| try nf.send(a, 1, 7001, @intCast(k));
        for (0..2) |k| try nf.send(b, 1, 7000, @intCast(k));
        for (0..4) |k| try nf.send(a, 1, 7002, @intCast(k));
        try nf.send(c, 1, 7000, 9);
        try dg.settle();
        try std.testing.expectEqual(@as(usize, 0), try nf.count(b));
        var got: [8]u32 = undefined;
        try std.testing.expectEqual(@as(usize, 1), try nf.drain(a, &got));
        try std.testing.expectEqual(@as(u32, 9), got[0]);
        try std.testing.expectEqual(@as(usize, 4), try nf.count(c));
        const st = netFaultStats();
        try std.testing.expectEqual(@as(u64, 5), st.silenced_at_send);
        try nf.noOtherCause(st, &.{"silenced_at_send"});
        try std.testing.expectEqual(@as(u64, 0), netFaultDraws());
        // The sender succeeded for every datagram, dropped or not.
        try std.testing.expectEqual(@as(u64, 7), datagramStats(a).sent);
        const rs = unsilence(nf.ep(1, 7001));
        try std.testing.expectEqual(RuleStats{ .matched = 5, .dropped = 5 }, rs);
        try nf.send(a, 1, 7001, 7);
        try dg.settle();
        try std.testing.expectEqual(@as(usize, 1), try nf.count(b));
    }
};

test "sim: net faults: a silence of an exact endpoint drops what it sends and what is sent to it, counts each, and unsilence ends it" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    try dg.run(1, nf_silence.body);
}

const nf_silence_forms = struct {
    fn body() !void {
        const a = try nf.sock(1, 7000);
        defer dg.os_net.close(a);
        const b = try nf.sock(1, 7001);
        defer dg.os_net.close(b);
        const w = try dg.open();
        defer dg.os_net.close(w);
        try dg.bindTo(w, dg.addr(0, 0, 0, 0, 7010));
        silence(.{ .port = 7010 });
        try nf.send(a, 1, 7010, 1);
        try nf.send(a, 3, 7010, 2);
        try nf.send(w, 1, 7000, 3);
        const h1 = try nf.sock(5, 7020);
        defer dg.os_net.close(h1);
        const h2 = try nf.sock(5, 7021);
        defer dg.os_net.close(h2);
        silence(.{ .ip = .{ 127, 0, 0, 5 } });
        try nf.send(a, 5, 7020, 4);
        try nf.send(a, 5, 7021, 5);
        try nf.send(h1, 1, 7000, 6);
        try nf.send(a, 1, 7001, 7);
        try dg.settle();
        try std.testing.expectEqual(@as(usize, 0), try nf.count(w));
        try std.testing.expectEqual(@as(usize, 0), try nf.count(h1));
        try std.testing.expectEqual(@as(usize, 0), try nf.count(h2));
        try std.testing.expectEqual(@as(usize, 0), try nf.count(a));
        try std.testing.expectEqual(@as(usize, 1), try nf.count(b));
        try std.testing.expectEqual(@as(u64, 6), netFaultStats().silenced_at_send);
        try std.testing.expectEqual(RuleStats{ .matched = 3, .dropped = 3 }, unsilence(.{ .port = 7010 }));
        try std.testing.expectEqual(RuleStats{ .matched = 3, .dropped = 3 }, unsilence(.{ .ip = .{ 127, 0, 0, 5 } }));
    }
};

test "sim: net faults: a silence by port alone covers a socket bound to the wildcard, and by address alone a whole host" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    try dg.run(1, nf_silence_forms.body);
}

const nf_partition = struct {
    fn body() !void {
        const a = try nf.sock(1, 7000);
        defer dg.os_net.close(a);
        const b = try nf.sock(1, 7001);
        defer dg.os_net.close(b);
        const ab: Path = .{ .from = nf.ep(1, 7000), .to = nf.ep(1, 7001) };
        setPartition(ab);
        for (0..2) |k| try nf.send(a, 1, 7001, @intCast(k));
        for (0..2) |k| try nf.send(b, 1, 7000, @intCast(k));
        try dg.settle();
        try std.testing.expectEqual(@as(usize, 0), try nf.count(b));
        try std.testing.expectEqual(@as(usize, 2), try nf.count(a));
        setPartition(ab.reversed());
        try nf.send(b, 1, 7000, 5);
        try nf.send(a, 1, 7001, 6);
        try dg.settle();
        try std.testing.expectEqual(@as(usize, 0), try nf.count(a));
        try std.testing.expectEqual(@as(usize, 0), try nf.count(b));
        try std.testing.expectEqual(RuleStats{ .matched = 3, .dropped = 3 }, healPartition(ab));
        try std.testing.expectEqual(RuleStats{ .matched = 1, .dropped = 1 }, healPartition(ab.reversed()));
        try nf.send(a, 1, 7001, 7);
        try nf.send(b, 1, 7000, 8);
        try dg.settle();
        try std.testing.expectEqual(@as(usize, 1), try nf.count(a));
        try std.testing.expectEqual(@as(usize, 1), try nf.count(b));
        try std.testing.expectEqual(@as(u64, 4), netFaultStats().partitioned_at_send);
        try nf.noOtherCause(netFaultStats(), &.{"partitioned_at_send"});
    }
};

test "sim: net faults: a partition drops one direction only, its reversed path the other, and heal restores both" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    try dg.run(1, nf_partition.body);
}

const nf_in_flight = struct {
    fn body() !void {
        const a = try nf.sock(1, 7000);
        defer dg.os_net.close(a);
        const b = try nf.sock(1, 7001);
        defer dg.os_net.close(b);
        const ab: Path = .{ .from = nf.ep(1, 7000), .to = nf.ep(1, 7001) };
        // On the wire when the partition starts: dropped at landing.
        try nf.send(a, 1, 7001, 1);
        setPartition(ab);
        try dg.settle();
        try std.testing.expectEqual(@as(usize, 0), try nf.count(b));
        try std.testing.expectEqual(RuleStats{ .matched = 1, .dropped = 1 }, healPartition(ab));
        // A partition set and healed while it is on the wire: it lands.
        try nf.send(a, 1, 7001, 2);
        setPartition(ab);
        try std.testing.expectEqual(RuleStats{}, healPartition(ab));
        try dg.settle();
        try std.testing.expectEqual(@as(usize, 1), try nf.count(b));
        // The same for a silence, of the sender this time.
        try nf.send(a, 1, 7001, 3);
        silence(nf.ep(1, 7000));
        try dg.settle();
        try std.testing.expectEqual(@as(usize, 0), try nf.count(b));
        try std.testing.expectEqual(RuleStats{ .matched = 1, .dropped = 1 }, unsilence(nf.ep(1, 7000)));
        const st = netFaultStats();
        try std.testing.expectEqual(@as(u64, 1), st.partitioned_in_flight);
        try std.testing.expectEqual(@as(u64, 1), st.silenced_in_flight);
        try nf.noOtherCause(st, &.{ "partitioned_in_flight", "silenced_in_flight" });
    }
};

test "sim: net faults: a partition or silence that starts while a datagram is on the wire drops it at landing, and one healed before landing lets it land" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    try dg.run(1, nf_in_flight.body);
}

/// The test's own copy of the draw formula in `faultDraw`'s doc comment,
/// and of the Gilbert rule in the fault protocol above `pub const Endpoint`.
/// It calls nothing of the sim.
const nf_model = struct {
    fn mix(x: u64) u64 {
        var z = x;
        z = (z ^ (z >> 30)) *% 0xbf58476d1ce4e5b9;
        z = (z ^ (z >> 27)) *% 0x94d049bb133111eb;
        return z ^ (z >> 31);
    }

    var draws: u64 = 0;

    fn draw(seed_value: u64, send_no: u64, purpose: u64) u64 {
        draws += 1;
        return mix(mix(seed_value ^ 0x6e65745f6661756c) +% (send_no *% 8 +% purpose) *% 0x9e3779b97f4a7c15);
    }

    fn hit(seed_value: u64, ppm: u32, send_no: u64, purpose: u64) bool {
        if (ppm == 0) return false;
        if (ppm >= 1_000_000) return true;
        return draw(seed_value, send_no, purpose) % 1_000_000 < ppm;
    }
};

const nf_loss = struct {
    const seed_value: u64 = 11;
    const n: u32 = 2000;
    const imp: Impairment = .{ .loss_ppm = 300_000, .burst = .{ .enter_ppm = 50_000, .leave_ppm = 250_000 } };
    var landed: [n]bool = undefined;

    fn body() !void {
        const a = try nf.sock(1, 7000);
        defer dg.os_net.close(a);
        const b = try nf.sock(1, 7001);
        defer dg.os_net.close(b);
        const c = try nf.sock(1, 7002);
        defer dg.os_net.close(c);
        setImpairment(.{ .from = nf.ep(1, 7000), .to = nf.ep(1, 7001) }, imp);
        landed = @splat(false);
        var k: u32 = 0;
        while (k < n) {
            // Sends draw nothing from the run's RNG, faulted or not.
            const before = rng.s;
            var j: u32 = 0;
            while (j < 100) : (j += 1) {
                try nf.send(a, 1, 7001, k + j); // send number 2(k+j)
                try nf.send(a, 1, 7002, k + j); // 2(k+j)+1, no rule
            }
            if (!std.mem.eql(u64, &before, &rng.s)) return error.FaultDrewTheRunRng;
            try dg.settle();
            var got: [100]u32 = undefined;
            for (got[0..try nf.drain(b, &got)]) |x| landed[x] = true;
            try std.testing.expectEqual(@as(usize, 100), try nf.count(c));
            k += 100;
        }
    }
};

test "sim: net faults: loss and burst loss drop exactly the datagrams the test's own copy of the draw formula predicts, and draw nothing from the run's RNG" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    try dg.run(nf_loss.seed_value, nf_loss.body);
    nf_model.draws = 0;
    var bad = false;
    var lost: u64 = 0;
    var lost_burst: u64 = 0;
    const b = nf_loss.imp.burst.?;
    for (0..nf_loss.n) |k| {
        const send_no: u64 = 2 * k;
        if (bad) {
            if (nf_model.hit(nf_loss.seed_value, b.leave_ppm, send_no, 1)) bad = false;
        } else {
            if (nf_model.hit(nf_loss.seed_value, b.enter_ppm, send_no, 1)) bad = true;
        }
        const want_landed = if (bad) blk: {
            lost_burst += 1;
            break :blk false;
        } else if (nf_model.hit(nf_loss.seed_value, nf_loss.imp.loss_ppm, send_no, 2)) blk: {
            lost += 1;
            break :blk false;
        } else true;
        if (want_landed != nf_loss.landed[k]) {
            std.debug.print("datagram {d}: model says landed={}, the sim {}\n", .{ k, want_landed, nf_loss.landed[k] });
            return error.ModelDiffers;
        }
    }
    // Both causes happened often enough to mean something.
    try std.testing.expect(lost > 200 and lost_burst > 200);
    const st = netFaultStats();
    try std.testing.expectEqual(lost, st.lost);
    try std.testing.expectEqual(lost_burst, st.lost_burst);
    try nf.noOtherCause(st, &.{ "lost", "lost_burst" });
    try std.testing.expectEqual(nf_model.draws, netFaultDraws());
    std.debug.print("loss model: {d} sent, {d} lost, {d} lost in bursts, {d} draws\n", .{ nf_loss.n, lost, lost_burst, nf_model.draws });
}

const nf_last_wins = struct {
    fn body() !void {
        const a = try nf.sock(1, 7000);
        defer dg.os_net.close(a);
        const b = try nf.sock(1, 7001);
        defer dg.os_net.close(b);
        const c = try nf.sock(1, 7002);
        defer dg.os_net.close(c);
        const ab: Path = .{ .from = nf.ep(1, 7000), .to = nf.ep(1, 7001) };
        // Everything lost, then a narrower rule that loses nothing it can
        // see: a burst that alternates, bad on the first datagram, good on
        // the next (enter and leave at 1000000 draw nothing).
        setImpairment(.{}, .{ .loss_ppm = 1_000_000 });
        setImpairment(ab, .{ .burst = .{ .enter_ppm = 1_000_000, .leave_ppm = 1_000_000 } });
        for (0..4) |k| try nf.send(a, 1, 7001, @intCast(k)); // bad, good, bad, good
        for (0..2) |k| try nf.send(a, 1, 7002, @intCast(k)); // the wide rule
        try dg.settle();
        var got: [8]u32 = undefined;
        try std.testing.expectEqual(@as(usize, 2), try nf.drain(b, &got));
        try std.testing.expectEqualSlices(u32, &.{ 1, 3 }, got[0..2]);
        try std.testing.expectEqual(@as(usize, 0), try nf.count(c));
        // After one more datagram the burst is bad; setting the rule again
        // starts it good, so the next datagram is lost (good to bad), not
        // landed (bad to good).
        try nf.send(a, 1, 7001, 4);
        setImpairment(ab, .{ .burst = .{ .enter_ppm = 1_000_000, .leave_ppm = 1_000_000 } });
        try nf.send(a, 1, 7001, 5);
        try dg.settle();
        try std.testing.expectEqual(@as(usize, 0), try nf.count(b));
        // The wide rule set again is the most recent: it acts on a to b too.
        setImpairment(.{}, .{ .loss_ppm = 1_000_000 });
        try nf.send(a, 1, 7001, 6);
        try nf.send(a, 1, 7001, 7);
        try dg.settle();
        try std.testing.expectEqual(@as(usize, 0), try nf.count(b));
        // The wide rule was replaced: its counts are of the last setting,
        // which matched the two datagrams sent since.
        try std.testing.expectEqual(RuleStats{ .matched = 2, .dropped = 2 }, clearImpairment(.{}));
        // The narrow rule matched 2 since it was set again, and dropped 1 of
        // them; the other was dropped by the wide rule.
        try std.testing.expectEqual(RuleStats{ .matched = 3, .dropped = 1 }, clearImpairment(ab));
        try std.testing.expectEqual(@as(u64, 0), netFaultDraws());
    }
};

test "sim: net faults: the most recently set impairment acts, setting a path again resets its burst, and every matching rule counts the datagram" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    try dg.run(1, nf_last_wins.body);
}

test "sim: net faults: a run with no rule, one with rules set and cleared before any send, and one with rules that match nothing draw nothing and give one trace and wire digest" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    const none = try dg_wire.runRules(9, .none);
    try std.testing.expectEqual(@as(u64, 0), netFaultDraws());
    const set_clear = try dg_wire.runRules(9, .set_clear);
    try std.testing.expectEqual(@as(u64, 0), netFaultDraws());
    const unmatched = try dg_wire.runRules(9, .unmatched);
    try std.testing.expectEqual(@as(u64, 0), netFaultDraws());
    try std.testing.expectEqual(none.trace, set_clear.trace);
    try std.testing.expectEqual(none.wire, set_clear.wire);
    try std.testing.expectEqual(none.trace, unmatched.trace);
    try std.testing.expectEqual(none.wire, unmatched.wire);
    // The rules are in the state digest, and only there.
    try std.testing.expect(none.state != set_clear.state);
    try std.testing.expect(none.state != unmatched.state);
}

const nf_records = struct {
    const Variant = enum { none, partition_at_send, silence_at_send, partition_in_flight, silence_in_flight, loss_at_send, burst_at_send };
    var variant: Variant = .none;
    const Out = struct { wire: u64, trace: u64, state: u64 };
    var out: Out = undefined;
    /// The test's own wire digest of what it expects, from the record
    /// format of `recordLanding`'s doc: time, source, destination, outcome
    /// code, length, then the bytes.
    var expect_hasher: std.hash.Wyhash = undefined;

    fn expectRecord(at: u64, k: u32, code: u8) void {
        var h: [25]u8 = undefined;
        std.mem.writeInt(u64, h[0..8], at, .little);
        @memcpy(h[8..12], &[4]u8{ 127, 0, 0, 1 });
        std.mem.writeInt(u16, h[12..14], 7000, .little);
        @memcpy(h[14..18], &[4]u8{ 127, 0, 0, 1 });
        std.mem.writeInt(u16, h[18..20], 7001, .little);
        h[20] = code;
        std.mem.writeInt(u32, h[21..25], 4, .little);
        expect_hasher.update(&h);
        var p: [4]u8 = undefined;
        std.mem.writeInt(u32, &p, k, .little);
        expect_hasher.update(&p);
    }

    fn body() !void {
        expect_hasher = std.hash.Wyhash.init(0);
        const lat = wire_latency_ns;
        const a = try nf.sock(1, 7000);
        defer dg.os_net.close(a);
        const b = try nf.sock(1, 7001);
        defer dg.os_net.close(b);
        const ab: Path = .{ .from = nf.ep(1, 7000), .to = nf.ep(1, 7001) };
        const t0 = clockNs();
        try nf.send(a, 1, 7001, 0);
        try nf.send(a, 1, 7001, 1);
        expectRecord(t0 + lat, 0, 0);
        expectRecord(t0 + lat, 1, 0);
        // Both land before any rule, so only datagram 2 can be dropped.
        try dg.settle();
        const t2 = clockNs();
        switch (variant) {
            .none => {
                try nf.send(a, 1, 7001, 2);
                expectRecord(t2 + lat, 2, 0);
            },
            .partition_at_send => {
                setPartition(ab);
                try nf.send(a, 1, 7001, 2);
                _ = healPartition(ab);
                expectRecord(t2, 2, 5);
            },
            .silence_at_send => {
                silence(nf.ep(1, 7001));
                try nf.send(a, 1, 7001, 2);
                _ = unsilence(nf.ep(1, 7001));
                expectRecord(t2, 2, 6);
            },
            .partition_in_flight => {
                try nf.send(a, 1, 7001, 2);
                setPartition(ab);
                try dg.settle();
                _ = healPartition(ab);
                expectRecord(t2 + lat, 2, 7);
            },
            .silence_in_flight => {
                try nf.send(a, 1, 7001, 2);
                silence(nf.ep(1, 7001));
                try dg.settle();
                _ = unsilence(nf.ep(1, 7001));
                expectRecord(t2 + lat, 2, 8);
            },
            .loss_at_send => {
                setImpairment(ab, .{ .loss_ppm = 1_000_000 });
                try nf.send(a, 1, 7001, 2);
                _ = clearImpairment(ab);
                expectRecord(t2, 2, 3);
            },
            .burst_at_send => {
                setImpairment(ab, .{ .burst = .{ .enter_ppm = 1_000_000, .leave_ppm = 1 } });
                try nf.send(a, 1, 7001, 2);
                _ = clearImpairment(ab);
                expectRecord(t2, 2, 4);
            },
        }
        const t3 = clockNs();
        try nf.send(a, 1, 7001, 3);
        expectRecord(t3 + lat, 3, 0);
        try dg.settle();
        const want: usize = if (variant == .none) 4 else 3;
        try std.testing.expectEqual(want, try nf.count(b));
        try std.testing.expectEqual(expect_hasher.final(), wireDigest());
        out = .{ .wire = wireDigest(), .trace = traceHash(), .state = stateDigest() };
    }

    fn run(v: Variant) !Out {
        variant = v;
        try dg.run(3, body);
        return out;
    }
};

test "sim: net faults: each drop is in the trace and the wire digest with its cause and time, as the test's own digest of the expected records says, and the same seed gives the same digests" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    const vs = comptime std.enums.values(nf_records.Variant);
    var outs: [vs.len]nf_records.Out = undefined;
    for (vs, 0..) |v, i| {
        outs[i] = try nf_records.run(v);
        try std.testing.expectEqual(outs[i], try nf_records.run(v));
    }
    // Every pair of variants differs in the wire digest and in the trace:
    // the cause and the time of the drop are both recorded. (The partition
    // set and healed in flight differs in time from the one at send.)
    for (outs, 0..) |x, i| {
        for (outs[i + 1 ..], i + 1..) |y, j| {
            if (x.wire == y.wire or x.trace == y.trace) {
                std.debug.print("variants {t} and {t} give one wire digest or trace\n", .{ vs[i], vs[j] });
                return error.SameDigest;
            }
        }
    }
}

const nf_matched = struct {
    fn body() !void {
        const a = try nf.sock(1, 7000);
        defer dg.os_net.close(a);
        const b = try nf.sock(1, 7001);
        defer dg.os_net.close(b);
        const ab: Path = .{ .from = nf.ep(1, 7000), .to = nf.ep(1, 7001) };
        // Matched at send, then cleared.
        setImpairment(ab, .{ .loss_ppm = 1_000_000 });
        try nf.send(a, 1, 7001, 0);
        _ = clearImpairment(ab);
        // Set twice: the first time matched nothing, the second time did.
        silence(nf.ep(1, 7001));
        _ = unsilence(nf.ep(1, 7001));
        silence(nf.ep(1, 7001));
        try nf.send(a, 1, 7001, 1);
        // Matched only in flight.
        try nf.send(a, 1, 7002, 2);
        setPartition(.{ .to = nf.ep(1, 7002) });
        try dg.settle();
        requireEveryRuleMatched();
    }
};

test "sim: net faults: requireEveryRuleMatched passes when every rule of the run matched, cleared ones and ones that matched only in flight included" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    try dg.run(1, nf_matched.body);
}

test "sim: net faults refuse: requireEveryRuleMatched with a cleared rule and a rule in force that matched nothing" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: net faults refuse: requireEveryRuleMatched with a cleared rule and a rule in force that matched nothing", "sim: 2 fault rules matched no datagram in this run: impairment from 127.0.0.1:7000 to 127.0.0.1:7999 (first set at 0 ns); partition from *:* to *:7998 (first set at 0 ns)", struct {
        fn body() !void {
            begin(1);
            setImpairment(.{ .from = nf.ep(1, 7000), .to = nf.ep(1, 7999) }, .{ .loss_ppm = 10 });
            _ = clearImpairment(.{ .from = nf.ep(1, 7000), .to = nf.ep(1, 7999) });
            setPartition(.{ .to = .{ .port = 7998 } });
            silence(.{ .port = 7997 });
            const fd = try dg.open();
            defer dg.os_net.close(fd);
            try dg.bindTo(fd, dg.addr(127, 0, 0, 1, 7997));
            _ = try dg.sendFrom(fd, dg.addr(127, 0, 0, 1, 7996), null, "x");
            requireEveryRuleMatched();
        }
    }.body);
}

// Misuse of the fault API panics with a message (the list is in the fault
// protocol above `pub const Endpoint`).
const nf_misuse = struct {
    fn inRun() void {
        begin(1);
    }
};

test "sim: net faults refuse: a rule outside a sim run" {
    try refusal("sim: net faults refuse: a rule outside a sim run", "sim: setPartition outside a sim run", struct {
        fn body() !void {
            setPartition(.{});
        }
    }.body);
}

test "sim: net faults refuse: a rule from another thread" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: net faults refuse: a rule from another thread", "sim: sim.silence" ++ other_thread_msg, struct {
        fn body() !void {
            try r3.up();
            try r3.other(struct {
                fn f() void {
                    silence(.{});
                }
            }.f);
        }
    }.body);
}

test "sim: net faults refuse: loss_ppm over 1000000" {
    try refusal("sim: net faults refuse: loss_ppm over 1000000", "sim: setImpairment: loss_ppm 1000001 is over 1000000", struct {
        fn body() !void {
            nf_misuse.inRun();
            setImpairment(.{}, .{ .loss_ppm = 1_000_001 });
        }
    }.body);
}

test "sim: net faults refuse: burst enter_ppm over 1000000" {
    try refusal("sim: net faults refuse: burst enter_ppm over 1000000", "sim: setImpairment: burst.enter_ppm 2000000 is over 1000000", struct {
        fn body() !void {
            nf_misuse.inRun();
            setImpairment(.{}, .{ .burst = .{ .enter_ppm = 2_000_000, .leave_ppm = 1 } });
        }
    }.body);
}

test "sim: net faults refuse: burst leave_ppm over 1000000" {
    try refusal("sim: net faults refuse: burst leave_ppm over 1000000", "sim: setImpairment: burst.leave_ppm 1000001 is over 1000000", struct {
        fn body() !void {
            nf_misuse.inRun();
            setImpairment(.{}, .{ .burst = .{ .enter_ppm = 1, .leave_ppm = 1_000_001 } });
        }
    }.body);
}

test "sim: net faults refuse: a burst that never starts" {
    try refusal("sim: net faults refuse: a burst that never starts", "sim: setImpairment: a burst with enter_ppm 0 never starts", struct {
        fn body() !void {
            nf_misuse.inRun();
            setImpairment(.{}, .{ .burst = .{ .enter_ppm = 0, .leave_ppm = 5 } });
        }
    }.body);
}

test "sim: net faults refuse: a burst that never ends" {
    try refusal("sim: net faults refuse: a burst that never ends", "sim: setImpairment: a burst with leave_ppm 0 never ends", struct {
        fn body() !void {
            nf_misuse.inRun();
            setImpairment(.{}, .{ .burst = .{ .enter_ppm = 5, .leave_ppm = 0 } });
        }
    }.body);
}

test "sim: net faults refuse: an impairment with every field at its default" {
    try refusal("sim: net faults refuse: an impairment with every field at its default", "sim: setImpairment with every field at its default", struct {
        fn body() !void {
            nf_misuse.inRun();
            setImpairment(.{ .to = .{ .port = 7000 } }, .{});
        }
    }.body);
}

test "sim: net faults refuse: an address outside 127/8" {
    try refusal("sim: net faults refuse: an address outside 127/8", "sim: setPartition: the address 10.0.0.1 is not in 127.0.0.0/8", struct {
        fn body() !void {
            nf_misuse.inRun();
            setPartition(.{ .to = .{ .ip = .{ 10, 0, 0, 1 } } });
        }
    }.body);
}

test "sim: net faults refuse: the wildcard address 0.0.0.0 in an endpoint" {
    try refusal("sim: net faults refuse: the wildcard address 0.0.0.0 in an endpoint", "sim: silence: the address 0.0.0.0 is not in 127.0.0.0/8 (a socket bound to 0.0.0.0 is matched by its port alone)", struct {
        fn body() !void {
            nf_misuse.inRun();
            silence(.{ .ip = .{ 0, 0, 0, 0 }, .port = 7000 });
        }
    }.body);
}

test "sim: net faults refuse: port 0 in an endpoint" {
    try refusal("sim: net faults refuse: port 0 in an endpoint", "sim: setImpairment: port 0 matches no datagram", struct {
        fn body() !void {
            nf_misuse.inRun();
            setImpairment(.{ .from = .{ .port = 0 } }, .{ .loss_ppm = 5 });
        }
    }.body);
}

test "sim: net faults refuse: clearImpairment of a path with none" {
    try refusal("sim: net faults refuse: clearImpairment of a path with none", "sim: clearImpairment of a path that has no impairment", struct {
        fn body() !void {
            nf_misuse.inRun();
            setImpairment(.{ .to = .{ .port = 7000 } }, .{ .loss_ppm = 5 });
            _ = clearImpairment(.{ .to = .{ .port = 7001 } });
        }
    }.body);
}

test "sim: net faults refuse: healPartition of a path that is not partitioned" {
    try refusal("sim: net faults refuse: healPartition of a path that is not partitioned", "sim: healPartition of a path that is not partitioned", struct {
        fn body() !void {
            nf_misuse.inRun();
            const p: Path = .{ .from = .{ .port = 7000 }, .to = .{ .port = 7001 } };
            setPartition(p);
            _ = healPartition(p.reversed());
        }
    }.body);
}

test "sim: net faults refuse: unsilence of an endpoint that is not silenced" {
    try refusal("sim: net faults refuse: unsilence of an endpoint that is not silenced", "sim: unsilence of an endpoint that is not silenced", struct {
        fn body() !void {
            nf_misuse.inRun();
            silence(.{ .port = 7000 });
            _ = unsilence(.{ .ip = .{ 127, 0, 0, 1 }, .port = 7000 });
        }
    }.body);
}

test "sim: net faults refuse: a second partition of one path" {
    try refusal("sim: net faults refuse: a second partition of one path", "sim: setPartition of a path that is already partitioned", struct {
        fn body() !void {
            nf_misuse.inRun();
            setPartition(.{ .to = .{ .port = 7000 } });
            setPartition(.{ .to = .{ .port = 7000 } });
        }
    }.body);
}

test "sim: net faults refuse: a second silence of one endpoint" {
    try refusal("sim: net faults refuse: a second silence of one endpoint", "sim: silence of an endpoint that is already silenced", struct {
        fn body() !void {
            nf_misuse.inRun();
            silence(.{ .port = 7000 });
            silence(.{ .port = 7000 });
        }
    }.body);
}

test "sim: net faults refuse: more rules in force than the table holds" {
    try refusal("sim: net faults refuse: more rules in force than the table holds", "sim: setPartition: 64 fault rules are in force, the most a run holds", struct {
        fn body() !void {
            nf_misuse.inRun();
            var p: u16 = 1;
            while (p <= max_rules + 1) : (p += 1) setPartition(.{ .to = .{ .port = p } });
        }
    }.body);
}

test "sim: net faults refuse: more distinct rules in a run than it records" {
    try refusal("sim: net faults refuse: more distinct rules in a run than it records", "sim: silence: 256 distinct fault rules were set in this run, the most it records", struct {
        fn body() !void {
            nf_misuse.inRun();
            var p: u16 = 1;
            while (p <= max_rule_history + 1) : (p += 1) {
                silence(.{ .port = p });
                _ = unsilence(.{ .port = p });
            }
        }
    }.body);
}

const nf_drop_order = struct {
    fn body() !void {
        const a = try nf.sock(1, 7000);
        defer dg.os_net.close(a);
        const b = try nf.sock(1, 7001);
        defer dg.os_net.close(b);
        const ab: Path = .{ .from = nf.ep(1, 7000), .to = nf.ep(1, 7001) };
        // Two partitions match: the most recent drops and is counted.
        setPartition(.{});
        setPartition(ab);
        try nf.send(a, 1, 7001, 0);
        // A silence set before a partition still acts first.
        _ = healPartition(ab);
        silence(nf.ep(1, 7001));
        setPartition(ab);
        try nf.send(a, 1, 7001, 1);
        try dg.settle();
        const st = netFaultStats();
        try std.testing.expectEqual(@as(u64, 1), st.partitioned_at_send);
        try std.testing.expectEqual(@as(u64, 1), st.silenced_at_send);
        try std.testing.expectEqual(RuleStats{ .matched = 1, .dropped = 0 }, healPartition(ab));
        try std.testing.expectEqual(RuleStats{ .matched = 1, .dropped = 1 }, unsilence(nf.ep(1, 7001)));
        try std.testing.expectEqual(RuleStats{ .matched = 2, .dropped = 0 }, healPartition(.{}));
    }
};

test "sim: net faults: a silence drops before a partition, and of two partitions the most recent drops; every matching rule counts the datagram" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    try dg.run(1, nf_drop_order.body);
}

// ---- Phase 2, slice 2: delay, jitter, the order clamp, reorder, duplication ----

/// A to B under one impairment with delay, jitter and duplication, one
/// datagram every 400 us. The test's model (below) computes every copy's
/// landing time from its own copy of the draw formula and the rules of
/// fault protocol above `pub const Endpoint`, then the order B receives them
/// in, the wire digest,
/// and the counts.
const nf_delay = struct {
    const n: u32 = 300;
    const gap_ns: u64 = 400_000;
    var imp: Impairment = undefined;
    var sent_at: [n]u64 = undefined;
    var got: [2 * n]u32 = undefined;
    var got_len: usize = 0;
    var stats: RuleStats = undefined;
    var wire_d: u64 = undefined;

    fn body() !void {
        const a = try nf.sock(1, 7000);
        defer dg.os_net.close(a);
        const b = try nf.sock(1, 7001);
        defer dg.os_net.close(b);
        setImpairment(.{ .from = nf.ep(1, 7000), .to = nf.ep(1, 7001) }, imp);
        for (0..n) |k| {
            sent_at[k] = clockNs();
            try nf.send(a, 1, 7001, @intCast(k));
            try dg.rt_mod.sleep(.fromNanoseconds(gap_ns));
        }
        try dg.rt_mod.sleep(.fromMilliseconds(10));
        got_len = try nf.drain(b, &got);
        stats = clearImpairment(.{ .from = nf.ep(1, 7000), .to = nf.ep(1, 7001) });
        wire_d = wireDigest();
    }

    const Landing = struct { at: u64, order: usize, k: u32, copy: bool };

    fn check(seed_value: u64, reorder: bool) !void {
        imp = .{ .delay_ns = 2 * std.time.ns_per_ms, .jitter_ns = 3 * std.time.ns_per_ms, .dup_ppm = 250_000, .reorder = reorder };
        try dg.run(seed_value, body);
        nf_model.draws = 0;
        var lands: [2 * n]Landing = undefined;
        var len: usize = 0;
        var dups: u64 = 0;
        var overtaking: u64 = 0;
        var clamped: u64 = 0;
        for (0..n) |k| {
            const t = sent_at[k];
            const copies: usize = if (nf_model.hit(seed_value, imp.dup_ppm, k, 3)) 2 else 1;
            if (copies == 2) dups += 1;
            for (0..copies) |c| {
                // The latest landing of the flow still on the wire at `t`.
                var latest: ?u64 = null;
                for (lands[0..len]) |l| {
                    if (l.at > t) latest = @max(latest orelse l.at, l.at);
                }
                var at = t + wire_latency_ns + imp.delay_ns + nf_model.draw(seed_value, k, 4 + c) % (imp.jitter_ns + 1);
                if (latest) |l| {
                    if (at < l) {
                        if (reorder) overtaking += 1 else {
                            at = l;
                            clamped += 1;
                        }
                    }
                }
                lands[len] = .{ .at = at, .order = len, .k = @intCast(k), .copy = c == 1 };
                len += 1;
            }
        }
        std.mem.sort(Landing, lands[0..len], {}, struct {
            fn lt(_: void, x: Landing, y: Landing) bool {
                return x.at < y.at or (x.at == y.at and x.order < y.order);
            }
        }.lt);
        var h = std.hash.Wyhash.init(0);
        for (lands[0..len], 0..) |l, i| {
            if (i >= got_len or got[i] != l.k) {
                std.debug.print("reorder={}: landing {d}: the model says datagram {d}, the receiver got {d}\n", .{ reorder, i, l.k, if (i < got_len) got[i] else 0 });
                return error.ModelDiffers;
            }
            var r: [25]u8 = undefined;
            std.mem.writeInt(u64, r[0..8], l.at, .little);
            @memcpy(r[8..12], &[4]u8{ 127, 0, 0, 1 });
            std.mem.writeInt(u16, r[12..14], 7000, .little);
            @memcpy(r[14..18], &[4]u8{ 127, 0, 0, 1 });
            std.mem.writeInt(u16, r[18..20], 7001, .little);
            r[20] = if (l.copy) 0x80 else 0;
            std.mem.writeInt(u32, r[21..25], 4, .little);
            h.update(&r);
            var p: [4]u8 = undefined;
            std.mem.writeInt(u32, &p, l.k, .little);
            h.update(&p);
        }
        try std.testing.expectEqual(len, got_len);
        try std.testing.expectEqual(h.final(), wire_d);
        const st = netFaultStats();
        try std.testing.expectEqual(dups, st.duplicated);
        try std.testing.expectEqual(@as(u64, len), st.delayed);
        try std.testing.expectEqual(overtaking, st.overtaking);
        try std.testing.expectEqual(RuleStats{ .matched = n, .dropped = 0, .duplicated = dups, .delayed = len }, stats);
        try std.testing.expectEqual(nf_model.draws, netFaultDraws());
        try nf.noOtherCause(st, &.{ "duplicated", "delayed", "overtaking" });
        // The cases happened: copies, and overtaking (with reorder) or
        // clamped copies (without).
        try std.testing.expect(dups > 30);
        try std.testing.expect(if (reorder) overtaking > 30 else clamped > 30);
        std.debug.print("delay model, reorder={}: {d} sent, {d} copies, {d} overtaking, {d} clamped, {d} draws\n", .{ reorder, n, len, overtaking, clamped, nf_model.draws });
    }
};

test "sim: net faults: delay, jitter and duplication with reorder land every copy when, and in the order, the test's own model says, and the wire digest records each copy" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    try nf_delay.check(21, true);
}

test "sim: net faults: delay, jitter and duplication without reorder keep each flow in order, every copy clamped where the test's own model says" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    try nf_delay.check(21, false);
}

const nf_route = struct {
    fn body() !void {
        const a = try nf.sock(1, 7000);
        defer dg.os_net.close(a);
        const b = try nf.sock(1, 7001);
        defer dg.os_net.close(b);
        const ab: Path = .{ .from = nf.ep(1, 7000), .to = nf.ep(1, 7001) };
        const ms = std.time.ns_per_ms;
        var got: [8]u32 = undefined;
        // A route change to a shorter delay, without reorder: the new
        // datagram waits behind the old one.
        setImpairment(ab, .{ .delay_ns = 10 * ms });
        const t0 = clockNs();
        try nf.send(a, 1, 7001, 0);
        setImpairment(ab, .{ .delay_ns = 1 * ms });
        try nf.send(a, 1, 7001, 1);
        try dg.rt_mod.sleep(.fromNanoseconds(10 * ms + wire_latency_ns - 1));
        try std.testing.expectEqual(@as(usize, 0), try nf.count(b));
        try dg.rt_mod.sleep(.fromNanoseconds(1));
        try std.testing.expectEqual(t0 + 10 * ms + wire_latency_ns, clockNs());
        try std.testing.expectEqual(@as(usize, 2), try nf.drain(b, &got));
        try std.testing.expectEqualSlices(u32, &.{ 0, 1 }, got[0..2]);
        // With reorder, the new datagram overtakes.
        setImpairment(ab, .{ .delay_ns = 10 * ms });
        try nf.send(a, 1, 7001, 2);
        setImpairment(ab, .{ .reorder = true });
        try nf.send(a, 1, 7001, 3);
        try dg.rt_mod.sleep(.fromMilliseconds(11));
        try std.testing.expectEqual(@as(usize, 2), try nf.drain(b, &got));
        try std.testing.expectEqualSlices(u32, &.{ 3, 2 }, got[0..2]);
        // After the rule is cleared, the flow still keeps its order behind
        // a datagram the rule delayed.
        setImpairment(ab, .{ .delay_ns = 10 * ms });
        try nf.send(a, 1, 7001, 4);
        _ = clearImpairment(ab);
        try nf.send(a, 1, 7001, 5);
        try dg.rt_mod.sleep(.fromMilliseconds(11));
        try std.testing.expectEqual(@as(usize, 2), try nf.drain(b, &got));
        try std.testing.expectEqualSlices(u32, &.{ 4, 5 }, got[0..2]);
        const st = netFaultStats();
        // 0, 1 (clamped), 2, 4, 5 (clamped, no rule): five delayed; 3 overtook.
        try std.testing.expectEqual(@as(u64, 5), st.delayed);
        try std.testing.expectEqual(@as(u64, 1), st.overtaking);
        try nf.noOtherCause(st, &.{ "delayed", "overtaking" });
        // Delay alone, reorder alone: nothing to draw.
        try std.testing.expectEqual(@as(u64, 0), netFaultDraws());
    }
};

test "sim: net faults: after a route change to a shorter delay a flow keeps its order unless the new rule says reorder, and keeps it after the rule is cleared; delay alone draws nothing" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    try dg.run(1, nf_route.body);
}

/// A delay moves the landing, and a landing
/// wakes its op at the time it lands, so `pushDueAt`'s check (a datagram op
/// is due now) holds. These run with that check in place.
const nf_wake = struct {
    const delay: u64 = 30 * std.time.ns_per_ms;
    var woke_at: u64 = 0;
    var got_len: usize = 0;
    var err: ?anyerror = null;
    var fd: i32 = 0;

    fn recvTask(rx: i32) void {
        var buf: [16]u8 = undefined;
        if (dg.evRecvFrom(rx, &buf)) |g| {
            got_len = g.len;
            err = null;
        } else |e| err = e;
        woke_at = clockNs();
    }

    fn pollTask(rx: i32) void {
        if (dg.evPoll(rx, .recv)) |_| {
            err = null;
        } else |e| err = e;
        woke_at = clockNs();
    }

    fn setup() !struct { a: i32, b: i32 } {
        const a = try nf.sock(1, 7000);
        const b = try nf.sock(1, 7001);
        setImpairment(.{ .to = nf.ep(1, 7001) }, .{ .delay_ns = delay });
        return .{ .a = a, .b = b };
    }

    fn recvBody() !void {
        const s = try setup();
        defer dg.os_net.close(s.a);
        defer dg.os_net.close(s.b);
        var h = try dg.runtime.spawn(recvTask, .{s.b});
        try dg.rt_mod.sleep(.fromMilliseconds(1));
        const t = clockNs();
        try nf.send(s.a, 1, 7001, 7);
        h.join();
        try std.testing.expectEqual(@as(?anyerror, null), err);
        try std.testing.expectEqual(@as(usize, 4), got_len);
        try std.testing.expectEqual(t + wire_latency_ns + delay, woke_at);
    }

    fn pollBody() !void {
        const s = try setup();
        defer dg.os_net.close(s.a);
        defer dg.os_net.close(s.b);
        var h = try dg.runtime.spawn(pollTask, .{s.b});
        try dg.rt_mod.sleep(.fromMilliseconds(1));
        const t = clockNs();
        try nf.send(s.a, 1, 7001, 7);
        h.join();
        try std.testing.expectEqual(@as(?anyerror, null), err);
        try std.testing.expectEqual(t + wire_latency_ns + delay, woke_at);
        try std.testing.expectEqual(@as(usize, 1), try nf.count(s.b));
    }

    /// The receiver's socket is closed while the datagram is in flight,
    /// and a new socket takes its number and its address: the old receive
    /// ends at the close, and the datagram lands in the new socket, whose
    /// own receive it wakes.
    fn reuseBody() !void {
        const s = try setup();
        defer dg.os_net.close(s.a);
        var h = try dg.runtime.spawn(recvTask, .{s.b});
        try dg.rt_mod.sleep(.fromMilliseconds(1));
        const t = clockNs();
        try nf.send(s.a, 1, 7001, 7);
        dg.evClose(s.b);
        h.join();
        try std.testing.expectEqual(@as(?anyerror, error.FileDescriptorNotASocket), err);
        try std.testing.expect(woke_at < t + delay);
        const b2 = try nf.sock(1, 7001);
        defer dg.os_net.close(b2);
        try std.testing.expectEqual(s.b, b2);
        got_len = 0;
        var h2 = try dg.runtime.spawn(recvTask, .{b2});
        h2.join();
        try std.testing.expectEqual(@as(?anyerror, null), err);
        try std.testing.expectEqual(@as(usize, 4), got_len);
        try std.testing.expectEqual(t + wire_latency_ns + delay, woke_at);
    }
};

test "sim: net faults: an ev receive parked before a delayed datagram is sent wakes at its delayed landing, with its bytes" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    try dg.run(1, nf_wake.recvBody);
}

test "sim: net faults: an ev poll parked before a delayed datagram is sent wakes at its delayed landing" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    try dg.run(1, nf_wake.pollBody);
}

test "sim: net faults: a receive whose socket is closed and its number reused while a delayed datagram is in flight ends at the close, and the datagram wakes the new socket's receive" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    try dg.run(1, nf_wake.reuseBody);
}

test "sim: net faults refuse: dup_ppm over 1000000" {
    try refusal("sim: net faults refuse: dup_ppm over 1000000", "sim: setImpairment: dup_ppm 1000001 is over 1000000", struct {
        fn body() !void {
            nf_misuse.inRun();
            setImpairment(.{}, .{ .dup_ppm = 1_000_001 });
        }
    }.body);
}

test "sim: net faults refuse: a delay over the limit" {
    try refusal("sim: net faults refuse: a delay over the limit", "sim: setImpairment: delay_ns 60000000001 plus jitter_ns 0 is over 60000000000 ns", struct {
        fn body() !void {
            nf_misuse.inRun();
            setImpairment(.{}, .{ .delay_ns = max_fault_delay_ns + 1 });
        }
    }.body);
}

test "sim: net faults refuse: a delay and a jitter each under the limit whose sum is over it" {
    try refusal("sim: net faults refuse: a delay and a jitter each under the limit whose sum is over it", "sim: setImpairment: delay_ns 40000000000 plus jitter_ns 30000000000 is over 60000000000 ns", struct {
        fn body() !void {
            nf_misuse.inRun();
            setImpairment(.{}, .{ .delay_ns = 40 * std.time.ns_per_s, .jitter_ns = 30 * std.time.ns_per_s });
        }
    }.body);
}

// ---- Phase 2, slice 4: faulted runs replay (design section 7, promise 3) ----

/// Three senders to one wildcard receiver under every kind of fault, set
/// and changed by a controller task at virtual times: an impairment with
/// loss, burst, delay, jitter, reorder and duplication on everything to
/// port 7000; a partition of sender 2 for 2 ms; a silence of sender 0 from
/// 3 ms on. An ev receive takes the first datagram to land; the receiver
/// writes its own transcript of the rest: the source port's low byte, the
/// length, and the bytes.
const dg_faulted = struct {
    const Variant = enum { base, partition_earlier, delay_one_ns_more };
    var variant: Variant = .base;
    var transcript: [2048]u8 = undefined;
    var transcript_len: usize = 0;
    const Out = struct { wire: u64, trace: u64, state: u64, stats: NetFaultStats };
    var out: Out = undefined;
    var senders: [3]i32 = undefined;

    fn body() !void {
        const rx = try dg.open();
        defer dg.os_net.close(rx);
        try dg.bindTo(rx, dg.addr(0, 0, 0, 0, 7000));
        try dg.os_net.enableDestinationAddress(rx);
        for (&senders, 0..) |*fd, i| fd.* = try nf.sock(1, @intCast(7100 + i));
        defer for (senders) |fd| dg.os_net.close(fd);
        transcript_len = 0;
        const delay: u64 = std.time.ns_per_ms + @as(u64, @intFromBool(variant == .delay_one_ns_more));
        setImpairment(.{ .to = .{ .port = 7000 } }, .{
            .loss_ppm = 150_000,
            .burst = .{ .enter_ppm = 80_000, .leave_ppm = 400_000 },
            .delay_ns = delay,
            .jitter_ns = 2 * std.time.ns_per_ms,
            .reorder = true,
            .dup_ppm = 300_000,
        });
        var first = try dg.runtime.spawn(evFirst, .{rx});
        var ctl = try dg.runtime.spawn(controller, .{});
        for (0..40) |k| {
            for (senders, 0..) |fd, i| {
                var p: [5]u8 = .{ 'f', @intCast('0' + i), @intCast(k), 'x', 'y' };
                _ = try dg.sendFrom(fd, dg.addr(127, 0, 0, @intCast(1 + i), 7000), null, p[0 .. 3 + k % 3]);
            }
            try dg.rt_mod.sleep(.fromNanoseconds(150_000));
        }
        try ctl.join();
        try dg.rt_mod.sleep(.fromMilliseconds(10));
        try first.join();
        var buf: [16]u8 = undefined;
        while (dg.recvInto(rx, &.{&buf})) |got| {
            note(@truncate(got.from.port), buf[0..got.info.len]);
        } else |err| if (err != error.WouldBlock) return err;
        _ = unsilence(nf.ep(1, 7100));
        _ = clearImpairment(.{ .to = .{ .port = 7000 } });
        out = .{ .wire = wireDigest(), .trace = traceHash(), .state = stateDigest(), .stats = netFaultStats() };
    }

    fn controller() !void {
        const start: u64 = if (variant == .partition_earlier) 600_000 else 1_500_000;
        try dg.rt_mod.sleep(.fromNanoseconds(start));
        const p2: Path = .{ .from = nf.ep(1, 7102) };
        setPartition(p2);
        try dg.rt_mod.sleep(.fromNanoseconds(3_000_000 - start));
        silence(nf.ep(1, 7100));
        try dg.rt_mod.sleep(.fromNanoseconds(500_000));
        _ = healPartition(p2);
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

    fn run(seed_value: u64, v: Variant) !Out {
        variant = v;
        try dg.run(seed_value, body);
        return out;
    }
};

test "sim: net faults: a faulted run repeats in one process, and one changed rule or another seed gives another wire digest, trace and transcript" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    const base = try dg_faulted.run(9, .base);
    var base_t: [2048]u8 = undefined;
    const bt = base_t[0..dg_faulted.transcript_len];
    @memcpy(bt, dg_faulted.transcript[0..dg_faulted.transcript_len]);
    // Every kind of fault acted in it.
    const st = base.stats;
    inline for (.{ "lost", "lost_burst", "partitioned_at_send", "silenced_at_send", "duplicated", "delayed", "overtaking" }) |name| {
        if (@field(st, name) == 0) {
            std.debug.print("{s} is 0 in the faulted workload\n", .{name});
            return error.FaultNeverActed;
        }
    }
    try std.testing.expectEqual(base, try dg_faulted.run(9, .base));
    try std.testing.expectEqualSlices(u8, bt, dg_faulted.transcript[0..dg_faulted.transcript_len]);
    for ([_]struct { u64, dg_faulted.Variant }{ .{ 9, .partition_earlier }, .{ 10, .base } }) |c| {
        const o = try dg_faulted.run(c[0], c[1]);
        if (o.wire == base.wire or o.trace == base.trace or std.mem.eql(u8, bt, dg_faulted.transcript[0..dg_faulted.transcript_len])) {
            std.debug.print("seed {d}, {t}: the same wire digest, trace or transcript as the base run\n", .{ c[0], c[1] });
            return error.SameDigest;
        }
    }
    // One more nanosecond of delay moves every landing and changes nothing
    // a receiver sees: the same transcript and counts. Only the wire
    // digest, which hashes each landing's time, can tell the runs apart.
    const later = try dg_faulted.run(9, .delay_one_ns_more);
    try std.testing.expectEqualSlices(u8, bt, dg_faulted.transcript[0..dg_faulted.transcript_len]);
    try std.testing.expectEqual(base.stats, later.stats);
    if (later.wire == base.wire) {
        std.debug.print("one more nanosecond of delay: the same wire digest\n", .{});
        return error.SameDigest;
    }
    std.debug.print("faulted workload: transcript {d} bytes, stats {any}\n", .{ bt.len, st });
}

test "sim: net faults: ten fresh processes with the heap moved give one transcript, wire digest, trace, state digest and fault counts for a faulted run" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    const name = "sim: net faults: ten fresh processes with the heap moved give one transcript, wire digest, trace, state digest and fault counts for a faulted run";
    if (isDeathChild()) {
        noCoreDump();
        const shift = std.fmt.parseInt(usize, std.mem.span(std.c.getenv("ZIO_SIM_HEAP_SHIFT") orelse "0"), 10) catch 0;
        if (shift > 0) _ = try std.heap.page_allocator.alloc(u8, shift * 4096 + 7);
        const o = try dg_faulted.run(9, .base);
        var line_buf: [8192]u8 = undefined;
        var w: std.Io.Writer = .fixed(&line_buf);
        try w.print("sim-faulted: wire={x:0>16} trace={x:0>16} state={x:0>16}", .{ o.wire, o.trace, o.state });
        inline for (comptime std.meta.fieldNames(NetFaultStats)) |f| try w.print(" {s}={d}", .{ f, @field(o.stats, f) });
        try w.writeAll(" transcript=");
        for (dg_faulted.transcript[0..dg_faulted.transcript_len]) |b| try w.print("{x:0>2}", .{b});
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
        const at = std.mem.indexOf(u8, res.stderr, "sim-faulted: ") orelse return error.NoLine;
        const end_at = std.mem.indexOfScalarPos(u8, res.stderr, at, '\n') orelse return error.NoLine;
        const got = res.stderr[at..end_at];
        if (first) |f| {
            if (!std.mem.eql(u8, f, got)) {
                std.debug.print("child {d}: {s}\nchild 1: {s}\n", .{ k, got, f });
                return error.ChildrenDiffer;
            }
        } else first = try gpa.dupe(u8, got);
    }
    // The transcript is not empty: datagrams landed in it.
    const t_at = (std.mem.indexOf(u8, first.?, "transcript=") orelse return error.NoTranscript) + "transcript=".len;
    try std.testing.expect(first.?.len - t_at > 100);
    std.debug.print("10 children: {s}\n", .{first.?[0..@min(first.?.len, 400)]});
}

// ---- Buffer counts up to IOV_MAX (t-4101) ----

/// IOV_MAX on macOS 25.6 and Linux 6.8 (measured, sitting 21). The tests
/// state it themselves rather than read `max_iovecs`, so a sim limit set
/// lower fails them by its refusal, not by their own array bounds.
const kernel_iov_max = 1024;

/// A datagram or pipe write gathered from `n` buffers of 1 to 3 bytes each,
/// and receive lists of `n` buffers of the same sizes. Static, so a test of
/// `kernel_iov_max + 1` buffers costs a task no stack.
const many_iov = struct {
    const os_net = @import("os/net.zig");
    var src: [3 * (kernel_iov_max + 1)]u8 = undefined;
    var siov: [kernel_iov_max + 1]os_net.iovec_const = undefined;
    var dst: [3 * (kernel_iov_max + 1)]u8 = undefined;
    var riov: [kernel_iov_max + 1]os_net.iovec = undefined;
    var whole: [3 * (kernel_iov_max + 1)]u8 = undefined;
    var whole_iov: [1]os_net.iovec = undefined;

    fn partLen(i: usize) usize {
        return 1 + i % 3;
    }

    /// The bytes are a count from 0 (mod 256), so order and loss both show.
    fn gather(n: usize) []const os_net.iovec_const {
        var at: usize = 0;
        for (0..n) |i| {
            for (0..partLen(i)) |j| src[at + j] = @truncate(at + j);
            siov[i] = os_net.iovecConstFromSlice(src[at..][0..partLen(i)]);
            at += partLen(i);
        }
        return siov[0..n];
    }

    fn total(n: usize) usize {
        var t: usize = 0;
        for (0..n) |i| t += partLen(i);
        return t;
    }

    fn scatter(n: usize) []os_net.iovec {
        @memset(&dst, 0xEE);
        var at: usize = 0;
        for (0..n) |i| {
            riov[i] = os_net.iovecFromSlice(dst[at..][0..partLen(i)]);
            at += partLen(i);
        }
        return riov[0..n];
    }

    fn one() []os_net.iovec {
        @memset(&whole, 0xEE);
        whole_iov[0] = os_net.iovecFromSlice(&whole);
        return &whole_iov;
    }

    fn check(got: []const u8, n: usize) !void {
        try std.testing.expectEqual(total(n), got.len);
        for (got, 0..) |b, k| {
            if (b != @as(u8, @truncate(k))) {
                std.debug.print("{d} buffers: byte {d} is {d}, expected {d}\n", .{ n, k, b, @as(u8, @truncate(k)) });
                return error.WrongByte;
            }
        }
    }

    fn sendmsgTo(fd: i32, to: *const os_net.sockaddr.in, n: usize) !usize {
        return os_net.sendmsg(fd, gather(n), .{}, @ptrCast(to), @sizeOf(os_net.sockaddr.in), null);
    }

    fn evSendTo(fd: i32, to: *const os_net.sockaddr.in, n: usize) !usize {
        var op = dg.ev.NetSendTo.init(fd, .{ .iovecs = gather(n) }, .{}, @ptrCast(to), @sizeOf(os_net.sockaddr.in));
        try dg.io_common.waitForIo(&op.c);
        return op.getResult();
    }

    fn evRecvFrom(fd: i32, bufs: []os_net.iovec) !usize {
        var from: os_net.sockaddr.in = undefined;
        var from_len: os_net.socklen_t = @sizeOf(os_net.sockaddr.in);
        var op = dg.ev.NetRecvFrom.init(fd, .{ .iovecs = bufs }, .{}, @ptrCast(&from), &from_len);
        try dg.io_common.waitForIo(&op.c);
        return op.getResult();
    }

    fn dgramBody() !void {
        const rx = try nf.sock(1, 7000);
        defer dg.os_net.close(rx);
        const tx = try nf.sock(1, 7001);
        defer dg.os_net.close(tx);
        const to = dg.addr(127, 0, 0, 1, 7000);
        for ([_]usize{ 65, kernel_iov_max }) |n| {
            // sendDatagram, read whole by the layer.
            try std.testing.expectEqual(total(n), try os_net.sendDatagram(tx, gather(n), .{}, &to, null));
            try dg.settle();
            const a = try os_net.recvDatagram(rx, one(), .{ .dontwait = true }, null);
            try check(whole[0..a.len], n);
            // sendmsg, scattered over n buffers by the layer.
            try std.testing.expectEqual(total(n), try sendmsgTo(tx, &to, n));
            try dg.settle();
            const b = try os_net.recvDatagram(rx, scatter(n), .{ .dontwait = true }, null);
            try check(dst[0..b.len], n);
            // The ev send, scattered over n buffers by the ev receive.
            try std.testing.expectEqual(total(n), try evSendTo(tx, &to, n));
            try check(dst[0..try evRecvFrom(rx, scatter(n))], n);
        }
    }

    fn pipeBody() !void {
        const fds = pipePair();
        for ([_]usize{ 65, kernel_iov_max }) |n| {
            var s = dg.ev.NetSend.init(fds[1], .{ .iovecs = gather(n) }, .{});
            try dg.io_common.waitForIo(&s.c);
            try std.testing.expectEqual(total(n), try s.getResult());
            var r = dg.ev.NetRecv.init(fds[0], .{ .iovecs = scatter(n) }, .{});
            try dg.io_common.waitForIo(&r.c);
            try check(dst[0..try r.getResult()], n);
        }
    }
};

test "sim: dgram iovecs: 65 and 1024 buffers through sendDatagram, sendmsg and the ev send arrive whole and in order, read into one buffer and scattered over as many by the layer and the ev receive" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    try dg.run(1, many_iov.dgramBody);
}

test "sim: pipe iovecs: an ev send of 65 and of 1024 buffers on a sim pipe arrives in order, read by an ev receive of as many buffers" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    try dg.run(1, many_iov.pipeBody);
}

const iov_over = kernel_iov_max + 1;

test "sim: dgram refuses: a sendDatagram of IOV_MAX + 1 buffers" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: dgram refuses: a sendDatagram of IOV_MAX + 1 buffers", "sim: a send of 1025 buffers on sim fd ", struct {
        fn body() !void {
            try dg.run(1, struct {
                fn b() !void {
                    const tx = try nf.sock(1, 7001);
                    defer dg.os_net.close(tx);
                    const to = dg.addr(127, 0, 0, 1, 7000);
                    _ = try many_iov.os_net.sendDatagram(tx, many_iov.gather(iov_over), .{}, &to, null);
                }
            }.b);
        }
    }.body);
}

test "sim: dgram refuses: a sendmsg of IOV_MAX + 1 buffers" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: dgram refuses: a sendmsg of IOV_MAX + 1 buffers", "sim: a send of 1025 buffers on sim fd ", struct {
        fn body() !void {
            try dg.run(1, struct {
                fn b() !void {
                    const tx = try nf.sock(1, 7001);
                    defer dg.os_net.close(tx);
                    const to = dg.addr(127, 0, 0, 1, 7000);
                    _ = try many_iov.sendmsgTo(tx, &to, iov_over);
                }
            }.b);
        }
    }.body);
}

test "sim: dgram refuses: an ev send of IOV_MAX + 1 buffers" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: dgram refuses: an ev send of IOV_MAX + 1 buffers", "sim: a send of 1025 buffers on sim fd ", struct {
        fn body() !void {
            try dg.run(1, struct {
                fn b() !void {
                    const tx = try nf.sock(1, 7001);
                    defer dg.os_net.close(tx);
                    const to = dg.addr(127, 0, 0, 1, 7000);
                    _ = try many_iov.evSendTo(tx, &to, iov_over);
                }
            }.b);
        }
    }.body);
}

test "sim: dgram refuses: a receive of IOV_MAX + 1 buffers" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: dgram refuses: a receive of IOV_MAX + 1 buffers", "sim: a receive of 1025 buffers on sim fd ", struct {
        fn body() !void {
            try dg.run(1, struct {
                fn b() !void {
                    const rx = try nf.sock(1, 7000);
                    defer dg.os_net.close(rx);
                    _ = many_iov.os_net.recvDatagram(rx, many_iov.scatter(iov_over), .{ .dontwait = true }, null) catch {};
                }
            }.b);
        }
    }.body);
}

test "sim: pipe refuses: an ev send of IOV_MAX + 1 buffers" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: pipe refuses: an ev send of IOV_MAX + 1 buffers", "sim: a send of 1025 buffers on sim fd ", struct {
        fn body() !void {
            try dg.run(1, struct {
                fn b() !void {
                    const fds = pipePair();
                    var s = dg.ev.NetSend.init(fds[1], .{ .iovecs = many_iov.gather(iov_over) }, .{});
                    try dg.io_common.waitForIo(&s.c);
                }
            }.b);
        }
    }.body);
}

test "sim: pipe refuses: an ev receive of IOV_MAX + 1 buffers" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    try refusal("sim: pipe refuses: an ev receive of IOV_MAX + 1 buffers", "sim: a receive of 1025 buffers on sim fd ", struct {
        fn body() !void {
            try dg.run(1, struct {
                fn b() !void {
                    const fds = pipePair();
                    var r = dg.ev.NetRecv.init(fds[0], .{ .iovecs = many_iov.scatter(iov_over) }, .{});
                    try dg.io_common.waitForIo(&r.c);
                }
            }.b);
        }
    }.body);
}

// ---- Round 5 of the outside review (gpt-simnet7.md, of 3340b0f) ----

/// Finding 1: a send that fails for memory must leave no fault decision
/// behind. The rule duplicates everything, delays it, and has a burst that
/// alternates (bad, good, ...) with no draw, so a decision that leaked into
/// the burst state shows in the next send's fate.
const r5_alloc = struct {
    var failing: std.testing.FailingAllocator = undefined;

    fn failingSend(a: i32, k: u32, fail_index: usize) !void {
        failing = std.testing.FailingAllocator.init(std.heap.smp_allocator, .{ .fail_index = fail_index });
        testSetNetAllocator(failing.allocator());
        defer testSetNetAllocator(std.heap.smp_allocator);
        try std.testing.expectError(error.SystemResources, nf.send(a, 1, 7001, k));
    }

    fn body() !void {
        const a = try nf.sock(1, 7000);
        defer dg.os_net.close(a);
        const b = try nf.sock(1, 7001);
        defer dg.os_net.close(b);
        const ab: Path = .{ .from = nf.ep(1, 7000), .to = nf.ep(1, 7001) };
        // The review's sequence: everything duplicated and delayed. A fresh
        // run's wire has no capacity, so a send allocates its payload copy
        // (0), the duplicate's copy (1), and the wire's growth (2).
        setImpairment(ab, .{ .dup_ppm = 1_000_000, .delay_ns = std.time.ns_per_ms });
        for ([_]usize{ 0, 1, 2 }) |fi| {
            try failingSend(a, 0, fi);
            try std.testing.expectEqual(NetFaultStats{}, netFaultStats());
            try std.testing.expectEqual(@as(u64, 0), netFaultDraws());
            try std.testing.expectEqual(@as(u64, 0), datagramStats(a).sent);
        }
        try std.testing.expectEqual(RuleStats{}, clearImpairment(ab));
        // The burst: the first send that succeeds goes good to bad, lost.
        setImpairment(ab, .{ .dup_ppm = 1_000_000, .delay_ns = std.time.ns_per_ms, .burst = .{ .enter_ppm = 1_000_000, .leave_ppm = 1_000_000 } });
        try nf.send(a, 1, 7001, 0);
        try std.testing.expectEqual(@as(u64, 1), netFaultStats().lost_burst);
        // The next send leaves the bad state; its duplicate's copy fails.
        // Its retry must still be that send: bad to good, two copies.
        try failingSend(a, 1, 1);
        try std.testing.expectEqual(NetFaultStats{ .lost_burst = 1 }, netFaultStats());
        try nf.send(a, 1, 7001, 1);
        try dg.rt_mod.sleep(.fromMilliseconds(2));
        try std.testing.expectEqual(@as(usize, 2), try nf.count(b));
        try std.testing.expectEqual(NetFaultStats{ .lost_burst = 1, .duplicated = 1, .delayed = 2 }, netFaultStats());
        try std.testing.expectEqual(@as(u64, 2), datagramStats(a).sent);
        try std.testing.expectEqual(RuleStats{ .matched = 2, .dropped = 1, .duplicated = 1, .delayed = 2 }, clearImpairment(ab));
    }
};

test "sim: net faults: a send whose payload copy, duplicate copy or wire growth fails commits no fault decision, and its retry gets the fate a first try would" {
    if (comptime !compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    try dg.run(1, r5_alloc.body);
}
