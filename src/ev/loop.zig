const std = @import("std");
const builtin = @import("builtin");
const Backend = @import("backend.zig").Backend;
const Completion = @import("completion.zig").Completion;
const single_owner = @import("completion.zig").single_owner;
const Group = @import("completion.zig").Group;
const Timer = @import("completion.zig").Timer;
const Async = @import("completion.zig").Async;
const Duration = @import("../time.zig").Duration;
const Timestamp = @import("../time.zig").Timestamp;
const Timeout = @import("../time.zig").Timeout;
const Clock = @import("../time.zig").Clock;
const compat = @import("../compat.zig");
const Queue = @import("queue.zig").Queue;
const Heap = @import("heap.zig").Heap;
const Work = @import("completion.zig").Work;
const DelegatedWork = @import("completion.zig").DelegatedWork;
const FileRead = @import("completion.zig").FileRead;
const NetSend = @import("completion.zig").NetSend;
const NetRecv = @import("completion.zig").NetRecv;
const NetClose = @import("completion.zig").NetClose;
const NetShutdown = @import("completion.zig").NetShutdown;
const NetSendTo = @import("completion.zig").NetSendTo;
const NetRecvFrom = @import("completion.zig").NetRecvFrom;
const NetSendMsg = @import("completion.zig").NetSendMsg;
const NetRecvMsg = @import("completion.zig").NetRecvMsg;
const NetPoll = @import("completion.zig").NetPoll;
const WallTimerMode = @import("completion.zig").WallTimerMode;
const NetSendFile = @import("completion.zig").NetSendFile;
const ReadBuf = @import("buf.zig").ReadBuf;
const WriteBuf = @import("buf.zig").WriteBuf;
const os = @import("../os/root.zig");
const ThreadPool = @import("thread_pool.zig").ThreadPool;
const time = @import("../os/time.zig");
const net = @import("../os/net.zig");
const common = @import("backends/common.zig");

const log = @import("../common.zig").log;
const zio_options = @import("../options.zig").options;
const sim = @import("../sim.zig");

const in_safe_mode = compat.is_safe;
const in_debug_mode = compat.is_debug;

/// The loop bound to the current thread (debug builds only), used by
/// `assertOwnThread`. Set by `Loop.init`, cleared by `Loop.deinit`.
threadlocal var current_loop: if (in_debug_mode) ?*Loop else void =
    if (in_debug_mode) null else {};

/// How the NetSendFile fallback lays out its scratch from the (up to two)
/// caller-provided buffers.
const SendfileStrategy = enum {
    /// Ping-pong between the two given buffers (read into one, send the other).
    both,
    /// Halve the larger buffer into two ping-pong buffers (the other is unused).
    split,
    /// Single buffer, serial read-then-send.
    single,
};

/// The larger buffer must be at least this big before halving it for overlap
/// beats just running serially over the whole thing.
const sendfile_min_split = 64 * 1024;

/// Pick a strategy from the two buffers' sizes (see `SendfileStrategy`).
fn chooseSendfileStrategy(len0: usize, len1: usize) SendfileStrategy {
    const small = @min(len0, len1);
    const large = @max(len0, len1);
    // Two buffers ping-ponging beats halving one whenever both are usable, so
    // prefer that. Only fall back to halving the larger when the smaller is too
    // small to be a useful second buffer (less than half the larger) *and* the
    // larger is big enough that two halves still overlap well. An empty smaller
    // buffer (only one buffer available) also lands here.
    if (large >= sendfile_min_split and small < large / 2) return .split;
    if (small > 0) return .both;
    return .single;
}

/// Resolve the two caller buffers into the two working ping-pong buffers. A
/// non-empty bufs[1] means double-buffering; an empty bufs[1] means serial.
fn sendfileLayout(bufs: [2][]u8) [2][]u8 {
    const big = if (bufs[0].len >= bufs[1].len) bufs[0] else bufs[1];
    return switch (chooseSendfileStrategy(bufs[0].len, bufs[1].len)) {
        .both => bufs,
        .split => blk: {
            const half = big.len / 2;
            if (half == 0) break :blk .{ big, &.{} };
            break :blk .{ big[0..half], big[half..] };
        },
        .single => .{ big, &.{} },
    };
}

test chooseSendfileStrategy {
    const S = SendfileStrategy;
    const expectEqual = std.testing.expectEqual;
    const k = 1024;

    // Balanced (incl. equal) pairs -> use both, regardless of size.
    try expectEqual(S.both, chooseSendfileStrategy(64 * k, 64 * k));
    try expectEqual(S.both, chooseSendfileStrategy(1 * k, 1 * k));
    try expectEqual(S.both, chooseSendfileStrategy(64 * k, 48 * k));

    // Small second buffer, but the larger isn't big enough to bother splitting.
    try expectEqual(S.both, chooseSendfileStrategy(32 * k, 1 * k));
    try expectEqual(S.both, chooseSendfileStrategy(1 * k, 32 * k)); // order-independent

    // Large buffer + a much smaller one -> halve the large into balanced halves.
    try expectEqual(S.split, chooseSendfileStrategy(256 * k, 1 * k));
    try expectEqual(S.split, chooseSendfileStrategy(1 * k, 256 * k));

    // Only one buffer (other empty): split if big enough, else serial.
    try expectEqual(S.split, chooseSendfileStrategy(256 * k, 0));
    try expectEqual(S.single, chooseSendfileStrategy(32 * k, 0));
    try expectEqual(S.single, chooseSendfileStrategy(0, 0));
}

test sendfileLayout {
    const expectEqual = std.testing.expectEqual;
    const k = 1024;
    var a: [64 * k]u8 = undefined;
    var b: [64 * k]u8 = undefined;

    // Two balanced buffers -> returned as-is (double-buffer).
    {
        const r = sendfileLayout(.{ a[0 .. 32 * k], b[0 .. 32 * k] });
        try expectEqual(32 * k, r[0].len);
        try expectEqual(32 * k, r[1].len);
        try std.testing.expect(r[0].ptr == a[0..].ptr);
    }
    // One large buffer -> two halves of it.
    {
        const r = sendfileLayout(.{ a[0..], &.{} });
        try expectEqual(32 * k, r[0].len);
        try expectEqual(32 * k, r[1].len);
        try std.testing.expect(r[0].ptr == a[0..].ptr);
        try std.testing.expect(r[1].ptr == a[32 * k ..].ptr);
    }
    // One small buffer -> serial (empty second slot).
    {
        var c: [4 * k]u8 = undefined;
        const r = sendfileLayout(.{ c[0..], &.{} });
        try expectEqual(4 * k, r[0].len);
        try expectEqual(0, r[1].len);
    }
}

pub const LoopGroup = struct {
    shared: Backend.SharedState = .{},
    /// Loops initialized with this group and not yet deinitialized.
    loops: std.atomic.Value(u32) = .init(0),
};

fn timerDeadlineLess(_: void, a: *Timer, b: *Timer) bool {
    return a.deadline.value < b.deadline.value;
}

const TimerHeap = Heap(Timer, void, timerDeadlineLess);

/// Timers are kept in one heap per wall-clock domain, since their deadlines
/// live in different epochs and can only be compared within a clock. The
/// CPU-time clocks are not valid for timers. `Clock` numbers the wall clocks
/// 0..wall_clock_count contiguously, so the enum value is the heap index.
const wall_clock_count = 3;

/// How often `tick` re-sends `SIGURG` to a worker still blocked in a canceled
/// syscall (see `LoopState.cancel_resend`). One signal almost always suffices;
/// this bounds how long a lost first signal delays the interruption.
const resend_interval: Duration = .fromMilliseconds(1);

fn clockIndex(clock: Clock) usize {
    // Where the platform has no distinct suspend-inclusive clock, `.boot` and
    // `.awake` are the same clock, so boot timers share the awake heap (index 0)
    // and are driven by the uncapped awake poll timeout instead of a separate
    // capped/native path.
    const c: Clock = if (clock == .boot and !time.boot_distinct_from_awake) .awake else clock;
    const idx = @backingInt(c);
    if (idx >= wall_clock_count) @panic("timers cannot use CPU-time clocks");
    return idx;
}

fn indexClock(index: usize) Clock {
    return @fromBackingInt(@intCast(index));
}

pub fn SimpleStack(comptime T: type) type {
    return struct {
        head: ?*T = null,

        pub fn push(self: *@This(), value: *T) void {
            value.next = self.head;
            self.head = value;
        }

        pub fn pop(self: *@This()) ?*T {
            const head = self.head orelse return null;
            self.head = head.next;
            head.next = null;
            return head;
        }

        pub fn empty(self: *const @This()) bool {
            return self.head == null;
        }
    };
}

pub fn AtomicStack(comptime T: type) type {
    return struct {
        head: std.atomic.Value(?*T) = .init(null),

        pub fn push(self: *@This(), value: *T) void {
            var head = self.head.load(.acquire);
            while (true) {
                value.next = head;
                if (self.head.cmpxchgWeak(head, value, .acq_rel, .acquire)) |prev_value| {
                    head = prev_value;
                    continue;
                }
                break;
            }
        }

        pub fn popAll(self: *@This()) SimpleStack(T) {
            const head = self.head.swap(null, .acq_rel);
            return .{ .head = head };
        }

        pub fn empty(self: *const @This()) bool {
            return self.head.load(.acquire) == null;
        }
    };
}

pub const LoopState = struct {
    loop: *Loop,

    initialized: bool = false,
    running: bool = false,
    stopped: bool = false,

    /// Thread-pool jobs this loop submitted whose completions it has not yet
    /// drained from `work_completions`. Owner thread only.
    pool_inflight: usize = 0,

    /// Wake requests (`wake_loop`, `wake_async`, `wake_cancel`) plus the
    /// `sleeping` bit the loop sets before a blocking backend poll. Cleared
    /// as a whole after every poll. See `requestWake`.
    wake_requested: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),

    /// Cached "now" per wall clock, indexed by `clockIndex`. `awake` is
    /// refreshed eagerly at the start of each scan (`updateNow`); `boot`/`real` are
    /// refreshed lazily on first use within a scan and cached for the rest of
    /// it. `tick` is a monotonically increasing scan counter; `now_tick[i]`
    /// records the scan that `now[i]` was last filled, so a mismatch refreshes.
    tick: u64 = 0,
    now: [wall_clock_count]Timestamp = .{ .zero, .zero, .zero },
    now_tick: [wall_clock_count]u64 = .{ 0, 0, 0 },
    timers: [wall_clock_count]TimerHeap = .{
        .{ .context = {} },
        .{ .context = {} },
        .{ .context = {} },
    },

    /// Intrusive list of thread-pool-delegated works whose cancellation has been
    /// requested but whose worker is still blocked in the canceled syscall. Each
    /// `tick` re-sends `SIGURG` to cover a first signal lost in the tiny window
    /// between `Syscall.begin()` and the kernel entering the sleep. Touched only
    /// on this loop's thread (added in `cancelLocal`, swept in `tick`, removed as
    /// the completion is finalized in the `work_completions` drain).
    cancel_resend: ?*Work = null,
    // TODO: Linked timers optimization
    // Instead of mutex-protected cross-thread timer cancellation, link timers to their
    // associated operations. When an operation completes, its linked timer is cleared
    // on the same thread (no mutex). When a timer fires, its linked operation is
    // cancelled on the same thread. This eliminates cross-thread synchronization for
    // the common timeout pattern:
    //   - Add `linked_timer: ?*Timer` to Completion
    //   - Add `linked_completion: ?*Completion` to Timer
    //   - On operation complete: clear linked timer (same thread, direct)
    //   - On timer fire: cancel linked operation (same thread, direct)
    // The cross-thread cancel mechanism remains for general cancellation (task migration,
    // external cancellation), but timeouts become zero-overhead pointer unlinking.
    /// Protects all timer heaps and the cached `now` values. A single lock is
    /// enough: it's local to the loop and effectively uncontended, only ever
    /// held for O(log n) heap ops with callbacks run outside it.
    timer_mutex: os.Mutex = .init(),

    async_handles: Queue(Completion) = .{},

    completions: Queue(Completion) = .{},
    /// Finished standalone completions awaiting user-callback dispatch, when the
    /// loop was created with `do_not_call_callbacks`. Drained by
    /// `Loop.nextDispatched`.
    dispatched: Queue(Completion) = .{},
    work_completions: AtomicStack(Completion) = .{},

    pub const wake_loop: u32 = 1;
    pub const wake_async: u32 = 2;
    pub const wake_cancel: u32 = 4;
    /// Set by the loop just before a blocking backend poll. The top bit,
    /// apart from the request bits below it.
    pub const sleeping: u32 = 1 << 31;
    const wake_mask: u32 = wake_loop | wake_async | wake_cancel;

    /// Record a wake request. Returns true if the caller must also wake the
    /// backend: the loop is blocked in (or about to enter) its poll and no
    /// other request has woken it yet. A loop that isn't sleeping picks the
    /// request up at its next poll without any syscall. Both sides do a single
    /// read-modify-write on this one word, so they are totally ordered: either
    /// the loop's `sleeping` comes first and the requester sees it, or the
    /// request comes first and the loop sees it and doesn't block.
    pub fn requestWake(self: *LoopState, bit: u32) bool {
        sim.refuseOtherThread("a loop wake");
        return self.wake_requested.fetchOr(bit, .acq_rel) == sleeping;
    }

    /// Called by backends when an operation they accepted completes. Tells the
    /// backend to drop its inflight count (the backend of the loop running the
    /// completion, whose storage covers the op on every backend) and marks the
    /// completion done.
    pub fn markCompletedFromBackend(self: *LoopState, completion: *Completion) void {
        self.loop.backend.decrInflight();
        self.markCompleted(completion);
    }

    pub fn markCompleted(self: *LoopState, completion: *Completion) void {
        std.debug.assert(completion.has_result);
        const old = completion.enterCompleted();
        // With a cancel pass in flight, that pass owns the dispatch.
        if (!old.cancel_inflight) {
            self.dispatchCompletion(completion);
        }
    }

    /// Finish now, or queue for processCompletions when the completion uses
    /// deferred finishing (rearm implies it).
    pub fn dispatchCompletion(self: *LoopState, completion: *Completion) void {
        if (completion.flags.defer_callback or completion.flags.rearm) {
            self.completions.push(completion);
        } else {
            self.finishCompletion(completion);
        }
    }

    pub fn finishCompletion(self: *LoopState, completion: *Completion) void {
        completion.enterDead();

        // Both callbacks below can free `completion`, so whichever may free it must
        // run LAST, with nothing touching `completion` afterward. Cache the owner
        // callback now, before `call` — for a standalone completion `call` wakes a
        // waiter that may free `completion`, so we must not read it afterward.
        // (Rearm handles are exempt from the freeing contract by definition.)
        const owner_callback = completion.group.owner_callback;
        const was_rearm = completion.flags.rearm;

        // Deferred dispatch: a standalone (non-rearm, no owner) completion is
        // queued for the driver to call itself via `nextDispatched`, rather than
        // invoked here. `completion` was just popped from `completions`, so its
        // queue link is free to reuse. Rearm/group completions fall through and
        // run inline, since their machinery must stay synchronous.
        //
        // A null callback opts a single completion into the same delivery:
        // it is handed out through `nextDispatched` instead of being invoked.
        // Task wakes travel this way (see Executor.drainDispatched).
        if ((self.loop.do_not_call_callbacks or completion.callback == null) and !was_rearm and owner_callback == null) {
            self.dispatched.push(completion);
            return;
        }

        // The completion's own callback runs first: for a group member it reports
        // the member's own result while the member is still alive; for a standalone
        // completion (no owner) it is the last thing we do.
        completion.call(self.loop);

        // Re-add persistent handles. Re-reading the flag lets a callback stop
        // its own handle; the cached value guards it, since only rearm
        // completions are guaranteed still alive here.
        if (was_rearm and completion.flags.rearm) {
            self.loop.add(completion);
        }

        // Then notify the group/queue owner. This can drive the group to completion
        // and free the frame this member lives on (e.g. the last `.gather` member
        // completing the group, whose callback frees the member), so it must run
        // last — `completion` may be dangling after this returns.
        if (owner_callback) |cb| {
            cb(self.loop, completion);
        }
    }

    /// Advance the scan counter and refresh the awake snapshot. Bumping `tick`
    /// invalidates the lazily-cached boot/real values for the new scan.
    /// Owner-thread only, so no timer lock: the cross-thread `clearTimer`
    /// never reads `now`/`tick`.
    pub fn updateNow(self: *LoopState) void {
        self.tick +%= 1;
        self.now[0] = time.now(.monotonic);
        self.now_tick[0] = self.tick;
    }

    /// Current time on the given clock, indexed by `clockIndex`. `awake` is a
    /// cache hit (primed eagerly by `updateNow`); `boot`/`real` are read fresh
    /// at most once per scan (cheap VDSO `clock_gettime`) and cached. Reading
    /// them per scan rather than per iteration is what lets the poll cap bound
    /// oversleep across suspend/steps.
    fn nowFor(self: *LoopState, clock: Clock) Timestamp {
        const idx = clockIndex(clock);
        if (self.now_tick[idx] != self.tick) {
            self.now[idx] = time.now(clock);
            self.now_tick[idx] = self.tick;
        }
        return self.now[idx];
    }

    pub fn lockTimers(self: *LoopState) void {
        self.timer_mutex.lock();
    }

    pub fn unlockTimers(self: *LoopState) void {
        self.timer_mutex.unlock();
    }

    /// Compute the deadline from `timer.timeout` and insert into the heap.
    /// The timer must not be in a heap.
    pub fn armTimer(self: *LoopState, timer: *Timer) void {
        switch (timer.timeout) {
            .none => timer.deadline = .{ .value = std.math.maxInt(time.TimeInt) },
            .duration => |d| {
                var now = self.nowFor(timer.clock);
                if (comptime zio_options.sim) {
                    if (sim.mutantArmTimerStale()) {
                        now = .fromNanoseconds(sim.epochNs(@backingInt(timer.clock)));
                    }
                }
                timer.deadline = now.addDuration(d);
                // Sim invariant (#711 class): a duration timer's deadline is
                // never behind the true logical now plus the duration. The
                // cached snapshot may lag only if a refresh was skipped.
                if (comptime zio_options.sim) {
                    if (d.value > 0) {
                        const dl = timer.deadline.toNanoseconds();
                        const true_now = sim.nowNsFor(@backingInt(timer.clock));
                        const expected = true_now +| d.toNanoseconds();
                        if (dl +| 1 < expected) {
                            sim.panic("armTimer backdated: deadline behind true now + duration", .{});
                        }
                    }
                }
            },
            .deadline => |ts| timer.deadline = ts,
        }
        if (Timer.can_extend) {
            // A deadline too far out to tell from `closed` cannot be extended.
            const open = timer.deadline.value & Timer.closed == 0;
            timer.extended.store(if (open) timer.deadline.value else Timer.closed, .monotonic);
        }
        self.timers[clockIndex(timer.clock)].insert(timer);
    }

    /// Remove from the heap. The timer must be in it (armed and not mid-fire).
    pub fn disarmTimer(self: *LoopState, timer: *Timer) void {
        self.timers[clockIndex(timer.clock)].remove(timer);
        timer.deadline = .zero;
        if (Timer.can_extend) timer.extended.store(Timer.closed, .monotonic);
    }

    /// For a timer whose `deadline` has passed: when its extended deadline
    /// has not, move it there in the heap and return true. Otherwise close
    /// it, committing it to fire; an `extend` racing this either lands first
    /// and is honored, or finds it closed.
    fn postponeTimer(self: *LoopState, timer: *Timer, now: Timestamp) bool {
        if (!Timer.can_extend) return false;
        var w = timer.extended.load(.monotonic);
        while (true) {
            if (w & Timer.closed == 0 and w > now.value) {
                const heap = &self.timers[clockIndex(timer.clock)];
                heap.remove(timer);
                timer.deadline = .{ .value = w };
                heap.insert(timer);
                return true;
            }
            w = timer.extended.cmpxchgWeak(w, Timer.closed, .monotonic, .monotonic) orelse return false;
        }
    }

    /// Add a canceled-but-blocked work to the cancel-resend list (idempotent).
    /// Loop-thread only. Only works whose worker is blocked-and-canceling belong
    /// here. `key` is the public completion whose finalization drops the entry
    /// (== `&work.c` for a plain `.work` op, or the owning op's completion for a
    /// delegated file op).
    fn addResend(self: *LoopState, work: *Work, key: *Completion) void {
        if (work.resend_key != null) return;
        work.resend_key = key;
        work.resend_next = self.cancel_resend;
        self.cancel_resend = work;
    }

    /// Remove the work owning `completion` from the resend list, if it is on it.
    /// Called as the completion is finalized (before its waiter is signaled) so
    /// the sweep never dereferences a token whose op is about to be freed. The
    /// list is empty in the common case, so this is O(1) then. Loop-thread only.
    fn removeResendByCompletion(self: *LoopState, completion: *Completion) void {
        if (self.cancel_resend == null) return;
        var slot = &self.cancel_resend;
        while (slot.*) |node| {
            if (node.resend_key == completion) {
                slot.* = node.resend_next;
                node.resend_next = null;
                node.resend_key = null;
                return;
            }
            slot = &node.resend_next;
        }
    }

    /// Re-send `SIGURG` to every still-blocked canceling worker, dropping any
    /// that have acknowledged. Called once per `tick`. Loop-thread only.
    fn sweepResend(self: *LoopState) void {
        var slot = &self.cancel_resend;
        while (slot.*) |node| {
            if (node.cancel_token.?.signal()) {
                // Still blocked-and-canceling; keep it and re-check next tick.
                slot = &node.resend_next;
            } else {
                // Worker acknowledged (or the op finished); stop re-sending.
                slot.* = node.resend_next;
                node.resend_next = null;
                node.resend_key = null;
                // Fire the release hook (drops the blocking task's keep-alive
                // ref) now that the entry is unlinked. Cleared so it runs once.
                if (node.resend_release) |release| {
                    node.resend_release = null;
                    release(node);
                }
            }
        }
    }
};

pub const Loop = struct {
    state: LoopState,
    backend: Backend,

    allocator: std.mem.Allocator,
    thread_pool: ?*ThreadPool = null,

    loop_group: *LoopGroup,
    internal_loop_group: LoopGroup = .{},

    max_wait: Duration = .fromSeconds(60),
    /// Upper bound on the poll wait while a boot/real timer is pending on a
    /// backend without native wall-clock timers. The poll clock (`awake`)
    /// can't track suspend or wall-clock steps, so we re-evaluate boot/real
    /// deadlines at least this often; this bounds how late such a timer can
    /// fire after a suspend/step. Unused once a backend arms them natively.
    wall_clock_cap: Duration = .fromSeconds(10),

    /// Cross-thread cancel queue (lock-free MPSC)
    cancel_queue: std.atomic.Value(?*Completion) = std.atomic.Value(?*Completion).init(null),

    in_add: if (in_safe_mode) bool else void = if (in_safe_mode) false else {},

    /// When true, `tick` does not dispatch finished standalone completions to
    /// their callbacks; it queues them instead, and the driver drains them with
    /// `nextDispatched`, invoking the callbacks itself. This lets an embedder
    /// (e.g. a CPython asyncio event loop) run the blocking poll with the GIL
    /// released and then invoke callbacks with the GIL held. Rearm handles and
    /// group-owner callbacks are unaffected (they still run inline).
    do_not_call_callbacks: bool = false,

    /// Sim: the one timer here that wakes no task, the stack pool's shrink
    /// timer (`Executor.init`). It re-arms forever, so a deadlock check that
    /// counted it would never fire: the clock would run ahead in its steps
    /// instead (t-4085). `simHasTaskTimer` leaves it out.
    sim_idle_timer: if (zio_options.sim) ?*Timer else void = if (zio_options.sim) null else {},
    /// Sim: this is its runtime's only loop, so `poll` decides a deadlock for
    /// the whole runtime. With two logical executors `Executor.simMuxStep`
    /// decides, because one loop's timers say nothing about the other's.
    sim_solo: if (zio_options.sim) bool else void = if (zio_options.sim) false else {},

    const default_queue_size = 256;

    pub const Options = struct {
        allocator: std.mem.Allocator = std.heap.page_allocator,
        thread_pool: ?*ThreadPool = null,
        loop_group: ?*LoopGroup = null,
        queue_size: u16 = default_queue_size,
        do_not_call_callbacks: bool = false,
    };

    pub fn init(self: *Loop, options: Options) !void {
        self.* = .{
            .state = .{ .loop = self },
            .backend = undefined,
            .allocator = options.allocator,
            .thread_pool = options.thread_pool,
            .loop_group = undefined,
            .do_not_call_callbacks = options.do_not_call_callbacks,
        };

        if (options.loop_group) |group| {
            self.loop_group = group;
        } else {
            self.loop_group = &self.internal_loop_group;
        }

        if (options.queue_size == 0) {
            return error.InvalidQueueSize;
        }

        net.ensureWSAInitialized();
        self.state.updateNow();

        // Grouped loops service and complete each other's operations, which
        // the plain state transitions of `single_owner` do not allow.
        if (self.loop_group.loops.fetchAdd(1, .monotonic) != 0 and single_owner) {
            @panic("zio: .single_executor scheduling allows only one loop per LoopGroup");
        }
        errdefer _ = self.loop_group.loops.fetchSub(1, .monotonic);

        // Sim opens no kernel backend: sim I/O is completed by this loop
        // (`submitSimIo`, `harvestSimIo`) and the backend is never reached.
        // Every backend's `poll` panics first thing under sim (the D4 trip).
        if (comptime zio_options.sim) {
            self.backend = undefined;
        } else {
            try self.backend.init(
                options.allocator,
                options.queue_size,
                &self.loop_group.shared,
            );
        }
        errdefer if (comptime !zio_options.sim) self.backend.deinit();

        self.state.initialized = true;

        if (in_debug_mode) current_loop = self;
    }

    pub fn deinit(self: *Loop) void {
        self.assertOwnThread();
        if (in_debug_mode) current_loop = null;
        if (comptime !zio_options.sim) self.backend.deinit();
        _ = self.loop_group.loops.fetchSub(1, .monotonic);
    }

    /// Debug-only: assert we're on the thread that owns this loop.
    inline fn assertOwnThread(self: *const Loop) void {
        if (in_debug_mode) std.debug.assert(current_loop == self);
    }

    /// Sim multiplex: this thread runs more than one logical loop. Bind
    /// before any add/poll/cancel on this loop.
    pub fn bindThread(self: *Loop) void {
        if (in_debug_mode) current_loop = self;
    }

    /// Sim: whether a timer other than `sim_idle_timer` is armed. In a
    /// pairing heap the root has no siblings, so a root with no child is the
    /// heap's only element.
    pub fn simHasTaskTimer(self: *Loop) bool {
        if (comptime !zio_options.sim) unreachable;
        self.state.lockTimers();
        defer self.state.unlockTimers();
        for (&self.state.timers) |*heap| {
            const root = heap.peek() orelse continue;
            if (root == self.sim_idle_timer and root.heap.child == null) continue;
            return true;
        }
        return false;
    }

    /// Sim: nothing waits to be processed here: no wake request (a
    /// cross-loop async notify or cancel lands as a bit), no finished or pool
    /// completion, no dispatched callback, no queued cross-loop cancel. A
    /// `poll(.zero)` of a loop that is not quiescent can make a task ready, so
    /// `Executor.simMuxStep` must not call the run a deadlock while any loop
    /// is not quiescent.
    pub fn simQuiescent(self: *Loop) bool {
        if (comptime !zio_options.sim) unreachable;
        return self.state.wake_requested.load(.acquire) & LoopState.wake_mask == 0 and
            self.state.completions.empty() and
            self.state.work_completions.empty() and
            self.state.dispatched.empty() and
            self.cancel_queue.load(.acquire) == null;
    }

    /// Sim: earliest pending timer remaining, or null. Does not fire.
    pub fn peekNextTimeout(self: *Loop) ?Duration {
        var next: ?Duration = null;
        for (0..wall_clock_count) |idx| {
            if (self.state.timers[idx].isEmpty()) continue;
            self.state.lockTimers();
            defer self.state.unlockTimers();
            const timer = self.state.timers[idx].peek() orelse continue;
            const clock = indexClock(idx);
            const now_clock = self.state.nowFor(clock);
            if (timer.deadline.value <= now_clock.value) {
                return .zero;
            }
            const remaining = now_clock.durationTo(timer.deadline);
            if (next == null or remaining.value < next.?.value) next = remaining;
        }
        return next;
    }

    /// Operations still out with the backend. Under sim there is no backend:
    /// the equivalent is a simulated I/O operation not yet completed.
    fn hasInflight(self: *const Loop) bool {
        if (comptime zio_options.sim) return sim.hasLiveIo();
        return self.backend.hasInflight();
    }

    /// Under sim no loop ever sleeps in a kernel poll, so `requestWake`
    /// cannot report a sleeping loop. Reaching a backend wake there means
    /// the sleeping bit leaked past `poll`.
    fn backendWake(self: *Loop) void {
        if (comptime zio_options.sim) sim.protocolPanic("sim: backend.wake requested for a sleeping loop");
        self.backend.wake(&self.state);
    }

    pub fn stop(self: *Loop) void {
        self.state.stopped = true;
    }

    /// Pop the next finished standalone completion awaiting user-callback
    /// dispatch: every finished completion when the loop was created with
    /// `do_not_call_callbacks`, and completions with a null callback always.
    /// Returns null when drained; the caller invokes `completion.call(loop)`
    /// or interprets the completion itself.
    pub fn nextDispatched(self: *Loop) ?*Completion {
        return self.state.dispatched.pop();
    }

    /// Take `completion` out of the dispatched queue if it is the last one
    /// there, as a null-callback completion that finished inside `add` is.
    /// Returns whether it was.
    pub fn takeLastDispatched(self: *Loop, completion: *Completion) bool {
        if (self.state.dispatched.tail != completion) return false;
        return self.state.dispatched.remove(completion);
    }

    pub fn stopped(self: *const Loop) bool {
        return self.state.stopped;
    }

    /// Whether this loop has nothing left to do: no backend I/O in flight, no
    /// armed timer, no registered async handle, no thread-pool job outstanding,
    /// and no finished completion waiting for its callback. On backends whose
    /// in-flight count is shared by a loop group (epoll, kqueue, IOCP), I/O
    /// submitted by any loop of the group keeps every loop of the group busy.
    /// Completions handed out via `nextDispatched` are already finished and do
    /// not keep the loop running.
    ///
    /// Only meaningful between polls (`run` and the guard in `poll`): while
    /// callbacks run, completions detached into a local batch by
    /// `processCompletions` or `checkTimers` are not visible here.
    fn done(self: *const Loop) bool {
        if (self.state.stopped) return true;
        if (self.hasInflight()) return false;
        for (&self.state.timers) |*heap| {
            if (!heap.isEmpty()) return false;
        }
        return self.state.async_handles.empty() and
            self.state.pool_inflight == 0 and
            self.state.completions.empty();
    }

    /// Get the current monotonic timestamp
    pub fn now(self: *const Loop) Timestamp {
        return self.state.now[0];
    }

    /// Wake up the loop from another thread (thread-safe). Costs a syscall
    /// only when the loop is sleeping in its poll and nobody has woken it yet.
    pub fn wake(self: *Loop) void {
        if (self.state.requestWake(LoopState.wake_loop)) self.backendWake();
    }

    /// Wake up the loop to process async handles (thread-safe)
    pub fn wakeAsync(self: *Loop) void {
        if (self.state.requestWake(LoopState.wake_async)) self.backendWake();
    }

    /// Set or reset a timer with a new timeout (works immediately, no completion required)
    pub fn setTimer(self: *Loop, timer: *Timer, timeout: Timeout) void {
        sim.refuseOtherThread("Loop.setTimer");
        self.state.lockTimers();
        defer self.state.unlockTimers();
        const st = timer.c.loadState();
        // A running timer sits in its owning loop's heap; re-arming it here
        // would remove it from this loop's heap instead. Clear it on the owning
        // loop first.
        std.debug.assert(st.phase != .running or timer.c.getLoop() == self);
        // A running timer with a result set is mid-fire (out of the heap, its
        // markCompleted pending outside this lock); rearm from the callback
        // (or after it), never concurrently with the fire.
        std.debug.assert(!(st.phase == .running and timer.c.has_result));
        // Advance the scan so this timer's deadline is computed against a fresh
        // `now` in its own clock (via `nowFor` in `armTimer`).
        self.state.updateNow();
        timer.timeout = timeout;
        if (st.phase == .running) {
            self.state.disarmTimer(timer);
        } else {
            timer.c.has_result = false;
            timer.c.err = null;
            timer.c.setLoop(self);
            _ = timer.c.enterRunning();
        }
        self.state.armTimer(timer);
    }

    /// Move an armed timer to a new timeout, if it is still armed on this
    /// loop and neither fired nor claimed by a cancel pass: its callback has
    /// not run and will not run for the old deadline. Returns false, leaving
    /// it untouched, otherwise; `clearTimer` and `setTimer` then do the same
    /// in two steps.
    pub fn rearmTimer(self: *Loop, timer: *Timer, timeout: Timeout) bool {
        sim.refuseOtherThread("Loop.rearmTimer");
        self.state.lockTimers();
        defer self.state.unlockTimers();
        // First, so this lock is the one that covers the fields read below.
        if (timer.c.getLoop() != self) return false;
        const st = timer.c.loadState();
        if (st.phase != .running or st.cancel_inflight or timer.c.has_result) return false;
        self.state.updateNow();
        timer.timeout = timeout;
        self.state.disarmTimer(timer);
        self.state.armTimer(timer);
        return true;
    }

    /// Clear a timer without completing it (works immediately, no cancellation
    /// completion required). Thread-safe: may be called from a thread that does
    /// not own the loop (a migrated task clearing its sleep timer).
    ///
    /// Returns true when the timer is the caller's again: it was disarmed here
    /// (or was never armed), and its callback will not run. Returns false when
    /// the timer is already on its way to completion, which means its callback
    /// has run or is still to run, and both the timer and whatever its
    /// `userdata` points at must stay alive until it does.
    pub fn clearTimer(self: *Loop, timer: *Timer) bool {
        sim.refuseOtherThread("Loop.clearTimer");
        self.state.lockTimers();
        defer self.state.unlockTimers();
        const st = timer.c.loadState();
        // Not armed: `.new` is ours to hand back, anything else is a fired
        // incarnation whose callback ran or is queued to run.
        if (st.phase != .running) return st.phase == .new;
        // A running timer that already has its result is in the fired/canceled
        // limbo window: checkTimers (or cancelLocal) removed it from the heap
        // and set its result under this lock, but its markCompleted runs after
        // unlocking. It is already on its way to completion; leave it be.
        if (timer.c.has_result) return false;
        // A cancel pass claimed it (possibly on another loop's thread, still
        // sitting in this loop's cancel queue); it owns the finish dispatch.
        if (!timer.c.tryDisarm()) return false;
        self.state.disarmTimer(timer);
        return true;
    }

    /// Cancel a completion directly without requiring a Cancel completion struct.
    /// This is a fire-and-forget, idempotent operation - the completion's callback will still be
    /// invoked when the operation completes (either with error.Canceled or its natural result).
    /// Must be called on this loop's own thread. The completion may be owned by a
    /// different loop; cross-loop cancels are routed through its cancel queue.
    pub fn cancel(self: *Loop, completion: *Completion) void {
        self.assertOwnThread();
        if (single_owner) {
            if (completion.getLoop()) |owner| {
                if (owner != self) @panic("zio: .single_executor scheduling does not allow canceling another loop's completion");
            }
        }

        const old = completion.requestCancel();
        // Nothing to route: already requested, too late, or not yet submitted
        // (enterRunning picks the latched request up).
        if (old.cancel_requested or old.phase != .running) return;

        // The CAS observed `.running`, so the loop published by enterRunning
        // is visible.
        const target = completion.getLoop().?;

        if (self == target) {
            // Same loop - cancel directly
            self.cancelLocal(completion);
        } else {
            // Push to target's cancel queue (lock-free Treiber stack)
            var head = target.cancel_queue.load(.acquire);
            while (true) {
                completion.cancel_next = head;
                head = target.cancel_queue.cmpxchgWeak(head, completion, .release, .acquire) orelse break;
            }

            if (target.state.requestWake(LoopState.wake_cancel)) target.backendWake();
        }
    }

    /// Cancel a completion on the local loop (must be called from the loop's thread)
    fn cancelLocal(self: *Loop, completion: *Completion) void {
        defer {
            const old = completion.finishCancelPass();
            if (old.phase == .completed) {
                self.state.dispatchCompletion(completion);
            }
        }

        // Completed while queued, or disarmed in the meantime (timers)
        if (completion.loadState().phase != .running) {
            return;
        }

        switch (completion.op) {
            .group => {
                const group = completion.cast(Group);
                var node = group.head;
                while (node) |n| {
                    const next = n.next;
                    const c: *Completion = @fieldParentPtr("group", n);
                    self.cancel(c);
                    node = next;
                }
            },
            .timer => {
                const timer = completion.cast(Timer);
                self.state.lockTimers();
                // Re-check under the lock: a cross-thread clearTimer may have
                // disarmed it, or the fire may have won (mid-fire limbo:
                // running with a result set, out of the heap already).
                if (timer.c.loadState().phase != .running or timer.c.has_result) {
                    self.state.unlockTimers();
                    return;
                }
                // Disarm first, closing the timer to `extend`, which does not
                // take this lock. Set the result under the lock: clearTimer
                // keys "already fired/canceled, hands off" on it.
                self.state.disarmTimer(timer);
                timer.c.setError(error.Canceled);
                self.state.unlockTimers();
                self.state.markCompleted(&timer.c);
            },
            .async => {
                const async_handle = completion.cast(Async);
                async_handle.c.setError(error.Canceled);
                _ = self.state.async_handles.remove(&async_handle.c);
                self.state.markCompleted(&async_handle.c);
            },
            .work => {
                const thread_pool = self.thread_pool orelse unreachable;
                const work = completion.cast(Work);
                thread_pool.cancel(work);
                // If the worker is blocked in the canceled syscall, the first
                // SIGURG (sent by cancel above) can be lost in the begin()->sleep
                // window. Track it so `tick` re-sends until the worker acks; the
                // entry is dropped when this completion finalizes.
                if (work.cancel_token) |token| {
                    if (token.isCanceling()) self.state.addResend(work, completion);
                }
            },
            .net_send_file => {
                if (comptime zio_options.sim) sim.panic("sim: unsimulated I/O op net_send_file", .{});
                const op = completion.cast(NetSendFile);
                switch (comptime Backend.capability(.net_send_file)) {
                    .yes => self.backend.cancel(&self.state, completion),
                    .no => self.netSendFileCancel(op),
                    .maybe => switch (op.route) {
                        .none => unreachable,
                        .backend => self.backend.cancel(&self.state, completion),
                        .fallback => self.netSendFileCancel(op),
                    },
                }
            },

            inline else => |op| {
                if (comptime zio_options.sim) {
                    self.cancelSimIo(completion);
                    return;
                }
                const op_data = completion.cast(op.toType());
                switch (comptime Backend.capability(op)) {
                    .yes => self.backend.cancel(&self.state, completion),
                    .no => self.cancelLinkedWork(completion, &op_data.linked_work),
                    .maybe => switch (op_data.route) {
                        .none => unreachable,
                        .backend => self.backend.cancel(&self.state, completion),
                        .fallback => self.cancelLinkedWork(completion, &op_data.linked_work),
                    },
                }
            },
        }
    }

    fn cancelLinkedWork(self: *Loop, completion: *Completion, linked_work: *DelegatedWork) void {
        const thread_pool = self.thread_pool orelse unreachable;
        thread_pool.cancel(&linked_work.work);
        if (linked_work.token.isCanceling()) {
            self.state.addResend(&linked_work.work, completion);
        }
    }

    /// Cancel a thread-pool `work` that was submitted directly to the pool (not
    /// through this loop), e.g. a blocking task. Loop-thread only.
    pub fn cancelWork(self: *Loop, work: *Work) void {
        const thread_pool = self.thread_pool orelse {
            if (work.resend_release) |release| {
                work.resend_release = null;
                release(work);
            }
            return;
        };
        thread_pool.cancel(work);
        if (work.cancel_token) |token| {
            if (token.isCanceling()) {
                self.state.addResend(work, &work.c);
                return;
            }
        }
        if (work.resend_release) |release| {
            work.resend_release = null;
            release(work);
        }
    }

    pub fn run(self: *Loop) !void {
        std.debug.assert(self.state.initialized);
        while (!self.done()) {
            try self.poll(.max);
        }
    }

    pub fn add(self: *Loop, completion: *Completion) void {
        self.assertOwnThread();
        if (in_safe_mode) {
            if (self.in_add) {
                @panic("recursive call to Loop.add() is not allowed");
            }
            self.in_add = true;
        }
        defer {
            if (in_safe_mode) self.in_add = false;
        }
        self.addInternal(completion);
    }

    fn addInternal(self: *Loop, completion: *Completion) void {
        completion.setLoop(self);
        const old = completion.enterRunning();
        if (old.phase == .dead) {
            completion.reset();
        }

        if (old.cancel_requested) {
            // Groups cannot be canceled before submission
            if (completion.op == .group) {
                @panic("cannot cancel a group before adding it to the loop");
            }
            completion.setError(error.Canceled);
            self.state.markCompleted(completion);
            return;
        }

        switch (completion.op) {
            .group => {
                const group = completion.cast(Group);

                if (group.remaining.load(.acquire) == 0) {
                    // Empty group - complete immediately
                    group.c.setResult(.group, {});
                    self.state.markCompleted(&group.c);
                } else {
                    // Add all children to the loop
                    var node = group.head;
                    while (node) |n| {
                        const next = n.next;
                        const c: *Completion = @fieldParentPtr("group", n);
                        self.addInternal(c);
                        node = next;
                    }
                }
                return;
            },
            .timer => {
                const timer = completion.cast(Timer);
                self.state.lockTimers();
                self.state.armTimer(timer);
                self.state.unlockTimers();
                return;
            },
            .async => {
                const async = completion.cast(Async);

                // Check if already notified before submission
                if (checkAndSetAsyncResult(async)) {
                    // Already pending - complete immediately
                    self.state.markCompleted(&async.c);
                } else {
                    // Not pending - add to queue to wait for notification
                    self.state.async_handles.push(&async.c);
                }
                return;
            },
            .work => {
                const work = completion.cast(Work);
                work.completion_fn = loopWorkComplete;
                work.completion_context = @ptrCast(self);
                if (self.thread_pool) |thread_pool| {
                    self.state.pool_inflight += 1;
                    thread_pool.submit(work);
                } else {
                    work.state.store(.completed, .release);
                    work.c.setError(error.NoThreadPool);
                    self.state.markCompleted(&work.c);
                }
                return;
            },
            .net_send_file => {
                if (comptime zio_options.sim) sim.panic("sim: unsimulated I/O op net_send_file", .{});
                const op = completion.cast(NetSendFile);
                switch (comptime Backend.capability(.net_send_file)) {
                    .yes => self.backend.submit(&self.state, .net_send_file, completion),
                    .no => netSendFileStart(self, op),
                    .maybe => {
                        if (self.backend.supports(.net_send_file, op)) {
                            op.route = .backend;
                            self.backend.submit(&self.state, .net_send_file, completion);
                        } else {
                            op.route = .fallback;
                            netSendFileStart(self, op);
                        }
                    },
                }
                return;
            },
            else => {
                if (comptime zio_options.sim) {
                    self.submitSimIo(completion);
                    return;
                }
                switch (completion.op) {
                    inline else => |op| {
                        const op_data = completion.cast(op.toType());
                        switch (comptime Backend.capability(op)) {
                            .yes => self.backend.submit(&self.state, op, completion),
                            .no => self.submitFileOpToThreadPool(completion),
                            .maybe => if (self.backend.supports(op, op_data)) {
                                op_data.route = .backend;
                                self.backend.submit(&self.state, op, completion);
                            } else {
                                op_data.route = .fallback;
                                self.submitFileOpToThreadPool(completion);
                            },
                        }
                    },
                }
                return;
            },
        }
    }

    const TimerCheckResult = struct {
        next_timeout: ?Duration,
        fired: bool,
    };

    /// Fire every timer whose deadline has passed and report the earliest one
    /// still pending. Scans against the current snapshot; `poll` owns the tick.
    fn checkTimers(self: *Loop) TimerCheckResult {
        // Per wall clock: whether the backend arms the deadline itself, and
        // whether the loop still has to re-examine it on the cap anyway.
        // Sim arms no kernel wall timer: every clock is folded into the
        // poll timeout, which is the logical clock's only advance.
        const modes: [wall_clock_count]WallTimerMode = if (zio_options.sim) @splat(.fallback) else Backend.wall_timer_modes;

        var fired = false;
        var next_timeout: ?Duration = null;
        // For native boot/real clocks: the earliest pending absolute deadline to
        // hand the backend, plus its capped remaining (computed during the scan
        // under the lock) to fold into the poll timeout if the backend can't arm.
        var wall_deadline: [wall_clock_count]?u64 = .{ null, null, null };
        var wall_remaining: [wall_clock_count]Duration = .{ .zero, .zero, .zero };

        // Each wall-clock domain has its own heap, compared against `now` in
        // that clock. The earliest remaining across all domains becomes the
        // poll timeout; `poll` caps it at `max_wait`, which bounds how
        // far a boot/real timer can oversleep after a suspend or clock step
        // (the re-read of `now(clock)` on the next scan corrects it).
        for (0..wall_clock_count) |idx| {
            const clock = indexClock(idx);

            // Lock-free fast path: an empty heap has nothing to fire and no
            // deadline to contribute, so skip the timer mutex entirely. A null
            // read is always real (see Heap.isEmpty); a stale non-null just falls
            // through to the locked drain below and re-checks.
            if (self.state.timers[idx].isEmpty()) continue;

            // Process fired timers in batches to avoid holding the lock during
            // callbacks. This prevents deadlock when callbacks set/clear timers.
            while (true) {
                var batch: [4]*Timer = undefined;
                var batch_count: usize = 0;

                self.state.lockTimers();
                // `nowFor` is read inside the loop, so an empty heap reads no
                // clock at all; for boot/real the first read fills the cache.
                while (self.state.timers[idx].peek()) |timer| {
                    const now_clock = self.state.nowFor(clock);
                    if (timer.deadline.value > now_clock.value) {
                        var remaining = now_clock.durationTo(timer.deadline);
                        // boot/real can't be tracked by the awake poll clock, so
                        // bound the wait to re-evaluate across suspend/steps.
                        if (clock != .awake and remaining.value > self.wall_clock_cap.value) {
                            remaining = self.wall_clock_cap;
                        }
                        // A clock the backend arms records its earliest deadline
                        // for syncWallTimer below, and keeps the capped remaining
                        // to fold only if that arming fails. `native_capped` folds
                        // it either way: there the kernel timer is a backstop for
                        // what the poll clock cannot see (a deadline coming due
                        // during suspend), and this timeout is still the schedule.
                        const fold = switch (modes[idx]) {
                            .fallback => true,
                            .native, .native_capped => blk: {
                                wall_deadline[idx] = timer.deadline.value;
                                wall_remaining[idx] = remaining;
                                break :blk modes[idx] == .native_capped;
                            },
                        };
                        if (fold and (next_timeout == null or remaining.value < next_timeout.?.value)) {
                            next_timeout = remaining;
                        }
                        break;
                    }
                    if (self.state.postponeTimer(timer, now_clock)) continue;
                    timer.c.setResult(.timer, {});
                    self.state.disarmTimer(timer);
                    batch[batch_count] = timer;
                    batch_count += 1;
                    if (batch_count >= batch.len) break;
                }
                self.state.unlockTimers();

                // Mark completions outside the lock
                if (comptime zio_options.sim) {
                    // Same-deadline fire order is a real source of
                    // nondeterminism. Shuffle the batch with the sim RNG.
                    var i: usize = batch_count;
                    while (i > 1) {
                        i -= 1;
                        const j = sim.pickIndex(i + 1);
                        const tmp = batch[i];
                        batch[i] = batch[j];
                        batch[j] = tmp;
                    }
                }
                for (batch[0..batch_count]) |timer| {
                    if (comptime zio_options.sim) {
                        sim.emit(.timer_fire, @backingInt(clock), @truncate(timer.deadline.value));
                    }
                    self.state.markCompleted(&timer.c);
                    fired = true;
                }

                // If we didn't fill the batch, we're done with this domain
                if (batch_count < batch.len) break;
            }
        }

        // Hand each boot/real minimum to the backend's native wall-clock timer.
        // syncWallTimer returns false only when it couldn't arm a pending
        // deadline (e.g. SQ full); then fold that clock's capped remaining into
        // the poll timeout so it's still re-evaluated within the cap.
        inline for (1..wall_clock_count) |idx| {
            // A clock left on the fallback never reaches the backend, and where
            // boot is not distinct from awake its timers live in the awake heap,
            // so its entry stays null and there is nothing to arm.
            if (comptime modes[idx] != .fallback and (idx != 1 or time.boot_distinct_from_awake)) {
                const clock = comptime indexClock(idx);
                if (!self.backend.syncWallTimer(clock, wall_deadline[idx])) {
                    const remaining = wall_remaining[idx];
                    if (next_timeout == null or remaining.value < next_timeout.?.value) {
                        next_timeout = remaining;
                    }
                }
            }
        }

        return .{ .next_timeout = next_timeout, .fired = fired };
    }

    /// Check if an async handle is pending and set its result if so.
    /// Returns true if the async was pending and had its result set.
    /// Caller is responsible for managing queues and calling markCompleted.
    fn checkAndSetAsyncResult(async_handle: *Async) bool {
        // acq_rel: pairs with the swap in Async.notify (see the comment there).
        // The release half publishes addInternal's setLoop to a notifier that
        // misses this pending flag.
        const was_pending = async_handle.pending.swap(0, .acq_rel);
        if (was_pending != 0) {
            async_handle.c.setResult(.async, {});
            return true;
        }
        return false;
    }

    /// Standard completion callback for user-submitted Work
    pub fn loopWorkComplete(ctx: ?*anyopaque, work: *Work) void {
        const loop: *Loop = @ptrCast(@alignCast(ctx));
        loop.state.work_completions.push(&work.c);
        loop.wake();
    }

    /// Linked work context for file operations
    pub const LinkedWorkContext = struct {
        loop: *Loop,
        linked: *Completion,
    };

    /// Completion callback for internal file ops with linked completion
    pub fn loopLinkedWorkComplete(ctx: ?*anyopaque, work: *Work) void {
        const context: *LinkedWorkContext = @ptrCast(@alignCast(ctx));
        // Publishing `linked` hands the containing operation back to the loop;
        // its waiter may then resume and free that operation before this worker
        // returns. Snapshot everything stored in the operation before the push.
        const loop = context.loop;
        const linked = context.linked;
        // Propagate cancel error from work to linked completion
        if (work.c.err) |err| {
            if (!linked.has_result) {
                linked.setError(err);
            }
        }
        loop.state.work_completions.push(linked);
        loop.wake();
    }

    pub fn processAsyncHandles(self: *Loop) void {
        // Check all async handles for pending notifications
        var c = self.state.async_handles.head;
        while (c) |completion| {
            const next = completion.next;
            const async_handle = completion.cast(Async);
            if (checkAndSetAsyncResult(async_handle)) {
                // This handle was notified - remove from queue and complete it
                _ = self.state.async_handles.remove(completion);
                self.state.markCompleted(&async_handle.c);
            }
            c = next;
        }
    }

    pub fn processCompletions(self: *Loop) void {
        var work_completions = self.state.work_completions.popAll();
        while (work_completions.pop()) |completion| {
            self.state.pool_inflight -= 1;
            // Drop from the cancel-resend list before finalizing: markCompleted
            // wakes the waiter, whose coroutine may then free the op (and its
            // token). Removing here keeps the sweep from touching freed memory.
            self.state.removeResendByCompletion(completion);
            self.state.markCompleted(completion);
        }

        // Drain only what was queued at entry: a rearm completion can
        // re-complete itself from its own callback, and chasing those here
        // would let a notify storm pin the loop. The rest runs next tick
        // (non-blocking; pending completions force a zero poll timeout).
        var snapshot = self.state.completions;
        self.state.completions = .{};
        while (snapshot.pop()) |completion| {
            self.state.finishCompletion(completion);
        }
    }

    /// Process cross-thread cancel requests
    fn processCancelQueue(self: *Loop) void {
        // Atomically swap the entire queue
        var c = self.cancel_queue.swap(null, .acquire);
        while (c) |completion| {
            const next = completion.cancel_next;
            completion.cancel_next = null;

            // cancelLocal re-checks the phase and clears cancel_inflight
            self.cancelLocal(completion);

            c = next;
        }
    }

    fn submitFileOpToThreadPool(self: *Loop, completion: *Completion) void {
        const tp = self.thread_pool orelse {
            // No thread pool - complete with error
            log.err("No thread pool available for file operation", .{});
            completion.setError(error.Unexpected);
            self.state.markCompleted(completion);
            return;
        };

        switch (completion.op) {
            inline .file_open, .file_create, .file_close, .file_read, .file_write, .file_read_streaming, .file_write_streaming, .file_sync, .file_sync_range, .file_set_size, .file_set_permissions, .file_set_owner, .file_set_timestamps, .dir_create_dir, .dir_rename, .dir_rename_preserve, .dir_delete_file, .dir_delete_dir, .file_size, .file_stat, .dir_open, .dir_close, .dir_read, .dir_set_permissions, .dir_set_owner, .dir_set_file_permissions, .dir_set_file_owner, .dir_set_file_timestamps, .dir_sym_link, .dir_read_link, .dir_hard_link, .dir_access, .dir_real_path, .dir_real_path_file, .file_real_path, .file_hard_link, .device_io_control, .process_wait => |op| {
                if (comptime Backend.capability(op) == .yes) {
                    unreachable;
                }

                const op_func = switch (op) {
                    .file_open => common.fileOpenWork,
                    .file_create => common.fileCreateWork,
                    .file_close => common.fileCloseWork,
                    .file_read => common.fileReadWork,
                    .file_write => common.fileWriteWork,
                    .file_read_streaming => common.fileReadStreamingWork,
                    .file_write_streaming => common.fileWriteStreamingWork,
                    .file_sync => common.fileSyncWork,
                    .file_sync_range => common.fileSyncRangeWork,
                    .file_set_size => common.fileSetSizeWork,
                    .file_set_permissions => common.fileSetPermissionsWork,
                    .file_set_owner => common.fileSetOwnerWork,
                    .file_set_timestamps => common.fileSetTimestampsWork,
                    .dir_create_dir => common.dirCreateDirWork,
                    .dir_rename => common.dirRenameWork,
                    .dir_rename_preserve => common.dirRenamePreserveWork,
                    .dir_delete_file => common.dirDeleteFileWork,
                    .dir_delete_dir => common.dirDeleteDirWork,
                    .file_size => common.fileSizeWork,
                    .file_stat => common.fileStatWork,
                    .dir_open => common.dirOpenWork,
                    .dir_close => common.dirCloseWork,
                    .dir_set_permissions => common.dirSetPermissionsWork,
                    .dir_set_owner => common.dirSetOwnerWork,
                    .dir_set_file_permissions => common.dirSetFilePermissionsWork,
                    .dir_set_file_owner => common.dirSetFileOwnerWork,
                    .dir_set_file_timestamps => common.dirSetFileTimestampsWork,
                    .dir_sym_link => common.dirSymLinkWork,
                    .dir_read_link => common.dirReadLinkWork,
                    .dir_hard_link => common.dirHardLinkWork,
                    .dir_access => common.dirAccessWork,
                    .dir_read => common.dirReadWork,
                    .dir_real_path => common.dirRealPathWork,
                    .dir_real_path_file => common.dirRealPathFileWork,
                    .file_real_path => common.fileRealPathWork,
                    .file_hard_link => common.fileHardLinkWork,
                    .device_io_control => common.deviceIoControlWork,
                    .process_wait => common.processWaitWork,
                    else => unreachable,
                };

                const op_data = completion.cast(op.toType());
                op_data.linked_work.allocator = self.allocator;
                op_data.linked_work.linked_context = .{
                    .loop = self,
                    .linked = completion,
                };
                op_data.linked_work.work = Work.init(op_func, null);
                op_data.linked_work.work.completion_fn = loopLinkedWorkComplete;
                op_data.linked_work.work.completion_context = @ptrCast(&op_data.linked_work.linked_context);
                op_data.linked_work.work.cancel_token = &op_data.linked_work.token;
                self.state.pool_inflight += 1;
                tp.submit(&op_data.linked_work.work);
            },
            else => unreachable,
        }
    }

    // --- NetSendFile generic fallback (double-buffered read/send loop) ---
    //
    // Used when the backend does not implement net_send_file natively. Two
    // buffers ping-pong: at most one FileRead and one NetSend are in flight at a
    // time, into *different* buffers, so a read of the next chunk overlaps the
    // send of the current one. Both children typically complete in the same poll
    // window, so the loop retires a read and a send per wakeup (~2x vs. a strict
    // serial read→send loop). `advance` is the pump: after start and after every
    // child completion it (re)starts a send and/or read as buffers allow, and
    // finishes when nothing is in flight and there is nothing left to read.
    //
    // Order is preserved by `next_read`/`next_send`, which both alternate 0,1,0,1
    // from the same start, so buffers are sent in the order they were read.

    fn netSendFileStart(self: *Loop, op: *NetSendFile) void {
        op.fallback = .{};
        op.fallback.read_remaining = op.remaining;
        // Lay out the working buffers from the (up to two) caller buffers. When
        // the result's bufs[1] is empty the index flips are suppressed (see
        // netSendFileStartRead / netSendFileOnSend), so the loop runs serially.
        op.fallback.bufs = sendfileLayout(op.bufs);
        self.netSendFileAdvance(op);
    }

    fn netSendFileStartRead(self: *Loop, op: *NetSendFile) void {
        const f = &op.fallback;
        const idx = f.next_read;
        const want = @min(f.bufs[idx].len, f.read_remaining);
        f.reading = idx;
        if (f.bufs[1].len != 0) f.next_read ^= 1;
        f.read = FileRead.init(op.file, ReadBuf.fromSlice(f.bufs[idx][0..want], &f.read_iov), op.offset);
        f.read.c.userdata = op;
        f.read.c.callback = netSendFileOnRead;
        self.addInternal(&f.read.c);
    }

    fn netSendFileStartSend(self: *Loop, op: *NetSendFile, idx: u1, from: usize) void {
        const f = &op.fallback;
        f.sending = idx;
        f.send = NetSend.init(op.handle, WriteBuf.fromSlice(f.bufs[idx][from..f.filled[idx]], &f.send_iov), .{});
        f.send.c.userdata = op;
        f.send.c.callback = netSendFileOnSend;
        self.addInternal(&f.send.c);
    }

    /// Forward a cancel to both in-flight children. Their canceled completions
    /// re-enter `netSendFileAdvance`, which finishes the parent only once both
    /// have drained.
    fn netSendFileCancel(self: *Loop, op: *NetSendFile) void {
        if (op.fallback.reading != null) self.cancel(&op.fallback.read.c);
        if (op.fallback.sending != null) self.cancel(&op.fallback.send.c);
    }

    fn netSendFileAdvance(self: *Loop, op: *NetSendFile) void {
        const f = &op.fallback;

        // A cancel that arrived between callbacks turns into a parked error.
        if (op.c.loadState().cancel_requested and f.pending_err == null) {
            f.pending_err = error.Canceled;
        }

        if (f.pending_err) |err| {
            // Cancel whatever is still in flight; only finish once *both* inner
            // completions are done, so we never complete the parent early.
            self.netSendFileCancel(op);
            if (f.reading == null and f.sending == null) self.netSendFileFinish(op, err);
            return;
        }

        // Start sending the next buffer in order, if it is filled and ready.
        if (f.sending == null and f.filled[f.next_send] > 0) {
            f.sent[f.next_send] = 0;
            self.netSendFileStartSend(op, f.next_send, 0);
        }

        // Start the next read into a free buffer, if more data is wanted.
        const reads_done = f.eof or f.read_remaining == 0;
        if (f.reading == null and !reads_done and
            f.filled[f.next_read] == 0 and f.sending != f.next_read)
        {
            self.netSendFileStartRead(op);
        }

        // Nothing in flight and nothing left to read → done.
        if (f.reading == null and f.sending == null and
            f.filled[0] == 0 and f.filled[1] == 0 and reads_done)
        {
            self.netSendFileFinish(op, null);
        }
    }

    fn netSendFileOnRead(loop: *Loop, child: *Completion) void {
        const op: *NetSendFile = @ptrCast(@alignCast(child.userdata.?));
        const f = &op.fallback;
        const idx = f.reading.?;
        f.reading = null;
        const n = child.cast(FileRead).getResult() catch |err| {
            f.eof = true;
            f.pending_err = f.pending_err orelse err;
            return loop.netSendFileAdvance(op);
        };
        if (n == 0) {
            f.eof = true;
        } else {
            f.filled[idx] = n;
            f.read_remaining -= n;
            op.offset += n;
        }
        loop.netSendFileAdvance(op);
    }

    fn netSendFileOnSend(loop: *Loop, child: *Completion) void {
        const op: *NetSendFile = @ptrCast(@alignCast(child.userdata.?));
        const f = &op.fallback;
        const idx = f.sending.?;
        const m = child.cast(NetSend).getResult() catch |err| {
            f.sending = null;
            f.pending_err = f.pending_err orelse err;
            return loop.netSendFileAdvance(op);
        };
        f.sent[idx] += m;
        f.total += m;
        if (f.sent[idx] < f.filled[idx]) {
            // Socket short-write: resume draining the same buffer.
            loop.netSendFileStartSend(op, idx, f.sent[idx]);
            return loop.netSendFileAdvance(op);
        }
        // Buffer fully drained; hand it back to the read side.
        f.filled[idx] = 0;
        f.sending = null;
        if (f.bufs[1].len != 0) f.next_send ^= 1;
        loop.netSendFileAdvance(op);
    }

    fn netSendFileFinish(self: *Loop, op: *NetSendFile, err: ?anyerror) void {
        // A cancel only wins when nothing was sent (see NetSendFile).
        if (err) |e| {
            if (e != error.Canceled or op.fallback.total == 0) {
                op.c.setError(e);
                return self.state.markCompleted(&op.c);
            }
        }
        op.c.setResult(.net_send_file, op.fallback.total);
        self.state.markCompleted(&op.c);
    }

    /// Process one batch of events: expire timers, poll the backend for
    /// completions, run callbacks. `wait_cap` bounds how long the backend poll
    /// may block: `.zero` never blocks (and skips the poll syscall entirely
    /// when nothing is in flight), `.max` waits for the next event, anything
    /// in between caps the wait at that duration.
    /// Timer deadlines, pending completions, and the loop's `max_wait` option
    /// can all shorten the wait; they never lengthen it.
    pub fn poll(self: *Loop, wait_cap: Duration) !void {
        std.debug.assert(self.state.initialized);
        if (self.done()) return;

        const wait = wait_cap.value != 0;

        self.state.updateNow();
        const timer_result = self.checkTimers();

        // Re-send SIGURG to any worker still blocked in a canceled syscall.
        self.state.sweepResend();

        var timeout: Duration = .zero;
        if (wait) {
            // Sim I/O already due is a completion waiting to be harvested.
            const has_due_io = if (comptime zio_options.sim) sim.hasDueIo() else false;
            // Don't block if we have completions waiting to be processed or timers fired
            if (!self.state.completions.empty() or !self.state.work_completions.empty() or timer_result.fired or has_due_io) {
                timeout = .zero;
            } else if (timer_result.next_timeout) |t| {
                // Use timer timeout, capped at max_wait
                timeout = if (t.value < self.max_wait.value) t else self.max_wait;
            } else {
                // No timers, wait for blocking I/O
                timeout = self.max_wait;
            }
            // While cancellations are pending, keep waking to re-send SIGURG so a
            // lost first signal is retried promptly rather than at max_wait.
            if (self.state.cancel_resend != null and timeout.value > resend_interval.value) {
                timeout = resend_interval;
            }
            if (wait_cap.value < timeout.value) {
                timeout = wait_cap;
            }
        }

        // Skip the backend poll when not waiting and there's nothing to retrieve.
        // This avoids syscall overhead for pure CPU-bound workloads.
        const should_poll = wait or self.hasInflight();
        if (should_poll and timeout.value != 0) {
            // Announce the sleep, so wakers know a syscall is needed from
            // here on. A request that got in first means there is no sleep.
            if (self.state.wake_requested.fetchOr(LoopState.sleeping, .acq_rel) & LoopState.wake_mask != 0) {
                timeout = .zero;
            }
        }
        if (comptime zio_options.sim) {
            // A full park (the caller's cap is at least the loop's max wait)
            // with no timer that can wake a task and no simulated I/O still
            // to come can never end on one thread: deadlock. Checked after the
            // sleep announcement, so a wake request that got in first is not
            // mistaken for one. As the only loop, this one leaves the shrink
            // timer out (`sim_idle_timer`); as one of two, it counts every
            // timer and `simMuxStep` makes the runtime-wide call.
            const no_task_timer = if (self.sim_solo) !self.simHasTaskTimer() else timer_result.next_timeout == null;
            if (timeout.value != 0 and no_task_timer and
                wait_cap.value >= self.max_wait.value and sim.nextDueIoRemaining() == null)
            {
                sim.deadlock();
            }
        }
        const timed_out = if (should_poll) try self.waitForEvents(timeout) else false;
        // Awake again: take the requests, including any that arrived during
        // the poll, and clear `sleeping`.
        const wake_flags = self.state.wake_requested.swap(0, .acq_rel);

        // The wait (`backend.poll`, or the sim's logical-clock advance) is
        // the only place the loop sleeps, so the snapshot is stale by the
        // whole sleep here. Refresh before anything that can arm a timer: the
        // callbacks below, and the caller's task batch. Sim must not have its
        // own copy of this statement: a production regression here has to be
        // a sim regression (#711).
        self.state.updateNow();

        // Process async handles if the async bit was set
        if (wake_flags & LoopState.wake_async != 0) {
            self.processAsyncHandles();
        }

        // Process cross-thread cancel requests
        if (wake_flags & LoopState.wake_cancel != 0) {
            self.processCancelQueue();
        }

        if (comptime zio_options.sim) {
            self.harvestSimIo();
        }

        // Process any work completions from thread pool
        self.processCompletions();

        // Only if we timed out: the timeout was the earliest deadline, so
        // waking ahead of it means nothing has expired.
        if (timed_out) {
            _ = self.checkTimers();
            if (comptime zio_options.sim) {
                self.harvestSimIo();
                self.processCompletions();
            }
        }
    }

    /// Block until the next event, or return at once for a zero timeout.
    /// Returns whether the full timeout passed. Production calls
    /// `backend.poll`. Sim advances the logical clock instead and never
    /// enters the kernel: to the next simulated I/O due time if that comes
    /// first (`false`, the mid-sleep I/O wake), else by the whole timeout.
    fn waitForEvents(self: *Loop, timeout: Duration) !bool {
        if (comptime zio_options.sim) {
            const timer_ns = timeout.toNanoseconds();
            if (sim.nextDueIoRemaining()) |io_ns| {
                if (timer_ns == 0) return false;
                if (io_ns <= timer_ns) {
                    if (io_ns != 0) sim.advanceNs(io_ns);
                    return false;
                }
            }
            if (timer_ns != 0) {
                sim.advanceNs(timer_ns);
                return true;
            }
            return false;
        }
        return try self.backend.poll(&self.state, timeout);
    }

    const sim_max_iovecs = 16;

    fn readSlices(buf: ReadBuf, out: *[sim_max_iovecs][]u8) [][]u8 {
        if (buf.iovecs.len > sim_max_iovecs) sim.panic("sim: recv with {d} iovecs (max {d})", .{ buf.iovecs.len, sim_max_iovecs });
        for (buf.iovecs, 0..) |v, i| out[i] = v.base[0..v.len];
        return out[0..buf.iovecs.len];
    }

    fn writeSlices(buf: WriteBuf, out: *[sim_max_iovecs][]const u8) []const []const u8 {
        if (buf.iovecs.len > sim_max_iovecs) sim.panic("sim: send with {d} iovecs (max {d})", .{ buf.iovecs.len, sim_max_iovecs });
        for (buf.iovecs, 0..) |v, i| out[i] = v.base[0..v.len];
        return out[0..buf.iovecs.len];
    }

    /// Sim: the fd a net op names, or null for one that names none.
    fn simOpHandle(c: *Completion) ?i32 {
        return switch (c.op) {
            .net_open => null,
            inline .net_bind, .net_listen, .net_connect, .net_accept, .net_recv, .net_send, .net_recvfrom, .net_sendto, .net_recvmsg, .net_recvmmsg, .net_sendmsg, .net_sendmmsg, .net_poll, .net_shutdown, .net_close => |op| c.cast(op.toType()).handle,
            else => null,
        };
    }

    /// Sim: start a net op. On a sim datagram socket (and every `net_open`
    /// during a run) it goes to `submitSimDgram`. On a `sim.pipePair` end it
    /// scatters or gathers across every iovec: a result the pipe can give
    /// now is set here and delivered after a seeded delay (`harvestSimIo`);
    /// otherwise the op parks on the pipe. A real fd never gets here: during
    /// a run that is input at a real time, so it refuses.
    fn submitSimIo(self: *Loop, c: *Completion) void {
        if (simOpHandle(c)) |h| {
            if (!sim.isSimFd(h)) sim.panic("sim: a real fd {d} reached sim op {s} during a run", .{ h, @tagName(c.op) });
            if (sim.isDgramFd(h)) return submitSimDgram(c);
        } else if (c.op == .net_open) {
            return submitSimDgram(c);
        }
        switch (c.op) {
            .net_recv => {
                const op = c.cast(NetRecv);
                refuseSimPipeFlags("receive", op.flags, op.handle, &.{"dontwait"});
                var sl: [sim_max_iovecs][]u8 = undefined;
                switch (sim.recvInto(op.handle, readSlices(op.buffers, &sl), c, op.flags.dontwait)) {
                    .due => |n| c.setResult(.net_recv, n),
                    .eof => c.setResult(.net_recv, 0),
                    .reset => c.setError(error.ConnectionResetByPeer),
                    .parked => {},
                    .would_block => {
                        c.setError(error.WouldBlock);
                        if (sim.takeOpId(c)) |id| sim.emit(.io_complete, @backingInt(c.op), id);
                        self.state.markCompleted(c);
                    },
                    .bad_fd => c.setError(error.FileDescriptorNotASocket),
                }
            },
            .net_send => {
                const op = c.cast(NetSend);
                refuseSimPipeFlags("send", op.flags, op.handle, &.{});
                var sl: [sim_max_iovecs][]const u8 = undefined;
                switch (sim.sendBytes(op.handle, writeSlices(op.buffer, &sl), c)) {
                    .due => |n| c.setResult(.net_send, n),
                    .eof => c.setError(error.BrokenPipe),
                    .reset => c.setError(error.ConnectionResetByPeer),
                    .parked => {},
                    .would_block => unreachable,
                    .bad_fd => c.setError(error.FileDescriptorNotASocket),
                }
            },
            .net_close => {
                const op = c.cast(NetClose);
                switch (sim.closeFd(op.handle, c)) {
                    .due => c.setResult(.net_close, {}),
                    .bad_fd => c.setError(error.FileDescriptorNotASocket),
                }
            },
            .net_shutdown => {
                const op = c.cast(NetShutdown);
                const how: sim.ShutdownHow = switch (op.how) {
                    .receive => .receive,
                    .send => .send,
                    .both => .both,
                };
                switch (sim.shutdownFd(op.handle, how, c)) {
                    .due => c.setResult(.net_shutdown, {}),
                    .bad_fd => c.setError(error.SocketUnconnected),
                }
            },
            else => sim.panic("sim: unsimulated I/O op {s}", .{@tagName(c.op)}),
        }
    }

    /// Sim: an op that can park on a datagram socket: a receive or a poll
    /// for reading, also one with `dontwait` (the one-waiter rule counts
    /// every such submit, as on a pipe end).
    fn simDgramMayWait(c: *Completion) bool {
        return switch (c.op) {
            .net_recvfrom, .net_recvmsg => true,
            .net_poll => c.cast(NetPoll).event == .recv,
            else => false,
        };
    }

    /// Sim: what a parked datagram op waits for: only a receive or a poll
    /// for reading ever parks (`tryDgramOp`).
    fn simDgramWait(c: *const Completion) sim.DgramWait {
        return if (c.op == .net_poll) .poll else .recv;
    }

    /// Sim: a sim pipe models only a send with the default flags and a
    /// receive's `dontwait`. Every other flag that differs from its default
    /// refuses, by name; the list comes from the type, so a flag added later
    /// refuses too. Why each is not modelled: a pipe send never raises
    /// SIGPIPE (so `no_signal = false` would differ) and parks on a full
    /// buffer (so `dontwait` would differ); `peek` would consume; `trunc`
    /// discards on a Linux stream; `oob`, `eor`, `waitall` and the rest have
    /// no meaning the sim gives them.
    fn refuseSimPipeFlags(comptime what: []const u8, flags: anytype, fd: i32, comptime modelled: []const []const u8) void {
        const T = @TypeOf(flags);
        const defaults: T = .{};
        inline for (comptime std.meta.fieldNames(T)) |name| {
            const is_modelled = comptime for (modelled) |m| {
                if (std.mem.eql(u8, m, name)) break true;
            } else false;
            if (!is_modelled and @field(flags, name) != @field(defaults, name)) {
                sim.panic("sim: " ++ what ++ " flag " ++ name ++ " on sim pipe fd {d} is not simulated", .{fd});
            }
        }
    }

    /// Sim: an ev op on a sim datagram socket. It does what the poll backend
    /// does once the socket is ready, through the same socket-layer calls
    /// (their sim branch, src/os/net.zig) and the same shared helpers for
    /// open, bind, shutdown and close. Where that backend would wait for
    /// readiness (a receive with nothing queued, a poll for reading), the op
    /// parks on the socket, and the datagram's landing makes it due
    /// (`sim.land`). A result is due at once, with no seeded delay: unlike a
    /// pipe, a datagram's timing is the wire's (`sim.wire_latency_ns`).
    fn submitSimDgram(c: *Completion) void {
        const id = sim.dgramOpStart(c, simOpHandle(c));
        if (simDgramMayWait(c)) sim.dgramRefuseSecondWaiter(simOpHandle(c).?, id);
        if (tryDgramOp(c)) {
            sim.dgramOpDone(c, id);
        } else {
            sim.dgramPark(simOpHandle(c).?, c, id, simDgramWait(c));
        }
    }

    /// Does a datagram op now if it can; false when it must wait for a
    /// datagram. The socket layer's receive is called with `dontwait`: the
    /// op parks instead of the layer refusing a blocking receive.
    fn tryDgramOp(c: *Completion) bool {
        switch (c.op) {
            .net_open => common.handleNetOpen(c),
            .net_bind => common.handleNetBind(c),
            .net_shutdown => common.handleNetShutdown(c),
            .net_close => common.handleNetClose(c),
            .net_send => {
                const op = c.cast(NetSend);
                if (net.send(op.handle, op.buffer.iovecs, op.flags)) |n| c.setResult(.net_send, n) else |err| c.setError(err);
            },
            .net_recv => {
                const op = c.cast(NetRecv);
                if (net.recv(op.handle, op.buffers.iovecs, op.flags)) |n| c.setResult(.net_recv, n) else |err| c.setError(err);
            },
            .net_sendto => {
                const op = c.cast(NetSendTo);
                if (net.sendto(op.handle, op.buffer.iovecs, op.flags, op.addr, op.addr_len)) |n| c.setResult(.net_sendto, n) else |err| c.setError(err);
            },
            .net_sendmsg => {
                const op = c.cast(NetSendMsg);
                if (net.sendmsg(op.handle, op.data.iovecs, op.flags, op.addr, op.addr_len, op.control)) |n| c.setResult(.net_sendmsg, n) else |err| c.setError(err);
            },
            .net_recvfrom => {
                const op = c.cast(NetRecvFrom);
                var flags = op.flags;
                flags.dontwait = true;
                if (net.recvfrom(op.handle, op.buffer.iovecs, flags, op.addr, op.addr_len)) |n| {
                    c.setResult(.net_recvfrom, n);
                } else |err| switch (err) {
                    error.WouldBlock => if (op.flags.dontwait) c.setError(err) else return false,
                    else => c.setError(err),
                }
            },
            .net_recvmsg => {
                const op = c.cast(NetRecvMsg);
                var flags = op.flags;
                flags.dontwait = true;
                if (net.recvmsg(op.handle, op.data.iovecs, flags, op.addr, op.addr_len, op.control)) |r| {
                    c.setResult(.net_recvmsg, r);
                } else |err| switch (err) {
                    error.WouldBlock => if (op.flags.dontwait) c.setError(err) else return false,
                    else => c.setError(err),
                }
            },
            .net_poll => {
                const op = c.cast(NetPoll);
                // Read for every event: it refuses a closed socket.
                const readable = sim.dgramReadable(op.handle);
                if (op.event == .recv and !readable) return false;
                c.setResult(.net_poll, {});
            },
            else => sim.panic("sim: unsimulated I/O op {s} on a datagram socket", .{@tagName(c.op)}),
        }
        return true;
    }

    /// Sim: called from `cancelLocal`, inside the pin's cancel pass. An op
    /// that already has its result keeps it (the cancel lost the race);
    /// otherwise it ends `Canceled`. `cancelLocal`'s epilogue dispatches.
    fn cancelSimIo(self: *Loop, c: *Completion) void {
        const id = sim.cancelIo(c);
        if (!c.has_result) c.setError(error.Canceled);
        if (id) |op_id| sim.emit(.io_complete, @backingInt(c.op), op_id);
        self.state.markCompleted(c);
    }

    /// Sim: complete every simulated op whose due time has come, in a
    /// seeded order.
    fn harvestSimIo(self: *Loop) void {
        var buf: [sim.max_due]sim.TakenDue = undefined;
        const n = sim.takeDue(&buf);
        var i: usize = 0;
        while (i < n) : (i += 1) {
            // `markCompleted` below can run callbacks at once (an op that does
            // not defer its finish). A group callback can cancel an op that is
            // still in this batch: it completes through the cancel, and its
            // frame can be freed. So the pointer is used only while this
            // exact submit (pointer and op id) is still live.
            if (!sim.isLive(buf[i].c, buf[i].id)) continue;
            const c: *Completion = @ptrCast(@alignCast(buf[i].c));
            if (!c.has_result) dgram: {
                // A parked datagram op that a landing or a close made due. It
                // must still have the socket it was submitted on: an earlier
                // entry's callback may have closed it and reused its number.
                const h = simOpHandle(c) orelse break :dgram;
                if (!sim.isDgramFd(h)) break :dgram;
                if (!sim.dgramOpOnItsSocket(buf[i].c, buf[i].id, h)) {
                    c.setError(error.FileDescriptorNotASocket);
                } else if (!tryDgramOp(c)) {
                    sim.dgramPark(h, c, buf[i].id, simDgramWait(c));
                    continue;
                }
            }
            if (!c.has_result) {
                switch (c.op) {
                    .net_recv => {
                        const op = c.cast(NetRecv);
                        var sl: [sim_max_iovecs][]u8 = undefined;
                        if (sim.isReset(op.handle)) {
                            c.setError(error.ConnectionResetByPeer);
                        } else if (sim.recvIsEof(op.handle)) {
                            c.setResult(.net_recv, 0);
                        } else {
                            c.setResult(.net_recv, sim.harvestRecv(op.handle, readSlices(op.buffers, &sl)));
                        }
                    },
                    .net_send => {
                        // Capacity-woken send: copy into the peer buffer.
                        // Do not re-queue this completion. BrokenPipe is
                        // only for a closed peer.
                        const op = c.cast(NetSend);
                        var sl: [sim_max_iovecs][]const u8 = undefined;
                        switch (sim.harvestSend(op.handle, writeSlices(op.buffer, &sl), c, buf[i].id)) {
                            .due => |sent| c.setResult(.net_send, sent),
                            .eof => c.setError(error.BrokenPipe),
                            .reset => c.setError(error.ConnectionResetByPeer),
                            .parked => unreachable,
                            .would_block => unreachable,
                            .bad_fd => c.setError(error.FileDescriptorNotASocket),
                        }
                    },
                    .net_close => c.setResult(.net_close, {}),
                    .net_shutdown => c.setResult(.net_shutdown, {}),
                    else => {},
                }
            }
            if (!c.has_result) continue;
            sim.endOp(buf[i].c, buf[i].id);
            sim.emit(.io_complete, @backingInt(c.op), buf[i].id);
            self.state.markCompleted(c);
        }
    }
};

test {
    _ = @import("tests.zig");
}
