const std = @import("std");
const builtin = @import("builtin");
const ev = @import("../root.zig");
const os_thread = @import("../../os/thread.zig");
const ResetEvent = os_thread.ResetEvent;

test "ev.ThreadPool: one task" {
    var thread_pool: ev.ThreadPool = undefined;
    try thread_pool.init(std.testing.allocator, .{
        .min_threads = 1,
        .max_threads = 1,
    });
    defer thread_pool.deinit();

    var loop: ev.Loop = undefined;
    try loop.init(.{
        .thread_pool = &thread_pool,
    });
    defer loop.deinit();
    // Join the pool's threads before the loop is torn down: completion
    // callbacks wake the loop, so the loop must outlive the pool's threads.
    defer thread_pool.stop();

    const TestFn = struct {
        called: usize = 0,
        pub fn main(work: *ev.Work) void {
            var self: *@This() = @ptrCast(@alignCast(work.userdata));
            self.called += 1;
        }
    };

    var test_fn: TestFn = .{};
    var work = ev.Work.init(&TestFn.main, @ptrCast(&test_fn));

    loop.add(&work.c);

    try loop.run();

    try std.testing.expectEqual(.dead, work.c.loadState().phase);
    try std.testing.expectEqual(1, test_fn.called);
}

test "ev.ThreadPool: reserved work runs even when the pool is saturated" {
    if (builtin.single_threaded) return error.SkipZigTest;

    var thread_pool: ev.ThreadPool = undefined;
    try thread_pool.init(std.testing.allocator, .{
        .min_threads = 0,
        .max_threads = 1,
    });
    defer thread_pool.deinit();
    defer thread_pool.stop();

    // A job that occupies the pool's single worker until the test releases it.
    const Blocker = struct {
        started: ResetEvent,
        gate: ResetEvent,
        pub fn main(work: *ev.Work) void {
            const self: *@This() = @ptrCast(@alignCast(work.userdata));
            self.started.set();
            self.gate.wait();
        }
    };

    // A job that records that it ran and signals completion. With max_threads == 1
    // fully occupied, this can only make progress if reserve_thread grows the pool.
    const Reserved = struct {
        ran: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
        done: ResetEvent,
        pub fn main(work: *ev.Work) void {
            const self: *@This() = @ptrCast(@alignCast(work.userdata));
            self.ran.store(true, .release);
            self.done.set();
        }
    };

    var blocker: Blocker = .{ .started = .init(), .gate = .init() };
    defer blocker.started.deinit();
    defer blocker.gate.deinit();
    var blocker_work = ev.Work.init(&Blocker.main, @ptrCast(&blocker));
    thread_pool.submit(&blocker_work);

    // Ensure the worker is inside the blocker (the pool is now saturated) before
    // submitting the reserved job.
    blocker.started.wait();

    var reserved: Reserved = .{ .done = .init() };
    defer reserved.done.deinit();
    var reserved_work = ev.Work.init(&Reserved.main, @ptrCast(&reserved));
    reserved_work.reserve_thread = true;
    thread_pool.submit(&reserved_work);

    // Without the reservation this would deadlock (queued behind the blocker);
    // with it, a second worker is spawned to run it.
    reserved.done.wait();
    try std.testing.expect(reserved.ran.load(.acquire));

    // Release the blocker so the pool can shut down cleanly.
    blocker.gate.set();
}

test "ev.ThreadPool: reserved work is taken ahead of older regular work" {
    if (builtin.single_threaded) return error.SkipZigTest;

    var thread_pool: ev.ThreadPool = undefined;
    try thread_pool.init(std.testing.allocator, .{
        .min_threads = 0,
        .max_threads = 1,
    });
    defer thread_pool.deinit();
    defer thread_pool.stop();

    // Both regular jobs wait on the gate: the first occupies the only worker,
    // the second waits in the queue. Only the reserved job opens the gate.
    const Waiter = struct {
        started: ResetEvent,
        done: ResetEvent,
        gate: *ResetEvent,
        pub fn main(work: *ev.Work) void {
            const self: *@This() = @ptrCast(@alignCast(work.userdata));
            self.started.set();
            self.gate.wait();
            self.done.set();
        }
    };
    const Opener = struct {
        gate: *ResetEvent,
        pub fn main(work: *ev.Work) void {
            const self: *@This() = @ptrCast(@alignCast(work.userdata));
            self.gate.set();
        }
    };

    var gate = ResetEvent.init();
    defer gate.deinit();

    var first: Waiter = .{ .started = .init(), .done = .init(), .gate = &gate };
    defer first.started.deinit();
    defer first.done.deinit();
    var first_work = ev.Work.init(&Waiter.main, @ptrCast(&first));
    thread_pool.submit(&first_work);
    first.started.wait();

    var queued: Waiter = .{ .started = .init(), .done = .init(), .gate = &gate };
    defer queued.started.deinit();
    defer queued.done.deinit();
    var queued_work = ev.Work.init(&Waiter.main, @ptrCast(&queued));
    thread_pool.submit(&queued_work);

    // The reservation spawns a second worker. If that worker took the queue
    // head it would run `queued`, which waits on the gate, and nothing would
    // ever open it.
    var opener: Opener = .{ .gate = &gate };
    var opener_work = ev.Work.init(&Opener.main, @ptrCast(&opener));
    opener_work.reserve_thread = true;
    thread_pool.submit(&opener_work);

    first.done.wait();
    queued.done.wait();
}

test "ev.ThreadPool: reserved work reuses an idle worker instead of spawning" {
    if (builtin.single_threaded) return error.SkipZigTest;

    var thread_pool: ev.ThreadPool = undefined;
    try thread_pool.init(std.testing.allocator, .{
        .min_threads = 2,
        .max_threads = 10,
    });
    defer thread_pool.deinit();
    defer thread_pool.stop();

    // Wait until both min-threads have parked, so there is idle capacity for the
    // reservation to reuse.
    var spins: usize = 0;
    while (thread_pool.idle_threads.load(.monotonic) < 2) {
        spins += 1;
        if (spins > 100_000_000) return error.TimedOut;
        os_thread.yield();
    }
    try std.testing.expectEqual(@as(usize, 2), thread_pool.running_threads.load(.monotonic));

    // A reserved job that parks so we can observe the worker count while it runs.
    const Job = struct {
        started: ResetEvent,
        gate: ResetEvent,
        pub fn main(work: *ev.Work) void {
            const self: *@This() = @ptrCast(@alignCast(work.userdata));
            self.started.set();
            self.gate.wait();
        }
    };

    var job: Job = .{ .started = .init(), .gate = .init() };
    defer job.started.deinit();
    defer job.gate.deinit();
    var work = ev.Work.init(&Job.main, @ptrCast(&job));
    work.reserve_thread = true;
    thread_pool.submit(&work);

    // The reserved job runs on one of the two idle workers; because idle capacity
    // already covers the reservation, no new thread is spawned.
    job.started.wait();
    try std.testing.expectEqual(@as(usize, 2), thread_pool.running_threads.load(.monotonic));

    job.gate.set();
}

test "ev.ThreadPool: regular work still scales when reserved work occupies max_threads" {
    if (builtin.single_threaded) return error.SkipZigTest;

    var thread_pool: ev.ThreadPool = undefined;
    try thread_pool.init(std.testing.allocator, .{
        .min_threads = 0,
        .max_threads = 1,
    });
    defer thread_pool.deinit();
    defer thread_pool.stop();

    // A reserved job occupies the pool's single regular-budget worker.
    const Reserved = struct {
        started: ResetEvent,
        gate: ResetEvent,
        pub fn main(work: *ev.Work) void {
            const self: *@This() = @ptrCast(@alignCast(work.userdata));
            self.started.set();
            self.gate.wait();
        }
    };
    var reserved: Reserved = .{ .started = .init(), .gate = .init() };
    defer reserved.started.deinit();
    defer reserved.gate.deinit();
    var reserved_work = ev.Work.init(&Reserved.main, @ptrCast(&reserved));
    reserved_work.reserve_thread = true;
    thread_pool.submit(&reserved_work);
    reserved.started.wait(); // now occupying the single max_threads slot

    // Regular work must still run: max_threads is the budget for regular work and
    // reserved work is additive, so the pool spawns a second worker for it. Without
    // counting reserved-occupied workers separately, this would starve behind the
    // blocked reserved job.
    const Regular = struct {
        ran: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
        done: ResetEvent,
        pub fn main(work: *ev.Work) void {
            const self: *@This() = @ptrCast(@alignCast(work.userdata));
            self.ran.store(true, .release);
            self.done.set();
        }
    };
    var regular: Regular = .{ .done = .init() };
    defer regular.done.deinit();
    var regular_work = ev.Work.init(&Regular.main, @ptrCast(&regular));
    thread_pool.submit(&regular_work);

    regular.done.wait();
    try std.testing.expect(regular.ran.load(.acquire));

    reserved.gate.set();
}

/// A job that signals it started, then blocks until the shared gate opens.
const GatedJob = struct {
    started: ResetEvent,
    gate: *ResetEvent,
    work: ev.Work,

    fn init(self: *GatedJob, gate: *ResetEvent) void {
        self.* = .{ .started = .init(), .gate = gate, .work = undefined };
        self.work = ev.Work.init(&main, @ptrCast(self));
    }

    fn deinit(self: *GatedJob) void {
        self.started.deinit();
    }

    fn main(work: *ev.Work) void {
        const self: *GatedJob = @ptrCast(@alignCast(work.userdata));
        self.started.set();
        self.gate.wait();
    }
};

test "ev.ThreadPool: work does not queue behind a blocked worker" {
    if (builtin.single_threaded) return error.SkipZigTest;

    var thread_pool: ev.ThreadPool = undefined;
    try thread_pool.init(std.testing.allocator, .{
        .min_threads = 0,
        .max_threads = 4,
    });
    defer thread_pool.deinit();
    defer thread_pool.stop();

    var gate = ResetEvent.init();
    defer gate.deinit();
    defer gate.set();

    // Like a console read on Windows, which blocks until the user types a line.
    var blocker: GatedJob = undefined;
    blocker.init(&gate);
    defer blocker.deinit();
    thread_pool.submit(&blocker.work);
    blocker.started.wait();

    var second: GatedJob = undefined;
    second.init(&gate);
    defer second.deinit();
    thread_pool.submit(&second.work);
    second.started.wait();
}

test "ev.ThreadPool: concurrent blocking work gets a worker each" {
    if (builtin.single_threaded) return error.SkipZigTest;

    var thread_pool: ev.ThreadPool = undefined;
    try thread_pool.init(std.testing.allocator, .{
        .min_threads = 0,
        .max_threads = 10,
    });
    defer thread_pool.deinit();
    defer thread_pool.stop();

    var gate = ResetEvent.init();
    defer gate.deinit();
    defer gate.set();

    var jobs: [10]GatedJob = undefined;
    for (&jobs) |*job| job.init(&gate);
    defer for (&jobs) |*job| job.deinit();

    for (&jobs) |*job| thread_pool.submit(&job.work);
    for (&jobs) |*job| job.started.wait();
}

test "ev.ThreadPool: an idle worker is counted for one queued job only" {
    if (builtin.single_threaded) return error.SkipZigTest;

    var thread_pool: ev.ThreadPool = undefined;
    try thread_pool.init(std.testing.allocator, .{
        .min_threads = 1,
        .max_threads = 4,
    });
    defer thread_pool.deinit();
    defer thread_pool.stop();

    var spins: usize = 0;
    while (thread_pool.idle_threads.load(.monotonic) < 1) {
        spins += 1;
        if (spins > 100_000_000) return error.TimedOut;
        os_thread.yield();
    }

    var gate = ResetEvent.init();
    defer gate.deinit();
    defer gate.set();

    // Both are submitted before the parked worker wakes, so both submits see it
    // as idle. Only the first may count on it; the second needs a new worker.
    var first: GatedJob = undefined;
    first.init(&gate);
    defer first.deinit();
    var second: GatedJob = undefined;
    second.init(&gate);
    defer second.deinit();
    thread_pool.submit(&first.work);
    thread_pool.submit(&second.work);

    first.started.wait();
    second.started.wait();
}

/// KiB of writable anonymous memory mapped in this process (the "rw" lines
/// with no file in /proc/self/maps). A pthread's stack is all writable and
/// stays mapped from its spawn until it is joined or detached, even after
/// the thread exits. The C library's malloc arenas, which it keeps on purpose
/// and adds under contention, reserve 64 MiB each but make only a small part
/// writable, so they barely move this count.
fn writableAnonKiB() !usize {
    const fd = std.c.open("/proc/self/maps", .{ .ACCMODE = .RDONLY });
    if (fd < 0) return error.OpenFailed;
    defer _ = std.c.close(fd);
    var buf: [4096]u8 = undefined;
    var line: [512]u8 = undefined;
    var line_len: usize = 0;
    var total: usize = 0;
    while (true) {
        const n = std.c.read(fd, &buf, buf.len);
        if (n < 0) return error.ReadFailed;
        if (n == 0) break;
        for (buf[0..@intCast(n)]) |c| {
            if (c != '\n') {
                if (line_len == line.len) return error.LineTooLong;
                line[line_len] = c;
                line_len += 1;
                continue;
            }
            total += try writableAnonLineKiB(line[0..line_len]);
            line_len = 0;
        }
    }
    return total;
}

/// One /proc/self/maps line: "start-end perms offset dev inode [path]".
fn writableAnonLineKiB(line: []const u8) !usize {
    var fields = std.mem.tokenizeScalar(u8, line, ' ');
    const range = fields.next() orelse return error.BadMaps;
    const perms = fields.next() orelse return error.BadMaps;
    _ = fields.next() orelse return error.BadMaps; // offset
    _ = fields.next() orelse return error.BadMaps; // dev
    const inode = fields.next() orelse return error.BadMaps;
    const path = fields.rest();
    if (!std.mem.startsWith(u8, perms, "rw")) return 0;
    if (!std.mem.eql(u8, inode, "0")) return 0;
    // [stack], [heap] and other named regions are not thread stacks.
    if (path.len != 0) return 0;
    const dash = std.mem.indexOfScalar(u8, range, '-') orelse return error.BadMaps;
    const start = try std.fmt.parseInt(usize, range[0..dash], 16);
    const end = try std.fmt.parseInt(usize, range[dash + 1 ..], 16);
    return (end - start) / 1024;
}

test "ev.ThreadPool: a worker that retires after its idle timeout is joined, not leaked" {
    // Linux only: the leak is observed through /proc/self/maps. A worker
    // thread that exits and is never joined or detached keeps its stack
    // mapped for the life of the process (and ThreadSanitizer reports a
    // "thread leak"), so each spawn-and-retire cycle adds a stack of
    // writable memory. A joined worker's stack goes back to the C library,
    // and the next spawn reuses it.
    if (builtin.single_threaded or builtin.os.tag != .linux) return error.SkipZigTest;

    var thread_pool: ev.ThreadPool = undefined;
    try thread_pool.init(std.testing.allocator, .{
        .min_threads = 0,
        .max_threads = 1,
        .idle_timeout_ms = 1,
    });
    defer thread_pool.deinit();
    defer thread_pool.stop();

    var open_gate = ResetEvent.init();
    defer open_gate.deinit();
    open_gate.set();

    const cycles = 32;
    var before: usize = 0;
    for (0..cycles + 1) |i| {
        var job: GatedJob = undefined;
        job.init(&open_gate);
        defer job.deinit();
        thread_pool.submit(&job.work);
        job.started.wait();
        // Wait until the worker retired: it left the running count and the
        // worker list. Each cycle then spawns a new worker.
        var spins: usize = 0;
        while (true) {
            thread_pool.workers_mutex.lock();
            const listed = thread_pool.workers.items.len;
            thread_pool.workers_mutex.unlock();
            if (listed == 0 and thread_pool.running_threads.load(.monotonic) == 0) break;
            spins += 1;
            if (spins > 100_000_000) return error.TimedOut;
            os_thread.yield();
        }
        // The first cycle warms the C library's stack cache; measure from there.
        if (i == 0) before = try writableAnonKiB();
    }
    const after = try writableAnonKiB();
    // Leaked: one stack per cycle. Joined: a stack or two stay mapped (the
    // last retired worker, which the next retirement or stop() joins, and
    // the cached one). Allow a quarter of the cycles.
    const stack_kib = std.Thread.SpawnConfig.default_stack_size / 1024;
    if (after > before + (cycles / 4) * stack_kib) {
        std.debug.print("ev.ThreadPool: {d} spawn-and-retire cycles grew writable anonymous memory from {d} KiB to {d} KiB ({d} stacks of {d} KiB)\n", .{ cycles, before, after, (after - before) / stack_kib, stack_kib });
        return error.RetiredWorkersLeaked;
    }
}

/// Run `n_workers` blocking jobs at once on a new pool (one worker each), let
/// every worker go idle, then stop the pool, so all of them wake and exit
/// together at `stop()`.
fn runAllThenStop(comptime n_workers: usize) !void {
    var thread_pool: ev.ThreadPool = undefined;
    try thread_pool.init(std.testing.allocator, .{
        .min_threads = 0,
        .max_threads = n_workers,
    });
    defer thread_pool.deinit();

    var gate = ResetEvent.init();
    defer gate.deinit();
    var jobs: [n_workers]GatedJob = undefined;
    for (&jobs) |*job| job.init(&gate);
    defer for (&jobs) |*job| job.deinit();
    for (&jobs) |*job| thread_pool.submit(&job.work);
    for (&jobs) |*job| job.started.wait();
    gate.set();
    var spins: usize = 0;
    while (thread_pool.idle_threads.load(.monotonic) < n_workers) {
        spins += 1;
        if (spins > 100_000_000) return error.TimedOut;
        os_thread.yield();
    }
    thread_pool.stop();
}

test "ev.ThreadPool: stop() joins every worker, including one that exits before stop() reaches it" {
    // Linux only, as above. At stop() every worker wakes, leaves its loop and
    // exits; stop() must join each one whatever the order. A worker that
    // took itself out of the pool's bookkeeping before stop() read it was
    // never joined, and kept its stack.
    if (builtin.single_threaded or builtin.os.tag != .linux) return error.SkipZigTest;

    const n_workers = 8;
    // The first round warms what the C library keeps on purpose (its stack
    // cache, malloc arenas for concurrent threads). Measure the second round
    // only.
    try runAllThenStop(n_workers);
    const before = try writableAnonKiB();
    try runAllThenStop(n_workers);
    const after = try writableAnonKiB();
    // Joined: the second round reuses the cached stacks and unmaps the
    // rest, so the count does not grow by a stack. Leaked: each worker
    // stop() missed keeps its whole stack. Half a stack separates the two.
    const stack_kib = std.Thread.SpawnConfig.default_stack_size / 1024;
    if (after > before + stack_kib / 2) {
        std.debug.print("ev.ThreadPool: a second round of stop() with {d} workers grew writable anonymous memory from {d} KiB to {d} KiB ({d} stacks of {d} KiB)\n", .{ n_workers, before, after, (after - before) / stack_kib, stack_kib });
        return error.StoppedWorkersLeaked;
    }
}
