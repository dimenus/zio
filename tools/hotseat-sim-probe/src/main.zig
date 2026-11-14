//! Probes for t-4083: what zio's simulator (`zio_options.sim`) does with each
//! thing hotseat's reconnect tests use. One probe per process, because a probe
//! of a refused operation ends in a panic.
//!
//!   cd tools/hotseat-sim-probe
//!   zig build run -- <probe> [seed]
//!
//! Probes that should finish print `RESULT ...` lines and exit 0. Probes of a
//! refused operation print `PROBE <name>: calling ...` and then zio's panic.
//! A probe that should panic and does not prints `UNEXPECTED: returned`.

const std = @import("std");
const zio = @import("zio");

/// What a second user declares: sim on, and a scheduling the simulator can
/// run (it refuses `.work_stealing` at compile time).
pub const zio_options: zio.Options = .{ .sim = true, .scheduling = .pinned };

extern fn probe_c_monotonic_ms() u64;
extern fn probe_c_virtual_ms() u64;
extern fn probe_c_sleep_ms(ms: u32) void;

/// The one route for foreign code to the virtual clock: a Zig export that
/// reads zio's clock. Under sim `zio.Timestamp.now` reads the logical clock.
export fn probe_zio_awake_ms() u64 {
    return @intCast(@divTrunc(zio.Timestamp.now(.awake).toNanoseconds(), std.time.ns_per_ms));
}

const Probe = enum {
    timers,
    pipe_bytes,
    executor_off,
    udp_bind,
    tcp_connect,
    udp_sendto_pipe,
    netpoll_raw_udp,
    file_open,
    dns,
    spawn_blocking,
    futex_main,
    futex_foreign_wake,
    foreign_clock,
    foreign_event,
    foreign_event_no_timeout,
    foreign_mutex,
    signal,
    signal_parked,
    foreign_event_no_timeout_no_shrink,
};

/// Set by `foreign_event_no_timeout_no_shrink`: no stack-pool shrink timer,
/// the setting starh2's dst-conn uses. That timer repeats every 60 s.
var no_shrink_timer = false;

fn startRuntime(gpa: std.mem.Allocator, seed: u64, executors: u8, main_executor: bool) !*zio.Runtime {
    zio.sim.begin(seed);
    var o: zio.RuntimeOptions = .{
        .executors = .exact(executors),
        .enable_main_executor = main_executor,
        .thread_pool = .{ .max_threads = 0 },
    };
    if (no_shrink_timer) o.stack_pool.shrink_interval = .zero;
    return zio.Runtime.init(gpa, o);
}

fn finish(rt: *zio.Runtime) void {
    std.debug.print("RESULT virtual_ns={d} trace={x:0>16} events={d}\n", .{ zio.sim.clockNs(), zio.sim.traceHash(), zio.sim.eventCount() });
    rt.deinit();
    zio.sim.end();
}

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, gpa);
    defer args.deinit();
    _ = args.next();
    const name = args.next() orelse return usage();
    const probe = std.meta.stringToEnum(Probe, name) orelse return usage();
    const seed: u64 = if (args.next()) |s| try std.fmt.parseInt(u64, s, 10) else 1;
    std.debug.print("PROBE {s} seed={d}\n", .{ name, seed });

    switch (probe) {
        .timers => try timers(gpa, seed),
        .pipe_bytes => try pipeBytes(gpa, seed),
        .executor_off => {
            std.debug.print("PROBE executor_off: calling Runtime.init(executors=1, enable_main_executor=false), App.create's setup\n", .{});
            const rt = try startRuntime(gpa, seed, 1, false);
            finish(rt);
            std.debug.print("UNEXPECTED: returned\n", .{});
        },
        .udp_bind, .tcp_connect, .udp_sendto_pipe, .netpoll_raw_udp, .file_open, .dns, .spawn_blocking, .futex_main, .signal => {
            const rt = try startRuntime(gpa, seed, 1, true);
            try refused(rt, probe);
            finish(rt);
        },
        .futex_foreign_wake => try futexForeignWake(gpa, seed),
        .foreign_clock => try foreignClock(gpa, seed),
        .foreign_event => try foreignEvent(gpa, seed, true),
        .foreign_event_no_timeout => try foreignEvent(gpa, seed, false),
        .foreign_mutex => try foreignMutex(gpa, seed),
        .signal_parked => try signalParked(gpa, seed),
        .foreign_event_no_timeout_no_shrink => {
            no_shrink_timer = true;
            try foreignEvent(gpa, seed, false);
        },
    }
}

fn usage() void {
    std.debug.print("usage: hotseat-sim-probe <probe> [seed]; probes:", .{});
    inline for (@typeInfo(Probe).@"enum".field_names) |f| std.debug.print(" {s}", .{f});
    std.debug.print("\n", .{});
    std.process.exit(2);
}

// ---- timers: the reconnect test's waits, on the virtual clock ----

const Log = struct {
    buf: [32]u8 = undefined,
    n: usize = 0,
    fn add(self: *Log, c: u8) void {
        self.buf[self.n] = c;
        self.n += 1;
    }
};

fn sleeper(log: *Log, id: u8, ms: u64) !void {
    try zio.sleep(.fromMilliseconds(ms));
    log.add(id);
}

/// hotseat's reconnect backoff shape: 250 ms, 500 ms, 1 s, 2 s.
fn backoff(log: *Log) !void {
    for ([_]u64{ 250, 500, 1000, 2000 }) |ms| try zio.sleep(.fromMilliseconds(ms));
    log.add('K');
}

/// A 2 s stream check that nobody answers: the Event times out.
fn streamCheck(log: *Log, ev: *zio.Event) !void {
    ev.waitTimeout(.fromNanoseconds(2 * std.time.ns_per_s)) catch |err| switch (err) {
        error.Timeout => return log.add('T'),
        else => return err,
    };
    log.add('E');
}

fn producer(ch: *zio.Channel(u8)) !void {
    for (0..3) |i| {
        try zio.sleep(.fromMilliseconds(100));
        try ch.send(@intCast('0' + i));
    }
}

fn consumer(log: *Log, ch: *zio.Channel(u8)) !void {
    for (0..3) |_| log.add(try ch.receive());
}

fn timers(gpa: std.mem.Allocator, seed: u64) !void {
    const real0 = probe_c_monotonic_ms();
    const rt = try startRuntime(gpa, seed, 1, true);
    var log: Log = .{};
    var ev: zio.Event = .init;
    var ch_buf: [4]u8 = undefined;
    var ch = zio.Channel(u8).init(&ch_buf);
    // Two tasks with the same deadline: the seed picks their order.
    var a = try rt.spawn(sleeper, .{ &log, 'A', 1000 });
    var b = try rt.spawn(sleeper, .{ &log, 'B', 1000 });
    // The 3 s link watch.
    var w = try rt.spawn(sleeper, .{ &log, 'W', 3000 });
    var k = try rt.spawn(backoff, .{&log});
    var t = try rt.spawn(streamCheck, .{ &log, &ev });
    var p = try rt.spawn(producer, .{&ch});
    var c = try rt.spawn(consumer, .{ &log, &ch });
    try a.join();
    try b.join();
    try w.join();
    try k.join();
    try t.join();
    try p.join();
    try c.join();
    const real_ms = probe_c_monotonic_ms() - real0;
    std.debug.print("RESULT order={s} real_ms={d}\n", .{ log.buf[0..log.n], real_ms });
    finish(rt);
}

// ---- pipe_bytes: a sim pipe is a byte stream, not datagrams ----

fn pipeBytes(gpa: std.mem.Allocator, seed: u64) !void {
    const rt = try startRuntime(gpa, seed, 1, true);
    const fds = zio.sim.pipePair();
    const a: zio.net.Socket = .{ .handle = fds[0], .address = undefined };
    const b: zio.net.Socket = .{ .handle = fds[1], .address = undefined };
    _ = try a.send("abc", .none);
    _ = try a.send("def", .none);
    try zio.sleep(.fromMilliseconds(10));
    var buf: [64]u8 = undefined;
    const n = try b.receive(&buf, .none);
    std.debug.print("RESULT two sends of 3 bytes, one receive got {d} bytes: \"{s}\"\n", .{ n, buf[0..n] });
    finish(rt);
}

// ---- operations the simulator refuses ----

fn refused(rt: *zio.Runtime, probe: Probe) !void {
    _ = rt;
    switch (probe) {
        .udp_bind => {
            std.debug.print("PROBE udp_bind: calling zio.net.IpAddress.bind (UDP socket open)\n", .{});
            const addr = try zio.net.IpAddress.parseIp4("127.0.0.1", 0);
            var s = try addr.bind(.{});
            s.close();
        },
        .tcp_connect => {
            std.debug.print("PROBE tcp_connect: calling zio.net.IpAddress.connect (the app's HTTPS to the stand-in host)\n", .{});
            const addr = try zio.net.IpAddress.parseIp4("127.0.0.1", 1);
            var s = try addr.connect(.{});
            s.close();
        },
        .udp_sendto_pipe => {
            std.debug.print("PROBE udp_sendto_pipe: calling Socket.sendTo on a sim pipe fd\n", .{});
            const fds = zio.sim.pipePair();
            const s: zio.net.Socket = .{ .handle = fds[0], .address = undefined };
            const to: zio.net.Address = .{ .ip = try zio.net.IpAddress.parseIp4("127.0.0.1", 9) };
            _ = try s.sendTo(to, "x", .none);
        },
        .netpoll_raw_udp => {
            // ENet opens its own UDP socket in C; hotseat waits for it with ev.NetPoll.
            const fd = std.c.socket(std.c.AF.INET, std.c.SOCK.DGRAM, 0);
            if (fd < 0) return error.SocketFailed;
            std.debug.print("PROBE netpoll_raw_udp: real UDP fd {d} from socket(2); calling ev.NetPoll(.recv) on it, as enet_control.zig does\n", .{fd});
            var cq: zio.CompletionQueue = .init();
            var poll = zio.ev.NetPoll.init(fd, .recv);
            try cq.submit(&poll.c);
            _ = try cq.wait();
        },
        .file_open => {
            std.debug.print("PROBE file_open: calling std.Io.Dir.cwd().openFile through the runtime's io\n", .{});
            const io = zio.getCurrentExecutor().runtime.io();
            var f = try std.Io.Dir.cwd().openFile(io, "build.zig", .{});
            f.close(io);
        },
        .dns => {
            std.debug.print("PROBE dns: calling zio.net.HostName.connect(\"localhost\")\n", .{});
            const h: zio.net.HostName = .{ .bytes = "localhost" };
            var s = try h.connect(1, .{});
            s.close();
        },
        .spawn_blocking => {
            std.debug.print("PROBE spawn_blocking: calling zio.spawnBlocking (video_decode.zig joins its thread this way)\n", .{});
            var h = try zio.spawnBlocking(noop, .{});
            h.join();
        },
        .futex_main => {
            std.debug.print("PROBE futex_main: calling zio.os.Futex.timedWait on the main thread (App.create, waitMail)\n", .{});
            var word: std.atomic.Value(u32) = .init(0);
            zio.os.Futex.timedWait(&word, 0, .fromMilliseconds(1)) catch {};
        },
        .signal => {
            std.debug.print("PROBE signal: Signal(.user1), raise(SIGUSR1), then wait\n", .{});
            var sig = try zio.Signal.init(.user1);
            defer sig.deinit();
            _ = std.c.raise(.USR1);
            try sig.waitTimeout(.fromNanoseconds(std.time.ns_per_s));
            std.debug.print("RESULT signal received\n", .{});
            return;
        },
        else => unreachable,
    }
    std.debug.print("UNEXPECTED: returned\n", .{});
}

fn noop() void {}

// ---- real OS threads next to the simulator ----

fn futexForeignWake(gpa: std.mem.Allocator, seed: u64) !void {
    const rt = try startRuntime(gpa, seed, 1, true);
    var word: std.atomic.Value(u32) = .init(0);
    std.debug.print("PROBE futex_foreign_wake: a std.Thread calls zio.os.Futex.wake (the decode thread's shape)\n", .{});
    const th = try std.Thread.spawn(.{}, struct {
        fn run(wd: *std.atomic.Value(u32)) void {
            zio.os.Futex.wake(wd, .all);
        }
    }.run, .{&word});
    th.join();
    std.debug.print("UNEXPECTED: returned\n", .{});
    finish(rt);
}

const ClockSeen = struct { zig_ms: u64 = 0, c_virtual_ms: u64 = 0, c_monotonic_ms: u64 = 0 };

fn foreignClock(gpa: std.mem.Allocator, seed: u64) !void {
    const rt = try startRuntime(gpa, seed, 1, true);
    try zio.sleep(.fromSeconds(5));
    var seen: ClockSeen = .{};
    const th = try std.Thread.spawn(.{}, struct {
        fn run(s: *ClockSeen) void {
            s.zig_ms = probe_zio_awake_ms();
            s.c_virtual_ms = probe_c_virtual_ms();
            s.c_monotonic_ms = probe_c_monotonic_ms();
        }
    }.run, .{&seen});
    th.join();
    std.debug.print("RESULT after a virtual 5 s sleep, read from a std.Thread: zio.Timestamp.now(.awake)={d} ms, C via Zig export={d} ms, C clock_gettime(MONOTONIC)={d} ms\n", .{ seen.zig_ms, seen.c_virtual_ms, seen.c_monotonic_ms });
    std.debug.print("RESULT same reads on the sim thread: zio={d} ms, C via Zig export={d} ms\n", .{ probe_zio_awake_ms(), probe_c_virtual_ms() });
    finish(rt);
}

const EventRig = struct { ev: zio.Event = .init, set_at_virtual_ms: u64 = 0 };

fn foreignSetter(r: *EventRig) void {
    probe_c_sleep_ms(50);
    r.set_at_virtual_ms = probe_zio_awake_ms();
    r.ev.set();
}

fn eventWaiter(r: *EventRig, timeout: bool) ![]const u8 {
    if (timeout) {
        r.ev.waitTimeout(.fromNanoseconds(10 * std.time.ns_per_s)) catch |err| switch (err) {
            error.Timeout => return "timed out",
            else => return err,
        };
    } else try r.ev.wait();
    return "event set";
}

/// The decode thread hands a frame to zio by setting an Event from a real
/// thread, 50 ms of real time after start. The task waits with a 10 s
/// timeout, or with none.
fn foreignEvent(gpa: std.mem.Allocator, seed: u64, timeout: bool) !void {
    const rt = try startRuntime(gpa, seed, 1, true);
    var r: EventRig = .{};
    const th = try std.Thread.spawn(.{}, foreignSetter, .{&r});
    var h = try rt.spawn(eventWaiter, .{ &r, timeout });
    const what = try h.join();
    const at = probe_zio_awake_ms();
    th.join();
    std.debug.print("RESULT waiter: {s} at virtual {d} ms; the thread set the event at virtual {d} ms\n", .{ what, at, r.set_at_virtual_ms });
    finish(rt);
}

const MutexRig = struct {
    m: zio.Mutex = .init,
    held: std.atomic.Value(bool) = .init(false),
    trying: std.atomic.Value(bool) = .init(false),
};

/// Holds the mutex until the thread has called lock, plus 20 ms of real time,
/// so the thread's lock is contended whatever the real scheduling.
fn holdMutex(r: *MutexRig) !void {
    r.m.lockUncancelable();
    r.held.store(true, .release);
    while (!r.trying.load(.acquire)) probe_c_sleep_ms(1);
    probe_c_sleep_ms(20);
    try zio.sleep(.fromSeconds(1));
    r.m.unlock();
}

fn contendMutex(r: *MutexRig) void {
    while (!r.held.load(.acquire)) probe_c_sleep_ms(1);
    r.trying.store(true, .release);
    r.m.lockUncancelable();
    r.m.unlock();
}

/// The main thread and the decode thread lock zio mutexes that tasks also hold.
fn foreignMutex(gpa: std.mem.Allocator, seed: u64) !void {
    const rt = try startRuntime(gpa, seed, 1, true);
    var r: MutexRig = .{};
    std.debug.print("PROBE foreign_mutex: a task holds a zio.Mutex; a std.Thread calls lockUncancelable while it is held\n", .{});
    const th = try std.Thread.spawn(.{}, contendMutex, .{&r});
    var h = try rt.spawn(holdMutex, .{&r});
    try h.join();
    th.join();
    std.debug.print("UNEXPECTED: returned\n", .{});
    finish(rt);
}

fn raiseLater() !void {
    try zio.sleep(.fromMilliseconds(500));
    _ = std.c.raise(.USR1);
}

fn waitSignal(sig: *zio.Signal) ![]const u8 {
    sig.waitTimeout(.fromNanoseconds(10 * std.time.ns_per_s)) catch |err| switch (err) {
        error.Timeout => return "timed out",
        else => return err,
    };
    return "received";
}

/// A task parked on a Signal; another task raises it at virtual 500 ms. The
/// handler runs on the raising thread, which is the sim thread.
fn signalParked(gpa: std.mem.Allocator, seed: u64) !void {
    const rt = try startRuntime(gpa, seed, 1, true);
    var sig = try zio.Signal.init(.user1);
    defer sig.deinit();
    var w = try rt.spawn(waitSignal, .{&sig});
    var r = try rt.spawn(raiseLater, .{});
    try r.join();
    const what = try w.join();
    std.debug.print("RESULT parked waiter: {s} at virtual {d} ms\n", .{ what, probe_zio_awake_ms() });
    finish(rt);
}
