// SPDX-FileCopyrightText: 2025 Lukáš Lalinský
// SPDX-License-Identifier: MIT
//
// Tests of the sim's stream sockets, slice 1 of design section 17.16:
// sockets, ports, the handshake, listeners, accept and close with no bytes.
// The protocol they test is the comment above `pub const StreamProfile` in
// sim.zig.
//
// How they run:
// - Each scenario is one sim run (`run`) with one executor, through zio's
//   public API (`zio.net`, or std.Io on zio's vtable) where a program would
//   use it. Where a case needs two ops woken in one harvest batch, the test
//   adds ev ops to the executor's loop itself, with callbacks that run
//   inside the harvest (`defer_callback = false`).
// - `L` is the wire latency, 100 µs. With no fault, a SYN sent at t lands at
//   t + L, its SYN-ACK at t + 2L (the connect completes), its ACK at t + 3L
//   (an accept completes). The tests check those times exactly.
// - Every scenario ends by closing its sockets and draining the stream wire
//   (`drain`), so `sim.end`'s teardown check runs on every one. A body that
//   fails prints its error before `end` can panic on what it left open.
// - Refusals run in a child process (`sim.expectChildPanics`) and match the
//   panic's own line.
// - The surface table (`surface table` test) has one row per ev op tag, per
//   `net*` field of the std.Io vtable, and per line of design 17.6's and
//   17.8's decision lists. A row of a later slice is pending: it has no
//   check, is printed, and is never counted as passed.
// - The parity table (`sim: stream parity`) runs rows 2, 12, 14, 31 and 32
//   of design 17.13 through zio's API: in a sim build in a sim run with the
//   host kernel's profile, in a build without sim on real loopback sockets,
//   with one expected column per kernel.

const std = @import("std");
const builtin = @import("builtin");
const sim = @import("sim.zig");
const rt_mod = @import("runtime.zig");
const zio_net = @import("net.zig");
const ev = @import("ev/root.zig");
const io_common = @import("common.zig");
const os_net = @import("os/net.zig");
const Timeout = @import("time.zig").Timeout;

const L = sim.wire_latency_ns;
const lo: [4]u8 = .{ 127, 0, 0, 1 };
const any: [4]u8 = .{ 0, 0, 0, 0 };
const skip = !sim.compiled_in or builtin.os.tag == .windows;
const expect = std.testing.expect;
const expectEqual = std.testing.expectEqual;
const expectError = std.testing.expectError;

/// Runs `body` as the one task of a fresh sim run.
fn run(seed: u64, profile: ?sim.StreamProfile, comptime body: fn () anyerror!void) !void {
    sim.begin(seed);
    defer sim.end();
    if (profile) |p| sim.setStreamProfile(p);
    var o: rt_mod.RuntimeOptions = .{
        .executors = .exact(1),
        .thread_pool = .{ .max_threads = 0 },
    };
    o.stack_pool.shrink_interval = .zero;
    const rt = try rt_mod.Runtime.init(std.heap.page_allocator, o);
    defer rt.deinit();
    runtime = rt;
    var h = try rt.spawn(body, .{});
    h.join() catch |err| {
        std.debug.print("stream test body failed: {t} (seed {d})\n", .{ err, seed });
        return err;
    };
}

var runtime: *rt_mod.Runtime = undefined;

/// Sleeps in virtual time while TCP entries are in flight: the protocol's
/// rule before `sim.end`.
fn drain() !void {
    var n: u32 = 0;
    while (sim.streamEntriesInFlight() != 0) : (n += 1) {
        if (n > 1000) return error.StreamWireNeverDrained;
        try rt_mod.sleep(.fromMicroseconds(100));
    }
}

fn listen(ip: [4]u8, port: u16, opts: zio_net.IpAddress.ListenOptions) !zio_net.Server {
    return zio_net.IpAddress.initIp4(ip, port).listen(opts);
}

fn connect(ip: [4]u8, port: u16, timeout: Timeout) !zio_net.Stream {
    return zio_net.IpAddress.initIp4(ip, port).connect(.{ .timeout = timeout });
}

fn portOf(s: zio_net.Server) u16 {
    return s.socket.address.ip.getPort();
}

fn localOf(fd: i32) !sim.Ip4 {
    var sa: os_net.sockaddr.in = undefined;
    var len: os_net.socklen_t = @sizeOf(os_net.sockaddr.in);
    try os_net.getsockname(fd, @ptrCast(&sa), &len);
    return .{ .ip = @bitCast(sa.addr), .port = std.mem.bigToNative(u16, sa.port) };
}

fn sockaddr(ip: [4]u8, port: u16) os_net.sockaddr.in {
    return .{ .port = std.mem.nativeToBig(u16, port), .addr = @bitCast(ip) };
}

fn evOpen() !i32 {
    var op = ev.NetOpen.init(.ipv4, .stream, .ip, .{ .nonblocking = true });
    try io_common.waitForIo(&op.c);
    return op.getResult();
}

fn evClose(fd: i32) void {
    var op = ev.NetClose.init(fd);
    io_common.waitForIoUncancelable(&op.c);
    op.getResult() catch {};
}

fn noop(_: *ev.Loop, _: *ev.Completion) void {}

// ---- The handshake and close, with no fault ----

const handshake = struct {
    var server: zio_net.Server = undefined;
    var accepted: ?zio_net.Stream = null;
    var accepted_at: u64 = 0;

    fn acceptor() !void {
        accepted = try server.accept(.{});
        accepted_at = sim.clockNs();
    }

    fn body() !void {
        accepted = null;
        server = try listen(lo, 0, .{});
        // The first TCP port of a run, apart from UDP's counter.
        try expectEqual(@as(u16, 49152), portOf(server));
        var h = try rt_mod.spawn(acceptor, .{});
        const t0 = sim.clockNs();
        const c = try connect(lo, 49152, .none);
        // The SYN-ACK lands two latencies after the SYN left.
        try expectEqual(t0 + 2 * L, sim.clockNs());
        try h.join();
        try expectEqual(t0 + 3 * L, accepted_at);
        const s = accepted.?;
        const cl = try localOf(c.socket.handle);
        try expectEqual(sim.Ip4{ .ip = lo, .port = 49153 }, cl);
        // accept writes the peer: the client's local address.
        try expectEqual(cl.port, s.socket.address.ip.getPort());
        try expectEqual(sim.Ip4{ .ip = lo, .port = 49152 }, try localOf(s.socket.handle));
        // TCP_NODELAY: set on the listener by `listen`, inherited by the
        // accepted socket; set by `connect` on the client.
        try expect(try s.socket.getNoDelay());
        try expect(try c.socket.getNoDelay());
        const cid = sim.testStreamEnd(c.socket.handle).conn_id;
        try expectEqual(@as(u32, 1), cid);
        try expectEqual(sim.StreamPhase.est, sim.testStreamEnd(s.socket.handle).phase);
        // The client closes first: its FIN lands one latency later, and it
        // is the active closer.
        c.close();
        try rt_mod.sleep(.fromMicroseconds(2 * L / 1000));
        try expect(sim.testStreamEnd(s.socket.handle).fin_in);
        try expectEqual(sim.StreamPhase.dead, sim.testConnEnd(cid, false).?.phase);
        s.close();
        server.close();
        try drain();
        try expectEqual(@as(usize, 1), sim.streamReservations());
        // The server's FIN landed at the dead client.
        try expectEqual(sim.StreamStats{ .dropped_dead = 1, .retired = 1 }, sim.streamStats());
        try expectEqual(@as(usize, 0), sim.testConnsLive());
    }
};

test "sim: stream: connect completes when the SYN-ACK lands (2L) and accept when the ACK lands (3L), with ports, peer address and NODELAY" {
    if (comptime skip) return error.SkipZigTest;
    try run(1, null, handshake.body);
}

const both_close = struct {
    fn body() !void {
        const server = try listen(lo, 0, .{});
        const c = try connect(lo, portOf(server), .none);
        const s = try server.accept(.{});
        // Both close at one instant: both send a FIN before one landed, so
        // both are active closers (simultaneous close).
        c.close();
        s.close();
        server.close();
        try drain();
        try expectEqual(@as(usize, 2), sim.streamReservations());
        try expectEqual(sim.StreamStats{ .dropped_dead = 2, .retired = 1 }, sim.streamStats());
    }
};

test "sim: stream: both ends closed at one instant are both active closers, and both FINs land at dead endpoints" {
    if (comptime skip) return error.SkipZigTest;
    try run(1, null, both_close.body);
}

const refused = struct {
    fn body() !void {
        const server = try listen(lo, 0, .{});
        const port = portOf(server);
        // Row 2's shape: the listener closed, its port is free.
        server.close();
        const t0 = sim.clockNs();
        try expectError(error.ConnectionRefused, connect(lo, port, .none));
        try expectEqual(t0 + 2 * L, sim.clockNs());
        // The wrapper closed the client; the server endpoint was never
        // made (absent), so the connection is retired already (review 3,
        // finding 1).
        try expectEqual(@as(usize, 0), sim.testConnsLive());
        try drain();
        try expectEqual(sim.StreamStats{ .refused = 1, .retired = 1 }, sim.streamStats());
        try expectEqual(@as(usize, 0), sim.streamReservations());
    }
};

test "sim: stream: a connect to a port with no listener is ConnectionRefused at 2L, and the connection retires with its server endpoint absent" {
    if (comptime skip) return error.SkipZigTest;
    try run(1, null, refused.body);
}

const cancel_before_syn = struct {
    fn body() !void {
        const server = try listen(lo, 0, .{});
        // The deadline comes before the SYN lands.
        try expectError(error.Timeout, connect(lo, portOf(server), .fromMicroseconds(L / 2000)));
        // The client is dead and its SYN still on the wire.
        try expectEqual(@as(usize, 1), sim.streamEntriesInFlight());
        try expectEqual(@as(usize, 1), sim.testConnsLive());
        try drain();
        // The SYN was dropped at landing and made no server state.
        try expectEqual(@as(usize, 0), sim.testListenerCount(server.socket.handle));
        try expectEqual(sim.StreamStats{ .syn_dropped_dead = 1, .retired = 1 }, sim.streamStats());
        try expectError(error.Timeout, server.accept(.{ .timeout = .fromMilliseconds(1) }));
        server.close();
    }
};

test "sim: stream: a connect cancelled before its SYN lands: the SYN is dropped at landing, no server state, and the connection retires" {
    if (comptime skip) return error.SkipZigTest;
    try run(1, null, cancel_before_syn.body);
}

const cancel_after_syn = struct {
    var second_at: u64 = 0;

    fn body() !void {
        // Backlog 1: the provisional endpoint takes the only place.
        const server = try listen(lo, 0, .{ .kernel_backlog = 1 });
        const port = portOf(server);
        try expectError(error.Timeout, connect(lo, port, .fromMicroseconds(3 * L / 2000)));
        // The SYN landed at L: one provisional endpoint.
        try expectEqual(@as(usize, 1), sim.testListenerCount(server.socket.handle));
        // At 2L the SYN-ACK lands at the dead client, which answers with an
        // RST; at 3L the RST kills the provisional endpoint (review 3,
        // finding 2), which frees the place.
        try rt_mod.sleep(.fromMicroseconds(3 * L / 2000));
        try expectEqual(@as(u64, 3 * L), sim.clockNs());
        try expectEqual(@as(usize, 0), sim.testListenerCount(server.socket.handle));
        try expectEqual(@as(u64, 1), sim.streamStats().dropped_dead);
        // A second connect fits the backlog of 1 only because the place was
        // freed; at a full listener its SYN would panic.
        const c = try connect(lo, port, .none);
        const s = try server.accept(.{});
        try expectEqual((try localOf(c.socket.handle)).port, s.socket.address.ip.getPort());
        c.close();
        s.close();
        server.close();
        try drain();
        try expectEqual(@as(u64, 2), sim.streamStats().retired);
    }
};

test "sim: stream: a connect cancelled after its SYN lands: the SYN-ACK is answered with an RST that kills the provisional endpoint and frees its listener's place" {
    if (comptime skip) return error.SkipZigTest;
    try run(1, null, cancel_after_syn.body);
}

const accepted_after_close = struct {
    fn body() !void {
        const server = try listen(lo, 0, .{});
        const c = try connect(lo, portOf(server), .none);
        // Closed between 2L and 3L: the connection is still accepted (rows
        // 26 and 27: queue ownership does not depend on the client).
        c.close();
        try rt_mod.sleep(.fromMilliseconds(1));
        const s = try server.accept(.{});
        const e = sim.testStreamEnd(s.socket.handle);
        try expectEqual(sim.StreamPhase.est, e.phase);
        try expect(e.fin_in);
        s.close();
        server.close();
        try drain();
        // Only the client was an active closer.
        try expectEqual(@as(usize, 1), sim.streamReservations());
    }
};

test "sim: stream: a client that closes before accept leaves a queued connection that accept returns with its FIN landed" {
    if (comptime skip) return error.SkipZigTest;
    try run(1, null, accepted_after_close.body);
}

// ---- Listener close ----

const listener_close = struct {
    var third: ?zio_net.Stream = null;
    var timed_out: bool = false;
    var port: u16 = 0;

    fn lateConnect() !void {
        third = try connect(lo, port, .none);
    }

    fn deadConnect() !void {
        if (connect(lo, port, .fromMicroseconds(3 * L / 2000))) |s| {
            s.close();
            return error.ConnectDidNotTimeOut;
        } else |err| {
            if (err != error.Timeout) return err;
            timed_out = true;
        }
    }

    fn body() !void {
        third = null;
        timed_out = false;
        const server = try listen(lo, 0, .{ .kernel_backlog = 8 });
        port = portOf(server);
        const a = try connect(lo, port, .none);
        const b = try connect(lo, port, .none);
        try rt_mod.sleep(.fromMicroseconds(L / 1000));
        // Two queued (their ACKs landed), and one accepted.
        const kept = try connect(lo, port, .none);
        const s_kept = try server.accept(.{});
        try expectEqual((try localOf(a.socket.handle)).port, s_kept.socket.address.ip.getPort());
        try rt_mod.sleep(.fromMicroseconds(L / 1000));
        // Now: b and kept queued. A third connect whose SYN lands, and a
        // fourth whose client is dead before the listener closes.
        const t0 = sim.clockNs();
        var h3 = try rt_mod.spawn(lateConnect, .{});
        var h4 = try rt_mod.spawn(deadConnect, .{});
        try rt_mod.sleep(.fromMicroseconds(16 * L / 10000));
        try expectEqual(t0 + 16 * L / 10, sim.clockNs());
        try expect(timed_out);
        try expectEqual(@as(usize, 4), sim.testListenerCount(server.socket.handle));
        server.close();
        try h3.join();
        try h4.join();
        try drain();
        // The queued two and the provisional one (whose SYN-ACK still
        // landed first) got the listener's RST. The accepted one did not
        // (row 29).
        for ([_]zio_net.Stream{ b, kept, third.? }) |x| {
            const e = sim.testStreamEnd(x.socket.handle);
            try expectEqual(sim.StreamPhase.est, e.phase);
            try expect(e.reset_received);
        }
        try expect(!sim.testStreamEnd(s_kept.socket.handle).reset_received);
        try expect(!sim.testStreamEnd(a.socket.handle).reset_received);
        // At dead endpoints: the third's ACK; the fourth's SYN-ACK (whose
        // RST back landed at a dead endpoint) and the listener's RST to it.
        try expectEqual(@as(u64, 4), sim.streamStats().dropped_dead);
        // Closes after a reset put nothing on the wire.
        for ([_]zio_net.Stream{ b, kept, third.? }) |x| x.close();
        try expectEqual(@as(usize, 0), sim.streamEntriesInFlight());
        a.close();
        s_kept.close();
        try drain();
        try expectEqual(@as(usize, 0), sim.testConnsLive());
    }
};

test "sim: stream: closing a listener resets its queued and provisional connections, leaves an accepted one alone, and an RST to a dead client is dropped" {
    if (comptime skip) return error.SkipZigTest;
    try run(1, null, listener_close.body);
}

const close_wakes_accept = struct {
    var server: zio_net.Server = undefined;
    var result: anyerror!void = {};

    fn acceptor() void {
        if (server.accept(.{})) |s| {
            s.close();
            result = error.AcceptedFromAClosedListener;
        } else |err| result = err;
    }

    fn body() !void {
        server = try listen(lo, 0, .{});
        var h = try rt_mod.spawn(acceptor, .{});
        try rt_mod.sleep(.fromMicroseconds(10));
        server.close();
        h.join();
        try expectError(error.FileDescriptorNotASocket, result);
    }
};

test "sim: stream: closing a listener ends an accept parked on it FileDescriptorNotASocket" {
    if (comptime skip) return error.SkipZigTest;
    try run(1, null, close_wakes_accept.body);
}

// ---- Two connects at one instant, in both creation orders ----

const equal_time = struct {
    var swap = false;
    var cs: [2]ev.NetConnect = undefined;
    var to: os_net.sockaddr.in = undefined;

    fn body() !void {
        const server = try listen(lo, 0, .{});
        to = sockaddr(lo, portOf(server));
        const fds = [2]i32{ try evOpen(), try evOpen() };
        const loop = &rt_mod.getCurrentExecutor().loop;
        // Both submitted at one virtual time; `swap` submits the higher fd
        // first. Their SYNs land at one instant.
        const order = if (swap) [2]usize{ 1, 0 } else [2]usize{ 0, 1 };
        for (order) |k| {
            cs[k] = ev.NetConnect.init(fds[k], @ptrCast(&to), @sizeOf(os_net.sockaddr.in));
            cs[k].c.callback = noop;
            cs[k].c.flags.defer_callback = false;
            loop.add(&cs[k].c);
        }
        try rt_mod.sleep(.fromMilliseconds(1));
        for (cs) |c| try c.getResult();
        // conn_id follows submission, and the queue follows conn_id.
        try expectEqual(@as(u32, 1), sim.testStreamEnd(fds[order[0]]).conn_id);
        const s1 = try server.accept(.{});
        const s2 = try server.accept(.{});
        try expectEqual((try localOf(fds[order[0]])).port, s1.socket.address.ip.getPort());
        try expectEqual((try localOf(fds[order[1]])).port, s2.socket.address.ip.getPort());
        for (fds) |fd| evClose(fd);
        s1.close();
        s2.close();
        server.close();
        try drain();
    }
};

test "sim: stream: two connects at one instant are queued in submission order, in both creation orders" {
    if (comptime skip) return error.SkipZigTest;
    for ([_]bool{ false, true }) |sw| {
        equal_time.swap = sw;
        try run(1, null, equal_time.body);
    }
    equal_time.swap = false;
}

// ---- Woken and not yet harvested: a cancel from inside the batch ----

/// At 3L two ops wake in one harvest batch: an accept (connection 1's ACK
/// lands) and a connect (connection 2's SYN-ACK lands). Whichever callback
/// runs first cancels the other, which is then woken and not yet
/// harvested. A cancelled accept claimed nothing, so connection 1 stays
/// queued; a cancelled connect still established, so its socket closes with
/// a FIN and the next accept returns that connection (design 17.6, review
/// 3, finding 3). The batch order is the seed's: seeds 1 to 16 give both.
const woken_cancel = struct {
    var acc: ev.NetAccept = undefined;
    var acc_addr: os_net.sockaddr.in = undefined;
    var acc_len: os_net.socklen_t = 0;
    var con: ev.NetConnect = undefined;
    var c0: ev.NetConnect = undefined;
    var to: os_net.sockaddr.in = undefined;
    var first: enum { none, accept, connect } = .none;
    var woke_at: u64 = 0;
    var seen: [2]u32 = .{ 0, 0 };

    fn onAccept(l: *ev.Loop, _: *ev.Completion) void {
        if (first != .none) return;
        first = .accept;
        woke_at = sim.clockNs();
        l.cancel(&con.c);
    }

    fn onConnect(l: *ev.Loop, _: *ev.Completion) void {
        if (first != .none) return;
        first = .connect;
        woke_at = sim.clockNs();
        l.cancel(&acc.c);
    }

    fn body() !void {
        first = .none;
        const server = try listen(lo, 0, .{});
        to = sockaddr(lo, portOf(server));
        const loop = &rt_mod.getCurrentExecutor().loop;
        const fd0 = try evOpen();
        const fd1 = try evOpen();
        c0 = ev.NetConnect.init(fd0, @ptrCast(&to), @sizeOf(os_net.sockaddr.in));
        c0.c.callback = noop;
        c0.c.flags.defer_callback = false;
        loop.add(&c0.c);
        acc_len = @sizeOf(os_net.sockaddr.in);
        acc = ev.NetAccept.init(server.socket.handle, @ptrCast(&acc_addr), &acc_len);
        acc.c.callback = onAccept;
        acc.c.flags.defer_callback = false;
        loop.add(&acc.c);
        try rt_mod.sleep(.fromMicroseconds(L / 1000));
        try expectEqual(@as(u64, L), sim.clockNs());
        con = ev.NetConnect.init(fd1, @ptrCast(&to), @sizeOf(os_net.sockaddr.in));
        con.c.callback = onConnect;
        con.c.flags.defer_callback = false;
        loop.add(&con.c);
        try rt_mod.sleep(.fromMilliseconds(1));
        try expectEqual(@as(u64, 3 * L), woke_at);
        try c0.getResult();
        const p0 = (try localOf(fd0)).port;
        const p1 = (try localOf(fd1)).port;
        switch (first) {
            .none => return error.NeitherWoke,
            .accept => {
                seen[0] += 1;
                const s0 = try acc.getResult();
                try expectEqual(p0, std.mem.bigToNative(u16, acc_addr.port));
                try expectError(error.Canceled, con.getResult());
                // Cancelling the wait did not abort the connect.
                try expectEqual(sim.StreamPhase.est, sim.testStreamEnd(fd1).phase);
                evClose(fd1);
                const s1 = try server.accept(.{});
                try expectEqual(p1, s1.socket.address.ip.getPort());
                try rt_mod.sleep(.fromMicroseconds(L / 1000));
                try expect(sim.testStreamEnd(s1.socket.handle).fin_in);
                evClose(s0);
                s1.close();
            },
            .connect => {
                seen[1] += 1;
                try con.getResult();
                try expectError(error.Canceled, acc.getResult());
                // The cancelled accept claimed nothing: connection 1 is still
                // the queue's head.
                const s0 = try server.accept(.{});
                try expectEqual(p0, s0.socket.address.ip.getPort());
                const s1 = try server.accept(.{});
                try expectEqual(p1, s1.socket.address.ip.getPort());
                evClose(fd1);
                s0.close();
                s1.close();
            },
        }
        evClose(fd0);
        server.close();
        try drain();
    }
};

test "sim: stream: an accept or a connect woken and cancelled inside the harvest batch: the accept claimed nothing, the connect still established" {
    if (comptime skip) return error.SkipZigTest;
    woken_cancel.seen = .{ 0, 0 };
    for (1..17) |s| try run(s, null, woken_cancel.body);
    // Both batch orders happened.
    try expect(woken_cancel.seen[0] > 0 and woken_cancel.seen[1] > 0);
}

/// The same batch, but the connect's callback closes the listener and opens
/// a stream socket that takes its number: the woken accept must end
/// FileDescriptorNotASocket (the generation), not act on the new socket.
/// The stream twin of `sim: dgram ev: a receive woken by a landing whose
/// socket a callback earlier in the same harvest closes and reopens`.
const batch_reopen = struct {
    var acc: ev.NetAccept = undefined;
    var acc_len: os_net.socklen_t = 0;
    var acc_addr: os_net.sockaddr.in = undefined;
    var con: ev.NetConnect = undefined;
    var c0: ev.NetConnect = undefined;
    var to: os_net.sockaddr.in = undefined;
    var listener_fd: i32 = -1;
    var reopened: i32 = -1;
    var first: enum { none, accept, connect } = .none;
    var seen: [2]u32 = .{ 0, 0 };

    fn onAccept(_: *ev.Loop, _: *ev.Completion) void {
        if (first == .none) first = .accept;
    }

    fn onConnect(_: *ev.Loop, _: *ev.Completion) void {
        if (first != .none) return;
        first = .connect;
        os_net.close(listener_fd);
        reopened = os_net.socket(.ipv4, .stream, .ip, .{ .nonblocking = true }) catch @panic("open");
    }

    fn body() !void {
        first = .none;
        reopened = -1;
        const server = try listen(lo, 0, .{});
        listener_fd = server.socket.handle;
        to = sockaddr(lo, portOf(server));
        const loop = &rt_mod.getCurrentExecutor().loop;
        const fd0 = try evOpen();
        const fd1 = try evOpen();
        c0 = ev.NetConnect.init(fd0, @ptrCast(&to), @sizeOf(os_net.sockaddr.in));
        c0.c.callback = noop;
        c0.c.flags.defer_callback = false;
        loop.add(&c0.c);
        acc_len = @sizeOf(os_net.sockaddr.in);
        acc = ev.NetAccept.init(listener_fd, @ptrCast(&acc_addr), &acc_len);
        acc.c.callback = onAccept;
        acc.c.flags.defer_callback = false;
        loop.add(&acc.c);
        try rt_mod.sleep(.fromMicroseconds(L / 1000));
        con = ev.NetConnect.init(fd1, @ptrCast(&to), @sizeOf(os_net.sockaddr.in));
        con.c.callback = onConnect;
        con.c.flags.defer_callback = false;
        loop.add(&con.c);
        try rt_mod.sleep(.fromMilliseconds(1));
        try c0.getResult();
        try con.getResult();
        switch (first) {
            .none => return error.NeitherWoke,
            .accept => {
                seen[0] += 1;
                evClose(try acc.getResult());
                server.close();
            },
            .connect => {
                seen[1] += 1;
                try expectEqual(listener_fd, reopened);
                try expectError(error.FileDescriptorNotASocket, acc.getResult());
                // The listener's close reset the queued connection.
                try expect(sim.testStreamEnd(fd0).reset_received);
                evClose(reopened);
            },
        }
        evClose(fd0);
        evClose(fd1);
        try drain();
    }
};

test "sim: stream: a woken accept whose listener a callback earlier in the batch closes and reopens ends FileDescriptorNotASocket" {
    if (comptime skip) return error.SkipZigTest;
    batch_reopen.seen = .{ 0, 0 };
    for (1..17) |s| try run(s, null, batch_reopen.body);
    try expect(batch_reopen.seen[0] > 0 and batch_reopen.seen[1] > 0);
}

// ---- Reuse against entries still on the wire ----

const fd_reuse = struct {
    fn body() !void {
        const server = try listen(lo, 0, .{});
        const port = portOf(server);
        const t0 = sim.clockNs();
        // The first client's SYN lands at L; it times out at 1.5L and is
        // closed. Its SYN-ACK is still on the wire.
        var sock = try zio_net.Socket.open(.stream, .ipv4, .ip);
        const old_fd = sock.handle;
        try expectError(error.Timeout, sock.connect(.{ .ip = zio_net.IpAddress.initIp4(lo, port) }, .{ .timeout = .fromMicroseconds(3 * L / 2000) }));
        sock.close();
        const t1 = sim.clockNs();
        try expectEqual(t0 + 3 * L / 2, t1);
        // The next socket gets the same number. The old SYN-ACK lands at 2L
        // and goes to the old connection by its conn_id, not to this socket:
        // this connect completes at its own SYN-ACK, t1 + 2L.
        const c = try connect(lo, port, .none);
        try expectEqual(old_fd, c.socket.handle);
        try expectEqual(t1 + 2 * L, sim.clockNs());
        try expectEqual(@as(u32, 2), sim.testStreamEnd(c.socket.handle).conn_id);
        const s = try server.accept(.{});
        try expectEqual((try localOf(c.socket.handle)).port, s.socket.address.ip.getPort());
        c.close();
        s.close();
        server.close();
        try drain();
        try expectEqual(@as(u64, 2), sim.streamStats().retired);
    }
};

const port_reuse = struct {
    var client: ?zio_net.Stream = null;

    fn connector() !void {
        client = try connect(lo, 7000, .none);
    }

    fn body() !void {
        client = null;
        const first = try listen(lo, 7000, .{});
        const t0 = sim.clockNs();
        var h = try rt_mod.spawn(connector, .{});
        // The SYN landed at L (provisional); close the listener at 1.5L, so
        // its RST follows the SYN-ACK, and bind the port again at once.
        try rt_mod.sleep(.fromMicroseconds(3 * L / 2000));
        first.close();
        const second = try listen(lo, 7000, .{});
        try h.join();
        try expectEqual(t0 + 2 * L, sim.clockNs());
        try drain();
        // The RST reached the old connection's client; the ACK landed at its
        // dead server endpoint; nothing reached the new listener.
        try expect(sim.testStreamEnd(client.?.socket.handle).reset_received);
        try expectEqual(@as(usize, 0), sim.testListenerCount(second.socket.handle));
        try expectError(error.Timeout, second.accept(.{ .timeout = .fromMilliseconds(1) }));
        try expectEqual(@as(u64, 1), sim.streamStats().dropped_dead);
        client.?.close();
        second.close();
        try drain();
        try expectEqual(@as(usize, 0), sim.streamReservations());
    }
};

test "sim: stream: a fd number reused while the old connection's SYN-ACK is on the wire: it lands at the old connection, not the new socket" {
    if (comptime skip) return error.SkipZigTest;
    try run(1, null, fd_reuse.body);
}

test "sim: stream: a listener port bound again while the old connection's entries are on the wire: they land at the old connection, not the new listener" {
    if (comptime skip) return error.SkipZigTest;
    try run(1, null, port_reuse.body);
}

// ---- Profiles: rows 15 and 30 ----

const other_loopback = struct {
    fn body() !void {
        const server = try listen(any, 0, .{});
        const port = portOf(server);
        const c = try connect(.{ 127, 0, 0, 2 }, port, .none);
        const s = try server.accept(.{});
        // `.linux` (rows 14, 30): the client binds 127.0.0.1; the accepted
        // socket's local address is the SYN's destination.
        try expectEqual(sim.Ip4{ .ip = lo, .port = s.socket.address.ip.getPort() }, try localOf(c.socket.handle));
        try expectEqual(sim.Ip4{ .ip = .{ 127, 0, 0, 2 }, .port = port }, try localOf(s.socket.handle));
        c.close();
        s.close();
        server.close();
        try drain();
    }
};

test "sim: stream: under .linux a connect to 127.0.0.2 reaches a 0.0.0.0 listener from 127.0.0.1 (rows 15 and 30)" {
    if (comptime skip) return error.SkipZigTest;
    try run(1, .linux, other_loopback.body);
}

// ---- Options ----

const options = struct {
    fn body() !void {
        var sock = try zio_net.Socket.open(.stream, .ipv4, .ip);
        try expect(!try sock.getNoDelay());
        try sock.setNoDelay(true);
        try expect(try sock.getNoDelay());
        // Nagle is not modelled: turning NODELAY off is unsupported, and
        // changes nothing.
        try expectError(error.OptionNotSupported, sock.setNoDelay(false));
        try expect(try sock.getNoDelay());
        // Advisory on a stream socket (zio's std.Io listen ignores it).
        try expectError(error.OptionNotSupported, sock.setReusePort(true));
        try sock.setReuseAddress(true);
        sock.close();
        // zio.net's listen does not ignore it: the error reaches the caller.
        try expectError(error.OptionNotSupported, listen(lo, 0, .{ .reuse_port = true }));
        // std.Io's listen with reuse_address sets SO_REUSEPORT and ignores
        // the answer.
        const io = runtime.io();
        var server = try std.Io.net.IpAddress.listen(&.{ .ip4 = .loopback(0) }, io, .{ .reuse_address = true });
        server.deinit(io);
    }
};

test "sim: stream: TCP_NODELAY 1 is stored and read back, 0 and SO_REUSEPORT are OptionNotSupported, SO_REUSEADDR is accepted" {
    if (comptime skip) return error.SkipZigTest;
    try run(1, null, options.body);
}

// ---- std.Io on zio's vtable, end to end ----

const std_io = struct {
    var server: std.Io.net.Server = undefined;
    var peer: ?std.Io.net.Stream = null;

    fn acceptor(io: std.Io) !void {
        peer = try server.accept(io);
    }

    fn body() !void {
        peer = null;
        const io = runtime.io();
        server = try std.Io.net.IpAddress.listen(&.{ .ip4 = .loopback(0) }, io, .{ .reuse_address = true });
        try expectEqual(@as(u16, 49152), server.socket.address.getPort());
        var h = try rt_mod.spawn(acceptor, .{io});
        const t0 = sim.clockNs();
        const client = try std.Io.net.IpAddress.connect(&server.socket.address, io, .{ .mode = .stream });
        try expectEqual(t0 + 2 * L, sim.clockNs());
        try h.join();
        try expectEqual(t0 + 3 * L, sim.clockNs());
        try expectEqual((try localOf(client.socket.handle)).port, peer.?.socket.address.getPort());
        // std.Io's connect sets TCP_NODELAY; the accepted socket inherits
        // the listener's.
        var v: c_int = 0;
        try os_net.getsockopt(client.socket.handle, os_net.IPPROTO.TCP, std.posix.TCP.NODELAY, std.mem.asBytes(&v));
        try expectEqual(@as(c_int, 1), v);
        v = 0;
        try os_net.getsockopt(peer.?.socket.handle, os_net.IPPROTO.TCP, std.posix.TCP.NODELAY, std.mem.asBytes(&v));
        try expectEqual(@as(c_int, 1), v);
        client.close(io);
        peer.?.close(io);
        server.deinit(io);
        try drain();
        try expectEqual(@as(u64, 1), sim.streamStats().retired);
    }
};

test "sim: stream: std.Io's listen with reuse_address, connect and accept on zio's vtable, end to end" {
    if (comptime skip) return error.SkipZigTest;
    try run(1, null, std_io.body);
}

// ---- Determinism ----

const digests = struct {
    var out: [3]u64 = undefined;

    fn body() !void {
        try handshake.body();
        out = .{ sim.traceHash(), sim.wireDigest(), sim.stateDigest() };
    }
};

test "sim: stream: one seed gives one trace, wire digest and state digest; another profile changes only the state digest" {
    if (comptime skip) return error.SkipZigTest;
    try run(3, null, digests.body);
    const a = digests.out;
    try run(3, null, digests.body);
    try expectEqual(a, digests.out);
    try run(3, .linux, digests.body);
    try expectEqual(a[0], digests.out[0]);
    try expectEqual(a[1], digests.out[1]);
    try expect(a[2] != digests.out[2]);
}

// ---- Refusals ----

fn refusal(comptime name: []const u8, comptime want: []const u8, comptime profile: ?sim.StreamProfile, comptime body: fn () anyerror!void) !void {
    if (comptime skip) return error.SkipZigTest;
    if (!sim.isDeathChild()) return sim.expectChildPanics(name, want);
    sim.noCoreDump();
    try run(1, profile, body);
}

/// A connected pair: the client is fd 100033, the accepted socket 100034.
fn pair() !struct { zio_net.Server, zio_net.Stream, zio_net.Stream } {
    const server = try listen(lo, 0, .{});
    const c = try connect(lo, portOf(server), .none);
    const s = try server.accept(.{});
    return .{ server, c, s };
}

const refuse_bodies = struct {
    fn send() !void {
        _, const c, _ = try pair();
        _ = try c.write("x", .none);
    }
    fn recv() !void {
        _, const c, _ = try pair();
        var b: [4]u8 = undefined;
        _ = try c.read(&b, .none);
    }
    fn shutdown() !void {
        _, const c, _ = try pair();
        try c.shutdown(.send);
    }
    fn poll() !void {
        _, const c, _ = try pair();
        var op = ev.NetPoll.init(c.socket.handle, .recv);
        try io_common.waitForIo(&op.c);
    }
    fn sendto() !void {
        _, const c, _ = try pair();
        var sock = c.socket;
        _ = try sock.sendTo(.{ .ip = zio_net.IpAddress.initIp4(lo, 9) }, "x", .none);
    }
    fn layerRecv() !void {
        _, const c, _ = try pair();
        var b: [4]u8 = undefined;
        var iov = [1]os_net.iovec{os_net.iovecFromSlice(&b)};
        _ = try os_net.recv(c.socket.handle, &iov, .{});
    }
    fn layerConnect() !void {
        const fd = try os_net.socket(.ipv4, .stream, .ip, .{ .nonblocking = true });
        const to = sockaddr(lo, 9);
        try os_net.connect(fd, @ptrCast(&to), @sizeOf(os_net.sockaddr.in));
    }
    fn linger() !void {
        const fd = try os_net.socket(.ipv4, .stream, .ip, .{ .nonblocking = true });
        const lg: [2]c_int = .{ 1, 0 };
        try os_net.setsockopt(fd, os_net.SOL.SOCKET, os_net.SO.LINGER, std.mem.asBytes(&lg));
    }
    fn rcvbuf() !void {
        const fd = try os_net.socket(.ipv4, .stream, .ip, .{ .nonblocking = true });
        const v: c_int = 4096;
        try os_net.setsockopt(fd, os_net.SOL.SOCKET, os_net.SO.RCVBUF, std.mem.asBytes(&v));
    }
    fn soError() !void {
        const fd = try os_net.socket(.ipv4, .stream, .ip, .{ .nonblocking = true });
        _ = try os_net.getSockError(fd);
    }
    fn listenUnbound() !void {
        var sock = try zio_net.Socket.open(.stream, .ipv4, .ip);
        try sock.listen(8);
    }
    fn connectBound() !void {
        var sock = try zio_net.Socket.open(.stream, .ipv4, .ip);
        try sock.bind(.{ .ip = zio_net.IpAddress.initIp4(lo, 0) });
        try sock.connect(.{ .ip = zio_net.IpAddress.initIp4(lo, 9) }, .{});
    }
    fn localAddress() !void {
        _ = try zio_net.IpAddress.initIp4(lo, 9).connect(.{ .local_address = zio_net.IpAddress.initIp4(lo, 0) });
    }
    fn acceptOpen() !void {
        const fd = try evOpen();
        var op = ev.NetAccept.init(fd, null, null);
        try io_common.waitForIo(&op.c);
    }
    var server: zio_net.Server = undefined;
    fn accepter() !void {
        _ = try server.accept(.{});
    }
    fn secondAccept() !void {
        server = try listen(lo, 0, .{});
        _ = try rt_mod.spawn(accepter, .{});
        try rt_mod.sleep(.fromMicroseconds(10));
        _ = try server.accept(.{});
    }
    var port: u16 = 0;
    fn connector() !void {
        _ = try connect(lo, port, .none);
    }
    fn backlog() !void {
        const s = try listen(lo, 0, .{ .kernel_backlog = 1 });
        port = portOf(s);
        _ = try rt_mod.spawn(connector, .{});
        _ = try connect(lo, port, .none);
        try rt_mod.sleep(.fromMilliseconds(1));
    }
    fn otherLoopback() !void {
        _ = try connect(.{ 127, 0, 0, 2 }, 9, .none);
    }
    fn row12() !void {
        const a = try listen(lo, 0, .{ .reuse_address = true });
        _ = try listen(any, portOf(a), .{ .reuse_address = true });
    }
    fn row31() !void {
        const l = try listen(lo, 0, .{});
        const p = portOf(l);
        const c = try connect(lo, p, .none);
        const s = try l.accept(.{});
        s.close();
        try rt_mod.sleep(.fromMilliseconds(1));
        c.close();
        l.close();
        try drain();
        _ = try listen(lo, p, .{ .reuse_address = true });
    }
    fn profileLate() !void {
        _ = try os_net.socket(.ipv4, .stream, .ip, .{ .nonblocking = true });
        sim.setStreamProfile(.linux);
    }
    fn rebind() !void {
        var sock = try zio_net.Socket.open(.stream, .ipv4, .ip);
        try sock.bind(.{ .ip = zio_net.IpAddress.initIp4(lo, 0) });
        try sock.bind(.{ .ip = zio_net.IpAddress.initIp4(lo, 0) });
    }
    fn ipv6() !void {
        _ = try os_net.socket(.ipv6, .stream, .ip, .{ .nonblocking = true });
    }
    fn leakOpen() !void {
        _ = try os_net.socket(.ipv4, .stream, .ip, .{ .nonblocking = true });
    }
    fn leakListener() !void {
        _ = try listen(lo, 0, .{});
    }
    fn leakWire() !void {
        const l, const c, const s = try pair();
        c.close();
        s.close();
        l.close();
    }
};

test "sim: stream refuses: NetSend on a stream socket (slice 2)" {
    try refusal("sim: stream refuses: NetSend on a stream socket", "sim: NetSend on sim stream socket 100033 is not simulated yet", null, refuse_bodies.send);
}
test "sim: stream refuses: NetRecv on a stream socket (slice 2)" {
    try refusal("sim: stream refuses: NetRecv on a stream socket", "sim: NetRecv on sim stream socket 100033 is not simulated yet", null, refuse_bodies.recv);
}
test "sim: stream refuses: NetShutdown on a stream socket (slice 2)" {
    try refusal("sim: stream refuses: NetShutdown on a stream socket", "sim: NetShutdown on sim stream socket 100033 is not simulated yet", null, refuse_bodies.shutdown);
}
test "sim: stream refuses: NetPoll on a stream socket" {
    try refusal("sim: stream refuses: NetPoll on a stream socket", "sim: NetPoll on sim stream socket 100033 is refused", null, refuse_bodies.poll);
}
test "sim: stream refuses: NetSendTo on a stream socket" {
    try refusal("sim: stream refuses: NetSendTo on a stream socket", "sim: net_sendto on sim stream socket 100033 is not simulated", null, refuse_bodies.sendto);
}
test "sim: stream refuses: the socket layer's recv on a stream socket" {
    try refusal("sim: stream refuses: the socket layer's recv on a stream socket", "sim: the socket layer's receive on sim stream socket 100033 is not simulated yet", null, refuse_bodies.layerRecv);
}
test "sim: stream refuses: the socket layer's connect on a stream socket" {
    try refusal("sim: stream refuses: the socket layer's connect on a stream socket", "sim: the socket layer's connect on sim stream socket 100032", null, refuse_bodies.layerConnect);
}
test "sim: stream refuses: SO_LINGER before slice 2" {
    try refusal("sim: stream refuses: SO_LINGER before slice 2", "sim: setsockopt SO_LINGER on sim stream socket 100032", null, refuse_bodies.linger);
}
test "sim: stream refuses: SO_RCVBUF on a stream socket is misuse" {
    try refusal("sim: stream refuses: SO_RCVBUF on a stream socket is misuse", "sim: setsockopt a SOL_SOCKET option", null, refuse_bodies.rcvbuf);
}
test "sim: stream refuses: getsockopt SO_ERROR" {
    try refusal("sim: stream refuses: getsockopt SO_ERROR", "sim: getsockopt SO_ERROR on sim stream socket 100032 is refused", null, refuse_bodies.soError);
}
test "sim: stream refuses: listen on an unbound socket" {
    try refusal("sim: stream refuses: listen on an unbound socket", "sim: listen on sim fd 100032, which is not bound", null, refuse_bodies.listenUnbound);
}
test "sim: stream refuses: connect from a socket the program bound" {
    try refusal("sim: stream refuses: connect from a socket the program bound", "sim: connect on sim fd 100032, which the program bound", null, refuse_bodies.connectBound);
}
test "sim: stream refuses: IpAddress.connect with local_address, before a socket opens" {
    try refusal("sim: stream refuses: IpAddress.connect with local_address", "sim: IpAddress.connect with local_address during a sim run", null, refuse_bodies.localAddress);
}
test "sim: stream refuses: accept on a socket that is not a listener" {
    try refusal("sim: stream refuses: accept on a socket that is not a listener", "sim: accept on sim fd 100032, a stream socket that is not connected", null, refuse_bodies.acceptOpen);
}
test "sim: stream refuses: a second accept while one is parked" {
    try refusal("sim: stream refuses: a second accept while one is parked", "sim: a second accept while one is parked, on sim listener 100032", null, refuse_bodies.secondAccept);
}
test "sim: stream refuses: a SYN at a full listener (backlog overflow)" {
    try refusal("sim: stream refuses: a SYN at a full listener", "sim: a SYN to port 49152 landed at a full listener (backlog 1)", null, refuse_bodies.backlog);
}
test "sim: stream refuses: a connect to 127.0.0.2 with no profile (rows 15, 30)" {
    try refusal("sim: stream refuses: a connect to 127.0.0.2 with no profile", "sim: rows 15 and 30, a connect to 127/8 other than 127.0.0.1: the kernels differ here", null, refuse_bodies.otherLoopback);
}
test "sim: stream refuses: a connect to 127.0.0.2 under .macos" {
    try refusal("sim: stream refuses: a connect to 127.0.0.2 under .macos", "sim: connect from sim fd 100032 to 127.0.0.2: refused under .macos", .macos, refuse_bodies.otherLoopback);
}
test "sim: stream refuses: row 12 with no profile" {
    try refusal("sim: stream refuses: row 12 with no profile", "sim: row 12, a bind with SO_REUSEADDR to a port a listener holds on another address: the kernels differ here", null, refuse_bodies.row12);
}
test "sim: stream refuses: row 12 under .macos" {
    try refusal("sim: stream refuses: row 12 under .macos", "sim: a bind of sim fd 100033 with SO_REUSEADDR to port 49152, which listener 100032 holds on another address: refused under .macos", .macos, refuse_bodies.row12);
}
test "sim: stream refuses: row 31 with no profile" {
    try refusal("sim: stream refuses: row 31 with no profile", "sim: row 31, a bind with SO_REUSEADDR over a TIME_WAIT reservation that did not set it: the kernels differ here", null, refuse_bodies.row31);
}
test "sim: stream refuses: setStreamProfile after the first stream socket" {
    try refusal("sim: stream refuses: setStreamProfile after the first stream socket", "sim: setStreamProfile(linux) after the run's first stream socket opened", null, refuse_bodies.profileLate);
}
test "sim: stream refuses: a bind of a bound stream socket" {
    try refusal("sim: stream refuses: a bind of a bound stream socket", "sim: bind on sim fd 100032, which is already bound or connected", null, refuse_bodies.rebind);
}
test "sim: stream refuses: an IPv6 stream socket" {
    try refusal("sim: stream refuses: an IPv6 stream socket", "sim: socket(ipv6, stream, ip) during a sim run: only IPv4 datagram and stream sockets are simulated", null, refuse_bodies.ipv6);
}
test "sim: stream refuses: the teardown check: an open socket never connected, left at sim.end" {
    try refusal("sim: stream refuses: the teardown check: an open socket", "sim: stream teardown: 1 stream objects remain at sim.end", null, refuse_bodies.leakOpen);
}
test "sim: stream refuses: the teardown check: an empty listener left at sim.end" {
    try refusal("sim: stream refuses: the teardown check: an empty listener", "sim: stream teardown: 1 stream objects remain at sim.end", null, refuse_bodies.leakListener);
}
test "sim: stream refuses: the teardown check: FINs still on the wire at sim.end" {
    try refusal("sim: stream refuses: the teardown check: FINs still on the wire", "sim: stream teardown: 1 stream objects remain at sim.end", null, refuse_bodies.leakWire);
}

// ---- Parity: rows of design 17.13 on real sockets and in the sim ----

/// One kernel's expected answers, from the probes of sittings 23 and 25
/// (macOS 25.6 and Linux 6.8, 3 runs each).
const Answer = enum { ok, in_use, refused };
const Kernel = enum { macos, linux };

const parity = struct {
    const Got = struct {
        row2: Answer = .ok,
        row12: ?Answer = null,
        row14_lo: bool = false,
        row31: [4]Answer = @splat(.ok),
        row32: [2]Answer = @splat(.ok),
    };
    const want = std.EnumArray(Kernel, Got).init(.{
        .macos = .{ .row2 = .refused, .row12 = .ok, .row14_lo = true, .row31 = .{ .in_use, .ok, .in_use, .ok }, .row32 = .{ .ok, .ok } },
        .linux = .{ .row2 = .refused, .row12 = .in_use, .row14_lo = true, .row31 = .{ .in_use, .in_use, .in_use, .ok }, .row32 = .{ .ok, .ok } },
    });

    var got: Got = .{};
    /// Row 12 is refused under `.macos` (a death test checks that).
    var skip_row12 = false;

    fn answer(r: anyerror!zio_net.Server) !Answer {
        const s = r catch |err| return if (err == error.AddressInUse) .in_use else err;
        s.close();
        return .ok;
    }

    /// Time for the kernel's FIN and ACK exchange, as the probe waits.
    fn pause() !void {
        try rt_mod.sleep(.fromMilliseconds(50));
    }

    fn row2() !Answer {
        const s = try listen(lo, 0, .{});
        const p = portOf(s);
        s.close();
        const c = connect(lo, p, .none) catch |err| return if (err == error.ConnectionRefused) .refused else err;
        c.close();
        return .ok;
    }

    fn row12() !Answer {
        const a = try listen(lo, 0, .{ .reuse_address = true });
        defer a.close();
        return answer(listen(any, portOf(a), .{ .reuse_address = true }));
    }

    fn row14() !bool {
        const s = try listen(lo, 0, .{});
        const c = try connect(lo, portOf(s), .none);
        const a = try s.accept(.{});
        const ip = (try localOf(c.socket.handle)).ip;
        c.close();
        a.close();
        s.close();
        return std.mem.eql(u8, &ip, &lo);
    }

    /// Server-side active close, then a new listener on the port.
    fn row31(old: bool, new: bool) !Answer {
        const l = try listen(lo, 0, .{ .reuse_address = old });
        const p = portOf(l);
        const c = try connect(lo, p, .none);
        const s = try l.accept(.{});
        s.close();
        try pause();
        c.close();
        l.close();
        try pause();
        return answer(listen(lo, p, .{ .reuse_address = new }));
    }

    /// Client-side active close, then a new listener on the port.
    fn row32(new: bool) !Answer {
        const l = try listen(lo, 0, .{ .reuse_address = true });
        const p = portOf(l);
        const c = try connect(lo, p, .none);
        const s = try l.accept(.{});
        c.close();
        try pause();
        s.close();
        l.close();
        try pause();
        return answer(listen(lo, p, .{ .reuse_address = new }));
    }

    fn body() !void {
        got = .{};
        got.row2 = try row2();
        got.row12 = if (skip_row12) null else try row12();
        got.row14_lo = try row14();
        for ([_][2]bool{ .{ false, false }, .{ false, true }, .{ true, false }, .{ true, true } }, 0..) |c, i| got.row31[i] = try row31(c[0], c[1]);
        for ([_]bool{ false, true }, 0..) |n, i| got.row32[i] = try row32(n);
        if (sim.isRunning()) try drain();
    }

    fn check(k: Kernel) !void {
        var w = want.get(k);
        if (skip_row12) w.row12 = null;
        expectEqual(w, got) catch |err| {
            std.debug.print("parity, {t} column: want {any}\n                   got  {any}\n", .{ k, w, got });
            return err;
        };
    }

    fn inSim(k: Kernel) !void {
        skip_row12 = k == .macos;
        defer skip_row12 = false;
        try run(1, if (k == .macos) .macos else .linux, body);
        try check(k);
    }
};

const host_kernel: Kernel = if (builtin.os.tag.isDarwin()) .macos else .linux;

test "sim: stream parity: rows 2, 12, 14, 31 and 32 through zio.net, on this host's kernel without sim, in a sim run with its profile with sim" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    if (comptime sim.compiled_in) return parity.inSim(host_kernel);
    const rt = try rt_mod.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var h = try rt.spawn(parity.body, .{});
    try h.join();
    try parity.check(host_kernel);
}

test "sim: stream parity: the other kernel's column in a sim run (row 12 under .macos is refused, a death test)" {
    if (comptime skip) return error.SkipZigTest;
    try parity.inSim(if (host_kernel == .macos) .linux else .macos);
}

// ---- The surface table (design 17.15 item 1) ----

/// The last slice of design 17.16 that is built. A row of a later slice is
/// pending: it has no check, so it cannot pass.
const built_slice = 1;

const Status = union(enum) {
    /// Built: `check` exercises it (a scenario, or a refusal's child).
    built: struct { slice: u8, check: *const fn () anyerror!void },
    /// Built in a later slice.
    pending: u8,
    /// Not a stream socket call, or outside the sim's scope; says why.
    outside: []const u8,
};

const Row = struct { name: []const u8, status: Status };

fn built(comptime check: fn () anyerror!void) Status {
    return .{ .built = .{ .slice = 1, .check = check } };
}

fn scenario(comptime profile: ?sim.StreamProfile, comptime body: fn () anyerror!void) fn () anyerror!void {
    return struct {
        fn f() anyerror!void {
            try run(1, profile, body);
        }
    }.f;
}

fn refusedIn(comptime test_name: []const u8, comptime want: []const u8) fn () anyerror!void {
    return struct {
        fn f() anyerror!void {
            try sim.expectChildPanics(test_name, want);
        }
    }.f;
}

const chk = struct {
    const handshake_ = scenario(null, handshake.body);
    const refused_ = scenario(null, refused.body);
    const cancel_before = scenario(null, cancel_before_syn.body);
    const cancel_after = scenario(null, cancel_after_syn.body);
    const closed_before_accept = scenario(null, accepted_after_close.body);
    const both_close_ = scenario(null, both_close.body);
    const listener_close_ = scenario(null, listener_close.body);
    const close_wakes = scenario(null, close_wakes_accept.body);
    const port_reuse_ = scenario(null, port_reuse.body);
    const fd_reuse_ = scenario(null, fd_reuse.body);
    const std_io_ = scenario(null, std_io.body);
    const other_lo = scenario(.linux, other_loopback.body);
    fn equalTime() anyerror!void {
        for ([_]bool{ false, true }) |sw| {
            equal_time.swap = sw;
            try run(1, null, equal_time.body);
        }
        equal_time.swap = false;
    }
    fn wokenCancel() anyerror!void {
        woken_cancel.seen = .{ 0, 0 };
        for (1..17) |s| try run(s, null, woken_cancel.body);
        if (woken_cancel.seen[0] == 0 or woken_cancel.seen[1] == 0) return error.OneBatchOrderNeverHappened;
    }
    fn batchReopen() anyerror!void {
        batch_reopen.seen = .{ 0, 0 };
        for (1..17) |s| try run(s, null, batch_reopen.body);
        if (batch_reopen.seen[0] == 0 or batch_reopen.seen[1] == 0) return error.OneBatchOrderNeverHappened;
    }
    fn parityBoth() anyerror!void {
        try parity.inSim(.linux);
        try parity.inSim(.macos);
    }
    const send = refusedIn("sim: stream refuses: NetSend on a stream socket", "sim: NetSend on sim stream socket 100033 is not simulated yet");
    const recv = refusedIn("sim: stream refuses: NetRecv on a stream socket", "sim: NetRecv on sim stream socket 100033 is not simulated yet");
    const shutdown = refusedIn("sim: stream refuses: NetShutdown on a stream socket", "sim: NetShutdown on sim stream socket 100033 is not simulated yet");
    const poll = refusedIn("sim: stream refuses: NetPoll on a stream socket", "sim: NetPoll on sim stream socket 100033 is refused");
    const sendto = refusedIn("sim: stream refuses: NetSendTo on a stream socket", "sim: net_sendto on sim stream socket 100033 is not simulated");
    const ipv6 = refusedIn("sim: stream refuses: an IPv6 stream socket", "sim: socket(ipv6, stream, ip) during a sim run");
    const backlog = refusedIn("sim: stream refuses: a SYN at a full listener", "sim: a SYN to port 49152 landed at a full listener (backlog 1)");
    const accept_kind = refusedIn("sim: stream refuses: accept on a socket that is not a listener", "sim: accept on sim fd 100032, a stream socket that is not connected");
    const accept_second = refusedIn("sim: stream refuses: a second accept while one is parked", "sim: a second accept while one is parked, on sim listener 100032");
    const connect_bound = refusedIn("sim: stream refuses: connect from a socket the program bound", "sim: connect on sim fd 100032, which the program bound");
    const local_address = refusedIn("sim: stream refuses: IpAddress.connect with local_address", "sim: IpAddress.connect with local_address during a sim run");
    const teardown_open = refusedIn("sim: stream refuses: the teardown check: an open socket", "sim: stream teardown: 1 stream objects remain at sim.end");
    const teardown_listener = refusedIn("sim: stream refuses: the teardown check: an empty listener", "sim: stream teardown: 1 stream objects remain at sim.end");
    const teardown_wire = refusedIn("sim: stream refuses: the teardown check: FINs still on the wire", "sim: stream teardown: 1 stream objects remain at sim.end");
    const row12_none = refusedIn("sim: stream refuses: row 12 with no profile", "sim: row 12, a bind with SO_REUSEADDR");
    const row31_none = refusedIn("sim: stream refuses: row 31 with no profile", "sim: row 31, a bind with SO_REUSEADDR");
    const lo2_none = refusedIn("sim: stream refuses: a connect to 127.0.0.2 with no profile", "sim: rows 15 and 30, a connect to 127/8 other than 127.0.0.1");
    const so_error = refusedIn("sim: stream refuses: getsockopt SO_ERROR", "sim: getsockopt SO_ERROR on sim stream socket 100032 is refused");
    const send_file = refusedIn("sim: stream refuses: NetSendFile", "sim: unsimulated I/O op net_send_file");
    const unix = refusedIn("sim: stream refuses: std.Io listen on a unix address", "sim: socket(unix, stream, ip) during a sim run");
};

const send_file_body = struct {
    fn body() !void {
        _, const c, _ = try pair();
        var b: [2][1]u8 = undefined;
        var op = ev.NetSendFile.init(c.socket.handle, 0, 0, 1, .{ &b[0], &b[1] });
        try io_common.waitForIo(&op.c);
    }
};

test "sim: stream refuses: NetSendFile" {
    try refusal("sim: stream refuses: NetSendFile", "sim: unsimulated I/O op net_send_file", null, send_file_body.body);
}

const unix_body = struct {
    fn body() !void {
        const io = runtime.io();
        const a = try std.Io.net.UnixAddress.init("/tmp/zio-sim-never");
        _ = try a.listen(io, .{});
    }
};

test "sim: stream refuses: std.Io listen on a unix address" {
    if (comptime !zio_net.has_unix_sockets) return error.SkipZigTest;
    try refusal("sim: stream refuses: std.Io listen on a unix address", "sim: socket(unix, stream, ip) during a sim run", null, unix_body.body);
}

/// One row per ev op tag that names a socket (`net_*`); every other tag is a
/// file, timer or pool op and takes no socket, so it gets an `outside` row
/// automatically. A new `net_*` tag fails the build until it has a row.
const op_rows = [_]Row{
    .{ .name = "net_open", .status = built(chk.handshake_) },
    .{ .name = "net_bind", .status = built(chk.handshake_) },
    .{ .name = "net_listen", .status = built(chk.handshake_) },
    .{ .name = "net_connect", .status = built(chk.handshake_) },
    .{ .name = "net_accept", .status = built(chk.handshake_) },
    .{ .name = "net_close", .status = built(chk.handshake_) },
    .{ .name = "net_recv", .status = .{ .pending = 2 } },
    .{ .name = "net_send", .status = .{ .pending = 2 } },
    .{ .name = "net_shutdown", .status = .{ .pending = 2 } },
    .{ .name = "net_poll", .status = built(chk.poll) },
    .{ .name = "net_recvfrom", .status = built(chk.sendto) },
    .{ .name = "net_sendto", .status = built(chk.sendto) },
    .{ .name = "net_recvmsg", .status = built(chk.sendto) },
    .{ .name = "net_recvmmsg", .status = built(chk.sendto) },
    .{ .name = "net_sendmsg", .status = built(chk.sendto) },
    .{ .name = "net_sendmmsg", .status = built(chk.sendto) },
    .{ .name = "net_send_file", .status = built(chk.send_file) },
};

/// One row per `net*` field of std.Io's vtable.
const vtable_rows = [_]Row{
    .{ .name = "netListenIp", .status = built(chk.std_io_) },
    .{ .name = "netAccept", .status = built(chk.std_io_) },
    .{ .name = "netBindIp", .status = .{ .outside = "a datagram bind (phase 1); with mode .stream it is NetOpen and NetBind, both built" } },
    .{ .name = "netConnectIp", .status = built(chk.std_io_) },
    .{ .name = "netListenUnix", .status = built(chk.unix) },
    .{ .name = "netConnectUnix", .status = .{ .outside = "AF_UNIX is refused at socket() (section 2), as for netListenUnix" } },
    .{ .name = "netSocketCreatePair", .status = .{ .outside = "a kernel socketpair, AF_UNIX in practice: real descriptors, which a sim op refuses (section 3.3)" } },
    .{ .name = "netWriteFile", .status = built(chk.send_file) },
    .{ .name = "netClose", .status = built(chk.std_io_) },
    .{ .name = "netShutdown", .status = .{ .pending = 2 } },
    .{ .name = "netInterfaceNameResolve", .status = .{ .outside = "interfaces and DNS are outside sim runs (section 2)" } },
    .{ .name = "netInterfaceName", .status = .{ .outside = "interfaces and DNS are outside sim runs (section 2)" } },
    .{ .name = "netLookup", .status = .{ .outside = "interfaces and DNS are outside sim runs (section 2)" } },
};

/// One row per line of design 17.6's and 17.8's decision lists, and the
/// other rules slice 1 builds; the later slices' lines are pending.
const rule_rows = [_]Row{
    .{ .name = "17.6 NetConnect 1: not an open stream socket, or bound by the program: refused", .status = built(chk.connect_bound) },
    .{ .name = "17.6 NetConnect 1: local_address refused in the adapter, before a socket opens", .status = built(chk.local_address) },
    .{ .name = "17.6 NetConnect 2: conn_id, C-syn, SYN, parks", .status = built(chk.handshake_) },
    .{ .name = "17.6 NetConnect harvest: generation, then ok at the SYN-ACK", .status = built(chk.handshake_) },
    .{ .name = "17.6 NetConnect harvest: ConnectionRefused at the RST", .status = built(chk.refused_) },
    .{ .name = "17.6 SYN 1: dead client: dropped, server stays absent", .status = built(chk.cancel_before) },
    .{ .name = "17.6 SYN 2: listener with room: S-prov, SYN-ACK (exact address, then 0.0.0.0)", .status = built(chk.other_lo) },
    .{ .name = "17.6 SYN 3: full listener: panic", .status = built(chk.backlog) },
    .{ .name = "17.6 SYN 4: no listener: RST, server absent", .status = built(chk.refused_) },
    .{ .name = "17.6 SYN-ACK 1: failed client (failStream)", .status = .{ .pending = 3 } },
    .{ .name = "17.6 SYN-ACK 2: dead client: RST back", .status = built(chk.cancel_after) },
    .{ .name = "17.6 SYN-ACK 3: C-syn: est, connect ok, ACK", .status = built(chk.handshake_) },
    .{ .name = "17.6 ACK 1: dead server endpoint: dropped", .status = built(chk.port_reuse_) },
    .{ .name = "17.6 ACK 2: S-prov: queued, accept woken", .status = built(chk.handshake_) },
    .{ .name = "17.6 RST at C-syn: C-failed, ConnectionRefused", .status = built(chk.refused_) },
    .{ .name = "17.6 cancel or deadline of a parked connect: Timeout, endpoint stays C-syn", .status = built(chk.cancel_after) },
    .{ .name = "17.6 cancel of a woken connect: Canceled, endpoint est", .status = built(chk.wokenCancel) },
    .{ .name = "17.6 close of a client 1: C-open or C-failed: nothing on the wire", .status = built(chk.refused_) },
    .{ .name = "17.6 close of a client 2: C-syn, SYN not landed", .status = built(chk.cancel_before) },
    .{ .name = "17.6 close of a client 3: C-syn, SYN landed", .status = built(chk.cancel_after) },
    .{ .name = "17.6 close of a client 4 and 17.8 close 3: est: FIN, active closer reserved", .status = built(chk.handshake_) },
    .{ .name = "17.6 NetAccept 1: not a listener: refused", .status = built(chk.accept_kind) },
    .{ .name = "17.6 NetAccept 2: a second unresolved accept: refused", .status = built(chk.accept_second) },
    .{ .name = "17.6 NetAccept 3: queue not empty: claimed at the call", .status = built(chk.closed_before_accept) },
    .{ .name = "17.6 NetAccept 4: parks; a wake claims nothing", .status = built(chk.wokenCancel) },
    .{ .name = "17.6 NetAccept harvest 1: generation: FileDescriptorNotASocket", .status = built(chk.close_wakes) },
    .{ .name = "17.6 NetAccept harvest 1: generation, the number reused inside the batch", .status = built(chk.batchReopen) },
    .{ .name = "17.6 NetAccept harvest 2: claims the head", .status = built(chk.handshake_) },
    .{ .name = "17.6 cancel of a woken accept: the connection stays queued", .status = built(chk.wokenCancel) },
    .{ .name = "17.6 listener close 1: S-prov and S-queued die, RST to each client", .status = built(chk.listener_close_) },
    .{ .name = "17.6 listener close 2: accepted connections not affected (row 29)", .status = built(chk.listener_close_) },
    .{ .name = "17.6 listener close 3: a parked accept ends FileDescriptorNotASocket", .status = built(chk.close_wakes) },
    .{ .name = "17.6 a queued endpoint keeps a FIN that lands on it (row 27)", .status = built(chk.closed_before_accept) },
    .{ .name = "17.6 equal times: conn_id order, both creation orders", .status = built(chk.equalTime) },
    .{ .name = "17.5 landings go by conn_id: fd number reused", .status = built(chk.fd_reuse_) },
    .{ .name = "17.5 landings go by conn_id: listener port bound again", .status = built(chk.port_reuse_) },
    .{ .name = "17.5 retirement: absent counts as dead", .status = built(chk.refused_) },
    .{ .name = "17.5 TIME_WAIT: both active closers at one instant", .status = built(chk.both_close_) },
    .{ .name = "17.5 TIME_WAIT rebind by profile (row 31), and rows 2, 12, 14, 32", .status = built(chk.parityBoth) },
    .{ .name = "17.8 row 12 with no profile: panic", .status = built(chk.row12_none) },
    .{ .name = "17.8 row 31 with no profile: panic", .status = built(chk.row31_none) },
    .{ .name = "17.8 rows 15 and 30 with no profile: panic", .status = built(chk.lo2_none) },
    .{ .name = "17.4 options: NODELAY, REUSEADDR, advisory REUSEPORT", .status = built(scenario(null, options.body)) },
    .{ .name = "17.4 getsockopt SO_ERROR: refused", .status = built(chk.so_error) },
    .{ .name = "17.4 IPv6 and AF_UNIX: refused", .status = built(chk.ipv6) },
    .{ .name = "17.8 RST 1: a second RST: dropped and counted", .status = .{ .pending = 2 } },
    .{ .name = "17.8 RST 2: dead endpoint: dropped", .status = built(chk.listener_close_) },
    .{ .name = "17.8 RST 3: live S-prov: dies, capacity freed", .status = built(chk.cancel_after) },
    .{ .name = "17.8 RST 5: S-queued or est: reset_received (the profile's pending error is slice 2)", .status = built(chk.listener_close_) },
    .{ .name = "17.8 FIN 1: dead endpoint: dropped", .status = built(chk.handshake_) },
    .{ .name = "17.8 FIN 2: recorded", .status = built(chk.handshake_) },
    .{ .name = "17.15 teardown: an open socket never connected", .status = built(chk.teardown_open) },
    .{ .name = "17.15 teardown: an empty listener", .status = built(chk.teardown_listener) },
    .{ .name = "17.15 teardown: an unretired connection (entries on the wire)", .status = built(chk.teardown_wire) },
    .{ .name = "17.7 admission, chunks, window, credit, conservation", .status = .{ .pending = 2 } },
    .{ .name = "17.8 NetRecv list (and its harvest list)", .status = .{ .pending = 2 } },
    .{ .name = "17.8 NetSend list (and its harvest list)", .status = .{ .pending = 2 } },
    .{ .name = "17.8 profiles: what an RST sets, reset reported once", .status = .{ .pending = 2 } },
    .{ .name = "17.8 shutdown(.send) and shutdown(.both)", .status = .{ .pending = 2 } },
    .{ .name = "17.8 close 1 and 2: after a reset; with unread bytes or SO_LINGER 0", .status = .{ .pending = 2 } },
    .{ .name = "17.8 data landing 1 to 5", .status = .{ .pending = 2 } },
    .{ .name = "17.8 credit landing 1 and 2", .status = .{ .pending = 2 } },
    .{ .name = "17.4 getpeername and bytesReadable (FIONREAD)", .status = .{ .pending = 2 } },
    .{ .name = "17.7 setStreamWindow and setStreamChunking", .status = .{ .pending = 2 } },
    .{ .name = "17.9 .tcp rules: delay, jitter, holding", .status = .{ .pending = 3 } },
    .{ .name = "17.9 failStream", .status = .{ .pending = 3 } },
    .{ .name = "17.9 the connect with no answer (a held SYN past its deadline)", .status = .{ .pending = 3 } },
    .{ .name = "17.16 slice 4: starh2's dst-conn accept step; hotseat's tests", .status = .{ .pending = 4 } },
};

fn findRow(comptime rows: []const Row, comptime name: []const u8) usize {
    var n: usize = 0;
    for (rows) |r| {
        if (std.mem.eql(u8, r.name, name)) n += 1;
    }
    return n;
}

// Every `net_*` ev op tag and every `net*` vtable field has exactly one row,
// and every row names one that exists: a new tag or field fails the build.
// Not on Windows: a socket there is a handle, not an `i32`, and the rows'
// checks are POSIX code (every one of these tests skips there).
comptime {
    if (builtin.os.tag != .windows) checkRows();
}

fn checkRows() void {
    @setEvalBranchQuota(100_000);
    for (@typeInfo(ev.Op).@"enum".field_names) |name| {
        if (std.mem.startsWith(u8, name, "net_") and findRow(&op_rows, name) != 1) @compileError("surface table: ev op " ++ name ++ " needs exactly one row");
    }
    for (op_rows) |r| {
        if (!@hasField(ev.Op, r.name)) @compileError("surface table: no ev op " ++ r.name);
    }
    for (@typeInfo(std.Io.VTable).@"struct".field_names) |name| {
        if (std.mem.startsWith(u8, name, "net") and findRow(&vtable_rows, name) != 1) @compileError("surface table: std.Io vtable field " ++ name ++ " needs exactly one row");
    }
    for (vtable_rows) |r| {
        if (!@hasField(std.Io.VTable, r.name)) @compileError("surface table: no std.Io vtable field " ++ r.name);
    }
    for (op_rows ++ vtable_rows ++ rule_rows) |r| switch (r.status) {
        .built => |b| if (b.slice > built_slice) @compileError("surface table: row of slice > built_slice marked built: " ++ r.name),
        .pending => |s| if (s <= built_slice) @compileError("surface table: row of a built slice marked pending: " ++ r.name),
        .outside => {},
    };
}

test "sim: stream surface table: every built row's check passes; pending rows are exactly the later slices' rows, printed, never passed" {
    if (comptime skip) return error.SkipZigTest;
    const all = op_rows ++ vtable_rows ++ rule_rows;
    var passed: usize = 0;
    var pending: usize = 0;
    var outside: usize = 0;
    for (all) |r| {
        switch (r.status) {
            .built => |b| {
                b.check() catch |err| {
                    std.debug.print("surface table: row \"{s}\" failed: {t}\n", .{ r.name, err });
                    return err;
                };
                passed += 1;
            },
            .pending => |s| {
                std.debug.print("surface table: pending (slice {d}): {s}\n", .{ s, r.name });
                pending += 1;
            },
            .outside => outside += 1,
        }
    }
    var auto_outside: usize = 0;
    inline for (comptime @typeInfo(ev.Op).@"enum".field_names) |name| {
        if (comptime !std.mem.startsWith(u8, name, "net_")) auto_outside += 1;
    }
    std.debug.print("surface table: {d} rows passed, {d} pending, {d} outside (and {d} ev op tags that take no socket)\n", .{ passed, pending, outside, auto_outside });
    // Which rows are pending is checked at compile time (the block above):
    // a built row of a later slice, or a pending row of a built one, does
    // not compile. Here: the run did the work.
    try expect(passed > 0 and pending > 0);
}
