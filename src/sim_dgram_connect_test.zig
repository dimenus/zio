// SPDX-FileCopyrightText: 2025 Lukáš Lalinský
// SPDX-License-Identifier: MIT
//
// Tests of connected datagram sockets under sim (t-4167). The protocol is
// the comment above `dgramConnect` in sim.zig.
//
// How they run:
// - The parity table (`sim: dgram connect parity`) runs 17 rows through
//   zio's API: in a build without sim on real loopback sockets, checked
//   against the host kernel's column; in a sim build in a sim run, checked
//   against the sim's column. On Linux run it in an io_uring and an epoll
//   binary. A row the sim refuses is null in the sim's column, and its
//   refusal is a death test below. The kernels' columns were measured on
//   macOS 25.6 (kqueue) and Linux 6.8 in colima (io_uring and epoll), 3
//   runs each, before the sim's column was written.
// - The scenario tests are sim runs that check exact virtual times, the
//   counts, and the fault rules on the ICMP's path. `L` is the wire latency:
//   a datagram sent at t to a closed port dies at t + L, and its ICMP lands
//   at t + 2L.
// - Every run closes its sockets before `sim.end`, which refuses an open
//   one.

const std = @import("std");
const builtin = @import("builtin");
const sim = @import("sim.zig");
const rt_mod = @import("runtime.zig");
const zio_net = @import("net.zig");
const ev = @import("ev/root.zig");
const io_common = @import("common.zig");
const os_net = @import("os/net.zig");
const t1 = @import("sim_stream_test.zig");
const zio_time = @import("time.zig");

const L = sim.wire_latency_ns;
const lo: [4]u8 = .{ 127, 0, 0, 1 };
const any: [4]u8 = .{ 0, 0, 0, 0 };
const skip = t1.skip;
const expect = std.testing.expect;
const expectEqual = std.testing.expectEqual;

/// A call's result: its count, or its error.
const Res = struct {
    n: ?usize = null,
    err: ?anyerror = null,
};

fn res(x: anytype) Res {
    if (x) |v| {
        return .{ .n = if (@TypeOf(v) == void) 0 else v };
    } else |e| return .{ .err = e };
}

const ok1: Res = .{ .n = 1 };
const ok0: Res = .{ .n = 0 };
const would_block: Res = .{ .err = error.WouldBlock };
const refused: Res = .{ .err = error.ConnectionRefused };
const unexpected: Res = .{ .err = error.Unexpected };

fn addr(ip: [4]u8, port: u16) zio_net.Address {
    return .{ .ip = zio_net.IpAddress.initIp4(ip, port) };
}

fn bound(ip: [4]u8, port: u16) !zio_net.Socket {
    return zio_net.IpAddress.initIp4(ip, port).bind(.{});
}

fn local(fd: i32) !sim.Ip4 {
    return t1.localOf(fd);
}

fn peer(fd: i32) !?sim.Ip4 {
    var sa: os_net.sockaddr.in = undefined;
    var len: os_net.socklen_t = @sizeOf(os_net.sockaddr.in);
    os_net.getpeername(fd, @ptrCast(&sa), &len) catch |err| return if (err == error.SocketUnconnected) null else err;
    return .{ .ip = @bitCast(sa.addr), .port = std.mem.bigToNative(u16, sa.port) };
}

fn portOf(s: zio_net.Socket) !u16 {
    return (try local(s.handle)).port;
}

/// Long enough for a kernel's ICMP, and for the sim's round trip (2L).
fn pause() !void {
    try rt_mod.sleep(.fromMilliseconds(20));
}

fn recvNow(fd: i32) Res {
    return recvNowFrom(fd)[0];
}

fn recvNowFrom(fd: i32) struct { Res, u16 } {
    var buf: [64]u8 = undefined;
    var iov = [1]os_net.iovec{os_net.iovecFromSlice(&buf)};
    var sa: os_net.sockaddr.in = undefined;
    var len: os_net.socklen_t = @sizeOf(os_net.sockaddr.in);
    const r = res(os_net.recvfrom(fd, &iov, .{ .dontwait = true }, @ptrCast(&sa), &len));
    return .{ r, if (r.n != null) std.mem.bigToNative(u16, sa.port) else 0 };
}

/// A port where nothing is bound: a socket's port, after its close.
fn closedPort() !u16 {
    const s = try bound(lo, 0);
    const p = try portOf(s);
    s.close();
    return p;
}

/// As Hotseat makes its media sockets: bound to 0.0.0.0 and port 0, then
/// connected.
fn connected(to_port: u16) !zio_net.Socket {
    var c = try bound(any, 0);
    errdefer c.close();
    try c.connect(addr(lo, to_port), .{});
    return c;
}

// ---- The parity table ----

const Kernel = enum { macos, linux, sim };

const Got = struct {
    // 1: bound to 0.0.0.0:0, then connect.
    r1_local_lo: bool = false,
    r1_same_port: bool = false,
    r1_peer: bool = false,
    // 2: unbound, then connect.
    r2_before_zero: bool = false,
    r2_local_lo: bool = false,
    r2_peer: bool = false,
    r2_exchange: bool = false,
    // 3: a stranger's datagram, then the peer's.
    r3_first_from_peer: bool = false,
    r3_second: Res = .{},
    // 4: send and receive with no address (NetSend, NetRecv).
    r4_send: Res = .{},
    r4_peer_from_c: bool = false,
    r4_receive: Res = .{},
    // 5: sendTo with an address (refused by the sim).
    r5_to_peer: ?Res = null,
    r5_to_other: ?Res = null,
    r5_peer_got: ?Res = null,
    r5_other_got: ?Res = null,
    // 6: a closed peer port; poll (refused by the sim), receive twice.
    r6_send: Res = .{},
    r6_poll_revents: ?i16 = null,
    r6_recv: [2]Res = @splat(.{}),
    // 7: the refused send is not sent.
    r7_send: [3]Res = @splat(.{}),
    r7_got: [2]Res = @splat(.{}),
    // 8: SO_ERROR reads and clears the error.
    r8_so_error_refused: bool = false,
    r8_again_zero: bool = false,
    r8_recv: Res = .{},
    // 9: a parked receive is woken with the error.
    r9_send: Res = .{},
    r9_parked: Res = .{},
    r9_after: Res = .{},
    // 10: a parked NetPoll is woken with success; the error stays.
    r10_send: Res = .{},
    r10_parked: Res = .{},
    r10_after: [2]Res = @splat(.{}),
    // 11: a queued datagram and the error: which first.
    r11_send: Res = .{},
    r11_recv: [3]Res = @splat(.{}),
    // 12: an unconnected socket never sees the error.
    r12_send: Res = .{},
    r12_recv: Res = .{},
    r12_send2: Res = .{},
    // 13: a second connect (refused by the sim).
    r13_same: ?Res = null,
    r13_other: ?Res = null,
    // 14: a stranger's datagram queued before the connect stays.
    r14_recv: Res = .{},
    r14_from_stranger: bool = false,
    // 15: a send with no address on an unconnected socket.
    r15_send: Res = .{},
    r15_peer_null: bool = false,
    // 16: two sends to a closed port back to back.
    r16_send: [2]Res = @splat(.{}),
    r16_recv: [2]Res = @splat(.{}),
    // 25: a send WITH an address while an error is pending, then a receive
    // (round 3 of the review, finding 1).
    r25_sendto: Res = .{},
    r25_recv: Res = .{},
    r25_other_got: Res = .{},
    // 17: a connected stranger, dropped by the filter, is refused.
    r17_send: Res = .{},
    r17_its_recv: Res = .{},
    r17_c_recv: Res = .{},
};

const macos_col: Got = .{
    .r1_local_lo = true,
    .r1_same_port = true,
    .r1_peer = true,
    .r2_before_zero = true,
    .r2_local_lo = true,
    .r2_peer = true,
    .r2_exchange = true,
    .r3_first_from_peer = true,
    .r3_second = would_block,
    .r4_send = ok1,
    .r4_peer_from_c = true,
    .r4_receive = .{ .n = 2 },
    .r5_to_peer = unexpected,
    .r5_to_other = unexpected,
    .r5_peer_got = would_block,
    .r5_other_got = would_block,
    .r6_send = ok1,
    .r6_poll_revents = 0x1,
    .r6_recv = .{ refused, would_block },
    .r7_send = .{ ok1, refused, ok1 },
    .r7_got = .{ ok1, would_block },
    .r8_so_error_refused = true,
    .r8_again_zero = true,
    .r8_recv = would_block,
    .r9_send = ok1,
    .r9_parked = refused,
    .r9_after = would_block,
    .r10_send = ok1,
    .r10_parked = ok0,
    .r10_after = .{ refused, would_block },
    .r11_send = ok1,
    .r11_recv = .{ .{ .n = 2 }, refused, would_block },
    .r12_send = ok1,
    .r12_recv = would_block,
    .r12_send2 = ok1,
    .r13_same = ok0,
    .r13_other = ok0,
    .r14_recv = ok1,
    .r14_from_stranger = true,
    .r15_send = unexpected,
    .r15_peer_null = true,
    .r16_send = .{ ok1, ok1 },
    .r16_recv = .{ refused, would_block },
    .r17_send = ok1,
    .r17_its_recv = refused,
    .r17_c_recv = would_block,
    .r25_sendto = refused,
    .r25_recv = would_block,
    .r25_other_got = would_block,
};

const linux_col: Got = blk: {
    var g = macos_col;
    // sendto with an address sends there.
    g.r5_to_peer = ok1;
    g.r5_to_other = ok1;
    g.r5_peer_got = ok1;
    g.r5_other_got = ok1;
    g.r6_poll_revents = 0x8;
    // The error before the queued datagram.
    g.r11_recv = .{ refused, .{ .n = 2 }, would_block };
    // The ICMP is back before the first send returns, so the second takes it.
    g.r16_send = .{ ok1, refused };
    g.r16_recv = .{ would_block, would_block };
    break :blk g;
};

/// The sim under a datagram profile: that kernel's column, with three
/// rows apart. Row 13 (a second connect) refuses under every profile (a
/// death test below). Row 15 keeps the sim's old answer for an
/// unconnected socket (t-4178). Row 16 under `.linux`: the sim always takes
/// a round trip, so both back-to-back sends succeed, as on macOS (a
/// schedule the sim does not model; the protocol above `dgramConnect`).
/// With no profile, rows 5, 6's poll and 11 refuse too (death tests below).
fn simColumn(p: ?sim.DatagramProfile) Got {
    var g = if (p == .linux) linux_col else macos_col;
    g.r13_same = null;
    g.r13_other = null;
    g.r15_send = .{ .err = error.OperationNotSupported };
    if (p == .linux) {
        g.r16_send = macos_col.r16_send;
        g.r16_recv = macos_col.r16_recv;
    }
    if (p == null) {
        g.r5_to_peer = null;
        g.r5_to_other = null;
        g.r5_peer_got = null;
        g.r5_other_got = null;
        g.r6_poll_revents = null;
        g.r11_send = .{};
        g.r11_recv = @splat(.{});
    }
    return g;
}

const parity = struct {
    var got: Got = .{};
    var parked: Res = .{};

    fn row1() !void {
        const p = try bound(lo, 0);
        defer p.close();
        var c = try bound(any, 0);
        defer c.close();
        const before = try local(c.handle);
        try c.connect(addr(lo, try portOf(p)), .{});
        const after = try local(c.handle);
        got.r1_local_lo = std.mem.eql(u8, &after.ip, &lo);
        got.r1_same_port = after.port == before.port and after.port != 0;
        got.r1_peer = std.meta.eql(try peer(c.handle), sim.Ip4{ .ip = lo, .port = try portOf(p) });
    }

    fn row2() !void {
        const p = try bound(lo, 0);
        defer p.close();
        var c = try zio_net.Socket.open(.dgram, .ipv4, .ip);
        defer c.close();
        got.r2_before_zero = std.meta.eql(try local(c.handle), sim.Ip4{ .ip = any, .port = 0 });
        try c.connect(addr(lo, try portOf(p)), .{});
        const after = try local(c.handle);
        got.r2_local_lo = std.mem.eql(u8, &after.ip, &lo) and after.port != 0;
        got.r2_peer = std.meta.eql(try peer(c.handle), sim.Ip4{ .ip = lo, .port = try portOf(p) });
        // The port the connect bound sends and receives.
        _ = try c.send("u", .none);
        _ = try p.sendTo(addr(lo, after.port), "v", .none);
        try pause();
        const a = recvNowFrom(p.handle);
        got.r2_exchange = a[0].n == 1 and a[1] == after.port and recvNow(c.handle).n == 1;
    }

    fn row3() !void {
        const p = try bound(lo, 0);
        defer p.close();
        const stranger = try bound(lo, 0);
        defer stranger.close();
        const c = try connected(try portOf(p));
        defer c.close();
        const cp = try portOf(c);
        _ = try stranger.sendTo(addr(lo, cp), "s", .none);
        _ = try p.sendTo(addr(lo, cp), "p", .none);
        try pause();
        const a = recvNowFrom(c.handle);
        got.r3_first_from_peer = a[0].n == 1 and a[1] == try portOf(p);
        got.r3_second = recvNow(c.handle);
    }

    fn row4() !void {
        const p = try bound(lo, 0);
        defer p.close();
        const c = try connected(try portOf(p));
        defer c.close();
        got.r4_send = res(c.send("x", .none));
        try pause();
        const a = recvNowFrom(p.handle);
        got.r4_peer_from_c = a[0].n == 1 and a[1] == try portOf(c);
        _ = try p.sendTo(addr(lo, try portOf(c)), "yy", .none);
        var buf: [8]u8 = undefined;
        got.r4_receive = res(c.receive(&buf, .{ .duration = .fromSeconds(1) }));
    }

    fn row5() !void {
        const p = try bound(lo, 0);
        defer p.close();
        const other = try bound(lo, 0);
        defer other.close();
        const c = try connected(try portOf(p));
        defer c.close();
        got.r5_to_peer = res(c.sendTo(addr(lo, try portOf(p)), "a", .none));
        got.r5_to_other = res(c.sendTo(addr(lo, try portOf(other)), "b", .none));
        try pause();
        got.r5_peer_got = recvNow(p.handle);
        got.r5_other_got = recvNow(other.handle);
    }

    fn row6() !void {
        const c = try connected(try closedPort());
        defer c.close();
        got.r6_send = res(c.send("x", .none));
        try pause();
        if (!sim.isRunning() or sim.datagramProfile() != null) {
            var fds = [1]os_net.pollfd{.{ .fd = c.handle, .events = os_net.POLL.IN, .revents = 0 }};
            _ = try os_net.poll(&fds, 0);
            got.r6_poll_revents = fds[0].revents;
        }
        got.r6_recv = .{ recvNow(c.handle), recvNow(c.handle) };
    }

    fn row7() !void {
        const x = try closedPort();
        const c = try connected(x);
        defer c.close();
        got.r7_send[0] = res(c.send("0", .none));
        try pause();
        const r = try bound(lo, x);
        defer r.close();
        got.r7_send[1] = res(c.send("1", .none));
        got.r7_send[2] = res(c.send("2", .none));
        try pause();
        got.r7_got = .{ recvNow(r.handle), recvNow(r.handle) };
    }

    fn row8() !void {
        const c = try connected(try closedPort());
        defer c.close();
        _ = try c.send("x", .none);
        try pause();
        var e: i32 = -1;
        try os_net.getsockopt(c.handle, os_net.SOL.SOCKET, os_net.SO.ERROR, std.mem.asBytes(&e));
        var e2: i32 = -1;
        try os_net.getsockopt(c.handle, os_net.SOL.SOCKET, os_net.SO.ERROR, std.mem.asBytes(&e2));
        got.r8_so_error_refused = e == @backingInt(std.posix.E.CONNREFUSED);
        got.r8_again_zero = e2 == 0;
        got.r8_recv = recvNow(c.handle);
    }

    fn parkedRecv(c: zio_net.Socket) void {
        var buf: [8]u8 = undefined;
        parked = res(c.receive(&buf, .{ .duration = .fromSeconds(2) }));
    }

    fn parkedPoll(c: zio_net.Socket) void {
        var op = ev.NetPoll.init(c.handle, .recv);
        io_common.timedWaitForIo(&op.c, .{ .duration = .fromSeconds(2) }) catch |err| {
            parked = .{ .err = err };
            return;
        };
        parked = res(op.getResult());
    }

    fn row9() !void {
        const c = try connected(try closedPort());
        defer c.close();
        parked = .{};
        var h = try rt_mod.spawn(parkedRecv, .{c});
        try pause();
        got.r9_send = res(c.send("x", .none));
        h.join();
        got.r9_parked = parked;
        got.r9_after = recvNow(c.handle);
    }

    fn row10() !void {
        const c = try connected(try closedPort());
        defer c.close();
        parked = .{};
        var h = try rt_mod.spawn(parkedPoll, .{c});
        try pause();
        got.r10_send = res(c.send("x", .none));
        h.join();
        got.r10_parked = parked;
        got.r10_after = .{ recvNow(c.handle), recvNow(c.handle) };
    }

    fn row11() !void {
        const p = try bound(lo, 0);
        const c = try connected(try portOf(p));
        defer c.close();
        _ = try p.sendTo(addr(lo, try portOf(c)), "dd", .none);
        try pause();
        p.close();
        got.r11_send = res(c.send("x", .none));
        try pause();
        got.r11_recv = .{ recvNow(c.handle), recvNow(c.handle), recvNow(c.handle) };
    }

    fn row12() !void {
        const x = try closedPort();
        const u = try bound(any, 0);
        defer u.close();
        got.r12_send = res(u.sendTo(addr(lo, x), "x", .none));
        try pause();
        got.r12_recv = recvNow(u.handle);
        got.r12_send2 = res(u.sendTo(addr(lo, x), "y", .none));
    }

    fn row13() !void {
        const p = try bound(lo, 0);
        defer p.close();
        const q = try bound(lo, 0);
        defer q.close();
        var c = try connected(try portOf(p));
        defer c.close();
        got.r13_same = res(c.connect(addr(lo, try portOf(p)), .{}));
        got.r13_other = res(c.connect(addr(lo, try portOf(q)), .{}));
    }

    fn row14() !void {
        const p = try bound(lo, 0);
        defer p.close();
        const stranger = try bound(lo, 0);
        defer stranger.close();
        var u = try bound(any, 0);
        defer u.close();
        _ = try stranger.sendTo(addr(lo, try portOf(u)), "s", .none);
        try pause();
        try u.connect(addr(lo, try portOf(p)), .{});
        const a = recvNowFrom(u.handle);
        got.r14_recv = a[0];
        got.r14_from_stranger = a[1] == try portOf(stranger);
    }

    fn row15() !void {
        const u = try bound(any, 0);
        defer u.close();
        var iov = [1]os_net.iovec_const{os_net.iovecConstFromSlice("x")};
        got.r15_send = res(os_net.send(u.handle, &iov, .{}));
        got.r15_peer_null = try peer(u.handle) == null;
    }

    fn row16() !void {
        const c = try connected(try closedPort());
        defer c.close();
        var iov = [1]os_net.iovec_const{os_net.iovecConstFromSlice("a")};
        got.r16_send = .{ res(os_net.send(c.handle, &iov, .{})), res(os_net.send(c.handle, &iov, .{})) };
        try pause();
        got.r16_recv = .{ recvNow(c.handle), recvNow(c.handle) };
    }

    fn row17() !void {
        const p = try bound(lo, 0);
        defer p.close();
        const c = try connected(try portOf(p));
        defer c.close();
        const s = try connected(try portOf(c));
        defer s.close();
        got.r17_send = res(s.send("x", .none));
        try pause();
        got.r17_its_recv = recvNow(s.handle);
        got.r17_c_recv = recvNow(c.handle);
    }

    fn row25() !void {
        const p = try bound(lo, 0);
        defer p.close();
        const c = try connected(try closedPort());
        defer c.close();
        _ = try c.send("x", .none);
        try pause();
        got.r25_sendto = res(c.sendTo(addr(lo, try portOf(p)), "a", .none));
        try pause();
        got.r25_recv = recvNow(c.handle);
        got.r25_other_got = recvNow(p.handle);
    }

    fn body() !void {
        got = .{};
        const in_sim = sim.isRunning();
        // In a sim run with no profile, the rows where the kernels differ
        // refuse (death tests below).
        const no_profile = in_sim and sim.datagramProfile() == null;
        try row1();
        try row2();
        try row3();
        try row4();
        if (!no_profile) try row5();
        try row6();
        try row7();
        try row8();
        try row9();
        try row10();
        if (!no_profile) try row11();
        try row12();
        if (!in_sim) try row13();
        try row14();
        try row15();
        try row16();
        try row17();
        try row25();
    }

    fn check(k: Kernel) !void {
        try checkAgainst(@tagName(k), switch (k) {
            .macos => macos_col,
            .linux => linux_col,
            .sim => simColumn(null),
        });
    }

    fn checkAgainst(name: []const u8, w: Got) !void {
        std.debug.print("dgram connect parity, {s} column: {any}\n", .{ name, got });
        expectEqual(w, got) catch |err| {
            std.debug.print("dgram connect parity, {s} column: want {any}\n", .{ name, w });
            return err;
        };
    }

    var profile: ?sim.DatagramProfile = null;

    fn bodyWithProfile() !void {
        if (profile) |p| sim.setDatagramProfile(p);
        try body();
    }

    /// The sim under `p`, against its column.
    fn inSim(p: ?sim.DatagramProfile) !void {
        profile = p;
        try t1.run(1, null, bodyWithProfile);
        try checkAgainst(if (p) |x| switch (x) {
            .linux => "sim, profile linux",
            .macos => "sim, profile macos",
        } else "sim, no profile", simColumn(p));
    }
};

const host_kernel: Kernel = if (builtin.os.tag.isDarwin()) .macos else .linux;

test "sim: dgram connect parity: 17 rows through zio.net, on this host's kernel without sim, in a sim run with sim" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    if (comptime sim.compiled_in) {
        try parity.inSim(null);
        try parity.inSim(.macos);
        try parity.inSim(.linux);
        return;
    }
    const rt = try rt_mod.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var h = try rt.spawn(parity.body, .{});
    try h.join();
    try parity.check(host_kernel);
}

// ---- Scenarios (sim only) ----

const Digests = struct { trace: u64, state: u64, wire: u64 };
var digests: Digests = undefined;

/// Records the run's digests; a body calls it last, after its closes.
fn capture() void {
    digests = .{ .trace = sim.traceHash(), .state = sim.stateDigest(), .wire = sim.wireDigest() };
}

var ended_at: u64 = 0;
var ended: Res = .{};

fn recvTask(c: zio_net.Socket) void {
    var buf: [8]u8 = undefined;
    ended = res(c.receive(&buf, .{ .duration = .fromSeconds(1) }));
    ended_at = sim.clockNs();
}

/// Starts a receive on `c` and lets it park.
fn parkReceive(c: zio_net.Socket) !rt_mod.JoinHandle(void) {
    ended = .{};
    ended_at = 0;
    const h = try rt_mod.spawn(recvTask, .{c});
    try rt_mod.sleep(.fromMicroseconds(10));
    try expect(ended_at == 0);
    return h;
}

const shape = struct {
    /// Hotseat's media socket: bound to 0.0.0.0:0, connected to the host;
    /// a ping by `Socket.send` (NetSend), a datagram by `Socket.receive`
    /// (NetRecv) parked first; then every other path with no address.
    fn body() !void {
        const h = try bound(lo, 0);
        defer h.close();
        const c = try connected(49152);
        defer c.close();
        try expectEqual(sim.Ip4{ .ip = lo, .port = 49153 }, try local(c.handle));
        var task = try parkReceive(c);
        const t0 = sim.clockNs();
        _ = try h.sendTo(addr(lo, 49153), "media", .none);
        task.join();
        try expectEqual(Res{ .n = 5 }, ended);
        try expectEqual(t0 + L, ended_at);
        try expectEqual(Res{ .n = 4 }, res(c.send("ping", .none)));
        try pause();
        const got = recvNowFrom(h.handle);
        try expectEqual(ok1.n.? + 3, got[0].n.?);
        try expectEqual(@as(u16, 49153), got[1]);
        // The layer's send and recv, sendmsg and NetSendMsg with no address,
        // and recvDatagram, whose source is the peer.
        var iov = [1]os_net.iovec_const{os_net.iovecConstFromSlice("a")};
        try expectEqual(ok1, res(os_net.send(c.handle, &iov, .{})));
        try expectEqual(ok1, res(os_net.sendmsg(c.handle, &iov, .{}, null, 0, null)));
        var op = ev.NetSendMsg.init(c.handle, .{ .iovecs = &iov }, .{}, null, 0, null);
        try io_common.waitForIo(&op.c);
        try expectEqual(ok1, res(op.getResult()));
        try pause();
        for (0..3) |_| try expectEqual(ok1, recvNow(h.handle));
        try expectEqual(would_block, recvNow(h.handle));
        _ = try h.sendTo(addr(lo, 49153), "bb", .none);
        _ = try h.sendTo(addr(lo, 49153), "c", .none);
        try pause();
        var buf: [8]u8 = undefined;
        var rv = [1]os_net.iovec{os_net.iovecFromSlice(&buf)};
        try expectEqual(Res{ .n = 2 }, res(os_net.recv(c.handle, &rv, .{ .dontwait = true })));
        var from: os_net.sockaddr.in = undefined;
        const info = try os_net.recvDatagram(c.handle, &rv, .{ .dontwait = true }, &from);
        try expectEqual(@as(usize, 1), info.len);
        try expectEqual(@as(u16, 49152), std.mem.bigToNative(u16, from.port));
        const st = sim.datagramStats(c.handle);
        try expectEqual(@as(u64, 4), st.sent);
        try expectEqual(@as(u64, 3), st.received);
        try expectEqual(@as(u64, 0), st.dropped_not_peer);
        capture();
    }
};

test "sim: dgram connect: Hotseat's shape: bound to 0.0.0.0:0, connected, a NetRecv parked first gets the host's datagram one latency after its send, and every path with no address goes to and comes from the peer" {
    if (comptime skip) return error.SkipZigTest;
    try t1.run(1, null, shape.body);
}

const filter = struct {
    /// Three datagrams from a stranger and one from the peer: only the
    /// peer's arrives; the stranger's are counted where they were dropped.
    fn body() !void {
        const p = try bound(lo, 0);
        defer p.close();
        const stranger = try bound(lo, 0);
        defer stranger.close();
        const c = try connected(49152);
        defer c.close();
        for (0..3) |_| _ = try stranger.sendTo(addr(lo, 49154), "s", .none);
        _ = try p.sendTo(addr(lo, 49154), "pp", .none);
        try pause();
        const a = recvNowFrom(c.handle);
        try expectEqual(Res{ .n = 2 }, a[0]);
        try expectEqual(@as(u16, 49152), a[1]);
        try expectEqual(would_block, recvNow(c.handle));
        const st = sim.datagramStats(c.handle);
        try expectEqual(@as(u64, 3), st.dropped_not_peer);
        try expectEqual(@as(u64, 1), st.received);
        try expectEqual(@as(u64, 0), sim.datagramsDroppedNoSocket());
        // The stranger is not connected: no ICMP, and it sees no error.
        try expectEqual(@as(u64, 0), sim.icmpsMade());
        try expectEqual(would_block, recvNow(stranger.handle));
        capture();
    }
};

test "sim: dgram connect: a connected socket drops a stranger's datagrams at the landing and counts them, and keeps its peer's" {
    if (comptime skip) return error.SkipZigTest;
    try t1.run(1, null, filter.body);
}

const icmp = struct {
    var x: u16 = 0;
    var cport: u16 = 0;

    fn reversed() sim.Path {
        return .{ .from = .{ .ip = lo, .port = x }, .to = .{ .ip = lo, .port = cport } };
    }

    /// A send to the closed peer port at t0 wakes the parked receive at
    /// t0 + 2L + `extra` with ConnectionRefused, or not at all (`null`).
    fn refusedAt(c: zio_net.Socket, extra: ?u64) !void {
        var task = try parkReceive(c);
        const t0 = sim.clockNs();
        try expectEqual(ok1, res(c.send("x", .none)));
        if (extra) |e| {
            task.join();
            try expectEqual(refused, ended);
            try expectEqual(t0 + 2 * L + e, ended_at);
        } else {
            try rt_mod.sleep(.fromMilliseconds(5));
            try expect(ended_at == 0);
            task.cancel();
        }
    }

    fn setUp() !zio_net.Socket {
        x = try closedPort();
        const c = try connected(x);
        cport = try portOf(c);
        return c;
    }

    fn plain() !void {
        const c = try setUp();
        defer c.close();
        try refusedAt(c, 0);
        try expectEqual(@as(u64, 1), sim.icmpsMade());
        try expectEqual(@as(u64, 1), sim.datagramStats(c.handle).refusals);
        capture();
    }

    fn delayed() !void {
        const c = try setUp();
        defer c.close();
        sim.setImpairment(reversed(), .{ .delay_ns = 1_000_000 });
        try refusedAt(c, 1_000_000);
        _ = sim.clearImpairment(reversed());
        capture();
    }

    fn partitioned() !void {
        const c = try setUp();
        defer c.close();
        sim.setPartition(reversed());
        try refusedAt(c, null);
        try expectEqual(@as(u64, 1), sim.netFaultStats().partitioned_at_send);
        _ = sim.healPartition(reversed());
        try expectEqual(would_block, recvNow(c.handle));
        capture();
    }

    fn partitionedInFlight() !void {
        const c = try setUp();
        defer c.close();
        try expectEqual(ok1, res(c.send("x", .none)));
        // The datagram dies at L; its ICMP is on the wire until 2L.
        try rt_mod.sleep(.fromMicroseconds(150));
        try expectEqual(@as(u64, 1), sim.icmpsMade());
        sim.setPartition(reversed());
        try pause();
        try expectEqual(@as(u64, 1), sim.netFaultStats().partitioned_in_flight);
        _ = sim.healPartition(reversed());
        try expectEqual(would_block, recvNow(c.handle));
        capture();
    }

    fn lost() !void {
        const c = try setUp();
        defer c.close();
        sim.setImpairment(reversed(), .{ .loss_ppm = 1_000_000 });
        try refusedAt(c, null);
        try expectEqual(@as(u64, 1), sim.netFaultStats().lost);
        _ = sim.clearImpairment(reversed());
        capture();
    }

    /// The forward path partitioned: the datagram never dies, so no ICMP.
    fn forwardPartitioned() !void {
        const c = try setUp();
        defer c.close();
        sim.setPartition(reversed().reversed());
        try refusedAt(c, null);
        try expectEqual(@as(u64, 0), sim.icmpsMade());
        _ = sim.healPartition(reversed().reversed());
        capture();
    }

    /// Jitter draws for the ICMP, from its own send numbers: each ICMP
    /// lands at its death plus L plus the draw the fault protocol's formula
    /// gives for send number `icmp_send_base + k`, computed here from that
    /// formula, not by the sim.
    fn jittered() !void {
        const c = try setUp();
        defer c.close();
        const jitter: u64 = 50_000;
        sim.setImpairment(reversed(), .{ .jitter_ns = jitter });
        var k: u64 = 0;
        while (k < 4) : (k += 1) {
            var task = try parkReceive(c);
            const t0 = sim.clockNs();
            _ = try c.send("x", .none);
            task.join();
            try expectEqual(refused, ended);
            try expectEqual(t0 + 2 * L + jitterDraw((1 << 60) + k) % (jitter + 1), ended_at);
        }
        try expectEqual(@as(u64, 4), sim.netFaultDraws());
        _ = sim.clearImpairment(reversed());
        capture();
    }
};

test "sim: dgram connect icmp: a send to a closed peer port wakes the parked receive at 2L with ConnectionRefused" {
    if (comptime skip) return error.SkipZigTest;
    try t1.run(1, null, icmp.plain);
}

test "sim: dgram connect icmp: a delay on the reversed path delays the error by as much" {
    if (comptime skip) return error.SkipZigTest;
    try t1.run(1, null, icmp.delayed);
}

test "sim: dgram connect icmp: a partition of the reversed path at the death drops the ICMP, and no error is ever pending" {
    if (comptime skip) return error.SkipZigTest;
    try t1.run(1, null, icmp.partitioned);
}

test "sim: dgram connect icmp: a partition set while the ICMP is in flight drops it at its landing" {
    if (comptime skip) return error.SkipZigTest;
    try t1.run(1, null, icmp.partitionedInFlight);
}

test "sim: dgram connect icmp: loss on the reversed path loses the ICMP" {
    if (comptime skip) return error.SkipZigTest;
    try t1.run(1, null, icmp.lost);
}

test "sim: dgram connect icmp: a partition of the forward path drops the datagram at its send, so nothing dies and no ICMP is made" {
    if (comptime skip) return error.SkipZigTest;
    try t1.run(1, null, icmp.forwardPartitioned);
}

const reuse = struct {
    /// c sends to the closed x and closes while the ICMP is in flight; c2
    /// binds c's address and connects to a live peer. The ICMP lands at c2,
    /// which is not connected to x: no error.
    fn body() !void {
        const p = try bound(lo, 0);
        defer p.close();
        const x = try closedPort();
        var c = try bound(lo, 0);
        const cp = try portOf(c);
        try c.connect(addr(lo, x), .{});
        _ = try c.send("x", .none);
        try rt_mod.sleep(.fromMicroseconds(150));
        try expectEqual(@as(u64, 1), sim.icmpsMade());
        c.close();
        var c2 = try bound(lo, cp);
        defer c2.close();
        try c2.connect(addr(lo, try portOf(p)), .{});
        try pause();
        try expectEqual(would_block, recvNow(c2.handle));
        try expectEqual(@as(u64, 0), sim.datagramStats(c2.handle).refusals);
        capture();
    }
};

test "sim: dgram connect icmp: a socket that took a closed sender's address and connected elsewhere does not take that sender's ICMP" {
    if (comptime skip) return error.SkipZigTest;
    try t1.run(1, null, reuse.body);
}

const unconnected = struct {
    /// In a run that connected a socket, an unconnected sender's datagram to
    /// a closed port makes no ICMP.
    fn body() !void {
        const p = try bound(lo, 0);
        defer p.close();
        const c = try connected(49152);
        defer c.close();
        const u = try bound(any, 0);
        defer u.close();
        _ = try u.sendTo(addr(lo, try closedPort()), "x", .none);
        try pause();
        try expectEqual(@as(u64, 0), sim.icmpsMade());
        try expectEqual(@as(u64, 1), sim.datagramsDroppedNoSocket());
        try expectEqual(would_block, recvNow(u.handle));
        capture();
    }
};

test "sim: dgram connect icmp: an unconnected sender to a closed port makes no ICMP, in a run that connected another socket" {
    if (comptime skip) return error.SkipZigTest;
    try t1.run(1, null, unconnected.body);
}

/// SplitMix64's output function, and the draw of the fault protocol above
/// `pub const Endpoint` in sim.zig for the first copy's jitter (purpose 4).
fn mix64(x: u64) u64 {
    var z = x;
    z = (z ^ (z >> 30)) *% 0xbf58476d1ce4e5b9;
    z = (z ^ (z >> 27)) *% 0x94d049bb133111eb;
    return z ^ (z >> 31);
}

fn jitterDraw(send_no: u64) u64 {
    const base = mix64(sim.seed() ^ 0x6e65745f6661756c);
    return mix64(base +% (send_no *% 8 +% 4) *% 0x9e3779b97f4a7c15);
}

test "sim: dgram connect icmp: jitter on the reversed path lands each ICMP at the draw of its own send number" {
    if (comptime skip) return error.SkipZigTest;
    try t1.run(1, null, icmp.jittered);
}

// ---- Determinism and the digests ----

fn parityBody() !void {
    try parity.body();
    capture();
}

test "sim: dgram connect: the same seed twice gives the same trace, state digest and wire digest, for every scenario and the parity rows" {
    if (comptime skip) return error.SkipZigTest;
    const bodies = .{ shape.body, filter.body, icmp.plain, icmp.delayed, icmp.partitioned, icmp.partitionedInFlight, icmp.lost, icmp.forwardPartitioned, icmp.jittered, reuse.body, unconnected.body, parityBody };
    inline for (bodies, 0..) |b, i| {
        try t1.run(7, null, b);
        const first = digests;
        try t1.run(7, null, b);
        expectEqual(first, digests) catch |err| {
            std.debug.print("scenario {d}: the second run differs\n", .{i});
            return err;
        };
    }
}

const profiles = struct {
    var p: sim.DatagramProfile = .macos;

    fn body() !void {
        sim.setDatagramProfile(p);
        const a = try bound(lo, 0);
        defer a.close();
        const c = try connected(try portOf(a));
        defer c.close();
        try pause();
        capture();
    }
};

test "sim: dgram connect: the state digest sees the datagram profile of a run that connected" {
    if (comptime skip) return error.SkipZigTest;
    profiles.p = .macos;
    try t1.run(1, null, profiles.body);
    const m = digests;
    profiles.p = .linux;
    try t1.run(1, null, profiles.body);
    try expectEqual(m.trace, digests.trace);
    try expect(m.state != digests.state);
}

const peers = struct {
    var to: u16 = 0;
    var stranger_sends: bool = false;

    /// c connects to `to` (49152 or 49153); with `stranger_sends`, 49153
    /// sends c one datagram.
    fn body() !void {
        const a = try bound(lo, 0);
        defer a.close();
        const b = try bound(lo, 0);
        defer b.close();
        const c = try connected(to);
        defer c.close();
        if (stranger_sends) _ = try b.sendTo(addr(lo, try portOf(c)), "s", .none);
        try pause();
        capture();
    }
};

test "sim: dgram connect: the state digest sees the peer, and the wire digest sees a datagram the filter dropped" {
    if (comptime skip) return error.SkipZigTest;
    peers.stranger_sends = false;
    peers.to = 49152;
    try t1.run(1, null, peers.body);
    const to_a = digests;
    peers.to = 49153;
    try t1.run(1, null, peers.body);
    try expect(to_a.state != digests.state);
    try expectEqual(to_a.trace, digests.trace);
    // The same send, dropped by the filter (peer 49152) or landed (49153).
    peers.stranger_sends = true;
    peers.to = 49152;
    try t1.run(1, null, peers.body);
    const dropped = digests;
    peers.to = 49153;
    try t1.run(1, null, peers.body);
    try expect(dropped.wire != digests.wire);
    try expect(dropped.state != digests.state);
}

const outcome = struct {
    var close_first: bool = false;

    /// A stranger's datagram lands at c's address: dropped by c's filter,
    /// or, with c closed first, at no socket. Nothing else lands, so the
    /// two wire digests differ only in that record's outcome.
    fn body() !void {
        const p = try bound(lo, 0);
        defer p.close();
        const s = try bound(lo, 0);
        defer s.close();
        const c = try connected(49152);
        const cp = try portOf(c);
        if (close_first) c.close();
        _ = try s.sendTo(addr(lo, cp), "s", .none);
        try pause();
        if (!close_first) c.close();
        capture();
    }
};

test "sim: dgram connect: the wire digest records a datagram the filter dropped apart from one that found no socket at the same address" {
    if (comptime skip) return error.SkipZigTest;
    outcome.close_first = false;
    try t1.run(1, null, outcome.body);
    const filtered = digests;
    outcome.close_first = true;
    try t1.run(1, null, outcome.body);
    try expect(filtered.wire != digests.wire);
}

// ---- Refusals ----

test "sim: dgram connect refuses: a second connect" {
    try t1.refusal("sim: dgram connect refuses: a second connect", "sim: a second connect on sim datagram socket 100033: both kernels allow it and replace the peer; not simulated", null, struct {
        fn body() !void {
            const p = try bound(lo, 0);
            defer p.close();
            var c = try connected(49152);
            defer c.close();
            try c.connect(addr(lo, 49152), .{});
        }
    }.body);
}

test "sim: dgram connect refuses: a disconnect (AF_UNSPEC)" {
    try t1.refusal("sim: dgram connect refuses: a disconnect (AF_UNSPEC)", "sim: connect with AF_UNSPEC (a disconnect) on sim datagram socket 100033 is not simulated", null, struct {
        fn body() !void {
            const p = try bound(lo, 0);
            defer p.close();
            const c = try connected(49152);
            defer c.close();
            var sa = std.mem.zeroes(os_net.sockaddr);
            sa.family = os_net.AF.UNSPEC;
            try os_net.connect(c.handle, &sa, @sizeOf(os_net.sockaddr));
        }
    }.body);
}

test "sim: dgram connect refuses: sendTo with an address on a connected socket, with no datagram profile" {
    try t1.refusal("sim: dgram connect refuses: sendTo with an address on a connected socket, with no datagram profile", "sim: a send with an address on a connected socket (row 5) on sim datagram socket 100033: the kernels differ here, and this run set no datagram profile", null, struct {
        fn body() !void {
            const p = try bound(lo, 0);
            defer p.close();
            const c = try connected(49152);
            defer c.close();
            _ = try c.sendTo(addr(lo, 49152), "x", .none);
        }
    }.body);
}

test "sim: dgram connect refuses: a layer poll of a socket with a pending error, with no datagram profile" {
    try t1.refusal("sim: dgram connect refuses: a layer poll of a socket with a pending error, with no datagram profile", "sim: a poll with an error pending (row 6) on sim datagram socket 100032: the kernels differ here, and this run set no datagram profile", null, struct {
        fn body() !void {
            const x = try closedPort();
            const c = try connected(x);
            defer c.close();
            _ = try c.send("x", .none);
            try pause();
            var fds = [1]os_net.pollfd{.{ .fd = c.handle, .events = os_net.POLL.IN, .revents = 0 }};
            _ = try os_net.poll(&fds, 0);
        }
    }.body);
}

test "sim: dgram connect refuses: shutdown of a connected socket" {
    try t1.refusal("sim: dgram connect refuses: shutdown of a connected socket", "sim: shutdown of connected sim datagram socket 100033 is not simulated", null, struct {
        fn body() !void {
            const p = try bound(lo, 0);
            defer p.close();
            const c = try connected(49152);
            defer c.close();
            try os_net.shutdown(c.handle, .both);
        }
    }.body);
}

test "sim: dgram connect refuses: a connect that makes a dead datagram's four-tuple while its ICMP could be on its way, in a run that set a fault rule" {
    try t1.refusal("sim: dgram connect refuses: a connect that makes a dead datagram's four-tuple while its ICMP could be on its way, in a run that set a fault rule", "sim: connect of sim datagram socket 100032 to 127.0.0.1:49152, where a datagram from its address died with no ICMP made", null, struct {
        fn body() !void {
            const x = try closedPort();
            var u = try bound(lo, 0);
            defer u.close();
            // A rule on another path: the ICMP's own fate was never drawn.
            const other: sim.Path = .{ .from = .{ .ip = .{ 127, 0, 0, 9 } }, .to = .{ .ip = .{ 127, 0, 0, 9 } } };
            sim.setPartition(other);
            _ = try u.sendTo(addr(lo, x), "x", .none);
            try rt_mod.sleep(.fromMicroseconds(150));
            try u.connect(addr(lo, x), .{});
        }
    }.body);
}

test "sim: dgram connect: with a fault rule set, a connect to another peer, or one after the bound, is not refused" {
    if (comptime skip) return error.SkipZigTest;
    try t1.run(1, null, struct {
        fn body() !void {
            const p = try bound(lo, 0);
            defer p.close();
            const x = try closedPort();
            var u = try bound(lo, 0);
            defer u.close();
            const other: sim.Path = .{ .from = .{ .ip = .{ 127, 0, 0, 9 } }, .to = .{ .ip = .{ 127, 0, 0, 9 } } };
            sim.setPartition(other);
            _ = try u.sendTo(addr(lo, x), "x", .none);
            try rt_mod.sleep(.fromMicroseconds(150));
            try u.connect(addr(lo, try portOf(p)), .{});
            _ = sim.healPartition(other);
        }
    }.body);
}

// Round 3, finding 2: the overflow refuses only a connect whose four-tuple
// was dropped (its bucket); `... 257 distinct deaths at one instant: an
// unrelated connect is not refused ...` shows the other side.
test "sim: dgram connect refuses: a connect whose four-tuple the full ghost table dropped" {
    try t1.refusal("sim: dgram connect refuses: a connect whose four-tuple the full ghost table dropped", "sim: connect of sim datagram socket 100032 to 127.0.0.1:60257: the ghost table was full", null, struct {
        fn body() !void {
            var u = try bound(lo, 0);
            defer u.close();
            // 257 distinct four-tuples die at L; the 257th goes to its bucket.
            var k: u16 = 0;
            while (k < sim.max_ghosts) : (k += 1) _ = try u.sendTo(addr(lo, 60001 + k), "x", .none);
            // The 257th dies 10 us after the others (flows due at one
            // instant land in a seeded order), so it is the one dropped.
            try rt_mod.sleep(.fromMicroseconds(10));
            _ = try u.sendTo(addr(lo, 60001 + sim.max_ghosts), "x", .none);
            try rt_mod.sleep(.fromMicroseconds(150));
            try u.connect(addr(lo, 60001 + sim.max_ghosts), .{});
        }
    }.body);
}

test "sim: dgram connect refuses: a receive with a datagram queued and an error pending, with no datagram profile" {
    try t1.refusal("sim: dgram connect refuses: a receive with a datagram queued and an error pending, with no datagram profile", "sim: a receive with a datagram queued and an error pending (row 11) on sim datagram socket 100033: the kernels differ here, and this run set no datagram profile", null, struct {
        fn body() !void {
            const p = try bound(lo, 0);
            const c = try connected(try portOf(p));
            defer c.close();
            _ = try p.sendTo(addr(lo, try portOf(c)), "dd", .none);
            try pause();
            p.close();
            _ = try c.send("x", .none);
            try pause();
            _ = recvNow(c.handle);
        }
    }.body);
}

test "sim: dgram connect refuses: setDatagramProfile after the run's first datagram socket" {
    try t1.refusal("sim: dgram connect refuses: setDatagramProfile after the run's first datagram socket", "sim: setDatagramProfile(linux) after the run's first datagram socket opened: set it before", null, struct {
        fn body() !void {
            const p = try bound(lo, 0);
            defer p.close();
            sim.setDatagramProfile(.linux);
        }
    }.body);
}

test "sim: dgram connect refuses: a second setDatagramProfile" {
    try t1.refusal("sim: dgram connect refuses: a second setDatagramProfile", "sim: setDatagramProfile(linux) after setDatagramProfile(macos): a run has one profile", null, struct {
        fn body() !void {
            sim.setDatagramProfile(.macos);
            sim.setDatagramProfile(.linux);
        }
    }.body);
}

test "sim: dgram connect refuses: the 2^60th datagram send of a run that connected" {
    try t1.refusal("sim: dgram connect refuses: the 2^60th datagram send of a run that connected", "sim: datagram send number 1152921504606846976 on sim fd 100032: from 2^60 on", null, struct {
        fn body() !void {
            var u = try bound(lo, 0);
            defer u.close();
            try u.connect(addr(lo, 9), .{});
            sim.testSetWireSeq(1 << 60);
            _ = try u.send("x", .none);
        }
    }.body);
}

test "sim: dgram connect refuses: a connect after 2^60 datagram sends" {
    try t1.refusal("sim: dgram connect refuses: a connect after 2^60 datagram sends", "sim: connect of sim datagram socket 100032 after 1152921504606846976 datagram sends", null, struct {
        fn body() !void {
            var u = try bound(lo, 0);
            defer u.close();
            sim.testSetWireSeq(1 << 60);
            try u.connect(addr(lo, 9), .{});
        }
    }.body);
}

test "sim: dgram connect refuses: a connect to port 0" {
    try t1.refusal("sim: dgram connect refuses: a connect to port 0", "sim: connect of sim datagram socket 100032 to port 0", null, struct {
        fn body() !void {
            var u = try bound(any, 0);
            defer u.close();
            try u.connect(addr(lo, 0), .{});
        }
    }.body);
}

test "sim: dgram connect refuses: sim.end with a connected datagram socket open" {
    try t1.refusal("sim: dgram connect refuses: sim.end with a connected datagram socket open", "sim: datagram teardown: sim fd 100033 is an open datagram socket", null, struct {
        fn body() !void {
            const p = try bound(lo, 0);
            defer p.close();
            _ = try connected(49152);
        }
    }.body);
}

// ---- Round 2 of the review (gpt-simnet18.md) ----

const r2 = struct {
    var from_res: Res = .{};

    fn parkRecvFrom(u: zio_net.Socket) void {
        var buf: [8]u8 = undefined;
        from_res = res(if (u.receiveFrom(&buf, .{ .duration = .fromSeconds(1) })) |r| r.len else |e| e);
    }

    /// Finding 4: a NetRecv on an UNconnected socket ends OperationNotSupported
    /// at once, as before t-4167, also while a NetRecvFrom is parked there.
    fn recvBesideParked() !void {
        const u = try bound(any, 0);
        from_res = .{};
        var h = try rt_mod.spawn(parkRecvFrom, .{u});
        try rt_mod.sleep(.fromMicroseconds(10));
        var buf: [8]u8 = undefined;
        try expectEqual(Res{ .err = error.OperationNotSupported }, res(u.receive(&buf, .none)));
        u.close();
        h.join();
        try expectEqual(Res{ .err = error.FileDescriptorNotASocket }, from_res);
        capture();
    }

    /// The same shape: the layer's send, recv and shutdown, and a NetRecv,
    /// on a CLOSED datagram fd keep their answers from before t-4167.
    fn closedFd() !void {
        const u = try bound(any, 0);
        const fd = u.handle;
        u.close();
        var iov = [1]os_net.iovec_const{os_net.iovecConstFromSlice("x")};
        try expectEqual(Res{ .err = error.OperationNotSupported }, res(os_net.send(fd, &iov, .{})));
        var buf: [8]u8 = undefined;
        var rv = [1]os_net.iovec{os_net.iovecFromSlice(&buf)};
        try expectEqual(Res{ .err = error.OperationNotSupported }, res(os_net.recv(fd, &rv, .{})));
        try expectEqual(Res{ .err = error.SocketUnconnected }, res(os_net.shutdown(fd, .both)));
        const s: zio_net.Socket = .{ .handle = fd, .address = undefined };
        try expectEqual(Res{ .err = error.OperationNotSupported }, res(s.receive(&buf, .none)));
        capture();
    }

    /// Finding 3, first sequence: a socket that sent to a closed port
    /// connects later to another, live peer, and works.
    fn otherPeer() !void {
        const p = try bound(lo, 0);
        defer p.close();
        var u = try bound(any, 0);
        defer u.close();
        _ = try u.sendTo(addr(lo, try closedPort()), "x", .none);
        try pause();
        try u.connect(addr(lo, try portOf(p)), .{});
        try expectEqual(ok1, res(u.send("y", .none)));
        try pause();
        try expectEqual(ok1, recvNow(p.handle));
        try expectEqual(would_block, recvNow(u.handle));
        capture();
    }

    /// Finding 3, second sequence: that socket closes; a new one binds its
    /// address and connects elsewhere.
    fn rebound() !void {
        const p = try bound(lo, 0);
        defer p.close();
        const x = try closedPort();
        const u = try bound(lo, 0);
        const up = try portOf(u);
        _ = try u.sendTo(addr(lo, x), "x", .none);
        try pause();
        u.close();
        var v = try bound(lo, up);
        defer v.close();
        try v.connect(addr(lo, try portOf(p)), .{});
        try expectEqual(ok1, res(v.send("y", .none)));
        try pause();
        try expectEqual(ok1, recvNow(p.handle));
        capture();
    }

    /// The case that must be modelled: the socket sent to the closed port
    /// while unconnected, and connects to it before the ICMP lands (it sent
    /// at t0; the datagram died at t0 + L; the ICMP lands at t0 + 2L). Both
    /// kernels look the socket up when the ICMP arrives.
    fn connectInFlight() !void {
        const x = try closedPort();
        var u = try bound(lo, 0);
        defer u.close();
        const t0 = sim.clockNs();
        _ = try u.sendTo(addr(lo, x), "x", .none);
        try rt_mod.sleep(.fromMicroseconds(150));
        try u.connect(addr(lo, x), .{});
        var task = try parkReceive(u);
        task.join();
        try expectEqual(refused, ended);
        try expectEqual(t0 + 2 * L, ended_at);
        capture();
    }

    /// Finding 5: c, connected to the closed x, sends; it closes while the
    /// ICMP is in flight; a new socket binds c's address and connects to x.
    /// The ICMP finds the new socket (the current four-tuple).
    fn replacement() !void {
        const x = try closedPort();
        var c = try bound(lo, 0);
        const cp = try portOf(c);
        try c.connect(addr(lo, x), .{});
        const t0 = sim.clockNs();
        _ = try c.send("x", .none);
        try rt_mod.sleep(.fromMicroseconds(150));
        c.close();
        var c2 = try bound(lo, cp);
        defer c2.close();
        try c2.connect(addr(lo, x), .{});
        var task = try parkReceive(c2);
        task.join();
        try expectEqual(refused, ended);
        try expectEqual(t0 + 2 * L, ended_at);
        capture();
    }
};

const ghost_other = struct {
    /// Row 23 in the sim: another socket connects to the dead address while
    /// the sender's ICMP is in flight. It makes no ICMP: the ghost's
    /// four-tuple is the sender's address, not this socket's.
    fn body() !void {
        const x = try closedPort();
        const u = try bound(lo, 0);
        defer u.close();
        _ = try u.sendTo(addr(lo, x), "x", .none);
        try rt_mod.sleep(.fromMicroseconds(150));
        var v = try bound(lo, 0);
        defer v.close();
        try v.connect(addr(lo, x), .{});
        try pause();
        try expectEqual(@as(u64, 0), sim.icmpsMade());
        try expectEqual(would_block, recvNow(v.handle));
        capture();
    }
};

const ghost_peer = struct {
    /// Row 20 in the sim: the sender connects to another, live peer while
    /// its ICMP is in flight. No ICMP is made: not the ghost's four-tuple.
    fn body() !void {
        const p = try bound(lo, 0);
        defer p.close();
        const x = try closedPort();
        var u = try bound(lo, 0);
        defer u.close();
        _ = try u.sendTo(addr(lo, x), "x", .none);
        try rt_mod.sleep(.fromMicroseconds(150));
        try u.connect(addr(lo, try portOf(p)), .{});
        try pause();
        try expectEqual(@as(u64, 0), sim.icmpsMade());
        try expectEqual(would_block, recvNow(u.handle));
        capture();
    }
};

test "sim: dgram connect r2: a sender that connects to another peer while its ICMP is in flight makes no ICMP" {
    if (comptime skip) return error.SkipZigTest;
    try t1.run(1, null, ghost_peer.body);
}

test "sim: dgram connect r2: another socket that connects to a dead address while a sender's ICMP is in flight makes no ICMP" {
    if (comptime skip) return error.SkipZigTest;
    try t1.run(1, null, ghost_other.body);
}

test "sim: dgram connect r2: a NetRecv on an unconnected socket with a NetRecvFrom parked ends OperationNotSupported, as before t-4167" {
    if (comptime skip) return error.SkipZigTest;
    try t1.run(1, null, r2.recvBesideParked);
}

test "sim: dgram connect r2: send, recv, shutdown and NetRecv on a closed datagram fd keep their answers from before t-4167" {
    if (comptime skip) return error.SkipZigTest;
    try t1.run(1, null, r2.closedFd);
}

test "sim: dgram connect r2: a socket that sent to a closed port later connects to another, live peer" {
    if (comptime skip) return error.SkipZigTest;
    try t1.run(1, null, r2.otherPeer);
}

test "sim: dgram connect r2: after a socket that sent to a closed port closes, a new one binds its address and connects elsewhere" {
    if (comptime skip) return error.SkipZigTest;
    try t1.run(1, null, r2.rebound);
}

test "sim: dgram connect r2: a socket that sent to a closed port and connects to it before the ICMP lands gets the refusal at 2L" {
    if (comptime skip) return error.SkipZigTest;
    try t1.run(1, null, r2.connectInFlight);
}

test "sim: dgram connect r2: a new socket that binds a closed sender's address and connects to the same peer while the ICMP is in flight gets the refusal" {
    if (comptime skip) return error.SkipZigTest;
    try t1.run(1, null, r2.replacement);
}

// ---- In-flight rows: what happens while an ICMP is on its way back ----
//
// On real loopback the ICMP comes back in microseconds (Linux: before the
// send returns), so nothing can happen between. These rows run on a kernel
// only with a delay on loopback: Linux in a container with
// `tc qdisc add dev lo root netem delay <ms>ms` and ZIO_NETEM_MS=<ms> set;
// then every datagram and ICMP takes that long, and the actions below run
// between the death and the ICMP. macOS needs root for that (dummynet), so
// its column is not run. In a sim run they always run, with the sim's L.

const InFlight = struct {
    r18_replacement: Res = .{},
    r19_connect_to_it: Res = .{},
    r20_other_peer: Res = .{},
    r20_exchange: bool = false,
    r21_sends: [2]Res = @splat(.{}),
    r21_recv: [2]Res = @splat(.{}),
    r22_rebound_same_peer: Res = .{},
    r23_other_address: Res = .{},
    r24_connect_after: Res = .{},
};

const in_flight_want: InFlight = .{
    .r18_replacement = refused,
    .r19_connect_to_it = refused,
    .r20_other_peer = would_block,
    .r20_exchange = true,
    .r21_sends = .{ ok1, ok1 },
    .r21_recv = .{ refused, would_block },
    .r22_rebound_same_peer = refused,
    .r23_other_address = would_block,
    .r24_connect_after = would_block,
};

const in_flight = struct {
    var got: InFlight = .{};
    var delay_ms: u64 = 0;

    /// Inside the window, at 1.5 path delays after the send: the datagram
    /// has died (one delay) and its ICMP is not back yet (two delays). So
    /// the change is made after the death, and the row shows whether the
    /// recipient is chosen at the death or looked up at the arrival (round
    /// 3 of the review, finding 4). Each row prints the time it made its
    /// change, measured from its send.
    fn soon(row: []const u8, t0: zio_time.Timestamp) !void {
        if (sim.isRunning()) try rt_mod.sleep(.fromMicroseconds(L * 3 / 2 / 1000)) else try rt_mod.sleep(.fromMilliseconds(delay_ms * 3 / 2));
        const ns = t0.durationTo(zio_time.Timestamp.now(.monotonic)).toNanoseconds();
        const d: u64 = if (sim.isRunning()) L else delay_ms * std.time.ns_per_ms;
        std.debug.print("in-flight {s}: change at {d} us after the send (one path delay {d} us)\n", .{ row, @divTrunc(ns, 1000), d / 1000 });
        if (ns <= d or ns >= 2 * d) return error.ChangeOutsideTheWindow;
    }

    fn sentAt() zio_time.Timestamp {
        return zio_time.Timestamp.now(.monotonic);
    }

    /// After the ICMP is back.
    fn settle() !void {
        if (sim.isRunning()) try rt_mod.sleep(.fromMilliseconds(1)) else try rt_mod.sleep(.fromMilliseconds(delay_ms * 4));
    }

    fn row18() !void {
        const x = try closedPort();
        var c = try bound(lo, 0);
        const cp = try portOf(c);
        try c.connect(addr(lo, x), .{});
        const t0 = sentAt();
        _ = try c.send("x", .none);
        try soon("row 18", t0);
        c.close();
        var c2 = try bound(lo, cp);
        defer c2.close();
        try c2.connect(addr(lo, x), .{});
        try settle();
        got.r18_replacement = recvNow(c2.handle);
    }

    fn row19() !void {
        const x = try closedPort();
        var u = try bound(lo, 0);
        defer u.close();
        const t0 = sentAt();
        _ = try u.sendTo(addr(lo, x), "x", .none);
        try soon("row 19", t0);
        try u.connect(addr(lo, x), .{});
        try settle();
        got.r19_connect_to_it = recvNow(u.handle);
    }

    fn row20() !void {
        const p = try bound(lo, 0);
        defer p.close();
        const x = try closedPort();
        var u = try bound(lo, 0);
        defer u.close();
        const t0 = sentAt();
        _ = try u.sendTo(addr(lo, x), "x", .none);
        try soon("row 20", t0);
        try u.connect(addr(lo, try portOf(p)), .{});
        _ = try u.send("y", .none);
        try settle();
        got.r20_other_peer = recvNow(u.handle);
        got.r20_exchange = recvNow(p.handle).n == 1;
    }

    fn row21() !void {
        const c = try connected(try closedPort());
        defer c.close();
        var iov = [1]os_net.iovec_const{os_net.iovecConstFromSlice("a")};
        got.r21_sends = .{ res(os_net.send(c.handle, &iov, .{})), res(os_net.send(c.handle, &iov, .{})) };
        try settle();
        got.r21_recv = .{ recvNow(c.handle), recvNow(c.handle) };
    }

    fn row22() !void {
        const x = try closedPort();
        const u = try bound(lo, 0);
        const up = try portOf(u);
        const t0 = sentAt();
        _ = try u.sendTo(addr(lo, x), "x", .none);
        try soon("row 22", t0);
        u.close();
        var v = try bound(lo, up);
        defer v.close();
        try v.connect(addr(lo, x), .{});
        try settle();
        got.r22_rebound_same_peer = recvNow(v.handle);
    }

    /// Another socket, on another address, connects to the dead address
    /// while the sender's ICMP is in flight: not its four-tuple.
    fn row23() !void {
        const x = try closedPort();
        const u = try bound(lo, 0);
        defer u.close();
        const t0 = sentAt();
        _ = try u.sendTo(addr(lo, x), "x", .none);
        try soon("row 23", t0);
        var v = try bound(lo, 0);
        defer v.close();
        try v.connect(addr(lo, x), .{});
        try settle();
        got.r23_other_address = recvNow(v.handle);
    }

    /// The sender connects to the dead address after its ICMP came back to
    /// it unconnected: nothing is pending.
    fn row24() !void {
        const x = try closedPort();
        var u = try bound(lo, 0);
        defer u.close();
        _ = try u.sendTo(addr(lo, x), "x", .none);
        try settle();
        try u.connect(addr(lo, x), .{});
        try settle();
        got.r24_connect_after = recvNow(u.handle);
    }

    fn body() !void {
        got = .{};
        try row18();
        try row23();
        try row24();
        try row19();
        try row20();
        try row21();
        try row22();
        capture();
    }

    fn check(column: []const u8) !void {
        std.debug.print("dgram connect in-flight rows, {s} column: {any}\n", .{ column, got });
        expectEqual(in_flight_want, got) catch |err| {
            std.debug.print("dgram connect in-flight rows, {s} column: want {any}\n", .{ column, in_flight_want });
            return err;
        };
    }
};

test "sim: dgram connect in-flight rows: a connect or a new socket while the ICMP is on its way back, on Linux with netem on loopback without sim, in a sim run with sim" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    if (comptime sim.compiled_in) {
        try t1.run(1, null, in_flight.body);
        return in_flight.check("sim");
    }
    const ms = std.c.getenv("ZIO_NETEM_MS") orelse return error.SkipZigTest;
    in_flight.delay_ms = try std.fmt.parseInt(u64, std.mem.span(ms), 10);
    const rt = try rt_mod.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var h = try rt.spawn(in_flight.body, .{});
    try h.join();
    try in_flight.check("linux with netem");
}

// ---- Round 3 of the review (gpt-simnet19.md) ----

const unrelated_path: sim.Path = .{ .from = .{ .ip = .{ 127, 0, 0, 9 } }, .to = .{ .ip = .{ 127, 0, 0, 9 } } };

const r3 = struct {
    /// Finding 2, Hotseat's shape: a fault rule set (on an unrelated path),
    /// a host sends 300 media datagrams to a client port that has closed,
    /// then an unrelated socket connects to a live peer. It must not be
    /// refused: none of the deaths has its four-tuple.
    fn manyDeathsUnrelatedConnect() !void {
        sim.setPartition(unrelated_path);
        const p = try bound(lo, 0);
        defer p.close();
        const x = try closedPort();
        const h = try bound(lo, 0);
        defer h.close();
        for (0..300) |_| {
            _ = try h.sendTo(addr(lo, x), "media", .none);
            try rt_mod.sleep(.fromMicroseconds(10));
        }
        try pause();
        var v = try bound(lo, 0);
        defer v.close();
        try v.connect(addr(lo, try portOf(p)), .{});
        try expectEqual(ok1, res(v.send("y", .none)));
        _ = sim.healPartition(unrelated_path);
        capture();
    }

    /// Finding 2 with no fault rule: 257 distinct four-tuples die at one
    /// instant (one more than the table holds). Another socket connects to
    /// a live peer: not refused. The sender connects to the first dead port
    /// in time: its ICMP is made, exactly at 2L.
    fn overflowUnrelated() !void {
        const p = try bound(lo, 0);
        defer p.close();
        var u = try bound(lo, 0);
        defer u.close();
        const t0 = sim.clockNs();
        var k: u16 = 0;
        while (k < sim.max_ghosts) : (k += 1) _ = try u.sendTo(addr(lo, 60001 + k), "x", .none);
        // The 257th dies 10 us after the others (flows due at one
        // instant land in a seeded order), so it is the one dropped.
        try rt_mod.sleep(.fromMicroseconds(10));
        _ = try u.sendTo(addr(lo, 60001 + sim.max_ghosts), "x", .none);
        try rt_mod.sleep(.fromMicroseconds(150));
        var v = try bound(lo, 0);
        defer v.close();
        try v.connect(addr(lo, try portOf(p)), .{});
        try u.connect(addr(lo, 60001), .{});
        var task = try parkReceive(u);
        task.join();
        try expectEqual(refused, ended);
        try expectEqual(t0 + 2 * L, ended_at);
        capture();
    }

    /// Finding 3: the datagram is due at L. Run A harvests it at L; run B
    /// jumps the clock to 1.5L first (`sim.advanceNs`), so it lands then.
    /// Both stand at 2.25L, then the sender connects to the dead address.
    /// The death is at L in both, its ICMP back at 2L: no ICMP either way,
    /// and equal digests.
    var jump: bool = false;
    fn lateHarvest() !void {
        const q = try bound(lo, 0);
        defer q.close();
        const c = try connected(try portOf(q));
        defer c.close();
        const x = try closedPort();
        var u = try bound(lo, 0);
        defer u.close();
        const t0 = sim.clockNs();
        _ = try u.sendTo(addr(lo, x), "x", .none);
        if (jump) sim.advanceNs(L * 3 / 2);
        try rt_mod.sleep(.fromNanoseconds(t0 + L * 9 / 4 - sim.clockNs()));
        try expectEqual(t0 + L * 9 / 4, sim.clockNs());
        try u.connect(addr(lo, x), .{});
        try pause();
        try expectEqual(@as(u64, 0), sim.icmpsMade());
        try expectEqual(would_block, recvNow(u.handle));
        capture();
    }

    /// Finding 5: a dead landing near the end of the clock's range, with
    /// no connect; with `faulted` after a fault rule (a 60 s horizon).
    var faulted: bool = false;
    fn endOfClock() !void {
        if (faulted) sim.setPartition(unrelated_path);
        const x = try closedPort();
        const u = try bound(lo, 0);
        defer u.close();
        sim.advanceNs(std.math.maxInt(u64) - sim.clockNs() - 150_000);
        _ = try u.sendTo(addr(lo, x), "x", .none);
        try rt_mod.sleep(.fromMicroseconds(100));
        try expectEqual(@as(u64, 1), sim.datagramsDroppedNoSocket());
        if (faulted) _ = sim.healPartition(unrelated_path);
    }

    /// Finding 6: a run that never connects sends its 2^60th datagram.
    fn bigSendNoConnect() !void {
        const u = try bound(lo, 0);
        defer u.close();
        sim.testSetWireSeq(1 << 60);
        try expectEqual(ok1, res(u.sendTo(addr(lo, 9), "x", .none)));
    }

    /// Finding 7: with a fault rule set, the sender connects to the dead
    /// address after the ghost's bound (death + L + max_fault_delay_ns):
    /// no refusal, no ICMP.
    fn faultedExpiry() !void {
        sim.setPartition(unrelated_path);
        const x = try closedPort();
        var u = try bound(lo, 0);
        defer u.close();
        _ = try u.sendTo(addr(lo, x), "x", .none);
        try rt_mod.sleep(.fromNanoseconds(2 * L + sim.max_fault_delay_ns + 1000));
        try u.connect(addr(lo, x), .{});
        try pause();
        try expectEqual(@as(u64, 0), sim.icmpsMade());
        try expectEqual(would_block, recvNow(u.handle));
        _ = sim.healPartition(unrelated_path);
    }

    /// Mutant 3 of round 3: a socket bound to 127.0.0.2 keeps that address
    /// at its connect, and its datagrams leave from it.
    fn otherLoopback() !void {
        const p = try bound(lo, 0);
        defer p.close();
        var c = try bound(.{ 127, 0, 0, 2 }, 0);
        defer c.close();
        try c.connect(addr(lo, try portOf(p)), .{});
        const l = try local(c.handle);
        try expectEqual([4]u8{ 127, 0, 0, 2 }, l.ip);
        _ = try c.send("x", .none);
        try pause();
        var buf: [8]u8 = undefined;
        var iov = [1]os_net.iovec{os_net.iovecFromSlice(&buf)};
        var from: os_net.sockaddr.in = undefined;
        _ = try os_net.recvDatagram(p.handle, &iov, .{ .dontwait = true }, &from);
        try expectEqual([4]u8{ 127, 0, 0, 2 }, @as([4]u8, @bitCast(from.addr)));
    }

    /// Mutant 1 of round 3: a pending error, or the same error taken by a
    /// read of SO_ERROR (which records nothing), at the digest.
    var take: bool = false;
    fn pendingByte() !void {
        const c = try connected(try closedPort());
        defer c.close();
        _ = try c.send("x", .none);
        try pause();
        if (take) {
            var e: i32 = 0;
            try os_net.getsockopt(c.handle, os_net.SOL.SOCKET, os_net.SO.ERROR, std.mem.asBytes(&e));
        }
        capture();
    }
};

test "sim: dgram connect r3: with a fault rule set, 300 deaths at a closed client, then an unrelated connect is not refused" {
    if (comptime skip) return error.SkipZigTest;
    try t1.run(1, null, r3.manyDeathsUnrelatedConnect);
}

test "sim: dgram connect r3: 257 distinct deaths at one instant: an unrelated connect is not refused, and a kept four-tuple still gets its ICMP at 2L" {
    if (comptime skip) return error.SkipZigTest;
    try t1.run(1, null, r3.overflowUnrelated);
}

test "sim: dgram connect r3: a landing harvested late (a clock jump) dies at its due time: the same ghost and digests as one harvested on time" {
    if (comptime skip) return error.SkipZigTest;
    r3.jump = false;
    try t1.run(1, null, r3.lateHarvest);
    const a = digests;
    r3.jump = true;
    try t1.run(1, null, r3.lateHarvest);
    try expectEqual(a.state, digests.state);
    try expectEqual(a.wire, digests.wire);
}

test "sim: dgram connect r3: a dead landing near the end of the clock's range, in a run that never connects, with and without a fault rule" {
    if (comptime skip) return error.SkipZigTest;
    r3.faulted = false;
    try t1.run(1, null, r3.endOfClock);
    r3.faulted = true;
    try t1.run(1, null, r3.endOfClock);
}

test "sim: dgram connect r3: a run that never connects may send its 2^60th datagram" {
    if (comptime skip) return error.SkipZigTest;
    try t1.run(1, null, r3.bigSendNoConnect);
}

test "sim: dgram connect r3: with a fault rule set, a connect to the dead address after the ghost's bound is not refused and makes no ICMP" {
    if (comptime skip) return error.SkipZigTest;
    try t1.run(1, null, r3.faultedExpiry);
}

test "sim: dgram connect r3: a socket bound to 127.0.0.2 keeps that address at its connect" {
    if (comptime skip) return error.SkipZigTest;
    try t1.run(1, null, r3.otherLoopback);
}

test "sim: dgram connect r3: the state digest sees a pending error that a read of SO_ERROR took" {
    if (comptime skip) return error.SkipZigTest;
    r3.take = false;
    try t1.run(1, null, r3.pendingByte);
    const a = digests;
    r3.take = true;
    try t1.run(1, null, r3.pendingByte);
    try expectEqual(a.trace, digests.trace);
    try expectEqual(a.wire, digests.wire);
    try expect(a.state != digests.state);
}

const reuse_runs = struct {
    /// A run that connects and leaves state behind at its end: an ICMP made
    /// (icmp_seq), live ghosts for 256 four-tuples from 127.0.0.1:49152
    /// and one in the overflow, the profile set.
    fn dirty() !void {
        sim.setDatagramProfile(.linux);
        // A rule set and healed: `udp_rule_since` must not reach the next run.
        sim.setPartition(unrelated_path);
        _ = sim.healPartition(unrelated_path);
        var u = try bound(lo, 0);
        defer u.close();
        const c = try connected(9);
        defer c.close();
        _ = try c.send("x", .none);
        var k: u16 = 0;
        while (k < sim.max_ghosts) : (k += 1) _ = try u.sendTo(addr(lo, 60001 + k), "x", .none);
        // The 257th dies 10 us after the others (flows due at one
        // instant land in a seeded order), so it is the one dropped.
        try rt_mod.sleep(.fromMicroseconds(10));
        _ = try u.sendTo(addr(lo, 60001 + sim.max_ghosts), "x", .none);
        try rt_mod.sleep(.fromMicroseconds(150));
    }

    /// A run that never connects.
    fn plain() !void {
        const u = try bound(lo, 0);
        defer u.close();
        _ = try u.sendTo(addr(lo, 60001), "x", .none);
        try pause();
        capture();
    }

    /// A run that connects from 127.0.0.1:49152 to two of the dead ports
    /// the dirty run used, before 2L.
    fn connects() !void {
        var u = try bound(lo, 0);
        try u.connect(addr(lo, 60001), .{});
        u.close();
        var v = try bound(lo, 49152);
        defer v.close();
        try v.connect(addr(lo, 60001 + sim.max_ghosts), .{});
        try pause();
        capture();
    }
};

test "sim: dgram connect r3: a run made after a run that connected has the digests it has alone" {
    if (comptime skip) return error.SkipZigTest;
    try t1.run(1, null, reuse_runs.plain);
    const plain = digests;
    try t1.run(1, null, reuse_runs.connects);
    const conn = digests;
    try t1.run(1, null, reuse_runs.dirty);
    try t1.run(1, null, reuse_runs.plain);
    try expectEqual(plain, digests);
    try t1.run(1, null, reuse_runs.dirty);
    try t1.run(1, null, reuse_runs.connects);
    try expectEqual(conn, digests);
    // An exact ghost converted at a connect (no rule in this run).
    r4.jump = false;
    try t1.run(1, null, r4.lateWindow);
    const window = digests;
    try t1.run(1, null, reuse_runs.dirty);
    try t1.run(1, null, r4.lateWindow);
    try expectEqual(window, digests);
}

const r3b = struct {
    /// Two deaths of one four-tuple at t0 + L and one at t0 + L + 10 us,
    /// then the sender connects in time: three ICMPs, at 2L, 2L and 2L +
    /// 10 us; the error is taken between them and set again.
    fn threeDeaths() !void {
        const x = try closedPort();
        var u = try bound(lo, 0);
        defer u.close();
        const t0 = sim.clockNs();
        _ = try u.sendTo(addr(lo, x), "a", .none);
        _ = try u.sendTo(addr(lo, x), "b", .none);
        try rt_mod.sleep(.fromMicroseconds(10));
        _ = try u.sendTo(addr(lo, x), "c", .none);
        try rt_mod.sleep(.fromNanoseconds(t0 + L * 3 / 2 - sim.clockNs()));
        try u.connect(addr(lo, x), .{});
        var task = try parkReceive(u);
        task.join();
        try expectEqual(refused, ended);
        try expectEqual(t0 + 2 * L, ended_at);
        // The third ICMP is 10 us later; the receive parks until it lands.
        var buf: [8]u8 = undefined;
        try expectEqual(refused, res(u.receive(&buf, .{ .duration = .fromSeconds(1) })));
        try expectEqual(t0 + 2 * L + 10_000, sim.clockNs());
        try expectEqual(@as(u64, 3), sim.icmpsMade());
        try expectEqual(@as(u64, 3), sim.datagramStats(u.handle).refusals);
    }

    /// 257 deaths of one four-tuple at 257 instants fill the table; it is
    /// compacted into one entry, so another four-tuple's death still gets
    /// an exact ghost: its sender connects in time and gets its ICMP at 2L.
    fn compacted() !void {
        const x = try closedPort();
        const u = try bound(lo, 0);
        defer u.close();
        for (0..sim.max_ghosts + 1) |_| {
            _ = try u.sendTo(addr(lo, x), "a", .none);
            try rt_mod.sleep(.fromNanoseconds(100));
        }
        var v = try bound(lo, 0);
        defer v.close();
        const t0 = sim.clockNs();
        _ = try v.sendTo(addr(lo, x), "b", .none);
        try rt_mod.sleep(.fromMicroseconds(150));
        try v.connect(addr(lo, x), .{});
        var task = try parkReceive(v);
        task.join();
        try expectEqual(refused, ended);
        try expectEqual(t0 + 2 * L, ended_at);
    }
};

test "sim: dgram connect r3: three deaths of one four-tuple at two instants make three ICMPs, each at its time" {
    if (comptime skip) return error.SkipZigTest;
    try t1.run(1, null, r3b.threeDeaths);
}

test "sim: dgram connect r3: after the table is compacted, another four-tuple keeps an exact ghost" {
    if (comptime skip) return error.SkipZigTest;
    try t1.run(1, null, r3b.compacted);
}

test "sim: dgram connect refuses: a connect that makes a four-tuple whose deaths were merged" {
    try t1.refusal("sim: dgram connect refuses: a connect that makes a four-tuple whose deaths were merged", "sim: connect of sim datagram socket 100032 to 127.0.0.1:49152, where a datagram from its address died with no ICMP made", null, struct {
        fn body() !void {
            const x = try closedPort();
            var u = try bound(lo, 0);
            defer u.close();
            const t0 = sim.clockNs();
            for (0..sim.max_ghosts + 1) |_| {
                _ = try u.sendTo(addr(lo, x), "a", .none);
                try rt_mod.sleep(.fromNanoseconds(100));
            }
            // After the last death (t0 + 25.6 us + L), before the first
            // arrival (t0 + 2L).
            try rt_mod.sleep(.fromNanoseconds(t0 + L * 3 / 2 - sim.clockNs()));
            try u.connect(addr(lo, x), .{});
        }
    }.body);
}

// ---- Round 4 of the review (gpt-simnet20.md): the late harvest ----
//
// A late harvest: a test jumps the clock (`sim.advanceNs`) past a wire
// entry and polls later, so the entry is processed after its `due_at`. No
// real program reaches it (`Loop.poll` never passes a wire entry). The rule
// (the protocol above `dgramConnect`): a late death whose sender is not
// connected to where it died is exact (a ghost from `due_at`, faulted only
// if a rule existed before the death); anything a late harvest would hand
// to a connected socket panics, naming the jump and the remedy.

const late_msg = "after a clock jump (sim.advanceNs) with no poll between";

const r4 = struct {
    /// Finding 1: an unconnected send at 0, a jump to 3L with no harvest,
    /// an unrelated rule set and healed, the harvest, then the sender
    /// connects to the dead address. The ICMP was back at 2L, before any
    /// rule: the connect succeeds, no ICMP, as on time.
    fn ruleAfterDeath() !void {
        const x = try closedPort();
        var u = try bound(lo, 0);
        defer u.close();
        _ = try u.sendTo(addr(lo, x), "x", .none);
        sim.advanceNs(3 * L);
        sim.setPartition(unrelated_path);
        _ = sim.healPartition(unrelated_path);
        try rt_mod.sleep(.fromNanoseconds(1));
        try u.connect(addr(lo, x), .{});
        try pause();
        try expectEqual(@as(u64, 0), sim.icmpsMade());
        try expectEqual(would_block, recvNow(u.handle));
    }

    /// Finding 7's window: the same death harvested on time, or late (a
    /// jump to 1.5L, then a poll); the sender connects at 1.6L, inside the
    /// ghost's window. Both get the ICMP at exactly 2L, and equal digests.
    var jump: bool = false;
    fn lateWindow() !void {
        const x = try closedPort();
        var u = try bound(lo, 0);
        defer u.close();
        const t0 = sim.clockNs();
        _ = try u.sendTo(addr(lo, x), "x", .none);
        if (jump) {
            sim.advanceNs(L * 3 / 2);
            try rt_mod.sleep(.fromNanoseconds(1));
        }
        try rt_mod.sleep(.fromNanoseconds(t0 + L * 8 / 5 - sim.clockNs()));
        try u.connect(addr(lo, x), .{});
        var task = try parkReceive(u);
        task.join();
        try expectEqual(refused, ended);
        try expectEqual(t0 + 2 * L, ended_at);
        capture();
    }

    /// Finding 5: destination addresses on, an error pending, the queue
    /// empty: `recvmsg` takes the error.
    fn recvmsgPending() !void {
        const c = try connected(try closedPort());
        defer c.close();
        try os_net.enableDestinationAddress(c.handle);
        _ = try c.send("x", .none);
        try pause();
        var buf: [8]u8 = undefined;
        var iov = [1]os_net.iovec{os_net.iovecFromSlice(&buf)};
        try expectEqual(refused, res(if (os_net.recvmsg(c.handle, &iov, .{ .dontwait = true }, null, null, null)) |r| r.len else |e| e));
    }

    /// The grader's mutant (`<` for `<=`): the sender connects at exactly
    /// the ICMP's arrival instant (2L). In the sim the ICMP's landing is
    /// harvested before any task runs at that instant, so it found the
    /// sender unconnected: no ICMP, as at row 24.
    fn connectAtArrival() !void {
        const x = try closedPort();
        var u = try bound(lo, 0);
        defer u.close();
        const t0 = sim.clockNs();
        _ = try u.sendTo(addr(lo, x), "x", .none);
        try rt_mod.sleep(.fromNanoseconds(t0 + 2 * L - sim.clockNs()));
        try expectEqual(t0 + 2 * L, sim.clockNs());
        try u.connect(addr(lo, x), .{});
        try pause();
        try expectEqual(@as(u64, 0), sim.icmpsMade());
        try expectEqual(would_block, recvNow(u.handle));
    }
};

test "sim: dgram connect r4: a rule set after a death that is harvested late does not extend its ghost" {
    if (comptime skip) return error.SkipZigTest;
    try t1.run(1, null, r4.ruleAfterDeath);
}

test "sim: dgram connect r4: a death harvested late, with its sender unconnected, gives the same ICMP time and digests as on time" {
    if (comptime skip) return error.SkipZigTest;
    r4.jump = false;
    try t1.run(1, null, r4.lateWindow);
    const a = digests;
    r4.jump = true;
    try t1.run(1, null, r4.lateWindow);
    // The state digest holds the trace and the event count, which the
    // jump's extra poll changes; the socket's answers are the wire's.
    try expectEqual(a.wire, digests.wire);
}

test "sim: dgram connect r4: recvmsg on a socket with destination addresses takes a pending error" {
    if (comptime skip) return error.SkipZigTest;
    try t1.run(1, null, r4.recvmsgPending);
}

test "sim: dgram connect r4: a connect at the ICMP's arrival instant comes after its landing: no ICMP" {
    if (comptime skip) return error.SkipZigTest;
    try t1.run(1, null, r4.connectAtArrival);
}

test "sim: dgram connect refuses: a late harvest of deaths of a connected sender (finding 2: two of one flow)" {
    try t1.refusal("sim: dgram connect refuses: a late harvest of deaths of a connected sender (finding 2: two of one flow)", "sim: a datagram from port 49153 to port 49152 due at 150000 ns was harvested at 300001 ns, " ++ late_msg, null, struct {
        fn body() !void {
            const c = try connected(try closedPort());
            defer c.close();
            _ = try c.send("a", .none);
            try rt_mod.sleep(.fromMicroseconds(50));
            _ = try c.send("b", .none);
            try rt_mod.sleep(.fromMicroseconds(60));
            sim.advanceNs(190_000);
            try rt_mod.sleep(.fromNanoseconds(1));
        }
    }.body);
}

test "sim: dgram connect refuses: a late harvest of a connected sender's death after a healed rule (finding 3)" {
    try t1.refusal("sim: dgram connect refuses: a late harvest of a connected sender's death after a healed rule (finding 3)", "sim: a datagram from port 49153 to port 49152 due at 100000 ns was harvested at 225001 ns, " ++ late_msg, null, struct {
        fn body() !void {
            sim.setPartition(unrelated_path);
            _ = sim.healPartition(unrelated_path);
            const c = try connected(try closedPort());
            defer c.close();
            _ = try c.send("a", .none);
            sim.advanceNs(225_000);
            try rt_mod.sleep(.fromNanoseconds(1));
        }
    }.body);
}

test "sim: dgram connect refuses: a connect after a clock jump, before the late harvest of the sender's death (finding 4)" {
    try t1.refusal("sim: dgram connect refuses: a connect after a clock jump, before the late harvest of the sender's death (finding 4)", "sim: a datagram from port 49153 to port 49152 due at 100000 ns was harvested at 300000 ns, " ++ late_msg, null, struct {
        fn body() !void {
            const x = try closedPort();
            var u = try bound(lo, 0);
            defer u.close();
            _ = try u.sendTo(addr(lo, x), "x", .none);
            sim.advanceNs(300_000);
            try u.connect(addr(lo, x), .{});
            try rt_mod.sleep(.fromNanoseconds(1));
        }
    }.body);
}

test "sim: dgram connect refuses: an ICMP harvested late at a connected socket" {
    try t1.refusal("sim: dgram connect refuses: an ICMP harvested late at a connected socket", "sim: a datagram from port 49152 to port 49153 due at 200000 ns was harvested at 300001 ns, " ++ late_msg, null, struct {
        fn body() !void {
            const c = try connected(try closedPort());
            defer c.close();
            _ = try c.send("a", .none);
            try rt_mod.sleep(.fromMicroseconds(150));
            sim.advanceNs(150_000);
            try rt_mod.sleep(.fromNanoseconds(1));
        }
    }.body);
}

test "sim: dgram connect refuses: a connect that makes a ghost whose count would have overflowed (finding 6)" {
    try t1.refusal("sim: dgram connect refuses: a connect that makes a ghost whose count would have overflowed (finding 6)", "sim: connect of sim datagram socket 100032 to 127.0.0.1:49152, where a datagram from its address died with no ICMP made", null, struct {
        fn body() !void {
            // Three deaths of one four-tuple at one instant, with room to
            // count two: the third makes the ghost inexact.
            sim.testSetGhostCountMax(2);
            const x = try closedPort();
            var u = try bound(lo, 0);
            defer u.close();
            for (0..3) |_| _ = try u.sendTo(addr(lo, x), "x", .none);
            try rt_mod.sleep(.fromMicroseconds(150));
            try u.connect(addr(lo, x), .{});
        }
    }.body);
}

test "sim: dgram connect: with room to count two deaths, two deaths at one instant still make two ICMPs" {
    if (comptime skip) return error.SkipZigTest;
    sim.testSetGhostCountMax(2);
    defer sim.testSetGhostCountMax(std.math.maxInt(u32));
    try t1.run(1, null, struct {
        fn body() !void {
            const x = try closedPort();
            var u = try bound(lo, 0);
            defer u.close();
            for (0..2) |_| _ = try u.sendTo(addr(lo, x), "x", .none);
            try rt_mod.sleep(.fromMicroseconds(150));
            try u.connect(addr(lo, x), .{});
            try pause();
            try expectEqual(@as(u64, 2), sim.icmpsMade());
        }
    }.body);
}
