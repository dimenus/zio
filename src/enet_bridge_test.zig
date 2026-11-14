// SPDX-FileCopyrightText: 2025 Lukáš Lalinský
// SPDX-License-Identifier: MIT
//
// The Zig half of the ENet-shaped bridge test (sim network design, section
// 11, acceptance groups 1, 2 and 7). `enet_bridge_test.c` holds the bridge
// and the C tests. This file exports the `zio_test_*` functions the bridge
// calls, and runs each C test from a zio task.
//
// One test, two builds:
// - with `-Dsim=true`, each test runs in a sim run (seed 1), so the bridge's
//   sockets are sim fds;
// - without sim, it runs on a plain runtime, and the same calls reach the
//   kernel, on real loopback sockets.
// The C file holds the expected values: one table for both builds, and one
// exception table, with a column per build (a source address the socket
// cannot use).
//
// The shims, for the C side:
// - Each returns a value >= 0 on success, or `-errno` for the layer's error:
//   the errno a kernel gives for that case (`errnoOf`). An error with no entry
//   there panics: no test should meet one, and a quiet -1 would hide it.
// - `zio_test_receive` and `zio_test_send` are `recvDatagram` and
//   `sendDatagram`, with no flags: like ENet, a receive relies on the
//   socket's non-blocking mode. The bridge's buffers are `iovec`s (the C file
//   asserts the layout).
// - `zio_test_wait` is `pollInterruptible`: 1 ready (with `revents`), 0 timed
//   out, `-EINTR` interrupted.
// - `zio_test_wait_readable` is not the bridge's: it parks the calling task in
//   an ev `NetPoll` until the socket is readable. In a sim run the sim clock
//   moves to the landing, and a datagram that never lands ends the run as a
//   deadlock; on real sockets it gives up after 2 s (`-ETIMEDOUT`).
//
// Built on macOS and Linux only (`build.zig` adds the C file there; the
// design's phase 1 runs there, section 14).

const std = @import("std");
const builtin = @import("builtin");
const sim = @import("sim.zig");
const os_net = @import("os/net.zig");
const ev = @import("ev/root.zig");
const common = @import("common.zig");
const rt_mod = @import("runtime.zig");

const E = os_net.E;
const in_sim = sim.compiled_in;

fn neg(e: E) c_int {
    return -@as(c_int, @intCast(@backingInt(e)));
}

fn errnoOf(err: anyerror) c_int {
    return neg(switch (err) {
        error.WouldBlock => .AGAIN,
        error.AddressUnavailable => .ADDRNOTAVAIL,
        error.AddressInUse => .ADDRINUSE,
        error.NetworkDown => .NETDOWN,
        error.NetworkUnreachable => .NETUNREACH,
        error.HostUnreachable => .HOSTUNREACH,
        error.OptionNotSupported => .NOPROTOOPT,
        error.Timeout => .TIMEDOUT,
        else => std.debug.panic("zio_test shim: no errno for error.{s}", .{@errorName(err)}),
    });
}

export fn zio_test_socket() c_int {
    return os_net.socket(.ipv4, .dgram, .udp, .{ .nonblocking = false }) catch |err| errnoOf(err);
}

export fn zio_test_enable_destination_address(fd: c_int) c_int {
    os_net.enableDestinationAddress(fd) catch |err| return errnoOf(err);
    return 0;
}

export fn zio_test_bind(fd: c_int, addr: *const os_net.sockaddr, len: os_net.socklen_t) c_int {
    os_net.bind(fd, addr, len) catch |err| return errnoOf(err);
    return 0;
}

export fn zio_test_getsockname(fd: c_int, addr: *os_net.sockaddr, len: *os_net.socklen_t) c_int {
    os_net.getsockname(fd, addr, len) catch |err| return errnoOf(err);
    return 0;
}

export fn zio_test_set_nonblocking(fd: c_int, on: c_int) c_int {
    os_net.setNonBlocking(fd, on != 0) catch |err| return errnoOf(err);
    return 0;
}

export fn zio_test_is_nonblocking(fd: c_int) c_int {
    return @intFromBool(os_net.isNonBlocking(fd) catch |err| return errnoOf(err));
}

export fn zio_test_setsockopt(fd: c_int, level: c_int, name: c_int, value: [*]const u8, len: os_net.socklen_t) c_int {
    os_net.setsockopt(fd, level, @intCast(name), value[0..len]) catch |err| return errnoOf(err);
    return 0;
}

export fn zio_test_getsockopt(fd: c_int, level: c_int, name: c_int, value: [*]u8, len: os_net.socklen_t) c_int {
    os_net.getsockopt(fd, level, @intCast(name), value[0..len]) catch |err| return errnoOf(err);
    return 0;
}

export fn zio_test_send(fd: c_int, to: *const os_net.sockaddr.in, from: ?*const [4]u8, buffers: [*]const os_net.iovec_const, count: usize) isize {
    const n = os_net.sendDatagram(fd, buffers[0..count], .{}, to, if (from) |f| f.* else null) catch |err| return errnoOf(err);
    return @intCast(n);
}

export fn zio_test_receive(fd: c_int, from: *os_net.sockaddr.in, buffers: [*]os_net.iovec, count: usize, truncated: *c_int, to: *[4]u8, has_to: *c_int) isize {
    const info = os_net.recvDatagram(fd, buffers[0..count], .{}, from) catch |err| return errnoOf(err);
    truncated.* = @intFromBool(info.truncated);
    has_to.* = @intFromBool(info.to != null);
    if (info.to) |t| to.* = t;
    return @intCast(info.len);
}

export fn zio_test_wait(fd: c_int, events: c_short, timeout_ms: c_int, revents: *c_short) c_int {
    const outcome = os_net.pollInterruptible(fd, events, timeout_ms) catch |err| return errnoOf(err);
    return switch (outcome) {
        .ready => |r| {
            revents.* = r;
            return 1;
        },
        .timed_out => 0,
        .interrupted => neg(.INTR),
    };
}

export fn zio_test_close(fd: c_int) void {
    os_net.close(fd);
}

export fn zio_test_wait_readable(fd: c_int) c_int {
    var op = ev.NetPoll.init(fd, .recv);
    waitIo(&op.c) catch |err| return errnoOf(err);
    op.getResult() catch |err| return errnoOf(err);
    return 0;
}

/// An ev op to completion: in a sim run the sim ends a run that can never
/// finish; on real sockets a 2 s bound stops a lost datagram hanging a test.
fn waitIo(c: *ev.Completion) !void {
    if (comptime in_sim) return common.waitForIo(c);
    return common.timedWaitForIo(c, .{ .duration = .fromSeconds(2) });
}

// ---- The C tests ----

extern var ebt_failure: [256]u8;
extern fn ebt_close_all() void;
extern fn ebt_exchange(peer_port: c_int, my_port: *c_int) c_int;
extern fn ebt_host_create() c_int;
extern fn ebt_would_block() c_int;
extern fn ebt_empty_datagram() c_int;
extern fn ebt_truncation() c_int;
extern fn ebt_unsupported_option() c_int;
extern fn ebt_wait() c_int;
extern fn ebt_wait_interrupt() c_int;
extern fn ebt_bind_conflict_and_destroy() c_int;
extern fn ebt_source_address(sim: c_int) c_int;

const sim_flag: c_int = @intFromBool(in_sim);

/// Runs `body` as the one task of a runtime: inside a sim run in a sim build,
/// on real sockets otherwise.
fn run(comptime body: fn () anyerror!void) !void {
    if (comptime in_sim) sim.begin(1);
    defer if (comptime in_sim) sim.end();
    var o: rt_mod.RuntimeOptions = .{
        .executors = .exact(1),
        .thread_pool = .{ .max_threads = 0 },
    };
    o.stack_pool.shrink_interval = .zero;
    const rt = try rt_mod.Runtime.init(std.heap.page_allocator, o);
    defer rt.deinit();
    var h = try rt.spawn(body, .{});
    try h.join();
}

/// A C test's result: 0, or its failing line, with the reason.
fn expectC(line: c_int) !void {
    if (line == 0) return;
    std.debug.print("enet_bridge_test.c:{d}: {s}\n", .{ line, std.mem.sliceTo(&ebt_failure, 0) });
    return error.TestUnexpectedResult;
}

/// Runs one C test, closing what it opened.
fn runC(comptime c_test: fn () callconv(.c) c_int) !void {
    try run(struct {
        fn body() !void {
            defer ebt_close_all();
            try expectC(c_test());
        }
    }.body);
}

/// Runs one C test that reads its column of the exception table.
fn runCForBuild(comptime c_test: fn (c_int) callconv(.c) c_int) !void {
    try run(struct {
        fn body() !void {
            defer ebt_close_all();
            try expectC(c_test(sim_flag));
        }
    }.body);
}

const Peer = struct {
    bytes: [16]u8,
    len: usize,
    from: os_net.sockaddr.in,
};

/// The zio side of the exchange: one ev receive into one buffer, then a reply
/// of two buffers, one datagram, to the source.
fn echoPeer(fd: i32) !Peer {
    var p: Peer = undefined;
    var iov: [1]os_net.iovec = undefined;
    var from_len: os_net.socklen_t = @sizeOf(os_net.sockaddr.in);
    var rx = ev.NetRecvFrom.init(fd, .fromSlice(&p.bytes, &iov), .{}, @ptrCast(&p.from), &from_len);
    try waitIo(&rx.c);
    p.len = try rx.getResult();
    var out = [2]os_net.iovec_const{ os_net.iovecConstFromSlice("wor"), os_net.iovecConstFromSlice("ld!") };
    var tx = ev.NetSendTo.init(fd, .{ .iovecs = &out }, .{}, @ptrCast(&p.from), @sizeOf(os_net.sockaddr.in));
    try waitIo(&tx.c);
    if (try tx.getResult() != 6) return error.ShortReply;
    return p;
}

test "sim: enet bridge: C sends two buffers to a zio task's ev receive, which replies with two buffers that C reads into two: bytes, sources, destination" {
    try run(struct {
        fn body() !void {
            defer ebt_close_all();
            var open = ev.NetOpen.init(.ipv4, .dgram, .ip, .{ .nonblocking = true });
            try waitIo(&open.c);
            const peer_fd = try open.getResult();
            defer {
                var close = ev.NetClose.init(peer_fd);
                common.waitForIoUncancelable(&close.c);
                close.getResult() catch {};
            }
            var sa: os_net.sockaddr.in = .{ .port = 0, .addr = @bitCast([4]u8{ 127, 0, 0, 1 }) };
            var sa_len: os_net.socklen_t = @sizeOf(os_net.sockaddr.in);
            var bind = ev.NetBind.init(peer_fd, @ptrCast(&sa), &sa_len);
            try waitIo(&bind.c);
            try bind.getResult();
            const peer_port = std.mem.bigToNative(u16, sa.port);

            var peer = try rt_mod.spawn(echoPeer, .{peer_fd});
            var my_port: c_int = 0;
            const line = ebt_exchange(peer_port, &my_port);
            const got = peer.join();
            try expectC(line);
            const p = try got;
            try std.testing.expectEqualStrings("hello", p.bytes[0..p.len]);
            try std.testing.expectEqual([4]u8{ 127, 0, 0, 1 }, @as([4]u8, @bitCast(p.from.addr)));
            try std.testing.expectEqual(@as(u16, @intCast(my_port)), std.mem.bigToNative(u16, p.from.port));
        }
    }.body);
}

test "sim: enet bridge: an ENet-shaped host creation: create, bind to port 0, NONBLOCK read back, RCVBUF, SNDBUF, get_address, get_option ERROR, then a datagram with its destination" {
    try runC(ebt_host_create);
}

test "sim: enet bridge: a receive with nothing queued is 0, errno EWOULDBLOCK, and writes no address" {
    try runC(ebt_would_block);
}

test "sim: enet bridge: an empty datagram is received as 0 with its source written, and the next datagram is next" {
    try runC(ebt_empty_datagram);
}

test "sim: enet bridge: a datagram that does not fit is -2 and is consumed" {
    try runC(ebt_truncation);
}

test "sim: enet bridge: BROADCAST and reading TTL give -1 and change nothing, and SNDBUF succeeds with the receive side unchanged" {
    try runC(ebt_unsupported_option);
}

test "sim: enet bridge: enet_socket_wait with a zero timeout reports receive only once a datagram is queued, and send always" {
    try runC(ebt_wait);
}

test "sim: enet bridge: real sockets only: a signal cuts a wait: ENET_SOCKET_WAIT_INTERRUPT when asked for, -1 and EINTR when not" {
    // A sim poll takes a zero timeout only, so nothing can cut one (design
    // section 10).
    if (comptime in_sim) return error.SkipZigTest;
    try runC(ebt_wait_interrupt);
}

test "sim: enet bridge: a bind to a port in use is -1 with EADDRINUSE, destroy frees the port, destroy of ENET_SOCKET_NULL does nothing" {
    try runC(ebt_bind_conflict_and_destroy);
}

test "sim: enet bridge: a source address the socket cannot use: the sim's column in a sim run, the kernel's otherwise" {
    try runCForBuild(ebt_source_address);
}
