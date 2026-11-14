// SPDX-FileCopyrightText: 2025 Lukáš Lalinský
// SPDX-License-Identifier: MIT
//
// Tests of the sim's stream bytes, slice 2 of design section 17.16: send
// and receive, chunks, the window and credit, FIN, RST with data, shutdown,
// close with bytes, the profiles' send and receive rules, `getpeername`,
// `bytesReadable`, and the window and chunking settings. The protocol is
// the comment above `pub const StreamProfile` in sim.zig.
//
// How they run: as slice 1's (`sim_stream_test.zig`, whose helpers these
// use): one sim run per scenario, one executor, through zio's public API
// where a program would use it, ev ops where a case needs a parked op of
// its own. After `pair()` the clock is 3L (300 µs): the client is fd
// 100033 and the accepted socket 100034, connection 1. Each scenario
// closes its sockets and drains the wire, so `sim.end`'s teardown check
// runs on every one.

const std = @import("std");
const builtin = @import("builtin");
const sim = @import("sim.zig");
const rt_mod = @import("runtime.zig");
const zio_net = @import("net.zig");
const ev = @import("ev/root.zig");
const io_common = @import("common.zig");
const os_net = @import("os/net.zig");
const t1 = @import("sim_stream_test.zig");

const L = t1.L;
const lo = t1.lo;
const skip = t1.skip;
const run = t1.run;
const drain = t1.drain;
const pair = t1.pair;
const refusal = t1.refusal;
const expect = std.testing.expect;
const expectEqual = std.testing.expectEqual;
const expectError = std.testing.expectError;
const us = 1000;

fn fill(buf: []u8, salt: u8) void {
    for (buf, 0..) |*b, i| b.* = @truncate(i *% 31 +% salt);
}

fn dir(from_client: bool) !sim.TestStreamDir {
    return sim.testStreamDir(1, from_client) orelse error.ConnectionRetired;
}

fn checkConserved() !void {
    for ([_]bool{ true, false }) |fc| {
        const d = sim.testStreamDir(1, fc) orelse continue;
        if (!d.conserved()) {
            std.debug.print("conservation: admitted {d} != in flight {d} + unread {d} + read {d} + discarded {d}\n", .{ d.admitted, d.in_flight, d.unread, d.read, d.discardedTotal() });
            return error.NotConserved;
        }
        try expect(d.credited <= d.read + d.reclaimed);
    }
}

/// An ev receive of `cap` bytes on `fd`, run to its end in this task.
fn evRecv(fd: i32, buf: []u8) !usize {
    var storage: [1]os_net.iovec = undefined;
    var op = ev.NetRecv.init(fd, .fromSlices(&.{buf}, &storage), .{});
    try io_common.waitForIo(&op.c);
    return op.getResult();
}

fn evSend(fd: i32, buf: []const u8) !usize {
    var storage: [1]os_net.iovec_const = undefined;
    var op = ev.NetSend.init(fd, .fromSlices(&.{buf}, &storage), .{});
    try io_common.waitForIo(&op.c);
    return op.getResult();
}

fn closeAll(socks: anytype) !void {
    inline for (socks) |x| x.close();
    try drain();
}

// ---- Bytes, chunks, the window and credit (design 17.7) ----

const round_trip = struct {
    var got: [20100]u8 = undefined;
    var got_n: usize = 0;
    var reads: usize = 0;

    fn reader(s: zio_net.Stream) !void {
        got_n = 0;
        reads = 0;
        while (true) {
            const n = try s.read(got[got_n..], .none);
            if (n == 0) break;
            got_n += n;
            reads += 1;
        }
    }

    fn body() !void {
        const srv, const c, const s = try pair();
        var data: [20000]u8 = undefined;
        fill(&data, 3);
        var h = try rt_mod.spawn(reader, .{s});
        const t0 = sim.clockNs();
        try c.writeAll(&data, .none);
        // The window (256 KiB) takes it all at once.
        try expectEqual(t0, sim.clockNs());
        try checkConserved();
        c.close();
        try h.join();
        try expectEqual(@as(usize, 20000), got_n);
        try expect(std.mem.eql(u8, &data, got[0..20000]));
        // Hashed chunks: the reader saw more than one prefix.
        try expect(reads > 1);
        try checkConserved();
        try closeAll(.{ s, srv });
    }
};

test "sim: stream bytes: 20000 bytes through zio.net arrive whole and in order, in more than one read, and the close's FIN reads as 0" {
    if (comptime skip) return error.SkipZigTest;
    try run(1, null, round_trip.body);
}

const chunk_times = struct {
    var times: [8]u64 = undefined;
    var sizes: [8]usize = undefined;

    fn body() !void {
        const srv, const c, const s = try pair();
        sim.setStreamChunking(c.socket.handle, .{ .fixed = 1000 });
        var data: [5000]u8 = undefined;
        fill(&data, 9);
        const t0 = sim.clockNs();
        try expectEqual(@as(usize, 5000), try c.write(&data, .none));
        try expectEqual(t0, sim.clockNs());
        const d = try dir(true);
        try expectEqual(@as(u64, 5000), d.admitted);
        try expectEqual(@as(u64, 5000), d.in_flight);
        try expectEqual(@as(u64, 1), d.data_send_index);
        var buf: [10000]u8 = undefined;
        var got: usize = 0;
        var k: usize = 0;
        while (got < 5000) : (k += 1) {
            const n = try s.read(buf[got..], .none);
            times[k] = sim.clockNs();
            sizes[k] = n;
            got += n;
            try checkConserved();
        }
        // Chunk k lands at t0 + L + k µs, each in its own poll.
        try expectEqual(@as(usize, 5), k);
        for (0..5) |i| {
            try expectEqual(t0 + L + i * us, times[i]);
            try expectEqual(@as(usize, 1000), sizes[i]);
        }
        try expect(std.mem.eql(u8, &data, buf[0..5000]));
        // The first read's credit left at once; the next four fell inside
        // one L, so one more is owed, for t0 + 2L.
        try expect((try dir(true)).owed);
        try closeAll(.{ c, s, srv });
    }
};

test "sim: stream bytes: with chunks of 1000, a send of 5000 admits at once and its chunks land at L, L + 1 µs, ... L + 4 µs, each read returning one" {
    if (comptime skip) return error.SkipZigTest;
    try run(1, null, chunk_times.body);
}

const chunk_off = struct {
    fn body() !void {
        const srv, const c, const s = try pair();
        sim.setStreamChunking(s.socket.handle, .off);
        var data: [70000]u8 = undefined;
        fill(&data, 1);
        try expectEqual(@as(usize, 70000), try c.write(&data, .none));
        var buf: [80000]u8 = undefined;
        const t0 = sim.clockNs();
        try expectEqual(@as(usize, 70000), try s.read(&buf, .none));
        try expectEqual(t0 + L, sim.clockNs());
        try expect(std.mem.eql(u8, &data, buf[0..70000]));
        try closeAll(.{ c, s, srv });
    }
};

test "sim: stream bytes: chunking .off sends one chunk per send, set from either end of the connection" {
    if (comptime skip) return error.SkipZigTest;
    try run(1, null, chunk_off.body);
}

const window = struct {
    var sent_at: [4]u64 = undefined;
    var sent_n: [4]usize = undefined;

    fn sender(c: zio_net.Stream) !void {
        var data: [5000]u8 = undefined;
        fill(&data, 5);
        var off: usize = 0;
        var i: usize = 0;
        while (off < data.len) : (i += 1) {
            const n = try c.write(data[off..], .none);
            sent_at[i] = sim.clockNs();
            sent_n[i] = n;
            off += n;
        }
    }

    fn body() !void {
        const srv, const c, const s = try pair();
        sim.setStreamChunking(c.socket.handle, .off);
        sim.setStreamWindow(s.socket.handle, 3000);
        try expectEqual(@as(u32, 3000), (try dir(true)).window);
        const t0 = sim.clockNs();
        var h = try rt_mod.spawn(sender, .{c});
        var buf: [1000]u8 = undefined;
        // The first send admits the window, 3000, and the second parks.
        try expectEqual(@as(usize, 1000), try s.read(&buf, .none));
        try expectEqual(t0 + L, sim.clockNs());
        try expectEqual(@as(usize, 3000), sent_n[0]);
        try expectEqual(t0, sent_at[0]);
        // The read's credit (1000) left at t0 + L and lands at t0 + 2L,
        // waking the send, which admits 1000.
        try rt_mod.sleep(.fromMicroseconds(150));
        try expectEqual(t0 + 2 * L, sent_at[1]);
        try expectEqual(@as(usize, 1000), sent_n[1]);
        // Two reads inside one L: the second's credit is owed, and leaves
        // L after the first's.
        var big: [3000]u8 = undefined;
        try expectEqual(@as(usize, 2000), try s.read(&big, .none));
        const t_read = sim.clockNs();
        try expect((try dir(true)).owed == false);
        try expectEqual(@as(usize, 1000), try s.read(&big, .none));
        try expect((try dir(true)).owed);
        try checkConserved();
        try h.join();
        // The last 1000 went with the credit of the 2000 read at t_read,
        // which landed at t_read + L.
        try expectEqual(t_read + L, sent_at[2]);
        try expectEqual(@as(usize, 1000), sent_n[2]);
        try checkConserved();
        try closeAll(.{ c, s, srv });
    }
};

test "sim: stream bytes: a 3000-byte window admits 3000, a read's credit lands L later and wakes the parked send, and a second read inside L owes its credit" {
    if (comptime skip) return error.SkipZigTest;
    try run(1, null, window.body);
}

const partial = struct {
    fn body() !void {
        const srv, const c, const s = try pair();
        sim.setStreamChunking(c.socket.handle, .off);
        try expectEqual(@as(usize, 10), try c.write("0123456789", .none));
        try rt_mod.sleep(.fromMicroseconds(L / us));
        try expectEqual(@as(usize, 10), try os_net.bytesReadable(s.socket.handle));
        var b: [4]u8 = undefined;
        try expectEqual(@as(usize, 4), try s.read(&b, .none));
        try expect(std.mem.eql(u8, "0123", &b));
        try expectEqual(@as(usize, 6), try os_net.bytesReadable(s.socket.handle));
        // A scatter read across two buffers takes the rest.
        var x: [2]u8 = undefined;
        var y: [8]u8 = undefined;
        var bufs = [_][]u8{ &x, &y };
        try expectEqual(@as(usize, 6), try s.readVec(&bufs, .none));
        try expect(std.mem.eql(u8, "45", &x));
        try expect(std.mem.eql(u8, "6789", y[0..4]));
        try expectEqual(@as(usize, 0), try os_net.bytesReadable(s.socket.handle));
        // A send of length 0 returns 0 and admits nothing.
        try expectEqual(@as(usize, 0), try evSend(c.socket.handle, ""));
        try expectEqual(@as(u64, 1), (try dir(true)).data_send_index);
        try checkConserved();
        try closeAll(.{ c, s, srv });
    }
};

test "sim: stream bytes: a read smaller than the unread bytes leaves the rest, bytesReadable counts them, a scatter read takes them, and a send of 0 returns 0" {
    if (comptime skip) return error.SkipZigTest;
    try run(1, null, partial.body);
}

/// The unread buffer moves its rest to the front once the read head is
/// 4096 or more and past half of it (`copyOutUnread`). This reads past that
/// point with bytes behind the head, then lands more behind the moved rest,
/// and checks every byte and count after it.
const compaction = struct {
    fn body() !void {
        const srv, const c, const s = try pair();
        sim.setStreamChunking(c.socket.handle, .off);
        var data: [9000]u8 = undefined;
        fill(&data, 11);
        try expectEqual(@as(usize, 6000), try c.write(data[0..6000], .none));
        try rt_mod.sleep(.fromMicroseconds(L / us));
        var buf: [9000]u8 = undefined;
        // The head moves to 4096 of 6000: the 1904 behind it move.
        try expectEqual(@as(usize, 4096), try s.read(buf[0..4096], .none));
        try expectEqual(@as(usize, 1904), try os_net.bytesReadable(s.socket.handle));
        try checkConserved();
        // More bytes land behind the moved rest.
        try expectEqual(@as(usize, 3000), try c.write(data[6000..], .none));
        try rt_mod.sleep(.fromMicroseconds(L / us));
        try expectEqual(@as(usize, 4904), try os_net.bytesReadable(s.socket.handle));
        try checkConserved();
        var got: usize = 4096;
        while (got < data.len) got += try s.read(buf[got..], .none);
        try expect(std.mem.eql(u8, &data, &buf));
        try expectEqual(@as(usize, 0), try os_net.bytesReadable(s.socket.handle));
        try checkConserved();
        try t1.closeAll3(srv, c, s);
    }
};

test "sim: stream bytes: a read that moves the unread rest to the front keeps every byte behind the head, and bytes that land after it follow in order" {
    if (comptime skip) return error.SkipZigTest;
    try run(1, null, compaction.body);
}

const peer_name = struct {
    fn peerOf(fd: i32) !sim.Ip4 {
        var sa: os_net.sockaddr.in = undefined;
        var len: os_net.socklen_t = @sizeOf(os_net.sockaddr.in);
        try os_net.getpeername(fd, @ptrCast(&sa), &len);
        return .{ .ip = @bitCast(sa.addr), .port = std.mem.bigToNative(u16, sa.port) };
    }

    fn body() !void {
        const srv, const c, const s = try pair();
        const cp = try peerOf(c.socket.handle);
        try expectEqual(@as(u16, 49152), cp.port);
        const sp = try peerOf(s.socket.handle);
        try expectEqual((try t1.localOf(c.socket.handle)).port, sp.port);
        try expectError(error.SocketUnconnected, peerOf(srv.socket.handle));
        // After a reset (row 9): SocketUnconnected.
        const lg: [2]c_int = .{ 1, 0 };
        try os_net.setsockopt(s.socket.handle, os_net.SOL.SOCKET, os_net.SO.LINGER, std.mem.asBytes(&lg));
        s.close();
        try rt_mod.sleep(.fromMicroseconds(L / us));
        try expect(sim.testStreamEnd(c.socket.handle).reset_received);
        try expectError(error.SocketUnconnected, peerOf(c.socket.handle));
        try closeAll(.{ c, srv });
    }
};

test "sim: stream bytes: getpeername gives each end its peer, SocketUnconnected on a listener and after a reset (row 9)" {
    if (comptime skip) return error.SkipZigTest;
    try run(1, null, peer_name.body);
}

// ---- Parity: the kernel rows slice 2 models (design 17.13, 17.15 item 5) ----
//
// Each row is a sequence through zio.net, as `tcpprobe3.c` and
// `tcpprobe4.c` ran it in C, with its answers written as tokens: a count,
// or an error as the kernel names it (`EPIPE` for BrokenPipe, `ECONNRESET`,
// `ENOTCONN` for SocketUnconnected, `TIMEOUT`). The expected columns are
// the probes' answers on macOS 25.6 and Linux 6.8. In a build without sim
// the host's column runs on real loopback sockets; in a sim build, each
// column runs in a sim run with its profile. Waits are 50 ms, real or
// virtual, as the probe waits. A blocked call is a task whose write or read
// parks, as a thread blocked in the probe.
//
// Not here: rows 1 to 4, 6, 10 to 15 (slice 1 or refused), 33 and 34
// (SO_ERROR is refused in a sim run), and row 41's later sends on Linux
// (the probe's are MSG_DONTWAIT, which the sim refuses).

const Kernel = enum { macos, linux };
const host_kernel: Kernel = if (builtin.os.tag.isDarwin()) .macos else .linux;

const Tok = struct {
    buf: [160]u8 = undefined,
    len: usize = 0,

    fn put(t: *Tok, s: []const u8) void {
        if (t.len != 0) {
            t.buf[t.len] = ',';
            t.len += 1;
        }
        @memcpy(t.buf[t.len..][0..s.len], s);
        t.len += s.len;
    }

    fn n(t: *Tok, r: anyerror!usize) void {
        const v = r catch |err| return t.e(err);
        var b: [24]u8 = undefined;
        t.put(std.fmt.bufPrint(&b, "{d}", .{v}) catch unreachable);
    }

    fn ok(t: *Tok, r: anyerror!void) void {
        r catch |err| return t.e(err);
        t.put("ok");
    }

    fn e(t: *Tok, err: anyerror) void {
        t.put(switch (err) {
            error.BrokenPipe => "EPIPE",
            error.ConnectionResetByPeer => "ECONNRESET",
            error.SocketUnconnected => "ENOTCONN",
            error.Timeout => "TIMEOUT",
            else => @errorName(err),
        });
    }

    fn str(t: *const Tok) []const u8 {
        return t.buf[0..t.len];
    }
};

const prow = struct {
    pub fn pause() !void {
        try rt_mod.sleep(.fromMilliseconds(50));
    }

    fn connected() !struct { zio_net.Server, zio_net.Stream, zio_net.Stream } {
        const l = try t1.listen(lo, 0, .{});
        const c = try t1.connect(lo, t1.portOf(l), .none);
        const s = try l.accept(.{});
        return .{ l, c, s };
    }

    pub fn linger0(x: zio_net.Stream) !void {
        const lg: [2]c_int = .{ 1, 0 };
        try os_net.setsockopt(x.socket.handle, os_net.SOL.SOCKET, os_net.SO.LINGER, std.mem.asBytes(&lg));
    }

    fn rd(t: *Tok, x: zio_net.Stream) void {
        var b: [64]u8 = undefined;
        t.n(x.read(&b, .none));
    }

    /// A read that has data gives "data" (row 41's receiver).
    fn rdData(t: *Tok, x: zio_net.Stream) void {
        var b: [64]u8 = undefined;
        const r = x.read(&b, .none) catch |err| return t.e(err);
        t.put(if (r > 0) "data" else "0");
    }

    fn wr(t: *Tok, x: zio_net.Stream, s: []const u8) void {
        t.n(x.write(s, .none));
    }

    pub var big: [32 << 20]u8 = undefined;

    const Blocked = struct {
        x: zio_net.Stream,
        is_send: bool,
        timeout: @import("time.zig").Timeout = .none,
        done: bool = false,
        result: anyerror!usize = 0,

        fn run(b: *Blocked) void {
            if (b.is_send) {
                if (b.x.writeAll(&big, b.timeout)) |_| {
                    b.result = big.len;
                } else |err| b.result = err;
            } else {
                var buf: [64]u8 = undefined;
                b.result = b.x.read(&buf, b.timeout);
            }
            b.done = true;
        }
    };

    fn r5(t: *Tok) !void {
        const l, const c, const s = try connected();
        defer l.close();
        defer c.close();
        defer s.close();
        t.ok(c.shutdown(.send));
        try pause();
        rd(t, s);
        wr(t, s, "y");
        try pause();
        rd(t, c);
        wr(t, c, "z");
    }
    fn r7(t: *Tok) !void {
        const l, const c, const s = try connected();
        defer l.close();
        defer c.close();
        wr(t, c, "abc");
        try pause();
        s.close();
        try pause();
        rd(t, c);
        rd(t, c);
    }
    fn r8(t: *Tok) !void {
        const l, const c, const s = try connected();
        defer l.close();
        defer c.close();
        s.close();
        try pause();
        wr(t, c, "x");
        try pause();
        wr(t, c, "y");
        rd(t, c);
    }
    fn r9(t: *Tok) !void {
        const l, const c, const s = try connected();
        defer l.close();
        defer c.close();
        try linger0(s);
        s.close();
        try pause();
        rd(t, c);
        var sa: os_net.sockaddr.in = undefined;
        var len: os_net.socklen_t = @sizeOf(os_net.sockaddr.in);
        t.ok(os_net.getpeername(c.socket.handle, @ptrCast(&sa), &len));
    }
    fn r16(t: *Tok) !void {
        const l, const c, const s = try connected();
        defer l.close();
        defer c.close();
        defer s.close();
        var b: Blocked = .{ .x = s, .is_send = false };
        var h = try rt_mod.spawn(Blocked.run, .{&b});
        try rt_mod.sleep(.fromMilliseconds(200));
        t.ok(s.shutdown(.both));
        h.join();
        t.n(b.result);
        rd(t, s);
        rd(t, c);
    }
    fn r17(t: *Tok) !void {
        const l, const c, const s = try connected();
        defer l.close();
        defer c.close();
        defer s.close();
        wr(t, c, "before");
        try pause();
        t.ok(s.shutdown(.both));
        rd(t, s);
        wr(t, c, "after");
        try pause();
        rd(t, s);
        wr(t, c, "again");
        try pause();
        rd(t, c);
        rd(t, c);
        rd(t, s);
    }
    fn r18(t: *Tok) !void {
        const l, const c, const s = try connected();
        defer l.close();
        defer c.close();
        defer s.close();
        t.ok(s.shutdown(.both));
        try pause();
        rd(t, c);
        wr(t, c, "after");
        try pause();
        rd(t, s);
        wr(t, c, "again");
        try pause();
        rd(t, c);
    }
    fn r19(t: *Tok) !void {
        const l, const c, const s = try connected();
        defer l.close();
        defer c.close();
        defer s.close();
        var b: Blocked = .{ .x = s, .is_send = false };
        var h = try rt_mod.spawn(Blocked.run, .{&b});
        try rt_mod.sleep(.fromMilliseconds(200));
        t.ok(s.shutdown(.send));
        try rt_mod.sleep(.fromMilliseconds(300));
        t.put(if (b.done) "woke" else "blocked");
        wr(t, c, "z");
        h.join();
        t.n(b.result);
    }
    fn r20(t: *Tok) !void {
        const l, const c, const s = try connected();
        defer l.close();
        defer c.close();
        wr(t, s, "landed");
        try pause();
        try linger0(s);
        s.close();
        try pause();
        rd(t, c);
        rd(t, c);
        rd(t, c);
        wr(t, c, "x");
    }
    fn r21(t: *Tok) !void {
        const l, const c, const s = try connected();
        defer l.close();
        defer c.close();
        wr(t, s, "landed");
        try pause();
        s.close();
        try pause();
        rd(t, c);
        rd(t, c);
        rd(t, c);
    }
    fn r22(t: *Tok) !void {
        const l, const c, const s = try connected();
        defer l.close();
        defer c.close();
        wr(t, s, "hello");
        s.close();
        try pause();
        wr(t, c, "x");
        try pause();
        rd(t, c);
        rd(t, c);
        rd(t, c);
        wr(t, c, "y");
    }
    /// A send blocked on a full window in a task, then `event`.
    fn blockedSend(t: *Tok, c: zio_net.Stream, timeout: @import("time.zig").Timeout, comptime event: fn () anyerror!void) !void {
        var b: Blocked = .{ .x = c, .is_send = true, .timeout = timeout };
        var h = try rt_mod.spawn(Blocked.run, .{&b});
        try rt_mod.sleep(.fromMilliseconds(200));
        t.put(if (b.done) "NOT-BLOCKED" else "blocked");
        try event();
        h.join();
        t.n(b.result);
    }
    var ev_c: zio_net.Stream = undefined;
    var ev_s: zio_net.Stream = undefined;
    fn peerResets() !void {
        try linger0(ev_s);
        ev_s.close();
    }
    fn ownShutWr() !void {
        try ev_c.shutdown(.send);
    }
    fn peerCloses() !void {
        ev_s.close();
    }
    fn peerShutBoth() !void {
        try ev_s.shutdown(.both);
    }
    fn r23(t: *Tok) !void {
        const l, ev_c, ev_s = try connected();
        defer l.close();
        defer ev_c.close();
        try blockedSend(t, ev_c, .none, peerResets);
        rd(t, ev_c);
        rd(t, ev_c);
    }
    fn r24(t: *Tok) !void {
        const l, ev_c, ev_s = try connected();
        defer l.close();
        defer ev_c.close();
        defer ev_s.close();
        try blockedSend(t, ev_c, .none, ownShutWr);
    }
    fn r25(t: *Tok) !void {
        const l, ev_c, ev_s = try connected();
        defer l.close();
        defer ev_c.close();
        try blockedSend(t, ev_c, .none, peerCloses);
    }
    /// Rows 26, 26b and 27: the client acts before accept.
    fn beforeAccept(t: *Tok, comptime client: fn (zio_net.Stream) anyerror!void, comptime after: fn (*Tok, zio_net.Stream) void) !void {
        const l = try t1.listen(lo, 0, .{});
        defer l.close();
        const c = try t1.connect(lo, t1.portOf(l), .none);
        try client(c);
        try pause();
        const s = l.accept(.{}) catch |err| return t.e(err);
        defer s.close();
        t.put("ok");
        after(t, s);
    }
    fn resetC(c: zio_net.Stream) !void {
        try pause();
        try linger0(c);
        c.close();
    }
    fn sendCloseC(c: zio_net.Stream) !void {
        _ = try c.write("q", .none);
        try pause();
        c.close();
    }
    fn readTwice(t: *Tok, s: zio_net.Stream) void {
        rd(t, s);
        rd(t, s);
    }
    fn sendTwiceReadTwice(t: *Tok, s: zio_net.Stream) void {
        wr(t, s, "s");
        wr(t, s, "t");
        rd(t, s);
        rd(t, s);
    }
    fn readTwiceSend(t: *Tok, s: zio_net.Stream) void {
        rd(t, s);
        rd(t, s);
        wr(t, s, "r");
    }
    fn r26(t: *Tok) !void {
        try beforeAccept(t, resetC, readTwice);
    }
    fn r26b(t: *Tok) !void {
        try beforeAccept(t, resetC, sendTwiceReadTwice);
    }
    fn r27(t: *Tok) !void {
        try beforeAccept(t, sendCloseC, readTwiceSend);
    }
    fn r28(t: *Tok) !void {
        const l = try t1.listen(lo, 0, .{});
        const c = try t1.connect(lo, t1.portOf(l), .none);
        defer c.close();
        wr(t, c, "q");
        try pause();
        l.close();
        try pause();
        rd(t, c);
        rd(t, c);
        wr(t, c, "x");
    }
    fn r29(t: *Tok) !void {
        const l, const c, const s = try connected();
        defer c.close();
        defer s.close();
        l.close();
        wr(t, c, "ok");
        try pause();
        rd(t, s);
    }
    fn r35(t: *Tok) !void {
        const l, const c, const s = try connected();
        defer l.close();
        defer c.close();
        defer s.close();
        wr(t, c, "c");
        wr(t, s, "s");
        t.ok(c.shutdown(.send));
        t.ok(s.shutdown(.send));
        try pause();
        rd(t, c);
        rd(t, c);
        rd(t, s);
        rd(t, s);
    }
    pub fn resetPair() !struct { zio_net.Server, zio_net.Stream } {
        const l, const c, const s = try connected();
        try linger0(s);
        s.close();
        try pause();
        return .{ l, c };
    }
    fn r36(t: *Tok) !void {
        const l, const c = try resetPair();
        defer l.close();
        defer c.close();
        wr(t, c, "x");
        wr(t, c, "y");
        rd(t, c);
        rd(t, c);
    }
    fn r37(t: *Tok) !void {
        const l, const c = try resetPair();
        defer l.close();
        defer c.close();
        t.ok(c.shutdown(.send));
        t.ok(c.shutdown(.both));
        rd(t, c);
    }
    fn r38(t: *Tok) !void {
        const l, const c = try resetPair();
        defer l.close();
        defer c.close();
        rd(t, c);
        t.ok(c.shutdown(.both));
    }
    /// Own SHUT_WR, then the peer resets: rows 39, 39r, 39u, 39w.
    fn shutThenReset(unread_peer: bool) !struct { zio_net.Server, zio_net.Stream } {
        const l, const c, const s = try connected();
        if (unread_peer) _ = try c.write("u", .none);
        try c.shutdown(.send);
        try pause();
        if (!unread_peer) try linger0(s);
        s.close();
        try pause();
        return .{ l, c };
    }
    fn r39(t: *Tok) !void {
        const l, const c = try shutThenReset(false);
        defer l.close();
        defer c.close();
        wr(t, c, "x");
        wr(t, c, "y");
        rd(t, c);
        rd(t, c);
    }
    fn r39r(t: *Tok) !void {
        const l, const c = try shutThenReset(false);
        defer l.close();
        defer c.close();
        rd(t, c);
        rd(t, c);
        wr(t, c, "x");
        wr(t, c, "y");
    }
    fn r39u(t: *Tok) !void {
        const l, const c = try shutThenReset(true);
        defer l.close();
        defer c.close();
        wr(t, c, "x");
        wr(t, c, "y");
        rd(t, c);
        rd(t, c);
    }
    fn r39w(t: *Tok) !void {
        const l, const c = try shutThenReset(false);
        defer l.close();
        defer c.close();
        t.ok(c.shutdown(.send));
        t.ok(c.shutdown(.both));
        rd(t, c);
        rd(t, c);
    }
    fn r40(t: *Tok) !void {
        const l, const c, const s = try connected();
        defer l.close();
        defer c.close();
        defer s.close();
        t.ok(s.shutdown(.both));
        try pause();
        t.n(c.write(big[0..70000], .none));
        try pause();
        rd(t, s);
        rd(t, s);
        rd(t, s);
        wr(t, c, "z");
        rd(t, c);
    }
    fn r41(t: *Tok) !void {
        const l, ev_c, ev_s = try connected();
        defer l.close();
        defer ev_c.close();
        defer ev_s.close();
        try blockedSend(t, ev_c, .fromSeconds(1), peerShutBoth);
        rd(t, ev_c);
        rdData(t, ev_s);
    }

    /// `macos` and `linux` are the design's columns: the probes' answers,
    /// which a sim run must give under that profile. `macos_zio` and
    /// `linux_zio`, where set, are what zio gives on that kernel's real
    /// sockets when it differs from the probe: an open difference (t-4119),
    /// asserted on both sides so that a change to either is seen.
    const Row = struct {
        name: []const u8,
        f: *const fn (*Tok) anyerror!void,
        macos: []const u8,
        linux: []const u8,
        macos_zio: ?[]const u8 = null,
        linux_zio: ?[]const u8 = null,
    };
    const rows = [_]Row{
        .{ .name = "5", .f = r5, .macos = "ok,0,1,1,EPIPE", .linux = "ok,0,1,1,EPIPE" },
        .{ .name = "7", .f = r7, .macos = "3,ECONNRESET,0", .linux = "3,ECONNRESET,0" },
        .{ .name = "8", .f = r8, .macos = "1,EPIPE,ECONNRESET", .linux = "1,EPIPE,0" },
        .{ .name = "9", .f = r9, .macos = "ECONNRESET,ENOTCONN", .linux = "ECONNRESET,ENOTCONN" },
        .{ .name = "16", .f = r16, .macos = "ok,0,0,0", .linux = "ok,0,0,0" },
        .{ .name = "17", .f = r17, .macos = "6,ok,0,5,0,EPIPE,ECONNRESET,0,0", .linux = "6,ok,6,5,ECONNRESET,EPIPE,0,0,0" },
        .{ .name = "18", .f = r18, .macos = "ok,0,5,0,EPIPE,ECONNRESET", .linux = "ok,0,5,ECONNRESET,EPIPE,0" },
        .{ .name = "19", .f = r19, .macos = "ok,blocked,1,1", .linux = "ok,blocked,1,1" },
        .{ .name = "20", .f = r20, .macos = "6,6,ECONNRESET,0,EPIPE", .linux = "6,6,ECONNRESET,0,EPIPE" },
        .{ .name = "21", .f = r21, .macos = "6,6,0,0", .linux = "6,6,0,0" },
        .{ .name = "22", .f = r22, .macos = "5,1,5,ECONNRESET,0,EPIPE", .linux = "5,1,5,0,0,EPIPE" },
        // t-4119: zio's kqueue backend ends a parked send with SO_ERROR
        // (ECONNRESET), which also clears it, so the reads give 0.
        .{ .name = "23", .f = r23, .macos = "blocked,EPIPE,ECONNRESET,0", .linux = "blocked,ECONNRESET,0,0", .macos_zio = "blocked,ECONNRESET,0,0" },
        .{ .name = "24", .f = r24, .macos = "blocked,EPIPE", .linux = "blocked,EPIPE" },
        .{ .name = "25", .f = r25, .macos = "blocked,EPIPE", .linux = "blocked,ECONNRESET", .macos_zio = "blocked,ECONNRESET" },
        .{ .name = "26", .f = r26, .macos = "ok,ECONNRESET,0", .linux = "ok,ECONNRESET,0" },
        .{ .name = "26b", .f = r26b, .macos = "ok,EPIPE,EPIPE,ECONNRESET,0", .linux = "ok,ECONNRESET,EPIPE,0,0" },
        .{ .name = "27", .f = r27, .macos = "ok,1,0,1", .linux = "ok,1,0,1" },
        .{ .name = "28", .f = r28, .macos = "1,ECONNRESET,0,EPIPE", .linux = "1,ECONNRESET,0,EPIPE" },
        .{ .name = "29", .f = r29, .macos = "2,2", .linux = "2,2" },
        .{ .name = "35", .f = r35, .macos = "1,1,ok,ok,1,0,1,0", .linux = "1,1,ok,ok,1,0,1,0" },
        .{ .name = "36", .f = r36, .macos = "EPIPE,EPIPE,ECONNRESET,0", .linux = "ECONNRESET,EPIPE,0,0" },
        .{ .name = "37", .f = r37, .macos = "ENOTCONN,ENOTCONN,ECONNRESET", .linux = "ENOTCONN,ENOTCONN,ECONNRESET" },
        .{ .name = "38", .f = r38, .macos = "ECONNRESET,ENOTCONN", .linux = "ECONNRESET,ENOTCONN" },
        .{ .name = "39", .f = r39, .macos = "EPIPE,EPIPE,ECONNRESET,0", .linux = "ECONNRESET,EPIPE,0,0" },
        .{ .name = "39r", .f = r39r, .macos = "ECONNRESET,0,EPIPE,EPIPE", .linux = "ECONNRESET,0,EPIPE,EPIPE" },
        .{ .name = "39u", .f = r39u, .macos = "EPIPE,EPIPE,ECONNRESET,0", .linux = "ECONNRESET,EPIPE,0,0" },
        .{ .name = "39w", .f = r39w, .macos = "ENOTCONN,ENOTCONN,ECONNRESET,0", .linux = "ENOTCONN,ENOTCONN,ECONNRESET,0" },
        .{ .name = "40", .f = r40, .macos = "ok,70000,0,0,0,EPIPE,ECONNRESET", .linux = "ok,70000,ECONNRESET,0,0,EPIPE,0" },
        // t-4119: on Linux zio's parked send ends EPIPE about 215 ms after
        // the receiver's SHUT_RDWR (one run with a 5 s timeout), where the
        // probe's blocked send stayed blocked past 1 s. Not understood.
        .{ .name = "41", .f = r41, .macos = "blocked,EPIPE,ECONNRESET,0", .linux = "blocked,TIMEOUT,0,data", .macos_zio = "blocked,ECONNRESET,0,0", .linux_zio = "blocked,EPIPE,0,data" },
    };

    var failed: usize = 0;
    var column: Kernel = .macos;

    fn body() !void {
        failed = 0;
        for (rows) |r| {
            var t: Tok = .{};
            r.f(&t) catch |err| t.put(@errorName(err));
            const design = if (column == .macos) r.macos else r.linux;
            const zio_col = if (column == .macos) r.macos_zio else r.linux_zio;
            const want = if (sim.isRunning()) design else zio_col orelse design;
            if (zio_col) |z| std.debug.print("parity row {s}, {t}: open difference (t-4119): the design's column {s}, zio on the kernel {s}; this run checks the {s} one\n", .{ r.name, column, design, z, if (sim.isRunning()) "design's" else "kernel's" });
            if (!std.mem.eql(u8, want, t.str())) {
                std.debug.print("parity row {s}, {t} column: want {s}\n                     got  {s}\n", .{ r.name, column, want, t.str() });
                failed += 1;
            }
        }
        if (sim.isRunning()) try drain();
    }

    fn inSim(k: Kernel) !void {
        column = k;
        try run(1, if (k == .macos) .macos else .linux, body);
        if (failed != 0) return error.ParityRowsDiffer;
    }
};

test "sim: stream parity: slice 2's rows (5, 7 to 9, 16 to 29, 35 to 41) through zio.net, on this host's kernel without sim, in a sim run with its profile with sim" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    if (comptime sim.compiled_in) return prow.inSim(host_kernel);
    prow.column = host_kernel;
    const rt = try rt_mod.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var h = try rt.spawn(prow.body, .{});
    try h.join();
    if (prow.failed != 0) return error.ParityRowsDiffer;
}

test "sim: stream parity: slice 2's rows, the other kernel's column in a sim run" {
    if (comptime skip) return error.SkipZigTest;
    try prow.inSim(if (host_kernel == .macos) .linux else .macos);
}

// ---- The wake rules and the cancel points (design 17.8) ----

fn noop(_: *ev.Loop, _: *ev.Completion) void {}

const cancel_parked = struct {
    var rop: ev.NetRecv = undefined;
    var sop: ev.NetSend = undefined;
    var rbuf: [16]u8 = undefined;
    var rst: [1]os_net.iovec = undefined;
    var sst: [1]os_net.iovec_const = undefined;

    fn body() !void {
        const srv, const c, const s = try pair();
        const loop = &rt_mod.getCurrentExecutor().loop;
        // A parked receive, cancelled: Canceled, and the bytes that land
        // later are there for the next receive.
        rop = ev.NetRecv.init(s.socket.handle, .fromSlices(&.{&rbuf}, &rst), .{});
        rop.c.callback = noop;
        rop.c.flags.defer_callback = false;
        loop.add(&rop.c);
        try rt_mod.sleep(.fromMicroseconds(10));
        loop.cancel(&rop.c);
        try expectError(error.Canceled, rop.getResult());
        try expectEqual(@as(usize, 3), try c.write("abc", .none));
        var b: [8]u8 = undefined;
        try expectEqual(@as(usize, 3), try s.read(&b, .none));
        // A parked send, cancelled: nothing more is admitted.
        sim.setStreamWindow(s.socket.handle, 3);
        try rt_mod.sleep(.fromMicroseconds(2 * L / us));
        try expectEqual(@as(usize, 3), try c.write("def", .none));
        sop = ev.NetSend.init(c.socket.handle, .fromSlices(&.{"ghi"}, &sst), .{});
        sop.c.callback = noop;
        sop.c.flags.defer_callback = false;
        loop.add(&sop.c);
        try rt_mod.sleep(.fromMicroseconds(10));
        loop.cancel(&sop.c);
        try expectError(error.Canceled, sop.getResult());
        try expectEqual(@as(u64, 6), (try dir(true)).admitted);
        // The window opens later: the next send takes the room.
        try expectEqual(@as(usize, 3), try s.read(&b, .none));
        try expectEqual(@as(usize, 3), try c.write("jkl", .none));
        try checkConserved();
        try t1.closeAll3(srv, c, s);
    }
};

test "sim: stream bytes: a parked receive cancelled gives Canceled and leaves later bytes to the next receive; a parked send cancelled admits nothing" {
    if (comptime skip) return error.SkipZigTest;
    try run(1, null, cancel_parked.body);
}

/// Two connections whose receives are woken at one instant: the first
/// harvested callback cancels the other one, woken and not yet harvested.
/// Its bytes stay unread for the next receive (a wake takes nothing).
const woken_recv_cancel = struct {
    var ops: [2]ev.NetRecv = undefined;
    var bufs: [2][8]u8 = undefined;
    var st: [2][1]os_net.iovec = undefined;
    var first: ?usize = null;
    var seen: [2]u32 = .{ 0, 0 };

    fn onDone(l: *ev.Loop, comp: *ev.Completion) void {
        if (first != null) return;
        const i: usize = if (comp == &ops[0].c) 0 else 1;
        first = i;
        l.cancel(&ops[1 - i].c);
    }

    fn body() !void {
        first = null;
        const l0 = try t1.listen(lo, 0, .{});
        const c0 = try t1.connect(lo, t1.portOf(l0), .none);
        const s0 = try l0.accept(.{});
        const c1 = try t1.connect(lo, t1.portOf(l0), .none);
        const s1 = try l0.accept(.{});
        const loop = &rt_mod.getCurrentExecutor().loop;
        for ([_]zio_net.Stream{ s0, s1 }, 0..) |s, i| {
            ops[i] = ev.NetRecv.init(s.socket.handle, .fromSlices(&.{&bufs[i]}, &st[i]), .{});
            ops[i].c.callback = onDone;
            ops[i].c.flags.defer_callback = false;
            loop.add(&ops[i].c);
        }
        _ = try c0.write("a", .none);
        _ = try c1.write("b", .none);
        try rt_mod.sleep(.fromMicroseconds(2 * L / us));
        const i = first orelse return error.NoWake;
        seen[i] += 1;
        try expectEqual(@as(usize, 1), try ops[i].getResult());
        try expectError(error.Canceled, ops[1 - i].getResult());
        var b: [8]u8 = undefined;
        const other = if (i == 0) s1 else s0;
        try expectEqual(@as(usize, 1), try other.read(&b, .none));
        try expectEqual(@as(u8, if (i == 0) 'b' else 'a'), b[0]);
        for ([_]zio_net.Stream{ c0, s0, c1, s1 }) |x| x.close();
        l0.close();
        try drain();
    }
};

test "sim: stream bytes: a receive woken by a landing and cancelled inside the harvest batch gives Canceled and leaves its bytes, in both batch orders" {
    if (comptime skip) return error.SkipZigTest;
    woken_recv_cancel.seen = .{ 0, 0 };
    for (1..17) |seed| try run(seed, null, woken_recv_cancel.body);
    if (woken_recv_cancel.seen[0] == 0 or woken_recv_cancel.seen[1] == 0) return error.OneBatchOrderNeverHappened;
}

/// Two connections whose parked sends are woken by credits landing at one
/// instant: the first harvested cancels the other, which then admitted
/// nothing (admission is at the harvest).
const woken_send_cancel = struct {
    var ops: [2]ev.NetSend = undefined;
    var st: [2][1]os_net.iovec_const = undefined;
    var first: ?usize = null;
    var seen: [2]u32 = .{ 0, 0 };

    fn onDone(l: *ev.Loop, comp: *ev.Completion) void {
        if (first != null) return;
        const i: usize = if (comp == &ops[0].c) 0 else 1;
        first = i;
        l.cancel(&ops[1 - i].c);
    }

    fn reader(s: zio_net.Stream) !void {
        var b: [4]u8 = undefined;
        _ = try s.read(&b, .none);
    }

    fn body() !void {
        first = null;
        const l0 = try t1.listen(lo, 0, .{});
        const c0 = try t1.connect(lo, t1.portOf(l0), .none);
        const s0 = try l0.accept(.{});
        const c1 = try t1.connect(lo, t1.portOf(l0), .none);
        const s1 = try l0.accept(.{});
        const loop = &rt_mod.getCurrentExecutor().loop;
        for ([_]zio_net.Stream{ s0, s1 }) |s| sim.setStreamWindow(s.socket.handle, 1);
        try rt_mod.sleep(.fromMicroseconds(L / us));
        _ = try c0.write("x", .none);
        _ = try c1.write("x", .none);
        for ([_]zio_net.Stream{ c0, c1 }, 0..) |c, i| {
            ops[i] = ev.NetSend.init(c.socket.handle, .fromSlices(&.{"y"}, &st[i]), .{});
            ops[i].c.callback = onDone;
            ops[i].c.flags.defer_callback = false;
            loop.add(&ops[i].c);
        }
        // Both reads at L, both credits land at 2L, in one harvest.
        var h0 = try rt_mod.spawn(reader, .{s0});
        var h1 = try rt_mod.spawn(reader, .{s1});
        try h0.join();
        try h1.join();
        try rt_mod.sleep(.fromMicroseconds(2 * L / us));
        const i = first orelse return error.NoWake;
        seen[i] += 1;
        try expectEqual(@as(usize, 1), try ops[i].getResult());
        try expectError(error.Canceled, ops[1 - i].getResult());
        try expectEqual(@as(u64, 1), sim.testStreamDir(@intCast(2 - i), true).?.admitted);
        try expectEqual(@as(u64, 2), sim.testStreamDir(@intCast(1 + i), true).?.admitted);
        for ([_]zio_net.Stream{ c0, s0, c1, s1 }) |x| x.close();
        l0.close();
        try drain();
    }
};

test "sim: stream bytes: a send woken by a credit and cancelled inside the harvest batch admits nothing, in both batch orders" {
    if (comptime skip) return error.SkipZigTest;
    woken_send_cancel.seen = .{ 0, 0 };
    for (1..17) |seed| try run(seed, null, woken_send_cancel.body);
    if (woken_send_cancel.seen[0] == 0 or woken_send_cancel.seen[1] == 0) return error.OneBatchOrderNeverHappened;
}

const close_wakes = struct {
    var rop: ev.NetRecv = undefined;
    var rbuf: [8]u8 = undefined;
    var rst: [1]os_net.iovec = undefined;

    fn body() !void {
        const srv, const c, const s = try pair();
        const loop = &rt_mod.getCurrentExecutor().loop;
        rop = ev.NetRecv.init(s.socket.handle, .fromSlices(&.{&rbuf}, &rst), .{});
        rop.c.callback = noop;
        rop.c.flags.defer_callback = false;
        loop.add(&rop.c);
        try rt_mod.sleep(.fromMicroseconds(10));
        s.close();
        try rt_mod.sleep(.fromMicroseconds(1));
        try expectError(error.FileDescriptorNotASocket, rop.getResult());
        try t1.closeAll2(srv, c);
    }
};

test "sim: stream bytes: closing the socket of a parked receive ends it FileDescriptorNotASocket" {
    if (comptime skip) return error.SkipZigTest;
    try run(1, null, close_wakes.body);
}

// ---- Close, reset and shutdown accounting (design 17.7, 17.8) ----

const close_after_reset = struct {
    fn body() !void {
        const srv, const c, const s = try pair();
        try expectEqual(@as(usize, 4), try s.write("abcd", .none));
        try rt_mod.sleep(.fromMicroseconds(L / us));
        var b: [1]u8 = undefined;
        try expectEqual(@as(usize, 1), try c.read(&b, .none));
        try prow.linger0(s);
        s.close();
        try rt_mod.sleep(.fromMicroseconds(L / us));
        try expect(sim.testStreamEnd(c.socket.handle).reset_received);
        try expectEqual(@as(usize, 3), sim.testStreamEnd(c.socket.handle).unread);
        // Review 3, finding 6: close after a reset discards and counts the
        // three unread bytes, and puts nothing on the wire.
        try checkConserved();
        c.close();
        try expectEqual(@as(usize, 0), sim.streamEntriesInFlight());
        try expectEqual(@as(u64, 3), sim.streamStats().discarded[@backingInt(sim.Discard.unread_at_close)]);
        try t1.closeAll1(srv);
    }
};

test "sim: stream bytes: close after a reset with unread bytes counts them unread_at_close and sends nothing (review 3, finding 6)" {
    if (comptime skip) return error.SkipZigTest;
    try run(1, null, close_after_reset.body);
}

const stale_credit = struct {
    fn body() !void {
        const srv, const c, const s = try pair();
        _ = try c.write("abc", .none);
        var b: [8]u8 = undefined;
        try expectEqual(@as(usize, 3), try s.read(&b, .none));
        // The credit left at this read; the sender closes before it lands.
        const before = sim.streamStats().stale_credit;
        c.close();
        try rt_mod.sleep(.fromMicroseconds(L / us));
        try expectEqual(before + 1, sim.streamStats().stale_credit);
        try t1.closeAll2(srv, s);
    }
};

test "sim: stream bytes: a credit that lands after its sender closed is dropped and counted stale_credit" {
    if (comptime skip) return error.SkipZigTest;
    try run(1, null, stale_credit.body);
}

const owed_after_close = struct {
    fn body() !void {
        const srv, const c, const s = try pair();
        sim.setStreamChunking(c.socket.handle, .{ .fixed = 1 });
        _ = try c.write("ab", .none);
        var b: [1]u8 = undefined;
        _ = try s.read(&b, .none);
        _ = try s.read(&b, .none);
        // The second read's credit is owed for one L after the first's.
        try expect((try dir(true)).owed);
        s.close();
        const t_close = sim.clockNs();
        try expect((try dir(true)).owed);
        // On the wire: the first read's credit and the close's FIN; and
        // the owed credit.
        try expectEqual(@as(usize, 3), sim.streamEntriesInFlight());
        c.close();
        try drain();
        try expect(sim.clockNs() >= t_close + L);
        try expectEqual(@as(?sim.TestStreamDir, null), sim.testStreamDir(1, true));
        srv.close();
    }
};

test "sim: stream bytes: a credit owed when its receiver closes still departs, and the connection retires only after it" {
    if (comptime skip) return error.SkipZigTest;
    try run(1, null, owed_after_close.body);
}

const fin_after_chunks = struct {
    fn body() !void {
        const srv, const c, const s = try pair();
        sim.setStreamChunking(c.socket.handle, .{ .fixed = 1000 });
        var data: [5000]u8 = undefined;
        fill(&data, 2);
        const t0 = sim.clockNs();
        _ = try c.write(&data, .none);
        // The FIN leaves in the same instant: it lands with the last chunk
        // (t0 + L + 4 µs), after it in sequence, never between chunks.
        try c.shutdown(.send);
        var buf: [6000]u8 = undefined;
        var got: usize = 0;
        while (true) {
            const n = try s.read(buf[got..], .none);
            if (n == 0) break;
            got += n;
        }
        try expectEqual(@as(usize, 5000), got);
        try expectEqual(t0 + L + 4 * us, sim.clockNs());
        try expect(std.mem.eql(u8, &data, buf[0..5000]));
        try t1.closeAll3(srv, c, s);
    }
};

test "sim: stream bytes: a FIN sent in the same instant as a five-chunk send lands with its last chunk and after it (the order within a direction)" {
    if (comptime skip) return error.SkipZigTest;
    try run(1, null, fin_after_chunks.body);
}

const equal_conns = struct {
    var order: [2]u8 = undefined;
    var n: usize = 0;
    fn reader(s: zio_net.Stream, tag: u8) !void {
        var b: [4]u8 = undefined;
        _ = try s.read(&b, .none);
        order[n] = tag;
        n += 1;
    }
    fn body() !void {
        n = 0;
        const l0 = try t1.listen(lo, 0, .{});
        const c0 = try t1.connect(lo, t1.portOf(l0), .none);
        const s0 = try l0.accept(.{});
        const c1 = try t1.connect(lo, t1.portOf(l0), .none);
        const s1 = try l0.accept(.{});
        // Connection 2 sends first; both chunks land at one instant, in
        // conn_id order, so connection 1's receiver has its bytes first.
        _ = try c1.write("2", .none);
        _ = try c0.write("1", .none);
        try expectEqual(@as(usize, 1), sim.testStreamEnd(s0.socket.handle).unread + 1);
        try rt_mod.sleep(.fromMicroseconds(L / us));
        try expectEqual(@as(usize, 1), sim.testStreamEnd(s0.socket.handle).unread);
        try expectEqual(@as(usize, 1), sim.testStreamEnd(s1.socket.handle).unread);
        for ([_]zio_net.Stream{ c0, s0, c1, s1 }) |x| x.close();
        l0.close();
        try drain();
    }
};

test "sim: stream bytes: chunks of two connections that land at one instant both land in that poll, whichever was sent first" {
    if (comptime skip) return error.SkipZigTest;
    try run(1, null, equal_conns.body);
}

const rst_second = struct {
    fn body() !void {
        const srv, const c, const s = try pair();
        try s.shutdown(.both);
        try rt_mod.sleep(.fromMicroseconds(L / us));
        _ = try c.write("late", .none);
        try rt_mod.sleep(.fromMicroseconds(L / us));
        // Under .linux the late chunk reset s locally (row 18).
        try expect(sim.testStreamEnd(s.socket.handle).reset_received);
        try expectEqual(@as(u64, 1), sim.streamStats().local_resets);
        // A second reset, an RST from c's abortive close, lands at s:
        // dropped and counted, and the first is still reported once.
        const before = sim.streamStats().dropped_after_reset;
        try prow.linger0(c);
        c.close();
        try rt_mod.sleep(.fromMicroseconds(L / us));
        try expectEqual(before + 1, sim.streamStats().dropped_after_reset);
        var b: [4]u8 = undefined;
        try expectError(error.ConnectionResetByPeer, s.read(&b, .none));
        try expectEqual(@as(usize, 0), try s.read(&b, .none));
        try t1.closeAll2(srv, s);
    }
};

test "sim: stream bytes: under .linux a local reset, then a landed RST: the second is dropped and counted, and the reset is reported once" {
    if (comptime skip) return error.SkipZigTest;
    try run(1, .linux, rst_second.body);
}

const release = struct {
    var sent: [2]usize = .{ 0, 0 };
    var at: u64 = 0;
    fn sender(c: zio_net.Stream) !void {
        var data: [3000]u8 = undefined;
        fill(&data, 4);
        sent[0] = try c.write(&data, .none);
        sent[1] = try c.write(&data, .none);
        at = sim.clockNs();
    }
    fn body() !void {
        const srv, const c, const s = try pair();
        sim.setStreamWindow(s.socket.handle, 3000);
        sim.setStreamChunking(c.socket.handle, .off);
        var h = try rt_mod.spawn(sender, .{c});
        try rt_mod.sleep(.fromMicroseconds(L / us + 10));
        try expectEqual(@as(usize, 3000), sim.testStreamEnd(s.socket.handle).unread);
        const t0 = sim.clockNs();
        // .macos: .both discards the 3000 and releases the window.
        try s.shutdown(.both);
        const d = try dir(true);
        try expectEqual(@as(u64, 3000), d.reclaimed);
        try expectEqual(@as(u64, 3000), d.discarded[@backingInt(sim.Discard.both_discard)]);
        try h.join();
        // The release's credit landed at t0 + L and woke the parked send.
        try expectEqual(t0 + L, at);
        try expectEqual(@as(usize, 3000), sent[1]);
        try checkConserved();
        try t1.closeAll3(srv, c, s);
    }
};

test "sim: stream bytes: under .macos shutdown(.both) discards the unread bytes, counts them reclaimed, and its credit wakes a sender parked on the full window (review 3, finding 7)" {
    if (comptime skip) return error.SkipZigTest;
    try run(1, .macos, release.body);
}

// ---- Refusals and the no-profile points (design 17.4, 17.7, 17.8) ----

const refuse2 = struct {
    fn zeroRecv() !void {
        _, const c, _ = try pair();
        var b: [0]u8 = undefined;
        _ = try evRecv(c.socket.handle, &b);
    }
    var srv_keep: zio_net.Stream = undefined;
    fn reader() !void {
        var b: [4]u8 = undefined;
        _ = try srv_keep.read(&b, .none);
    }
    fn secondRecv() !void {
        _, const c, _ = try pair();
        srv_keep = c;
        _ = try rt_mod.spawn(reader, .{});
        try rt_mod.sleep(.fromMicroseconds(10));
        var b: [4]u8 = undefined;
        _ = try c.read(&b, .none);
    }
    fn writer() !void {
        _ = try srv_keep.write("z", .none);
    }
    fn secondSend() !void {
        _, const c, const s = try pair();
        sim.setStreamWindow(s.socket.handle, 1);
        _ = try c.write("y", .none);
        srv_keep = c;
        _ = try rt_mod.spawn(writer, .{});
        try rt_mod.sleep(.fromMicroseconds(10));
        _ = try c.write("z", .none);
    }
    fn window0() !void {
        _, _, const s = try pair();
        sim.setStreamWindow(s.socket.handle, 0);
    }
    fn fixed0() !void {
        sim.setStreamChunkingDefault(.{ .fixed = 0 });
    }
    fn entryLimit() !void {
        _, const c, const s = try pair();
        sim.setStreamWindow(s.socket.handle, 70000);
        sim.setStreamChunking(c.socket.handle, .{ .fixed = 1 });
        _ = try c.write(prow.big[0..70000], .none);
    }
    fn recvCSyn() !void {
        const l = try t1.listen(lo, 0, .{});
        const fd = try t1.evOpen();
        var to = t1.sockaddr(lo, t1.portOf(l));
        var op = ev.NetConnect.init(fd, @ptrCast(&to), @sizeOf(os_net.sockaddr.in));
        op.c.callback = noop;
        rt_mod.getCurrentExecutor().loop.add(&op.c);
        var b: [4]u8 = undefined;
        _ = try evRecv(fd, &b);
    }
    fn sendMeetsReset() !void {
        _, const c = try prow.resetPair();
        _ = try c.write("x", .none);
    }
    fn readAfterFinReset() !void {
        _, const c, const s = try pair();
        s.close();
        try prow.pause();
        _ = try c.write("x", .none);
        try prow.pause();
        var b: [4]u8 = undefined;
        _ = try c.read(&b, .none);
    }
    fn bothUnread() !void {
        _, const c, const s = try pair();
        _ = try c.write("x", .none);
        try prow.pause();
        try s.shutdown(.both);
    }
    fn dataAfterBoth() !void {
        _, const c, const s = try pair();
        try s.shutdown(.both);
        try prow.pause();
        _ = try c.write("x", .none);
        try prow.pause();
    }
};

test "sim: stream refuses: a receive of 0 bytes (t-4096's ruling)" {
    try refusal("sim: stream refuses: a receive of 0 bytes", "sim: a receive of 0 bytes on sim stream socket 100033 is refused", null, refuse2.zeroRecv);
}
test "sim: stream refuses: a second receive while one is parked" {
    try refusal("sim: stream refuses: a second receive while one is parked", "sim: a second receive while one is parked, on sim stream socket 100033", null, refuse2.secondRecv);
}
test "sim: stream refuses: a second send while one is parked" {
    try refusal("sim: stream refuses: a second send while one is parked", "sim: a second send while one is parked, on sim stream socket 100033", null, refuse2.secondSend);
}
test "sim: stream refuses: a window of 0 bytes" {
    try refusal("sim: stream refuses: a window of 0 bytes", "sim: a stream window of 0 bytes: the bounds are 1 to 16777216", null, refuse2.window0);
}
test "sim: stream refuses: chunk mode .fixed = 0" {
    try refusal("sim: stream refuses: chunk mode .fixed = 0", "sim: chunk mode .fixed = 0: the bounds are 1 to 65536", null, refuse2.fixed0);
}
test "sim: stream refuses: a send whose chunks pass max_tcp_entries" {
    try refusal("sim: stream refuses: a send whose chunks pass max_tcp_entries", "sim: a send of 70000 bytes on connection 1 needs 70000 chunks", null, refuse2.entryLimit);
}
test "sim: stream refuses: a receive on a socket whose connect is parked" {
    try refusal("sim: stream refuses: a receive on a socket whose connect is parked", "sim: NetRecv on sim fd 100033, whose connection is in phase c_syn, not established", null, refuse2.recvCSyn);
}
test "sim: stream refuses: with no profile, a send that meets a reset with no FIN first (rows 23, 25, 26b, 36, 39)" {
    try refusal("sim: stream refuses: with no profile, a send that meets a reset", "sim: rows 23, 25, 26b, 36 and 39, a send that meets a reset with no FIN before it: the kernels differ here", null, refuse2.sendMeetsReset);
}
test "sim: stream refuses: with no profile, a read that meets a reset after a FIN (rows 8, 22)" {
    try refusal("sim: stream refuses: with no profile, a read that meets a reset after a FIN", "sim: rows 8 and 22, a read that meets a reset after a FIN had landed: the kernels differ here", null, refuse2.readAfterFinReset);
}
test "sim: stream refuses: with no profile, shutdown(.both) with unread bytes (row 17)" {
    try refusal("sim: stream refuses: with no profile, shutdown(.both) with unread bytes", "sim: row 17, shutdown(.both) with unread bytes: the kernels differ here", null, refuse2.bothUnread);
}
test "sim: stream refuses: with no profile, data that lands after shutdown(.both) (row 18)" {
    try refusal("sim: stream refuses: with no profile, data that lands after shutdown(.both)", "sim: row 18, a data chunk that lands after the endpoint's own shutdown(.both): the kernels differ here", null, refuse2.dataAfterBoth);
}

// ---- Determinism: fresh processes (design 17.15 item 4) ----

const replay = struct {
    var transcript: [4096]u8 = undefined;
    var t_len: usize = 0;
    var out: [3]u64 = undefined;

    fn body() !void {
        t_len = 0;
        const srv, const c, const s = try pair();
        var data: [9000]u8 = undefined;
        fill(&data, 7);
        var h = try rt_mod.spawn(echo, .{s});
        try c.writeAll(&data, .none);
        try c.shutdown(.send);
        var buf: [256]u8 = undefined;
        while (true) {
            const n = try c.read(&buf, .none);
            if (n == 0) break;
            // The transcript: each read's length and clock, a byte each.
            if (t_len + 2 <= transcript.len) {
                transcript[t_len] = @truncate(n);
                transcript[t_len + 1] = @truncate(sim.clockNs() / us);
                t_len += 2;
            }
        }
        try h.join();
        out = .{ sim.traceHash(), sim.wireDigest(), sim.stateDigest() };
        try t1.closeAll3(srv, c, s);
    }

    fn echo(s: zio_net.Stream) !void {
        var buf: [700]u8 = undefined;
        while (true) {
            const n = try s.read(&buf, .none);
            if (n == 0) break;
            try s.writeAll(buf[0..n], .none);
        }
        try s.shutdown(.send);
    }
};

test "sim: stream bytes: five fresh processes with the heap moved give one transcript, wire digest, trace and state digest for an echo with hashed chunks" {
    if (comptime skip) return error.SkipZigTest;
    const name = "sim: stream bytes: five fresh processes with the heap moved give one transcript";
    if (sim.isDeathChild()) {
        sim.noCoreDump();
        const shift = std.fmt.parseInt(usize, std.mem.span(std.c.getenv("ZIO_SIM_HEAP_SHIFT") orelse "0"), 10) catch 0;
        if (shift > 0) _ = try std.heap.page_allocator.alloc(u8, shift * 4096 + 7);
        try run(5, .linux, replay.body);
        var line_buf: [9000]u8 = undefined;
        var w: std.Io.Writer = .fixed(&line_buf);
        try w.print("sim-stream: trace={x:0>16} wire={x:0>16} state={x:0>16} transcript=", .{ replay.out[0], replay.out[1], replay.out[2] });
        for (replay.transcript[0..replay.t_len]) |b| try w.print("{x:0>2}", .{b});
        try w.writeByte('\n');
        _ = std.c.write(2, w.buffered().ptr, w.buffered().len);
        sim.childDone(name);
        return;
    }
    const io = std.testing.io;
    const gpa = std.testing.allocator;
    var exe_buf: [std.fs.max_path_bytes]u8 = undefined;
    const exe_len = try std.process.executablePath(io, &exe_buf);
    var first: ?[]u8 = null;
    defer if (first) |f| gpa.free(f);
    for (1..6) |k| {
        var env: std.process.Environ.Map = .init(gpa);
        defer env.deinit();
        if (std.c.getenv("PATH")) |path| try env.put("PATH", std.mem.span(path));
        try env.put("TEST_FILTER", name);
        try env.put(sim.death_child_env, "1");
        var shift_buf: [16]u8 = undefined;
        try env.put("ZIO_SIM_HEAP_SHIFT", try std.fmt.bufPrint(&shift_buf, "{d}", .{k * 13}));
        const res = try std.process.run(gpa, io, .{
            .argv = &.{exe_buf[0..exe_len]},
            .environ_map = &env,
            .timeout = .{ .duration = .{ .raw = .fromSeconds(30), .clock = .awake } },
        });
        defer gpa.free(res.stdout);
        defer gpa.free(res.stderr);
        try sim.checkDoneMarkers(name, res.stderr);
        const at = std.mem.indexOf(u8, res.stderr, "sim-stream: ") orelse return error.NoLine;
        const end = std.mem.indexOfScalarPos(u8, res.stderr, at, '\n') orelse return error.NoLine;
        const line = res.stderr[at..end];
        if (first) |f| {
            if (!std.mem.eql(u8, f, line)) {
                std.debug.print("process 1: {s}\nprocess {d}: {s}\n", .{ f, k, line });
                return error.ProcessesDiffer;
            }
        } else {
            first = try gpa.dupe(u8, line);
            std.debug.print("{s}\n", .{line[0..@min(line.len, 120)]});
        }
    }
}

// ---- std.Io on zio's vtable (netRead, netWrite, netShutdown) ----

const std_io_bytes = struct {
    var server: std.Io.net.Server = undefined;
    var got: [12000]u8 = undefined;
    var got_n: usize = 0;

    fn peer(io: std.Io) !void {
        const p = try server.accept(io);
        defer p.close(io);
        var rbuf: [512]u8 = undefined;
        var r = std.Io.net.Stream.Reader.init(p, io, &rbuf);
        got_n = try r.interface.readSliceShort(&got);
        // It echoes the last 3 bytes back and shuts its send side.
        var wbuf: [64]u8 = undefined;
        var w = std.Io.net.Stream.Writer.init(p, io, &wbuf);
        try w.interface.writeAll(got[got_n - 3 .. got_n]);
        try w.interface.flush();
        try p.shutdown(io, .send);
    }

    fn body() !void {
        got_n = 0;
        const io = t1.runtime.io();
        server = try std.Io.net.IpAddress.listen(&.{ .ip4 = .loopback(0) }, io, .{ .reuse_address = true });
        defer server.deinit(io);
        var h = try rt_mod.spawn(peer, .{io});
        const client = try std.Io.net.IpAddress.connect(&server.socket.address, io, .{ .mode = .stream });
        defer client.close(io);
        var data: [10000]u8 = undefined;
        fill(&data, 6);
        var wbuf: [1024]u8 = undefined;
        var w = std.Io.net.Stream.Writer.init(client, io, &wbuf);
        try w.interface.writeAll(&data);
        try w.interface.flush();
        try client.shutdown(io, .send);
        var rbuf: [64]u8 = undefined;
        var r = std.Io.net.Stream.Reader.init(client, io, &rbuf);
        var back: [8]u8 = undefined;
        try expectEqual(@as(usize, 3), try r.interface.readSliceShort(&back));
        try h.join();
        try expectEqual(@as(usize, 10000), got_n);
        try expect(std.mem.eql(u8, &data, got[0..10000]));
        try expect(std.mem.eql(u8, data[9997..], back[0..3]));
    }
};

test "sim: stream bytes: std.Io's reader and writer on zio's vtable carry 10000 bytes and an echo, and std.Io's shutdown ends each side" {
    if (comptime skip) return error.SkipZigTest;
    try run(1, null, std_io_drained);
}

fn std_io_drained() !void {
    try std_io_bytes.body();
    try drain();
}

/// The surface table's checks for slice 2's rows (`sim_stream_test.zig`).
pub const chk2 = struct {
    fn scenario(comptime p: ?sim.StreamProfile, comptime f: fn () anyerror!void) fn () anyerror!void {
        return struct {
            fn g() anyerror!void {
                try run(1, p, f);
            }
        }.g;
    }
    pub const recv_ = scenario(null, round_trip.body);
    pub const send_ = scenario(null, window.body);
    pub const shutdown_ = scenario(null, fin_after_chunks.body);
    pub const std_io_ = scenario(null, std_io_drained);
    pub const rst_second_ = scenario(.linux, rst_second.body);
    pub fn bytes() anyerror!void {
        try run(1, null, chunk_times.body);
        try run(1, null, window.body);
        try run(1, null, owed_after_close.body);
    }
    pub fn recvLists() anyerror!void {
        try run(1, null, partial.body);
        try run(1, null, cancel_parked.body);
        try run(1, null, close_wakes.body);
        try sim.expectChildPanics("sim: stream refuses: a second receive while one is parked", "sim: a second receive while one is parked, on sim stream socket 100033");
    }
    pub fn sendLists() anyerror!void {
        try run(1, null, window.body);
        try sim.expectChildPanics("sim: stream refuses: a second send while one is parked", "sim: a second send while one is parked, on sim stream socket 100033");
    }
    pub fn profiles() anyerror!void {
        try prow.inSim(.linux);
        try prow.inSim(.macos);
        try sim.expectChildPanics("sim: stream refuses: with no profile, a send that meets a reset", "sim: rows 23, 25, 26b, 36 and 39, a send that meets a reset with no FIN before it: the kernels differ here");
    }
    pub fn shutdownRules() anyerror!void {
        try run(1, null, fin_after_chunks.body);
        try run(1, .macos, release.body);
        try sim.expectChildPanics("sim: stream refuses: with no profile, shutdown(.both) with unread bytes", "sim: row 17, shutdown(.both) with unread bytes: the kernels differ here");
    }
    pub const close_ = scenario(null, close_after_reset.body);
    pub fn dataLanding() anyerror!void {
        try run(1, .linux, rst_second.body);
        try run(1, null, equal_conns.body);
        try sim.expectChildPanics("sim: stream refuses: with no profile, data that lands after shutdown(.both)", "sim: row 18, a data chunk that lands after the endpoint's own shutdown(.both): the kernels differ here");
    }
    pub const credit_ = scenario(null, stale_credit.body);
    pub fn peerAndReadable() anyerror!void {
        try run(1, null, peer_name.body);
        try run(1, null, partial.body);
    }
    pub fn settings() anyerror!void {
        try run(1, null, chunk_off.body);
        try sim.expectChildPanics("sim: stream refuses: a window of 0 bytes", "sim: a stream window of 0 bytes");
        try sim.expectChildPanics("sim: stream refuses: chunk mode .fixed = 0", "sim: chunk mode .fixed = 0");
    }
};

const rearm = struct {
    fn body() !void {
        const srv, const c, const s = try pair();
        try s.shutdown(.both);
        try rt_mod.sleep(.fromMicroseconds(L / us));
        sim.setStreamChunking(c.socket.handle, .{ .fixed = 1 });
        const t0 = sim.clockNs();
        try expectEqual(@as(usize, 2), try c.write("ab", .none));
        // Chunk 0 lands at t0 + L: the local reset, read once. (A read
        // after .both does not park: it gives 0 at once, so wait first.)
        try rt_mod.sleep(.fromMicroseconds(L / us));
        try expectEqual(t0 + L, sim.clockNs());
        try expectEqual(@as(u64, 1), sim.streamStats().local_resets);
        var b: [4]u8 = undefined;
        try expectError(error.ConnectionResetByPeer, s.read(&b, .none));
        // Chunk 1 lands 1 µs later and must not re-arm it.
        try rt_mod.sleep(.fromMicroseconds(1));
        try expectEqual(@as(u64, 1), sim.streamStats().local_resets);
        try expectEqual(@as(usize, 0), try s.read(&b, .none));
        try expectEqual(@as(u64, 1), sim.streamStats().discarded[@backingInt(sim.Discard.shut_receiver)]);
        try expectEqual(@as(u64, 1), sim.streamStats().discarded[@backingInt(sim.Discard.after_reset)]);
        try t1.closeAll3(srv, c, s);
    }
};

test "sim: stream bytes: under .linux, a read between two chunks that land after shutdown(.both) reports the local reset once; the second chunk does not re-arm it (review 3, finding 5)" {
    if (comptime skip) return error.SkipZigTest;
    try run(1, .linux, rearm.body);
}

const fin_wakes = struct {
    var at: u64 = 0;
    var got: anyerror!usize = 0;
    fn reader(s: zio_net.Stream) void {
        var b: [4]u8 = undefined;
        got = s.read(&b, .none);
        at = sim.clockNs();
    }
    fn body() !void {
        const srv, const c, const s = try pair();
        var h = try rt_mod.spawn(reader, .{s});
        try rt_mod.sleep(.fromMicroseconds(10));
        const t0 = sim.clockNs();
        try c.shutdown(.send);
        h.join();
        try expectEqual(@as(usize, 0), try got);
        try expectEqual(t0 + L, at);
        try t1.closeAll3(srv, c, s);
    }
};

test "sim: stream bytes: a receive parked when a FIN lands alone is woken at the landing and gives 0" {
    if (comptime skip) return error.SkipZigTest;
    try run(1, null, fin_wakes.body);
}

const owed_last = struct {
    fn body() !void {
        const srv, const c, const s = try pair();
        sim.setStreamChunking(c.socket.handle, .off);
        // Two sends in one instant land in one instant, T.
        _ = try c.write("a", .none);
        _ = try c.write("b", .none);
        try rt_mod.sleep(.fromMicroseconds(L / us));
        const t = sim.clockNs();
        var b: [1]u8 = undefined;
        // Two reads at T: the first's credit leaves, the second's is owed
        // for T + L, when both closes' FINs and that credit also land.
        _ = try s.read(&b, .none);
        _ = try s.read(&b, .none);
        try expectEqual(t, sim.clockNs());
        try expect((try dir(true)).owed);
        const stale0 = sim.streamStats().stale_credit;
        s.close();
        c.close();
        try drain();
        // At T + L the wire empties, and the owed credit still departs:
        // its direction is terminal (c is dead), so nothing leaves and it
        // is counted, as is the credit that landed at the dead c.
        try expectEqual(stale0 + 2, sim.streamStats().stale_credit);
        srv.close();
    }
};

test "sim: stream bytes: a connection whose wire empties at the instant a credit is owed retires only after that departure" {
    if (comptime skip) return error.SkipZigTest;
    try run(1, null, owed_last.body);
}
