// SPDX-FileCopyrightText: 2025 Lukáš Lalinský
// SPDX-License-Identifier: MIT
//
// Tests of the sim's stream faults, slice 3 of design section 17.16: `.tcp`
// fault rules (delay, jitter, and holding by partition and silence),
// `failStream`, and the connect with no answer. The protocol is the comment
// above `pub const StreamProfile` in sim.zig, and `failStream`'s doc
// comment.
//
// How they run: as slice 1's and 2's (`sim_stream_test.zig`), one sim run
// per scenario. After `pair()` the clock is 3L (300 µs): the client is fd
// 100033 on port 49153, the accepted socket fd 100034 on port 49152,
// connection 1. Each scenario clears its rules, closes its sockets and
// drains, so `sim.end`'s teardown check runs on every one. Times are
// checked exactly: an entry sent at t with no fault lands at t + L.

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

const cport: u16 = 49153;
const sport: u16 = 49152;

fn ep(port: u16) sim.Endpoint {
    return .{ .ip = lo, .port = port, .protocol = .tcp };
}

/// The client-to-server direction of the pair, and its reverse.
const c2s: sim.Path = .{ .from = ep(cport), .to = ep(sport) };
const s2c: sim.Path = c2s.reversed();

fn dir(from_client: bool) !sim.TestStreamDir {
    return sim.testStreamDir(1, from_client) orelse error.ConnectionRetired;
}

fn checkConserved() !void {
    for ([_]bool{ true, false }) |fc| {
        const d = sim.testStreamDir(1, fc) orelse continue;
        if (!d.conserved()) {
            std.debug.print("conservation: admitted {d} != in flight {d} + held {d} + unread {d} + read {d} + discarded {d}\n", .{ d.admitted, d.in_flight, d.held, d.unread, d.read, d.discardedTotal() });
            return error.NotConserved;
        }
        try expect(d.credited <= d.read + d.reclaimed);
    }
}

fn unread(x: zio_net.Stream) usize {
    return sim.testStreamEnd(x.socket.handle).unread;
}

/// Sleeps until the virtual time `t` (which must not be past).
fn sleepUntil(t: u64) !void {
    const now = sim.clockNs();
    if (t < now) return error.TimeAlreadyPast;
    if (t > now) try rt_mod.sleep(.fromNanoseconds(t - now));
    try expectEqual(t, sim.clockNs());
}

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

fn linger0(x: zio_net.Stream) !void {
    const lg: [2]c_int = .{ 1, 0 };
    try os_net.setsockopt(x.socket.handle, os_net.SOL.SOCKET, os_net.SO.LINGER, std.mem.asBytes(&lg));
}

fn chunksOff(c: zio_net.Stream, s: zio_net.Stream) void {
    sim.setStreamChunking(c.socket.handle, .off);
    sim.setStreamChunking(s.socket.handle, .off);
}

// ---- Delay and jitter (design 17.9; 17.9a points 4 and 5) ----

const delay_dirs = struct {
    const D: u64 = 50_000;
    var which: enum { c2s, s2c, both } = .c2s;

    fn body() !void {
        const srv, const c, const s = try pair();
        chunksOff(c, s);
        const fwd = which != .s2c;
        const back = which != .c2s;
        if (fwd) sim.setImpairment(c2s, .{ .delay_ns = D });
        if (back) sim.setImpairment(s2c, .{ .delay_ns = D });
        const t0 = sim.clockNs();
        try expectEqual(@as(usize, 4), try c.write("abcd", .none));
        // The data lands at t0 + L, plus D when its direction is delayed.
        const t_data = t0 + L + (if (fwd) D else 0);
        try sleepUntil(t_data - 1);
        try expectEqual(@as(usize, 0), unread(s));
        try sleepUntil(t_data);
        try expectEqual(@as(usize, 4), unread(s));
        var b: [4]u8 = undefined;
        try expectEqual(@as(usize, 4), try s.read(&b, .none));
        // The credit leaves now and lands L later, plus D the other way.
        const t_credit = t_data + L + (if (back) D else 0);
        try sleepUntil(t_credit - 1);
        try expectEqual(@as(u64, 0), (try dir(true)).credited);
        try sleepUntil(t_credit);
        try expectEqual(@as(u64, 4), (try dir(true)).credited);
        try expectEqual(@as(u64, @intFromBool(fwd)) + @intFromBool(back), sim.streamStats().delayed);
        // A fixed delay draws nothing (17.9a point 4).
        try expectEqual(@as(u64, 0), sim.streamFaultDraws());
        if (fwd) try expectEqual(@as(u64, 1), sim.clearImpairment(c2s).delayed);
        if (back) try expectEqual(@as(u64, 1), sim.clearImpairment(s2c).delayed);
        try t1.closeAll3(c, s, srv);
    }
};

test "sim: stream faults: a .tcp delay moves landings in its direction only: client to server, server to client, and both" {
    if (comptime skip) return error.SkipZigTest;
    for ([_]@TypeOf(delay_dirs.which){ .c2s, .s2c, .both }) |w| {
        delay_dirs.which = w;
        try run(1, .linux, delay_dirs.body);
    }
}

/// The test's own copy of the jitter draw (the doc comment of
/// `tcpJitterDraw` in sim.zig).
fn jitterDraw(seed: u64, conn_id: u32, dir_: u8, seq: u64) u64 {
    var key: [16]u8 = @splat(0);
    std.mem.writeInt(u64, key[0..8], seed, .little);
    var m: [19]u8 = undefined;
    std.mem.writeInt(u32, m[0..4], conn_id, .little);
    m[4] = dir_;
    std.mem.writeInt(u64, m[5..13], seq, .little);
    @memcpy(m[13..19], "jitter");
    return std.hash.SipHash64(2, 4).toInt(&m, &key);
}

const jitter = struct {
    const J: u64 = 40_000;
    var seed: u64 = 1;
    var clamped: u64 = 0;

    fn body() !void {
        const srv, const c, const s = try pair();
        sim.setStreamChunking(c.socket.handle, .{ .fixed = 100 });
        sim.setImpairment(c2s, .{ .jitter_ns = J });
        const t0 = sim.clockNs();
        var data: [1000]u8 = undefined;
        for (&data, 0..) |*x, i| x.* = @truncate(i);
        try expectEqual(@as(usize, 1000), try c.write(&data, .none));
        // Ten chunks; the client's direction sent its SYN (0) and ACK (1),
        // so they are sequences 2 to 11. Chunk k lands at t0 + L + k µs +
        // its jitter, never before chunk k - 1.
        var dues: [16]u64 = undefined;
        try expectEqual(@as(usize, 10), sim.testTcpWireDues(&dues));
        var last: u64 = 0;
        for (0..10) |k| {
            const own = t0 + L + k * 1000 + jitterDraw(seed, 1, 0, 2 + k) % (J + 1);
            if (own < last) clamped += 1;
            const want = @max(own, last);
            try expectEqual(want, dues[k]);
            last = want;
        }
        try expectEqual(@as(u64, 10), sim.streamFaultDraws());
        var got: [1000]u8 = undefined;
        var n: usize = 0;
        while (n < 1000) n += try s.read(got[n..], .none);
        try expect(std.mem.eql(u8, &data, &got));
        _ = sim.clearImpairment(c2s);
        try t1.closeAll3(c, s, srv);
    }
};

test "sim: stream faults: a .tcp jitter is the test's own copy of the draw, per entry, and the order clamp holds; 16 seeds" {
    if (comptime skip) return error.SkipZigTest;
    jitter.clamped = 0;
    for (1..17) |sd| {
        jitter.seed = sd;
        try run(sd, .linux, jitter.body);
    }
    // The clamp must have acted, or this test did not test it.
    std.debug.print("jitter: {d} chunks clamped behind the one before, over 16 seeds\n", .{jitter.clamped});
    try expect(jitter.clamped > 0);
}

const delay_cleared_in_flight = struct {
    const D: u64 = 500_000;
    fn body() !void {
        const srv, const c, const s = try pair();
        chunksOff(c, s);
        sim.setImpairment(c2s, .{ .delay_ns = D });
        const t0 = sim.clockNs();
        _ = try c.write("ab", .none);
        // The rule ends while its entry is in flight: the entry keeps the
        // delay drawn at its send. A send after the clear has none, but it
        // lands behind the first.
        try sleepUntil(t0 + L);
        _ = sim.clearImpairment(c2s);
        _ = try c.write("cd", .none);
        var dues: [4]u64 = undefined;
        try expectEqual(@as(usize, 2), sim.testTcpWireDues(&dues));
        try expectEqual(t0 + L + D, dues[0]);
        try expectEqual(t0 + L + D, dues[1]);
        try sleepUntil(t0 + L + D);
        try expectEqual(@as(usize, 4), unread(s));
        var b: [4]u8 = undefined;
        try expectEqual(@as(usize, 4), try s.read(&b, .none));
        try expect(std.mem.eql(u8, "abcd", &b));
        try t1.closeAll3(c, s, srv);
    }
};

test "sim: stream faults: a delay cleared while its entry is in flight keeps that entry's landing, and the next entry lands behind it" {
    if (comptime skip) return error.SkipZigTest;
    try run(1, .linux, delay_cleared_in_flight.body);
}

// ---- Holding (design 17.9, "held, defined") ----

const held_at_send = struct {
    var which: enum { partition, silence } = .partition;

    fn body() !void {
        const srv, const c, const s = try pair();
        chunksOff(c, s);
        switch (which) {
            .partition => sim.setPartition(c2s),
            .silence => sim.silence(ep(cport)),
        }
        _ = try c.write("hello", .none);
        var d = try dir(true);
        try expectEqual(@as(u64, 5), d.held);
        try expectEqual(@as(u32, 1), d.held_entries);
        try expectEqual(@as(u64, 0), d.in_flight);
        try checkConserved();
        // Held entries never stop the clock: a long sleep passes, nothing
        // lands, and the entry stays counted in flight.
        try rt_mod.sleep(.fromMilliseconds(5));
        try expectEqual(@as(usize, 0), unread(s));
        try expectEqual(@as(usize, 1), sim.streamEntriesInFlight());
        const t1_ = sim.clockNs();
        const st = switch (which) {
            .partition => sim.healPartition(c2s),
            .silence => sim.unsilence(ep(cport)),
        };
        try expectEqual(@as(u64, 1), st.matched);
        d = try dir(true);
        try expectEqual(@as(u64, 0), d.held);
        try expectEqual(@as(u64, 5), d.in_flight);
        // Released now, it lands L later.
        try sleepUntil(t1_ + L - 1);
        try expectEqual(@as(usize, 0), unread(s));
        try sleepUntil(t1_ + L);
        try expectEqual(@as(usize, 5), unread(s));
        const ss = sim.streamStats();
        try expectEqual(@as(u64, 1), ss.held_at_send);
        try expectEqual(@as(u64, 1), ss.released);
        try checkConserved();
        try t1.closeAll3(c, s, srv);
    }
};

test "sim: stream faults: a .tcp partition, and a silence, hold data at its send, still charged, until the clear releases it to land L later" {
    if (comptime skip) return error.SkipZigTest;
    held_at_send.which = .partition;
    try run(1, .linux, held_at_send.body);
    held_at_send.which = .silence;
    try run(1, .linux, held_at_send.body);
}

const held_at_landing = struct {
    fn body() !void {
        const srv, const c, const s = try pair();
        // Two chunks, 1 µs apart: "ab" and "cd".
        sim.setStreamChunking(c.socket.handle, .{ .fixed = 2 });
        const t0 = sim.clockNs();
        _ = try c.write("abcd", .none);
        // The partition starts while the entries are in flight: "ab" is
        // held at its landing (t0 + L), not lost, and "cd", still on the
        // wire behind it, is held behind it then (sitting 33: before, "cd"
        // was held at its own landing, 1 µs later, and these counts were
        // 2 at landing and none behind).
        try sleepUntil(t0 + L / 2);
        sim.setPartition(c2s);
        try sleepUntil(t0 + L);
        try expectEqual(@as(u64, 4), (try dir(true)).held);
        try expectEqual(@as(u64, 1), sim.streamStats().held_at_landing);
        try expectEqual(@as(u64, 1), sim.streamStats().held_behind);
        try sleepUntil(t0 + L + 1000);
        try expectEqual(@as(usize, 0), unread(s));
        try expectEqual(@as(u64, 4), (try dir(true)).held);
        try checkConserved();
        // A later send in the direction is held behind them; it is newer,
        // with less extra time than "cd" (no spacing), so a release in any
        // order but the sequence would land it before "cd".
        _ = try c.write("ef", .none);
        try expectEqual(@as(u32, 3), (try dir(true)).held_entries);
        try sleepUntil(t0 + 2 * L);
        // It matched two: "ab" at its landing and "ef" at its send. "cd"
        // was held behind "ab" and never met the rule (it was 3 before
        // sitting 33, when "cd" met it at its own landing).
        try expectEqual(@as(u64, 2), sim.healPartition(c2s).matched);
        // "ab" at t0 + 3L, "cd" 1 µs later, "ef" behind "cd".
        try sleepUntil(t0 + 3 * L + 1000);
        try expectEqual(@as(usize, 6), unread(s));
        var b: [6]u8 = undefined;
        try expectEqual(@as(usize, 6), try s.read(&b, .none));
        try expect(std.mem.eql(u8, "abcdef", &b));
        try t1.closeAll3(c, s, srv);
    }
};

test "sim: stream faults: a partition that starts with an entry in flight holds it at its landing, later entries queue behind it, and the release lands them in order" {
    if (comptime skip) return error.SkipZigTest;
    try run(1, .linux, held_at_landing.body);
}

const partition_and_delay = struct {
    const D: u64 = 70_000;
    fn body() !void {
        const srv, const c, const s = try pair();
        chunksOff(c, s);
        sim.setPartition(c2s);
        sim.setImpairment(c2s, .{ .delay_ns = D });
        _ = try c.write("x", .none);
        try rt_mod.sleep(.fromMilliseconds(1));
        // The delay rule still matches; it does not stop the release. The
        // entry keeps the delay drawn at its send (17.9 items 4, 5).
        const t_rel = sim.clockNs();
        _ = sim.healPartition(c2s);
        try sleepUntil(t_rel + L + D - 1);
        try expectEqual(@as(usize, 0), unread(s));
        try sleepUntil(t_rel + L + D);
        try expectEqual(@as(usize, 1), unread(s));
        _ = sim.clearImpairment(c2s);
        try t1.closeAll3(c, s, srv);
    }
};

// An entry held at its landing, while a later entry of its direction is
// still on the wire (design 17.9 item 2: every later entry is held behind
// it). Two ways the later one could otherwise get out of order: a release
// before it lands, and an abort that discards the held one.
const later_on_wire = struct {
    var which: enum { release, abort } = .release;

    fn body() !void {
        const srv, const c, const s = try pair();
        chunksOff(c, s);
        const t0 = sim.clockNs();
        // A lands at t0 + L, B at t0 + 3L/2.
        try expectEqual(@as(usize, 1), try c.write("a", .none));
        try sleepUntil(t0 + L / 2);
        try expectEqual(@as(usize, 1), try c.write("b", .none));
        try sleepUntil(t0 + 3 * L / 4);
        sim.setPartition(c2s);
        try sleepUntil(t0 + L);
        try expectEqual(@as(usize, 0), unread(s));
        switch (which) {
            .release => {
                // The clear comes after A was held and before B's landing
                // time. A lands L after the release; B must not land first.
                try sleepUntil(t0 + 5 * L / 4);
                const t_rel = sim.clockNs();
                _ = sim.healPartition(c2s);
                try sleepUntil(t0 + 3 * L / 2);
                try expectEqual(@as(usize, 0), unread(s));
                try sleepUntil(t_rel + L);
                var b: [2]u8 = undefined;
                try expectEqual(@as(usize, 2), try s.read(&b, .none));
                try expect(std.mem.eql(u8, "ab", &b));
            },
            .abort => {
                // The sender aborts while A is held: A and B both go, as
                // `abort`. If B stayed on the wire it would be held at its
                // landing and then delivered without A: a hole in the
                // stream.
                try linger0(c);
                c.close();
                try sleepUntil(t0 + 2 * L);
                const t_rel = sim.clockNs();
                _ = sim.healPartition(c2s);
                try sleepUntil(t_rel + L);
                try expect(sim.testStreamEnd(s.socket.handle).reset_received);
                try expectEqual(@as(usize, 0), unread(s));
                try expectEqual(@as(u64, 2), (try dir(true)).discarded[@backingInt(sim.Discard.abort)]);
                try checkConserved();
                s.close();
                try t1.closeAll1(srv);
                return;
            },
        }
        try checkConserved();
        try t1.closeAll3(c, s, srv);
    }
};

test "sim: stream faults: an entry held at its landing holds the later entries of its direction still on the wire: a release before their landing keeps the order, and an abort discards them with it" {
    if (comptime skip) return error.SkipZigTest;
    for ([_]@TypeOf(later_on_wire.which){ .release, .abort }) |w| {
        later_on_wire.which = w;
        try run(1, .linux, later_on_wire.body);
        try run(1, .macos, later_on_wire.body);
    }
}

test "sim: stream faults: a partition and a delay over one entry: the clear of the partition releases it, and it lands L plus its delay after the release" {
    if (comptime skip) return error.SkipZigTest;
    try run(1, .linux, partition_and_delay.body);
}

const reverse_stall = struct {
    var done_at: u64 = 0;
    var sent: usize = 0;

    fn sender(c: zio_net.Stream) !void {
        sent = try evSend(c.socket.handle, "ef");
        done_at = sim.clockNs();
    }

    fn body() !void {
        done_at = 0;
        const srv, const c, const s = try pair();
        chunksOff(c, s);
        sim.setStreamWindow(s.socket.handle, 4);
        // Server to client: the credits are held, so the sender stalls.
        sim.setPartition(s2c);
        _ = try c.write("abcd", .none);
        try rt_mod.sleep(.fromNanoseconds(L));
        var b: [4]u8 = undefined;
        try expectEqual(@as(usize, 4), try s.read(&b, .none));
        try expectEqual(@as(u32, 1), (try dir(false)).held_entries);
        var h = try rt_mod.spawn(sender, .{c});
        defer h.cancel();
        try rt_mod.sleep(.fromMilliseconds(5));
        try expectEqual(@as(u64, 0), done_at);
        try expectEqual(@as(u64, 0), (try dir(true)).credited);
        try checkConserved();
        const t_rel = sim.clockNs();
        _ = sim.healPartition(s2c);
        try h.join();
        try expectEqual(t_rel + L, done_at);
        try expectEqual(@as(usize, 2), sent);
        try expectEqual(@as(u64, 4), (try dir(true)).credited);
        try t1.closeAll3(c, s, srv);
    }
};

test "sim: stream faults: a reverse partition holds the credits and stalls a sender on a full window, and the release wakes it L later" {
    if (comptime skip) return error.SkipZigTest;
    try run(1, .linux, reverse_stall.body);
    try run(1, .macos, reverse_stall.body);
}

const coalesce = struct {
    fn body() !void {
        const srv, const c, const s = try pair();
        chunksOff(c, s);
        sim.setPartition(s2c);
        _ = try c.write("abcdefgh", .none);
        try rt_mod.sleep(.fromNanoseconds(L));
        var b: [4]u8 = undefined;
        // Two reads: the first credit leaves now (held), the second is owed
        // for L later and then held too, where it replaces the first.
        try expectEqual(@as(usize, 4), try s.read(&b, .none));
        try rt_mod.sleep(.fromMicroseconds(1));
        try expectEqual(@as(usize, 4), try s.read(&b, .none));
        try rt_mod.sleep(.fromNanoseconds(L));
        const ss = sim.streamStats();
        try expectEqual(@as(u64, 1), ss.credits_coalesced);
        try expectEqual(@as(u32, 1), (try dir(false)).held_entries);
        const t_rel = sim.clockNs();
        _ = sim.healPartition(s2c);
        try sleepUntil(t_rel + L);
        try expectEqual(@as(u64, 8), (try dir(true)).credited);
        try expectEqual(@as(u64, 1), sim.streamStats().credits_landed);
        try t1.closeAll3(c, s, srv);
    }
};

test "sim: stream faults: two credits held in one direction coalesce to the newer, which lands once with the newer count" {
    if (comptime skip) return error.SkipZigTest;
    try run(1, .linux, coalesce.body);
}

// The outside review of slice 3 (gpt-simnet15.md, section 1, first P2): an
// older credit that was on the wire reaches the held list after a newer one
// was held at its send. Arriving is not being newer.
const older_credit = struct {
    fn body() !void {
        const srv, const c, const s = try pair();
        chunksOff(c, s);
        sim.setStreamWindow(s.socket.handle, 2);
        try expectEqual(@as(usize, 2), try c.write("ab", .none));
        try rt_mod.sleep(.fromNanoseconds(L));
        try expectEqual(@as(usize, 2), unread(s));
        // C1 (count 1) leaves at t1 with a delay of 2L: it lands at t1 + 3L.
        sim.setImpairment(s2c, .{ .delay_ns = 2 * L });
        const t1_ = sim.clockNs();
        var b: [1]u8 = undefined;
        try expectEqual(@as(usize, 1), try s.read(&b, .none));
        // C2 (count 2) is owed for t1 + L, and the partition holds it at
        // its send.
        sim.setPartition(s2c);
        try expectEqual(@as(usize, 1), try s.read(&b, .none));
        try sleepUntil(t1_ + L);
        try expectEqual(@as(u32, 1), (try dir(false)).held_entries);
        // C1 reaches its landing and is held there, after C2: the two
        // coalesce to C2, the newer by sequence, whose count is 2.
        try sleepUntil(t1_ + 3 * L);
        try expectEqual(@as(u32, 1), (try dir(false)).held_entries);
        try expectEqual(@as(u64, 1), sim.streamStats().credits_coalesced);
        try expectEqual(@as(u64, 0), (try dir(true)).credited);
        // With a window of 1, a credit that is 1 short leaves the sender
        // no room for ever: nothing is unread, owed or held to fix it.
        sim.setStreamWindow(s.socket.handle, 1);
        _ = sim.clearImpairment(s2c);
        const t_rel = sim.clockNs();
        _ = sim.healPartition(s2c);
        try sleepUntil(t_rel + 3 * L);
        try expectEqual(@as(u64, 2), (try dir(true)).credited);
        try expectEqual(@as(u64, 1), sim.streamStats().credits_landed);
        try expectEqual(@as(usize, 1), try c.write("c", .none));
        try rt_mod.sleep(.fromNanoseconds(L));
        try expectEqual(@as(usize, 1), try s.read(&b, .none));
        try expectEqual(@as(u8, 'c'), b[0]);
        try t1.closeAll3(c, s, srv);
    }
};

test "sim: stream faults: an older credit that reaches the held list after a newer one was held at its send coalesces to the newer, and the sender is not left short" {
    if (comptime skip) return error.SkipZigTest;
    try run(1, .linux, older_credit.body);
    try run(1, .macos, older_credit.body);
}

const fin_held = struct {
    var which: enum { shutdown, close } = .shutdown;

    fn body() !void {
        const srv, const c, const s = try pair();
        chunksOff(c, s);
        sim.setPartition(c2s);
        _ = try c.write("ab", .none);
        switch (which) {
            .shutdown => try c.shutdown(.send),
            .close => c.close(),
        }
        try expectEqual(@as(u32, 2), (try dir(true)).held_entries);
        try rt_mod.sleep(.fromMilliseconds(1));
        try expect(!sim.testStreamEnd(s.socket.handle).fin_in);
        const t_rel = sim.clockNs();
        _ = sim.healPartition(c2s);
        try sleepUntil(t_rel + L);
        // The data, then the FIN, in order.
        var b: [4]u8 = undefined;
        try expectEqual(@as(usize, 2), try s.read(&b, .none));
        try expectEqual(@as(usize, 0), try s.read(&b, .none));
        if (which == .shutdown) c.close();
        try t1.closeAll2(s, srv);
    }
};

test "sim: stream faults: a FIN held behind held data, by shutdown(.send) and by close, lands after it at the release" {
    if (comptime skip) return error.SkipZigTest;
    fin_held.which = .shutdown;
    try run(1, .linux, fin_held.body);
    fin_held.which = .close;
    try run(1, .linux, fin_held.body);
}

const abort_held = struct {
    var which: enum { close_unread, linger0, rst_lands } = .close_unread;

    fn body() !void {
        const srv, const c, const s = try pair();
        chunksOff(c, s);
        sim.setPartition(c2s);
        _ = try c.write("xyz", .none);
        try c.shutdown(.send);
        try expectEqual(@as(u32, 2), (try dir(true)).held_entries);
        switch (which) {
            .close_unread => {
                // Unread bytes at the client: its close is an abort.
                _ = try s.write("q", .none);
                try rt_mod.sleep(.fromNanoseconds(L));
                c.close();
            },
            .linger0 => {
                try linger0(c);
                c.close();
            },
            .rst_lands => {
                // The server aborts: its RST lands at the client, whose
                // held data and FIN are discarded then.
                try linger0(s);
                s.close();
                try rt_mod.sleep(.fromNanoseconds(L));
                try expect(sim.testStreamEnd(c.socket.handle).reset_received);
            },
        }
        const d = try dir(true);
        try expectEqual(@as(u64, 3), d.discarded[@backingInt(sim.Discard.abort)]);
        try expectEqual(@as(u64, 0), d.held);
        try checkConserved();
        const ss = sim.streamStats();
        try expectEqual(@as(u64, 2), ss.discarded_held);
        switch (which) {
            // The aborter's RST follows; the partition holds it.
            .close_unread, .linger0 => {
                try expectEqual(@as(u32, 1), d.held_entries);
                const t_rel = sim.clockNs();
                _ = sim.healPartition(c2s);
                try sleepUntil(t_rel + L);
                try expect(sim.testStreamEnd(s.socket.handle).reset_received);
                s.close();
            },
            .rst_lands => {
                try expectEqual(@as(u32, 0), d.held_entries);
                _ = sim.healPartition(c2s);
                c.close();
            },
        }
        try t1.closeAll1(srv);
    }
};

test "sim: stream faults: an abort discards its held data and FIN as abort, recorded: a close with unread bytes, SO_LINGER {1, 0}, and an RST that lands; its own RST is held by the partition" {
    if (comptime skip) return error.SkipZigTest;
    for ([_]@TypeOf(abort_held.which){ .close_unread, .linger0, .rst_lands }) |w| {
        abort_held.which = w;
        try run(1, .linux, abort_held.body);
    }
}

const dead_inbound = struct {
    fn body() !void {
        const srv, const c, const s = try pair();
        chunksOff(c, s);
        sim.setPartition(c2s);
        _ = try c.write("ab", .none);
        s.close();
        try rt_mod.sleep(.fromMilliseconds(1));
        // Held entries inbound to a dead endpoint stay held; the release
        // lands them by 17.8's rules: data at a closed endpoint is dropped,
        // and an RST goes back.
        const t_rel = sim.clockNs();
        _ = sim.healPartition(c2s);
        try sleepUntil(t_rel + L);
        try expectEqual(@as(u64, 2), (try dir(true)).discarded[@backingInt(sim.Discard.closed_receiver)]);
        try sleepUntil(t_rel + 2 * L);
        try expect(sim.testStreamEnd(c.socket.handle).reset_received);
        try t1.closeAll2(c, srv);
    }
};

test "sim: stream faults: held data inbound to an endpoint that closed lands at the release by the landing rules: dropped, and an RST goes back" {
    if (comptime skip) return error.SkipZigTest;
    try run(1, .linux, dead_inbound.body);
}

// The outside review of slice 3, second P2: the entry-limit check ran
// before the discards and the coalescing that free entries, so a step that
// would not pass the limit panicked. Both sequences are the review's.
const max_e = sim.max_tcp_entries;
var limit_bytes: [max_e]u8 = @splat('x');

/// Every entry of the run held on one connection, then an aborting close:
/// it discards all of them and holds one RST.
const abort_at_limit = struct {
    fn body() !void {
        const srv, const c, const s = try pair();
        sim.setStreamChunking(c.socket.handle, .{ .fixed = 1 });
        sim.setPartition(c2s);
        try expectEqual(@as(usize, max_e), try c.write(&limit_bytes, .none));
        try expectEqual(@as(usize, max_e), sim.streamEntriesInFlight());
        try expectEqual(@as(u32, max_e), (try dir(true)).held_entries);
        try linger0(c);
        c.close();
        try expectEqual(@as(usize, 1), sim.streamEntriesInFlight());
        try expectEqual(@as(u64, max_e), (try dir(true)).discarded[@backingInt(sim.Discard.abort)]);
        try expectEqual(@as(u32, 1), (try dir(true)).held_entries);
        const t_rel = sim.clockNs();
        _ = sim.healPartition(c2s);
        try sleepUntil(t_rel + L);
        try expect(sim.testStreamEnd(s.socket.handle).reset_received);
        try t1.closeAll2(s, srv);
    }
};

test "sim: stream faults: an aborting close with max_tcp_entries held discards them and holds its RST, under the limit" {
    if (comptime skip) return error.SkipZigTest;
    try run(1, .linux, abort_at_limit.body);
}

/// One held credit and `max_tcp_entries - 1` held data entries, then a
/// read whose credit is held and coalesces with the first: the count stays
/// at the limit.
const credit_at_limit = struct {
    fn body() !void {
        const srv, const c, const s = try pair();
        sim.setStreamChunking(c.socket.handle, .{ .fixed = 1 });
        try expectEqual(@as(usize, 2), try c.write("ab", .none));
        try rt_mod.sleep(.fromNanoseconds(L + 1000));
        try expectEqual(@as(usize, 2), unread(s));
        sim.setPartition(s2c);
        var b: [1]u8 = undefined;
        try expectEqual(@as(usize, 1), try s.read(&b, .none));
        sim.setPartition(c2s);
        try expectEqual(@as(usize, max_e - 1), try c.write(limit_bytes[0 .. max_e - 1], .none));
        try expectEqual(@as(usize, max_e), sim.streamEntriesInFlight());
        // L after the first credit left, the second leaves at the read and
        // is held: it replaces the first.
        try rt_mod.sleep(.fromNanoseconds(L));
        try expectEqual(@as(usize, 1), try s.read(&b, .none));
        try expectEqual(@as(usize, max_e), sim.streamEntriesInFlight());
        try expectEqual(@as(u64, 1), sim.streamStats().credits_coalesced);
        try expectEqual(@as(u32, 1), (try dir(false)).held_entries);
        const t_rel = sim.clockNs();
        _ = sim.healPartition(s2c);
        try sleepUntil(t_rel + L);
        try expectEqual(@as(u64, 2), (try dir(true)).credited);
        // The held data goes with an abort, so nothing has to land.
        try linger0(c);
        c.close();
        _ = sim.healPartition(c2s);
        try t1.closeAll2(s, srv);
    }
};

test "sim: stream faults: a credit that coalesces with a held one when max_tcp_entries are in flight leaves the count at the limit" {
    if (comptime skip) return error.SkipZigTest;
    try run(1, .linux, credit_at_limit.body);
}

/// The same shape at `shutdown(.both)` under `.macos`: its FIN is held,
/// and its window release's credit is held and replaces the held credit.
/// One short of the limit before, at the limit after.
const both_at_limit = struct {
    fn body() !void {
        const srv, const c, const s = try pair();
        chunksOff(c, s);
        try expectEqual(@as(usize, 3), try c.write("abc", .none));
        try rt_mod.sleep(.fromNanoseconds(L));
        sim.setPartition(s2c);
        var b: [1]u8 = undefined;
        try expectEqual(@as(usize, 1), try s.read(&b, .none));
        sim.setPartition(c2s);
        sim.setStreamChunking(c.socket.handle, .{ .fixed = 1 });
        try expectEqual(@as(usize, max_e - 2), try c.write(limit_bytes[0 .. max_e - 2], .none));
        try expectEqual(@as(usize, max_e - 1), sim.streamEntriesInFlight());
        try rt_mod.sleep(.fromNanoseconds(L));
        try s.shutdown(.both);
        try expectEqual(@as(usize, max_e), sim.streamEntriesInFlight());
        try expectEqual(@as(u64, 1), sim.streamStats().credits_coalesced);
        try expectEqual(@as(u32, 2), (try dir(false)).held_entries);
        try expectEqual(@as(u64, 2), (try dir(true)).reclaimed);
        const t_rel = sim.clockNs();
        _ = sim.healPartition(s2c);
        try sleepUntil(t_rel + L);
        try expectEqual(@as(u64, 3), (try dir(true)).credited);
        try expect(sim.testStreamEnd(c.socket.handle).fin_in);
        try linger0(c);
        c.close();
        _ = sim.healPartition(c2s);
        try t1.closeAll2(s, srv);
    }
};

test "sim: stream faults: under .macos, shutdown(.both) one short of max_tcp_entries holds its FIN and a release credit that replaces a held one, at the limit" {
    if (comptime skip) return error.SkipZigTest;
    try run(1, .macos, both_at_limit.body);
}

const drain_needs_clear = struct {
    fn body() !void {
        const srv, const c, const s = try pair();
        sim.setPartition(c2s);
        c.close();
        s.close();
        srv.close();
        // The client's FIN is held: the connection cannot retire until the
        // rule is cleared (17.9 item 8, 17.5).
        try rt_mod.sleep(.fromMilliseconds(5));
        try expectEqual(@as(usize, 1), sim.testConnsLive());
        try expect(sim.streamEntriesInFlight() > 0);
        _ = sim.healPartition(c2s);
        try drain();
        try expectEqual(@as(usize, 0), sim.testConnsLive());
    }
};

test "sim: stream faults: a connection with a held entry does not retire, and retires once the release lands it" {
    if (comptime skip) return error.SkipZigTest;
    try run(1, .linux, drain_needs_clear.body);
}

const udp_beside = struct {
    fn body() !void {
        const srv, const c, const s = try pair();
        chunksOff(c, s);
        // Two datagram sockets on the same two ports as the connection.
        const ua = try sim.dgramOpen(true);
        const ub2 = try sim.dgramOpen(true);
        try sim.dgramBind(ua, .{ .ip = lo, .port = cport });
        try sim.dgramBind(ub2, .{ .ip = lo, .port = sport });
        // A .tcp partition holds the stream and leaves the datagrams.
        sim.setPartition(c2s);
        _ = try c.write("t", .none);
        _ = try sim.dgramSend(ua, &[_][]const u8{"u"}, .{ .ip = lo, .port = sport }, null);
        try rt_mod.sleep(.fromNanoseconds(L));
        try expectEqual(@as(usize, 0), unread(s));
        var ub: [4]u8 = undefined;
        try expectEqual(@as(usize, 1), (try sim.dgramRecv(ub2, &[_][]u8{&ub}, true)).len);
        _ = sim.healPartition(c2s);
        try rt_mod.sleep(.fromNanoseconds(L));
        try expectEqual(@as(usize, 1), unread(s));
        // A .udp partition on the same ports drops the datagram and leaves
        // the stream.
        const u_path: sim.Path = .{ .from = .{ .ip = lo, .port = cport }, .to = .{ .ip = lo, .port = sport } };
        sim.setPartition(u_path);
        _ = try c.write("t", .none);
        _ = try sim.dgramSend(ua, &[_][]const u8{"u"}, .{ .ip = lo, .port = sport }, null);
        try rt_mod.sleep(.fromNanoseconds(L));
        try expectEqual(@as(usize, 2), unread(s));
        try expectError(error.WouldBlock, sim.dgramRecv(ub2, &[_][]u8{&ub}, true));
        try expectEqual(@as(u64, 1), sim.netFaultStats().partitioned_at_send);
        _ = sim.healPartition(u_path);
        sim.dgramClose(ua);
        sim.dgramClose(ub2);
        try t1.closeAll3(c, s, srv);
    }
};

test "sim: stream faults: a UDP flow and a TCP connection on one address pair: a .tcp rule holds only the stream, a .udp rule drops only the datagram" {
    if (comptime skip) return error.SkipZigTest;
    try run(1, .linux, udp_beside.body);
}

const delayed_rst_reuse = struct {
    fn body() !void {
        const srv, const c, const s = try pair();
        // The server's RST is delayed 1 ms; before it lands, both sockets
        // close and a new connection takes the client's fd number.
        sim.setImpairment(s2c, .{ .delay_ns = 1_000_000 });
        try linger0(s);
        s.close();
        c.close();
        const c2 = try t1.connect(lo, t1.portOf(srv), .none);
        try expectEqual(@as(i32, 100033), c2.socket.handle);
        const s2 = try srv.accept(.{});
        _ = sim.clearImpairment(s2c);
        const dead0 = sim.streamStats().dropped_dead;
        try rt_mod.sleep(.fromMilliseconds(2));
        // It landed at the old connection's dead client, by conn_id: the
        // new connection on the same fd number is untouched.
        try expectEqual(dead0 + 1, sim.streamStats().dropped_dead);
        try expect(!sim.testStreamEnd(c2.socket.handle).reset_received);
        try expectEqual(@as(u32, 2), sim.testStreamEnd(c2.socket.handle).conn_id);
        _ = try c2.write("ok", .none);
        var b: [2]u8 = undefined;
        try expectEqual(@as(usize, 2), try s2.read(&b, .none));
        try t1.closeAll3(c2, s2, srv);
    }
};

test "sim: stream faults: a delayed RST that lands after its client's fd number was reused reaches the old connection only" {
    if (comptime skip) return error.SkipZigTest;
    try run(1, .linux, delayed_rst_reuse.body);
}

// ---- failStream (design 17.9's table) ----

const fail_parked = struct {
    var how: sim.StreamFailure = .reset;
    var which: enum { recv, send, none_recv_first, none_send_first } = .recv;
    var res: anyerror!usize = 0;
    var at: u64 = 0;

    fn recvTask(fd: i32) void {
        var b: [8]u8 = undefined;
        res = evRecv(fd, &b);
        at = sim.clockNs();
    }

    fn sendTask(fd: i32) void {
        res = evSend(fd, "b");
        at = sim.clockNs();
    }

    fn opTask(fd: i32) void {
        if (which == .recv) recvTask(fd) else sendTask(fd);
    }

    fn want() anyerror {
        return switch (how) {
            .reset => error.ConnectionResetByPeer,
            .timed_out => error.ConnectionTimedOut,
        };
    }

    fn body() !void {
        res = 0;
        at = 0;
        const srv, const c, const s = try pair();
        const fd = c.socket.handle;
        var b: [8]u8 = undefined;
        switch (which) {
            .recv, .send => {
                if (which == .send) {
                    sim.setStreamWindow(s.socket.handle, 1);
                    _ = try c.write("a", .none);
                }
                var h = try rt_mod.spawn(opTask, .{fd});
                defer h.cancel();
                try rt_mod.sleep(.fromMicroseconds(10));
                const t = sim.clockNs();
                sim.failStream(fd, how);
                h.join();
                // Woken at the call, harvested before the clock moves.
                try expectEqual(t, at);
                try expectError(want(), res);
                // Reported: receives give 0, sends BrokenPipe.
                try expectEqual(@as(usize, 0), try evRecv(fd, &b));
                try expectError(error.BrokenPipe, evSend(fd, "c"));
            },
            .none_recv_first => {
                sim.failStream(fd, how);
                try expectError(want(), evRecv(fd, &b));
                try expectEqual(@as(usize, 0), try evRecv(fd, &b));
                try expectError(error.BrokenPipe, evSend(fd, "c"));
            },
            .none_send_first => {
                sim.failStream(fd, how);
                try expectError(want(), evSend(fd, "c"));
                try expectError(error.BrokenPipe, evSend(fd, "c"));
                try expectEqual(@as(usize, 0), try evRecv(fd, &b));
            },
        }
        const e = sim.testStreamEnd(fd);
        try expectEqual(how, e.failed.?);
        try expect(e.fail_reported);
        try t1.closeAll3(c, s, srv);
    }
};

test "sim: stream faults: failStream ends a parked receive, a parked send, or the next call, once, with the failure's error; then 0 and BrokenPipe; both kinds, both profiles" {
    if (comptime skip) return error.SkipZigTest;
    for ([_]sim.StreamProfile{ .linux, .macos }) |p| {
        for ([_]sim.StreamFailure{ .reset, .timed_out }) |how| {
            for ([_]@TypeOf(fail_parked.which){ .recv, .send, .none_recv_first, .none_send_first }) |w| {
                fail_parked.how = how;
                fail_parked.which = w;
                try run(1, p, fail_parked.body);
            }
        }
    }
}

const fail_both = struct {
    var seen: [2]u32 = .{ 0, 0 };
    var r: anyerror!usize = 0;
    var w: anyerror!usize = 0;

    fn recvTask(fd: i32) void {
        var b: [8]u8 = undefined;
        r = evRecv(fd, &b);
    }

    fn sendTask(fd: i32) void {
        w = evSend(fd, "b");
    }

    fn body() !void {
        const srv, const c, const s = try pair();
        const fd = c.socket.handle;
        sim.setStreamWindow(s.socket.handle, 1);
        _ = try c.write("a", .none);
        var hr = try rt_mod.spawn(recvTask, .{fd});
        defer hr.cancel();
        var hw = try rt_mod.spawn(sendTask, .{fd});
        defer hw.cancel();
        try rt_mod.sleep(.fromMicroseconds(10));
        sim.failStream(fd, .timed_out);
        hr.join();
        hw.join();
        // One of them reports the failure; the other gets its line 2.
        if (r) |n| {
            try expectEqual(@as(usize, 0), n);
            try expectError(error.ConnectionTimedOut, w);
            seen[0] += 1;
        } else |err| {
            try expectEqual(error.ConnectionTimedOut, err);
            try expectError(error.BrokenPipe, w);
            seen[1] += 1;
        }
        try t1.closeAll3(c, s, srv);
    }
};

test "sim: stream faults: failStream with a receive and a send parked: the first harvested gets the error and the other 0 or BrokenPipe, in both orders over 16 seeds, both profiles" {
    if (comptime skip) return error.SkipZigTest;
    for ([_]sim.StreamProfile{ .linux, .macos }) |p| {
        fail_both.seen = .{ 0, 0 };
        for (1..17) |sd| try run(sd, p, fail_both.body);
        std.debug.print("failStream with both parked, {t}: send first {d}, receive first {d} of 16 seeds\n", .{ p, fail_both.seen[0], fail_both.seen[1] });
        if (fail_both.seen[0] == 0 or fail_both.seen[1] == 0) return error.OneHarvestOrderNeverHappened;
    }
}

const fail_effects = struct {
    fn body() !void {
        const srv, const c, const s = try pair();
        chunksOff(c, s);
        const fd = c.socket.handle;
        // The client has: 2 unread bytes, a credit owed (two reads in one
        // L), and held outbound data behind a partition.
        _ = try s.write("hijk", .none);
        try rt_mod.sleep(.fromNanoseconds(L));
        var b: [1]u8 = undefined;
        _ = try c.read(&b, .none);
        _ = try c.read(&b, .none);
        try expect((try dir(false)).owed);
        sim.setPartition(c2s);
        _ = try c.write("xyz", .none);
        const stale0 = sim.streamStats().stale_credit;
        sim.failStream(fd, .reset);
        try expectEqual(@as(u64, 3), (try dir(true)).discarded[@backingInt(sim.Discard.failed_sender)]);
        try expectEqual(@as(u64, 2), (try dir(false)).discarded[@backingInt(sim.Discard.failed_receiver)]);
        try expect(!(try dir(false)).owed);
        try expectEqual(stale0 + 1, sim.streamStats().stale_credit);
        try checkConserved();
        // Data that lands later is dropped, and nothing answers.
        _ = try s.write("mn", .none);
        try rt_mod.sleep(.fromNanoseconds(L));
        try expectEqual(@as(u64, 4), (try dir(false)).discarded[@backingInt(sim.Discard.failed_receiver)]);
        try expect(!sim.testStreamEnd(fd).rst_sent);
        try expectEqual(@as(u64, 1), sim.streamStats().dropped_failed);
        // shutdown and getpeername see a dead transport; close sends nothing.
        try expectError(error.SocketUnconnected, c.shutdown(.send));
        var sa: os_net.sockaddr.in = undefined;
        var len: os_net.socklen_t = @sizeOf(os_net.sockaddr.in);
        try expectError(error.SocketUnconnected, os_net.getpeername(fd, @ptrCast(&sa), &len));
        _ = sim.healPartition(c2s);
        const before = sim.streamEntriesInFlight();
        c.close();
        try expectEqual(before, sim.streamEntriesInFlight());
        try t1.closeAll2(s, srv);
    }
};

test "sim: stream faults: failStream discards held outbound as failed_sender, unread as failed_receiver, and the owed credit; later data is dropped with no answer; shutdown and getpeername are SocketUnconnected; close is quiet" {
    if (comptime skip) return error.SkipZigTest;
    try run(1, .linux, fail_effects.body);
}

const fail_peer = struct {
    var res: anyerror!usize = 0;

    fn body() !void {
        const srv, const c, const s = try pair();
        chunksOff(c, s);
        // The client's data already on the wire still lands at the peer.
        _ = try c.write("ab", .none);
        sim.failStream(c.socket.handle, .timed_out);
        try rt_mod.sleep(.fromNanoseconds(L));
        try expectEqual(@as(usize, 2), unread(s));
        try expect(!sim.testStreamEnd(s.socket.handle).reset_received);
        // The peer's sends are admitted while it has room, then park: a
        // failed endpoint sends no credit.
        sim.setStreamWindow(c.socket.handle, 8);
        try expectEqual(@as(usize, 8), try s.write("12345678", .none));
        try expectError(error.Timeout, s.write("9", .fromMilliseconds(5)));
        try expectEqual(@as(u64, 0), (try dir(false)).credited);
        try t1.closeAll3(c, s, srv);
    }
};

test "sim: stream faults: after failStream the peer is not told: data already on the wire lands, and its sends fill the window and park" {
    if (comptime skip) return error.SkipZigTest;
    try run(1, .linux, fail_peer.body);
    try run(1, .macos, fail_peer.body);
}

const fail_c_syn = struct {
    var how: sim.StreamFailure = .reset;
    var keep_open: bool = false;
    var res: anyerror!void = {};
    var at: u64 = 0;
    /// The client: the first socket after the listener.
    const fd: i32 = 100033;
    var server: zio_net.Server = undefined;
    var op: ev.NetConnect = undefined;
    var to: os_net.sockaddr.in = undefined;

    /// zio's connect, whose wrapper closes the socket when it fails.
    fn connector() void {
        if (t1.connect(lo, t1.portOf(server), .none)) |c| {
            c.close();
            res = {};
        } else |e| res = e;
        at = sim.clockNs();
    }

    fn onDone(_: *ev.Loop, _: *ev.Completion) void {
        res = op.getResult();
        at = sim.clockNs();
    }

    fn body() !void {
        res = {};
        at = 0;
        server = try t1.listen(lo, 0, .{});
        var h: ?rt_mod.JoinHandle(void) = null;
        defer if (h) |*x| x.cancel();
        if (keep_open) {
            // An ev connect: nothing closes the socket when it fails.
            to = t1.sockaddr(lo, t1.portOf(server));
            try expectEqual(fd, try t1.evOpen());
            op = ev.NetConnect.init(fd, @ptrCast(&to), @sizeOf(os_net.sockaddr.in));
            op.c.callback = onDone;
            op.c.flags.defer_callback = false;
            rt_mod.getCurrentExecutor().loop.add(&op.c);
        } else h = try rt_mod.spawn(connector, .{});
        try rt_mod.sleep(.fromNanoseconds(L / 2));
        try expectEqual(sim.StreamPhase.c_syn, sim.testStreamEnd(fd).phase);
        const t = sim.clockNs();
        sim.failStream(fd, how);
        if (h) |*x| {
            x.join();
            h = null;
        } else try rt_mod.sleep(.fromNanoseconds(1));
        // The connect fails at the call.
        try expectEqual(t, at);
        try expectError(switch (how) {
            .reset => error.ConnectionResetByPeer,
            .timed_out => error.Timeout,
        }, res);
        try sleepUntil(5 * L);
        if (keep_open) {
            // The SYN landed (L): a provisional endpoint. Its SYN-ACK
            // landed at the failed client (2L) and got no answer, so the
            // provisional endpoint stays until the listener closes.
            try expectEqual(sim.StreamPhase.c_failed, sim.testStreamEnd(fd).phase);
            try expectEqual(@as(usize, 1), sim.testListenerCount(server.socket.handle));
            try expectEqual(@as(u64, 1), sim.streamStats().dropped_failed);
            try expect(!sim.testStreamEnd(fd).rst_sent);
            t1.evClose(fd);
        } else {
            // The wrapper closed it at the failure, before the SYN landed:
            // the SYN is dropped at its landing and makes no server state
            // (17.6, SYN line 1).
            try expectEqual(@as(usize, 0), sim.testListenerCount(server.socket.handle));
            try expectEqual(@as(u64, 1), sim.streamStats().syn_dropped_dead);
        }
        server.close();
        try drain();
        try expectEqual(@as(usize, 0), sim.testConnsLive());
    }
};

test "sim: stream faults: failStream in C-syn fails the parked connect at the call, ConnectionResetByPeer or Timeout; a SYN-ACK that reaches the failed client gets no answer" {
    if (comptime skip) return error.SkipZigTest;
    for ([_]bool{ false, true }) |keep| {
        for ([_]sim.StreamFailure{ .reset, .timed_out }) |how| {
            fail_c_syn.how = how;
            fail_c_syn.keep_open = keep;
            try run(1, .linux, fail_c_syn.body);
        }
    }
}

const refuse3 = struct {
    fn listener() !void {
        const l = try t1.listen(lo, 0, .{});
        sim.failStream(l.socket.handle, .reset);
    }
    fn openSock() !void {
        const fd = try t1.evOpen();
        sim.failStream(fd, .reset);
    }
    /// A release whose landing would pass maxInt(u64) ns: the held entry's
    /// delay is fixed at its send, and the clock has moved to L + 500 ns
    /// before the end of time, so `release + L` fits and `+ 1000` does not.
    fn releaseAtLimit() !void {
        _, const c, _ = try pair();
        sim.setPartition(c2s);
        sim.setImpairment(c2s, .{ .delay_ns = 1000 });
        _ = try c.write("x", .none);
        sim.advanceNs(std.math.maxInt(u64) - sim.clockNs() - L - 500);
        _ = sim.healPartition(c2s);
    }
    fn twice() !void {
        _, const c, _ = try pair();
        sim.failStream(c.socket.handle, .reset);
        sim.failStream(c.socket.handle, .reset);
    }
    fn afterReset() !void {
        _, const c, const s = try pair();
        try linger0(s);
        s.close();
        try rt_mod.sleep(.fromNanoseconds(L));
        sim.failStream(c.socket.handle, .reset);
    }
    fn tcpLoss() !void {
        sim.setImpairment(c2s, .{ .loss_ppm = 1 });
    }
    fn tcpBurst() !void {
        sim.setImpairment(c2s, .{ .burst = .{ .enter_ppm = 1, .leave_ppm = 1 } });
    }
    fn tcpDup() !void {
        sim.setImpairment(c2s, .{ .dup_ppm = 1 });
    }
    fn tcpReorder() !void {
        sim.setImpairment(c2s, .{ .reorder = true, .delay_ns = 1 });
    }
    fn mixedPath() !void {
        sim.setPartition(.{ .from = ep(cport), .to = .{ .ip = lo, .port = sport } });
    }
};

test "sim: stream faults refuse: failStream on a listener" {
    try refusal("sim: stream faults refuse: failStream on a listener", "sim: failStream on sim fd 100032, a listener: only a connection's endpoint can fail", .linux, refuse3.listener);
}
test "sim: stream faults refuse: failStream on an open socket" {
    try refusal("sim: stream faults refuse: failStream on an open socket", "sim: failStream on sim fd 100032, a stream socket that is not connected: only a connection's endpoint can fail", .linux, refuse3.openSock);
}
test "sim: stream faults refuse: a release whose landing would pass maxInt(u64) ns" {
    try refusal("sim: stream faults refuse: a release whose landing would pass", "sim: a TCP entry sent at 18446744073709451115 ns would land after maxInt(u64) ns: the sim's time limit", .linux, refuse3.releaseAtLimit);
}
test "sim: stream faults refuse: failStream twice" {
    try refusal("sim: stream faults refuse: failStream twice", "sim: failStream on sim fd 100033, which already failed", .linux, refuse3.twice);
}
test "sim: stream faults refuse: failStream after a reset was received" {
    try refusal("sim: stream faults refuse: failStream after a reset was received", "sim: failStream on sim fd 100033, which has received a reset: its transport is already over", .linux, refuse3.afterReset);
}
test "sim: stream faults refuse: loss_ppm on a .tcp path" {
    try refusal("sim: stream faults refuse: loss_ppm on a .tcp path", "sim: setImpairment: loss_ppm on a .tcp path is refused", .linux, refuse3.tcpLoss);
}
test "sim: stream faults refuse: burst on a .tcp path" {
    try refusal("sim: stream faults refuse: burst on a .tcp path", "sim: setImpairment: burst on a .tcp path is refused", .linux, refuse3.tcpBurst);
}
test "sim: stream faults refuse: dup_ppm on a .tcp path" {
    try refusal("sim: stream faults refuse: dup_ppm on a .tcp path", "sim: setImpairment: dup_ppm on a .tcp path is refused", .linux, refuse3.tcpDup);
}
test "sim: stream faults refuse: reorder on a .tcp path" {
    try refusal("sim: stream faults refuse: reorder on a .tcp path", "sim: setImpairment: reorder on a .tcp path is refused", .linux, refuse3.tcpReorder);
}
test "sim: stream faults refuse: a path from a .tcp endpoint to a .udp one" {
    try refusal("sim: stream faults refuse: a path from a .tcp endpoint to a .udp one", "sim: setPartition: a path from a tcp endpoint to a udp one", .linux, refuse3.mixedPath);
}

// ---- The connect with no answer (17.9 item 9; 17.9a point 13) ----

const no_answer = struct {
    var which: enum { deadline, deadline_release, cancel, close, cancel_release } = .deadline;
    var op: ev.NetConnect = undefined;
    var to: os_net.sockaddr.in = undefined;
    var calls: u32 = 0;

    fn onDone(_: *ev.Loop, _: *ev.Completion) void {
        calls += 1;
    }

    fn body() !void {
        calls = 0;
        const server = try t1.listen(lo, 0, .{});
        const lport = t1.portOf(server);
        // Everything to the listener's port is held: the SYN gets no answer.
        const p: sim.Path = .{ .from = .{ .protocol = .tcp }, .to = ep(lport) };
        sim.setPartition(p);
        switch (which) {
            .deadline => {
                // zio's connect with a deadline: Timeout at the deadline,
                // the op parked until then; the wrapper's close discards
                // the held SYN, and the connection retires at once.
                try expectError(error.Timeout, t1.connect(lo, lport, .fromMilliseconds(1)));
                try expectEqual(@as(u64, 1_000_000), sim.clockNs());
            },
            .deadline_release => {
                // Held past its deadline, then released: a connect with a
                // deadline on a socket that the caller keeps (zio's
                // `Socket.connect` does not close it, unlike
                // `IpAddress.connect`). The deadline ends the wait with
                // Timeout and leaves the endpoint in C-syn (17.6); the
                // release lands the SYN L later, and the handshake goes on
                // with no op waiting.
                var sock = try zio_net.Socket.open(.stream, .ipv4, .ip);
                try expectError(error.Timeout, sock.connect(.{ .ip = zio_net.IpAddress.initIp4(lo, lport) }, .{ .timeout = .fromMilliseconds(1) }));
                try expectEqual(@as(u64, 1_000_000), sim.clockNs());
                try expectEqual(sim.StreamPhase.c_syn, sim.testStreamEnd(sock.handle).phase);
                try expectEqual(@as(usize, 1), sim.streamEntriesInFlight());
                const t_rel = sim.clockNs();
                _ = sim.healPartition(p);
                const s = try server.accept(.{});
                try expectEqual(t_rel + 3 * L, sim.clockNs());
                try expectEqual(sim.StreamPhase.est, sim.testStreamEnd(sock.handle).phase);
                s.close();
                sock.close();
            },
            .cancel, .close, .cancel_release => {
                to = t1.sockaddr(lo, lport);
                const fd = try t1.evOpen();
                const loop = &rt_mod.getCurrentExecutor().loop;
                op = ev.NetConnect.init(fd, @ptrCast(&to), @sizeOf(os_net.sockaddr.in));
                op.c.callback = onDone;
                op.c.flags.defer_callback = false;
                loop.add(&op.c);
                try rt_mod.sleep(.fromMilliseconds(1));
                try expectEqual(@as(u32, 0), calls);
                try expectEqual(sim.StreamPhase.c_syn, sim.testStreamEnd(fd).phase);
                if (which == .close) {
                    t1.evClose(fd);
                    try rt_mod.sleep(.fromMicroseconds(1));
                    try expectEqual(@as(u32, 1), calls);
                    try expectError(error.FileDescriptorNotASocket, op.getResult());
                } else {
                    loop.cancel(&op.c);
                    try rt_mod.sleep(.fromMicroseconds(1));
                    try expectEqual(@as(u32, 1), calls);
                    try expectError(error.Canceled, op.getResult());
                    // Canceling the wait does not abort the connect.
                    try expectEqual(sim.StreamPhase.c_syn, sim.testStreamEnd(fd).phase);
                    if (which == .cancel_release) {
                        // A canceled wait, then released (the cancel is not
                        // a deadline: the op had none): the SYN lands L
                        // after the release, and the handshake goes on with
                        // no op waiting (17.6).
                        const t_rel = sim.clockNs();
                        _ = sim.healPartition(p);
                        const s = try server.accept(.{});
                        try expectEqual(t_rel + 3 * L, sim.clockNs());
                        try expectEqual(sim.StreamPhase.est, sim.testStreamEnd(fd).phase);
                        try expectEqual(@as(u32, 1), calls);
                        s.close();
                    }
                    t1.evClose(fd);
                }
            },
        }
        if (which != .cancel_release and which != .deadline_release) {
            try expectEqual(@as(u64, 1), sim.streamStats().syn_dropped_dead);
            try expectEqual(@as(u64, 1), sim.streamStats().discarded_held);
            try expectEqual(@as(usize, 0), sim.streamEntriesInFlight());
            try expectEqual(@as(usize, 0), sim.testConnsLive());
            try expectEqual(@as(usize, 0), sim.testListenerCount(server.socket.handle));
            _ = sim.healPartition(p);
        }
        server.close();
        try drain();
    }
};

test "sim: stream faults: the connect with no answer: a held SYN ends at the deadline (Timeout), a cancel or a close, and the close discards it; released after the deadline of a connect that keeps its socket, or after a canceled wait, it still connects" {
    if (comptime skip) return error.SkipZigTest;
    for ([_]@TypeOf(no_answer.which){ .deadline, .deadline_release, .cancel, .close, .cancel_release }) |w| {
        no_answer.which = w;
        try run(1, .linux, no_answer.body);
    }
}

// ---- The outside review of slice 3, section 8 and section 4: cases no
// test had (sitting 33) ----

/// A held credit goes when its sender aborts (17.9a point 9) or fails
/// (17.9): it never lands, and the data's sender stays credited 0.
const held_credit_gone = struct {
    var which: enum { abort, fail } = .abort;

    fn body() !void {
        const srv, const c, const s = try pair();
        chunksOff(c, s);
        try expectEqual(@as(usize, 2), try c.write("ab", .none));
        try rt_mod.sleep(.fromNanoseconds(L));
        sim.setPartition(s2c);
        var b: [1]u8 = undefined;
        try expectEqual(@as(usize, 1), try s.read(&b, .none));
        try expectEqual(@as(u32, 1), (try dir(false)).held_entries);
        const before = sim.streamStats().discarded_held;
        switch (which) {
            // One byte unread: the close is an abort. Its RST is held by
            // the partition, alone: the credit went first.
            .abort => s.close(),
            .fail => sim.failStream(s.socket.handle, .reset),
        }
        try expectEqual(before + 1, sim.streamStats().discarded_held);
        try expectEqual(@as(u32, @intFromBool(which == .abort)), (try dir(false)).held_entries);
        const t_rel = sim.clockNs();
        _ = sim.healPartition(s2c);
        try sleepUntil(t_rel + L);
        try expectEqual(@as(u64, 0), (try dir(true)).credited);
        try expectEqual(@as(u64, 0), sim.streamStats().credits_landed);
        try expectEqual(which == .abort, sim.testStreamEnd(c.socket.handle).reset_received);
        try checkConserved();
        if (which == .fail) s.close();
        try t1.closeAll2(c, srv);
    }
};

test "sim: stream faults: a held credit is discarded when its sender aborts or fails, and never lands" {
    if (comptime skip) return error.SkipZigTest;
    for ([_]@TypeOf(held_credit_gone.which){ .abort, .fail }) |w| {
        held_credit_gone.which = w;
        try run(1, .linux, held_credit_gone.body);
    }
}

/// Hold, release, hold again: an entry released onto the wire meets a new
/// partition before it lands, is held at its landing, and a later send is
/// held behind it; the second release lands both in order.
const rehold = struct {
    fn body() !void {
        const srv, const c, const s = try pair();
        chunksOff(c, s);
        sim.setPartition(c2s);
        _ = try c.write("x", .none);
        try rt_mod.sleep(.fromNanoseconds(L));
        const t1r = sim.clockNs();
        _ = sim.healPartition(c2s);
        try sleepUntil(t1r + L / 2);
        try expectEqual(@as(u32, 0), (try dir(true)).held_entries);
        sim.setPartition(c2s);
        try sleepUntil(t1r + L);
        try expectEqual(@as(usize, 0), unread(s));
        try expectEqual(@as(u32, 1), (try dir(true)).held_entries);
        _ = try c.write("y", .none);
        try expectEqual(@as(u32, 2), (try dir(true)).held_entries);
        const ss = sim.streamStats();
        try expectEqual(@as(u64, 2), ss.held_at_send);
        try expectEqual(@as(u64, 1), ss.held_at_landing);
        try expectEqual(@as(u64, 1), ss.released);
        try checkConserved();
        try sleepUntil(t1r + 2 * L);
        const t2 = sim.clockNs();
        _ = sim.healPartition(c2s);
        try sleepUntil(t2 + L - 1);
        try expectEqual(@as(usize, 0), unread(s));
        try sleepUntil(t2 + L);
        var b: [2]u8 = undefined;
        try expectEqual(@as(usize, 2), try s.read(&b, .none));
        try expect(std.mem.eql(u8, "xy", &b));
        try expectEqual(@as(u64, 3), sim.streamStats().released);
        try t1.closeAll3(c, s, srv);
    }
};

test "sim: stream faults: hold, release, hold again: a released entry held at its landing by a new partition, with a later send behind it, lands in order at the second release" {
    if (comptime skip) return error.SkipZigTest;
    try run(1, .linux, rehold.body);
}

/// `failStream` wakes a parked receive, and before the harvest the op is
/// canceled or its socket closed: it ends Canceled or
/// FileDescriptorNotASocket, and the failure is not reported by it. After
/// a cancel, the next receive gets the failure once.
const fail_unharvested = struct {
    var which: enum { cancel, close } = .cancel;
    var op: ev.NetRecv = undefined;
    var storage: [1]os_net.iovec = undefined;
    var buf: [4]u8 = undefined;
    var calls: u32 = 0;

    fn onDone(_: *ev.Loop, _: *ev.Completion) void {
        calls += 1;
    }

    fn body() !void {
        calls = 0;
        const srv, const c, const s = try pair();
        const loop = &rt_mod.getCurrentExecutor().loop;
        op = ev.NetRecv.init(c.socket.handle, .fromSlices(&.{&buf}, &storage), .{});
        op.c.callback = onDone;
        op.c.flags.defer_callback = false;
        loop.add(&op.c);
        try rt_mod.sleep(.fromMicroseconds(1));
        try expectEqual(@as(u32, 0), calls);
        try expect(sim.testStreamParked(c.socket.handle).recv);
        // Woken by the failure and not yet harvested: nothing yields
        // between these lines.
        sim.failStream(c.socket.handle, .reset);
        try expect(!sim.testStreamParked(c.socket.handle).recv);
        switch (which) {
            .cancel => loop.cancel(&op.c),
            .close => c.close(),
        }
        try rt_mod.sleep(.fromMicroseconds(1));
        try expectEqual(@as(u32, 1), calls);
        const end = sim.testConnEnd(1, false) orelse return error.ConnectionRetired;
        try expect(!end.fail_reported);
        switch (which) {
            .cancel => {
                try expectError(error.Canceled, op.getResult());
                var b: [4]u8 = undefined;
                try expectError(error.ConnectionResetByPeer, c.read(&b, .none));
                try expect(sim.testStreamEnd(c.socket.handle).fail_reported);
                try expectEqual(@as(usize, 0), try c.read(&b, .none));
                try t1.closeAll3(c, s, srv);
            },
            .close => {
                try expectError(error.FileDescriptorNotASocket, op.getResult());
                try t1.closeAll2(s, srv);
            },
        }
    }
};

test "sim: stream faults: a receive woken by failStream and canceled, or whose socket is closed, before its harvest does not report the failure; after a cancel the next receive does, once" {
    if (comptime skip) return error.SkipZigTest;
    for ([_]@TypeOf(fail_unharvested.which){ .cancel, .close }) |w| {
        fail_unharvested.which = w;
        try run(1, .linux, fail_unharvested.body);
    }
}

/// A held SYN-ACK (the server's answer held) and a held ACK (the client's
/// last handshake entry held), each with what can end the wait meanwhile.
/// The op model keeps the handshake out of its rules (17.9a point 15), so
/// these are scenario tests.
const held_handshake = struct {
    var which: enum { synack_deadline, synack_cancel, synack_close, ack_accept_deadline, ack_client_close, ack_listener_close } = .synack_deadline;
    var op: ev.NetConnect = undefined;
    var to: os_net.sockaddr.in = undefined;
    var calls: u32 = 0;

    fn onDone(_: *ev.Loop, _: *ev.Completion) void {
        calls += 1;
    }

    fn body() !void {
        calls = 0;
        const server = try t1.listen(lo, 0, .{});
        const lport = t1.portOf(server);
        const back: sim.Path = .{ .from = ep(lport), .to = .{ .protocol = .tcp } };
        const fwd: sim.Path = .{ .from = .{ .protocol = .tcp }, .to = ep(lport) };
        const t0 = sim.clockNs();
        switch (which) {
            .synack_deadline => {
                // The SYN lands; its SYN-ACK is held. zio's connect times
                // out and closes its socket: the client is dead with
                // nothing of its own held. The release lands the SYN-ACK
                // at the dead client, which answers one RST; that kills
                // the provisional server endpoint.
                sim.setPartition(back);
                try expectError(error.Timeout, t1.connect(lo, lport, .fromMilliseconds(1)));
                try expectEqual(@as(usize, 1), sim.testListenerCount(server.socket.handle));
                try expectEqual(@as(usize, 1), sim.streamEntriesInFlight());
                const t_rel = sim.clockNs();
                _ = sim.healPartition(back);
                try sleepUntil(t_rel + 2 * L);
                try expectEqual(@as(usize, 0), sim.testListenerCount(server.socket.handle));
                try expectEqual(@as(usize, 0), sim.streamEntriesInFlight());
            },
            .synack_cancel, .synack_close => {
                sim.setPartition(back);
                to = t1.sockaddr(lo, lport);
                const fd = try t1.evOpen();
                const loop = &rt_mod.getCurrentExecutor().loop;
                op = ev.NetConnect.init(fd, @ptrCast(&to), @sizeOf(os_net.sockaddr.in));
                op.c.callback = onDone;
                op.c.flags.defer_callback = false;
                loop.add(&op.c);
                try rt_mod.sleep(.fromMilliseconds(1));
                try expectEqual(@as(u32, 0), calls);
                try expectEqual(@as(usize, 1), sim.testListenerCount(server.socket.handle));
                if (which == .synack_cancel) {
                    // The wait ends; the connect goes on: the release lands
                    // the SYN-ACK and the client is established with no op
                    // waiting.
                    loop.cancel(&op.c);
                    try rt_mod.sleep(.fromMicroseconds(1));
                    try expectError(error.Canceled, op.getResult());
                    try expectEqual(sim.StreamPhase.c_syn, sim.testStreamEnd(fd).phase);
                    const t_rel = sim.clockNs();
                    _ = sim.healPartition(back);
                    const s = try server.accept(.{});
                    try expectEqual(t_rel + 2 * L, sim.clockNs());
                    try expectEqual(sim.StreamPhase.est, sim.testStreamEnd(fd).phase);
                    s.close();
                } else {
                    t1.evClose(fd);
                    try rt_mod.sleep(.fromMicroseconds(1));
                    try expectError(error.FileDescriptorNotASocket, op.getResult());
                    const t_rel = sim.clockNs();
                    _ = sim.healPartition(back);
                    try sleepUntil(t_rel + 2 * L);
                    try expectEqual(@as(usize, 0), sim.testListenerCount(server.socket.handle));
                }
                try expectEqual(@as(u32, 1), calls);
                if (which == .synack_cancel) t1.evClose(fd);
            },
            .ack_accept_deadline, .ack_client_close, .ack_listener_close => {
                // The SYN and the SYN-ACK pass; the partition starts before
                // the SYN-ACK lands, so the client's ACK is held at its
                // send. The client is established; the server endpoint
                // stays provisional and no accept completes.
                to = t1.sockaddr(lo, lport);
                const fd = try t1.evOpen();
                const loop = &rt_mod.getCurrentExecutor().loop;
                op = ev.NetConnect.init(fd, @ptrCast(&to), @sizeOf(os_net.sockaddr.in));
                op.c.callback = onDone;
                op.c.flags.defer_callback = false;
                loop.add(&op.c);
                try sleepUntil(t0 + 3 * L / 2);
                sim.setPartition(fwd);
                try sleepUntil(t0 + 2 * L);
                try expectEqual(@as(u32, 1), calls);
                try op.getResult();
                try expectEqual(sim.StreamPhase.est, sim.testStreamEnd(fd).phase);
                const cs = sim.testConnEnd(1, true) orelse return error.ConnectionRetired;
                try expectEqual(sim.StreamPhase.s_prov, cs.phase);
                try expectEqual(@as(u32, 1), (try dir(true)).held_entries);
                switch (which) {
                    .ack_accept_deadline => {
                        try expectError(error.Timeout, server.accept(.{ .timeout = .fromMilliseconds(1) }));
                        const t_rel = sim.clockNs();
                        _ = sim.healPartition(fwd);
                        const s = try server.accept(.{});
                        try expectEqual(t_rel + L, sim.clockNs());
                        s.close();
                        t1.evClose(fd);
                    },
                    .ack_client_close => {
                        // The client's FIN is held behind its ACK (17.9a
                        // point 9: handshake entries stay held, and
                        // everything after them).
                        t1.evClose(fd);
                        try expectEqual(@as(u32, 2), (try dir(true)).held_entries);
                        const t_rel = sim.clockNs();
                        _ = sim.healPartition(fwd);
                        const s = try server.accept(.{});
                        try expectEqual(t_rel + L, sim.clockNs());
                        var b: [1]u8 = undefined;
                        try expectEqual(@as(usize, 0), try s.read(&b, .none));
                        s.close();
                    },
                    .ack_listener_close => {
                        // The provisional endpoint dies with its listener
                        // and answers the client with an RST, which is not
                        // held (the partition is the other way). The ACK,
                        // released later, lands at a dead endpoint.
                        server.close();
                        try rt_mod.sleep(.fromNanoseconds(L));
                        try expect(sim.testStreamEnd(fd).reset_received);
                        const dead0 = sim.streamStats().dropped_dead;
                        const t_rel = sim.clockNs();
                        _ = sim.healPartition(fwd);
                        try sleepUntil(t_rel + L);
                        try expectEqual(dead0 + 1, sim.streamStats().dropped_dead);
                        t1.evClose(fd);
                        try drain();
                        try expectEqual(@as(usize, 0), sim.testConnsLive());
                        return;
                    },
                    else => unreachable,
                }
            },
        }
        server.close();
        try drain();
        try expectEqual(@as(usize, 0), sim.testConnsLive());
    }
};

test "sim: stream faults: a held SYN-ACK past a connect's deadline, cancel or close, and a held ACK past an accept's deadline, a client close or a listener close: each ends as the handshake rules say once released" {
    if (comptime skip) return error.SkipZigTest;
    for ([_]@TypeOf(held_handshake.which){ .synack_deadline, .synack_cancel, .synack_close, .ack_accept_deadline, .ack_client_close, .ack_listener_close }) |w| {
        held_handshake.which = w;
        try run(1, .linux, held_handshake.body);
    }
}

// ---- t-4139: what std.Io's reader and writer see for failStream ----
//
// zio's ev layer and zio.net give `ConnectionTimedOut` for `.timed_out`
// (the tests above). std.Io has no such read or write error, and zio's
// src/io.zig maps it to `ConnectionResetByPeer` (`recvErrToReadErr`,
// `sendErrToWriteErr`). So a std.Io program, hotseat's TestHost among them,
// sees the same error for both kinds. After the failure is reported, a
// read gives end of stream and a write `SocketUnconnected` (BrokenPipe,
// mapped). This pins today's mapping; it does not judge it (the mapping is
// upstream's file).

const std_io_fail = struct {
    var how: sim.StreamFailure = .timed_out;
    var first: enum { read, write } = .read;

    fn body() !void {
        const io = t1.runtime.io();
        const server = try t1.listen(lo, 0, .{});
        const addr: std.Io.net.IpAddress = .{ .ip4 = .{ .bytes = lo, .port = t1.portOf(server) } };
        const client = try std.Io.net.IpAddress.connect(&addr, io, .{ .mode = .stream });
        const s = try server.accept(.{});
        sim.failStream(client.socket.handle, how);
        var rbuf: [64]u8 = undefined;
        var r = std.Io.net.Stream.Reader.init(client, io, &rbuf);
        var wbuf: [64]u8 = undefined;
        var w = std.Io.net.Stream.Writer.init(client, io, &wbuf);
        var b: [8]u8 = undefined;
        switch (first) {
            .read => {
                try expectError(error.ReadFailed, r.interface.readSliceShort(&b));
                try expectEqual(@as(?std.Io.net.Stream.Reader.Error, error.ConnectionResetByPeer), r.err);
                try w.interface.writeAll("x");
                try expectError(error.WriteFailed, w.interface.flush());
                try expectEqual(@as(?std.Io.net.Stream.Writer.Error, error.SocketUnconnected), w.err);
            },
            .write => {
                try w.interface.writeAll("x");
                try expectError(error.WriteFailed, w.interface.flush());
                try expectEqual(@as(?std.Io.net.Stream.Writer.Error, error.ConnectionResetByPeer), w.err);
                try expectEqual(@as(usize, 0), try r.interface.readSliceShort(&b));
            },
        }
        try expect(sim.testStreamEnd(client.socket.handle).fail_reported);
        client.close(io);
        s.close();
        server.close();
        try drain();
    }
};

test "sim: stream faults: std.Io's reader and writer see ConnectionResetByPeer for failStream .timed_out and .reset alike, then end of stream and SocketUnconnected (t-4139: zio's io.zig mapping, pinned)" {
    if (comptime skip) return error.SkipZigTest;
    for ([_]sim.StreamFailure{ .timed_out, .reset }) |how| {
        for ([_]@TypeOf(std_io_fail.first){ .read, .write }) |f| {
            std_io_fail.how = how;
            std_io_fail.first = f;
            try run(1, .linux, std_io_fail.body);
            try run(1, .macos, std_io_fail.body);
        }
    }
}

// ---- Parity: the faults a loopback kernel can show (17.15 item 5) ----
//
// A fault rule has no kernel switch on loopback, so a row runs the sim's
// fault in a sim run and the kernel's own case on real sockets, and both
// must give the same tokens through zio.net:
// - no answer: the sim holds the SYN (a `.tcp` partition); macOS gives no
//   answer to a SYN for 127.0.0.2, which lo0 does not carry by default
//   (row 15). Linux answers on all of 127/8, so it has no column: there
//   the sim's rule is its own (the connect-with-no-answer tests).
// - a peer that stops reading: the sim fails the reader (`failStream`, so
//   it sends no credit); on the kernel the reader never reads. Both
//   kernels have a column.
// Not measurable on loopback, so the sim's own rules: delay and jitter, the
// release of held entries, and what `failStream` reports (a kernel gives
// ETIMEDOUT only after minutes of retransmission).

const Kernel = enum { macos, linux };
const host_kernel: Kernel = if (builtin.os.tag.isDarwin()) .macos else .linux;
const Timestamp = @import("time.zig").Timestamp;

const prow3 = struct {
    const wait_ms = 200;
    var out: [128]u8 = undefined;
    var len: usize = 0;
    var big: [32 << 20]u8 = undefined;

    fn put(t: []const u8) void {
        if (len != 0) {
            out[len] = ',';
            len += 1;
        }
        @memcpy(out[len..][0..t.len], t);
        len += t.len;
    }

    fn errTok(err: anyerror) []const u8 {
        return switch (err) {
            error.BrokenPipe => "EPIPE",
            error.ConnectionResetByPeer => "ECONNRESET",
            error.Timeout => "TIMEOUT",
            else => @errorName(err),
        };
    }

    fn msSince(t0: Timestamp) u64 {
        return (Timestamp.now(.monotonic).toNanoseconds() - t0.toNanoseconds()) / std.time.ns_per_ms;
    }

    /// A connect whose SYN gets no answer: TIMEOUT at its deadline, the op
    /// parked until then.
    fn noAnswer() !void {
        const l = try t1.listen(.{ 0, 0, 0, 0 }, 0, .{});
        defer l.close();
        const port = t1.portOf(l);
        const p: sim.Path = .{ .from = .{ .protocol = .tcp }, .to = ep(port) };
        if (sim.isRunning()) sim.setPartition(p);
        defer if (sim.isRunning()) {
            _ = sim.healPartition(p);
        };
        const target: [4]u8 = if (sim.isRunning()) lo else .{ 127, 0, 0, 2 };
        const t0 = Timestamp.now(.monotonic);
        if (t1.connect(target, port, .fromMilliseconds(wait_ms))) |c| {
            c.close();
            put("connected");
        } else |err| put(errTok(err));
        put(if (msSince(t0) >= wait_ms) "parked" else "early");
    }

    var w_result: anyerror!void = {};
    var w_done = false;

    fn writer(c: zio_net.Stream) void {
        w_result = c.writeAll(&big, .fromMilliseconds(wait_ms));
        w_done = true;
    }

    /// A reader that stops reading: the writer fills what the path holds,
    /// then parks until its deadline; nothing comes back to it.
    fn peerStops() !void {
        w_done = false;
        const l = try t1.listen(lo, 0, .{});
        defer l.close();
        const c = try t1.connect(lo, t1.portOf(l), .none);
        defer c.close();
        const s = try l.accept(.{});
        defer s.close();
        if (sim.isRunning()) sim.failStream(s.socket.handle, .timed_out);
        var h = try rt_mod.spawn(writer, .{c});
        defer h.cancel();
        try rt_mod.sleep(.fromMilliseconds(50));
        put(if (w_done) "done" else "blocked");
        h.join();
        if (w_result) |_| put("all") else |err| put(errTok(err));
        var b: [16]u8 = undefined;
        if (c.read(&b, .fromMilliseconds(50))) |n| {
            put(if (n == 0) "0" else "data");
        } else |err| put(errTok(err));
    }

    const Row = struct { name: []const u8, f: *const fn () anyerror!void, macos: ?[]const u8, linux: ?[]const u8 };
    const rows = [_]Row{
        .{ .name = "no answer", .f = noAnswer, .macos = "TIMEOUT,parked", .linux = null },
        .{ .name = "peer stops reading", .f = peerStops, .macos = "blocked,TIMEOUT,TIMEOUT", .linux = "blocked,TIMEOUT,TIMEOUT" },
    };

    var failed: usize = 0;
    var column: Kernel = .macos;

    fn body() !void {
        failed = 0;
        for (rows) |r| {
            const want = (if (column == .macos) r.macos else r.linux) orelse {
                std.debug.print("parity row {s}, {t} column: not run: not measurable on this kernel's loopback; the sim's rule is its own\n", .{ r.name, column });
                continue;
            };
            len = 0;
            r.f() catch |err| put(@errorName(err));
            std.debug.print("parity row {s}, {t} column, {s}: {s}\n", .{ r.name, column, if (sim.isRunning()) "sim" else "kernel", out[0..len] });
            if (!std.mem.eql(u8, want, out[0..len])) {
                std.debug.print("parity row {s}, {t} column: want {s}\n                     got  {s}\n", .{ r.name, column, want, out[0..len] });
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

test "sim: stream parity: slice 3's rows (no answer, a peer that stops reading) through zio.net, on this host's kernel without sim, in a sim run with its profile with sim" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    if (comptime sim.compiled_in) return prow3.inSim(host_kernel);
    prow3.column = host_kernel;
    const rt = try rt_mod.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var h = try rt.spawn(prow3.body, .{});
    try h.join();
    if (prow3.failed != 0) return error.ParityRowsDiffer;
}

test "sim: stream parity: slice 3's rows, the other kernel's column in a sim run" {
    if (comptime skip) return error.SkipZigTest;
    try prow3.inSim(if (host_kernel == .macos) .linux else .macos);
}

// ---- Sensitivity (design 17.11; 17.9a point 14) ----

/// One run whose state digest is taken with a setting `which` chose, before
/// the teardown: two runs that differ only there must give two digests.
const snap = struct {
    var which: u8 = 0;
    var digest: u64 = 0;

    fn body() !void {
        switch (which) {
            2 => sim.setStreamWindowDefault(1000),
            else => {},
        }
        const server = try t1.listen(lo, 0, .{ .kernel_backlog = if (which == 3) 5 else 4 });
        const c = try t1.connect(lo, t1.portOf(server), .none);
        const s = try server.accept(.{});
        switch (which) {
            0 => sim.setStreamChunking(c.socket.handle, .off),
            1 => sim.setStreamChunking(c.socket.handle, .{ .fixed = 1 }),
            // A held byte, 'x' or 'y'.
            4, 5 => {
                sim.setPartition(c2s);
                _ = try c.write(if (which == 4) "x" else "y", .none);
            },
            // A held entry whose delay differs by 1 ns. The impairment is
            // cleared before the snapshot, so only the entry's extra time
            // differs.
            6, 7 => {
                sim.setPartition(c2s);
                sim.setImpairment(c2s, .{ .delay_ns = if (which == 6) 1000 else 1001 });
                _ = try c.write("x", .none);
                _ = sim.clearImpairment(c2s);
            },
            8 => sim.failStream(c.socket.handle, .reset),
            9 => sim.failStream(c.socket.handle, .timed_out),
            // The same byte and the same rule as 4, but the byte left
            // before the partition was set: it is on the wire, not held,
            // when the snapshot is taken (it lands L later).
            14 => {
                _ = try c.write("x", .none);
                sim.setPartition(c2s);
            },
            // A .tcp rule against the same rule as .udp, matching nothing.
            // Both runs set another .tcp rule first, so only the rule's
            // protocol differs.
            10, 11 => {
                sim.setPartition(.{ .from = ep(3), .to = ep(4) });
                if (which == 10) sim.setPartition(.{ .from = ep(1), .to = ep(2) }) else sim.setPartition(.{ .from = .{ .ip = lo, .port = 1 }, .to = .{ .ip = lo, .port = 2 } });
            },
            else => {},
        }
        digest = sim.stateDigest();
        switch (which) {
            4, 5, 14 => _ = sim.healPartition(c2s),
            6, 7 => _ = sim.healPartition(c2s),
            10, 11 => {
                _ = sim.healPartition(.{ .from = ep(3), .to = ep(4) });
                if (which == 10) _ = sim.healPartition(.{ .from = ep(1), .to = ep(2) }) else _ = sim.healPartition(.{ .from = .{ .ip = lo, .port = 1 }, .to = .{ .ip = lo, .port = 2 } });
            },
            else => {},
        }
        c.close();
        s.close();
        server.close();
        try drain();
    }

    fn digestOf(w: u8, profile: sim.StreamProfile) !u64 {
        which = w;
        try run(1, profile, body);
        return digest;
    }
};

test "sim: stream faults: sensitivity: two runs equal but for one setting give two state digests: chunking .off and .fixed = 1, the default window, the profile, the backlog, a held byte, a held entry's delay, the failure's kind, held against on the wire, failed against not, a .tcp rule and a .udp one" {
    if (comptime skip) return error.SkipZigTest;
    const pairs = [_]struct { u8, u8, sim.StreamProfile, sim.StreamProfile, []const u8 }{
        .{ 0, 1, .linux, .linux, "chunking .off against .fixed = 1" },
        .{ 0, 2, .linux, .linux, "the default window" },
        .{ 0, 0, .linux, .macos, "the profile" },
        .{ 0, 3, .linux, .linux, "the backlog" },
        .{ 4, 5, .linux, .linux, "a held byte" },
        .{ 6, 7, .linux, .linux, "a held entry's delay, 1 ns apart" },
        .{ 8, 9, .linux, .linux, "the failure's kind" },
        // 17.9a point 14's two distinctions (outside review of slice 3,
        // P3: the pairs above vary a held byte and a failure's kind, never
        // held against on the wire or failed against not). 12 is a run with
        // no setting.
        .{ 4, 14, .linux, .linux, "an entry held against the same entry on the wire" },
        .{ 12, 8, .linux, .linux, "a failed endpoint against one not failed" },
        .{ 10, 11, .linux, .linux, "a .tcp rule against a .udp one" },
    };
    for (pairs) |pr| {
        const a = try snap.digestOf(pr[0], pr[2]);
        const b = try snap.digestOf(pr[1], pr[3]);
        // And each run repeats itself: the digest is not noise.
        const a2 = try snap.digestOf(pr[0], pr[2]);
        std.debug.print("sensitivity, {s}: {x:0>16} against {x:0>16}\n", .{ pr[4], a, b });
        try expectEqual(a, a2);
        try expect(a != b);
    }
}

/// A faulted exchange: an echo through a delay with jitter and a partition
/// held and healed, every byte recorded by the receiver.
const faulted_echo = struct {
    var delay: u64 = 30_000;
    var transcript: [4000]u8 = undefined;
    var got: usize = 0;

    fn body() !void {
        got = 0;
        const srv, const c, const s = try pair();
        sim.setImpairment(c2s, .{ .delay_ns = delay, .jitter_ns = 20_000 });
        sim.setPartition(s2c);
        var data: [2000]u8 = undefined;
        for (&data, 0..) |*x, i| x.* = @truncate(i *% 7);
        _ = try c.write(&data, .none);
        var n: usize = 0;
        while (n < data.len) n += try s.read(transcript[n..data.len], .none);
        _ = try s.write(transcript[0..n], .none);
        try rt_mod.sleep(.fromMilliseconds(1));
        _ = sim.healPartition(s2c);
        while (got < data.len) got += try c.read(transcript[data.len + got .. 2 * data.len], .none);
        _ = sim.clearImpairment(c2s);
        try t1.closeAll3(c, s, srv);
    }

    const Result = struct { wire: u64, trace: u64, transcript: [4000]u8 };

    var last_wire: u64 = 0;
    var last_trace: u64 = 0;
};

const faulted_echo_run = struct {
    fn body() !void {
        try faulted_echo.body();
        faulted_echo.last_wire = sim.wireDigest();
        faulted_echo.last_trace = sim.traceHash();
    }
};

fn faultedEcho(seed: u64, d: u64) !faulted_echo.Result {
    faulted_echo.delay = d;
    try run(seed, .linux, faulted_echo_run.body);
    return .{ .wire = faulted_echo.last_wire, .trace = faulted_echo.last_trace, .transcript = faulted_echo.transcript };
}

test "sim: stream faults: sensitivity: a faulted run repeats in one process; 1 ns more delay gives another wire digest with the same transcript; another seed gives another wire digest" {
    if (comptime skip) return error.SkipZigTest;
    const a = try faultedEcho(1, 30_000);
    const a2 = try faultedEcho(1, 30_000);
    try expectEqual(a.wire, a2.wire);
    try expectEqual(a.trace, a2.trace);
    try expect(std.mem.eql(u8, &a.transcript, &a2.transcript));
    const b = try faultedEcho(1, 30_001);
    try expect(a.wire != b.wire);
    try expect(std.mem.eql(u8, &a.transcript, &b.transcript));
    const c = try faultedEcho(2, 30_000);
    try expect(a.wire != c.wire);
    try expect(std.mem.eql(u8, &a.transcript, &c.transcript));
    std.debug.print("faulted echo: wire {x:0>16}, +1 ns {x:0>16}, seed 2 {x:0>16}\n", .{ a.wire, b.wire, c.wire });
}

// ---- The surface table's slice 3 rows (design 17.15 item 1) ----

/// Each of slice 3's four rows runs its own scenarios again.
pub const chk3 = struct {
    /// 17.6 SYN-ACK 1: a SYN-ACK at a failed client is dropped, no answer.
    pub fn synackFailedClient() anyerror!void {
        fail_c_syn.keep_open = true;
        for ([_]sim.StreamFailure{ .reset, .timed_out }) |how| {
            fail_c_syn.how = how;
            try run(1, .linux, fail_c_syn.body);
        }
    }

    /// 17.9 `.tcp` rules: delay, jitter, holding at send and at landing,
    /// release, credits held and coalesced, FIN and abort while held.
    pub fn tcpRules() anyerror!void {
        for ([_]@TypeOf(delay_dirs.which){ .c2s, .s2c, .both }) |w| {
            delay_dirs.which = w;
            try run(1, .linux, delay_dirs.body);
        }
        jitter.seed = 1;
        try run(1, .linux, jitter.body);
        held_at_send.which = .partition;
        try run(1, .linux, held_at_send.body);
        held_at_send.which = .silence;
        try run(1, .linux, held_at_send.body);
        try run(1, .linux, held_at_landing.body);
        try run(1, .linux, partition_and_delay.body);
        try run(1, .linux, reverse_stall.body);
        try run(1, .linux, coalesce.body);
        fin_held.which = .shutdown;
        try run(1, .linux, fin_held.body);
        abort_held.which = .rst_lands;
        try run(1, .linux, abort_held.body);
    }

    /// 17.9 failStream in `est`: each parked op, both, and none.
    pub fn failStream() anyerror!void {
        for ([_]@TypeOf(fail_parked.which){ .recv, .send, .none_recv_first, .none_send_first }) |w| {
            fail_parked.how = .reset;
            fail_parked.which = w;
            try run(1, .linux, fail_parked.body);
        }
        fail_both.seen = .{ 0, 0 };
        for (1..17) |sd| try run(sd, .linux, fail_both.body);
        if (fail_both.seen[0] == 0 or fail_both.seen[1] == 0) return error.OneHarvestOrderNeverHappened;
        try run(1, .linux, fail_effects.body);
        try run(1, .linux, fail_peer.body);
    }

    /// 17.9 the connect with no answer: deadline, a release after the
    /// deadline, cancel, close, a release after a cancel.
    pub fn connectNoAnswer() anyerror!void {
        for ([_]@TypeOf(no_answer.which){ .deadline, .deadline_release, .cancel, .close, .cancel_release }) |w| {
            no_answer.which = w;
            try run(1, .linux, no_answer.body);
        }
    }
};
