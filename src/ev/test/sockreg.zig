// SPDX-FileCopyrightText: 2025 Lukáš Lalinský
// SPDX-License-Identifier: MIT

//! Interleavings of the shared socket registration table (`ev/sockreg.zig`)
//! that need two loops, reproduced on one thread by driving the real
//! `service` and `detach` with a fake backend and loop state.

const std = @import("std");
const sockreg = @import("../sockreg.zig");
const Completion = @import("../completion.zig").Completion;
const NetPoll = @import("../completion.zig").NetPoll;
const net = @import("../../os/net.zig");

/// The owner loop's backend: every waiter's syscall completes.
const FakeBackend = struct {
    shared: struct { sock_table: sockreg.Table = .{} } = .{},

    pub fn checkCompletion(c: *Completion, event: void) enum { completed, requeue } {
        _ = c;
        _ = event;
        return .completed;
    }
};

/// The owner loop's state. When `service` finishes the first waiter, another
/// loop cancels `cancel_target` (`Loop.cancel` -> `cancelLocal` -> the
/// backend's `cancel` -> `sockreg.detach`). That is a real interleaving:
/// `service` has dropped the shard lock before it finishes anything, and the
/// target is still `.running` until `service` reaches it.
const FakeState = struct {
    backend: *FakeBackend,
    cancel_target: *Completion,
    finished: [8]*Completion = undefined,
    finished_n: usize = 0,
    detach_claimed: ?bool = null,

    pub fn markCompletedFromBackend(self: *FakeState, c: *Completion) void {
        self.finished[self.finished_n] = c;
        self.finished_n += 1;
        if (self.detach_claimed == null) self.detach_claimed = sockreg.detach(self.backend, self.cancel_target);
    }

    fn finishedCount(self: *const FakeState, c: *Completion) usize {
        var n: usize = 0;
        for (self.finished[0..self.finished_n]) |f| n += @intFromBool(f == c);
        return n;
    }
};

test "sockreg: a cancel during service's drain does not claim an op service is finishing" {
    if (@typeInfo(net.fd_t) != .int) return error.SkipZigTest;
    const fd: net.fd_t = 7;

    var backend: FakeBackend = .{};
    backend.shared.sock_table.acquire(std.testing.allocator);
    defer backend.shared.sock_table.release();

    var polls: [3]NetPoll = .{ .init(fd, .recv), .init(fd, .recv), .init(fd, .recv) };
    for (&polls) |*p| _ = p.c.enterRunning();

    // Three waiters parked on (fd, read), as `park` leaves them.
    const shard = backend.shared.sock_table.shardForFd(fd);
    const gop = try shard.map.getOrPut(std.testing.allocator, @as(u32, @bitCast(fd)));
    gop.value_ptr.* = .{};
    for (&polls) |*p| gop.value_ptr.waiters(.read).push(&p.c);

    var state: FakeState = .{ .backend = &backend, .cancel_target = &polls[1].c };
    sockreg.service(&backend, &state, fd, .read, {});

    // `service` took all three out of the waiter queue under the lock, so the
    // op is already its own: the cancel must lose, and `service` finishes each
    // op exactly once. A cancel that claims it too would finish it a second
    // time, with `error.Canceled`.
    try std.testing.expectEqual(@as(?bool, false), state.detach_claimed);
    for (&polls) |*p| try std.testing.expectEqual(@as(usize, 1), state.finishedCount(&p.c));
    const entry = shard.map.getPtr(@as(u32, @bitCast(fd))).?;
    try std.testing.expect(entry.waiters(.read).head == null);
    try std.testing.expect(entry.waiters(.read).tail == null);
}

test "sockreg: a cancel claims an op that is still parked" {
    if (@typeInfo(net.fd_t) != .int) return error.SkipZigTest;
    const fd: net.fd_t = 7;

    var backend: FakeBackend = .{};
    backend.shared.sock_table.acquire(std.testing.allocator);
    defer backend.shared.sock_table.release();

    var polls: [3]NetPoll = .{ .init(fd, .recv), .init(fd, .recv), .init(fd, .recv) };
    for (&polls) |*p| _ = p.c.enterRunning();
    const shard = backend.shared.sock_table.shardForFd(fd);
    const gop = try shard.map.getOrPut(std.testing.allocator, @as(u32, @bitCast(fd)));
    gop.value_ptr.* = .{};
    for (&polls) |*p| gop.value_ptr.waiters(.read).push(&p.c);

    // The middle one: both its links are set, and it really is parked.
    try std.testing.expect(sockreg.detach(&backend, &polls[1].c));
    // A second cancel of the same op finds it gone.
    try std.testing.expect(!sockreg.detach(&backend, &polls[1].c));

    const q = gop.value_ptr.waiters(.read);
    try std.testing.expect(q.head == &polls[0].c);
    try std.testing.expect(q.tail == &polls[2].c);
    try std.testing.expect(polls[0].c.next == &polls[2].c);
    try std.testing.expect(polls[2].c.prev == &polls[0].c);
}
