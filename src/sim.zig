// SPDX-FileCopyrightText: 2025 Lukáš Lalinský
// SPDX-License-Identifier: MIT
//
// Deterministic simulation (DST) support. Compiled in only when
// `zio_options.sim` is true. Production builds keep the option off, so these
// paths do not exist there.

const std = @import("std");
const zio_options = @import("zio_options");

pub const compiled_in = zio_options.sim;

pub const EventKind = enum(u8) {
    task_switch = 1,
    wake = 2,
    timer_fire = 3,
    cq_pop = 4,
};

const TaskIdEnt = struct { ptr: usize, id: u32 };

var begun: bool = false;
var clock_routed: bool = true;
var current_seed: u64 = 0;
var clock_ns: u64 = 0;
var rng: std.Random.DefaultPrng = undefined;
var hasher: std.hash.Wyhash = undefined;
var event_count: u64 = 0;
var next_task_id: u32 = 1;
var task_ids: [2048]TaskIdEnt = undefined;
var task_id_len: u32 = 0;

pub fn isBegun() bool {
    return begun;
}

pub fn seed() u64 {
    return current_seed;
}

pub fn begin(seed_value: u64) void {
    if (comptime !compiled_in) {
        @panic("sim: not compiled in (build zio with -Dsim=true)");
    }
    if (begun) @panic("sim: begin() called twice without end()");
    begun = true;
    clock_routed = true;
    current_seed = seed_value;
    clock_ns = 0;
    rng = std.Random.DefaultPrng.init(seed_value);
    hasher = std.hash.Wyhash.init(0);
    event_count = 0;
    next_task_id = 1;
    task_id_len = 0;
}

pub fn end() void {
    begun = false;
    clock_routed = true;
}

pub fn setClockRouted(routed: bool) void {
    clock_routed = routed;
}

/// Unix-scale offset for `.real`, matching the #717 witness: a `.real`
/// timestamp is ~1.7e18 ns and must not sit on the awake heap.
pub const real_epoch_ns: u64 = 1_700_000_000_000_000_000;

/// Logical time in nanoseconds. Panics if sim mode is on but the clock is
/// not routed here (the D4 trip).
pub fn nowNs() u64 {
    if (comptime !compiled_in) unreachable;
    if (!begun) @panic("sim: clock read before begin");
    if (!clock_routed) @panic("sim: real clock_gettime");
    return clock_ns;
}

/// Wall-clock now. `clock_idx` is `@intFromEnum(time.Clock)`. `.real` (2)
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
    if (!begun) @panic("sim: advance before begin");
    clock_ns +|= ns;
}

pub fn pickIndex(n: usize) usize {
    if (comptime !compiled_in) unreachable;
    if (!begun) @panic("sim: pick before begin");
    if (n == 0) @panic("sim: pickIndex(0)");
    if (n == 1) return 0;
    return rng.random().uintLessThan(usize, n);
}

pub fn taskId(ptr: usize) u32 {
    if (comptime !compiled_in) return 0;
    if (!begun) return 0;
    var i: u32 = 0;
    while (i < task_id_len) : (i += 1) {
        if (task_ids[i].ptr == ptr) return task_ids[i].id;
    }
    if (task_id_len >= task_ids.len) @panic("sim: task id table full");
    const id = next_task_id;
    next_task_id += 1;
    task_ids[task_id_len] = .{ .ptr = ptr, .id = id };
    task_id_len += 1;
    return id;
}

pub fn emit(kind: EventKind, a: u32, b: u32) void {
    if (comptime !compiled_in) return;
    if (!begun) return;
    var word: [8]u8 = undefined;
    word[0] = @intFromEnum(kind);
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

/// Final-state digest: clock, event count, assigned task ids. Does not
/// include the RNG stream.
pub fn stateDigest() u64 {
    var h = std.hash.Wyhash.init(1);
    var buf: [24]u8 = undefined;
    std.mem.writeInt(u64, buf[0..8], clock_ns, .little);
    std.mem.writeInt(u64, buf[8..16], event_count, .little);
    std.mem.writeInt(u32, buf[16..20], next_task_id, .little);
    std.mem.writeInt(u32, buf[20..24], task_id_len, .little);
    h.update(&buf);
    return h.final();
}

pub fn deadlock() noreturn {
    std.debug.panic(
        "sim: deadlock SEED={d} clock_ns={d} events={d}",
        .{ current_seed, clock_ns, event_count },
    );
}

pub fn forbidKernelFutex() void {
    if (comptime !compiled_in) return;
    if (!begun) return;
    std.debug.panic("sim: real kernel futex SEED={d}", .{current_seed});
}

pub fn printScope() void {
    std.debug.print("SCOPE simulated: clock, futex_park, task_pick, timer_heap, cq, executor_csprng, real_epoch\n", .{});
    std.debug.print("SCOPE real: kqueue_fd (Loop.init, never polled), allocator, libc\n", .{});
    std.debug.print("SCOPE unsimulated: io_uring, file_io, net_io, extra_os_threads, dns, boot_vs_awake (boot==awake)\n", .{});
}

pub fn mutantOmitTimeoutRecheck() bool {
    if (comptime !compiled_in) return false;
    return comptime std.mem.eql(u8, zio_options.sim_mutant, "omit_timeout_recheck");
}

pub fn mutantArmTimerStale() bool {
    if (comptime !compiled_in) return false;
    return comptime std.mem.eql(u8, zio_options.sim_mutant, "arm_timer_stale");
}
