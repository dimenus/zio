// SPDX-FileCopyrightText: 2025 Lukáš Lalinský
// SPDX-License-Identifier: MIT
//
// The op model of the sim's stream sockets: slice 2's gate (design 17.15
// item 2, without faults). Random ops on several connections against a
// reference model, under each profile, and the points where a run with no
// profile must panic.
//
// How it runs:
// - One sim run per seed, with a bare `ev.Loop` and no runtime, so every step
//   is exact: the test calls `Loop.add`, `Loop.cancel` and `Loop.poll` itself.
//   A timer far in the future keeps the loop from counting as done.
// - One listener; up to `max_live` connections at a time and
//   `max_seed_conns` in a seed, each made by an ev connect and an ev accept, then given a random
//   window per direction and a random chunk mode. A step is one of
//   `StepKind`: an ev send or receive on a random endpoint, a poll, a
//   shutdown (.send or .both), a close (plain, or with SO_LINGER {1, 0}), a
//   window or chunk mode change, a cancel of a parked op, a new
//   connection, or `owed_close`: two reads in one instant (the second's
//   credit owed) and then both closes, so the wire empties while a credit
//   is owed. A receive or a send lives in a slot until its callback.
// - Three seed blocks, each with its own step weights: seeds 1 to 16 are
//   slice 2's mix with no fault step; 17 to 32 add slice 3's fault steps
//   (a partition, a silence, a delay with jitter, a clear, `failStream`);
//   33 to 40 add two scripted steps, `train` and `late_credit` (above
//   `stepTrain`), which put the model into states the random steps reach
//   too rarely: a release or an abort right after a pull, a send after a
//   hold in the other direction, and an older credit held after a newer
//   one. A step added later gets weight 0 in the older blocks, so their
//   draws, steps and digest lines never change.
// - The parent runs every seed in one child per profile, twice, and
//   compares each seed's digest line between the two children (same seed,
//   two processes). Then, per seed, a child with no profile: it must panic
//   with the line, and at the step, that the profiled run's no-profile
//   shadow predicted (below), or finish if it predicted none.
//
// The model shares no helper with the sim. It keeps each connection's two
// endpoints (flags, unread bytes) and two directions (window, admitted,
// credited, read, reclaimed, discards by cause, credit pacing), and its own
// wire: every data chunk, FIN, RST and credit with its landing time. It cuts
// chunks with its own copy of the chunk-size draw (the doc comment of
// `chunkDraw` in sim.zig), lands an entry no earlier than the one sent
// before it in its direction, lands entries at one instant in (conn_id,
// direction, send order), departs owed credits after the landings of their
// instant in (conn_id, direction) order, and decides every call, harvest and
// landing by design 17.8's lists written as its own (`recvEval`,
// `sendEval`, `landData`, ...). The handshake is not modelled: a connection
// joins the model once its accept has completed, when nothing of it is on
// the wire.
//
// What it checks:
// - every op's result: the count, the error, and every byte a receive
//   returns (the model's own copy of what was sent). Apart from how the
//   wire and the held list order entries, every chunk that reaches a
//   receiver's buffer must be the next bytes of its stream (its offset
//   equals the bytes delivered before it), so a chunk out of order, or
//   one delivered after a discarded one, fails at its landing. A result decided at the
//   call must come back unchanged. An op that parked must not complete
//   until the model woke it; once woken it must complete in that poll, with
//   the model's result at its harvest, in the order the loop harvests.
// - after every step: for each connection, every counter of both directions
//   (`sim.testStreamDir`, conservation included) and the flags and unread
//   bytes of both endpoints (`sim.testConnEnd`) equal the model's; a
//   connection is retired exactly when the model says it can be; the
//   entries in flight (`sim.streamEntriesInFlight`) equal the model's; every
//   field of `sim.streamStats()` equals the model's own count (by
//   reflection, so a new field is compared from its first run); every
//   entry on the wire and every held entry is the model's by what it is
//   (kind, sequence, a credit's count, a chunk's length), so a sim that
//   kept the wrong entry is named at the step that kept it; a live
//   direction with nothing of its credit in flight, held or owed has been
//   credited all its receiver read or released (`checkCreditLive`, on the
//   sim's counts alone: a lost credit, which conservation cannot see);
//   and on each
//   open endpoint the layer's `bytesReadable` is its unread bytes and
//   `getpeername` its peer, or `SocketUnconnected` after a reset.
// - after every poll: the clock did not pass the model's next landing or
//   owed credit, nor the poll's cap.
// - at the end, with every rule cleared and the wire drained: no send is
//   parked while its receiver has room (a stranded sender); then each op
//   still parked is given what it waits for (a byte
//   from its peer, or one more byte of window) and must complete, one at a
//   time; then every socket closes, the wire drains, and every connection
//   retires.
// - counts: how many of each step ran and were skipped (by reason), and of
//   each outcome; the child fails if a required count is 0 over its seeds
//   (`floors`), so a run where a kind never happened does not count. A
//   floor counts its case where the case is decided, not a neighbour of it:
//   line 3a by the saved send serial, a stale credit at departure apart from
//   one at landing, a wake by op and by why (a shutdown by its kind).
//
// The no-profile shadow. Up to the first point where the kernels differ, a
// run with no profile does what a profiled run does, except one flag: under
// `.linux` a send after a FIN-then-RST consumes the pending error, which with
// no profile it does not. So the model keeps a second copy of that flag
// (`np_reported`) and, at each of 17.8's no-profile points, records the
// first step at which a run with no profile would panic and the row it would
// name. `.linux` and `.macos` must predict the same point for a seed.
//
// Rules the generator keeps, so that the model has one answer:
// - one unresolved receive and one unresolved send per endpoint;
// - no window change while the sender's send is woken and not harvested;
// - no op on an endpoint after its close; a cancel only of a parked op (not
//   woken), between polls;
// - the chunk mode `.fixed` only with windows of 16 KiB or less, so the TCP
//   entries stay far below `max_tcp_entries`;
// - the generator never reads the profile, so the runs with each profile
//   and with none take the same steps up to the first point that differs.
//
// What it does not reach: a fault on the handshake (17.9a point 15: the
// scenario tests have it); the listener and handshake rules (slice 1's
// tests); a cancel inside a harvest
// batch (the bytes tests have it); a window change while a send is woken
// (refused; a refusal test has it); the TCP entry limit (the bytes tests
// fill the wire on purpose; the generator keeps far below it).
//
// A failure prints `stream model: FAIL seed S step K: ...` and panics; the
// child prints `stream model: seed S step K` before each step, and the parent
// prints the last one. `ZIO_STREAM_MODEL_SEED=S` replays one seed,
// `ZIO_STREAM_MODEL_STEPS=N` sets the length, `ZIO_STREAM_MODEL_PROFILE` is
// `linux`, `macos` or `none`. Set for the parent, any of them makes a replay
// (`replay`): the parent passes them on to one child per profile asked for,
// and runs no floors, second process or no-profile runs.

const std = @import("std");
const builtin = @import("builtin");
const sim = @import("sim.zig");
const ev = @import("ev/root.zig");
const os_net = @import("os/net.zig");

const parent_name = "sim: stream op model: connections under random sends, receives, shutdowns, closes, window and chunk changes and cancels, per profile and with none";
const child_name = parent_name ++ ", in a child process";

const L = sim.wire_latency_ns;
const us: u64 = 1000;
const default_steps = 1500;
/// Seeds 1 to `n_seeds` are slice 2's mix, unchanged; seeds `n_seeds + 1`
/// to `2 * n_seeds` add slice 3's fault steps (`fault_weights`); the
/// `n_pull_seeds` after them add the pull steps (`pull_weights`).
const n_seeds = 16;
const n_pull_seeds = 8;
const all_seeds = 2 * n_seeds + n_pull_seeds;
const max_live = 4;
/// Connections made in a seed; the model's table holds those not retired.
const max_seed_conns = 80;
const max_conns = 16;
const max_slots = 2 * 2 * max_live;
const buf_cap = 6000;

const StepKind = enum { send, recv, poll, settle, shut_send, shut_both, close, abort, window, chunking, cancel, connect, owed_close, partition, silence, delay, clear_rule, fail, train, late_credit };
/// Slice 2's weights, with 0 for slice 3's steps: the same draws give the
/// same steps as before slice 3, so seeds 1 to 16 are unchanged. A step
/// added later gets 0 in the older tables, for the same reason: a 0 at the
/// end changes neither the total nor the walk, so seeds 1 to 32 keep their
/// steps and digests.
const step_weights = [_]u32{ 24, 22, 18, 6, 1, 1, 2, 1, 5, 2, 4, 6, 3, 0, 0, 0, 0, 0, 0, 0 };
const fault_weights = [_]u32{ 24, 22, 18, 6, 1, 1, 2, 1, 5, 2, 4, 6, 3, 3, 1, 2, 5, 1, 0, 0 };
/// The pull seeds: slice 3's mix, with the scripted `train` and
/// `late_credit` steps, and more clears, so that a rule rarely lives long.
const pull_weights = [_]u32{ 24, 22, 18, 6, 1, 1, 2, 1, 5, 2, 4, 6, 3, 3, 1, 2, 9, 1, 6, 3 };
/// Rules in force at once, and distinct rule keys per seed (the sim keeps
/// `max_rule_history`, 256).
const max_model_rules = 6;
const max_rule_keys = 200;

/// What a model op ends with.
const Want = union(enum) { n: usize, err: anyerror };

const OpKind = enum { recv, send };
const OpState = enum { decided, parked, woken, cancelled };

const Slot = struct {
    used: bool = false,
    kind: OpKind = .recv,
    state: OpState = .decided,
    serial: u32 = 0,
    ci: u8 = 0,
    side: u1 = 0,
    fd: i32 = 0,
    len: usize = 0,
    want: Want = .{ .n = 0 },
    /// A receive's expected bytes (the model's copy), when `want` is a count.
    want_bytes: [buf_cap]u8 = undefined,
    /// Why it was woken (`Counts.woken`), for the counts.
    wake: Wake = .none,
    recv: ev.NetRecv = undefined,
    send: ev.NetSend = undefined,
    buf: [buf_cap]u8 = undefined,
    riov: [1]os_net.iovec = undefined,
    siov: [1]os_net.iovec_const = undefined,
};

/// Why a parked op was woken. A shutdown's wake is split by its kind: a
/// receive is woken by its own `.both` only (row 19: not by `.send`), a send
/// by either (rows 24 and 16's `.both`).
const Wake = enum { none, data, fin, rst, local_reset, credit, grow, shut_send, shut_both, close, fail };
const n_wake = @typeInfo(Wake).@"enum".field_names.len;

const n_discard = @typeInfo(sim.Discard).@"enum".field_names.len;

const MEnd = struct {
    fd: i32 = -1,
    open: bool = false,
    fin_in: bool = false,
    fin_out: bool = false,
    rst_sent: bool = false,
    reset_received: bool = false,
    fin_before_reset: bool = false,
    reset_reported: bool = false,
    /// `reset_reported` as a run with no profile has it (the shadow).
    np_reported: bool = false,
    shut_rd: bool = false,
    linger0: bool = false,
    /// The serial of the send parked when the reset arrived.
    reset_parked_send: ?u32 = null,
    /// The local port, from `getsockname` at the connect: the peer's
    /// `getpeername` answer, and what the `.tcp` rules match.
    port: u16 = 0,
    /// `failStream`: how, and whether an op reported it.
    failed: ?sim.StreamFailure = null,
    fail_reported: bool = false,
    unread: std.ArrayList(u8) = .empty,
    recv: ?u8 = null,
    send: ?u8 = null,
};

const MDir = struct {
    window: u32 = sim.default_stream_window,
    admitted: u64 = 0,
    credited: u64 = 0,
    read: u64 = 0,
    reclaimed: u64 = 0,
    discarded: [n_discard]u64 = @splat(0),
    data_send_index: u64 = 0,
    last_credit_at: ?u64 = null,
    owed_at: ?u64 = null,
    /// Bytes of data sent (the next chunk's `off`), and bytes appended to
    /// the receiver's buffer.
    sent_off: u64 = 0,
    delivered: u64 = 0,

    fn room(d: MDir) u64 {
        const used = d.admitted - d.credited;
        return if (used >= d.window) 0 else d.window - used;
    }
};

const MConn = struct {
    used: bool = false,
    id: u32 = 0,
    ends: [2]MEnd = .{ .{}, .{} },
    dirs: [2]MDir = .{ .{}, .{} },
    chunking: sim.ChunkMode = .hashed,
    next_order: u64 = 0,
    /// Each direction's next sequence number: the client's SYN and ACK
    /// were 0 and 1, the server's SYN-ACK 0.
    next_seq: [2]u64 = .{ 2, 1 },
    /// Each direction's held entries, in send order (design 17.9).
    held: [2]std.ArrayList(MEntry) = .{ .empty, .empty },
    /// For the counts only, never for a prediction. `pulled_due`: the
    /// latest old landing time of the entries a pull took off the wire,
    /// until the direction's next release; `pull_aborted`: an abort
    /// discarded held entries since that pull. `hold_watch`: the other
    /// direction had an entry held at its landing while this one had
    /// entries on the wire, and this one has put nothing on the wire since.
    pulled_due: [2]?u64 = .{ null, null },
    pull_aborted: [2]bool = .{ false, false },
    hold_watch: [2]bool = .{ false, false },
};

const EKind = enum { data, fin, rst, credit };

/// A `.tcp` rule of the model. A partition and an impairment match
/// entries from port `from` to port `to`; a silence matches either.
const MRuleKind = enum { partition, silence, impairment };
const MRule = struct {
    kind: MRuleKind,
    from: u16,
    to: u16,
    delay: u64 = 0,
    jitter: u64 = 0,
    matched: u64 = 0,
    delayed: u64 = 0,
};

const MEntry = struct {
    due: u64,
    conn_id: u32,
    ci: u8,
    dir: u1,
    /// Send order across the run: within a direction, the sim's sequence
    /// order.
    order: u64,
    kind: EKind,
    bytes: []const u8 = &.{},
    count: u64 = 0,
    /// The sim's per-direction sequence: the jitter draw's key.
    seq: u64 = 0,
    /// Time after L, fixed at the send: chunk spacing, delay, jitter.
    extra: u64 = 0,
    /// A `.tcp` impairment added time to it.
    own_delay: bool = false,
    /// A data chunk's place in its direction's byte stream: the bytes of
    /// data sent before it.
    off: u64 = 0,
    /// It was held and released (`released_to_dead` counts at its landing).
    released: bool = false,
};

const Counts = struct {
    ran: [@typeInfo(StepKind).@"enum".field_names.len]u64 = @splat(0),
    skipped: [@typeInfo(StepKind).@"enum".field_names.len]u64 = @splat(0),
    /// Skips by reason.
    skip_no_endpoint: u64 = 0,
    skip_unresolved: u64 = 0,
    skip_no_parked: u64 = 0,
    skip_conn_limit: u64 = 0,
    skip_fixed_window: u64 = 0,
    skip_no_owed_shape: u64 = 0,
    skip_rule_limit: u64 = 0,
    skip_rule_exists: u64 = 0,
    skip_no_rule: u64 = 0,
    skip_no_fail_target: u64 = 0,
    skip_no_train_shape: u64 = 0,
    skip_no_late_credit_shape: u64 = 0,
    send_admitted: u64 = 0,
    send_partial: u64 = 0,
    send_zero: u64 = 0,
    send_parked: u64 = 0,
    send_epipe: u64 = 0,
    send_reset: u64 = 0,
    /// Line 3a ran: the harvest of the send whose serial the endpoint saved
    /// when the reset arrived (`reset_parked_send`), with no FIN first and
    /// after one.
    line3a_no_fin: u64 = 0,
    line3a_after_fin: u64 = 0,
    recv_data: u64 = 0,
    recv_eof: u64 = 0,
    recv_reset: u64 = 0,
    recv_parked: u64 = 0,
    closed_op: u64 = 0,
    cancel_recv: u64 = 0,
    cancel_send: u64 = 0,
    /// Wakes by op (`OpKind`: receive, send) and by why.
    woken: [2][n_wake]u64 = @splat(@splat(0)),
    shut_unconnected: u64 = 0,
    close_fin: u64 = 0,
    close_rst: u64 = 0,
    close_quiet: u64 = 0,
    rst_answers: u64 = 0,
    local_resets: u64 = 0,
    releases: u64 = 0,
    /// Credits for a terminal direction: suppressed at their departure, and
    /// dropped at their landing (design 17.7; `stale_credit` counts both).
    stale_departed: u64 = 0,
    stale_landed: u64 = 0,
    /// `owed_close` steps that ran: both ends closed in the instant a read
    /// left a credit owed, so the wire can empty while it is still owed.
    owed_close: u64 = 0,
    /// `getpeername` and `bytesReadable` (FIONREAD) on every open endpoint
    /// after every step; `peer_unconnected` is a reset endpoint's answer.
    peer_checks: u64 = 0,
    peer_unconnected: u64 = 0,
    readable_checks: u64 = 0,
    owed_departed: u64 = 0,
    credits_landed: u64 = 0,
    shrink_below_use: u64 = 0,
    shared_reset: u64 = 0,
    batches: u64 = 0,
    chunks: [3]u64 = @splat(0),
    end_proofs: u64 = 0,
    conns: u64 = 0,
    polls: u64 = 0,
    // Slice 3 (seeds 17 to 32), each counted where its case is decided.
    held_send: u64 = 0,
    held_landing: u64 = 0,
    released: u64 = 0,
    coalesced: u64 = 0,
    /// A credit that reached the held list after a newer credit of its
    /// direction was held (it left first, and was held at its landing):
    /// the case of the outside review of slice 3, first P2.
    older_credit_after_newer: u64 = 0,
    /// Entries taken off the wire behind one held at its landing.
    held_behind: u64 = 0,
    /// `checkCreditLive` ran on a direction with nothing of its credit in
    /// flight; the end's stranded-sender check ran on a parked send.
    live_checks: u64 = 0,
    end_live_checks: u64 = 0,
    /// A partition step that chose the direction of a credit landing more
    /// than L from now (`lateCredit`).
    partition_late_credit: u64 = 0,
    /// A FIN, an RST or a credit held (at its send or landing).
    held_fin: u64 = 0,
    held_rst: u64 = 0,
    held_credit: u64 = 0,
    /// Steps after which some data was held (conservation checked then).
    held_data_steps: u64 = 0,
    /// An abort that discarded held data, and a failStream that did; and
    /// each of them discarding a held credit.
    abort_discards: u64 = 0,
    fail_discards: u64 = 0,
    abort_held_credit: u64 = 0,
    fail_held_credit: u64 = 0,
    /// A close step on an endpoint whose outbound direction held entries.
    close_while_held: u64 = 0,
    /// A release whose direction held a credit while its data's sender had
    /// a send parked: the reverse partition that stalled a sender.
    stall_released: u64 = 0,
    /// A held entry with a delay of its own, released while a delay rule
    /// that matches its path is still in force.
    held_delayed_released: u64 = 0,
    /// An impairment cleared while an entry it delayed was on the wire.
    clear_delay_in_flight: u64 = 0,
    /// Released data that lands at an endpoint that closed meanwhile,
    /// counted at that landing.
    released_to_dead: u64 = 0,
    delayed: u64 = 0,
    jitter_draws: u64 = 0,
    /// failStream by what was parked on the endpoint, and in which order
    /// the two ops took the error when both were.
    fail_recv_parked: u64 = 0,
    fail_send_parked: u64 = 0,
    fail_both_parked: u64 = 0,
    fail_none_parked: u64 = 0,
    fail_first_recv: u64 = 0,
    fail_first_send: u64 = 0,
    /// failStream on an endpoint whose connection had entries in flight
    /// both ways.
    fail_both_ways: u64 = 0,
    fail_timed_out: u64 = 0,
    dropped_failed: u64 = 0,
    /// A rule's counts returned by its clear, compared with the model's.
    rule_clears: u64 = 0,
    // The pull seeds (t-4148, t-4154), each counted where its case is
    // decided.
    /// A release whose first entry's right landing time is before the old
    /// landing time of an entry the pull took off the wire: a fence kept
    /// from the pull would land it late. `pull_abort_release` is the same
    /// with an abort between the pull and the release (its RST is first).
    pull_release: u64 = 0,
    pull_abort_release: u64 = 0,
    /// The first entry a direction puts on the wire after the other
    /// direction had an entry held at its landing, when an entry of its own
    /// sent before that hold is still on the wire and lands after this
    /// one's own time: a fence reset for both directions at the hold would
    /// land it too early.
    clamp_after_other_hold: u64 = 0,
    /// The scripted steps that set their case up, by how they went on.
    train_pulls: u64 = 0,
    late_credit_setups: u64 = 0,

    fn add(a: *Counts, b: Counts) void {
        const info = @typeInfo(Counts).@"struct";
        inline for (info.field_names, info.field_types) |name, T| {
            switch (@typeInfo(T)) {
                .int => @field(a, name) += @field(b, name),
                .array => |arr| switch (@typeInfo(arr.child)) {
                    .int => for (&@field(a, name), @field(b, name)) |*x, y| {
                        x.* += y;
                    },
                    else => for (&@field(a, name), @field(b, name)) |*row, brow| {
                        for (row, brow) |*x, y| x.* += y;
                    },
                },
                else => {},
            }
        }
    }
};

const Profile = enum { linux, macos, none };

const M = struct {
    var seed: u64 = 0;
    var step: usize = 0;
    var rng: std.Random.DefaultPrng = undefined;
    var arena: std.heap.ArenaAllocator = undefined;
    var loop: ev.Loop = undefined;
    var keepalive: ev.Timer = undefined;
    var profile: Profile = .linux;
    var conns: [max_conns]MConn = @splat(.{});
    var n_conns: usize = 0;
    var listener: i32 = -1;
    var port: u16 = 0;
    var wire: std.ArrayList(MEntry) = .empty;
    var next_order: u64 = 0;
    var slots: [max_slots]Slot = undefined;
    var next_serial: u32 = 1;
    var model_clock: u64 = 0;
    var c: Counts = .{};
    /// Slice 3: the `.tcp` rules in force, oldest first; whether the seed
    /// ever set one; its distinct rule keys; the jitter draws.
    var rules: [max_model_rules]MRule = undefined;
    var n_rules: usize = 0;
    var rules_ever: bool = false;
    var rule_keys: usize = 0;
    var draws: u64 = 0;
    var faults: bool = false;
    /// A pull seed: `pull_weights`.
    var pull: bool = false;
    /// A failStream step found a receive and a send parked: the one
    /// harvested first is counted by which it was.
    var fail_both_now: bool = false;
    /// The run's `sim.streamStats()` as the model counts it: compared,
    /// field by field, after every step (`checkStats`).
    var st: sim.StreamStats = .{};
    var in_poll: bool = false;
    var batch: usize = 0;
    /// The no-profile shadow's first point: the step and the row text.
    var np_step: ?usize = null;
    var np_text: []const u8 = "";
    /// With no profile: the call point the model expects the sim to refuse.
    var expect_refusal: ?[]const u8 = null;
    /// A setup or end action in progress (no step's counts).
    var setup_done: [2]bool = .{ false, false };
    var keepalive_done: bool = false;
};

fn keepaliveDone(_: *ev.Loop, _: *ev.Completion) void {
    M.keepalive_done = true;
}

fn fail(comptime fmt: []const u8, args: anytype) noreturn {
    std.debug.print("stream model: FAIL seed {d} step {d}: " ++ fmt ++ "\n", .{ M.seed, M.step } ++ args);
    @panic("stream model: FAIL");
}

fn progress() void {
    var b: [64]u8 = undefined;
    const line = std.fmt.bufPrint(&b, "stream model: seed {d} step {d}\n", .{ M.seed, M.step }) catch unreachable;
    _ = std.c.write(2, line.ptr, line.len);
}

fn rand(n: usize) usize {
    return M.rng.random().uintLessThan(usize, n);
}

fn chance(pct: u32) bool {
    return M.rng.random().uintLessThan(u32, 100) < pct;
}

fn other(s: u1) u1 {
    return 1 - s;
}

fn now() u64 {
    return sim.clockNs();
}

// ---- The no-profile points (design 17.8) ----

/// A point where the kernels differ. Profiled: the shadow records it (the
/// first one only) and the profile answers. With no profile: a call point
/// is the sim's to refuse (`expect_refusal`, checked after the call); a
/// landing or harvest point means the sim should already have panicked.
fn profilePoint(comptime text: []const u8, at_call: bool) Profile {
    switch (M.profile) {
        .none => {
            if (!at_call) fail("the sim did not panic at a no-profile point it should have: {s}", .{text});
            std.debug.print("stream model: expect the sim to refuse: sim: {s}: the kernels differ here\n", .{text});
            M.expect_refusal = text;
            return .linux;
        },
        else => {
            shadowPoint(text);
            return M.profile;
        },
    }
}

fn shadowPoint(comptime text: []const u8) void {
    if (M.np_step != null) return;
    M.np_step = M.step;
    M.np_text = text;
}

fn report(e: *MEnd) void {
    e.reset_reported = true;
    e.np_reported = true;
}

// ---- The model's wire ----

fn entryBefore(a: MEntry, b: MEntry) bool {
    if (a.due != b.due) return a.due < b.due;
    if (a.conn_id != b.conn_id) return a.conn_id < b.conn_id;
    if (a.dir != b.dir) return a.dir < b.dir;
    return a.order < b.order;
}

fn sendEntry(ci: u8, from: u1, kind: EKind, extra: u64, bytes: []const u8, count: u64) void {
    const k = &M.conns[ci];
    var e: MEntry = .{ .due = 0, .conn_id = k.id, .ci = ci, .dir = from, .order = M.next_order, .kind = kind, .bytes = bytes, .count = count, .seq = k.next_seq[from], .extra = extra };
    M.next_order += 1;
    k.next_seq[from] += 1;
    if (kind == .data) {
        e.off = k.dirs[from].sent_off;
        k.dirs[from].sent_off += bytes.len;
    }
    if (M.rules_ever) {
        const fp, const tp = ports(k, from);
        for (M.rules[0..M.n_rules]) |*r| {
            if (ruleMatches(r.*, fp, tp)) r.matched += 1;
        }
        if (actingImpairment(fp, tp)) |r| {
            var add = r.delay;
            if (r.jitter != 0) {
                add += jitterDraw(k.id, from, e.seq) % (r.jitter + 1);
                M.draws += 1;
                M.c.jitter_draws += 1;
            }
            if (add > 0) {
                e.extra += add;
                e.own_delay = true;
                r.delayed += 1;
                M.st.delayed += 1;
                M.c.delayed += 1;
            }
        }
        if (k.held[from].items.len > 0 or blocked(fp, tp)) {
            M.st.held_at_send += 1;
            M.c.held_send += 1;
            hold(e);
            return;
        }
    }
    putOnWire(e);
}

/// Puts `e` on the model's wire. Its landing time is the design's rule,
/// read from what is on the wire, with no stored fence (sitting 34: the
/// model kept a `last_due` beside the sim's, the two went stale together
/// after a pull, and the model accepted the sim's wrong times): L plus its
/// extra time after now (17.7; 17.9 item 5 for a release), and not before
/// any entry of its direction that is really on the wire, which are all
/// sent before it.
fn putOnWire(entry: MEntry) void {
    var e = entry;
    e.due = now() + L + e.extra;
    const own = e.due;
    for (M.wire.items) |w| {
        if (w.ci != e.ci or w.dir != e.dir) continue;
        if (w.seq >= e.seq) fail("connection {d} direction {d}: sequence {d} goes on the wire behind sequence {d}, which is already there", .{ e.conn_id, e.dir, e.seq, w.seq });
        e.due = @max(e.due, w.due);
    }
    const k = &M.conns[e.ci];
    if (k.hold_watch[e.dir]) {
        // Every entry of the direction on the wire was put there before
        // the other direction's hold: this put is the first since.
        k.hold_watch[e.dir] = false;
        if (e.due > own) M.c.clamp_after_other_hold += 1;
    }
    var pos = M.wire.items.len;
    while (pos > 0 and entryBefore(e, M.wire.items[pos - 1])) pos -= 1;
    M.wire.insert(M.arena.allocator(), pos, e) catch @panic("oom");
}

// ---- Slice 3: the model's own `.tcp` rules (design 17.9, 17.9a) ----

/// The ports an entry of direction `from` goes between: the sender's and
/// the receiver's (the model's endpoints are all on 127.0.0.1).
fn ports(k: *const MConn, from: u1) struct { u16, u16 } {
    return .{ k.ends[from].port, k.ends[other(from)].port };
}

fn ruleMatches(r: MRule, fp: u16, tp: u16) bool {
    return switch (r.kind) {
        .partition, .impairment => r.from == fp and r.to == tp,
        .silence => r.from == fp or r.from == tp,
    };
}

fn blocked(fp: u16, tp: u16) bool {
    for (M.rules[0..M.n_rules]) |r| {
        if (r.kind != .impairment and ruleMatches(r, fp, tp)) return true;
    }
    return false;
}

/// The impairment set last among those that match.
fn actingImpairment(fp: u16, tp: u16) ?*MRule {
    var i = M.n_rules;
    while (i > 0) {
        i -= 1;
        if (M.rules[i].kind == .impairment and ruleMatches(M.rules[i], fp, tp)) return &M.rules[i];
    }
    return null;
}

/// The model's own copy of the jitter draw (the doc comment of
/// `tcpJitterDraw` in sim.zig).
fn jitterDraw(conn_id: u32, dir: u1, seq: u64) u64 {
    var key: [16]u8 = @splat(0);
    std.mem.writeInt(u64, key[0..8], M.seed, .little);
    var m: [19]u8 = undefined;
    std.mem.writeInt(u32, m[0..4], conn_id, .little);
    m[4] = dir;
    std.mem.writeInt(u64, m[5..13], seq, .little);
    @memcpy(m[13..19], "jitter");
    return std.hash.SipHash64(2, 4).toInt(&m, &key);
}

/// Holds `e` behind its direction's held entries. Two held credits of a
/// direction coalesce to the one that carries the larger count (design
/// 17.9a point 8): a credit's count is the receiver's cumulative read, so
/// the larger one says everything the smaller one says. Which of the two
/// came to the list first does not matter: a credit held at its landing
/// left before one held at its send.
fn hold(e: MEntry) void {
    const list = &M.conns[e.ci].held[e.dir];
    if (e.kind == .credit) {
        M.c.held_credit += 1;
        for (list.items, 0..) |h, i| {
            if (h.kind != .credit) continue;
            if (h.count == e.count) fail("two held credits of connection {d} carry the same count {d}", .{ e.conn_id, e.count });
            M.st.credits_coalesced += 1;
            M.c.coalesced += 1;
            if (h.count > e.count) {
                // The arriving one is older: it goes, the held one stays.
                M.c.older_credit_after_newer += 1;
                return;
            }
            _ = list.orderedRemove(i);
            break;
        }
    }
    if (e.kind == .fin) M.c.held_fin += 1;
    if (e.kind == .rst) M.c.held_rst += 1;
    // In sequence order: an entry held at its landing goes before one sent
    // later and held at its send.
    var pos = list.items.len;
    while (pos > 0 and list.items[pos - 1].seq > e.seq) pos -= 1;
    list.insert(M.arena.allocator(), pos, e) catch @panic("oom");
}

/// At a landing: held if a partition or silence matches now, or its
/// direction holds entries. A drop rule that matches counts it.
fn holdAtLanding(w: MEntry) bool {
    const k = &M.conns[w.ci];
    const fp, const tp = ports(k, w.dir);
    var matched = false;
    for (M.rules[0..M.n_rules]) |*r| {
        if (r.kind != .impairment and ruleMatches(r.*, fp, tp)) {
            r.matched += 1;
            matched = true;
        }
    }
    if (!matched and k.held[w.dir].items.len == 0) return false;
    M.st.held_at_landing += 1;
    M.c.held_landing += 1;
    hold(w);
    // Design 17.9 item 2: once a direction holds an entry, every later
    // entry of it is held behind it, matching or not. Those still on the
    // wire are later (an earlier one would have landed first), so they are
    // held now.
    var i: usize = 0;
    while (i < M.wire.items.len) {
        const x = M.wire.items[i];
        if (x.ci != w.ci or x.dir != w.dir) {
            i += 1;
            continue;
        }
        _ = M.wire.orderedRemove(i);
        M.st.held_behind += 1;
        M.c.held_behind += 1;
        k.pulled_due[w.dir] = @max(k.pulled_due[w.dir] orelse 0, x.due);
        hold(x);
    }
    for (M.wire.items) |x| {
        if (x.ci == w.ci and x.dir == other(w.dir)) {
            k.hold_watch[other(w.dir)] = true;
            break;
        }
    }
    return true;
}

/// After a partition or silence is cleared: every held direction that no
/// drop rule matches now goes on the wire, in (conn_id, direction) order,
/// each entry L plus its extra time after now, in order.
fn release() void {
    var order: [max_conns]u8 = undefined;
    var n: usize = 0;
    for (M.conns, 0..) |k, ci| {
        if (!k.used or k.held[0].items.len + k.held[1].items.len == 0) continue;
        var pos = n;
        while (pos > 0 and M.conns[order[pos - 1]].id > k.id) : (pos -= 1) order[pos] = order[pos - 1];
        order[pos] = @intCast(ci);
        n += 1;
    }
    for (order[0..n]) |ci| {
        const k = &M.conns[ci];
        for (0..2) |dd| {
            const d: u1 = @intCast(dd);
            if (k.held[d].items.len == 0) continue;
            const fp, const tp = ports(k, d);
            if (blocked(fp, tp)) continue;
            // The case a fence kept from a pull gets wrong (t-4148): the
            // first entry's right time, by the design's rule, is before a
            // pulled entry's old time.
            if (k.pulled_due[d]) |old| {
                const h0 = k.held[d].items[0];
                var right = now() + L + h0.extra;
                for (M.wire.items) |w| {
                    if (w.ci == ci and w.dir == d) right = @max(right, w.due);
                }
                if (right < old) {
                    if (k.pull_aborted[d] and h0.kind == .rst) M.c.pull_abort_release += 1 else M.c.pull_release += 1;
                }
                k.pulled_due[d] = null;
                k.pull_aborted[d] = false;
            }
            var has_credit = false;
            // A delay rule that still matches does not stop the release,
            // and the entry keeps the delay drawn at its send (17.9 item
            // 4): counted only while such a rule is in force now.
            const delay_now = actingImpairment(fp, tp) != null;
            for (k.held[d].items) |h| {
                if (h.kind == .credit) has_credit = true;
                if (h.own_delay and delay_now) M.c.held_delayed_released += 1;
                var x = h;
                x.released = true;
                putOnWire(x);
                M.st.released += 1;
                M.c.released += 1;
            }
            if (has_credit) {
                if (k.ends[other(d)].send) |i| {
                    if (M.slots[i].state == .parked) M.c.stall_released += 1;
                }
            }
            k.held[d].clearRetainingCapacity();
        }
    }
}

/// What `discardHeldOutbound` discarded, by kind: the floors count held
/// data and held credits at an abort or a failStream apart, so a FIN alone
/// does not stand in for them.
const HeldDiscard = struct { data: usize = 0, credit: usize = 0 };

/// Side `side`'s held data, FIN and credits go: an abort or a failStream.
fn discardHeldOutbound(ci: u8, side: u1, cause: sim.Discard) HeldDiscard {
    const list = &M.conns[ci].held[side];
    if (cause == .abort and list.items.len > 0 and M.conns[ci].pulled_due[side] != null) M.conns[ci].pull_aborted[side] = true;
    var n: HeldDiscard = .{};
    var i: usize = 0;
    while (i < list.items.len) {
        const h = list.items[i];
        if (h.kind == .rst) {
            i += 1;
            continue;
        }
        _ = list.orderedRemove(i);
        if (h.kind == .data) {
            discard(ci, side, cause, h.bytes.len);
            n.data += 1;
        }
        if (h.kind == .credit) n.credit += 1;
        M.st.discarded_held += 1;
    }
    return n;
}

/// An abort's held discards: data (17.15's "a reset with held data") and a
/// credit (17.9a point 9), each its own count.
fn countAbort(n: HeldDiscard) void {
    if (n.data > 0) M.c.abort_discards += 1;
    if (n.credit > 0) M.c.abort_held_credit += 1;
}

/// The next time something happens on the model's wire: a landing or an
/// owed credit's departure.
fn nextEvent() ?u64 {
    var t: ?u64 = if (M.wire.items.len > 0) M.wire.items[0].due else null;
    for (M.conns) |k| {
        if (!k.used) continue;
        for (k.dirs) |d| {
            const o = d.owed_at orelse continue;
            if (t == null or o < t.?) t = o;
        }
    }
    return t;
}

/// Lands and departs everything up to `t`, time by time, as the sim's polls
/// do: the landings of an instant in order, then its owed credits.
fn advanceTo(t: u64) void {
    while (nextEvent()) |e| {
        if (e > t) break;
        if (e < M.model_clock) fail("the model's wire has an event at {d} ns, before its clock {d} ns", .{ e, M.model_clock });
        M.model_clock = e;
        while (M.wire.items.len > 0 and M.wire.items[0].due == e) {
            const w = M.wire.orderedRemove(0);
            if (M.rules_ever and holdAtLanding(w)) continue;
            land(w);
        }
        departOwed(e);
    }
    M.model_clock = t;
}

fn departOwed(t: u64) void {
    while (true) {
        var best: ?u8 = null;
        var best_d: u1 = 0;
        for (&M.conns, 0..) |*k, ci| {
            if (!k.used) continue;
            for (k.dirs, 0..) |d, di| {
                const o = d.owed_at orelse continue;
                if (o > t) continue;
                if (best == null or k.id < M.conns[best.?].id) {
                    best = @intCast(ci);
                    best_d = @intCast(di);
                }
            }
        }
        const ci = best orelse return;
        M.c.owed_departed += 1;
        departCredit(ci, best_d);
    }
}

fn dirTerminal(k: *const MConn, data: u1) bool {
    const x = k.ends[data];
    return !x.open or x.reset_received or x.failed != null;
}

fn departCredit(ci: u8, data: u1) void {
    const k = &M.conns[ci];
    const d = &k.dirs[data];
    if (dirTerminal(k, data)) {
        d.owed_at = null;
        M.c.stale_departed += 1;
        M.st.stale_credit += 1;
        return;
    }
    d.owed_at = null;
    d.last_credit_at = now();
    sendEntry(ci, other(data), .credit, 0, &.{}, d.read + d.reclaimed);
}

/// A read or a release of `data` owes a credit (design 17.7).
fn creditOwed(ci: u8, data: u1) void {
    const d = &M.conns[ci].dirs[data];
    if (d.owed_at != null) return;
    if (d.last_credit_at) |t| {
        if (now() < t + L) {
            d.owed_at = t + L;
            return;
        }
    }
    departCredit(ci, data);
}

/// The chunk-size draw, the model's own copy of the formula in the doc
/// comment of `chunkDraw` in sim.zig.
fn chunkDraw(conn_id: u32, dir: u1, send_index: u64, k: u64) u64 {
    var key: [16]u8 = @splat(0);
    std.mem.writeInt(u64, key[0..8], M.seed, .little);
    var m: [26]u8 = undefined;
    std.mem.writeInt(u32, m[0..4], conn_id, .little);
    m[4] = dir;
    std.mem.writeInt(u64, m[5..13], send_index, .little);
    std.mem.writeInt(u64, m[13..21], k, .little);
    @memcpy(m[21..26], "chunk");
    return std.hash.SipHash64(2, 4).toInt(&m, &key);
}

fn admit(ci: u8, side: u1, bytes: []const u8) void {
    const k = &M.conns[ci];
    const d = &k.dirs[side];
    M.c.chunks[
        switch (k.chunking) {
            .hashed => 0,
            .fixed => 1,
            .off => 2,
        }
    ] += 1;
    var rest = bytes.len;
    var off: usize = 0;
    var idx: u64 = 0;
    while (rest > 0) : (idx += 1) {
        const want: u64 = switch (k.chunking) {
            .off => rest,
            .fixed => |n| n,
            .hashed => blk: {
                const h = chunkDraw(k.id, side, d.data_send_index, idx);
                break :blk if (h & 3 == 0) 1 + (h >> 2) % 16 else 512 + (h >> 2) % 3585;
            },
        };
        const size: usize = @intCast(@min(want, rest));
        const copy = M.arena.allocator().dupe(u8, bytes[off..][0..size]) catch @panic("oom");
        sendEntry(ci, side, .data, idx * us, copy, 0);
        off += size;
        rest -= size;
    }
    d.admitted += bytes.len;
    d.data_send_index += 1;
}

fn discard(ci: u8, data: u1, cause: sim.Discard, n: usize) void {
    M.conns[ci].dirs[data].discarded[@backingInt(cause)] += n;
    M.st.discarded[@backingInt(cause)] += n;
}

fn discardUnread(ci: u8, side: u1, cause: sim.Discard) usize {
    const e = &M.conns[ci].ends[side];
    const n = e.unread.items.len;
    discard(ci, other(side), cause, n);
    e.unread.clearRetainingCapacity();
    return n;
}

fn wakeRecv(ci: u8, side: u1, why: Wake) void {
    const e = &M.conns[ci].ends[side];
    const i = e.recv orelse return;
    if (M.slots[i].state != .parked) return;
    M.slots[i].state = .woken;
    M.slots[i].wake = why;
}

fn wakeSend(ci: u8, side: u1, why: Wake) void {
    const e = &M.conns[ci].ends[side];
    const i = e.send orelse return;
    if (M.slots[i].state != .parked) return;
    M.slots[i].state = .woken;
    M.slots[i].wake = why;
}

fn answerRst(ci: u8, at: u1) void {
    const x = &M.conns[ci].ends[at];
    if (x.rst_sent) return;
    x.rst_sent = true;
    M.c.rst_answers += 1;
    sendEntry(ci, at, .rst, 0, &.{}, 0);
}

fn parkedSendSerial(x: *const MEnd) ?u32 {
    const i = x.send orelse return null;
    return if (M.slots[i].state == .parked) M.slots[i].serial else null;
}

fn land(w: MEntry) void {
    const ci = w.ci;
    const at = other(w.dir);
    const k = &M.conns[ci];
    const x = &k.ends[at];
    switch (w.kind) {
        .data => {
            const n = w.bytes.len;
            if (x.failed != null) {
                M.st.dropped_failed += 1;
                M.c.dropped_failed += 1;
                return discard(ci, w.dir, .failed_receiver, n);
            }
            if (x.reset_received) {
                M.st.dropped_after_reset += 1;
                return discard(ci, w.dir, .after_reset, n);
            }
            if (!x.open) {
                M.st.dropped_dead += 1;
                if (w.released) M.c.released_to_dead += 1;
                answerRst(ci, at);
                return discard(ci, w.dir, .closed_receiver, n);
            }
            if (x.shut_rd) {
                const p = profilePoint("row 18, a data chunk that lands after the endpoint's own shutdown(.both)", false);
                answerRst(ci, at);
                discard(ci, w.dir, .shut_receiver, n);
                if (p == .linux) {
                    x.reset_received = true;
                    x.fin_before_reset = false;
                    x.reset_parked_send = parkedSendSerial(x);
                    countAbort(discardHeldOutbound(ci, at, .abort));
                    M.c.local_resets += 1;
                    M.st.local_resets += 1;
                    wakeRecv(ci, at, .local_reset);
                    wakeSend(ci, at, .local_reset);
                }
                return;
            }
            // The stream's own rule, apart from how the wire and the held
            // list order entries: a chunk that reaches the buffer is the
            // next bytes of the stream. A chunk out of order, or one after
            // a chunk that was discarded (a hole), fails here.
            const dd = &k.dirs[w.dir];
            if (w.off != dd.delivered) fail("connection {d} direction from side {d}: a chunk of {d} bytes at stream offset {d} reached the buffer, but {d} bytes were delivered before it", .{ k.id, w.dir, n, w.off, dd.delivered });
            dd.delivered += n;
            x.unread.appendSlice(M.arena.allocator(), w.bytes) catch @panic("oom");
            M.st.chunks_landed += 1;
            wakeRecv(ci, at, .data);
        },
        .fin => {
            // Failed, then dead, then reset (design 17.8, the FIN landing
            // list).
            if (x.failed != null) {
                M.st.dropped_failed += 1;
                M.c.dropped_failed += 1;
                return;
            }
            if (!x.open) {
                M.st.dropped_dead += 1;
                return;
            }
            if (x.reset_received) {
                M.st.dropped_after_reset += 1;
                return;
            }
            x.fin_in = true;
            wakeRecv(ci, at, .fin);
        },
        .rst => {
            // Failed, then reset, then dead (design 17.8, the RST landing
            // list).
            if (x.failed != null) {
                M.st.dropped_failed += 1;
                M.c.dropped_failed += 1;
                return;
            }
            if (x.reset_received) {
                M.st.dropped_after_reset += 1;
                return;
            }
            if (!x.open) {
                M.st.dropped_dead += 1;
                return;
            }
            x.reset_received = true;
            x.fin_before_reset = x.fin_in;
            x.reset_parked_send = parkedSendSerial(x);
            countAbort(discardHeldOutbound(ci, at, .abort));
            const both = x.recv != null and x.send != null and M.slots[x.recv.?].state == .parked and M.slots[x.send.?].state == .parked;
            if (both) M.c.shared_reset += 1;
            wakeRecv(ci, at, .rst);
            wakeSend(ci, at, .rst);
        },
        .credit => {
            if (dirTerminal(k, at)) {
                M.c.stale_landed += 1;
                M.st.stale_credit += 1;
                return;
            }
            const d = &k.dirs[at];
            if (w.count <= d.credited) fail("the model's own credit of {d} on connection {d} landed after one of {d}", .{ w.count, k.id, d.credited });
            d.credited = w.count;
            M.c.credits_landed += 1;
            M.st.credits_landed += 1;
            if (d.room() > 0) wakeSend(ci, at, .credit);
        },
    }
}

// ---- 17.8's evaluations, as the model's own ----

fn recvEval(ci: u8, side: u1, s: *Slot) ?Want {
    const k = &M.conns[ci];
    const e = &k.ends[side];
    if (e.failed) |how| {
        if (e.fail_reported) return .{ .n = 0 };
        e.fail_reported = true;
        if (s.state == .woken and M.fail_both_now) {
            M.c.fail_first_recv += 1;
            M.fail_both_now = false;
        }
        return .{ .err = failErr(how) };
    }
    if (e.unread.items.len > 0) {
        const n = @min(s.len, e.unread.items.len);
        @memcpy(s.want_bytes[0..n], e.unread.items[0..n]);
        e.unread.replaceRange(M.arena.allocator(), 0, n, &.{}) catch @panic("oom");
        k.dirs[other(side)].read += n;
        creditOwed(ci, other(side));
        return .{ .n = n };
    }
    if (e.reset_received) {
        // The shadow first: a run with no profile has its own flag.
        if (!e.np_reported and e.fin_before_reset) shadowPoint("rows 8 and 22, a read that meets a reset after a FIN had landed");
        if (!e.reset_reported) {
            if (!e.fin_before_reset) {
                report(e);
                return .{ .err = error.ConnectionResetByPeer };
            }
            switch (profilePointNoShadow("rows 8 and 22, a read that meets a reset after a FIN had landed", s.state == .decided)) {
                .linux, .none => {},
                .macos => {
                    report(e);
                    return .{ .err = error.ConnectionResetByPeer };
                },
            }
        }
    }
    if (e.fin_in or e.shut_rd or e.reset_received) return .{ .n = 0 };
    return null;
}

/// `profilePoint` without the shadow: for the read point, where the shadow
/// decides from its own flag, which the profiled branch does not read.
fn profilePointNoShadow(comptime text: []const u8, at_call: bool) Profile {
    if (M.profile == .none) return profilePoint(text, at_call);
    return M.profile;
}

fn failErr(how: sim.StreamFailure) anyerror {
    return switch (how) {
        .reset => error.ConnectionResetByPeer,
        .timed_out => error.ConnectionTimedOut,
    };
}

fn sendEval(ci: u8, side: u1, s: *Slot, bytes: []const u8) ?Want {
    const k = &M.conns[ci];
    const e = &k.ends[side];
    if (e.failed) |how| {
        if (e.fail_reported) return .{ .err = error.BrokenPipe };
        e.fail_reported = true;
        if (s.state == .woken and M.fail_both_now) {
            M.c.fail_first_send += 1;
            M.fail_both_now = false;
        }
        return .{ .err = failErr(how) };
    }
    const at_call = s.state == .decided;
    if (e.reset_received) {
        const parked = !at_call and e.reset_parked_send == s.serial;
        // The shadow.
        if (!e.np_reported) {
            if (parked) {
                if (e.fin_before_reset) shadowPoint("row 41, a send parked when a reset arrives after a FIN");
            } else if (!e.fin_before_reset) shadowPoint("rows 26b, 36 and 39, a send that meets a reset with no FIN before it");
        }
        if (e.reset_reported) return .{ .err = error.BrokenPipe };
        if (parked) {
            if (!e.fin_before_reset) {
                M.c.line3a_no_fin += 1;
                report(e);
                return .{ .err = error.ConnectionResetByPeer };
            }
            const p = profilePointNoShadow("row 41, a send parked when a reset arrives after a FIN", false);
            M.c.line3a_after_fin += 1;
            report(e);
            return .{ .err = if (p == .linux) error.BrokenPipe else error.ConnectionResetByPeer };
        }
        if (e.fin_before_reset) {
            if (M.profile == .linux) e.reset_reported = true;
            return .{ .err = error.BrokenPipe };
        }
        switch (profilePointNoShadow("rows 26b, 36 and 39, a send that meets a reset with no FIN before it", at_call)) {
            .linux, .none => {
                report(e);
                return .{ .err = error.ConnectionResetByPeer };
            },
            .macos => return .{ .err = error.BrokenPipe },
        }
    }
    if (e.fin_out) return .{ .err = error.BrokenPipe };
    const room = k.dirs[side].room();
    if (room == 0) return null;
    const n: usize = @intCast(@min(room, bytes.len));
    admit(ci, side, bytes[0..n]);
    return .{ .n = n };
}

// ---- Sim calls ----

fn sockAddr(port: u16) os_net.sockaddr.in {
    return .{ .port = std.mem.nativeToBig(u16, port), .addr = @bitCast([4]u8{ 127, 0, 0, 1 }) };
}

fn setupDone(_: *ev.Loop, _: *ev.Completion) void {
    M.setup_done[0] = true;
}

fn connectDone(_: *ev.Loop, _: *ev.Completion) void {
    M.setup_done[1] = true;
}

/// Runs a setup, shutdown or close op to its end, polling as a step does,
/// at most `max_polls` polls of `cap_ns`.
fn runOp(comp: *ev.Completion, cap_ns: u64, max_polls: usize) void {
    M.setup_done[0] = false;
    comp.callback = setupDone;
    comp.flags.defer_callback = false;
    M.loop.add(comp);
    var n: usize = 0;
    while (!M.setup_done[0]) : (n += 1) {
        if (n >= max_polls) fail("a {t} op did not complete in {d} polls", .{ comp.op, max_polls });
        pollOnce(cap_ns);
    }
}

fn openListener() void {
    var op = ev.NetOpen.init(.ipv4, .stream, .ip, .{ .nonblocking = true });
    runOp(&op.c, 50 * us, 100);
    M.listener = op.getResult() catch |e| fail("listener open: {t}", .{e});
    var a = sockAddr(0);
    var alen: os_net.socklen_t = @sizeOf(os_net.sockaddr.in);
    var b = ev.NetBind.init(M.listener, @ptrCast(&a), &alen);
    runOp(&b.c, 50 * us, 100);
    b.getResult() catch |e| fail("listener bind: {t}", .{e});
    M.port = std.mem.bigToNative(u16, a.port);
    var l = ev.NetListen.init(M.listener, 64);
    runOp(&l.c, 50 * us, 100);
    l.getResult() catch |e| fail("listen: {t}", .{e});
}

fn usedConns() usize {
    var n: usize = 0;
    for (M.conns) |k| n += @intFromBool(k.used);
    return n;
}

fn liveConns() usize {
    var n: usize = 0;
    for (M.conns) |k| {
        if (k.used and (k.ends[0].open or k.ends[1].open)) n += 1;
    }
    return n;
}

fn connect() bool {
    if (M.n_conns >= max_seed_conns or liveConns() >= max_live or usedConns() >= max_conns) {
        M.c.skip_conn_limit += 1;
        return false;
    }
    var o = ev.NetOpen.init(.ipv4, .stream, .ip, .{ .nonblocking = true });
    runOp(&o.c, 50 * us, 100);
    const cfd = o.getResult() catch |e| fail("open: {t}", .{e});
    var to = sockAddr(M.port);
    var cop = ev.NetConnect.init(cfd, @ptrCast(&to), @sizeOf(os_net.sockaddr.in));
    M.setup_done[1] = false;
    cop.c.callback = connectDone;
    cop.c.flags.defer_callback = false;
    M.loop.add(&cop.c);
    var aop = ev.NetAccept.init(M.listener, null, null);
    // Each poll stops at the next landing, and a release can put hundreds
    // of entries on the wire 1 µs apart: the handshake (3L) is bounded by
    // time, not by a count of polls.
    runOp(&aop.c, 50 * us, 100_000);
    const sfd = aop.getResult() catch |e| fail("accept: {t}", .{e});
    if (!M.setup_done[1]) fail("the connect did not complete before its accept", .{});
    cop.getResult() catch |e| fail("connect: {t}", .{e});
    var ci: u8 = 0;
    while (M.conns[ci].used) ci += 1;
    M.n_conns += 1;
    M.c.conns += 1;
    const k = &M.conns[ci];
    k.* = .{ .used = true, .id = @intCast(M.n_conns) };
    var ca: os_net.sockaddr.in = undefined;
    var calen: os_net.socklen_t = @sizeOf(os_net.sockaddr.in);
    os_net.getsockname(cfd, @ptrCast(&ca), &calen) catch |e| fail("getsockname: {t}", .{e});
    k.ends[0] = .{ .fd = cfd, .open = true, .port = std.mem.bigToNative(u16, ca.port) };
    k.ends[1] = .{ .fd = sfd, .open = true, .port = M.port };
    // Windows: tiny, small, medium or the default; the chunk mode by them.
    for (0..2) |side| {
        const w: u32 = switch (rand(4)) {
            0 => @intCast(1 + rand(16)),
            1 => @intCast(17 + rand(4096)),
            2 => @intCast(4097 + rand(12288)),
            else => sim.default_stream_window,
        };
        // The window toward `ends[side]`: data from the other side.
        sim.setStreamWindow(k.ends[side].fd, w);
        k.dirs[other(@intCast(side))].window = w;
    }
    const mode = randomChunking(ci) orelse sim.ChunkMode.hashed;
    sim.setStreamChunking(cfd, mode);
    k.chunking = mode;
    return true;
}

fn randomChunking(ci: u8) ?sim.ChunkMode {
    const k = &M.conns[ci];
    return switch (rand(3)) {
        0 => .hashed,
        1 => if (k.dirs[0].window <= 16384 and k.dirs[1].window <= 16384) .{ .fixed = @intCast(4 + rand(700)) } else null,
        else => .off,
    };
}

/// A random open endpoint, or null.
fn pickEnd() ?struct { u8, u1 } {
    var cand: [2 * max_conns]struct { u8, u1 } = undefined;
    var n: usize = 0;
    for (M.conns, 0..) |k, ci| {
        if (!k.used) continue;
        for (k.ends, 0..) |e, s| {
            if (e.open) {
                cand[n] = .{ @intCast(ci), @intCast(s) };
                n += 1;
            }
        }
    }
    if (n == 0) return null;
    return cand[rand(n)];
}

fn freeSlot() ?u8 {
    for (&M.slots, 0..) |*s, i| {
        if (!s.used) return @intCast(i);
    }
    return null;
}

fn onDone(_: *ev.Loop, comp: *ev.Completion) void {
    for (&M.slots, 0..) |*s, i| {
        if (!s.used) continue;
        const mine = switch (s.kind) {
            .recv => comp == &s.recv.c,
            .send => comp == &s.send.c,
        };
        if (mine) return complete(@intCast(i));
    }
    fail("a callback for an op the model does not have", .{});
}

fn complete(i: u8) void {
    const s = &M.slots[i];
    if (M.in_poll) {
        advanceTo(now());
        M.batch += 1;
    }
    const k = &M.conns[s.ci];
    const e = &k.ends[s.side];
    switch (s.state) {
        .parked => fail("{t} op #{d} completed though the model never woke it", .{ s.kind, s.serial }),
        .cancelled => s.want = .{ .err = error.Canceled },
        .decided => {},
        .woken => {
            M.c.woken[@backingInt(s.kind)][@backingInt(s.wake)] += 1;
            if (!e.open or e.fd != s.fd) {
                s.want = .{ .err = error.FileDescriptorNotASocket };
                M.c.closed_op += 1;
            } else {
                const w = switch (s.kind) {
                    .recv => recvEval(s.ci, s.side, s),
                    .send => sendEval(s.ci, s.side, s, s.buf[0..s.len]),
                };
                s.want = w orelse fail("woken {t} op #{d} has no result at its harvest in the model", .{ s.kind, s.serial });
                countResult(s);
            }
        },
    }
    const got: anyerror!usize = switch (s.kind) {
        .recv => s.recv.getResult(),
        .send => s.send.getResult(),
    };
    switch (s.want) {
        .n => |n| {
            const g = got catch |err| fail("{t} op #{d} on connection {d}: want {d}, got {t}", .{ s.kind, s.serial, k.id, n, err });
            if (g != n) fail("{t} op #{d} on connection {d}: want {d}, got {d}", .{ s.kind, s.serial, k.id, n, g });
            if (s.kind == .recv and !std.mem.eql(u8, s.want_bytes[0..n], s.buf[0..n])) fail("receive op #{d} on connection {d}: the bytes differ from the model's", .{ s.serial, k.id });
        },
        .err => |want_err| {
            if (got) |g| fail("{t} op #{d} on connection {d}: want {t}, got {d}", .{ s.kind, s.serial, k.id, want_err, g }) else |err| {
                if (err != want_err) fail("{t} op #{d} on connection {d}: want {t}, got {t}", .{ s.kind, s.serial, k.id, want_err, err });
            }
        },
    }
    // The endpoint's op is resolved (the fd may hold another endpoint now).
    if (e.fd == s.fd) {
        switch (s.kind) {
            .recv => if (e.recv == i) {
                e.recv = null;
            },
            .send => if (e.send == i) {
                e.send = null;
            },
        }
    }
    s.used = false;
}

fn countResult(s: *const Slot) void {
    switch (s.want) {
        .n => |n| switch (s.kind) {
            .recv => if (n == 0) {
                M.c.recv_eof += 1;
            } else {
                M.c.recv_data += 1;
            },
            .send => if (n == 0) {
                M.c.send_zero += 1;
            } else {
                M.c.send_admitted += 1;
                if (n < s.len) M.c.send_partial += 1;
            },
        },
        .err => |err| switch (s.kind) {
            .recv => if (err == error.ConnectionResetByPeer) {
                M.c.recv_reset += 1;
            },
            .send => if (err == error.BrokenPipe) {
                M.c.send_epipe += 1;
            } else if (err == error.ConnectionResetByPeer) {
                M.c.send_reset += 1;
            },
        },
    }
}

/// Submits a receive or a send on (`ci`, `side`) with the model's call-time
/// result decided first.
fn submit(kind: OpKind, ci: u8, side: u1, len: usize) void {
    const i = freeSlot() orelse fail("the slot pool is full", .{});
    const s = &M.slots[i];
    const e = &M.conns[ci].ends[side];
    s.* = .{ .used = true, .kind = kind, .serial = M.next_serial, .ci = ci, .side = side, .fd = e.fd, .len = len };
    M.next_serial += 1;
    switch (kind) {
        .recv => {
            s.recv = ev.NetRecv.init(e.fd, .fromSlices(&.{s.buf[0..len]}, &s.riov), .{});
            s.recv.c.callback = onDone;
            s.recv.c.flags.defer_callback = false;
        },
        .send => {
            for (s.buf[0..len], 0..) |*b, j| b.* = @truncate(s.serial *% 131 +% j *% 7);
            s.send = ev.NetSend.init(e.fd, .fromSlices(&.{s.buf[0..len]}, &s.siov), .{});
            s.send.c.callback = onDone;
            s.send.c.flags.defer_callback = false;
        },
    }
    // The call list: a send of 0 is 0; else the evaluation.
    const w: ?Want = switch (kind) {
        .recv => recvEval(ci, side, s),
        .send => if (len == 0) Want{ .n = 0 } else sendEval(ci, side, s, s.buf[0..len]),
    };
    if (w) |r| {
        s.want = r;
        s.state = .decided;
        countResult(s);
    } else {
        s.state = .parked;
        switch (kind) {
            .recv => M.c.recv_parked += 1,
            .send => M.c.send_parked += 1,
        }
    }
    if (len > 0 or kind == .recv) switch (kind) {
        .recv => e.recv = i,
        .send => e.send = i,
    };
    M.loop.add(switch (kind) {
        .recv => &s.recv.c,
        .send => &s.send.c,
    });
    if (M.expect_refusal) |t| fail("the sim did not refuse a call it should have: {s}", .{t});
}

// ---- Steps ----

fn stepSend() bool {
    const ci, const side = pickEnd() orelse return skip(.no_endpoint);
    if (M.conns[ci].ends[side].send != null) return skip(.unresolved);
    const len: usize = switch (rand(10)) {
        0 => 0,
        1, 2, 3 => 1 + rand(16),
        4, 5, 6 => 17 + rand(1500),
        else => 1517 + rand(buf_cap - 1517),
    };
    submit(.send, ci, side, len);
    return true;
}

fn stepRecv() bool {
    const ci, const side = pickEnd() orelse return skip(.no_endpoint);
    if (M.conns[ci].ends[side].recv != null) return skip(.unresolved);
    const len: usize = switch (rand(3)) {
        0 => 1 + rand(16),
        1 => 17 + rand(2000),
        else => buf_cap,
    };
    submit(.recv, ci, side, len);
    return true;
}

const SkipWhy = enum { no_endpoint, unresolved, no_parked, fixed_window, no_owed_shape, rule_limit, rule_exists, no_rule, no_fail_target, no_train_shape, no_late_credit_shape };

fn skip(why: SkipWhy) bool {
    switch (why) {
        .no_endpoint => M.c.skip_no_endpoint += 1,
        .unresolved => M.c.skip_unresolved += 1,
        .no_parked => M.c.skip_no_parked += 1,
        .fixed_window => M.c.skip_fixed_window += 1,
        .no_owed_shape => M.c.skip_no_owed_shape += 1,
        .rule_limit => M.c.skip_rule_limit += 1,
        .rule_exists => M.c.skip_rule_exists += 1,
        .no_rule => M.c.skip_no_rule += 1,
        .no_fail_target => M.c.skip_no_fail_target += 1,
        .no_train_shape => M.c.skip_no_train_shape += 1,
        .no_late_credit_shape => M.c.skip_no_late_credit_shape += 1,
    }
    return false;
}

// ---- Slice 3's steps (seeds 17 to 32) ----
//
// Rules name one connection: a partition or an impairment its two ports in
// one direction, a silence its client's port. A new connection's handshake
// has a new client port and so matches none (design 17.9a, point 15).

fn tcpEp(port: u16) sim.Endpoint {
    return .{ .ip = .{ 127, 0, 0, 1 }, .port = port, .protocol = .tcp };
}

fn tcpPath(fp: u16, tp: u16) sim.Path {
    return .{ .from = tcpEp(fp), .to = tcpEp(tp) };
}

/// A connection with an end still open.
fn pickConn() ?u8 {
    var cand: [max_conns]u8 = undefined;
    var n: usize = 0;
    for (M.conns, 0..) |k, ci| {
        if (k.used and (k.ends[0].open or k.ends[1].open)) {
            cand[n] = @intCast(ci);
            n += 1;
        }
    }
    if (n == 0) return null;
    return cand[rand(n)];
}

fn findModelRule(kind: MRuleKind, from: u16, to: u16) ?usize {
    for (M.rules[0..M.n_rules], 0..) |r, i| {
        if (r.kind == kind and r.from == from and r.to == to) return i;
    }
    return null;
}

fn addModelRule(r: MRule) void {
    M.rules[M.n_rules] = r;
    M.n_rules += 1;
    M.rules_ever = true;
    M.rule_keys += 1;
}

fn ruleRoom() bool {
    return M.n_rules < max_model_rules and M.rule_keys < max_rule_keys;
}

/// A credit on the wire that lands more than L from now (a delay rule on
/// its direction): a partition of its direction now lets a newer credit be
/// held at its send before this one lands and is held (the outside review
/// of slice 3, first P2: `older_credit_after_newer`).
fn lateCredit() ?MEntry {
    for (M.wire.items) |w| {
        if (w.kind == .credit and w.due > now() + L) return w;
    }
    return null;
}

fn stepPartition() bool {
    if (!ruleRoom()) return skip(.rule_limit);
    if (chance(50)) {
        if (lateCredit()) |w| {
            const fp, const tp = ports(&M.conns[w.ci], w.dir);
            if (findModelRule(.partition, fp, tp) == null) {
                sim.setPartition(tcpPath(fp, tp));
                addModelRule(.{ .kind = .partition, .from = fp, .to = tp });
                M.c.partition_late_credit += 1;
                return true;
            }
        }
    }
    const ci = pickConn() orelse return skip(.no_endpoint);
    const fp, const tp = ports(&M.conns[ci], @intCast(rand(2)));
    if (findModelRule(.partition, fp, tp) != null) return skip(.rule_exists);
    sim.setPartition(tcpPath(fp, tp));
    addModelRule(.{ .kind = .partition, .from = fp, .to = tp });
    return true;
}

fn stepSilence() bool {
    if (!ruleRoom()) return skip(.rule_limit);
    const ci = pickConn() orelse return skip(.no_endpoint);
    const port = M.conns[ci].ends[0].port;
    if (findModelRule(.silence, port, 0) != null) return skip(.rule_exists);
    sim.silence(tcpEp(port));
    addModelRule(.{ .kind = .silence, .from = port, .to = 0 });
    return true;
}

fn stepDelay() bool {
    if (!ruleRoom()) return skip(.rule_limit);
    const ci = pickConn() orelse return skip(.no_endpoint);
    const fp, const tp = ports(&M.conns[ci], @intCast(rand(2)));
    var delay: u64 = if (chance(70)) (1 + rand(300)) * us else 0;
    const jit: u64 = if (chance(60)) (1 + rand(200)) * us else 0;
    if (delay == 0 and jit == 0) delay = 50 * us;
    sim.setImpairment(tcpPath(fp, tp), .{ .delay_ns = delay, .jitter_ns = jit });
    // The sim replaces one already there, which becomes the newest.
    if (findModelRule(.impairment, fp, tp)) |i| removeModelRule(i);
    addModelRule(.{ .kind = .impairment, .from = fp, .to = tp, .delay = delay, .jitter = jit });
    return true;
}

fn removeModelRule(i: usize) void {
    std.mem.copyForwards(MRule, M.rules[i .. M.n_rules - 1], M.rules[i + 1 .. M.n_rules]);
    M.n_rules -= 1;
}

/// Clears rule `i` in the sim and the model, checks the counts the sim
/// returns against the model's, and releases what it held.
fn clearRule(i: usize) void {
    const r = M.rules[i];
    const st = switch (r.kind) {
        .partition => sim.healPartition(tcpPath(r.from, r.to)),
        .silence => sim.unsilence(tcpEp(r.from)),
        .impairment => sim.clearImpairment(tcpPath(r.from, r.to)),
    };
    if (st.matched != r.matched or st.delayed != r.delayed) fail("cleared {t} rule {d} -> {d}: sim matched {d} delayed {d}, model matched {d} delayed {d}", .{ r.kind, r.from, r.to, st.matched, st.delayed, r.matched, r.delayed });
    M.c.rule_clears += 1;
    if (r.kind == .impairment) {
        for (M.wire.items) |w| {
            const k = &M.conns[w.ci];
            const fp, const tp = ports(k, w.dir);
            if (w.own_delay and fp == r.from and tp == r.to) {
                M.c.clear_delay_in_flight += 1;
                break;
            }
        }
    }
    removeModelRule(i);
    if (r.kind != .impairment) release();
}

fn stepClearRule() bool {
    if (M.n_rules == 0) return skip(.no_rule);
    clearRule(rand(M.n_rules));
    return true;
}

fn stepFail() bool {
    var cand: [2 * max_conns]struct { u8, u1 } = undefined;
    var n: usize = 0;
    for (M.conns, 0..) |k, ci| {
        if (!k.used) continue;
        for (k.ends, 0..) |e, side| {
            if (e.open and e.failed == null and !e.reset_received) {
                cand[n] = .{ @intCast(ci), @intCast(side) };
                n += 1;
            }
        }
    }
    if (n == 0) return skip(.no_fail_target);
    const ci, const side = cand[rand(n)];
    const k = &M.conns[ci];
    const e = &k.ends[side];
    const how: sim.StreamFailure = if (chance(50)) .reset else .timed_out;
    if (how == .timed_out) M.c.fail_timed_out += 1;
    const rp = if (e.recv) |i| M.slots[i].state == .parked else false;
    const sp = if (e.send) |i| M.slots[i].state == .parked else false;
    if (rp and sp) {
        M.c.fail_both_parked += 1;
        M.fail_both_now = true;
    } else if (rp) M.c.fail_recv_parked += 1 else if (sp) M.c.fail_send_parked += 1 else M.c.fail_none_parked += 1;
    var ways = [2]bool{ false, false };
    for (M.wire.items) |w| {
        if (w.ci == ci) ways[w.dir] = true;
    }
    if (ways[0] and ways[1]) M.c.fail_both_ways += 1;
    // The model's table (design 17.9).
    e.failed = how;
    const fd_n = discardHeldOutbound(ci, side, .failed_sender);
    if (fd_n.data > 0) M.c.fail_discards += 1;
    if (fd_n.credit > 0) M.c.fail_held_credit += 1;
    const din = &k.dirs[other(side)];
    if (din.owed_at != null) {
        din.owed_at = null;
        M.st.stale_credit += 1;
    }
    _ = discardUnread(ci, side, .failed_receiver);
    wakeRecv(ci, side, .fail);
    wakeSend(ci, side, .fail);
    sim.failStream(e.fd, how);
    pollOnce(0);
    M.fail_both_now = false;
    return true;
}

// ---- The pull seeds' scripted steps (t-4148, t-4154) ----
//
// Each step puts the model into a state that the random steps reach too
// rarely to test (sitting 34 measured 0 releases right after a pull over
// seeds 17 to 32), then stops: what follows is either one scripted action
// or, often, whatever the random steps do next. The steps decide nothing
// themselves: every call goes through the same `submit`, `clearRule` and
// `closeSide` as the random steps, so the model's checks run unchanged.

/// Side `side` of `k` can start a send: open, no FIN out, no reset, not
/// failed, and no unresolved send.
fn canSend(k: *const MConn, side: u1) bool {
    const e = k.ends[side];
    return e.open and !e.fin_out and !e.reset_received and !e.rst_sent and e.failed == null and e.send == null;
}

/// No model rule matches either direction of `k`, and neither holds.
fn quietConn(k: *const MConn) bool {
    for (0..2) |dd| {
        const d: u1 = @intCast(dd);
        const fp, const tp = ports(k, d);
        for (M.rules[0..M.n_rules]) |r| {
            if (ruleMatches(r, fp, tp)) return false;
        }
        if (k.held[d].items.len > 0) return false;
    }
    return true;
}

/// Polls until `done` holds or the clock reaches `t` (bounded by time, as
/// a poll stops at every landing).
fn pollUntil(t: u64, comptime done: fn () bool) void {
    var n: usize = 0;
    while (!done() and now() < t) : (n += 1) {
        if (n > 100_000) fail("pollUntil did not reach {d} ns", .{t});
        pollOnce(t - now());
    }
}

var train_ci: u8 = 0;
var train_dir: u1 = 0;
fn trainHeld() bool {
    return M.conns[train_ci].held[train_dir].items.len > 0;
}

/// A long train of small fixed chunks in one direction, a partition of
/// that direction at once, and a poll to the train's first landing: the
/// first chunk is held and the rest pulled (design 17.9 item 2). Half the
/// time the other direction first gets a train of its own, under a delay
/// rule half of those times. Then one of: the clear soon after (a release
/// right after a pull), an abort of the sender and the clear, a send in
/// the other direction with its delay rule cleared, or nothing.
fn stepTrain() bool {
    if (M.n_rules + 2 > max_model_rules or M.rule_keys + 2 > max_rule_keys) return skip(.rule_limit);
    var cand: [2 * max_conns]struct { u8, u1 } = undefined;
    var n: usize = 0;
    for (&M.conns, 0..) |*k, ci| {
        if (!k.used or !k.ends[0].open or !k.ends[1].open or !quietConn(k)) continue;
        for (0..2) |side| {
            const d: u1 = @intCast(side);
            if (!canSend(k, d)) continue;
            // A window change needs the other sender's send not woken.
            if (k.ends[other(d)].send) |i| {
                if (M.slots[i].state == .woken) continue;
            }
            cand[n] = .{ @intCast(ci), d };
            n += 1;
        }
    }
    if (n == 0) return skip(.no_train_shape);
    const ci, const d = cand[rand(n)];
    const k = &M.conns[ci];
    const r = other(d);
    // `.fixed` needs both windows at 16 KiB or less; a long train needs
    // room in d's.
    for (0..2) |side| {
        const x: u1 = @intCast(side);
        const w = k.dirs[other(x)].window;
        if (w < 4097 or w > 16384) applyWindow(ci, x, @intCast(4097 + rand(12288)));
    }
    const chunk: usize = 1 + rand(7);
    const mode: sim.ChunkMode = .{ .fixed = @intCast(chunk) };
    sim.setStreamChunking(k.ends[0].fd, mode);
    k.chunking = mode;
    const then = rand(4);
    const fp, const tp = ports(k, d);
    var reverse_delay = false;
    if (chance(50) and canSend(k, r)) {
        if (chance(50)) {
            const delay = (150 + rand(250)) * us;
            sim.setImpairment(tcpPath(tp, fp), .{ .delay_ns = delay, .jitter_ns = 0 });
            addModelRule(.{ .kind = .impairment, .from = tp, .to = fp, .delay = delay, .jitter = 0 });
            reverse_delay = true;
        }
        submit(.send, ci, r, 1 + rand(600));
    }
    // 150 to 649 chunks: more than L / 1 µs, so the train's tail lands
    // more than L after its head, and a release at the pull is before it.
    submit(.send, ci, d, @min(buf_cap, chunk * (150 + rand(500))));
    pollOnce(0);
    sim.setPartition(tcpPath(fp, tp));
    addModelRule(.{ .kind = .partition, .from = fp, .to = tp });
    train_ci = ci;
    train_dir = d;
    pollUntil(now() + 2 * L, trainHeld);
    if (trainHeld()) M.c.train_pulls += 1;
    switch (then) {
        0 => {
            if (chance(50)) pollOnce(rand(60) * us);
            if (findModelRule(.partition, fp, tp)) |i| clearRule(i);
        },
        1 => {
            if (k.ends[d].open) closeSide(ci, d, true);
            if (findModelRule(.partition, fp, tp)) |i| clearRule(i);
        },
        2 => {
            if (reverse_delay) {
                if (findModelRule(.impairment, tp, fp)) |i| clearRule(i);
            }
            if (canSend(k, r)) submit(.send, ci, r, 1 + rand(16));
            pollOnce(0);
        },
        else => {},
    }
    return true;
}

fn never() bool {
    return false;
}

/// An older credit that reaches the held list after a newer one (the
/// outside review of slice 3, first P2). The receiver x gets two bytes or
/// more (its peer sends them when it has none), reads one, and the credit
/// leaves under a delay of more than 2L; the credit's direction is
/// partitioned; one L later x reads again, and that newer credit is held
/// at its send; the older one is held at its landing after it.
fn stepLateCredit() bool {
    if (M.n_rules + 2 > max_model_rules or M.rule_keys + 2 > max_rule_keys) return skip(.rule_limit);
    var cand: [2 * max_conns]struct { u8, u1 } = undefined;
    var n: usize = 0;
    for (&M.conns, 0..) |*k, ci| {
        if (!k.used or !k.ends[0].open or !k.ends[1].open or !quietConn(k)) continue;
        for (0..2) |side| {
            const x: u1 = @intCast(side);
            const e = &k.ends[x];
            if (e.recv != null or e.shut_rd or e.reset_received or e.failed != null or dirTerminal(k, other(x))) continue;
            if (e.unread.items.len < 2 and !(canSend(k, other(x)) and k.dirs[other(x)].room() >= 2)) continue;
            cand[n] = .{ @intCast(ci), x };
            n += 1;
        }
    }
    if (n == 0) return skip(.no_late_credit_shape);
    const ci, const x = cand[rand(n)];
    const k = &M.conns[ci];
    lc_ci = ci;
    lc_x = x;
    if (k.ends[x].unread.items.len < 2) {
        submit(.send, ci, other(x), 2 + rand(15));
        pollOnce(0);
        pollUntil(now() + 2 * L + 20 * us, lcReady);
    }
    // The pacing: a credit may leave at once.
    const dd = &k.dirs[other(x)];
    if (dd.owed_at) |t| pollUntil(t, lcOwedGone);
    if (dd.last_credit_at) |t| pollUntil(t + L, never);
    if (!lcReady() or dd.owed_at != null or !k.ends[other(x)].open or dirTerminal(k, other(x))) return true;
    // Credits for the data toward x leave from x.
    const fp, const tp = ports(k, x);
    const delay = (2 * L / us + 50 + rand(200)) * us;
    sim.setImpairment(tcpPath(fp, tp), .{ .delay_ns = delay, .jitter_ns = 0 });
    addModelRule(.{ .kind = .impairment, .from = fp, .to = tp, .delay = delay, .jitter = 0 });
    submit(.recv, ci, x, 1);
    pollOnce(0);
    sim.setPartition(tcpPath(fp, tp));
    addModelRule(.{ .kind = .partition, .from = fp, .to = tp });
    pollUntil(now() + L, never);
    const e = &k.ends[x];
    if (e.open and e.recv == null and e.unread.items.len > 0) {
        submit(.recv, ci, x, 1);
        pollOnce(0);
        M.c.late_credit_setups += 1;
    }
    return true;
}

var lc_ci: u8 = 0;
var lc_x: u1 = 0;
/// x is open, with no receive, and two bytes or more unread.
fn lcReady() bool {
    const e = &M.conns[lc_ci].ends[lc_x];
    return e.open and e.recv == null and !e.shut_rd and e.unread.items.len >= 2;
}
fn lcOwedGone() bool {
    return M.conns[lc_ci].dirs[other(lc_x)].owed_at == null;
}

fn shutdown(how: os_net.ShutdownHow) bool {
    const ci, const side = pickEnd() orelse return skip(.no_endpoint);
    const k = &M.conns[ci];
    const e = &k.ends[side];
    // The model's list first (design 17.8, shutdown).
    var want_err = false;
    if (e.reset_received or e.failed != null) {
        want_err = true;
        M.c.shut_unconnected += 1;
    } else {
        const shut_rd = how == .both and !e.shut_rd;
        const discards = shut_rd and e.unread.items.len > 0;
        const p: Profile = if (discards) profilePoint("row 17, shutdown(.both) with unread bytes", true) else M.profile;
        const why: Wake = if (how == .both) .shut_both else .shut_send;
        if (!e.fin_out) {
            e.fin_out = true;
            sendEntry(ci, side, .fin, 0, &.{}, 0);
            wakeSend(ci, side, why);
        }
        if (shut_rd) {
            e.shut_rd = true;
            wakeRecv(ci, side, why);
            if (discards and p == .macos) {
                const n = discardUnread(ci, side, .both_discard);
                k.dirs[other(side)].reclaimed += n;
                M.c.releases += 1;
                creditOwed(ci, other(side));
            }
        }
    }
    var op = ev.NetShutdown.init(e.fd, how);
    M.setup_done[0] = false;
    op.c.callback = setupDone;
    op.c.flags.defer_callback = false;
    M.loop.add(&op.c);
    if (M.expect_refusal) |t| fail("the sim did not refuse a call it should have: {s}", .{t});
    pollOnce(0);
    if (!M.setup_done[0]) fail("a shutdown did not complete in a poll of 0", .{});
    if (op.getResult()) |_| {
        if (want_err) fail("shutdown({t}) on connection {d}: want SocketUnconnected, got ok", .{ how, k.id });
    } else |err| {
        if (!want_err or err != error.SocketUnconnected) fail("shutdown({t}) on connection {d}: got {t}", .{ how, k.id, err });
    }
    return true;
}

fn close(abortive: bool) bool {
    const ci, const side = pickEnd() orelse return skip(.no_endpoint);
    closeSide(ci, side, abortive);
    return true;
}

/// A close step on (`ci`, `side`), which is open.
fn closeSide(ci: u8, side: u1, abortive: bool) void {
    const k = &M.conns[ci];
    const e = &k.ends[side];
    if (abortive) {
        const lg: [2]c_int = .{ 1, 0 };
        os_net.setsockopt(e.fd, os_net.SOL.SOCKET, os_net.SO.LINGER, std.mem.asBytes(&lg)) catch |err| fail("SO_LINGER: {t}", .{err});
        e.linger0 = true;
    }
    // The model's close list (design 17.8).
    if (k.held[side].items.len > 0) M.c.close_while_held += 1;
    const quiet = e.reset_received or e.rst_sent or e.failed != null;
    const had_unread = e.unread.items.len > 0;
    const rst = !quiet and (had_unread or e.linger0);
    const fin = !quiet and !rst and !e.fin_out;
    _ = discardUnread(ci, side, .unread_at_close);
    if (rst) {
        e.rst_sent = true;
        M.c.close_rst += 1;
        countAbort(discardHeldOutbound(ci, side, .abort));
        sendEntry(ci, side, .rst, 0, &.{}, 0);
    } else if (fin) {
        e.fin_out = true;
        M.c.close_fin += 1;
        sendEntry(ci, side, .fin, 0, &.{}, 0);
    } else M.c.close_quiet += 1;
    e.open = false;
    wakeRecv(ci, side, .close);
    wakeSend(ci, side, .close);
    var op = ev.NetClose.init(e.fd);
    runOp(&op.c, 0, 1);
    op.getResult() catch |err| fail("close: {t}", .{err});
}

fn stepWindow() bool {
    const ci, const side = pickEnd() orelse return skip(.no_endpoint);
    const k = &M.conns[ci];
    // Refused while the sender's send is woken and not yet harvested (a
    // refusal test has that case).
    if (k.ends[other(side)].send) |i| {
        if (M.slots[i].state == .woken) return skip(.unresolved);
    }
    const w: u32 = switch (rand(4)) {
        0 => @intCast(1 + rand(16)),
        1 => @intCast(17 + rand(4096)),
        2 => @intCast(4097 + rand(12288)),
        else => if (k.chunking == .fixed) return skip(.fixed_window) else sim.default_stream_window,
    };
    applyWindow(ci, side, w);
    return true;
}

/// Sets the window toward (`ci`, `side`): its sender's send is not woken.
fn applyWindow(ci: u8, side: u1, w: u32) void {
    const k = &M.conns[ci];
    const d = &k.dirs[other(side)];
    sim.setStreamWindow(k.ends[side].fd, w);
    const used = d.admitted - d.credited;
    if (used > w) M.c.shrink_below_use += 1;
    d.window = w;
    if (k.ends[other(side)].open and d.room() > 0) wakeSend(ci, other(side), .grow);
}

fn stepChunking() bool {
    const ci, const side = pickEnd() orelse return skip(.no_endpoint);
    const mode = randomChunking(ci) orelse return skip(.fixed_window);
    sim.setStreamChunking(M.conns[ci].ends[side].fd, mode);
    M.conns[ci].chunking = mode;
    return true;
}

/// Both ends closed in the instant a read leaves a credit owed (t-4130,
/// mutant S2-20). A read whose credit leaves now, a second read in the same
/// instant, whose credit is owed for one L later, then both closes. The
/// first credit and the closes' FIN or RST land at that later instant too,
/// so the connection's wire empties while its credit is still owed: the
/// connection may retire only after the credit departs (stale, as its
/// sender is dead), never at those landings.
fn stepOwedClose() bool {
    var cand: [2 * max_conns]struct { u8, u1 } = undefined;
    var n: usize = 0;
    for (&M.conns, 0..) |*k, ci| {
        if (!k.used or !k.ends[0].open or !k.ends[1].open) continue;
        for (0..2) |side| {
            const x: u1 = @intCast(side);
            const e = &k.ends[x];
            const d = k.dirs[other(x)];
            if (e.unread.items.len < 2 or e.recv != null or dirTerminal(k, other(x))) continue;
            if (d.owed_at != null) continue;
            if (d.last_credit_at) |lt| {
                if (now() < lt + L) continue;
            }
            cand[n] = .{ @intCast(ci), x };
            n += 1;
        }
    }
    if (n == 0) return skip(.no_owed_shape);
    const ci, const x = cand[rand(n)];
    const k = &M.conns[ci];
    for (0..2) |_| {
        submit(.recv, ci, x, 1);
        pollOnce(0);
    }
    const t = now();
    if (k.dirs[other(x)].owed_at != t + L or k.dirs[other(x)].last_credit_at != t) fail("owed_close on connection {d}: the second read's credit is not owed for {d} ns", .{ k.id, t + L });
    closeEnd(k, &k.ends[x]);
    closeEnd(k, &k.ends[other(x)]);
    M.c.owed_close += 1;
    return true;
}

fn stepCancel() bool {
    var cand: [max_slots]u8 = undefined;
    var n: usize = 0;
    for (&M.slots, 0..) |*s, i| {
        if (s.used and s.state == .parked) {
            cand[n] = @intCast(i);
            n += 1;
        }
    }
    if (n == 0) return skip(.no_parked);
    const i = cand[rand(n)];
    const s = &M.slots[i];
    const e = &M.conns[s.ci].ends[s.side];
    s.state = .cancelled;
    switch (s.kind) {
        .recv => {
            M.c.cancel_recv += 1;
            e.recv = null;
            M.loop.cancel(&s.recv.c);
        },
        .send => {
            M.c.cancel_send += 1;
            e.send = null;
            M.loop.cancel(&s.send.c);
        },
    }
    pollOnce(0);
    if (M.slots[i].used and M.slots[i].serial == s.serial) fail("a cancelled {t} op #{d} did not complete in a poll of 0", .{ s.kind, s.serial });
    return true;
}

/// One poll: the clock moves by at most its cap and at most to the model's
/// next event. Every woken op must complete in it.
fn pollOnce(cap_ns: u64) void {
    advanceTo(now());
    const before = now();
    const next = nextEvent();
    M.in_poll = true;
    M.batch = 0;
    M.loop.poll(.fromNanoseconds(cap_ns)) catch |e| fail("poll: {t}", .{e});
    M.in_poll = false;
    M.c.polls += 1;
    if (M.batch > 1) M.c.batches += 1;
    const after = now();
    if (after < before) fail("a poll moved the clock back, from {d} ns to {d} ns", .{ before, after });
    if (after - before > cap_ns) fail("a poll capped at {d} ns moved the clock {d} ns", .{ cap_ns, after - before });
    if (next) |t| {
        if (t >= before and after > t) fail("a poll moved the clock to {d} ns, past the model's next event at {d} ns", .{ after, t });
    }
    advanceTo(after);
    for (&M.slots) |*s| {
        if (s.used and (s.state == .woken or s.state == .decided or s.state == .cancelled)) {
            // An op decided at the call, or woken by now, is harvested in
            // this poll; one woken by a landing at `after` too.
            fail("{t} op #{d} ({t}) did not complete in the poll", .{ s.kind, s.serial, s.state });
        }
    }
}

fn settle() void {
    const t0 = now();
    const span = (1 + rand(5)) * L;
    var n: usize = 0;
    while (now() < t0 + span) : (n += 1) {
        if (n > 10000) fail("settle did not move the clock", .{});
        pollOnce(t0 + span - now());
    }
}

fn runStep(kind: StepKind) bool {
    return switch (kind) {
        .send => stepSend(),
        .recv => stepRecv(),
        .poll => blk: {
            pollOnce(switch (rand(3)) {
                0 => 0,
                1 => 1 + rand(50) * us,
                else => 100 * us + rand(200) * us,
            });
            break :blk true;
        },
        .settle => blk: {
            settle();
            break :blk true;
        },
        .shut_send => shutdown(.send),
        .shut_both => shutdown(.both),
        .close => close(false),
        .abort => close(true),
        .window => stepWindow(),
        .chunking => stepChunking(),
        .cancel => stepCancel(),
        .connect => connect(),
        .owed_close => stepOwedClose(),
        .partition => stepPartition(),
        .silence => stepSilence(),
        .delay => stepDelay(),
        .clear_rule => stepClearRule(),
        .fail => stepFail(),
        .train => stepTrain(),
        .late_credit => stepLateCredit(),
    };
}

fn pickStep() StepKind {
    const weights = if (M.pull) &pull_weights else if (M.faults) &fault_weights else &step_weights;
    var total: u32 = 0;
    for (weights) |w| total += w;
    var r = M.rng.random().uintLessThan(u32, total);
    for (weights, 0..) |w, i| {
        if (r < w) return @fromBackingInt(@as(u5, @intCast(i)));
        r -= w;
    }
    unreachable;
}

// ---- Checks ----

fn retirable(k: *const MConn, ci: usize) bool {
    if (k.ends[0].open or k.ends[1].open) return false;
    for (M.wire.items) |w| {
        if (w.ci == ci) return false;
    }
    if (k.held[0].items.len + k.held[1].items.len != 0) return false;
    return k.dirs[0].owed_at == null and k.dirs[1].owed_at == null;
}

fn checkState() void {
    var in_flight: usize = M.wire.items.len;
    var held_data = false;
    defer if (held_data) {
        M.c.held_data_steps += 1;
    };
    for (&M.conns, 0..) |*k, ci| {
        if (!k.used) continue;
        if (retirable(k, ci)) {
            if (sim.testConnEnd(k.id, false) != null) fail("connection {d} is not retired, and the model has nothing of it left", .{k.id});
            for (&k.ends) |*e| e.unread.deinit(M.arena.allocator());
            k.used = false;
            M.st.retired += 1;
            continue;
        }
        for (k.dirs) |d| in_flight += @intFromBool(d.owed_at != null);
        in_flight += k.held[0].items.len + k.held[1].items.len;
        for (0..2) |side| {
            const s: u1 = @intCast(side);
            const e = &k.ends[s];
            const got = sim.testConnEnd(k.id, s == 1) orelse fail("connection {d} retired, and the model still has it", .{k.id});
            const phase: sim.StreamPhase = if (e.open) .est else .dead;
            if (got.phase != phase or got.fin_in != e.fin_in or got.fin_out != e.fin_out or got.rst_sent != e.rst_sent or got.reset_received != e.reset_received or
                got.fin_before_reset != e.fin_before_reset or got.shut_rd != e.shut_rd or got.reset_reported != e.reset_reported or got.unread != e.unread.items.len or
                got.failed != e.failed or got.fail_reported != e.fail_reported)
            {
                fail("connection {d} side {d}: sim {any}, model phase {t} fin_in {} fin_out {} rst_sent {} reset_received {} fin_before_reset {} shut_rd {} reset_reported {} unread {d}", .{ k.id, s, got, phase, e.fin_in, e.fin_out, e.rst_sent, e.reset_received, e.fin_before_reset, e.shut_rd, e.reset_reported, e.unread.items.len });
            }
            if (e.open) checkEndCalls(k, s);
            const d = k.dirs[s];
            const g = sim.testStreamDir(k.id, s == 0) orelse unreachable;
            var fl: u64 = 0;
            for (M.wire.items) |w| {
                if (w.ci == ci and w.dir == s and w.kind == .data) fl += w.bytes.len;
            }
            var hb: u64 = 0;
            for (k.held[s].items) |w| {
                if (w.kind == .data) hb += w.bytes.len;
            }
            checkHeld(k, s);
            checkCreditLive(k, @intCast(ci), s, g);
            if (g.held != hb or g.held_entries != k.held[s].items.len) fail("connection {d} direction from side {d}: held sim {d} bytes in {d} entries, model {d} in {d}", .{ k.id, s, g.held, g.held_entries, hb, k.held[s].items.len });
            if (hb > 0) held_data = true;
            if (g.window != d.window or g.admitted != d.admitted or g.credited != d.credited or g.read != d.read or g.reclaimed != d.reclaimed or
                !std.mem.eql(u64, &g.discarded, &d.discarded) or g.in_flight != fl or g.unread != k.ends[other(s)].unread.items.len or
                g.data_send_index != d.data_send_index or g.owed != (d.owed_at != null))
            {
                fail("connection {d} direction from side {d}: sim {any}, model window {d} admitted {d} credited {d} read {d} reclaimed {d} discarded {any} in flight {d} data_send_index {d} owed {}", .{ k.id, s, g, d.window, d.admitted, d.credited, d.read, d.reclaimed, d.discarded, fl, d.data_send_index, d.owed_at != null });
            }
            if (!g.conserved()) fail("connection {d} direction from side {d}: not conserved: {any}", .{ k.id, s, g });
            if (g.credited > g.read + g.reclaimed) fail("connection {d} direction from side {d}: credited {d} > read {d} + reclaimed {d}", .{ k.id, s, g.credited, g.read, g.reclaimed });
        }
    }
    const sim_n = sim.streamEntriesInFlight();
    if (sim_n != in_flight) fail("TCP entries in flight: sim {d}, model {d}", .{ sim_n, in_flight });
    // Every entry on the wire, in landing order (design 17.15 item 2): what
    // it is, then when it lands. A delay, a jitter or a release that the
    // model times otherwise fails here at the step that made it, and an
    // entry the sim kept in place of another is named.
    const nd = sim.testTcpEntries(false, &entry_buf);
    if (nd != M.wire.items.len) fail("TCP entries on the wire: sim {d}, model {d}", .{ nd, M.wire.items.len });
    for (M.wire.items, entry_buf[0..nd], 0..) |w, t, i| {
        if (!sameEntry(w, t)) wireTimesByIdentity(nd);
        if (!sameEntry(w, t)) fail("wire entry {d}: sim has connection {d} direction {d} {t} sequence {d} count {d} length {d}, model connection {d} direction {d} {t} sequence {d} count {d} length {d}", .{ i, t.conn_id, @intFromBool(!t.from_client), t.kind, t.seq, t.count, t.len, w.conn_id, w.dir, w.kind, w.seq, w.count, w.bytes.len });
        if (w.due != t.due_at) fail("wire entry {d} (connection {d}, direction {d}, {t}, sequence {d}): sim lands it at {d} ns, model at {d} ns", .{ i, w.conn_id, w.dir, w.kind, w.seq, t.due_at, w.due });
    }
    checkStats();
}

/// On a wire whose order differs: the wire is in landing order, so an
/// entry the sim lands at another time than the model shows first as two
/// different entries at one index. Find each sim entry's model entry by
/// what it is, and fail naming both landing times, before the order check
/// names only the identities.
fn wireTimesByIdentity(nd: usize) void {
    for (entry_buf[0..nd]) |t| {
        for (M.wire.items) |w| {
            if (!sameEntry(w, t)) continue;
            if (w.due != t.due_at) fail("wire entry (connection {d}, direction {d}, {t}, sequence {d}): sim lands it at {d} ns, model at {d} ns (found by what it is: the wire's order differs)", .{ w.conn_id, w.dir, w.kind, w.seq, t.due_at, w.due });
            break;
        }
    }
}

var entry_buf: [32768]sim.TestTcpEntry = undefined;
var held_buf: [32768]sim.TestTcpEntry = undefined;

fn sameEntry(w: MEntry, t: sim.TestTcpEntry) bool {
    return w.conn_id == t.conn_id and w.dir == @intFromBool(!t.from_client) and w.seq == t.seq and std.mem.eql(u8, @tagName(w.kind), @tagName(t.kind)) and w.count == t.count and w.bytes.len == t.len;
}

/// Each held entry of connection `k`'s direction from side `s`, by what it
/// is: kind, sequence, a credit's count, a chunk's length. Coalesced
/// credits that kept the wrong one fail here, at the step that held them,
/// naming both.
fn checkHeld(k: *const MConn, s: u1) void {
    const n = sim.testTcpEntries(true, &held_buf);
    var i: usize = 0;
    for (held_buf[0..n]) |t| {
        if (t.conn_id != k.id or @intFromBool(!t.from_client) != s) continue;
        if (i >= k.held[s].items.len) fail("connection {d} direction {d}: the sim holds {t} sequence {d} count {d}, which the model does not", .{ k.id, s, t.kind, t.seq, t.count });
        const w = k.held[s].items[i];
        if (!sameEntry(w, t)) fail("connection {d} direction {d}: held entry {d}: sim {t} sequence {d} count {d} length {d}, model {t} sequence {d} count {d} length {d}", .{ k.id, s, i, t.kind, t.seq, t.count, t.len, w.kind, w.seq, w.count, w.bytes.len });
        i += 1;
    }
    if (i != k.held[s].items.len) fail("connection {d} direction {d}: the sim holds {d} entries, the model {d}", .{ k.id, s, i, k.held[s].items.len });
}

/// Liveness of the window, checked on the sim's own counts before they are
/// compared with the model's: a live direction with no credit of its own in
/// flight, held or owed has been credited every byte its receiver read or
/// released (design 17.7: each read or release sends a credit or owes one,
/// and a credit carries the cumulative count). If not, a credit was lost
/// and nothing will ever bring it, so a sender on a full window parks for
/// ever. Conservation of bytes cannot see this: no byte is lost. The
/// model's own wire and held lists say what is in flight, so a model that
/// coalesced held credits the same wrong way as the sim still fails here.
/// Not checked: a direction whose sender is terminal, or whose receiver is
/// closed, failed or reset (its credits are discarded or stale by design).
fn checkCreditLive(k: *const MConn, ci: u8, s: u1, g: sim.TestStreamDir) void {
    const r = other(s);
    const rx = k.ends[r];
    if (dirTerminal(k, s) or !rx.open or rx.failed != null or rx.rst_sent or rx.reset_received) return;
    if (k.dirs[s].owed_at != null) return;
    for (M.wire.items) |w| {
        if (w.ci == ci and w.dir == r and w.kind == .credit) return;
    }
    for (k.held[r].items) |w| {
        if (w.kind == .credit) return;
    }
    M.c.live_checks += 1;
    if (g.credited != g.read + g.reclaimed) fail("connection {d} direction from side {d}: a credit was lost: credited {d}, but its receiver read {d} and released {d}, and no credit is in flight, held or owed", .{ k.id, s, g.credited, g.read, g.reclaimed });
}

/// The layer's `bytesReadable` (FIONREAD) and `getpeername` on an open
/// endpoint: its unread bytes, and its peer until a reset was received,
/// then `SocketUnconnected` (design 17.4, row 9).
fn checkEndCalls(k: *const MConn, s: u1) void {
    const e = &k.ends[s];
    const n = os_net.bytesReadable(e.fd) catch |err| fail("bytesReadable on connection {d} side {d}: {t}", .{ k.id, s, err });
    if (n != e.unread.items.len) fail("bytesReadable on connection {d} side {d}: sim {d}, model {d}", .{ k.id, s, n, e.unread.items.len });
    M.c.readable_checks += 1;
    var sa: os_net.sockaddr.in = undefined;
    var len: os_net.socklen_t = @sizeOf(os_net.sockaddr.in);
    const dead = e.reset_received or e.failed != null;
    if (os_net.getpeername(e.fd, @ptrCast(&sa), &len)) |_| {
        if (dead) fail("getpeername on connection {d} side {d}: a peer, after a reset or a failure (want SocketUnconnected)", .{ k.id, s });
        const ip: [4]u8 = @bitCast(sa.addr);
        if (!std.mem.eql(u8, &ip, &.{ 127, 0, 0, 1 }) or std.mem.bigToNative(u16, sa.port) != k.ends[other(s)].port) fail("getpeername on connection {d} side {d}: {any}:{d}, want 127.0.0.1:{d}", .{ k.id, s, ip, std.mem.bigToNative(u16, sa.port), k.ends[other(s)].port });
    } else |err| {
        if (!dead or err != error.SocketUnconnected) fail("getpeername on connection {d} side {d}: {t} (reset received: {}, failed: {})", .{ k.id, s, err, e.reset_received, e.failed != null });
        M.c.peer_unconnected += 1;
    }
    M.c.peer_checks += 1;
}

/// Every field of `sim.streamStats()` against the model's count, by
/// reflection: a field added to `StreamStats` is compared from its first
/// run, and fails here until the model counts it. The handshake's fields
/// (`syn_dropped_dead`, `refused`) stay 0: the model only joins a
/// connection after its accept.
fn checkStats() void {
    if (sim.streamFaultDraws() != M.draws) fail("streamFaultDraws: sim {d}, model {d}", .{ sim.streamFaultDraws(), M.draws });
    const got = sim.streamStats();
    inline for (@typeInfo(sim.StreamStats).@"struct".field_names) |name| {
        if (!std.meta.eql(@field(got, name), @field(M.st, name))) fail("streamStats().{s}: sim {any}, model {any}", .{ name, @field(got, name), @field(M.st, name) });
    }
}

// ---- The end ----

fn endProofs() void {
    // Clear every rule (releasing what is held), then settle.
    while (M.n_rules > 0) clearRule(M.n_rules - 1);
    var n: usize = 0;
    while (nextEvent() != null) : (n += 1) {
        if (n > 100000) fail("the wire did not drain", .{});
        pollOnce(10 * L);
    }
    // With no rule and nothing in flight, held or owed: no send is parked
    // while its receiver has room to take more (window minus unread). A
    // lost credit leaves exactly that, and the proofs below would hide it,
    // because they grow the window of a parked send.
    for (M.slots) |s| {
        if (!s.used or s.state != .parked or s.kind != .send) continue;
        const k = &M.conns[s.ci];
        const rx = k.ends[other(s.side)];
        if (!rx.open or rx.failed != null or rx.shut_rd or rx.rst_sent or rx.reset_received) continue;
        const g = sim.testStreamDir(k.id, s.side == 0) orelse fail("connection {d} retired with a send parked", .{k.id});
        M.c.end_live_checks += 1;
        if (g.unread < g.window) fail("the end: send #{d} on connection {d} is parked while its receiver has room for {d} more bytes (window {d}, unread {d}, credited {d}, read {d}, released {d}): a stranded sender", .{ s.serial, k.id, g.window - g.unread, g.window, g.unread, g.credited, g.read, g.reclaimed });
    }
    // Each op still parked, alone: give it what it waits for.
    for (&M.slots, 0..) |*s, i| {
        if (!s.used or s.state != .parked) continue;
        const k = &M.conns[s.ci];
        const peer = &k.ends[other(s.side)];
        switch (s.kind) {
            .recv => {
                // A byte from the peer, if it can still send one.
                if (!peer.open or peer.fin_out or peer.reset_received or peer.failed != null or peer.send != null or k.dirs[other(s.side)].room() == 0) continue;
                submit(.send, s.ci, other(s.side), 1);
            },
            .send => {
                // One more byte of window, if the receiver is still there.
                if (!peer.open) continue;
                const d = &k.dirs[s.side];
                const w: u32 = @intCast(@min(d.admitted - d.credited + 1, sim.max_stream_window));
                if (w <= d.window) continue;
                sim.setStreamWindow(peer.fd, w);
                d.window = w;
                if (d.room() > 0) wakeSend(s.ci, s.side, .grow);
            },
        }
        M.c.end_proofs += 1;
        var p: usize = 0;
        while (M.slots[i].used and M.slots[i].serial == s.serial) : (p += 1) {
            if (p > 1000) fail("an end proof: parked {t} op #{d} on connection {d} never completed", .{ s.kind, s.serial, k.id });
            pollOnce(10 * L);
        }
    }
    // Close everything; ops still parked end FileDescriptorNotASocket.
    for (&M.conns) |*k| {
        if (!k.used) continue;
        for (&k.ends, 0..) |*e, side| {
            if (!e.open) continue;
            _ = side;
            closeEnd(k, e);
        }
    }
    n = 0;
    while (nextEvent() != null or anySlot()) : (n += 1) {
        if (n > 100000) fail("the end did not drain", .{});
        pollOnce(10 * L);
    }
    checkState();
    for (M.conns) |k| {
        if (k.used) fail("connection {d} did not retire at the end", .{k.id});
    }
    if (sim.streamEntriesInFlight() != 0) fail("{d} TCP entries in flight at the end", .{sim.streamEntriesInFlight()});
    var op = ev.NetClose.init(M.listener);
    runOp(&op.c, 0, 1);
}

fn anySlot() bool {
    for (M.slots) |s| {
        if (s.used) return true;
    }
    return false;
}

fn closeEnd(k: *MConn, e: *MEnd) void {
    // The same as a close step on this endpoint.
    for (&M.conns, 0..) |*kk, ci| {
        if (kk != k) continue;
        const side: u1 = if (e == &kk.ends[0]) 0 else 1;
        const quiet = e.reset_received or e.rst_sent or e.failed != null;
        const rst = !quiet and (e.unread.items.len > 0 or e.linger0);
        const fin = !quiet and !rst and !e.fin_out;
        _ = discardUnread(@intCast(ci), side, .unread_at_close);
        if (rst) {
            e.rst_sent = true;
            countAbort(discardHeldOutbound(@intCast(ci), side, .abort));
            sendEntry(@intCast(ci), side, .rst, 0, &.{}, 0);
        } else if (fin) {
            e.fin_out = true;
            sendEntry(@intCast(ci), side, .fin, 0, &.{}, 0);
        }
        e.open = false;
        wakeRecv(@intCast(ci), side, .close);
        wakeSend(@intCast(ci), side, .close);
        var op = ev.NetClose.init(e.fd);
        runOp(&op.c, 0, 1);
        op.getResult() catch |err| fail("close: {t}", .{err});
        return;
    }
}

// ---- A seed ----

fn runSeed(seed_value: u64, steps: usize, profile: Profile, child: bool) Counts {
    M.seed = seed_value;
    M.step = 0;
    M.profile = profile;
    M.rng = .init(seed_value ^ 0x7374_7265_616d);
    M.arena = .init(std.heap.page_allocator);
    defer M.arena.deinit();
    M.conns = @splat(.{});
    M.n_conns = 0;
    M.wire = .empty;
    M.next_order = 0;
    for (&M.slots) |*s| s.used = false;
    M.next_serial = 1;
    M.c = .{};
    M.st = .{};
    M.np_step = null;
    M.np_text = "";
    M.expect_refusal = null;
    M.in_poll = false;
    M.n_rules = 0;
    M.rules_ever = false;
    M.rule_keys = 0;
    M.draws = 0;
    M.faults = seed_value > n_seeds;
    M.pull = seed_value > 2 * n_seeds;
    M.fail_both_now = false;

    sim.begin(seed_value);
    defer sim.end();
    switch (profile) {
        .linux => sim.setStreamProfile(.linux),
        .macos => sim.setStreamProfile(.macos),
        .none => {},
    }
    M.model_clock = now();
    M.loop.init(.{}) catch |e| fail("loop init: {t}", .{e});
    defer M.loop.deinit();
    M.keepalive = ev.Timer.init(.{ .duration = .fromSeconds(1_000_000) });
    M.keepalive.c.callback = keepaliveDone;
    M.keepalive_done = false;
    M.loop.add(&M.keepalive.c);
    openListener();
    for (0..3) |_| _ = connect();
    checkState();

    while (M.step < steps) : (M.step += 1) {
        if (child) progress();
        const kind = pickStep();
        if (runStep(kind)) M.c.ran[@backingInt(kind)] += 1 else M.c.skipped[@backingInt(kind)] += 1;
        checkState();
    }
    M.step = steps;
    if (child) progress();
    endProofs();
    M.loop.cancel(&M.keepalive.c);
    var n: usize = 0;
    while (!M.keepalive_done) : (n += 1) {
        if (n > 10) fail("the keepalive timer did not end", .{});
        M.loop.poll(.fromNanoseconds(0)) catch |e| fail("poll: {t}", .{e});
    }
    // One line per seed, compared between two processes by the parent.
    std.debug.print("stream model digest: profile {t} seed {d}: trace {x:0>16} wire {x:0>16} state {x:0>16}\n", .{ profile, seed_value, sim.traceHash(), sim.wireDigest(), sim.stateDigest() });
    if (M.np_step) |k| {
        std.debug.print("stream model no-profile: seed {d}: step {d}: {s}\n", .{ seed_value, k, M.np_text });
    } else std.debug.print("stream model no-profile: seed {d}: none\n", .{seed_value});
    return M.c;
}

fn envU64(name: [*:0]const u8) ?u64 {
    const v = std.c.getenv(name) orelse return null;
    return std.fmt.parseInt(u64, std.mem.span(v), 10) catch @panic("stream model: a number was expected in the environment variable");
}

fn envProfile() Profile {
    const v = std.c.getenv("ZIO_STREAM_MODEL_PROFILE") orelse @panic("stream model: ZIO_STREAM_MODEL_PROFILE is linux, macos or none");
    return std.meta.stringToEnum(Profile, std.mem.span(v)) orelse @panic("stream model: ZIO_STREAM_MODEL_PROFILE is linux, macos or none");
}

fn printCounts(c: Counts, profile: Profile, seeds: usize, steps: usize) void {
    std.debug.print("stream model counts ({t}, {d} seeds x {d} steps):\n", .{ profile, seeds, steps });
    inline for (@typeInfo(StepKind).@"enum".field_names, 0..) |name, i| {
        std.debug.print("  step {s}: ran {d}, skipped {d}\n", .{ name, c.ran[i], c.skipped[i] });
    }
    std.debug.print("  skips: no endpoint {d}, op unresolved {d}, no parked op {d}, connection limit {d}, .fixed with a large window {d}, no owed_close shape {d}\n", .{ c.skip_no_endpoint, c.skip_unresolved, c.skip_no_parked, c.skip_conn_limit, c.skip_fixed_window, c.skip_no_owed_shape });
    std.debug.print("  fault skips: rule limit {d}, rule exists {d}, no rule {d}, no failStream target {d}\n", .{ c.skip_rule_limit, c.skip_rule_exists, c.skip_no_rule, c.skip_no_fail_target });
    std.debug.print("  pull skips: no train shape {d}, no late_credit shape {d}\n", .{ c.skip_no_train_shape, c.skip_no_late_credit_shape });
    inline for (fault_floor_names) |name| std.debug.print("  {s}: {d}\n", .{ name, @field(c, name) });
    const info = @typeInfo(Counts).@"struct";
    inline for (info.field_names, info.field_types) |name, T| {
        if (comptime T == u64 and !std.mem.startsWith(u8, name, "skip_")) std.debug.print("  {s}: {d}\n", .{ name, @field(c, name) });
    }
    inline for (@typeInfo(Wake).@"enum".field_names, 0..) |name, i| {
        if (i != 0) std.debug.print("  woken by {s}: receive {d}, send {d}\n", .{ name, c.woken[0][i], c.woken[1][i] });
    }
    std.debug.print("  sends by chunk mode: hashed {d}, fixed {d}, off {d}\n", .{ c.chunks[0], c.chunks[1], c.chunks[2] });
}

/// The counts that must not be 0 over a profile's seeds: a run where one
/// is 0 did not test what it claims.
/// Slice 3's counts, each of its own case (seeds 17 to 32).
const fault_floor_names = .{
    "held_send",             "held_landing",          "released",         "coalesced",                "held_fin",         "held_rst",
    "held_credit",           "held_data_steps",       "abort_discards",   "fail_discards",            "close_while_held", "stall_released",
    "held_delayed_released", "clear_delay_in_flight", "released_to_dead", "delayed",                  "jitter_draws",     "fail_recv_parked",
    "fail_send_parked",      "fail_both_parked",      "fail_none_parked", "fail_first_recv",          "fail_first_send",  "fail_both_ways",
    "fail_timed_out",        "dropped_failed",        "rule_clears",      "older_credit_after_newer", "held_behind",      "abort_held_credit",
    "fail_held_credit",
};

/// The pull seeds' cases (t-4148, t-4154). Each needs `pull_floor_min`
/// over a profile's seeds, not only 1: a case that a floor of 1 lets
/// through once per profile is one generator change away from 0.
const pull_floor_names = .{ "pull_release", "pull_abort_release", "clamp_after_other_hold", "older_credit_after_newer" };
const pull_floor_min = 10;

fn floors(c: Counts, profile: Profile) void {
    // Every zero count is printed before the child fails, so one run names
    // them all (a mutant that zeroes several counts shows each floor).
    var zero: usize = 0;
    inline for (fault_floor_names) |name| {
        if (@field(c, name) == 0) {
            std.debug.print("stream model: count {s} is 0 over the seeds of profile {t}: the run did not test it\n", .{ name, profile });
            zero += 1;
        }
    }
    inline for (pull_floor_names) |name| {
        if (@field(c, name) < pull_floor_min) {
            std.debug.print("stream model: count {s} is {d} over the seeds of profile {t}, under {d}: the run did not test it enough\n", .{ name, @field(c, name), profile, pull_floor_min });
            zero += 1;
        }
    }
    if (zero != 0) fail("{d} of slice 3's counts are 0, or under their floor, over the seeds of profile {t} (listed above)", .{ zero, profile });
    const req = .{
        "send_admitted",    "send_partial",     "send_zero",     "send_parked",      "send_epipe",       "send_reset",
        "recv_data",        "recv_eof",         "recv_reset",    "recv_parked",      "closed_op",        "cancel_recv",
        "cancel_send",      "shut_unconnected", "close_fin",     "close_rst",        "close_quiet",      "rst_answers",
        "stale_departed",   "stale_landed",     "owed_departed", "credits_landed",   "shrink_below_use", "shared_reset",
        "batches",          "end_proofs",       "line3a_no_fin", "line3a_after_fin", "owed_close",       "peer_checks",
        "peer_unconnected", "readable_checks",  "live_checks",   "end_live_checks",
    };
    inline for (req) |name| {
        if (@field(c, name) == 0) fail("count {s} is 0 over the seeds of profile {t}: the run did not test it", .{ name, profile });
    }
    inline for (@typeInfo(StepKind).@"enum".field_names, 0..) |name, i| {
        if (c.ran[i] == 0) fail("step {s} never ran under profile {t}", .{ name, profile });
    }
    // Each wake an op can have, by op (design 17.8, what wakes a parked
    // op). Not needed: a local reset wakes nothing (after its own .both an
    // endpoint's receive gives 0 at once and its send BrokenPipe, so neither
    // parks); a receive is never woken by data's room (credit, grow) or by
    // `.send` (row 19), and a send never by data or a FIN (row 5).
    const need = [2][]const Wake{
        &.{ .data, .fin, .rst, .shut_both, .close, .fail },
        &.{ .rst, .credit, .grow, .shut_send, .shut_both, .close, .fail },
    };
    for (need, 0..) |list, op| {
        for (list) |w| {
            if (c.woken[op][@backingInt(w)] == 0) fail("no {s} was woken by {t} under profile {t}", .{ if (op == 0) "receive" else "send", w, profile });
        }
    }
    for (c.chunks, 0..) |v, i| {
        if (v == 0) fail("no send in chunk mode {d} under profile {t}", .{ i, profile });
    }
    switch (profile) {
        .linux => if (c.local_resets == 0) fail("no local reset under .linux", .{}),
        .macos => if (c.releases == 0) fail("no window release under .macos", .{}),
        .none => {},
    }
}

test "sim: stream op model: connections under random sends, receives, shutdowns, closes, window and chunk changes and cancels, per profile and with none, in a child process" {
    if (comptime !sim.compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    if (!sim.isDeathChild()) return;
    sim.noCoreDump();
    const profile = envProfile();
    const steps: usize = @intCast(envU64("ZIO_STREAM_MODEL_STEPS") orelse default_steps);
    var total: Counts = .{};
    if (envU64("ZIO_STREAM_MODEL_SEED")) |one| {
        total.add(runSeed(one, steps, profile, true));
        printCounts(total, profile, 1, steps);
        sim.childDone(child_name);
        return;
    }
    for (1..all_seeds + 1) |s| total.add(runSeed(s, steps, profile, true));
    printCounts(total, profile, all_seeds, steps);
    if (std.c.getenv("ZIO_STREAM_MODEL_STEPS") == null) floors(total, profile);
    sim.childDone(child_name);
}

const Child = struct { term: std.process.Child.Term, stderr: []u8 };

fn runChild(gpa: std.mem.Allocator, profile: []const u8, seed: ?u64) !Child {
    const io = std.testing.io;
    var exe_buf: [std.fs.max_path_bytes]u8 = undefined;
    const exe_len = try std.process.executablePath(io, &exe_buf);
    var env: std.process.Environ.Map = .init(gpa);
    defer env.deinit();
    if (std.c.getenv("PATH")) |path| try env.put("PATH", std.mem.span(path));
    try env.put("TEST_FILTER", child_name);
    try env.put(sim.death_child_env, "1");
    try env.put("ZIO_STREAM_MODEL_PROFILE", profile);
    if (std.c.getenv("ZIO_STREAM_MODEL_STEPS")) |v| try env.put("ZIO_STREAM_MODEL_STEPS", std.mem.span(v));
    var sb: [24]u8 = undefined;
    if (seed) |s| try env.put("ZIO_STREAM_MODEL_SEED", try std.fmt.bufPrint(&sb, "{d}", .{s}));
    const res = try std.process.run(gpa, io, .{
        .argv = &.{exe_buf[0..exe_len]},
        .environ_map = &env,
        .stdout_limit = .limited(1 << 20),
        .stderr_limit = .limited(64 << 20),
        .timeout = .{ .duration = .{ .raw = .fromSeconds(600), .clock = .awake } },
    });
    gpa.free(res.stdout);
    return .{ .term = res.term, .stderr = res.stderr };
}

/// A replay, a diagnostic: one child per profile asked for (`linux` and
/// `macos` when none is), with the seed and the length asked for, its own
/// output printed whole. A profiled child must pass; a child with no
/// profile is only printed, as its panic is the point. No floors (a child
/// given a seed or a length skips them), no second process, no no-profile
/// runs.
fn replay(gpa: std.mem.Allocator, o_profile: ?[*:0]const u8, o_seed: ?u64) !void {
    const both = [_][]const u8{ "linux", "macos" };
    const one = [_][]const u8{if (o_profile) |v| std.mem.span(v) else ""};
    const list: []const []const u8 = if (o_profile != null) &one else &both;
    for (list) |p| {
        if (std.meta.stringToEnum(Profile, p) == null) @panic("stream model: ZIO_STREAM_MODEL_PROFILE is linux, macos or none");
        const r = try runChild(gpa, p, o_seed);
        defer gpa.free(r.stderr);
        printChild(r.stderr);
        std.debug.print("stream model replay: profile {s}, seed {?d}: term {any}; the last step it started: {s}\n", .{ p, o_seed, r.term, lastProgress(r.stderr) });
        if (std.mem.eql(u8, p, "none")) continue;
        if (!r.term.success()) return error.StreamModelFailed;
        try sim.checkDoneMarkers(child_name, r.stderr);
    }
}

/// The lines of `stderr` that start with `prefix`, joined.
fn linesWith(gpa: std.mem.Allocator, stderr: []const u8, prefix: []const u8) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(gpa);
    var it = std.mem.splitScalar(u8, stderr, '\n');
    while (it.next()) |line| {
        if (std.mem.indexOf(u8, line, prefix)) |at| {
            try out.appendSlice(gpa, line[at..]);
            try out.append(gpa, '\n');
        }
    }
    return out.toOwnedSlice(gpa);
}

fn lastProgress(stderr: []const u8) []const u8 {
    var last: []const u8 = "none";
    var it = std.mem.splitScalar(u8, stderr, '\n');
    while (it.next()) |line| {
        if (std.mem.indexOf(u8, line, "stream model: seed ")) |at| last = line[at..];
    }
    return last;
}

/// A child's own output, minus the per-step progress lines.
fn printChild(stderr: []const u8) void {
    var it = std.mem.splitScalar(u8, stderr, '\n');
    while (it.next()) |line| {
        if (std.mem.indexOf(u8, line, "stream model: seed ") != null) continue;
        if (std.mem.indexOf(u8, line, "stream model") != null or std.mem.indexOf(u8, line, "panic") != null) std.debug.print("{s}\n", .{line});
    }
}

test "sim: stream op model: connections under random sends, receives, shutdowns, closes, window and chunk changes and cancels, per profile and with none" {
    if (comptime !sim.compiled_in or builtin.os.tag == .windows) return error.SkipZigTest;
    if (sim.isDeathChild()) return;
    const gpa = std.testing.allocator;
    // A replay: the parent passes the overrides to its children itself (a
    // child's environment is built here, not inherited).
    const o_profile = std.c.getenv("ZIO_STREAM_MODEL_PROFILE");
    const o_seed = envU64("ZIO_STREAM_MODEL_SEED");
    if (o_profile != null or o_seed != null or std.c.getenv("ZIO_STREAM_MODEL_STEPS") != null) return replay(gpa, o_profile, o_seed);
    var np: [2][]u8 = undefined;
    var np_n: usize = 0;
    defer for (np[0..np_n]) |x| gpa.free(x);
    for ([_][]const u8{ "linux", "macos" }) |p| {
        var digests: [2][]u8 = undefined;
        var dn: usize = 0;
        defer for (digests[0..dn]) |x| gpa.free(x);
        for (0..2) |run| {
            const r = try runChild(gpa, p, null);
            defer gpa.free(r.stderr);
            if (run == 0) printChild(r.stderr);
            if (!r.term.success()) {
                std.debug.print("stream model child ({s}): term {any}; the last step it started: {s}\n", .{ p, r.term, lastProgress(r.stderr) });
                printChild(r.stderr);
                return error.StreamModelFailed;
            }
            try sim.checkDoneMarkers(child_name, r.stderr);
            digests[dn] = try linesWith(gpa, r.stderr, "stream model digest: ");
            dn += 1;
            if (run == 0) {
                np[np_n] = try linesWith(gpa, r.stderr, "stream model no-profile: ");
                np_n += 1;
            }
        }
        if (!std.mem.eql(u8, digests[0], digests[1])) {
            std.debug.print("two processes, profile {s}, differ:\n{s}\n----\n{s}\n", .{ p, digests[0], digests[1] });
            return error.ProcessesDiffer;
        }
        std.debug.print("stream model: profile {s}: two processes give the same {d} digest lines\n", .{ p, std.mem.count(u8, digests[0], "\n") });
    }
    // The no-profile shadow: both profiles predict the same point per seed.
    if (!std.mem.eql(u8, np[0], np[1])) {
        std.debug.print("the no-profile shadows differ:\n.linux:\n{s}\n.macos:\n{s}\n", .{ np[0], np[1] });
        return error.ShadowsDiffer;
    }
    // Then a run with no profile per seed must panic there, or finish.
    var panics: usize = 0;
    var it = std.mem.splitScalar(u8, np[0], '\n');
    while (it.next()) |line| {
        if (line.len == 0) continue;
        // "stream model no-profile: seed S: step K: <text>" or "...: none".
        var parts = std.mem.splitSequence(u8, line["stream model no-profile: seed ".len..], ": ");
        const seed = try std.fmt.parseInt(u64, parts.next().?, 10);
        const rest = parts.rest();
        const r = try runChild(gpa, "none", seed);
        defer gpa.free(r.stderr);
        if (std.mem.eql(u8, rest, "none")) {
            if (!r.term.success()) {
                std.debug.print("no-profile seed {d}: predicted no panic; term {any}; last step {s}\n", .{ seed, r.term, lastProgress(r.stderr) });
                printChild(r.stderr);
                return error.NoProfileRunFailed;
            }
            try sim.checkDoneMarkers(child_name, r.stderr);
            continue;
        }
        const k_end = std.mem.indexOf(u8, rest, ": ").?;
        const step = rest["step ".len..k_end];
        const text = rest[k_end + 2 ..];
        var want_buf: [256]u8 = undefined;
        const want = try std.fmt.bufPrint(&want_buf, "sim: {s}: the kernels differ here", .{text});
        var at_buf: [64]u8 = undefined;
        const at = try std.fmt.bufPrint(&at_buf, "stream model: seed {d} step {s}", .{ seed, step });
        const died = r.term == .signal;
        if (!died or !sim.hasPanicLine(r.stderr, want) or !std.mem.eql(u8, lastProgress(r.stderr), at)) {
            std.debug.print("no-profile seed {d}: wanted the panic \"{s}\" in \"{s}\"; term {any}; last step {s}\n", .{ seed, want, at, r.term, lastProgress(r.stderr) });
            printChild(r.stderr);
            return error.NoProfileRunDiffers;
        }
        panics += 1;
        std.debug.print("no-profile seed {d}: panicked at step {s}: {s}\n", .{ seed, step, text });
    }
    if (panics == 0) return error.NoProfilePointEverReached;
}
