// SPDX-FileCopyrightText: 2025 Lukáš Lalinský
// SPDX-License-Identifier: MIT
//
// The structural check of the native-exit refusals (sim network design,
// section 3). A sim fd is not a kernel descriptor, so no syscall wrapper in
// `src/os/` may take one. Each function there that takes an fd either has a
// sim branch (the socket layer, `src/os/net.zig`) or starts with
// `sim.refuseSimFd(@src(), fd)`. This test reads every file in `src/os/` and
// fails the build when one does not. It fails toward noise: a file it does
// not know, a signature or body it cannot parse, a parameter it cannot
// classify, or zero functions checked, each fail the test.
//
// How "takes an fd" is decided:
// - by type: a parameter whose type is `fd_t` or ends in `.fd_t`;
// - by name: a parameter whose name is in `fd_names` and whose type is `i32`
//   or `c_int`. An fd-like name with any other type is unclassified and
//   fails, unless the pair is in `not_fds` below with a reason.
// - any other plain integer parameter (or `anytype`, which can be one) fails
//   unless its name is in `non_fd_ints`. An fd is just an integer, so a name
//   this check does not know is no proof that it is not one.
//
// A private function or an extern is covered when every call to it in its
// file is from a covered function. That holds only while nothing else can
// reach it: so a name used as a value (`pub const x = y`, a pointer in a
// table) makes the function need its own refusal.
//
// What it cannot see, listed by hand (`fd_carriers`) and tested by death
// tests in `sim.zig`: an fd inside a struct or a slice (`pollfd`, `msghdr`,
// `mmsghdr`, a control buffer). A function with such a parameter must be in
// that list, or the test fails. Also outside its reach: direct syscalls made
// by zio files outside `src/os/` (the backends, which never run under sim,
// and tests), listed in the commit that added this file.

const std = @import("std");
const builtin = @import("builtin");

const Verdict = enum { scanned, windows_only };

const File = struct { path: []const u8, verdict: Verdict, src: []const u8 };

/// Every file in `src/os/`. A file that is not here fails the test.
const files = [_]File{
    .{ .path = "base.zig", .verdict = .scanned, .src = @embedFile("os/base.zig") },
    .{ .path = "c.zig", .verdict = .scanned, .src = @embedFile("os/c.zig") },
    .{ .path = "darwin.zig", .verdict = .scanned, .src = @embedFile("os/darwin.zig") },
    .{ .path = "dragonfly.zig", .verdict = .scanned, .src = @embedFile("os/dragonfly.zig") },
    .{ .path = "eventfd.zig", .verdict = .scanned, .src = @embedFile("os/eventfd.zig") },
    .{ .path = "freebsd.zig", .verdict = .scanned, .src = @embedFile("os/freebsd.zig") },
    .{ .path = "fs.zig", .verdict = .scanned, .src = @embedFile("os/fs.zig") },
    .{ .path = "linux.zig", .verdict = .scanned, .src = @embedFile("os/linux.zig") },
    .{ .path = "net.zig", .verdict = .scanned, .src = @embedFile("os/net.zig") },
    .{ .path = "netbsd.zig", .verdict = .scanned, .src = @embedFile("os/netbsd.zig") },
    .{ .path = "openbsd.zig", .verdict = .scanned, .src = @embedFile("os/openbsd.zig") },
    .{ .path = "posix.zig", .verdict = .scanned, .src = @embedFile("os/posix.zig") },
    .{ .path = "process.zig", .verdict = .scanned, .src = @embedFile("os/process.zig") },
    .{ .path = "root.zig", .verdict = .scanned, .src = @embedFile("os/root.zig") },
    .{ .path = "syscall_cancel.zig", .verdict = .scanned, .src = @embedFile("os/syscall_cancel.zig") },
    .{ .path = "system/c.zig", .verdict = .scanned, .src = @embedFile("os/system/c.zig") },
    .{ .path = "system/linux.zig", .verdict = .scanned, .src = @embedFile("os/system/linux.zig") },
    .{ .path = "thread.zig", .verdict = .scanned, .src = @embedFile("os/thread.zig") },
    .{ .path = "time.zig", .verdict = .scanned, .src = @embedFile("os/time.zig") },
    // There are no sim fds on Windows (design section 2): sim sockets refuse
    // there, and `sim.refuseSimFd` is a no-op.
    .{ .path = "windows.zig", .verdict = .windows_only, .src = "" },
    .{ .path = "windows/win32error.zig", .verdict = .windows_only, .src = "" },
};

const fd_names = [_][]const u8{ "fd", "dirfd", "dir", "oldfd", "newfd", "old_dir", "new_dir", "handle", "sock", "epoll_fd", "fromfd", "tofd" };

/// Integer (and `anytype`) parameter names that are never an fd in
/// `src/os/`. Every other integer parameter fails the test unless its name is
/// in `fd_names`: an fd can be any integer, so a name this check does not know
/// is not proof that it is not one.
const non_fd_ints = [_][]const u8{ "_", "advice", "af", "argsz", "backlog", "code", "count", "current", "end", "err", "events", "expected", "flags", "futex_op", "ifindex", "index", "initval", "len", "length", "level", "maxevents", "min_complete", "mode", "nbytes", "number", "offset", "op", "operation", "optname", "port", "prot", "protocol", "rc", "sig", "sock_type", "start", "timeout", "timeout_ms", "timeout_us", "to_submit", "val", "val2", "val3", "value", "vlen", "wake_value", "whence" };

/// Parameters with an fd-like name that are not fds, each with why.
const not_fds = [_]struct { file: []const u8, func: []const u8, param: []const u8, why: []const u8 }{};

/// Functions that carry fds inside a struct or slice, and what covers them.
const fd_carriers = [_]struct { file: []const u8, func: []const u8, covered_by: []const u8 }{
    .{ .file = "net.zig", .func = "poll", .covered_by = "simPoll: every fd in the set must be a sim fd or none; tests \"sim: dgram ev refuses: a poll that mixes sim and real fds\", \"sim: exits refuse: a poll set of sim pipe fds\"" },
    .{ .file = "net.zig", .func = "simPoll", .covered_by = "the sim branch of poll itself" },
    .{ .file = "net.zig", .func = "recvmmsg", .covered_by = "its fd is refused first; the mmsghdr array holds buffers and addresses, not fds" },
    .{ .file = "net.zig", .func = "sendmmsg", .covered_by = "its fd is refused first; the mmsghdr array holds buffers and addresses, not fds" },
    .{ .file = "net.zig", .func = "recvmmsg_syscall", .covered_by = "called only by recvmmsg" },
    .{ .file = "net.zig", .func = "sendmmsg_syscall", .covered_by = "called only by sendmmsg" },
    .{ .file = "net.zig", .func = "sourceAddressControl", .covered_by = "builds an IP_PKTINFO message: an address, no fd" },
};

/// Public functions whose fd goes only to a sim-branched function in the same
/// file: the fd appears once in the body, as that call's first argument.
const delegates = [_]struct { file: []const u8, func: []const u8, call: []const u8 }{
    .{ .file = "net.zig", .func = "getSockError", .call = "getsockopt(fd," },
    .{ .file = "net.zig", .func = "setNoDelay", .call = "setsockopt(fd," },
};

/// C declarations that take an fd and have no caller in their own file: they
/// are called from other files, which this check does not follow. Each was
/// checked by hand (grep) when it was listed; a new one fails the test.
const extern_fds = [_]struct { file: []const u8, func: []const u8, callers: []const u8 }{
    .{ .file = "system/c.zig", .func = "fchmodat", .callers = "fs.zig dirSetFilePermissions (refuses), the backends" },
    .{ .file = "system/c.zig", .func = "fchownat", .callers = "fs.zig dirSetFileOwner (refuses), the backends" },
    .{ .file = "system/c.zig", .func = "faccessat", .callers = "fs.zig dirAccess (refuses), the backends" },
    .{ .file = "system/c.zig", .func = "linkat", .callers = "fs.zig dirHardLink, fileHardLink (refuse), the backends" },
    .{ .file = "system/c.zig", .func = "unlinkat", .callers = "fs.zig dirDeleteFile, dirDeleteDir (refuse), the backends" },
    .{ .file = "system/c.zig", .func = "renameat", .callers = "fs.zig renameat (refuses), the backends" },
    .{ .file = "system/c.zig", .func = "mkdirat", .callers = "fs.zig mkdirat (refuses), the backends" },
    .{ .file = "system/c.zig", .func = "mmap", .callers = "posix.zig mmap (refuses)" },
    .{ .file = "system/c.zig", .func = "utimensat", .callers = "fs.zig dirSetFileTimestamps (refuses), the backends" },
    .{ .file = "system/c.zig", .func = "lseek", .callers = "its own wrapper lseek in system/c.zig (refuses)" },
    .{ .file = "darwin.zig", .func = "renameatx_np", .callers = "fs.zig renameatPreserve (refuses)" },
};

/// Private functions called with an fd their caller made itself, not one it
/// was given: the coverage rule for callers does not apply.
const own_fds = [_]struct { file: []const u8, func: []const u8, why: []const u8 }{
    .{ .file = "net.zig", .func = "disableUdpResetReports", .why = "Windows only; called by socket() with the socket it just created" },
};

const Param = struct { name: []const u8, ty: []const u8 };

const Fn = struct {
    name: []const u8,
    is_pub: bool,
    is_extern: bool,
    params: [16]Param = undefined,
    n_params: usize = 0,
    /// Byte range of the body, without its braces; empty for an extern.
    body_start: usize = 0,
    body_end: usize = 0,
};

const ParseError = error{Unparseable};

/// Skips a comment, a string or a char literal at `i`, returning the index
/// after it, or null when `i` starts none of them.
fn skipLiteral(s: []const u8, i: usize) ParseError!?usize {
    if (i + 1 < s.len and s[i] == '/' and s[i + 1] == '/') {
        return std.mem.indexOfScalarPos(u8, s, i, '\n') orelse s.len;
    }
    if (i + 1 < s.len and s[i] == '\\' and s[i + 1] == '\\') {
        return std.mem.indexOfScalarPos(u8, s, i, '\n') orelse s.len;
    }
    if (s[i] == '"' or s[i] == '\'') {
        const q = s[i];
        var j = i + 1;
        while (j < s.len) : (j += 1) {
            if (s[j] == '\\') {
                j += 1;
                continue;
            }
            if (s[j] == q) return j + 1;
            if (s[j] == '\n') return error.Unparseable;
        }
        return error.Unparseable;
    }
    return null;
}

/// The index of the bracket that closes the one at `open`.
fn matching(s: []const u8, open: usize) ParseError!usize {
    var depth: usize = 0;
    var i = open;
    while (i < s.len) {
        if (try skipLiteral(s, i)) |next| {
            i = next;
            continue;
        }
        switch (s[i]) {
            '(', '[', '{' => depth += 1,
            ')', ']', '}' => {
                depth -= 1;
                if (depth == 0) return i;
            },
            else => {},
        }
        i += 1;
    }
    return error.Unparseable;
}

fn isIdent(c: u8) bool {
    return std.ascii.isAlphanumeric(c) or c == '_';
}

/// Every function declaration in `s`: lines whose code starts with `fn`,
/// `pub fn`, `inline fn`, `export fn` or an `extern` declaration.
fn parseFns(s: []const u8, out: []Fn) ParseError!usize {
    var n: usize = 0;
    var i: usize = 0;
    while (i < s.len) {
        if (try skipLiteral(s, i)) |next| {
            i = next;
            continue;
        }
        const at_word_start = i == 0 or !isIdent(s[i - 1]);
        if (at_word_start and std.mem.startsWith(u8, s[i..], "fn ")) {
            // The qualifiers before `fn` on its line.
            const line_start = if (std.mem.lastIndexOfScalar(u8, s[0..i], '\n')) |p| p + 1 else 0;
            const pre = std.mem.trim(u8, s[line_start..i], " \t");
            const is_pub = std.mem.startsWith(u8, pre, "pub");
            const is_extern = std.mem.indexOf(u8, pre, "extern") != null;
            const known = pre.len == 0 or std.mem.eql(u8, pre, "pub") or std.mem.eql(u8, pre, "inline") or
                std.mem.eql(u8, pre, "pub inline") or std.mem.eql(u8, pre, "export") or is_extern;
            if (!known) {
                // `fn` inside an expression (a function type): not a declaration.
                i += 3;
                continue;
            }
            var j = i + 3;
            const name_start = j;
            while (j < s.len and isIdent(s[j])) j += 1;
            if (j == name_start) {
                i += 3;
                continue;
            }
            if (n == out.len) return error.Unparseable;
            var f: Fn = .{ .name = s[name_start..j], .is_pub = is_pub, .is_extern = is_extern };
            while (j < s.len and s[j] == ' ') j += 1;
            if (j >= s.len or s[j] != '(') return error.Unparseable;
            const close = try matching(s, j);
            try parseParams(s[j + 1 .. close], &f);
            // The body: the first `{` after the return type, or `;` for none.
            var k = close + 1;
            while (k < s.len and s[k] != ';') : (k += 1) {
                if (s[k] == '(' or s[k] == '[') {
                    k = try matching(s, k);
                } else if (s[k] == '{') {
                    // A `{` in the return type is not the body: after
                    // `error`, a container keyword, or `switch (...)`.
                    const before = std.mem.trimEnd(u8, s[close + 1 .. k], " \n");
                    const in_type = for ([_][]const u8{ "error", "struct", "union", "enum", "opaque" }) |kw| {
                        if (std.mem.endsWith(u8, before, kw)) break true;
                    } else std.mem.endsWith(u8, before, ")") and std.mem.startsWith(u8, std.mem.trimStart(u8, before, " "), "switch (");
                    if (!in_type) break;
                    k = try matching(s, k);
                }
            }
            if (k >= s.len) return error.Unparseable;
            if (s[k] == '{') {
                const end = try matching(s, k);
                // A body ends a declaration: its `}` ends the line. Anything
                // else means the body was found in the wrong place.
                if (end + 1 < s.len and s[end + 1] != '\n' and s[end + 1] != '\r') return error.Unparseable;
                f.body_start = k + 1;
                f.body_end = end;
                i = k + 1;
            } else {
                if (!is_extern) return error.Unparseable;
                i = k + 1;
            }
            out[n] = f;
            n += 1;
            continue;
        }
        i += 1;
    }
    return n;
}

fn parseParams(s: []const u8, f: *Fn) ParseError!void {
    var start: usize = 0;
    var i: usize = 0;
    while (i <= s.len) {
        if (i < s.len) {
            if (try skipLiteral(s, i)) |next| {
                i = next;
                continue;
            }
            if (s[i] == '(' or s[i] == '[' or s[i] == '{') {
                i = try matching(s, i) + 1;
                continue;
            }
        }
        if (i == s.len or s[i] == ',') {
            const raw = std.mem.trim(u8, stripComments(s[start..i]), " \t\r\n");
            if (raw.len > 0 and !std.mem.eql(u8, raw, "...")) {
                var p = raw;
                for ([_][]const u8{ "comptime ", "noalias " }) |q| {
                    if (std.mem.startsWith(u8, p, q)) p = p[q.len..];
                }
                const colon = std.mem.indexOfScalar(u8, p, ':') orelse return error.Unparseable;
                const name = std.mem.trim(u8, p[0..colon], " \t\r\n");
                for (name) |c| if (!isIdent(c)) return error.Unparseable;
                if (name.len == 0 or f.n_params == f.params.len) return error.Unparseable;
                f.params[f.n_params] = .{ .name = name, .ty = std.mem.trim(u8, p[colon + 1 ..], " \t\r\n") };
                f.n_params += 1;
            }
            start = i + 1;
        }
        i += 1;
    }
}

/// `s` with whole-line `//` comments removed (parameters can have them).
fn stripComments(s: []const u8) []const u8 {
    var t = std.mem.trim(u8, s, " \t\r\n");
    while (std.mem.startsWith(u8, t, "//")) {
        const nl = std.mem.indexOfScalar(u8, t, '\n') orelse return "";
        t = std.mem.trim(u8, t[nl + 1 ..], " \t\r\n");
    }
    return t;
}

const Kind = enum { fd, carrier, other, unclassified };

/// A plain integer type, or `anytype`, which can be one: such a parameter
/// can carry an fd whatever its name, so its name must say which it is.
fn isIntType(ty: []const u8) bool {
    const t = if (std.mem.startsWith(u8, ty, "?")) ty[1..] else ty;
    for ([_][]const u8{ "anytype", "usize", "isize", "comptime_int", "c_int", "c_uint", "c_long", "c_ulong", "c_short", "c_ushort", "c_longlong", "c_ulonglong", "c_char" }) |n| {
        if (std.mem.eql(u8, t, n)) return true;
    }
    if (t.len < 2 or (t[0] != 'i' and t[0] != 'u')) return false;
    for (t[1..]) |c| if (!std.ascii.isDigit(c)) return false;
    return true;
}

fn classify(file: []const u8, func: []const u8, p: Param) Kind {
    const last = if (std.mem.lastIndexOfScalar(u8, p.ty, '.')) |d| p.ty[d + 1 ..] else p.ty;
    if (std.mem.eql(u8, last, "fd_t")) return .fd;
    for ([_][]const u8{ "pollfd", "msghdr", "mmsghdr", "cmsghdr" }) |c| {
        if (std.mem.indexOf(u8, p.ty, c) != null) return .carrier;
    }
    var fd_like = false;
    for (fd_names) |n| if (std.mem.eql(u8, p.name, n)) {
        fd_like = true;
    };
    if (fd_like and (std.mem.eql(u8, p.ty, "i32") or std.mem.eql(u8, p.ty, "c_int"))) return .fd;
    for (not_fds) |x| {
        if (std.mem.eql(u8, x.file, file) and std.mem.eql(u8, x.func, func) and std.mem.eql(u8, x.param, p.name)) return .other;
    }
    if (fd_like) return .unclassified;
    if (!isIntType(p.ty)) return .other;
    for (non_fd_ints) |n| if (std.mem.eql(u8, p.name, n)) return .other;
    return .unclassified;
}

fn isListed(comptime list: anytype, file: []const u8, func: []const u8) bool {
    for (list) |x| {
        if (std.mem.eql(u8, x.file, file) and std.mem.eql(u8, x.func, func)) return true;
    }
    return false;
}

/// The first statement of a body, without leading whitespace and comments.
fn firstStatement(body: []const u8) []const u8 {
    var t = std.mem.trimStart(u8, body, " \t\r\n");
    while (std.mem.startsWith(u8, t, "//")) {
        const nl = std.mem.indexOfScalar(u8, t, '\n') orelse return "";
        t = std.mem.trimStart(u8, t[nl + 1 ..], " \t\r\n");
    }
    const end = std.mem.indexOfScalar(u8, t, '\n') orelse t.len;
    return std.mem.trimEnd(u8, t[0..end], " \t\r");
}

const Report = struct {
    checked: usize = 0,
    refusing: usize = 0,
    sim_branched: usize = 0,
    delegating: usize = 0,
    private_covered: usize = 0,
    externs: usize = 0,
    carriers: usize = 0,
    /// Checked functions left uncovered (each also a problem).
    uncovered: usize = 0,
    problems: usize = 0,
};

fn problem(r: *Report, comptime fmt: []const u8, args: anytype) void {
    r.problems += 1;
    std.debug.print("sim exits: " ++ fmt ++ "\n", args);
}

/// Checks one file's functions. Returns false only for a parse failure.
fn checkFile(file: []const u8, src: []const u8, r: *Report) void {
    var fns: [400]Fn = undefined;
    const n = parseFns(src, &fns) catch {
        problem(r, "{s}: cannot parse its functions", .{file});
        return;
    };
    // Which functions are covered: their fds never reach a syscall unrefused.
    var covered: [400]bool = @splat(false);
    var takes_fd: [400]bool = @splat(false);
    for (fns[0..n], 0..) |f, idx| {
        var fd_params: [16][]const u8 = undefined;
        var n_fd: usize = 0;
        var carrier = false;
        for (f.params[0..f.n_params]) |p| switch (classify(file, f.name, p)) {
            .fd => {
                fd_params[n_fd] = p.name;
                n_fd += 1;
            },
            .carrier => carrier = true,
            .other => {},
            .unclassified => problem(r, "{s}: {s}: parameter `{s}: {s}` is not classified: an integer (or anytype) whose name is in neither fd_names nor non_fd_ints, or an fd-like name with a type this check does not know", .{ file, f.name, p.name, p.ty }),
        };
        if (carrier) {
            if (!isListed(fd_carriers, file, f.name)) problem(r, "{s}: {s} carries fds in a struct or slice and is not in fd_carriers", .{ file, f.name });
            r.carriers += 1;
        }
        if (n_fd == 0) continue;
        takes_fd[idx] = true;
        r.checked += 1;
        if (f.is_extern) {
            // A C declaration: its callers in this file are checked below.
            r.externs += 1;
            continue;
        }
        const body = src[f.body_start..f.body_end];
        const first = firstStatement(body);
        if (isRefusal(first, fd_params[0..n_fd])) {
            r.refusing += 1;
            covered[idx] = true;
            continue;
        }
        if (simBranched(body, fd_params[0..n_fd])) {
            r.sim_branched += 1;
            covered[idx] = true;
            continue;
        }
        for (delegates) |d| {
            if (std.mem.eql(u8, d.file, file) and std.mem.eql(u8, d.func, f.name) and
                n_fd == 1 and identCount(body, fd_params[0]) == 1 and std.mem.indexOf(u8, body, d.call) != null)
            {
                r.delegating += 1;
                covered[idx] = true;
            }
        }
        if (covered[idx]) continue;
        if (f.is_pub) r.uncovered += 1;
        if (f.is_pub) problem(r, "{s}: pub fn {s} takes an fd but does not start with sim.refuseSimFd(@src(), ...) for all of them, and has no sim branch", .{ file, f.name });
    }
    // A private function or an extern whose name is used as a value (an
    // alias such as `pub const x = y`, a pointer in a table) can be called
    // from anywhere, so its callers here cannot cover it: it must refuse
    // first itself.
    for (fns[0..n], 0..) |f, idx| {
        if (!takes_fd[idx] or covered[idx] or (f.is_pub and !f.is_extern)) continue;
        // A listed extern is re-exported on purpose; its callers in other
        // files were checked by hand when it was listed.
        if (f.is_extern and isListed(extern_fds, file, f.name)) continue;
        if (valueUse(src, f.name)) |at| {
            r.uncovered += 1;
            covered[idx] = true; // reported here, not again below
            problem(r, "{s}: {s} takes an fd and is used as a value at line {d} (an alias or a pointer), so it must start with sim.refuseSimFd(@src(), ...) itself", .{ file, f.name, lineOf(src, at) });
        }
    }
    // A private function or an extern that takes an fd is covered when every
    // call to it is from a covered function in this file; repeat until
    // nothing changes, then report what is left.
    var changed = true;
    while (changed) {
        changed = false;
        for (fns[0..n], 0..) |f, idx| {
            if (!takes_fd[idx] or covered[idx] or (f.is_pub and !f.is_extern)) continue;
            const vacuous = f.is_extern and !hasCaller(src, fns[0..n], f.name);
            if (vacuous and !isListed(extern_fds, file, f.name)) continue;
            if (isListed(own_fds, file, f.name) or allCallersCovered(src, fns[0..n], &covered, f.name)) {
                covered[idx] = true;
                r.private_covered += 1;
                changed = true;
            }
        }
    }
    for (fns[0..n], 0..) |f, idx| {
        if (!takes_fd[idx] or covered[idx] or (f.is_pub and !f.is_extern)) continue;
        r.uncovered += 1;
        if (f.is_extern and !hasCaller(src, fns[0..n], f.name)) {
            problem(r, "{s}: extern {s} takes an fd, is called only from other files, and is not in extern_fds", .{ file, f.name });
        } else problem(r, "{s}: {s} takes an fd and is called from a function that does not refuse a sim fd first", .{ file, f.name });
    }
}

/// The first use of `name` in `src` that is neither its declaration
/// (`fn name(`), a call (`name(`, also `x.name(`) nor a doctest's name
/// (`test name {`), outside comments and
/// strings: an alias, a pointer, a field of that name. Fails toward noise: a
/// same-named field of some other value counts too.
fn valueUse(src: []const u8, name: []const u8) ?usize {
    var i: usize = 0;
    while (i < src.len) {
        if (skipLiteral(src, i) catch null) |next| {
            i = next;
            continue;
        }
        if (std.mem.startsWith(u8, src[i..], name) and (i == 0 or !isIdent(src[i - 1]))) {
            const after = i + name.len;
            if (after >= src.len or !isIdent(src[after])) {
                const is_call = after < src.len and src[after] == '(';
                const is_decl = i >= 3 and std.mem.eql(u8, src[i - 3 .. i], "fn ");
                const is_test_name = i >= 5 and std.mem.eql(u8, src[i - 5 .. i], "test ");
                if (!is_call and !is_decl and !is_test_name) return i;
            }
            i = after;
            continue;
        }
        i += 1;
    }
    return null;
}

fn lineOf(src: []const u8, at: usize) usize {
    return std.mem.count(u8, src[0..at], "\n") + 1;
}

fn allCallersCovered(src: []const u8, fns: []const Fn, covered: []const bool, name: []const u8) bool {
    for (fns, 0..) |caller, cidx| {
        if (caller.is_extern or covered[cidx]) continue;
        const body = src[caller.body_start..caller.body_end];
        var pos: usize = 0;
        while (std.mem.indexOfPos(u8, body, pos, name)) |at| : (pos = at + name.len) {
            const after = at + name.len;
            if ((at == 0 or !isIdent(body[at - 1])) and after < body.len and body[after] == '(') return false;
        }
    }
    return true;
}

fn hasCaller(src: []const u8, fns: []const Fn, name: []const u8) bool {
    for (fns) |caller| {
        if (caller.is_extern) continue;
        const body = src[caller.body_start..caller.body_end];
        var pos: usize = 0;
        while (std.mem.indexOfPos(u8, body, pos, name)) |at| : (pos = at + name.len) {
            const after = at + name.len;
            if ((at == 0 or !isIdent(body[at - 1])) and after < body.len and body[after] == '(') return true;
        }
    }
    return false;
}

fn identCount(body: []const u8, name: []const u8) usize {
    var n: usize = 0;
    var pos: usize = 0;
    while (std.mem.indexOfPos(u8, body, pos, name)) |at| : (pos = at + name.len) {
        const after = at + name.len;
        if ((at == 0 or !isIdent(body[at - 1])) and (after >= body.len or !isIdent(body[after]))) n += 1;
    }
    return n;
}

fn isRefusal(first: []const u8, fds: []const []const u8) bool {
    if (fds.len == 1) {
        var buf: [96]u8 = undefined;
        const want = std.fmt.bufPrint(&buf, "sim.refuseSimFd(@src(), {s});", .{fds[0]}) catch return false;
        return std.mem.eql(u8, first, want);
    }
    const prefix = "sim.refuseSimFds(@src(), .{ ";
    if (!std.mem.startsWith(u8, first, prefix) or !std.mem.endsWith(u8, first, " });")) return false;
    var it = std.mem.splitSequence(u8, first[prefix.len .. first.len - " });".len], ", ");
    var k: usize = 0;
    while (it.next()) |name| : (k += 1) {
        if (k >= fds.len or !std.mem.eql(u8, name, fds[k])) return false;
    }
    return k == fds.len;
}

/// A sim branch for every fd: `sim.isSimFd(fd)`, or `sim.isDgramFd(fd)`
/// followed by the refusal for the rest of the sim range.
fn simBranched(body: []const u8, fds: []const []const u8) bool {
    // The fds go into a poll set whose sim branch checks every entry.
    if (std.mem.indexOf(u8, body, "if (simPoll(") != null) return true;
    for (fds) |fd| {
        var buf: [3][96]u8 = undefined;
        const a = std.fmt.bufPrint(&buf[0], "sim.isSimFd({s})", .{fd}) catch return false;
        const b = std.fmt.bufPrint(&buf[1], "sim.isDgramFd({s})", .{fd}) catch return false;
        const c = std.fmt.bufPrint(&buf[2], "sim.refuseSimFd(@src(), {s});", .{fd}) catch return false;
        const has_a = std.mem.indexOf(u8, body, a) != null;
        const has_bc = std.mem.indexOf(u8, body, b) != null and std.mem.indexOf(u8, body, c) != null;
        if (!has_a and !has_bc) return false;
    }
    return true;
}

/// The files under `dir` (relative paths), from the file system.
fn listDir(gpa: std.mem.Allocator, root: [:0]const u8, rel: []const u8, out: *std.ArrayList([]u8)) !void {
    var path_buf: [512]u8 = undefined;
    const path = try std.fmt.bufPrintSentinel(&path_buf, "{s}{s}{s}", .{ root, if (rel.len > 0) "/" else "", rel }, 0);
    const d = std.c.opendir(path) orelse {
        std.debug.print("sim exits: cannot open {s} (errno {d}); run the test from the repository root, with src/os beside it\n", .{ path, std.c._errno().* });
        return error.CannotListSrcOs;
    };
    defer _ = std.c.closedir(d);
    while (std.c.readdir(d)) |ent| {
        const name = std.mem.sliceTo(@as([*:0]const u8, @ptrCast(&ent.name)), 0);
        if (std.mem.eql(u8, name, ".") or std.mem.eql(u8, name, "..")) continue;
        const sub = if (rel.len > 0) try std.fmt.allocPrint(gpa, "{s}/{s}", .{ rel, name }) else try gpa.dupe(u8, name);
        if (ent.type == std.c.DT.DIR) {
            defer gpa.free(sub);
            try listDir(gpa, root, sub, out);
        } else try out.append(gpa, sub);
    }
}

pub fn runCheck(gpa: std.mem.Allocator, os_dir: [:0]const u8, report: *Report) !void {
    var on_disk: std.ArrayList([]u8) = .empty;
    defer {
        for (on_disk.items) |p| gpa.free(p);
        on_disk.deinit(gpa);
    }
    try listDir(gpa, os_dir, "", &on_disk);
    for (on_disk.items) |p| {
        var known = false;
        for (files) |f| if (std.mem.eql(u8, f.path, p)) {
            known = true;
        };
        if (!known) problem(report, "src/os/{s} is not in this test's file list: add it, scanned or with a reason", .{p});
    }
    for (files) |f| {
        var present = false;
        for (on_disk.items) |p| if (std.mem.eql(u8, f.path, p)) {
            present = true;
        };
        if (!present) problem(report, "src/os/{s} is in this test's file list but not on disk", .{f.path});
        if (f.verdict == .scanned) checkFile(f.path, f.src, report);
    }
}

test "sim: exits: every function in src/os that takes an fd refuses a sim fd first, or has a sim branch" {
    if (builtin.os.tag == .windows) return error.SkipZigTest;
    var r: Report = .{};
    try runCheck(std.testing.allocator, "src/os", &r);
    std.debug.print("sim exits: {d} fd-taking functions checked: {d} refuse first, {d} have a sim branch, {d} delegate, {d} private or extern covered by their callers ({d} of them externs); {d} carry fds in a struct or slice, each listed by hand\n", .{ r.checked, r.refusing, r.sim_branched, r.delegating, r.private_covered, r.externs, r.carriers });
    try std.testing.expectEqual(@as(usize, 0), r.problems);
    try std.testing.expect(r.checked > 0);
    // Every checked function is in exactly one bucket.
    try std.testing.expectEqual(r.checked, r.refusing + r.sim_branched + r.delegating + r.private_covered + r.uncovered);
}

test "sim: exits: the check itself fails on what it cannot parse" {
    var fns: [4]Fn = undefined;
    try std.testing.expectError(error.Unparseable, parseFns("pub fn f(fd: fd_t {\n", &fns));
    try std.testing.expectError(error.Unparseable, parseFns("pub fn f(fd fd_t) void {}\n", &fns));
    try std.testing.expectError(error.Unparseable, parseFns("pub fn f(fd: fd_t) void { const s = \"unterminated;\n}\n", &fns));
    try std.testing.expectEqual(@as(usize, 1), try parseFns("pub fn f(fd: fd_t) void { _ = \"{\"; }\n", &fns));
    // A `{` in the return type is not taken for the body.
    try std.testing.expectEqual(@as(usize, 1), try parseFns("fn f(fd: fd_t) switch (x) { .a => u8, else => u16 } {\n    g(fd);\n}\n", &fns));
    try std.testing.expect(std.mem.indexOf(u8, "fn f(fd: fd_t) switch (x) { .a => u8, else => u16 } {\n    g(fd);\n}\n"[fns[0].body_start..fns[0].body_end], "g(fd)") != null);
    try std.testing.expectEqual(@as(usize, 1), try parseFns("pub fn f(fd: fd_t) error{A}!void {\n    g(fd);\n}\n", &fns));
}
