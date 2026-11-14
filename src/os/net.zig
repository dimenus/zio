const std = @import("std");
const builtin = @import("builtin");
const posix = @import("posix.zig");
const windows = @import("windows.zig");

const unexpectedError = @import("base.zig").unexpectedError;
const thread = @import("thread.zig");
const syscall_cancel = @import("syscall_cancel.zig");

const log = @import("../common.zig").log;
const sim = @import("../sim.zig");

/// Whether this build has the sim branch of the socket layer (sim network
/// design rev 3, section 3.1). Without sim, every `if (comptime sim_layer)`
/// block is gone and each function is the syscall.
const sim_layer = sim.compiled_in and builtin.os.tag != .windows;

// Windows addrinfo definitions
const windows_addrinfo = if (builtin.os.tag == .windows) extern struct {
    flags: AI,
    family: i32,
    socktype: i32,
    protocol: i32,
    addrlen: usize,
    canonname: ?[*:0]u8,
    addr: ?*windows.sockaddr,
    next: ?*windows_addrinfo,
} else void;

const windows_extern = if (builtin.os.tag == .windows) struct {
    pub extern "ws2_32" fn getaddrinfo(
        pNodeName: ?[*:0]const u8,
        pServiceName: ?[*:0]const u8,
        pHints: ?*const windows_addrinfo,
        ppResult: *?*windows_addrinfo,
    ) callconv(.winapi) i32;

    pub extern "ws2_32" fn freeaddrinfo(
        pAddrInfo: *windows_addrinfo,
    ) callconv(.winapi) void;
} else struct {};

pub const has_unix_sockets = switch (builtin.os.tag) {
    .windows => builtin.os.version_range.windows.isAtLeast(.win10_rs4) orelse false,
    .wasi => false,
    else => true,
};

pub const has_unix_dgram_sockets = switch (builtin.os.tag) {
    .windows => false, // Windows only supports stream Unix sockets
    .wasi => false,
    else => true,
};

var wsa_init_done: std.atomic.Value(bool) = .init(false);
var wsa_init_mutex: thread.Mutex = .init();

fn wsaInit() void {
    if (builtin.os.tag == .windows) {
        var wsa_data: windows.WSADATA = undefined;
        _ = windows.WSAStartup(2 << 8 | 2, &wsa_data);
    }
}

pub fn ensureWSAInitialized() void {
    if (wsa_init_done.load(.acquire)) return;
    wsa_init_mutex.lock();
    defer wsa_init_mutex.unlock();
    if (wsa_init_done.load(.acquire)) return;
    wsaInit();
    wsa_init_done.store(true, .release);
}

pub const fd_t = switch (builtin.os.tag) {
    .windows => windows.SOCKET,
    else => posix.system.fd_t,
};

pub const pollfd = switch (builtin.os.tag) {
    .windows => windows.pollfd,
    else => posix.system.pollfd,
};

pub const POLL = switch (builtin.os.tag) {
    .windows => windows.POLL,
    else => posix.system.POLL,
};

pub const sockaddr = posix.system.sockaddr;
pub const AF = posix.system.AF;
pub const SOCK = posix.system.SOCK;
pub const IPPROTO = posix.IPPROTO;
pub const socklen_t = if (builtin.os.tag == .windows) i32 else posix.system.socklen_t;
pub const SOL = posix.system.SOL;
pub const SO = posix.system.SO;

pub const E = if (builtin.os.tag == .windows) windows.WinsockError else posix.system.E;

pub const iovec = @import("base.zig").iovec;
pub const iovec_const = @import("base.zig").iovec_const;

// Helper functions for single buffer conversion
pub inline fn iovecFromSlice(buffer: []u8) iovec {
    return switch (builtin.os.tag) {
        .windows => .{ .len = @intCast(buffer.len), .buf = buffer.ptr },
        else => .{ .base = buffer.ptr, .len = buffer.len },
    };
}

pub inline fn iovecConstFromSlice(buffer: []const u8) iovec_const {
    return switch (builtin.os.tag) {
        .windows => .{ .len = @intCast(buffer.len), .buf = @constCast(buffer.ptr) },
        else => .{ .base = buffer.ptr, .len = buffer.len },
    };
}

pub const PollError = error{
    SystemResources,
    Unexpected,
};

pub fn poll(fds: []pollfd, timeout: i32) PollError!usize {
    if (comptime sim_layer) {
        if (simPoll(fds, timeout)) |n| return n;
    }
    switch (builtin.os.tag) {
        .windows => {
            while (true) {
                const rc = windows.WSAPoll(fds.ptr, @intCast(fds.len), timeout);
                if (rc >= 0) {
                    return @intCast(rc);
                }
                const err = windows.WSAGetLastError();
                switch (err) {
                    .EINTR => continue,
                    .ENOBUFS => return error.SystemResources,
                    .EFAULT => unreachable,
                    .EINVAL => {
                        log.err("WSAPoll returned WSAEINVAL - invalid parameter (fds.len={}, timeout={})", .{ fds.len, timeout });
                        return unexpectedError(err);
                    },
                    else => return unexpectedError(err),
                }
            }
        },
        else => {
            while (true) {
                const rc = posix.system.poll(fds.ptr, @intCast(fds.len), timeout);
                switch (posix.errno(rc)) {
                    .SUCCESS => return @intCast(rc),
                    .FAULT => unreachable,
                    .INTR => continue,
                    .INVAL => return error.SystemResources,
                    .NOMEM => return error.SystemResources,
                    else => |err| return unexpectedError(err),
                }
            }
        },
    }
}

pub fn close(fd: fd_t) void {
    if (comptime sim_layer) {
        if (sim.isDgramFd(fd)) return sim.dgramClose(fd);
    }
    switch (builtin.os.tag) {
        .windows => {
            _ = windows.closesocket(fd);
        },
        else => {
            while (true) {
                const rc = posix.system.close(fd);
                switch (posix.errno(rc)) {
                    .SUCCESS => return,
                    .INTR => continue,
                    .BADF => unreachable, // sockfd is not a valid file descriptor - would be a bug
                    // Note: EIO, ENOSPC, EDQUOT can occur but are rare; we treat them as unexpected
                    else => |err| {
                        unexpectedError(err) catch {};
                        return;
                    },
                }
            }
        },
    }
}

pub const ShutdownHow = enum {
    receive,
    send,
    both,
};

pub const ShutdownError = error{
    SocketUnconnected,
    ConnectionAborted,
    ConnectionResetByPeer,
    NetworkDown,
    Canceled,
    Unexpected,
};

pub fn shutdown(fd: fd_t, how: ShutdownHow) ShutdownError!void {
    if (comptime sim_layer) {
        // An unconnected UDP socket: ENOTCONN, as Linux answers.
        if (sim.isDgramFd(fd)) return error.SocketUnconnected;
    }
    switch (builtin.os.tag) {
        .windows => {
            const system_how: i32 = switch (how) {
                .receive => windows.SD_RECEIVE,
                .send => windows.SD_SEND,
                .both => windows.SD_BOTH,
            };
            const rc = windows.shutdown(fd, system_how);
            if (rc == windows.SOCKET_ERROR) {
                const err = windows.WSAGetLastError();
                return errnoToShutdownError(err);
            }
        },
        else => {
            const system_how: c_int = switch (how) {
                .receive => posix.system.SHUT.RD,
                .send => posix.system.SHUT.WR,
                .both => posix.system.SHUT.RDWR,
            };
            while (true) {
                const rc = posix.system.shutdown(fd, system_how);
                switch (posix.errno(rc)) {
                    .SUCCESS => return,
                    .INTR => continue,
                    else => |err| return errnoToShutdownError(err),
                }
            }
        },
    }
}

pub const Domain = enum(c_int) {
    unspec = AF.UNSPEC,
    ipv4 = AF.INET,
    ipv6 = AF.INET6,
    unix = AF.UNIX,
    _,

    /// Convert from POSIX address family constant
    pub fn fromPosix(af: anytype) Domain {
        return @fromBackingInt(@intCast(af));
    }

    /// Convert to POSIX address family constant
    pub fn toPosix(self: Domain) c_int {
        return @backingInt(self);
    }
};

pub const Type = enum(c_int) {
    stream = SOCK.STREAM,
    dgram = SOCK.DGRAM,
    raw = SOCK.RAW,
    seqpacket = SOCK.SEQPACKET,
    rdm = SOCK.RDM,
    _,

    /// Convert from POSIX socket type constant
    pub fn fromPosix(sock_type: anytype) Type {
        return @fromBackingInt(@intCast(sock_type));
    }

    /// Convert to POSIX socket type constant
    pub fn toPosix(self: Type) c_int {
        return @backingInt(self);
    }

    /// Convert from std.Io socket mode
    pub fn fromStd(mode: std.Io.net.Socket.Mode) Type {
        return switch (mode) {
            .stream => .stream,
            .dgram => .dgram,
            .seqpacket => .seqpacket,
            .raw => .raw,
            .rdm => .rdm,
        };
    }
};

pub const Protocol = enum(c_int) {
    ip = IPPROTO.IP,
    icmp = IPPROTO.ICMP,
    tcp = IPPROTO.TCP,
    udp = IPPROTO.UDP,
    ipv6 = IPPROTO.IPV6,
    icmpv6 = IPPROTO.ICMPV6,
    raw = IPPROTO.RAW,
    _,

    /// Convert from POSIX protocol constant
    pub fn fromPosix(protocol: anytype) Protocol {
        return @fromBackingInt(@intCast(protocol));
    }

    /// Convert to POSIX protocol constant
    pub fn toPosix(self: Protocol) c_int {
        return @backingInt(self);
    }

    /// Convert from std.Io protocol
    pub fn fromStd(protocol: std.Io.net.Protocol) Protocol {
        return @fromBackingInt(@intCast(@backingInt(protocol)));
    }
};

pub const OpenFlags = packed struct {
    nonblocking: bool,
    cloexec: bool = true,
};

pub const OpenError = error{
    AddressFamilyUnsupported,
    ProtocolNotSupported,
    ProcessFdQuotaExceeded,
    SystemFdQuotaExceeded,
    SystemResources,
    PermissionDenied,
    Canceled,
    Unexpected,
};

/// Windows fails the next receive on an unconnected UDP socket with
/// WSAECONNRESET when an ICMP port unreachable comes back for a datagram it
/// sent earlier (and with WSAENETRESET for an ICMP time exceeded), unlike
/// POSIX systems: one peer that went away would break the receive loop
/// that serves all the others. Both reports are turned off, as libuv and Go
/// do. Best-effort.
fn disableUdpResetReports(sock: fd_t) void {
    const off: windows.BOOL = windows.FALSE;
    var returned: windows.DWORD = 0;
    for ([_]windows.DWORD{ windows.SIO_UDP_CONNRESET, windows.SIO_UDP_NETRESET }) |code| {
        _ = windows.WSAIoctl(sock, code, &off, @sizeOf(windows.BOOL), null, 0, &returned, null, null);
    }
}

pub fn socket(domain: Domain, socket_type: Type, protocol: Protocol, flags: OpenFlags) OpenError!fd_t {
    if (comptime sim_layer) {
        if (sim.isRunning()) {
            if (domain != .ipv4 or socket_type != .dgram or (protocol != .ip and protocol != .udp)) {
                sim.panic("sim: socket({t}, {t}, {t}) during a sim run: only IPv4 datagram sockets are simulated", .{ domain, socket_type, protocol });
            }
            return sim.dgramOpen(flags.nonblocking);
        }
    }
    switch (builtin.os.tag) {
        .windows => {
            const sock = windows.WSASocketW(
                domain.toPosix(),
                socket_type.toPosix(),
                protocol.toPosix(),
                null,
                0,
                windows.WSA_FLAG_OVERLAPPED,
            );
            if (sock == windows.INVALID_SOCKET) {
                const err = windows.WSAGetLastError();
                return errnoToOpenError(err);
            }
            if (flags.nonblocking) {
                var mode: c_ulong = 1;
                _ = windows.ioctlsocket(sock, windows.FIONBIO, &mode);
            }
            if (socket_type == .dgram) disableUdpResetReports(sock);
            return sock;
        },
        else => {
            var sock_flags: c_int = socket_type.toPosix();
            // Linux supports SOCK_NONBLOCK and SOCK_CLOEXEC flags in socket()
            // BSD (macOS, FreeBSD, etc.) requires fcntl() instead
            if (builtin.os.tag == .linux) {
                if (flags.nonblocking) {
                    sock_flags |= SOCK.NONBLOCK;
                }
                if (flags.cloexec) {
                    sock_flags |= SOCK.CLOEXEC;
                }
            }

            while (true) {
                const rc = posix.system.socket(
                    @intCast(domain.toPosix()),
                    @intCast(sock_flags),
                    @intCast(protocol.toPosix()),
                );
                switch (posix.errno(rc)) {
                    .SUCCESS => {
                        const fd: fd_t = @intCast(rc);
                        errdefer close(fd);

                        // On non-Linux systems, set flags using fcntl
                        if (builtin.os.tag != .linux) {
                            if (flags.nonblocking) {
                                try posix.setNonblocking(fd);
                            }
                            if (flags.cloexec) {
                                try posix.setCloexec(fd);
                            }
                        }

                        return fd;
                    },
                    .INTR => continue,
                    else => |err| return errnoToOpenError(err),
                }
            }
        },
    }
}

pub const SocketPairError = error{
    OperationUnsupported,
    AccessDenied,
    AddressFamilyUnsupported,
    ProtocolUnsupportedBySystem,
    ProcessFdQuotaExceeded,
    SystemFdQuotaExceeded,
    SystemResources,
    ProtocolUnsupportedByAddressFamily,
    SocketModeUnsupported,
    Unexpected,
};

pub fn socketpair(domain: Domain, socket_type: Type, protocol: Protocol, flags: OpenFlags) SocketPairError![2]fd_t {
    switch (builtin.os.tag) {
        .windows => return error.OperationUnsupported,
        else => {
            if (@TypeOf(posix.system.socketpair) == void) return error.OperationUnsupported;

            var sock_flags: c_int = socket_type.toPosix();
            if (builtin.os.tag == .linux) {
                if (flags.nonblocking) sock_flags |= SOCK.NONBLOCK;
                if (flags.cloexec) sock_flags |= SOCK.CLOEXEC;
            }

            var fds: [2]fd_t = undefined;
            while (true) {
                const rc = posix.system.socketpair(
                    @intCast(domain.toPosix()),
                    @intCast(sock_flags),
                    @intCast(protocol.toPosix()),
                    &fds,
                );
                const err = posix.errno(rc);
                // Darwin with __DARWIN_UNIX03 returns EOPNOTSUPP = 102, but Zig's
                // darwin E enum defines OPNOTSUPP = 45 (the legacy ENOTSUP alias).
                if (builtin.os.tag.isDarwin() and @backingInt(err) == 102) {
                    return error.OperationUnsupported;
                }
                switch (err) {
                    .SUCCESS => {
                        errdefer {
                            close(fds[0]);
                            close(fds[1]);
                        }
                        if (builtin.os.tag != .linux) {
                            if (flags.nonblocking) {
                                try posix.setNonblocking(fds[0]);
                                try posix.setNonblocking(fds[1]);
                            }
                            if (flags.cloexec) {
                                try posix.setCloexec(fds[0]);
                                try posix.setCloexec(fds[1]);
                            }
                        }
                        return fds;
                    },
                    .INTR => continue,
                    .ACCES => return error.AccessDenied,
                    .AFNOSUPPORT => return error.AddressFamilyUnsupported,
                    .INVAL => return error.ProtocolUnsupportedBySystem,
                    .MFILE => return error.ProcessFdQuotaExceeded,
                    .NFILE => return error.SystemFdQuotaExceeded,
                    .NOBUFS, .NOMEM => return error.SystemResources,
                    .PROTONOSUPPORT => return error.ProtocolUnsupportedByAddressFamily,
                    .PROTOTYPE => return error.SocketModeUnsupported,
                    .OPNOTSUPP => return error.OperationUnsupported,
                    else => |e| return unexpectedError(e),
                }
            }
        },
    }
}

pub const BindError = error{
    AddressInUse,
    AddressUnavailable,
    AddressFamilyUnsupported,
    AccessDenied,
    FileDescriptorNotASocket,
    SymLinkLoop,
    NameTooLong,
    FileNotFound,
    NotDir,
    ReadOnlyFileSystem,
    NetworkDown,
    InputOutput,
    SystemResources,
    Canceled,
    Unexpected,
};

pub fn errnoToBindError(err: E) BindError {
    switch (builtin.os.tag) {
        .windows => {
            return switch (err) {
                .EADDRINUSE => error.AddressInUse,
                .EADDRNOTAVAIL => error.AddressUnavailable,
                .EAFNOSUPPORT => error.AddressFamilyUnsupported,
                .EACCES => error.AccessDenied,
                .ENOTSOCK => error.FileDescriptorNotASocket,
                .ENETDOWN => error.NetworkDown,
                .ENOBUFS => error.SystemResources,
                .OPERATION_ABORTED => error.Canceled,
                else => unexpectedError(err),
            };
        },
        else => {
            return switch (err) {
                .SUCCESS => unreachable,
                .ACCES, .PERM => error.AccessDenied,
                .ADDRINUSE => error.AddressInUse,
                .NOTSOCK => error.FileDescriptorNotASocket,
                .AFNOSUPPORT => error.AddressFamilyUnsupported,
                .ADDRNOTAVAIL => error.AddressUnavailable,
                .LOOP => error.SymLinkLoop,
                .NAMETOOLONG => error.NameTooLong,
                .NOENT => error.FileNotFound,
                .NOMEM => error.SystemResources,
                .AGAIN => error.SystemResources, // Kernel resources temporarily unavailable (FreeBSD)
                .NOTDIR => error.NotDir,
                .ROFS => error.ReadOnlyFileSystem,
                .IO => error.InputOutput,
                .NETDOWN => error.NetworkDown,
                .CANCELED => error.Canceled, // io_uring completion after cancel
                else => |e| unexpectedError(e),
            };
        },
    }
}

pub fn bind(fd: fd_t, addr: *const sockaddr, addr_len: socklen_t) BindError!void {
    if (comptime sim_layer) {
        if (sim.isSimFd(fd)) return sim.dgramBind(fd, simAddr("bind", addr, addr_len));
    }
    switch (builtin.os.tag) {
        .windows => {
            const rc = windows.bind(fd, @ptrCast(addr), @intCast(addr_len));
            if (rc == windows.SOCKET_ERROR) {
                const err = windows.WSAGetLastError();
                return errnoToBindError(err);
            }
        },
        else => {
            while (true) {
                const rc = posix.system.bind(fd, addr, addr_len);
                switch (posix.errno(rc)) {
                    .SUCCESS => return,
                    .INTR => continue,
                    else => |err| return errnoToBindError(err),
                }
            }
        },
    }
}

pub const ListenError = error{
    AddressInUse,
    AlreadyConnected,
    OperationNotSupported,
    FileDescriptorNotASocket,
    NetworkDown,
    SystemResources,
    Canceled,
    Unexpected,
};

pub fn errnoToListenError(err: E) ListenError {
    switch (builtin.os.tag) {
        .windows => {
            return switch (err) {
                .EADDRINUSE => error.AddressInUse,
                .EISCONN => error.AlreadyConnected,
                .EOPNOTSUPP => error.OperationNotSupported,
                .ENOTSOCK => error.FileDescriptorNotASocket,
                .ENETDOWN => error.NetworkDown,
                .ENOBUFS, .EMFILE => error.SystemResources,
                .OPERATION_ABORTED => error.Canceled,
                else => unexpectedError(err),
            };
        },
        else => {
            return switch (err) {
                .SUCCESS => unreachable,
                .ADDRINUSE => error.AddressInUse,
                .OPNOTSUPP => error.OperationNotSupported,
                .NOTSOCK => error.FileDescriptorNotASocket,
                .NETDOWN => error.NetworkDown,
                .CANCELED => error.Canceled, // io_uring completion after cancel
                else => |e| unexpectedError(e),
            };
        },
    }
}

pub fn listen(fd: fd_t, backlog: u31) ListenError!void {
    if (comptime sim_layer) {
        if (sim.isDgramFd(fd)) return error.OperationNotSupported;
    }
    switch (builtin.os.tag) {
        .windows => {
            const rc = windows.listen(fd, backlog);
            if (rc == windows.SOCKET_ERROR) {
                const err = windows.WSAGetLastError();
                return errnoToListenError(err);
            }
        },
        else => {
            while (true) {
                const rc = posix.system.listen(fd, backlog);
                switch (posix.errno(rc)) {
                    .SUCCESS => return,
                    .INTR => continue,
                    else => |err| return errnoToListenError(err),
                }
            }
        },
    }
}

pub const ConnectError = error{
    AccessDenied,
    AddressInUse,
    AddressUnavailable,
    AddressFamilyUnsupported,
    WouldBlock,
    AlreadyConnected,
    ConnectionPending,
    ConnectionRefused,
    ConnectionResetByPeer,
    Timeout,
    NetworkUnreachable,
    HostUnreachable,
    FileDescriptorNotASocket,
    FileNotFound,
    SymLinkLoop,
    NameTooLong,
    NotDir,
    NetworkDown,
    SystemResources,
    Canceled,
    Unexpected,
};

pub fn connect(fd: fd_t, addr: *const sockaddr, addr_len: socklen_t) ConnectError!void {
    if (comptime sim_layer) {
        if (sim.isDgramFd(fd)) sim.panic("sim: connect on sim datagram socket {d}: connected UDP is not simulated", .{fd});
    }
    switch (builtin.os.tag) {
        .windows => {
            const rc = windows.connect(fd, @ptrCast(addr), @intCast(addr_len));
            if (rc == windows.SOCKET_ERROR) {
                const err = windows.WSAGetLastError();
                return errnoToConnectError(err);
            }
        },
        else => {
            while (true) {
                const rc = posix.system.connect(fd, addr, addr_len);
                switch (posix.errno(rc)) {
                    .SUCCESS => return,
                    .INTR => continue,
                    else => |err| return errnoToConnectError(err),
                }
            }
        },
    }
}

pub const AcceptError = error{
    WouldBlock,
    ConnectionAborted,
    ProcessFdQuotaExceeded,
    SystemFdQuotaExceeded,
    SystemResources,
    FileDescriptorNotASocket,
    SocketNotListening,
    OperationNotSupported,
    ProtocolFailure,
    BlockedByFirewall,
    NetworkDown,
    Canceled,
    Unexpected,
};

pub fn accept(fd: fd_t, addr: ?*sockaddr, addr_len: ?*socklen_t, flags: OpenFlags) AcceptError!fd_t {
    if (comptime sim_layer) {
        if (sim.isDgramFd(fd)) return error.OperationNotSupported;
    }
    switch (builtin.os.tag) {
        .windows => {
            const sock = windows.accept(
                fd,
                if (addr) |a| @ptrCast(a) else null,
                addr_len,
            );
            if (sock == windows.INVALID_SOCKET) {
                const err = windows.WSAGetLastError();
                return errnoToAcceptError(err);
            }
            if (flags.nonblocking) {
                var mode: c_ulong = 1;
                _ = windows.ioctlsocket(sock, windows.FIONBIO, &mode);
            }
            return sock;
        },
        else => {
            // Linux supports accept4 with SOCK_NONBLOCK and SOCK_CLOEXEC flags
            // BSD (macOS, FreeBSD, etc.) requires fcntl() instead
            var accept_flags: c_int = 0;
            if (builtin.os.tag == .linux) {
                if (flags.nonblocking) {
                    accept_flags |= SOCK.NONBLOCK;
                }
                if (flags.cloexec) {
                    accept_flags |= SOCK.CLOEXEC;
                }
            }

            while (true) {
                var addr_len_tmp: posix.system.socklen_t = if (addr_len) |len| len.* else 0;
                const rc = if (builtin.os.tag == .linux)
                    posix.system.accept4(
                        fd,
                        if (addr) |a| @ptrCast(@alignCast(a)) else null,
                        if (addr_len != null) &addr_len_tmp else null,
                        @intCast(accept_flags),
                    )
                else
                    posix.system.accept(
                        fd,
                        if (addr) |a| @ptrCast(@alignCast(a)) else null,
                        if (addr_len != null) &addr_len_tmp else null,
                    );

                switch (posix.errno(rc)) {
                    .SUCCESS => {
                        if (addr_len) |len| {
                            len.* = addr_len_tmp;
                        }
                        const sock: fd_t = @intCast(rc);
                        errdefer close(sock);

                        // On non-Linux systems, set flags using fcntl
                        if (builtin.os.tag != .linux) {
                            if (flags.nonblocking) {
                                try posix.setNonblocking(sock);
                            }
                            if (flags.cloexec) {
                                try posix.setCloexec(sock);
                            }
                        }

                        return sock;
                    },
                    .INTR => continue,
                    else => |err| return errnoToAcceptError(err),
                }
            }
        },
    }
}

pub const GetSockNameError = error{Unexpected};

pub fn getsockname(fd: fd_t, addr: *sockaddr, addr_len: *socklen_t) GetSockNameError!void {
    if (comptime sim_layer) {
        if (sim.isSimFd(fd)) return writeSimAddr(sim.dgramLocal(fd), addr, addr_len);
    }
    switch (builtin.os.tag) {
        .windows => {
            const rc = windows.getsockname(fd, @ptrCast(addr), @ptrCast(addr_len));
            if (rc != 0) {
                return unexpectedError(windows.WSAGetLastError());
            }
        },
        else => {
            const rc = posix.system.getsockname(fd, addr, addr_len);
            if (rc != 0) {
                return unexpectedError(posix.errno(rc));
            }
        },
    }
}

pub const GetSockErrorError = error{Unexpected};

pub fn getSockError(fd: fd_t) GetSockErrorError!i32 {
    var err: i32 = 0;
    try getsockopt(fd, SOL.SOCKET, SO.ERROR, std.mem.asBytes(&err));
    return err;
}

pub const SetsockoptError = error{ OptionNotSupported, Unexpected };

pub fn setsockopt(fd: fd_t, level: i32, optname: u32, optval: []const u8) SetsockoptError!void {
    if (comptime sim_layer) {
        if (sim.isSimFd(fd)) sim.panic("sim: setsockopt(level {d}, option {d}) on sim fd {d}: socket options are not simulated yet", .{ level, optname, fd });
    }
    switch (builtin.os.tag) {
        .windows => {
            const rc = windows.setsockopt(fd, level, optname, optval.ptr, @intCast(optval.len));
            if (rc != 0) return switch (windows.WSAGetLastError()) {
                .ENOPROTOOPT => error.OptionNotSupported,
                else => |err| unexpectedError(err),
            };
        },
        else => {
            const rc = posix.system.setsockopt(fd, level, @intCast(optname), optval.ptr, @intCast(optval.len));
            switch (posix.errno(rc)) {
                .SUCCESS => {},
                .NOPROTOOPT => return error.OptionNotSupported,
                else => |err| return unexpectedError(err),
            }
        },
    }
}

/// Sets TCP_NODELAY, which disables Nagle's algorithm when enabled.
pub fn setNoDelay(fd: fd_t, enabled: bool) SetsockoptError!void {
    const value: c_int = @intFromBool(enabled);
    return setsockopt(fd, IPPROTO.TCP, posix.TCP.NODELAY, std.mem.asBytes(&value));
}

pub const GetsockoptError = error{Unexpected};

pub fn getsockopt(fd: fd_t, level: i32, optname: u32, optval: []u8) GetsockoptError!void {
    if (comptime sim_layer) {
        if (sim.isSimFd(fd)) sim.panic("sim: getsockopt(level {d}, option {d}) on sim fd {d}: socket options are not simulated yet", .{ level, optname, fd });
    }
    switch (builtin.os.tag) {
        .windows => {
            var len: i32 = @intCast(optval.len);
            const rc = windows.getsockopt(fd, level, @intCast(optname), optval.ptr, &len);
            if (rc != 0) return unexpectedError(windows.WSAGetLastError());
        },
        else => {
            var len: socklen_t = @intCast(optval.len);
            const rc = posix.system.getsockopt(fd, level, @intCast(optname), optval.ptr, &len);
            switch (posix.errno(rc)) {
                .SUCCESS => {},
                else => |err| return unexpectedError(err),
            }
        },
    }
}

pub fn errnoToConnectError(err: E) ConnectError {
    switch (builtin.os.tag) {
        .windows => {
            return switch (err) {
                .ECONNREFUSED => error.ConnectionRefused,
                .ETIMEDOUT => error.Timeout,
                .ENETUNREACH => error.NetworkUnreachable,
                .EHOSTUNREACH => error.HostUnreachable,
                .EACCES => error.AccessDenied,
                .EADDRINUSE => error.AddressInUse,
                .EADDRNOTAVAIL => error.AddressUnavailable,
                .EAFNOSUPPORT => error.AddressFamilyUnsupported,
                .EISCONN => error.AlreadyConnected,
                .EALREADY => error.ConnectionPending,
                .EWOULDBLOCK => error.WouldBlock,
                .ENOTSOCK => error.FileDescriptorNotASocket,
                .ENETDOWN => error.NetworkDown,
                .ENOBUFS => error.SystemResources,
                .OPERATION_ABORTED => error.Canceled,
                else => unexpectedError(err),
            };
        },
        else => {
            return switch (err) {
                .SUCCESS => unreachable,
                .CONNREFUSED => error.ConnectionRefused,
                .CONNRESET => error.ConnectionResetByPeer,
                .TIMEDOUT => error.Timeout,
                .NETUNREACH => error.NetworkUnreachable,
                .HOSTUNREACH, .HOSTDOWN => error.HostUnreachable,
                .NETDOWN => error.NetworkDown,
                .ACCES, .PERM => error.AccessDenied,
                .ADDRINUSE => error.AddressInUse,
                .ADDRNOTAVAIL => error.AddressUnavailable,
                .AFNOSUPPORT => error.AddressFamilyUnsupported,
                .ISCONN => error.AlreadyConnected,
                .ALREADY, .INPROGRESS => error.ConnectionPending,
                .AGAIN => error.WouldBlock, // Also: insufficient routing cache or no auto-assigned ports
                .NOTSOCK => error.FileDescriptorNotASocket,
                .NOENT => error.FileNotFound,
                .LOOP => error.SymLinkLoop,
                .NAMETOOLONG => error.NameTooLong,
                .NOTDIR => error.NotDir,
                .CANCELED => error.Canceled,
                else => |e| unexpectedError(e),
            };
        },
    }
}

// Winsock reports "the queued connection went away before we took it" as
// ECONNRESET, POSIX as ECONNABORTED; both collapse into ConnectionAborted.
pub fn errnoToAcceptError(err: E) AcceptError {
    switch (builtin.os.tag) {
        .windows => {
            return switch (err) {
                .EWOULDBLOCK => error.WouldBlock,
                .ECONNABORTED, .ECONNRESET => error.ConnectionAborted,
                .EMFILE => error.ProcessFdQuotaExceeded,
                .ENOBUFS => error.SystemResources,
                .ENOTSOCK => error.FileDescriptorNotASocket,
                .EINVAL => error.SocketNotListening,
                .EOPNOTSUPP => error.ProtocolFailure,
                .EPROTONOSUPPORT => error.ProtocolFailure,
                .ENETDOWN => error.NetworkDown,
                .OPERATION_ABORTED => error.Canceled,
                else => unexpectedError(err),
            };
        },
        else => {
            return switch (err) {
                .SUCCESS => unreachable,
                .AGAIN => error.WouldBlock,
                .CONNABORTED, .CONNRESET => error.ConnectionAborted,
                .MFILE => error.ProcessFdQuotaExceeded,
                .NFILE => error.SystemFdQuotaExceeded,
                .NOMEM, .NOBUFS => error.SystemResources,
                .NOTSOCK => error.FileDescriptorNotASocket,
                .INVAL => error.SocketNotListening,
                .OPNOTSUPP => error.OperationNotSupported,
                .PROTO => error.ProtocolFailure,
                .PERM => error.BlockedByFirewall,
                .NETDOWN => error.NetworkDown,
                .CANCELED => error.Canceled,
                else => |e| unexpectedError(e),
            };
        },
    }
}

pub fn errnoToRecvError(err: E) RecvError {
    switch (builtin.os.tag) {
        .windows => {
            return switch (err) {
                .EWOULDBLOCK => error.WouldBlock,
                .ECONNREFUSED => error.ConnectionRefused,
                .ECONNRESET, .ENETRESET => error.ConnectionResetByPeer,
                .ECONNABORTED => error.ConnectionAborted,
                .ETIMEDOUT => error.ConnectionTimedOut,
                .ENOTCONN => error.SocketNotConnected,
                .ENOTSOCK => error.FileDescriptorNotASocket,
                .ESHUTDOWN => error.SocketShutdown,
                .EOPNOTSUPP => error.OperationNotSupported,
                .ENETDOWN => error.NetworkDown,
                .EHOSTUNREACH, .ENETUNREACH => error.NetworkUnreachable,
                .EMSGSIZE => error.MessageOversize,
                .ENOBUFS => error.SystemResources,
                .OPERATION_ABORTED => error.Canceled,
                else => unexpectedError(err),
            };
        },
        else => {
            return switch (err) {
                .SUCCESS => unreachable,
                .AGAIN => error.WouldBlock,
                .CONNREFUSED => error.ConnectionRefused,
                .CONNRESET => error.ConnectionResetByPeer,
                .TIMEDOUT => error.ConnectionTimedOut,
                .NOTCONN => error.SocketNotConnected,
                .NOTSOCK => error.FileDescriptorNotASocket,
                .NETDOWN => error.NetworkDown,
                .HOSTUNREACH, .HOSTDOWN, .NETUNREACH => error.NetworkUnreachable,
                .NOBUFS, .NOMEM => error.SystemResources,
                .MSGSIZE => error.MessageOversize,
                // recvmsg with SCM_RIGHTS passes fds from the sender; if the
                // receiving process/system is at its fd quota, the kernel fails
                // the call with these errnos. Plain recv/recvfrom cannot
                // produce them.
                .MFILE => error.ProcessFdQuotaExceeded,
                .NFILE => error.SystemFdQuotaExceeded,
                .CANCELED => error.Canceled,
                else => |e| unexpectedError(e),
            };
        },
    }
}

pub fn errnoToSendError(err: E) SendError {
    switch (builtin.os.tag) {
        .windows => {
            return switch (err) {
                .EWOULDBLOCK => error.WouldBlock,
                .EACCES => error.AccessDenied,
                .ECONNRESET, .ENETRESET => error.ConnectionResetByPeer,
                .ECONNABORTED => error.ConnectionAborted,
                .ETIMEDOUT => error.ConnectionTimedOut,
                .ENOTCONN => error.SocketNotConnected,
                .ENOTSOCK => error.FileDescriptorNotASocket,
                .EMSGSIZE => error.MessageTooBig,
                .ESHUTDOWN => error.BrokenPipe,
                .ENETDOWN => error.NetworkDown,
                .ENETUNREACH => error.NetworkUnreachable,
                .EHOSTUNREACH => error.HostUnreachable,
                .EAFNOSUPPORT => error.AddressFamilyUnsupported,
                .EOPNOTSUPP => error.OperationNotSupported,
                .EADDRNOTAVAIL => error.AddressUnavailable,
                .ENOBUFS => error.SystemResources,
                .OPERATION_ABORTED => error.Canceled,
                else => unexpectedError(err),
            };
        },
        else => {
            return switch (err) {
                .SUCCESS => unreachable,
                .AGAIN => error.WouldBlock,
                .ACCES, .PERM => error.AccessDenied,
                .ADDRNOTAVAIL => error.AddressUnavailable,
                .CONNRESET => error.ConnectionResetByPeer,
                .CONNABORTED => error.ConnectionAborted,
                .TIMEDOUT => error.ConnectionTimedOut,
                .NOTCONN => error.SocketNotConnected,
                .NOTSOCK => error.FileDescriptorNotASocket,
                .MSGSIZE => error.MessageTooBig,
                .OPNOTSUPP => error.OperationNotSupported,
                .PIPE => error.BrokenPipe,
                .NETUNREACH => error.NetworkUnreachable,
                .HOSTUNREACH, .HOSTDOWN => error.HostUnreachable,
                .NETDOWN => error.NetworkDown,
                .CONNREFUSED => error.ConnectionRefused,
                .AFNOSUPPORT => error.AddressFamilyUnsupported,
                .ALREADY => error.FastOpenAlreadyInProgress,
                .NOBUFS, .NOMEM => error.SystemResources,
                .CANCELED => error.Canceled,
                else => |e| unexpectedError(e),
            };
        },
    }
}

pub fn errnoToShutdownError(err: E) ShutdownError {
    switch (builtin.os.tag) {
        .windows => {
            return switch (err) {
                .ENOTCONN => error.SocketUnconnected,
                .ENOTSOCK => error.Unexpected,
                .ECONNABORTED => error.ConnectionAborted,
                .ECONNRESET => error.ConnectionResetByPeer,
                .ENETDOWN => error.NetworkDown,
                .OPERATION_ABORTED => error.Canceled,
                else => unexpectedError(err),
            };
        },
        else => {
            return switch (err) {
                .SUCCESS => unreachable,
                .NOTCONN => error.SocketUnconnected,
                .NOTSOCK => error.Unexpected,
                .CONNABORTED => error.ConnectionAborted,
                .CONNRESET => error.ConnectionResetByPeer,
                .NETDOWN => error.NetworkDown,
                .CANCELED => error.Canceled,
                else => |e| unexpectedError(e),
            };
        },
    }
}

pub fn errnoToOpenError(err: E) OpenError {
    switch (builtin.os.tag) {
        .windows => {
            return switch (err) {
                .EAFNOSUPPORT => error.AddressFamilyUnsupported,
                .EPROTONOSUPPORT => error.ProtocolNotSupported,
                .EMFILE => error.ProcessFdQuotaExceeded,
                .ENOBUFS => error.SystemResources,
                .OPERATION_ABORTED => error.Canceled,
                else => unexpectedError(err),
            };
        },
        else => {
            return switch (err) {
                .SUCCESS => unreachable,
                .ACCES => error.PermissionDenied,
                .AFNOSUPPORT => error.AddressFamilyUnsupported,
                .MFILE => error.ProcessFdQuotaExceeded,
                .NFILE => error.SystemFdQuotaExceeded,
                .NOBUFS, .NOMEM => error.SystemResources,
                .PROTONOSUPPORT => error.ProtocolNotSupported,
                .CANCELED => error.Canceled,
                else => |e| unexpectedError(e),
            };
        },
    }
}

pub const RecvFlags = packed struct {
    peek: bool = false,
    waitall: bool = false,
    oob: bool = false,
    /// On Linux, asks the kernel to report the full datagram length even when
    /// it's larger than the supplied buffer (so the caller can detect
    /// truncation). Silently ignored on platforms without MSG_TRUNC input
    /// semantics (e.g. Windows).
    trunc: bool = false,
    /// Fail with `error.WouldBlock` instead of waiting when nothing is queued.
    /// On POSIX this is MSG_DONTWAIT and holds regardless of the socket's
    /// blocking mode. Windows has no such flag, so there it relies on the
    /// socket being in FIONBIO mode. Backends that declare
    /// `supports_recv_dontwait` complete such an operation immediately instead
    /// of parking it on readiness.
    dontwait: bool = false,
};

fn recvFlagsToSys(flags: RecvFlags) c_int {
    var sys_flags: c_int = 0;
    const MSG = if (builtin.os.tag == .windows) windows.MSG else posix.system.MSG;
    if (flags.peek and @hasDecl(MSG, "PEEK")) sys_flags |= MSG.PEEK;
    if (flags.dontwait and @hasDecl(MSG, "DONTWAIT")) sys_flags |= MSG.DONTWAIT;
    if (flags.waitall and @hasDecl(MSG, "WAITALL")) sys_flags |= MSG.WAITALL;
    if (flags.oob and @hasDecl(MSG, "OOB")) sys_flags |= MSG.OOB;
    // MSG_TRUNC as an *input* flag is Linux-specific; on other POSIX systems
    // the constant exists only as an output flag. Restrict it to Linux to
    // avoid corrupting platforms that repurpose the bit.
    if (flags.trunc and builtin.os.tag == .linux) sys_flags |= MSG.TRUNC;
    return sys_flags;
}

pub const RecvError = error{
    WouldBlock,
    ConnectionRefused,
    ConnectionResetByPeer,
    ConnectionAborted,
    ConnectionTimedOut,
    SocketNotConnected,
    FileDescriptorNotASocket,
    SocketShutdown,
    OperationNotSupported,
    NetworkDown,
    NetworkUnreachable,
    SystemResources,
    MessageOversize,
    ProcessFdQuotaExceeded,
    SystemFdQuotaExceeded,
    Canceled,
    Unexpected,
};

pub fn recv(fd: fd_t, buffers: []iovec, flags: RecvFlags) RecvError!usize {
    if (buffers.len == 0) return 0;
    if (comptime sim_layer) {
        if (sim.isDgramFd(fd)) return error.OperationNotSupported;
    }

    const sys_flags = recvFlagsToSys(flags);

    switch (builtin.os.tag) {
        .windows => {
            var bytes_received: windows.DWORD = 0;
            var win_flags: windows.DWORD = @intCast(sys_flags);
            const rc = windows.WSARecv(
                fd,
                @ptrCast(buffers.ptr),
                @intCast(buffers.len),
                &bytes_received,
                &win_flags,
                null,
                null,
            );
            if (rc == windows.SOCKET_ERROR) {
                const err = windows.WSAGetLastError();
                return errnoToRecvError(err);
            }
            return bytes_received;
        },
        else => {
            if (buffers.len == 1) {
                // Single buffer - use recv/recvfrom
                const buffer = buffers[0];
                while (true) {
                    const rc = if (builtin.os.tag == .linux)
                        posix.system.recvfrom(fd, buffer.base, buffer.len, @intCast(sys_flags), null, null)
                    else
                        posix.system.recv(fd, buffer.base, buffer.len, @intCast(sys_flags));

                    switch (posix.errno(rc)) {
                        .SUCCESS => return @intCast(rc),
                        .INTR => continue,
                        else => |err| return errnoToRecvError(err),
                    }
                }
            } else {
                // Multiple buffers - use recvmsg
                var msg: posix.system.msghdr = .{
                    .name = null,
                    .namelen = 0,
                    .iov = buffers.ptr,
                    .iovlen = @intCast(buffers.len),
                    .control = null,
                    .controllen = 0,
                    .flags = 0,
                };

                while (true) {
                    const rc = posix.system.recvmsg(fd, &msg, @intCast(sys_flags));

                    switch (posix.errno(rc)) {
                        .SUCCESS => return @intCast(rc),
                        .INTR => continue,
                        else => |err| return errnoToRecvError(err),
                    }
                }
            }
        },
    }
}

pub const SendFlags = packed struct {
    no_signal: bool = true,
    confirm: bool = false,
    dont_route: bool = false,
    eor: bool = false,
    oob: bool = false,
    fastopen: bool = false,
    /// Fail with `error.WouldBlock` instead of blocking in the send syscall
    /// when the socket can't take the data. On POSIX this is MSG_DONTWAIT and
    /// holds regardless of the socket's blocking mode. Windows has no such
    /// flag, so there it relies on the socket being in FIONBIO mode. It does
    /// not stop an operation from waiting for the socket to become writable.
    dontwait: bool = false,
};

fn sendFlagsToSys(flags: SendFlags) c_int {
    var sys_flags: c_int = 0;
    const MSG = posix.system.MSG;
    if (builtin.os.tag != .windows and flags.no_signal) sys_flags |= MSG.NOSIGNAL;
    if (flags.confirm and @hasDecl(MSG, "CONFIRM")) sys_flags |= MSG.CONFIRM;
    if (flags.dont_route and @hasDecl(MSG, "DONTROUTE")) sys_flags |= MSG.DONTROUTE;
    if (flags.eor and @hasDecl(MSG, "EOR")) sys_flags |= MSG.EOR;
    if (flags.oob and @hasDecl(MSG, "OOB")) sys_flags |= MSG.OOB;
    if (flags.fastopen and @hasDecl(MSG, "FASTOPEN")) sys_flags |= MSG.FASTOPEN;
    if (flags.dontwait and @hasDecl(MSG, "DONTWAIT")) sys_flags |= MSG.DONTWAIT;
    return sys_flags;
}

pub const SendError = error{
    WouldBlock,
    AccessDenied,
    /// EADDRNOTAVAIL: the source address the send asked for (`sendmsg` with a
    /// source address) is not one this host or socket can send from. ENet
    /// treats it as transient and drops that one datagram, so it must not
    /// fall into `Unexpected`.
    AddressUnavailable,
    ConnectionResetByPeer,
    ConnectionAborted,
    ConnectionTimedOut,
    SocketNotConnected,
    FileDescriptorNotASocket,
    MessageTooBig,
    BrokenPipe,
    NetworkUnreachable,
    HostUnreachable,
    NetworkDown,
    ConnectionRefused,
    AddressFamilyUnsupported,
    FastOpenAlreadyInProgress,
    OperationNotSupported,
    SystemResources,
    Canceled,
    Unexpected,
};

pub fn send(fd: fd_t, buffers: []const iovec_const, flags: SendFlags) SendError!usize {
    if (buffers.len == 0) return 0;
    if (comptime sim_layer) {
        if (sim.isDgramFd(fd)) return error.OperationNotSupported;
    }

    const sys_flags = sendFlagsToSys(flags);

    switch (builtin.os.tag) {
        .windows => {
            var bytes_sent: windows.DWORD = 0;
            const rc = windows.WSASend(
                fd,
                @ptrCast(@constCast(buffers.ptr)),
                @intCast(buffers.len),
                &bytes_sent,
                @intCast(sys_flags),
                null,
                null,
            );
            if (rc == windows.SOCKET_ERROR) {
                const err = windows.WSAGetLastError();
                return errnoToSendError(err);
            }
            return bytes_sent;
        },
        else => {
            if (buffers.len == 1) {
                // Single buffer - use send/sendto
                const buffer = buffers[0];
                while (true) {
                    const rc = if (builtin.os.tag == .linux)
                        posix.system.sendto(fd, buffer.base, buffer.len, @intCast(sys_flags), null, 0)
                    else
                        posix.system.send(fd, buffer.base, buffer.len, @intCast(sys_flags));

                    switch (posix.errno(rc)) {
                        .SUCCESS => return @intCast(rc),
                        .INTR => continue,
                        else => |err| return errnoToSendError(err),
                    }
                }
            } else {
                // Multiple buffers - use sendmsg
                var msg: posix.system.msghdr_const = .{
                    .name = null,
                    .namelen = 0,
                    .iov = buffers.ptr,
                    .iovlen = @intCast(buffers.len),
                    .control = null,
                    .controllen = 0,
                    .flags = 0,
                };

                while (true) {
                    const rc = posix.system.sendmsg(fd, &msg, @intCast(sys_flags));

                    switch (posix.errno(rc)) {
                        .SUCCESS => return @intCast(rc),
                        .INTR => continue,
                        else => |err| return errnoToSendError(err),
                    }
                }
            }
        },
    }
}

pub fn recvfrom(
    fd: fd_t,
    buffers: []iovec,
    flags: RecvFlags,
    addr: ?*sockaddr,
    addr_len: ?*socklen_t,
) RecvError!usize {
    if (buffers.len == 0) return 0;
    if (comptime sim_layer) {
        if (sim.isSimFd(fd)) {
            const r = try simRecv(fd, buffers, flags);
            if (addr) |a| writeSimAddr(r.from, a, addr_len orelse sim.panic("sim: recvfrom with an address but no length", .{}));
            return simRecvLen(r, flags);
        }
    }

    var sys_flags = recvFlagsToSys(flags);

    switch (builtin.os.tag) {
        .windows => {
            var bytes_received: windows.DWORD = 0;
            var from_len: c_int = if (addr_len) |len| @intCast(len.*) else 0;
            const rc = windows.WSARecvFrom(
                fd,
                @ptrCast(buffers.ptr),
                @intCast(buffers.len),
                &bytes_received,
                @ptrCast(&sys_flags),
                addr,
                if (addr_len != null) &from_len else null,
                null,
                null,
            );
            if (addr_len) |len| len.* = @intCast(from_len);
            if (rc == windows.SOCKET_ERROR) {
                const err = windows.WSAGetLastError();
                return errnoToRecvError(err);
            }
            return bytes_received;
        },
        else => {
            if (buffers.len == 1) {
                // Single buffer: use recvfrom directly
                const buffer = buffers[0];
                while (true) {
                    const rc = posix.system.recvfrom(
                        fd,
                        buffer.base,
                        buffer.len,
                        @intCast(sys_flags),
                        addr,
                        addr_len,
                    );

                    switch (posix.errno(rc)) {
                        .SUCCESS => return @intCast(rc),
                        .INTR => continue,
                        else => |err| return errnoToRecvError(err),
                    }
                }
            } else {
                // Multiple buffers: use recvmsg
                var msg: posix.system.msghdr = .{
                    .name = addr,
                    .namelen = if (addr_len) |len| len.* else 0,
                    .iov = buffers.ptr,
                    .iovlen = @intCast(buffers.len),
                    .control = null,
                    .controllen = 0,
                    .flags = 0,
                };

                while (true) {
                    const rc = posix.system.recvmsg(fd, &msg, @intCast(sys_flags));

                    switch (posix.errno(rc)) {
                        .SUCCESS => {
                            if (addr_len) |len| len.* = msg.namelen;
                            return @intCast(rc);
                        },
                        .INTR => continue,
                        else => |err| return errnoToRecvError(err),
                    }
                }
            }
        },
    }
}

pub fn sendto(
    fd: fd_t,
    buffers: []const iovec_const,
    flags: SendFlags,
    addr: *const sockaddr,
    addr_len: socklen_t,
) SendError!usize {
    if (buffers.len == 0) return 0;
    if (comptime sim_layer) {
        if (sim.isSimFd(fd)) return simSend(fd, buffers, simAddr("sendto", addr, addr_len), null);
    }

    const sys_flags = sendFlagsToSys(flags);

    switch (builtin.os.tag) {
        .windows => {
            var bytes_sent: windows.DWORD = 0;
            const rc = windows.WSASendTo(
                fd,
                @ptrCast(@constCast(buffers.ptr)),
                @intCast(buffers.len),
                &bytes_sent,
                @intCast(sys_flags),
                addr,
                @intCast(addr_len),
                null,
                null,
            );
            if (rc == windows.SOCKET_ERROR) {
                const err = windows.WSAGetLastError();
                return errnoToSendError(err);
            }
            return bytes_sent;
        },
        else => {
            if (buffers.len == 1) {
                // Single buffer - use sendto
                const buffer = buffers[0];
                while (true) {
                    const rc = posix.system.sendto(
                        fd,
                        buffer.base,
                        buffer.len,
                        @intCast(sys_flags),
                        addr,
                        addr_len,
                    );

                    switch (posix.errno(rc)) {
                        .SUCCESS => return @intCast(rc),
                        .INTR => continue,
                        else => |err| return errnoToSendError(err),
                    }
                }
            } else {
                // Multiple buffers - use sendmsg
                var msg: posix.system.msghdr_const = .{
                    .name = addr,
                    .namelen = addr_len,
                    .iov = buffers.ptr,
                    .iovlen = @intCast(buffers.len),
                    .control = null,
                    .controllen = 0,
                    .flags = 0,
                };

                while (true) {
                    const rc = posix.system.sendmsg(fd, &msg, @intCast(sys_flags));

                    switch (posix.errno(rc)) {
                        .SUCCESS => return @intCast(rc),
                        .INTR => continue,
                        else => |err| return errnoToSendError(err),
                    }
                }
            }
        },
    }
}

pub const RecvMsgResult = struct {
    len: usize,
    flags: u32,
    /// Actual control-data length written by the kernel into the caller's
    /// control buffer. 0 when no control buffer was supplied, or on platforms
    /// where control messages are not supported.
    controllen: usize,
};

pub fn recvmsg(
    fd: fd_t,
    buffers: []iovec,
    flags: RecvFlags,
    addr: ?*sockaddr,
    addr_len: ?*socklen_t,
    control: ?[]u8,
) RecvError!RecvMsgResult {
    if (buffers.len == 0) return .{ .len = 0, .flags = 0, .controllen = 0 };
    if (comptime sim_layer) {
        if (sim.isSimFd(fd)) {
            // The sim does not write control messages: `recvDatagram`
            // carries the destination address as a typed value instead.
            if (control != null and sim.dgramDestinationAddressEnabled(fd)) {
                sim.panic("sim: recvmsg with a control buffer on sim fd {d}, which asked for destination addresses: use recvDatagram", .{fd});
            }
            const r = try simRecv(fd, buffers, flags);
            if (addr) |a| writeSimAddr(r.from, a, addr_len orelse sim.panic("sim: recvmsg with an address but no length", .{}));
            return .{
                .len = simRecvLen(r, flags),
                .flags = if (r.copied < r.len) posix.system.MSG.TRUNC else 0,
                .controllen = 0,
            };
        }
    }

    const sys_flags = recvFlagsToSys(flags);

    switch (builtin.os.tag) {
        .windows => {
            // Control messages are POSIX-specific and not supported on Windows
            if (control != null and control.?.len > 0) {
                return error.OperationNotSupported;
            }

            // Emulate with recvfrom or recv
            const bytes_read = if (addr != null)
                try recvfrom(fd, buffers, flags, addr, addr_len)
            else
                try recv(fd, buffers, flags);

            return .{
                .len = bytes_read,
                .flags = 0,
                .controllen = 0,
            };
        },
        else => {
            var msg: posix.system.msghdr = .{
                .name = if (addr) |a| @ptrCast(a) else null,
                .namelen = if (addr_len) |len| len.* else 0,
                .iov = buffers.ptr,
                .iovlen = @intCast(buffers.len),
                .control = if (control) |ctl| ctl.ptr else null,
                .controllen = if (control) |ctl| @intCast(ctl.len) else 0,
                .flags = 0,
            };

            const has_cmsg_cloexec = @hasDecl(posix.system.MSG, "CMSG_CLOEXEC");
            const cloexec_flag: c_int = if (has_cmsg_cloexec and control != null) posix.system.MSG.CMSG_CLOEXEC else 0;

            while (true) {
                const rc = posix.system.recvmsg(fd, &msg, @intCast(sys_flags | cloexec_flag));

                switch (posix.errno(rc)) {
                    .SUCCESS => {
                        if (addr_len) |len| len.* = msg.namelen;
                        if (!has_cmsg_cloexec and msg.controllen > 0) setReceivedFdsCloexec(control.?[0..msg.controllen]);
                        return .{
                            .len = @intCast(rc),
                            .flags = @intCast(msg.flags),
                            .controllen = @intCast(msg.controllen),
                        };
                    },
                    .INTR => continue,
                    else => |err| return errnoToRecvError(err),
                }
            }
        },
    }
}

/// Alignment of control messages and their data (`CMSG_ALIGN`).
pub const cmsg_align = if (builtin.os.tag.isDarwin()) @sizeOf(u32) else @sizeOf(usize);

/// Offset of a control message's data from its header (`CMSG_DATA`).
pub const cmsg_data_offset = if (builtin.os.tag == .windows)
    0
else
    std.mem.alignForward(usize, @sizeOf(posix.system.cmsghdr), cmsg_align);

/// Marks the descriptors in received SCM_RIGHTS messages close-on-exec, for
/// systems without MSG_CMSG_CLOEXEC.
fn setReceivedFdsCloexec(control: []const u8) void {
    const cmsghdr = posix.system.cmsghdr;
    const data_offset = cmsg_data_offset;
    var offset: usize = 0;
    while (offset + @sizeOf(cmsghdr) <= control.len) {
        const hdr = std.mem.bytesToValue(cmsghdr, control[offset..][0..@sizeOf(cmsghdr)]);
        if (hdr.len < data_offset or hdr.len > control.len - offset) break;
        if (hdr.level == SOL.SOCKET and hdr.type == posix.system.SCM.RIGHTS) {
            const fds = control[offset + data_offset .. offset + hdr.len];
            var i: usize = 0;
            while (i + @sizeOf(fd_t) <= fds.len) : (i += @sizeOf(fd_t)) {
                posix.setCloexec(std.mem.bytesToValue(fd_t, fds[i..][0..@sizeOf(fd_t)])) catch {};
            }
        }
        offset += std.mem.alignForward(usize, hdr.len, cmsg_align);
    }
}

pub const has_recvmmsg = switch (builtin.os.tag) {
    .linux, .freebsd, .netbsd => true,
    else => false,
};

pub const has_sendmmsg = switch (builtin.os.tag) {
    .linux, .freebsd, .netbsd => true,
    else => false,
};

pub const mmsghdr = switch (builtin.os.tag) {
    .linux, .netbsd => posix.system.mmsghdr,
    // msg_len is ssize_t on FreeBSD; std.c declares it as u32.
    .freebsd => extern struct {
        hdr: posix.system.msghdr,
        len: isize,
    },
    else => struct {},
};

pub fn recvmmsg(
    fd: fd_t,
    msgvec: [*]mmsghdr,
    vlen: u32,
    flags: RecvFlags,
) RecvError!u32 {
    const sys_flags = recvFlagsToSys(flags);

    while (true) {
        const rc = recvmmsg_syscall(fd, msgvec, vlen, @intCast(sys_flags));

        switch (posix.errno(rc)) {
            .SUCCESS => return @intCast(rc),
            .INTR => continue,
            else => |err| return errnoToRecvError(err),
        }
    }
}

fn recvmmsg_syscall(fd: fd_t, msgvec: [*]mmsghdr, vlen: u32, flags: c_int) switch (builtin.os.tag) {
    .linux => usize,
    .freebsd => isize,
    else => c_int,
} {
    switch (builtin.os.tag) {
        .linux => return posix.system.recvmmsg(fd, msgvec, vlen, @intCast(flags), null),
        .freebsd, .netbsd => return mmsg_libc.recvmmsg(fd, msgvec, vlen, @bitCast(flags), null),
        else => unreachable,
    }
}

fn sendmmsg_syscall(fd: fd_t, msgvec: [*]mmsghdr, vlen: u32, flags: c_int) switch (builtin.os.tag) {
    .linux => usize,
    .freebsd => isize,
    else => c_int,
} {
    switch (builtin.os.tag) {
        .linux => return posix.system.sendmmsg(fd, msgvec, vlen, @intCast(flags)),
        .freebsd, .netbsd => return mmsg_libc.sendmmsg(fd, msgvec, vlen, @bitCast(flags)),
        else => unreachable,
    }
}

/// As each libc declares them in sys/socket.h.
const mmsg_libc = switch (builtin.os.tag) {
    .freebsd => struct {
        extern "c" fn recvmmsg(sockfd: fd_t, msgvec: [*]mmsghdr, vlen: usize, flags: c_int, timeout: ?*const posix.system.timespec) isize;
        extern "c" fn sendmmsg(sockfd: fd_t, msgvec: [*]mmsghdr, vlen: usize, flags: c_int) isize;
    },
    .netbsd => struct {
        extern "c" fn recvmmsg(sockfd: fd_t, msgvec: [*]mmsghdr, vlen: c_uint, flags: c_uint, timeout: ?*posix.system.timespec) c_int;
        extern "c" fn sendmmsg(sockfd: fd_t, msgvec: [*]mmsghdr, vlen: c_uint, flags: c_uint) c_int;
    },
    else => struct {},
};

pub fn sendmmsg(
    fd: fd_t,
    msgvec: [*]mmsghdr,
    vlen: u32,
    flags: SendFlags,
) SendError!u32 {
    const sys_flags = sendFlagsToSys(flags);

    while (true) {
        const rc = sendmmsg_syscall(fd, msgvec, vlen, sys_flags);

        switch (posix.errno(rc)) {
            .SUCCESS => return @intCast(rc),
            .INTR => continue,
            else => |err| return errnoToSendError(err),
        }
    }
}

pub fn sendmsg(
    fd: fd_t,
    buffers: []const iovec_const,
    flags: SendFlags,
    addr: ?*const sockaddr,
    addr_len: socklen_t,
    control: ?[]const u8,
) SendError!usize {
    if (buffers.len == 0) return 0;
    if (comptime sim_layer) {
        if (sim.isSimFd(fd)) {
            if (control != null and control.?.len > 0) {
                sim.panic("sim: sendmsg with control data on sim fd {d}: use sendDatagram for a source address", .{fd});
            }
            const a = addr orelse sim.panic("sim: sendmsg with no address on sim fd {d}: connected UDP is not simulated", .{fd});
            return simSend(fd, buffers, simAddr("sendmsg", a, addr_len), null);
        }
    }

    const sys_flags = sendFlagsToSys(flags);

    switch (builtin.os.tag) {
        .windows => {
            // Control messages are POSIX-specific and not supported on Windows
            if (control != null and control.?.len > 0) {
                return error.OperationNotSupported;
            }

            // Emulate with sendto or send
            return if (addr) |a|
                try sendto(fd, buffers, flags, a, addr_len)
            else
                try send(fd, buffers, flags);
        },
        else => {
            var msg: posix.system.msghdr_const = .{
                .name = if (addr) |a| @ptrCast(a) else null,
                .namelen = addr_len,
                .iov = buffers.ptr,
                .iovlen = @intCast(buffers.len),
                .control = if (control) |ctl| ctl.ptr else null,
                .controllen = if (control) |ctl| @intCast(ctl.len) else 0,
                .flags = 0,
            };

            while (true) {
                const rc = posix.system.sendmsg(fd, &msg, @intCast(sys_flags));

                switch (posix.errno(rc)) {
                    .SUCCESS => return @intCast(rc),
                    .INTR => continue,
                    else => |err| return errnoToSendError(err),
                }
            }
        },
    }
}

// ---- Datagram metadata, in every build (sim network design rev 3, 3.1) ----
//
// A consumer that needs a datagram's destination address, or must send from
// a chosen source address (ENet's bridge in hotseat), uses these typed calls
// instead of building control messages itself. They are the same calls in a
// build with sim and without: under sim, during a run, a sim fd takes the sim
// branch; any other fd is the syscall. POSIX only for now: on Windows each is
// a compile error, so no untested Windows code ships.

/// Sets or clears the socket's non-blocking mode (`O_NONBLOCK`).
pub fn setNonBlocking(fd: fd_t, on: bool) error{Unexpected}!void {
    if (comptime builtin.os.tag == .windows) @compileError("setNonBlocking: not on Windows yet");
    if (comptime sim_layer) {
        if (sim.isSimFd(fd)) return sim.dgramSetNonBlocking(fd, on);
    }
    const fl = posix.system.fcntl(fd, posix.system.F.GETFL, @as(c_int, 0));
    switch (posix.errno(fl)) {
        .SUCCESS => {},
        else => |err| return unexpectedError(err),
    }
    // `fcntl` returns `c_int` through libc and `usize` as a raw Linux
    // syscall: the bit takes whichever type it is.
    const bit: @TypeOf(fl) = 1 << @bitOffsetOf(posix.system.O, "NONBLOCK");
    const new_flags = if (on) fl | bit else fl & ~bit;
    switch (posix.errno(posix.system.fcntl(fd, posix.system.F.SETFL, new_flags))) {
        .SUCCESS => {},
        else => |err| return unexpectedError(err),
    }
}

/// Whether the socket is in non-blocking mode. POSIX only: Windows cannot
/// read a socket's blocking mode back.
pub fn isNonBlocking(fd: fd_t) error{Unexpected}!bool {
    if (comptime builtin.os.tag == .windows) @compileError("isNonBlocking: Windows cannot read a socket's blocking mode back");
    if (comptime sim_layer) {
        if (sim.isSimFd(fd)) return sim.dgramIsNonBlocking(fd);
    }
    const fl = posix.system.fcntl(fd, posix.system.F.GETFL, @as(c_int, 0));
    switch (posix.errno(fl)) {
        .SUCCESS => {},
        else => |err| return unexpectedError(err),
    }
    const bit: @TypeOf(fl) = 1 << @bitOffsetOf(posix.system.O, "NONBLOCK");
    return fl & bit != 0;
}

/// Asks the kernel to report each datagram's destination address, which
/// `recvDatagram` then returns as `to`: `IP_PKTINFO` on Linux,
/// `IP_RECVDSTADDR` on macOS and the BSDs.
pub fn enableDestinationAddress(fd: fd_t) SetsockoptError!void {
    if (comptime builtin.os.tag == .windows) @compileError("enableDestinationAddress: not on Windows yet");
    if (comptime sim_layer) {
        if (sim.isSimFd(fd)) return sim.dgramEnableDestinationAddress(fd);
    }
    const one: c_int = 1;
    return switch (builtin.os.tag) {
        .linux => setsockopt(fd, IPPROTO.IP, posix.system.IP.PKTINFO, std.mem.asBytes(&one)),
        else => setsockopt(fd, IPPROTO.IP, posix.system.IP.RECVDSTADDR, std.mem.asBytes(&one)),
    };
}

pub const DatagramInfo = struct {
    /// Bytes copied into the buffers. On Linux with `flags.trunc`, the
    /// datagram's full length instead (other systems ignore that flag).
    len: usize,
    /// The datagram did not fit the buffers. The rest of it is gone: the next
    /// receive is the next datagram.
    truncated: bool,
    /// The address the datagram was sent to, after `enableDestinationAddress`;
    /// null otherwise.
    to: ?[4]u8,
};

/// Receives one IPv4 datagram, with its source (`from`, if given) and its
/// destination address (`DatagramInfo.to`). With no buffers it receives
/// nothing and returns a zero length, as `recvmsg` here does: to take an
/// empty datagram, pass one empty buffer.
pub fn recvDatagram(fd: fd_t, buffers: []iovec, flags: RecvFlags, from: ?*sockaddr.in) RecvError!DatagramInfo {
    if (comptime builtin.os.tag == .windows) @compileError("recvDatagram: not on Windows yet");
    if (buffers.len == 0) return .{ .len = 0, .truncated = false, .to = null };
    if (comptime sim_layer) {
        if (sim.isSimFd(fd)) {
            const r = try simRecv(fd, buffers, flags);
            if (from) |f| f.* = simSockaddr(r.from);
            return .{ .len = simRecvLen(r, flags), .truncated = r.copied < r.len, .to = r.to };
        }
    }
    var control: [128]u8 align(@alignOf(posix.system.cmsghdr)) = undefined;
    var name: sockaddr.in = undefined;
    var name_len: socklen_t = @sizeOf(sockaddr.in);
    const r = try recvmsg(fd, buffers, flags, @ptrCast(&name), &name_len, &control);
    if (from) |f| f.* = name;
    return .{
        .len = r.len,
        .truncated = r.flags & posix.system.MSG.TRUNC != 0,
        .to = destinationFromControl(control[0..r.controllen]),
    };
}

/// Sends one IPv4 datagram to `to`. With `from`, it leaves from that
/// address (`IP_PKTINFO`'s `ipi_spec_dst` on Linux and macOS). An address
/// the host does not have fails: `error.AddressUnavailable` on macOS,
/// `error.NetworkUnreachable` on Linux. Linux sends from any address it has,
/// even one the socket is not bound to. With no
/// buffers it sends nothing and returns 0, as `sendto` here does: to send an
/// empty datagram, pass one empty buffer.
pub fn sendDatagram(fd: fd_t, buffers: []const iovec_const, flags: SendFlags, to: *const sockaddr.in, from: ?[4]u8) SendError!usize {
    if (comptime builtin.os.tag == .windows) @compileError("sendDatagram: not on Windows yet");
    if (buffers.len == 0) return 0;
    if (comptime sim_layer) {
        if (sim.isSimFd(fd)) return simSend(fd, buffers, simAddrIn("sendDatagram", to), from);
    }
    const f = from orelse return sendto(fd, buffers, flags, @ptrCast(to), @sizeOf(sockaddr.in));
    var control: [64]u8 align(@alignOf(posix.system.cmsghdr)) = @splat(0);
    const used = sourceAddressControl(&control, f);
    return sendmsg(fd, buffers, flags, @ptrCast(to), @sizeOf(sockaddr.in), control[0..used]);
}

/// `struct in_pktinfo` as Linux and macOS lay it out.
const InPktinfo = extern struct {
    ifindex: u32,
    spec_dst: [4]u8,
    addr: [4]u8,
};

/// Writes one `IP_PKTINFO` control message asking for source address `from`;
/// returns its space.
fn sourceAddressControl(buf: []align(@alignOf(posix.system.cmsghdr)) u8, from: [4]u8) usize {
    const info: InPktinfo = .{ .ifindex = 0, .spec_dst = from, .addr = @splat(0) };
    const hdr: posix.system.cmsghdr = .{
        .len = @intCast(cmsg_data_offset + @sizeOf(InPktinfo)),
        .level = IPPROTO.IP,
        .type = posix.system.IP.PKTINFO,
    };
    @memcpy(buf[0..@sizeOf(posix.system.cmsghdr)], std.mem.asBytes(&hdr));
    @memcpy(buf[cmsg_data_offset..][0..@sizeOf(InPktinfo)], std.mem.asBytes(&info));
    return std.mem.alignForward(usize, cmsg_data_offset + @sizeOf(InPktinfo), cmsg_align);
}

/// The destination address in a received control message, if one is there:
/// `IP_PKTINFO`'s `ipi_addr`, or `IP_RECVDSTADDR`'s address.
fn destinationFromControl(control: []const u8) ?[4]u8 {
    const cmsghdr = posix.system.cmsghdr;
    var offset: usize = 0;
    while (offset + @sizeOf(cmsghdr) <= control.len) {
        const hdr = std.mem.bytesToValue(cmsghdr, control[offset..][0..@sizeOf(cmsghdr)]);
        if (hdr.len < cmsg_data_offset or hdr.len > control.len - offset) return null;
        const data = control[offset + cmsg_data_offset .. offset + hdr.len];
        if (hdr.level == IPPROTO.IP) {
            if (hdr.type == posix.system.IP.PKTINFO and data.len >= @sizeOf(InPktinfo)) {
                return std.mem.bytesToValue(InPktinfo, data[0..@sizeOf(InPktinfo)]).addr;
            }
            if (builtin.os.tag != .linux and hdr.type == posix.system.IP.RECVDSTADDR and data.len >= 4) {
                return data[0..4].*;
            }
        }
        offset += std.mem.alignForward(usize, hdr.len, cmsg_align);
    }
    return null;
}

pub const PollOutcome = union(enum) {
    /// The `revents` the kernel returned.
    ready: i16,
    timed_out,
    /// A signal interrupted the wait (EINTR). `poll` retries instead.
    interrupted,
};

/// One `poll` of one fd, for a caller that must see a signal: EINTR returns
/// `.interrupted` instead of being retried. ENet's `enet_socket_wait` reports
/// it as `ENET_SOCKET_WAIT_INTERRUPT`. Under sim, a sim fd takes a zero
/// timeout only (it can never be interrupted then); a positive one refuses.
pub fn pollInterruptible(fd: fd_t, events: i16, timeout_ms: i32) PollError!PollOutcome {
    if (comptime builtin.os.tag == .windows) @compileError("pollInterruptible: not on Windows yet");
    var fds = [1]pollfd{.{ .fd = fd, .events = events, .revents = 0 }};
    if (comptime sim_layer) {
        if (simPoll(&fds, timeout_ms)) |n| return if (n == 0) .timed_out else .{ .ready = fds[0].revents };
    }
    const rc = posix.system.poll(&fds, 1, timeout_ms);
    return switch (posix.errno(rc)) {
        .SUCCESS => if (rc == 0) .timed_out else .{ .ready = fds[0].revents },
        .INTR => .interrupted,
        .INVAL, .NOMEM => error.SystemResources,
        else => |err| unexpectedError(err),
    };
}

// ---- The sim branch's helpers (only with sim, never on Windows) ----

/// `poll` over sim fds: null when none of `fds` is a sim fd. A sim fd is
/// readable when a datagram is queued, and always writable. Nothing can land
/// while the sim thread waits, so only a zero timeout is allowed, and real and
/// sim fds cannot be mixed in one call.
fn simPoll(fds: []pollfd, timeout: i32) ?usize {
    var sim_fds: usize = 0;
    for (fds) |f| {
        if (sim.isSimFd(f.fd)) sim_fds += 1;
    }
    if (sim_fds == 0) return null;
    if (sim_fds != fds.len) sim.panic("sim: a poll of {d} sim fds and {d} real fds: they cannot be mixed", .{ sim_fds, fds.len - sim_fds });
    if (timeout != 0) sim.panic("sim: a poll with a timeout of {d} ms on sim fds: only a zero timeout is allowed", .{timeout});
    var ready: usize = 0;
    for (fds) |*f| {
        f.revents = 0;
        if (f.events & POLL.IN != 0 and sim.dgramReadable(f.fd)) f.revents |= POLL.IN;
        if (f.events & POLL.OUT != 0) f.revents |= POLL.OUT;
        if (f.revents != 0) ready += 1;
    }
    return ready;
}

const sim_max_iovecs = 16;

fn simAddr(comptime what: []const u8, addr: *const sockaddr, len: socklen_t) sim.Ip4 {
    // The family is checked once, in `simAddrIn`, before anything else reads
    // the address as IPv4.
    if (len < @sizeOf(sockaddr.in)) sim.panic("sim: " ++ what ++ " with an address of {d} bytes", .{len});
    return simAddrIn(what, @ptrCast(@alignCast(addr)));
}

fn simAddrIn(comptime what: []const u8, a: *const sockaddr.in) sim.Ip4 {
    if (a.family != AF.INET) sim.panic("sim: " ++ what ++ " with address family {d}: only IPv4 is simulated", .{a.family});
    return .{ .ip = @bitCast(a.addr), .port = std.mem.bigToNative(u16, a.port) };
}

fn simSockaddr(a: sim.Ip4) sockaddr.in {
    return .{ .port = std.mem.nativeToBig(u16, a.port), .addr = @bitCast(a.ip) };
}

/// As the kernel does: copies as much of the address as fits, and sets the
/// length to the address's full size.
fn writeSimAddr(a: sim.Ip4, out: *sockaddr, out_len: *socklen_t) void {
    const in = simSockaddr(a);
    const n = @min(@as(usize, @intCast(out_len.*)), @sizeOf(sockaddr.in));
    @memcpy(@as([*]u8, @ptrCast(out))[0..n], std.mem.asBytes(&in)[0..n]);
    out_len.* = @sizeOf(sockaddr.in);
}

fn simRecv(fd: fd_t, buffers: []iovec, flags: RecvFlags) RecvError!sim.Received {
    if (flags.peek) sim.panic("sim: MSG_PEEK on sim fd {d} is not simulated", .{fd});
    if (flags.waitall or flags.oob) sim.panic("sim: MSG_WAITALL or MSG_OOB on sim fd {d} is not simulated", .{fd});
    if (buffers.len > sim_max_iovecs) sim.panic("sim: a receive with {d} buffers (max {d})", .{ buffers.len, sim_max_iovecs });
    var sl: [sim_max_iovecs][]u8 = undefined;
    for (buffers, 0..) |b, i| sl[i] = b.base[0..b.len];
    return sim.dgramRecv(fd, sl[0..buffers.len], flags.dontwait);
}

/// The length a receive reports, as the host's kernel does: Linux gives the
/// full datagram length for `MSG_TRUNC`; other systems ignore that flag.
fn simRecvLen(r: sim.Received, flags: RecvFlags) usize {
    return if (flags.trunc and builtin.os.tag == .linux) r.len else r.copied;
}

fn simSend(fd: fd_t, buffers: []const iovec_const, to: sim.Ip4, from: ?[4]u8) SendError!usize {
    if (buffers.len > sim_max_iovecs) sim.panic("sim: a send with {d} buffers (max {d})", .{ buffers.len, sim_max_iovecs });
    var sl: [sim_max_iovecs][]const u8 = undefined;
    for (buffers, 0..) |b, i| sl[i] = b.base[0..b.len];
    return sim.dgramSend(fd, sl[0..buffers.len], to, from);
}

/// Creates a connected socket pair using loopback connection (for Windows async wakeup)
/// Returns [read_socket, write_socket] - writing to write_socket wakes up poll on read_socket
pub const CreateLoopbackSocketPairError = OpenError || BindError || ListenError || ConnectError || AcceptError || GetSockNameError;

pub fn createLoopbackSocketPair() CreateLoopbackSocketPairError![2]fd_t {
    ensureWSAInitialized();

    // Create a listening socket on loopback
    const listen_sock = try socket(.ipv4, .stream, .ip, .{ .nonblocking = true });
    errdefer close(listen_sock);

    // Bind to 127.0.0.1:0 (any available port)
    const bind_addr: posix.system.sockaddr.in = .{
        .family = AF.INET,
        .port = 0, // Let OS choose port
        .addr = 0x0100007F, // 127.0.0.1 in network byte order (little-endian)
        .zero = @splat(0),
    };
    try bind(listen_sock, @ptrCast(&bind_addr), @sizeOf(posix.system.sockaddr.in));

    // Listen for connections
    try listen(listen_sock, 1);

    // Get the actual bound address
    var actual_addr: sockaddr = undefined;
    var addr_len: socklen_t = @sizeOf(sockaddr);
    try getsockname(listen_sock, &actual_addr, &addr_len);

    // Create connecting socket
    const write_sock = try socket(.ipv4, .stream, .ip, .{ .nonblocking = true });
    errdefer close(write_sock);

    // Connect to the listening socket
    connect(write_sock, &actual_addr, addr_len) catch |err| {
        if (err != error.WouldBlock) return err;
        // WouldBlock is expected for non-blocking connect
    };

    // Accept the connection
    const read_sock = try accept(listen_sock, null, null, .{ .nonblocking = true });
    errdefer close(read_sock);

    // Close the listening socket - no longer needed
    close(listen_sock);

    return .{ read_sock, write_sock };
}

pub const addrinfo = switch (builtin.os.tag) {
    .windows => windows_addrinfo,
    else => std.c.addrinfo,
};

pub const AI = switch (builtin.os.tag) {
    .windows => windows.AI,
    else => std.c.AI,
};

pub const GetAddrInfoError = error{
    AddressFamilyUnsupported,
    TemporaryNameServerFailure,
    InvalidFlags,
    NameServerFailure,
    UnknownHostName,
    ServiceNotAvailable,
    SocketTypeNotSupported,
    SystemResources,
    Unexpected,
};

pub fn getaddrinfo(
    node: ?[*:0]const u8,
    service: ?[*:0]const u8,
    hints: ?*const addrinfo,
    res: *?*addrinfo,
) GetAddrInfoError!void {
    switch (builtin.os.tag) {
        .windows => {
            const rc = windows_extern.getaddrinfo(node, service, hints, res);
            if (rc != 0) {
                return errnoToGetAddrInfoError(rc);
            }
        },
        else => {
            const rc = std.c.getaddrinfo(node, service, hints, res);
            const rc_int: c_int = @backingInt(rc);
            if (rc_int != 0) {
                return errnoToGetAddrInfoError(rc);
            }
        },
    }
}

pub fn freeaddrinfo(res: *addrinfo) void {
    switch (builtin.os.tag) {
        .windows => {
            windows_extern.freeaddrinfo(res);
        },
        else => {
            std.c.freeaddrinfo(res);
        },
    }
}

fn errnoToGetAddrInfoError(err: anytype) GetAddrInfoError {
    switch (builtin.os.tag) {
        .windows => {
            const wsa_err: windows.WinsockError = @fromBackingInt(@intCast(@as(u16, @intCast(err))));
            return switch (wsa_err) {
                .EAFNOSUPPORT => error.AddressFamilyUnsupported,
                .EINVAL => error.InvalidFlags,
                .ESOCKTNOSUPPORT => error.SocketTypeNotSupported,
                .NO_DATA => error.UnknownHostName,
                .NO_RECOVERY => error.NameServerFailure,
                .NOTINITIALISED => error.SystemResources,
                .TRY_AGAIN => error.TemporaryNameServerFailure,
                .TYPE_NOT_FOUND => error.ServiceNotAvailable,
                .NOT_ENOUGH_MEMORY => error.SystemResources,
                else => unexpectedError(wsa_err),
            };
        },
        else => {
            // EAI_* error codes - err is std.c.EAI
            return switch (err) {
                .ADDRFAMILY => error.AddressFamilyUnsupported,
                .AGAIN => error.TemporaryNameServerFailure,
                .BADFLAGS => error.InvalidFlags,
                .FAIL => error.NameServerFailure,
                .FAMILY => error.AddressFamilyUnsupported,
                .MEMORY => error.SystemResources,
                .NODATA => error.UnknownHostName,
                .NONAME => error.UnknownHostName,
                .SERVICE => error.ServiceNotAvailable,
                .SOCKTYPE => error.SocketTypeNotSupported,
                .SYSTEM => {
                    // EAI.SYSTEM means we need to check errno
                    const errno_val: posix.system.E = @fromBackingInt(@intCast(std.c._errno().*));
                    return switch (errno_val) {
                        .SUCCESS => unreachable,
                        .NOMEM => error.SystemResources,
                        else => |e| unexpectedError(e),
                    };
                },
                else => {
                    log.err("getaddrinfo returned unexpected EAI error: {}", .{err});
                    return error.Unexpected;
                },
            };
        },
    }
}

// libc gives us if_nametoindex but not its inverse, so declare if_indextoname
// ourselves. The name buffer it writes into must hold at least IF_NAMESIZE bytes.
pub const IF_NAMESIZE = std.c.IFNAMESIZE;
extern "c" fn if_indextoname(ifindex: c_uint, ifname: [*]u8) ?[*:0]u8;

pub const InterfaceNameToIndexError = error{
    InterfaceNotFound,
    Canceled,
    Unexpected,
};

/// Resolve a network interface name to its index. Wraps if_nametoindex(3).
pub fn interfaceNameToIndex(name: [*:0]const u8) InterfaceNameToIndexError!u32 {
    // A quick libc call, not a blocking syscall SIGURG could interrupt, so the
    // most we can do for cancellation is honor a request that already arrived.
    try syscall_cancel.checkCanceled();
    const index = std.c.if_nametoindex(name);
    // if_nametoindex returns 0 for an unknown interface; errno is not reliably
    // set across platforms, so a zero result is all we have to go on.
    if (index == 0) return error.InterfaceNotFound;
    return @intCast(index);
}

pub const InterfaceIndexToNameError = error{
    InterfaceNotFound,
    Canceled,
    Unexpected,
};

/// Look up a network interface name by index, writing it into `buffer` (which
/// must be at least `IF_NAMESIZE` bytes) and returning its length. Wraps
/// if_indextoname(3).
pub fn interfaceIndexToName(index: u32, buffer: []u8) InterfaceIndexToNameError!usize {
    std.debug.assert(buffer.len >= IF_NAMESIZE);
    try syscall_cancel.checkCanceled();
    if (if_indextoname(@intCast(index), buffer.ptr) == null) return error.InterfaceNotFound;
    return std.mem.sliceTo(buffer[0..IF_NAMESIZE], 0).len;
}

test "interface index/name round-trip for loopback" {
    if (builtin.os.tag == .windows) return error.SkipZigTest;

    // The loopback interface exists on every system we run these tests on, but
    // its name varies (Linux "lo", the BSDs "lo0"), so resolve by index instead
    // of assuming a name: index 1 is the loopback on all of them.
    var buf: [IF_NAMESIZE]u8 = undefined;
    const name_len = interfaceIndexToName(1, &buf) catch |err| switch (err) {
        error.InterfaceNotFound => return error.SkipZigTest,
        else => return err,
    };
    const name = buf[0..name_len :0];

    const index = try interfaceNameToIndex(name);
    try std.testing.expectEqual(1, index);
}

test "interfaceNameToIndex on unknown interface returns InterfaceNotFound" {
    if (builtin.os.tag == .windows) return error.SkipZigTest;

    try std.testing.expectError(error.InterfaceNotFound, interfaceNameToIndex("zzznope0"));
}

test "getaddrinfo - resolve example.com" {
    ensureWSAInitialized();

    // Set up hints for IPv4 stream socket
    var hints: addrinfo = std.mem.zeroes(addrinfo);
    hints.family = AF.INET;
    hints.socktype = std.c.SOCK.STREAM;

    var res: ?*addrinfo = null;
    try getaddrinfo("example.com", "80", &hints, &res);
    defer if (res) |r| freeaddrinfo(r);

    // Verify we got results
    try std.testing.expect(res != null);

    // Verify we can iterate through results
    var current = res;
    var count: usize = 0;
    while (current) |info| {
        count += 1;
        try std.testing.expect(info.family == AF.INET);
        try std.testing.expect(info.socktype == std.c.SOCK.STREAM);
        current = info.next;
    }
    try std.testing.expect(count > 0);
}

test "Domain, Type, and Protocol conversions" {
    // Test raw socket type
    const raw_type = Type.raw;
    try std.testing.expectEqual(SOCK.RAW, raw_type.toPosix());

    // Test unspec domain
    const unspec_domain = Domain.unspec;
    try std.testing.expectEqual(AF.UNSPEC, unspec_domain.toPosix());

    // Test ICMP protocol
    const icmp_proto = Protocol.icmp;
    try std.testing.expectEqual(IPPROTO.ICMP, icmp_proto.toPosix());

    // Test fromPosix roundtrip for Type
    const t1 = Type.fromPosix(SOCK.STREAM);
    const t2 = Type.fromPosix(SOCK.DGRAM);
    const t3 = Type.fromPosix(SOCK.RAW);
    try std.testing.expectEqual(Type.stream, t1);
    try std.testing.expectEqual(Type.dgram, t2);
    try std.testing.expectEqual(Type.raw, t3);

    // Test fromPosix roundtrip for Domain
    const d0 = Domain.fromPosix(AF.UNSPEC);
    const d2 = Domain.fromPosix(AF.INET);
    try std.testing.expectEqual(Domain.unspec, d0);
    try std.testing.expectEqual(Domain.ipv4, d2);

    // Test fromPosix roundtrip for Protocol
    const p0 = Protocol.fromPosix(IPPROTO.IP);
    const p1 = Protocol.fromPosix(IPPROTO.ICMP);
    const p6 = Protocol.fromPosix(IPPROTO.TCP);
    const p17 = Protocol.fromPosix(IPPROTO.UDP);
    try std.testing.expectEqual(Protocol.ip, p0);
    try std.testing.expectEqual(Protocol.icmp, p1);
    try std.testing.expectEqual(Protocol.tcp, p6);
    try std.testing.expectEqual(Protocol.udp, p17);

    // Test unknown value uses catch-all _
    const t_unknown = Type.fromPosix(999);
    try std.testing.expectEqual(999, t_unknown.toPosix());

    const d_unknown = Domain.fromPosix(999);
    try std.testing.expectEqual(999, d_unknown.toPosix());

    const p_unknown = Protocol.fromPosix(999);
    try std.testing.expectEqual(999, p_unknown.toPosix());
}

test "socketpair AF_UNIX round-trip" {
    if (builtin.os.tag == .windows) return error.SkipZigTest;

    const fds = try socketpair(.unix, .stream, .ip, .{ .nonblocking = false });
    defer close(fds[0]);
    defer close(fds[1]);

    const msg = "ping";
    const send_iov = iovecConstFromSlice(msg);
    const n_written = try send(fds[0], (&send_iov)[0..1], .{});
    try std.testing.expectEqual(msg.len, n_written);

    var buf: [16]u8 = undefined;
    var recv_iov = iovecFromSlice(&buf);
    const n_read = try recv(fds[1], (&recv_iov)[0..1], .{});
    try std.testing.expectEqualStrings(msg, buf[0..n_read]);
}

test "recv on non-blocking socket without data returns WouldBlock" {
    if (builtin.os.tag == .windows) return error.SkipZigTest;

    const fds = try socketpair(.unix, .stream, .ip, .{ .nonblocking = true });
    defer close(fds[0]);
    defer close(fds[1]);

    var buf: [16]u8 = undefined;
    var recv_iov = iovecFromSlice(&buf);
    const result = recv(fds[0], (&recv_iov)[0..1], .{});

    try std.testing.expectError(error.WouldBlock, result);
}

test "a send that fails with EADDRNOTAVAIL is AddressUnavailable, not Unexpected" {
    const e: E = if (builtin.os.tag == .windows) .EADDRNOTAVAIL else .ADDRNOTAVAIL;
    try std.testing.expectEqual(error.AddressUnavailable, errnoToSendError(e));
}

const real_dgram = struct {
    fn open() !fd_t {
        return socket(.ipv4, .dgram, .ip, .{ .nonblocking = true });
    }

    fn in4(ip: [4]u8, port: u16) sockaddr.in {
        return .{ .port = std.mem.nativeToBig(u16, port), .addr = @bitCast(ip) };
    }

    fn bindAt(fd: fd_t, ip: [4]u8) !u16 {
        const a = in4(ip, 0);
        try bind(fd, @ptrCast(&a), @sizeOf(sockaddr.in));
        var got: sockaddr.in = undefined;
        var len: socklen_t = @sizeOf(sockaddr.in);
        try getsockname(fd, @ptrCast(&got), &len);
        return std.mem.bigToNative(u16, got.port);
    }

    /// Waits up to a second for `fd` to be readable.
    fn waitReadable(fd: fd_t) !void {
        var fds = [1]pollfd{.{ .fd = fd, .events = POLL.IN, .revents = 0 }};
        if (try poll(&fds, 1000) != 1) return error.NothingArrived;
    }

    fn send1(fd: fd_t, to: sockaddr.in, from: ?[4]u8, data: []const u8) !usize {
        var iov = [1]iovec_const{iovecConstFromSlice(data)};
        return sendDatagram(fd, &iov, .{}, &to, from);
    }

    fn recv1(fd: fd_t, buf: []u8, flags: RecvFlags, from: *sockaddr.in) !DatagramInfo {
        try waitReadable(fd);
        var iov = [1]iovec{iovecFromSlice(buf)};
        return recvDatagram(fd, &iov, flags, from);
    }
};

test "datagram metadata: a wildcard receiver gets the destination address and the sender's source" {
    if (builtin.os.tag == .windows) return error.SkipZigTest;
    const rx = try real_dgram.open();
    defer close(rx);
    const tx = try real_dgram.open();
    defer close(tx);
    const rx_port = try real_dgram.bindAt(rx, .{ 0, 0, 0, 0 });
    const tx_port = try real_dgram.bindAt(tx, .{ 127, 0, 0, 1 });
    try enableDestinationAddress(rx);
    try std.testing.expectEqual(@as(usize, 3), try real_dgram.send1(tx, real_dgram.in4(.{ 127, 0, 0, 1 }, rx_port), null, "abc"));
    var buf: [16]u8 = undefined;
    var from: sockaddr.in = undefined;
    const info = try real_dgram.recv1(rx, &buf, .{}, &from);
    try std.testing.expectEqual(@as(usize, 3), info.len);
    try std.testing.expect(!info.truncated);
    try std.testing.expectEqualStrings("abc", buf[0..3]);
    try std.testing.expectEqual(@as(?[4]u8, .{ 127, 0, 0, 1 }), info.to);
    try std.testing.expectEqual([4]u8{ 127, 0, 0, 1 }, @as([4]u8, @bitCast(from.addr)));
    try std.testing.expectEqual(tx_port, std.mem.bigToNative(u16, from.port));
}

test "datagram metadata: without enableDestinationAddress there is no destination" {
    if (builtin.os.tag == .windows) return error.SkipZigTest;
    const rx = try real_dgram.open();
    defer close(rx);
    const tx = try real_dgram.open();
    defer close(tx);
    const rx_port = try real_dgram.bindAt(rx, .{ 127, 0, 0, 1 });
    _ = try real_dgram.send1(tx, real_dgram.in4(.{ 127, 0, 0, 1 }, rx_port), null, "x");
    var buf: [4]u8 = undefined;
    var from: sockaddr.in = undefined;
    const info = try real_dgram.recv1(rx, &buf, .{}, &from);
    try std.testing.expectEqual(@as(?[4]u8, null), info.to);
}

test "datagram metadata: a send from a chosen source address leaves from it" {
    if (builtin.os.tag == .windows) return error.SkipZigTest;
    const rx = try real_dgram.open();
    defer close(rx);
    const tx = try real_dgram.open();
    defer close(tx);
    const rx_port = try real_dgram.bindAt(rx, .{ 127, 0, 0, 1 });
    const tx_port = try real_dgram.bindAt(tx, .{ 0, 0, 0, 0 });
    _ = try real_dgram.send1(tx, real_dgram.in4(.{ 127, 0, 0, 1 }, rx_port), .{ 127, 0, 0, 1 }, "hi");
    var buf: [4]u8 = undefined;
    var from: sockaddr.in = undefined;
    _ = try real_dgram.recv1(rx, &buf, .{}, &from);
    try std.testing.expectEqual([4]u8{ 127, 0, 0, 1 }, @as([4]u8, @bitCast(from.addr)));
    try std.testing.expectEqual(tx_port, std.mem.bigToNative(u16, from.port));
}

// The kernels answer differently, and the sim models macOS's answer (see
// `sim.dgramSend`): macOS refuses with EADDRNOTAVAIL; Linux 6.8 looks for a
// route from that source and answers ENETUNREACH.
test "datagram metadata: a send from an address the host does not have fails: AddressUnavailable on macOS, NetworkUnreachable on Linux" {
    if (builtin.os.tag == .windows) return error.SkipZigTest;
    const tx = try real_dgram.open();
    defer close(tx);
    _ = try real_dgram.bindAt(tx, .{ 0, 0, 0, 0 });
    // 192.0.2.1 is TEST-NET-1 (RFC 5737): no host has it.
    const want = if (builtin.os.tag == .linux) error.NetworkUnreachable else error.AddressUnavailable;
    try std.testing.expectError(want, real_dgram.send1(tx, real_dgram.in4(.{ 127, 0, 0, 1 }, 9), .{ 192, 0, 2, 1 }, "x"));
}

// Linux has all of 127/8 on its loopback, so a second loopback address shows
// the source address is used. macOS has only 127.0.0.1 there.
test "datagram metadata: on Linux a send from 127.0.0.2 leaves from 127.0.0.2" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    const rx = try real_dgram.open();
    defer close(rx);
    const tx = try real_dgram.open();
    defer close(tx);
    const rx_port = try real_dgram.bindAt(rx, .{ 0, 0, 0, 0 });
    _ = try real_dgram.bindAt(tx, .{ 0, 0, 0, 0 });
    _ = try real_dgram.send1(tx, real_dgram.in4(.{ 127, 0, 0, 1 }, rx_port), .{ 127, 0, 0, 2 }, "hi");
    var buf: [4]u8 = undefined;
    var from: sockaddr.in = undefined;
    _ = try real_dgram.recv1(rx, &buf, .{}, &from);
    try std.testing.expectEqual([4]u8{ 127, 0, 0, 2 }, @as([4]u8, @bitCast(from.addr)));
}

test "datagram metadata: truncation is reported, and the rest of the datagram is gone" {
    if (builtin.os.tag == .windows) return error.SkipZigTest;
    const rx = try real_dgram.open();
    defer close(rx);
    const tx = try real_dgram.open();
    defer close(tx);
    const rx_port = try real_dgram.bindAt(rx, .{ 127, 0, 0, 1 });
    const to = real_dgram.in4(.{ 127, 0, 0, 1 }, rx_port);
    _ = try real_dgram.send1(tx, to, null, "0123456789");
    _ = try real_dgram.send1(tx, to, null, "next");
    var buf: [4]u8 = undefined;
    var from: sockaddr.in = undefined;
    const info = try real_dgram.recv1(rx, &buf, .{ .trunc = true }, &from);
    try std.testing.expect(info.truncated);
    try std.testing.expectEqual(@as(usize, if (builtin.os.tag == .linux) 10 else 4), info.len);
    try std.testing.expectEqualStrings("0123", &buf);
    const next = try real_dgram.recv1(rx, &buf, .{}, &from);
    try std.testing.expect(!next.truncated);
    try std.testing.expectEqualStrings("next", buf[0..next.len]);
}

test "datagram metadata: an empty datagram arrives as a datagram of length 0" {
    if (builtin.os.tag == .windows) return error.SkipZigTest;
    const rx = try real_dgram.open();
    defer close(rx);
    const tx = try real_dgram.open();
    defer close(tx);
    const rx_port = try real_dgram.bindAt(rx, .{ 127, 0, 0, 1 });
    try std.testing.expectEqual(@as(usize, 0), try real_dgram.send1(tx, real_dgram.in4(.{ 127, 0, 0, 1 }, rx_port), null, ""));
    var buf: [4]u8 = undefined;
    var from: sockaddr.in = undefined;
    const info = try real_dgram.recv1(rx, &buf, .{}, &from);
    try std.testing.expectEqual(@as(usize, 0), info.len);
    try std.testing.expect(!info.truncated);
}

test "setNonBlocking and isNonBlocking agree with each other and with socket's flag" {
    if (builtin.os.tag == .windows) return error.SkipZigTest;
    const fd = try socket(.ipv4, .dgram, .ip, .{ .nonblocking = false });
    defer close(fd);
    try std.testing.expect(!try isNonBlocking(fd));
    try setNonBlocking(fd, true);
    try std.testing.expect(try isNonBlocking(fd));
    try setNonBlocking(fd, false);
    try std.testing.expect(!try isNonBlocking(fd));
    const nb = try real_dgram.open();
    defer close(nb);
    try std.testing.expect(try isNonBlocking(nb));
}

test "pollInterruptible: ready when a datagram is queued, timed out when none comes" {
    if (builtin.os.tag == .windows) return error.SkipZigTest;
    const rx = try real_dgram.open();
    defer close(rx);
    const tx = try real_dgram.open();
    defer close(tx);
    const port = try real_dgram.bindAt(rx, .{ 127, 0, 0, 1 });
    try std.testing.expectEqual(PollOutcome.timed_out, try pollInterruptible(rx, POLL.IN, 10));
    _ = try real_dgram.send1(tx, real_dgram.in4(.{ 127, 0, 0, 1 }, port), null, "x");
    try std.testing.expectEqual(PollOutcome{ .ready = POLL.IN }, try pollInterruptible(rx, POLL.IN, 1000));
}

test "pollInterruptible: a signal during the wait is .interrupted, where poll would retry" {
    if (builtin.os.tag == .windows) return error.SkipZigTest;
    const fd = try real_dgram.open();
    defer close(fd);
    _ = try real_dgram.bindAt(fd, .{ 127, 0, 0, 1 });
    const S = struct {
        var done: std.atomic.Value(bool) = .init(false);
        fn handler(_: posix.SIG) callconv(.c) void {}
        /// Signals the polling thread until it reports back, so a signal
        /// that arrives before the poll starts cannot make the test wait out
        /// its timeout.
        fn kicker(target: std.c.pthread_t) void {
            while (!done.load(.acquire)) {
                _ = std.c.pthread_kill(target, posix.SIG.USR2);
                @import("time.zig").sleep(.fromMilliseconds(20));
            }
        }
    };
    // No SA_RESTART: the kernel ends the poll with EINTR.
    const act: posix.Sigaction = .{ .handler = .{ .handler = S.handler }, .mask = posix.sigemptyset(), .flags = 0 };
    var old: posix.Sigaction = undefined;
    posix.sigaction(posix.SIG.USR2, &act, &old);
    defer posix.sigaction(posix.SIG.USR2, &old, null);
    S.done.store(false, .release);
    const t = try std.Thread.spawn(.{}, S.kicker, .{std.c.pthread_self()});
    const outcome = pollInterruptible(fd, POLL.IN, 5000);
    S.done.store(true, .release);
    t.join();
    try std.testing.expectEqual(PollOutcome.interrupted, try outcome);
}
