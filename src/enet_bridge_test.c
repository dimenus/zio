// SPDX-FileCopyrightText: 2025 Lukáš Lalinský
// SPDX-License-Identifier: MIT
//
// An ENet-shaped socket bridge over zio's socket layer, and the C tests that
// drive it. A test fixture: the production bridge will live in hotseat. Its
// Zig half, `enet_bridge_test.zig`, runs each test, in a sim run in a sim
// build and on real loopback sockets in a build without sim.
//
// The contract it copies is unix.c of cgutman/enet at aca87840b57f (the ENet
// that hotseat vendors), for enet_socket_create, _bind, _get_address,
// _set_option, _get_option, _send, _receive, _wait and _destroy:
// - send: the bytes sent; 0 for would-block, and for EADDRNOTAVAIL, ENETDOWN,
//   ENETUNREACH, EHOSTDOWN and EHOSTUNREACH, which ENet treats as transient;
//   -1 for anything else. errno is left as the layer's error.
// - receive: the bytes received; 0 for would-block; -2 when the datagram did
//   not fit (it is consumed, and nothing else is written); -1 for anything
//   else. peerAddress gets the source. localAddress gets the destination
//   address when the socket reports it, and is left alone otherwise. An empty
//   datagram also returns 0, as in ENet: only the written peerAddress tells
//   it from would-block.
// - wait: 0, with the ready conditions in *condition. A signal that cuts the
//   wait gives 0 and ENET_SOCKET_WAIT_INTERRUPT when the caller asked for
//   it, else -1 with errno EINTR.
// - set_option and get_option: 0, or -1. ENet ignores set_option's -1
//   (host.c:66-68).
// - destroy: closes the socket; skips ENET_SOCKET_NULL.
//
// Where it differs from unix.c, on purpose:
// - IPv4 datagram sockets only. create gives ENET_SOCKET_NULL for anything
//   else, and send gives -1 without a peer address.
// - set_option has cases only for the options ENet's host creation sets
//   (NONBLOCK, RCVBUF, SNDBUF, host.c:66-68). Any other option gives -1, as
//   unix.c does for an option it has no case for. ENet's QOS option
//   (protocol.c:1731) is left to the production bridge.
// - get_option reads ERROR only. TTL gives -1 without a read: the sim does
//   not model reading IP_TTL (sim network design, section 8).
// - No enet_socketset_select. unix.h makes ENetSocketSet an fd_set, and a
//   sim fd is 100000 or more, past FD_SETSIZE (sim network design, section
//   3.2). ENet's core waits with enet_socket_wait, and hotseat calls neither.
// - No control messages: the layer's recvDatagram and sendDatagram carry the
//   destination and source addresses as values.
//
// The bridge reaches the layer only through the zio_test_* functions,
// exported by `enet_bridge_test.zig` in a test build. Each returns a value
// >= 0, or -errno for the layer's error.
//
// Each test returns 0 when it passes, else its failing line, with the reason
// in ebt_failure.

#define _GNU_SOURCE 1

#include <errno.h>
#include <netinet/in.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <time.h>

// ---- ENet's types and constants, as enet.h and unix.h declare them ----

typedef int ENetSocket;
#define ENET_SOCKET_NULL -1
typedef uint32_t enet_uint32;

typedef struct {
    void *data;
    size_t dataLength;
} ENetBuffer;

// ENet passes its buffers to sendmsg and recvmsg as iovecs.
_Static_assert(sizeof(ENetBuffer) == sizeof(struct iovec), "ENetBuffer is an iovec");
_Static_assert(offsetof(ENetBuffer, data) == offsetof(struct iovec, iov_base), "ENetBuffer is an iovec");
_Static_assert(offsetof(ENetBuffer, dataLength) == offsetof(struct iovec, iov_len), "ENetBuffer is an iovec");

typedef struct {
    socklen_t addressLength;
    struct sockaddr_storage address;
} ENetAddress;

typedef enum { ENET_SOCKET_TYPE_STREAM = 1, ENET_SOCKET_TYPE_DATAGRAM = 2 } ENetSocketType;

enum {
    ENET_SOCKET_WAIT_NONE = 0,
    ENET_SOCKET_WAIT_SEND = 1 << 0,
    ENET_SOCKET_WAIT_RECEIVE = 1 << 1,
    ENET_SOCKET_WAIT_INTERRUPT = 1 << 2,
};

typedef enum {
    ENET_SOCKOPT_NONBLOCK = 1,
    ENET_SOCKOPT_BROADCAST = 2,
    ENET_SOCKOPT_RCVBUF = 3,
    ENET_SOCKOPT_SNDBUF = 4,
    ENET_SOCKOPT_REUSEADDR = 5,
    ENET_SOCKOPT_RCVTIMEO = 6,
    ENET_SOCKOPT_SNDTIMEO = 7,
    ENET_SOCKOPT_ERROR = 8,
    ENET_SOCKOPT_NODELAY = 9,
    ENET_SOCKOPT_TTL = 10,
    ENET_SOCKOPT_QOS = 11,
} ENetSocketOption;

enum { ENET_HOST_RECEIVE_BUFFER_SIZE = 256 * 1024, ENET_HOST_SEND_BUFFER_SIZE = 256 * 1024 };

// ---- The layer, through enet_bridge_test.zig ----

extern int zio_test_socket(void);
extern int zio_test_enable_destination_address(int fd);
extern int zio_test_bind(int fd, const struct sockaddr *addr, socklen_t len);
extern int zio_test_getsockname(int fd, struct sockaddr *addr, socklen_t *len);
extern int zio_test_set_nonblocking(int fd, int on);
extern int zio_test_is_nonblocking(int fd);
extern int zio_test_setsockopt(int fd, int level, int name, const void *value, socklen_t len);
extern int zio_test_getsockopt(int fd, int level, int name, void *value, socklen_t len);
extern ssize_t zio_test_send(int fd, const struct sockaddr_in *to, const uint8_t *from, const ENetBuffer *buffers, size_t count);
extern ssize_t zio_test_receive(int fd, struct sockaddr_in *from, ENetBuffer *buffers, size_t count, int *truncated, uint8_t *to, int *has_to);
extern int zio_test_wait(int fd, short events, int timeout_ms, short *revents);
extern void zio_test_close(int fd);
// Not the bridge's: parks the calling zio task until fd is readable.
extern int zio_test_wait_readable(int fd);

// ---- The bridge ----

static ENetSocket enet_socket_create(int af, ENetSocketType type) {
    if (af != AF_INET || type != ENET_SOCKET_TYPE_DATAGRAM) {
        errno = EAFNOSUPPORT;
        return ENET_SOCKET_NULL;
    }
    int sock = zio_test_socket();
    if (sock < 0) {
        errno = -sock;
        return ENET_SOCKET_NULL;
    }
    // unix.c turns on IP_PKTINFO for every socket and ignores a failure.
    (void)zio_test_enable_destination_address(sock);
    return sock;
}

static int enet_socket_bind(ENetSocket socket, const ENetAddress *address) {
    int rc = zio_test_bind(socket, (const struct sockaddr *)&address->address, address->addressLength);
    if (rc < 0) {
        errno = -rc;
        return -1;
    }
    return 0;
}

static int enet_socket_get_address(ENetSocket socket, ENetAddress *address) {
    address->addressLength = sizeof(address->address);
    int rc = zio_test_getsockname(socket, (struct sockaddr *)&address->address, &address->addressLength);
    if (rc < 0) {
        errno = -rc;
        return -1;
    }
    return 0;
}

static int enet_socket_set_option(ENetSocket socket, ENetSocketOption option, int value) {
    int rc;
    switch (option) {
    case ENET_SOCKOPT_NONBLOCK:
        rc = zio_test_set_nonblocking(socket, value != 0);
        break;
    case ENET_SOCKOPT_RCVBUF:
        rc = zio_test_setsockopt(socket, SOL_SOCKET, SO_RCVBUF, &value, sizeof(value));
        break;
    case ENET_SOCKOPT_SNDBUF:
        rc = zio_test_setsockopt(socket, SOL_SOCKET, SO_SNDBUF, &value, sizeof(value));
        break;
    default:
        return -1;
    }
    if (rc < 0) {
        errno = -rc;
        return -1;
    }
    return 0;
}

static int enet_socket_get_option(ENetSocket socket, ENetSocketOption option, int *value) {
    if (option != ENET_SOCKOPT_ERROR) return -1;
    int rc = zio_test_getsockopt(socket, SOL_SOCKET, SO_ERROR, value, sizeof(*value));
    if (rc < 0) {
        errno = -rc;
        return -1;
    }
    return 0;
}

static void enet_socket_destroy(ENetSocket socket) {
    if (socket != -1) zio_test_close(socket);
}

static int enet_socket_send(ENetSocket socket, const ENetAddress *peerAddress, const ENetAddress *localAddress,
                            const ENetBuffer *buffers, size_t bufferCount) {
    if (peerAddress == NULL || peerAddress->address.ss_family != AF_INET) {
        errno = EAFNOSUPPORT;
        return -1;
    }
    // We always send traffic from the same local address as we last received
    // from this peer (unix.c's comment); only an IPv4 local address counts.
    const uint8_t *from = NULL;
    if (localAddress != NULL && localAddress->address.ss_family == AF_INET) {
        from = (const uint8_t *)&((const struct sockaddr_in *)&localAddress->address)->sin_addr;
    }
    ssize_t sent = zio_test_send(socket, (const struct sockaddr_in *)&peerAddress->address, from, buffers, bufferCount);
    if (sent < 0) {
        errno = (int)-sent;
        switch (errno) {
        case EWOULDBLOCK:
            return 0;
        case EADDRNOTAVAIL:
        case ENETDOWN:
        case ENETUNREACH:
#if !defined(EHOSTDOWN) || (EHOSTDOWN != EHOSTUNREACH)
        case EHOSTDOWN:
#endif
        case EHOSTUNREACH:
            return 0;
        default:
            return -1;
        }
    }
    return (int)sent;
}

static int enet_socket_receive(ENetSocket socket, ENetAddress *peerAddress, ENetAddress *localAddress,
                               ENetBuffer *buffers, size_t bufferCount) {
    struct sockaddr_in from;
    uint8_t to[4];
    int truncated = 0, has_to = 0;
    ssize_t received = zio_test_receive(socket, &from, buffers, bufferCount, &truncated, to, &has_to);
    if (received < 0) {
        errno = (int)-received;
        return errno == EWOULDBLOCK ? 0 : -1;
    }
    if (truncated) return -2;
    if (localAddress != NULL && has_to) {
        struct sockaddr_in *localAddr = (struct sockaddr_in *)&localAddress->address;
        localAddr->sin_family = AF_INET;
        memcpy(&localAddr->sin_addr, to, 4);
        localAddress->addressLength = sizeof(*localAddr);
    }
    if (peerAddress != NULL) {
        memcpy(&peerAddress->address, &from, sizeof(from));
        peerAddress->addressLength = sizeof(from);
    }
    return (int)received;
}

static int enet_socket_wait(ENetSocket socket, enet_uint32 *condition, enet_uint32 timeout) {
    short events = 0, revents = 0;
    if (*condition & ENET_SOCKET_WAIT_SEND) events |= POLLOUT;
    if (*condition & ENET_SOCKET_WAIT_RECEIVE) events |= POLLIN;
    int rc = zio_test_wait(socket, events, (int)timeout, &revents);
    if (rc < 0) {
        errno = -rc;
        if (errno == EINTR && *condition & ENET_SOCKET_WAIT_INTERRUPT) {
            *condition = ENET_SOCKET_WAIT_INTERRUPT;
            return 0;
        }
        return -1;
    }
    *condition = ENET_SOCKET_WAIT_NONE;
    if (rc == 0) return 0;
    if (revents & POLLOUT) *condition |= ENET_SOCKET_WAIT_SEND;
    if (revents & POLLIN) *condition |= ENET_SOCKET_WAIT_RECEIVE;
    return 0;
}

// ---- Expected values ----

// The bridge's answers, one value for a sim run and for real loopback
// sockets.
static const struct {
    int would_block;    // receive with nothing queued (errno EWOULDBLOCK)
    int truncated;      // receive of a datagram that did not fit
    int no_case;        // set_option or get_option of an option the bridge has no case for
    int bind_in_use;    // bind to an address another socket holds (errno EADDRINUSE)
    int empty_datagram; // send and receive of an empty datagram
    int sndbuf;         // set_option SNDBUF: a kernel takes it; a sim run accepts it with no effect (section 8)
    int ok;             // bind, set_option, get_option, get_address, wait
} expect = {
    .would_block = 0,
    .truncated = -2,
    .no_case = -1,
    .bind_in_use = -1,
    .empty_datagram = 0,
    .sndbuf = 0,
    .ok = 0,
};

// The one exception: an answer a sim run and the kernel do not share, with a
// column per build. It is a decision of the sim network design.
enum { COL_SIM, COL_LINUX, COL_MACOS };

static int column(int sim) {
    if (sim) return COL_SIM;
#if defined(__linux__)
    return COL_LINUX;
#elif defined(__APPLE__)
    return COL_MACOS;
#else
#error "the ENet bridge test runs on macOS and Linux only"
#endif
}

// A source address the socket cannot use (design sections 3.1 and 11). SENT:
// the send returns its length, and the datagram arrives from that address.
// An errno: the send returns 0, as ENet does for a transient error, and
// errno is that value. The sim takes macOS's strict answer on every host,
// with all of 127/8 local.
#define SENT 0
static const struct {
    uint8_t bound[4];
    uint8_t from[4];
    int want[3];
} source_rows[] = {
    // bound to          send from            sim             Linux        macOS
    {{0, 0, 0, 0}, {127, 0, 0, 1}, {[COL_SIM] = SENT, [COL_LINUX] = SENT, [COL_MACOS] = SENT}},
    {{0, 0, 0, 0}, {127, 0, 0, 2}, {[COL_SIM] = SENT, [COL_LINUX] = SENT, [COL_MACOS] = EADDRNOTAVAIL}},
    {{127, 0, 0, 1}, {127, 0, 0, 2}, {[COL_SIM] = EADDRNOTAVAIL, [COL_LINUX] = SENT, [COL_MACOS] = EADDRNOTAVAIL}},
    {{0, 0, 0, 0}, {192, 0, 2, 1}, {[COL_SIM] = EADDRNOTAVAIL, [COL_LINUX] = ENETUNREACH, [COL_MACOS] = EADDRNOTAVAIL}},
};

// ---- Test helpers ----

char ebt_failure[256];

#define FAIL(...)                                                   \
    do {                                                            \
        snprintf(ebt_failure, sizeof(ebt_failure), __VA_ARGS__);    \
        return __LINE__;                                            \
    } while (0)

#define EXPECT(what, got, want)                                                                         \
    do {                                                                                                \
        long got_ = (long)(got), want_ = (long)(want);                                                  \
        if (got_ != want_) FAIL("%s: got %ld, expected %ld (errno %d)", (what), got_, want_, errno);    \
    } while (0)

#define TRY(call)                \
    do {                         \
        int line_ = (call);      \
        if (line_) return line_; \
    } while (0)

static const uint8_t loopback[4] = {127, 0, 0, 1};
static const uint8_t any[4] = {0, 0, 0, 0};

// The sockets a test opened, closed by ebt_close_all after it, pass or fail.
static ENetSocket opened[16];
static int opened_count;

static void track(ENetSocket s) {
    if (opened_count == 16) abort();
    opened[opened_count++] = s;
}

static void untrack(ENetSocket s) {
    for (int i = 0; i < opened_count; i++) {
        if (opened[i] == s) {
            opened[i] = opened[--opened_count];
            return;
        }
    }
    abort();
}

void ebt_close_all(void) {
    for (int i = 0; i < opened_count; i++) enet_socket_destroy(opened[i]);
    opened_count = 0;
}

static ENetAddress ip4(const uint8_t ip[4], uint16_t port) {
    ENetAddress e;
    memset(&e, 0, sizeof(e));
    struct sockaddr_in *in = (struct sockaddr_in *)&e.address;
#if defined(__APPLE__)
    in->sin_len = sizeof(*in);
#endif
    in->sin_family = AF_INET;
    in->sin_port = htons(port);
    memcpy(&in->sin_addr, ip, 4);
    e.addressLength = sizeof(*in);
    return e;
}

static uint16_t port_of(const ENetAddress *e) { return ntohs(((const struct sockaddr_in *)&e->address)->sin_port); }

static const uint8_t *ip_of(const ENetAddress *e) {
    return (const uint8_t *)&((const struct sockaddr_in *)&e->address)->sin_addr;
}

#define EXPECT_IP(what, e, want)                                                                         \
    do {                                                                                                 \
        const uint8_t *g_ = ip_of(e);                                                                    \
        if (memcmp(g_, (want), 4) != 0)                                                                  \
            FAIL("%s: got %u.%u.%u.%u, expected %u.%u.%u.%u", (what), g_[0], g_[1], g_[2], g_[3],        \
                 (want)[0], (want)[1], (want)[2], (want)[3]);                                            \
    } while (0)

// A non-blocking socket bound to ip and port 0, as ENet opens one; its port.
static int open_bound(const uint8_t ip[4], ENetSocket *s, uint16_t *port) {
    *s = enet_socket_create(AF_INET, ENET_SOCKET_TYPE_DATAGRAM);
    if (*s == ENET_SOCKET_NULL) FAIL("enet_socket_create: ENET_SOCKET_NULL (errno %d)", errno);
    track(*s);
    EXPECT("set_option NONBLOCK 1", enet_socket_set_option(*s, ENET_SOCKOPT_NONBLOCK, 1), expect.ok);
    ENetAddress a = ip4(ip, 0);
    EXPECT("bind to port 0", enet_socket_bind(*s, &a), expect.ok);
    ENetAddress got;
    EXPECT("get_address", enet_socket_get_address(*s, &got), expect.ok);
    if (port != NULL) *port = port_of(&got);
    return 0;
}

static int send_text(ENetSocket s, uint16_t port, const uint8_t *from, const char *text) {
    ENetAddress to = ip4(loopback, port);
    ENetAddress local;
    if (from != NULL) local = ip4(from, 0);
    ENetBuffer b = {.data = (void *)text, .dataLength = strlen(text)};
    return enet_socket_send(s, &to, from != NULL ? &local : NULL, &b, 1);
}

static int arrive(ENetSocket s) {
    int rc = zio_test_wait_readable(s);
    if (rc != 0) FAIL("nothing arrived on socket %d (%d)", s, rc);
    return 0;
}

// ---- Tests ----

// Group 1: C sends two buffers to a zio task's ev receiver, which gets one
// datagram; the task replies with two buffers, and C reads that one datagram
// into two buffers. The zio side checks what it got (enet_bridge_test.zig).
int ebt_exchange(int peer_port, int *my_port) {
    ENetSocket s;
    uint16_t port;
    TRY(open_bound(loopback, &s, &port));
    *my_port = port;
    ENetAddress peer = ip4(loopback, (uint16_t)peer_port);
    ENetBuffer out[2] = {{.data = "hel", .dataLength = 3}, {.data = "lo", .dataLength = 2}};
    EXPECT("send of two buffers to the ev receiver", enet_socket_send(s, &peer, NULL, out, 2), 5);
    TRY(arrive(s));
    char b1[2], b2[8];
    ENetBuffer in[2] = {{.data = b1, .dataLength = sizeof(b1)}, {.data = b2, .dataLength = sizeof(b2)}};
    ENetAddress from, local;
    memset(&from, 0, sizeof(from));
    memset(&local, 0, sizeof(local));
    EXPECT("receive of the ev reply into two buffers", enet_socket_receive(s, &from, &local, in, 2), 6);
    if (memcmp(b1, "wo", 2) != 0 || memcmp(b2, "rld!", 4) != 0) FAIL("the reply's bytes: got %.2s|%.4s, expected wo|rld!", b1, b2);
    EXPECT_IP("the reply's source", &from, loopback);
    EXPECT("the reply's source port", port_of(&from), peer_port);
    EXPECT("the reply's source length", from.addressLength, sizeof(struct sockaddr_in));
    EXPECT_IP("the reply's destination", &local, loopback);
    EXPECT("the reply's destination family", local.address.ss_family, AF_INET);
    return 0;
}

// Group 2: ENet's host creation (host.c:52-70), in its order, then a
// datagram that arrives with its destination address.
int ebt_host_create(void) {
    ENetAddress address = ip4(any, 0);
    ENetSocket s = enet_socket_create(AF_INET, ENET_SOCKET_TYPE_DATAGRAM);
    if (s == ENET_SOCKET_NULL) FAIL("enet_socket_create: ENET_SOCKET_NULL (errno %d)", errno);
    track(s);
    EXPECT("bind to 0.0.0.0 port 0", enet_socket_bind(s, &address), expect.ok);
    EXPECT("set_option NONBLOCK 1", enet_socket_set_option(s, ENET_SOCKOPT_NONBLOCK, 1), expect.ok);
    EXPECT("non-blocking, read back", zio_test_is_nonblocking(s), 1);
    EXPECT("set_option RCVBUF", enet_socket_set_option(s, ENET_SOCKOPT_RCVBUF, ENET_HOST_RECEIVE_BUFFER_SIZE), expect.ok);
    EXPECT("set_option SNDBUF", enet_socket_set_option(s, ENET_SOCKOPT_SNDBUF, ENET_HOST_SEND_BUFFER_SIZE),
           expect.sndbuf);
    ENetAddress got;
    EXPECT("get_address", enet_socket_get_address(s, &got), expect.ok);
    EXPECT("get_address length", got.addressLength, sizeof(struct sockaddr_in));
    EXPECT("get_address family", got.address.ss_family, AF_INET);
    EXPECT_IP("get_address address", &got, any);
    if (port_of(&got) == 0) FAIL("get_address: port 0 after a bind to port 0");
    int err = -1;
    EXPECT("get_option ERROR", enet_socket_get_option(s, ENET_SOCKOPT_ERROR, &err), expect.ok);
    EXPECT("SO_ERROR", err, 0);
    // Non-blocking off and on again, read back each time.
    EXPECT("set_option NONBLOCK 0", enet_socket_set_option(s, ENET_SOCKOPT_NONBLOCK, 0), expect.ok);
    EXPECT("blocking, read back", zio_test_is_nonblocking(s), 0);
    EXPECT("set_option NONBLOCK 1 again", enet_socket_set_option(s, ENET_SOCKOPT_NONBLOCK, 1), expect.ok);
    EXPECT("non-blocking again, read back", zio_test_is_nonblocking(s), 1);
    // create turned the destination address on.
    ENetSocket tx;
    uint16_t tx_port;
    TRY(open_bound(loopback, &tx, &tx_port));
    EXPECT("send to the host", send_text(tx, port_of(&got), NULL, "host"), 4);
    TRY(arrive(s));
    char buf[8];
    ENetBuffer b = {.data = buf, .dataLength = sizeof(buf)};
    ENetAddress from, local;
    memset(&local, 0, sizeof(local));
    EXPECT("host receive", enet_socket_receive(s, &from, &local, &b, 1), 4);
    EXPECT_IP("host receive: destination", &local, loopback);
    EXPECT("host receive: source port", port_of(&from), tx_port);
    return 0;
}

// Group 2 and 7: nothing queued is 0, with errno EWOULDBLOCK and no address
// written.
int ebt_would_block(void) {
    ENetSocket s;
    TRY(open_bound(loopback, &s, NULL));
    char buf[8];
    ENetBuffer b = {.data = buf, .dataLength = sizeof(buf)};
    ENetAddress from;
    memset(&from, 0xAB, sizeof(from));
    errno = 0;
    EXPECT("receive with nothing queued", enet_socket_receive(s, &from, NULL, &b, 1), expect.would_block);
    EXPECT("errno after an empty receive", errno, EWOULDBLOCK);
    EXPECT("peer address length after an empty receive", from.addressLength, 0xABABABAB);
    return 0;
}

// Group 2: an empty datagram is a datagram. Its receive returns 0, as
// would-block does, but writes the source, and the next datagram is next.
int ebt_empty_datagram(void) {
    ENetSocket rx, tx;
    uint16_t rx_port, tx_port;
    TRY(open_bound(loopback, &rx, &rx_port));
    TRY(open_bound(loopback, &tx, &tx_port));
    EXPECT("send of an empty datagram", send_text(tx, rx_port, NULL, ""), expect.empty_datagram);
    EXPECT("send of x", send_text(tx, rx_port, NULL, "x"), 1);
    char buf[8];
    ENetBuffer b = {.data = buf, .dataLength = sizeof(buf)};
    ENetAddress from;
    TRY(arrive(rx));
    memset(&from, 0xAB, sizeof(from));
    EXPECT("receive of the empty datagram", enet_socket_receive(rx, &from, NULL, &b, 1), expect.empty_datagram);
    EXPECT("its source length", from.addressLength, sizeof(struct sockaddr_in));
    EXPECT("its source port", port_of(&from), tx_port);
    TRY(arrive(rx));
    EXPECT("receive of the next datagram", enet_socket_receive(rx, &from, NULL, &b, 1), 1);
    if (buf[0] != 'x') FAIL("the next datagram: got %c, expected x", buf[0]);
    memset(&from, 0xAB, sizeof(from));
    EXPECT("receive with nothing left", enet_socket_receive(rx, &from, NULL, &b, 1), expect.would_block);
    EXPECT("peer address length after it", from.addressLength, 0xABABABAB);
    return 0;
}

// Group 2 and 7: a datagram that does not fit is -2 and is consumed: the
// next receive is the next datagram.
int ebt_truncation(void) {
    ENetSocket rx, tx;
    uint16_t rx_port;
    TRY(open_bound(loopback, &rx, &rx_port));
    TRY(open_bound(loopback, &tx, NULL));
    EXPECT("send of 10 bytes", send_text(tx, rx_port, NULL, "0123456789"), 10);
    EXPECT("send of next", send_text(tx, rx_port, NULL, "next"), 4);
    char small[4], big[16];
    ENetBuffer bs = {.data = small, .dataLength = sizeof(small)};
    ENetBuffer bb = {.data = big, .dataLength = sizeof(big)};
    ENetAddress from;
    TRY(arrive(rx));
    EXPECT("receive of 10 bytes into 4", enet_socket_receive(rx, &from, NULL, &bs, 1), expect.truncated);
    TRY(arrive(rx));
    EXPECT("receive after the truncation", enet_socket_receive(rx, &from, NULL, &bb, 1), 4);
    if (memcmp(big, "next", 4) != 0) FAIL("after the truncation: got %.4s, expected next", big);
    return 0;
}

// Group 2: options the bridge has no case for give -1 and change nothing;
// SNDBUF succeeds and leaves the receive side alone.
int ebt_unsupported_option(void) {
    ENetSocket s, tx;
    uint16_t port;
    TRY(open_bound(loopback, &s, &port));
    int before = -1, after = -1;
    EXPECT("SO_RCVBUF before", zio_test_getsockopt(s, SOL_SOCKET, SO_RCVBUF, &before, sizeof(before)), 0);
    EXPECT("set_option SNDBUF", enet_socket_set_option(s, ENET_SOCKOPT_SNDBUF, 4096), expect.sndbuf);
    EXPECT("set_option BROADCAST", enet_socket_set_option(s, ENET_SOCKOPT_BROADCAST, 1), expect.no_case);
    int ttl = -7;
    EXPECT("get_option TTL", enet_socket_get_option(s, ENET_SOCKOPT_TTL, &ttl), expect.no_case);
    EXPECT("get_option TTL leaves the value", ttl, -7);
    EXPECT("SO_RCVBUF after", zio_test_getsockopt(s, SOL_SOCKET, SO_RCVBUF, &after, sizeof(after)), 0);
    EXPECT("SO_RCVBUF unchanged", after, before);
    EXPECT("still non-blocking", zio_test_is_nonblocking(s), 1);
    TRY(open_bound(loopback, &tx, NULL));
    EXPECT("send after the options", send_text(tx, port, NULL, "ok"), 2);
    TRY(arrive(s));
    char buf[4];
    ENetBuffer b = {.data = buf, .dataLength = sizeof(buf)};
    ENetAddress from;
    EXPECT("receive after the options", enet_socket_receive(s, &from, NULL, &b, 1), 2);
    return 0;
}

// Group 2: enet_socket_wait with a zero timeout, as hotseat calls ENet.
int ebt_wait(void) {
    ENetSocket s, tx;
    uint16_t port;
    TRY(open_bound(loopback, &s, &port));
    TRY(open_bound(loopback, &tx, NULL));
    enet_uint32 c = ENET_SOCKET_WAIT_RECEIVE;
    EXPECT("wait to receive, nothing queued", enet_socket_wait(s, &c, 0), expect.ok);
    EXPECT("its condition", c, ENET_SOCKET_WAIT_NONE);
    c = ENET_SOCKET_WAIT_SEND;
    EXPECT("wait to send", enet_socket_wait(s, &c, 0), expect.ok);
    EXPECT("its condition", c, ENET_SOCKET_WAIT_SEND);
    EXPECT("send", send_text(tx, port, NULL, "w"), 1);
    TRY(arrive(s));
    c = ENET_SOCKET_WAIT_RECEIVE | ENET_SOCKET_WAIT_SEND;
    EXPECT("wait to receive or send, one queued", enet_socket_wait(s, &c, 0), expect.ok);
    EXPECT("its condition", c, ENET_SOCKET_WAIT_RECEIVE | ENET_SOCKET_WAIT_SEND);
    char buf[4];
    ENetBuffer b = {.data = buf, .dataLength = sizeof(buf)};
    ENetAddress from;
    EXPECT("receive", enet_socket_receive(s, &from, NULL, &b, 1), 1);
    c = ENET_SOCKET_WAIT_RECEIVE;
    EXPECT("wait to receive after it", enet_socket_wait(s, &c, 0), expect.ok);
    EXPECT("its condition", c, ENET_SOCKET_WAIT_NONE);
    return 0;
}

// Group 2, real sockets only: a signal cuts a wait. A sim poll takes a zero
// timeout only, so nothing can cut one (design section 10). Another thread
// sends SIGURG to this one every 2 ms until both waits are done; SIGURG is
// ignored by default, so one that arrives after the handler is gone does
// nothing.
static void on_signal(int sig) { (void)sig; }

struct kicker {
    pthread_t target;
    atomic_int done;
};

static void *kick(void *arg) {
    struct kicker *k = arg;
    struct timespec two_ms = {.tv_sec = 0, .tv_nsec = 2000000};
    while (!atomic_load(&k->done)) {
        pthread_kill(k->target, SIGURG);
        nanosleep(&two_ms, NULL);
    }
    return NULL;
}

int ebt_wait_interrupt(void) {
    ENetSocket s;
    TRY(open_bound(loopback, &s, NULL));
    struct sigaction sa, old;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = on_signal;
    sigemptyset(&sa.sa_mask);
    if (sigaction(SIGURG, &sa, &old) != 0) FAIL("sigaction: errno %d", errno);
    struct kicker k = {.target = pthread_self()};
    atomic_init(&k.done, 0);
    pthread_t thread;
    if (pthread_create(&thread, NULL, kick, &k) != 0) {
        sigaction(SIGURG, &old, NULL);
        FAIL("pthread_create failed");
    }
    enet_uint32 asked = ENET_SOCKET_WAIT_RECEIVE | ENET_SOCKET_WAIT_INTERRUPT;
    int rc_asked = enet_socket_wait(s, &asked, 1000);
    enet_uint32 not_asked = ENET_SOCKET_WAIT_RECEIVE;
    errno = 0;
    int rc_not_asked = enet_socket_wait(s, &not_asked, 1000);
    int errno_not_asked = errno;
    atomic_store(&k.done, 1);
    pthread_join(thread, NULL);
    sigaction(SIGURG, &old, NULL);
    EXPECT("wait asking for interrupts", rc_asked, expect.ok);
    EXPECT("its condition", asked, ENET_SOCKET_WAIT_INTERRUPT);
    EXPECT("wait not asking for interrupts", rc_not_asked, -1);
    EXPECT("its errno", errno_not_asked, EINTR);
    return 0;
}

// Group 2: a bind to a port another socket holds is -1 with EADDRINUSE;
// destroy frees the port; destroy of ENET_SOCKET_NULL does nothing.
int ebt_bind_conflict_and_destroy(void) {
    ENetSocket a, tx;
    uint16_t port;
    TRY(open_bound(loopback, &a, &port));
    ENetSocket b = enet_socket_create(AF_INET, ENET_SOCKET_TYPE_DATAGRAM);
    if (b == ENET_SOCKET_NULL) FAIL("enet_socket_create: ENET_SOCKET_NULL (errno %d)", errno);
    track(b);
    EXPECT("set_option NONBLOCK 1", enet_socket_set_option(b, ENET_SOCKOPT_NONBLOCK, 1), expect.ok);
    ENetAddress taken = ip4(loopback, port);
    errno = 0;
    EXPECT("bind to a port in use", enet_socket_bind(b, &taken), expect.bind_in_use);
    EXPECT("its errno", errno, EADDRINUSE);
    untrack(a);
    enet_socket_destroy(a);
    enet_socket_destroy(ENET_SOCKET_NULL);
    EXPECT("bind after the holder was destroyed", enet_socket_bind(b, &taken), expect.ok);
    TRY(open_bound(loopback, &tx, NULL));
    EXPECT("send to the port", send_text(tx, port, NULL, "d"), 1);
    TRY(arrive(b));
    char buf[4];
    ENetBuffer bb = {.data = buf, .dataLength = sizeof(buf)};
    ENetAddress from;
    EXPECT("receive on the new holder", enet_socket_receive(b, &from, NULL, &bb, 1), 1);
    return 0;
}

// Group 7's exception: a source address the socket cannot use, one row per
// case of source_rows, each against the column for this build.
int ebt_source_address(int sim) {
    ENetSocket rx;
    uint16_t rx_port;
    TRY(open_bound(any, &rx, &rx_port));
    for (size_t i = 0; i < sizeof(source_rows) / sizeof(source_rows[0]); i++) {
        ENetSocket tx;
        TRY(open_bound(source_rows[i].bound, &tx, NULL));
        int want = source_rows[i].want[column(sim)];
        errno = 0;
        int sent = send_text(tx, rx_port, source_rows[i].from, "src");
        int sent_errno = errno;
        if (want == SENT) {
            if (sent != 3) FAIL("row %zu: send returned %d (errno %d), expected 3", i, sent, sent_errno);
            TRY(arrive(rx));
            char buf[4];
            ENetBuffer b = {.data = buf, .dataLength = sizeof(buf)};
            ENetAddress from;
            EXPECT("receive", enet_socket_receive(rx, &from, NULL, &b, 1), 3);
            EXPECT_IP("the datagram's source", &from, source_rows[i].from);
        } else {
            if (sent != 0 || sent_errno != want)
                FAIL("row %zu: send returned %d with errno %d, expected 0 with errno %d", i, sent, sent_errno, want);
        }
        untrack(tx);
        enet_socket_destroy(tx);
    }
    return 0;
}

// A socket that sends before it is bound, as an ENet client host does (it
// never binds). Measured on real sockets, 3 runs each (design section 4):
// on macOS and Linux the first send binds the wildcard address and an
// ephemeral port, a reply to any local address on that port arrives, and a
// send to any 127/8 address leaves from 127.0.0.1. macOS has only 127.0.0.1
// on lo0, so the steps that use 127.0.0.2 run in a sim run and on Linux.
static const uint8_t loopback2[4] = {127, 0, 0, 2};

static int send_to(ENetSocket s, const uint8_t ip[4], uint16_t port, const char *text) {
    ENetAddress to = ip4(ip, port);
    ENetBuffer b = {.data = (void *)text, .dataLength = strlen(text)};
    return enet_socket_send(s, &to, NULL, &b, 1);
}

static int receive_from(ENetSocket s, const char *what, int len, const uint8_t ip[4], uint16_t port) {
    TRY(arrive(s));
    char buf[8];
    ENetBuffer b = {.data = buf, .dataLength = sizeof(buf)};
    ENetAddress from;
    EXPECT(what, enet_socket_receive(s, &from, NULL, &b, 1), len);
    EXPECT_IP(what, &from, ip);
    EXPECT(what, port_of(&from), port);
    return 0;
}

static int open_unbound(ENetSocket *s) {
    *s = enet_socket_create(AF_INET, ENET_SOCKET_TYPE_DATAGRAM);
    if (*s == ENET_SOCKET_NULL) FAIL("enet_socket_create: ENET_SOCKET_NULL (errno %d)", errno);
    track(*s);
    EXPECT("set_option NONBLOCK 1", enet_socket_set_option(*s, ENET_SOCKOPT_NONBLOCK, 1), expect.ok);
    return 0;
}

int ebt_send_before_bind(int sim) {
    ENetSocket rx, c;
    uint16_t rx_port;
    TRY(open_bound(any, &rx, &rx_port));
    TRY(open_unbound(&c));
    ENetAddress got;
    EXPECT("get_address before a send", enet_socket_get_address(c, &got), expect.ok);
    EXPECT_IP("the address before a send", &got, any);
    EXPECT("the port before a send", port_of(&got), 0);
    EXPECT("the first send", send_to(c, loopback, rx_port, "hi"), 2);
    EXPECT("get_address after it", enet_socket_get_address(c, &got), expect.ok);
    EXPECT_IP("the address after the first send", &got, any);
    uint16_t c_port = port_of(&got);
    if (c_port == 0) FAIL("the port after the first send: 0");
    TRY(receive_from(rx, "the first datagram at rx", 2, loopback, c_port));
    EXPECT("a reply to 127.0.0.1", send_to(rx, loopback, c_port, "re1"), 3);
    TRY(receive_from(c, "the reply to 127.0.0.1", 3, loopback, rx_port));
    if (column(sim) == COL_MACOS) return 0;
    EXPECT("a reply to 127.0.0.2", send_to(rx, loopback2, c_port, "re2"), 3);
    TRY(receive_from(c, "the reply to 127.0.0.2", 3, loopback, rx_port));
    ENetSocket c2;
    TRY(open_unbound(&c2));
    EXPECT("a second socket's first send, to 127.0.0.2", send_to(c2, loopback2, rx_port, "to2"), 3);
    EXPECT("get_address of the second socket", enet_socket_get_address(c2, &got), expect.ok);
    EXPECT_IP("its address", &got, any);
    TRY(receive_from(rx, "its datagram at rx", 3, loopback, port_of(&got)));
    return 0;
}
