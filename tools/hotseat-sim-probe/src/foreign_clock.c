/* Foreign (C) code reading time two ways, as ENet does through hotseat's
 * patched gettimeofday:
 * - probe_c_monotonic_ms: what hotseat's monotonic_time.c does today, the
 *   kernel's CLOCK_MONOTONIC. The simulator cannot see this call.
 * - probe_c_virtual_ms: the same shape, but routed to a Zig export that reads
 *   zio's clock, which under sim is the virtual clock. */
#include <stdint.h>
#include <time.h>

extern uint64_t probe_zio_awake_ms(void);

uint64_t probe_c_monotonic_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t) ts.tv_sec * 1000u + (uint64_t) ts.tv_nsec / 1000000u;
}

uint64_t probe_c_virtual_ms(void)
{
    return probe_zio_awake_ms();
}

/* Real sleep on a real thread: the decode thread's wait for a frame. */
void probe_c_sleep_ms(unsigned ms)
{
    struct timespec ts = { (time_t) (ms / 1000u), (long) (ms % 1000u) * 1000000L };
    nanosleep(&ts, NULL);
}
