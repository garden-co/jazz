// LD_PRELOAD shim that injects fixed latency into read and write syscalls,
// simulating a slow disk for the storage benchmark.
//   WRITE_DELAY_NS  ns added to write/pwrite/writev/pwritev/fsync/fdatasync
//   READ_DELAY_NS   ns added to read/pread/readv/preadv
// Both default to 0 (no delay). Only descriptors > 2 are delayed, so stdio is
// untouched.
#define _GNU_SOURCE
#include <dlfcn.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <sys/uio.h>
#include <sys/types.h>

static long write_delay_ns = 0;
static long read_delay_ns = 0;
static int inited = 0;

static ssize_t (*real_write)(int, const void *, size_t) = 0;
static ssize_t (*real_pwrite)(int, const void *, size_t, off_t) = 0;
static ssize_t (*real_writev)(int, const struct iovec *, int) = 0;
static ssize_t (*real_pwritev)(int, const struct iovec *, int, off_t) = 0;
static int (*real_fsync)(int) = 0;
static int (*real_fdatasync)(int) = 0;
static ssize_t (*real_read)(int, void *, size_t) = 0;
static ssize_t (*real_pread)(int, void *, size_t, off_t) = 0;
static ssize_t (*real_readv)(int, const struct iovec *, int) = 0;
static ssize_t (*real_preadv)(int, const struct iovec *, int, off_t) = 0;

static void init(void) {
    if (inited) return;
    real_write = dlsym(RTLD_NEXT, "write");
    real_pwrite = dlsym(RTLD_NEXT, "pwrite");
    real_writev = dlsym(RTLD_NEXT, "writev");
    real_pwritev = dlsym(RTLD_NEXT, "pwritev");
    real_fsync = dlsym(RTLD_NEXT, "fsync");
    real_fdatasync = dlsym(RTLD_NEXT, "fdatasync");
    real_read = dlsym(RTLD_NEXT, "read");
    real_pread = dlsym(RTLD_NEXT, "pread");
    real_readv = dlsym(RTLD_NEXT, "readv");
    real_preadv = dlsym(RTLD_NEXT, "preadv");
    const char *w = getenv("WRITE_DELAY_NS");
    const char *r = getenv("READ_DELAY_NS");
    if (w) write_delay_ns = atol(w);
    if (r) read_delay_ns = atol(r);
    inited = 1;
}

static void snooze(int fd, long ns) {
    if (fd > 2 && ns > 0) {
        struct timespec ts = {ns / 1000000000L, ns % 1000000000L};
        nanosleep(&ts, 0);
    }
}

ssize_t write(int fd, const void *b, size_t n) {
    init();
    snooze(fd, write_delay_ns);
    return real_write(fd, b, n);
}
ssize_t pwrite(int fd, const void *b, size_t n, off_t o) {
    init();
    snooze(fd, write_delay_ns);
    return real_pwrite(fd, b, n, o);
}
ssize_t writev(int fd, const struct iovec *v, int c) {
    init();
    snooze(fd, write_delay_ns);
    return real_writev(fd, v, c);
}
ssize_t pwritev(int fd, const struct iovec *v, int c, off_t o) {
    init();
    snooze(fd, write_delay_ns);
    return real_pwritev(fd, v, c, o);
}
int fsync(int fd) {
    init();
    snooze(fd, write_delay_ns);
    return real_fsync(fd);
}
int fdatasync(int fd) {
    init();
    snooze(fd, write_delay_ns);
    return real_fdatasync(fd);
}
ssize_t read(int fd, void *b, size_t n) {
    init();
    snooze(fd, read_delay_ns);
    return real_read(fd, b, n);
}
ssize_t pread(int fd, void *b, size_t n, off_t o) {
    init();
    snooze(fd, read_delay_ns);
    return real_pread(fd, b, n, o);
}
ssize_t readv(int fd, const struct iovec *v, int c) {
    init();
    snooze(fd, read_delay_ns);
    return real_readv(fd, v, c);
}
ssize_t preadv(int fd, const struct iovec *v, int c, off_t o) {
    init();
    snooze(fd, read_delay_ns);
    return real_preadv(fd, v, c, o);
}
