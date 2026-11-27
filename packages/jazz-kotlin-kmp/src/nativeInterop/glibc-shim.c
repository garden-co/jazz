/*
 * Kotlin/Native links Linux binaries against its own glibc 2.19 sysroot; a
 * libjazz_ffi.a built on a newer glibc calls fcntl64 (glibc 2.28+, from the
 * bundled SQLite). On LP64 it is fcntl. Production would rather build the
 * Linux archive for an old glibc (e.g. cargo-zigbuild, x86_64-unknown-linux-gnu.2.17).
 */
#include <fcntl.h>
#include <stdarg.h>

int fcntl64(int fd, int cmd, ...) {
    va_list ap;
    va_start(ap, cmd);
    void *arg = va_arg(ap, void *);
    va_end(ap);
    return fcntl(fd, cmd, arg);
}
