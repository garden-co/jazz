/* rt_internal.h: helpers shared between the runtime's own .c files (not for ported code). */
#ifndef JZ_RT_INTERNAL_H
#define JZ_RT_INTERNAL_H
#include "jz_rt.h"
#include <stdarg.h>

void jz_str_vappendf(jz_str *s, const char *fmt, va_list ap);
void jz_str_reserve(jz_str *s, u32 extra);
void jz_bytes_reserve(jz_bytes *b, u32 extra);
/* grow a capacity the way RawVec does: max(2*cap, need, min_cap) */
u32 jz_grow_cap(u32 cap, u32 need, u32 min_cap);
void jz_mem_swap(void *a, void *b, usize n);
#endif
