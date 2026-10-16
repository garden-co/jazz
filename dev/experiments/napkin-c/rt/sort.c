/* sort.c: slice::sort (stable) / sort_unstable / binary_search, one type-erased copy.
 *  - jz_sort: stable natural merge sort (runs detected and extended with insertion sort to MIN_RUN,
 *    merged bottom-up through a scratch buffer of n/2 elements, like std's pre-driftsort merge sort).
 *  - jz_sort_unstable: introsort (median-of-3 / ninther pivot, insertion sort for small slices,
 *    heapsort fallback when recursion depth exceeds 2*log2(n)), like pdqsort's skeleton. */
#include "rt_internal.h"

#define AT(b, i) ((u8 *)(b) + (usize)(i) * elem)
#define SMALL 20

/* insertion sort of base[0..n), assuming base[0..done) already sorted */
static void insertion_sort_from(u8 *base, u32 done, u32 n, usize elem, jz_cmp_fn cmp, void *ctx, u8 *tmp) {
    if (done == 0) done = 1;
    for (u32 i = done; i < n; i++) {
        if (cmp(AT(base, i), AT(base, i - 1), ctx) >= 0) continue;
        memcpy(tmp, AT(base, i), elem);
        u32 j = i;
        do {
            memcpy(AT(base, j), AT(base, j - 1), elem);
            j--;
        } while (j > 0 && cmp(tmp, AT(base, j - 1), ctx) < 0);
        memcpy(AT(base, j), tmp, elem);
    }
}

static void reverse(u8 *base, u32 n, usize elem) {
    for (u32 i = 0, j = n - 1; i < j; i++, j--) jz_mem_swap(AT(base, i), AT(base, j), elem);
}

/* merge base[0..mid) and base[mid..n), both sorted; buf holds >= min(mid, n-mid) elements */
static void merge(u8 *base, u32 mid, u32 n, usize elem, jz_cmp_fn cmp, void *ctx, u8 *buf) {
    u32 left = mid, right = n - mid;
    if (left <= right) {
        /* copy left run out, merge forwards */
        memcpy(buf, base, (usize)left * elem);
        u8 *l = buf, *le = buf + (usize)left * elem;
        u8 *r = AT(base, mid), *re = AT(base, n);
        u8 *out = base;
        while (l < le && r < re) {
            if (cmp(r, l, ctx) < 0) { memcpy(out, r, elem); r += elem; }
            else { memcpy(out, l, elem); l += elem; }
            out += elem;
        }
        memcpy(out, l, (usize)(le - l));
    } else {
        /* copy right run out, merge backwards */
        memcpy(buf, AT(base, mid), (usize)right * elem);
        u8 *l = AT(base, mid), *lb = base;
        u8 *r = buf + (usize)right * elem, *rb = buf;
        u8 *out = AT(base, n);
        while (l > lb && r > rb) {
            out -= elem;
            if (cmp(r - elem, l - elem, ctx) < 0) { l -= elem; memcpy(out, l, elem); }
            else { r -= elem; memcpy(out, r, elem); }
        }
        memcpy(out - (r - rb), rb, (usize)(r - rb));
    }
}

typedef struct run { u32 start, len; } run;

void jz_sort(void *vbase, u32 n, usize elem, jz_cmp_fn cmp, void *ctx) {
    u8 *base = (u8 *)vbase;
    if (n < 2 || elem == 0) return;
    u8 stack_tmp[64];
    u8 *tmp = elem <= sizeof(stack_tmp) ? stack_tmp : (u8 *)jz_alloc(elem);
    if (n <= SMALL) {
        insertion_sort_from(base, 1, n, elem, cmp, ctx, tmp);
        if (tmp != stack_tmp) jz_free(tmp);
        return;
    }
    u8 *buf = (u8 *)jz_alloc((usize)(n / 2 + 1) * elem);
    run runs[64];
    u32 nruns = 0;
    u32 end = n;
    /* scan runs from the back, like std's merge_sort, keeping the stack invariants of TimSort */
    while (end > 0) {
        u32 start = end - 1;
        if (start > 0) {
            start--;
            if (cmp(AT(base, start + 1), AT(base, start), ctx) < 0) {
                while (start > 0 && cmp(AT(base, start), AT(base, start - 1), ctx) < 0) start--;
                reverse(AT(base, start), end - start, elem);
            } else {
                while (start > 0 && cmp(AT(base, start), AT(base, start - 1), ctx) >= 0) start--;
            }
        }
        /* extend short runs to SMALL with insertion sort (inserting elements from the front) */
        while (start > 0 && end - start < SMALL) {
            start--;
            /* insert base[start] into the sorted run base[start+1..end) */
            u32 len = end - start;
            memcpy(tmp, AT(base, start), elem);
            u32 j = 0;
            while (j + 1 < len && cmp(AT(base, start + j + 1), tmp, ctx) < 0) {
                memcpy(AT(base, start + j), AT(base, start + j + 1), elem);
                j++;
            }
            memcpy(AT(base, start + j), tmp, elem);
        }
        runs[nruns].start = start;
        runs[nruns].len = end - start;
        nruns++;
        end = start;
        /* collapse */
        for (;;) {
            u32 k = nruns;
            u32 r;
            if (k >= 2 && (runs[k - 1].start == 0 || runs[k - 2].len <= runs[k - 1].len ||
                           (k >= 3 && runs[k - 3].len <= runs[k - 2].len + runs[k - 1].len) ||
                           (k >= 4 && runs[k - 4].len <= runs[k - 3].len + runs[k - 2].len))) {
                r = (k >= 3 && runs[k - 3].len < runs[k - 1].len) ? k - 3 : k - 2;
            } else {
                break;
            }
            run left = runs[r + 1], right = runs[r];
            merge(AT(base, left.start), left.len, left.len + right.len, elem, cmp, ctx, buf);
            runs[r].start = left.start;
            runs[r].len = left.len + right.len;
            for (u32 i = r + 1; i + 1 < nruns; i++) runs[i] = runs[i + 1];
            nruns--;
        }
    }
    jz_free(buf);
    if (tmp != stack_tmp) jz_free(tmp);
}

/* ---------- unstable: introsort ---------- */
static void sift_down(u8 *base, u32 node, u32 n, usize elem, jz_cmp_fn cmp, void *ctx) {
    for (;;) {
        u32 child = 2 * node + 1;
        if (child >= n) break;
        if (child + 1 < n && cmp(AT(base, child), AT(base, child + 1), ctx) < 0) child++;
        if (cmp(AT(base, node), AT(base, child), ctx) >= 0) break;
        jz_mem_swap(AT(base, node), AT(base, child), elem);
        node = child;
    }
}

static void heapsort(u8 *base, u32 n, usize elem, jz_cmp_fn cmp, void *ctx) {
    for (u32 i = n / 2; i-- > 0;) sift_down(base, i, n, elem, cmp, ctx);
    for (u32 i = n; i-- > 1;) {
        jz_mem_swap(base, AT(base, i), elem);
        sift_down(base, 0, i, elem, cmp, ctx);
    }
}

static u32 median3(u8 *base, u32 a, u32 b, u32 c, usize elem, jz_cmp_fn cmp, void *ctx) {
    bool ab = cmp(AT(base, a), AT(base, b), ctx) < 0;
    bool bc = cmp(AT(base, b), AT(base, c), ctx) < 0;
    if (ab == bc) return b;
    bool ac = cmp(AT(base, a), AT(base, c), ctx) < 0;
    return (ab == ac) ? c : a;
}

static void introsort(u8 *base, u32 n, usize elem, jz_cmp_fn cmp, void *ctx, u32 limit, u8 *tmp) {
    while (n > SMALL) {
        if (limit == 0) {
            heapsort(base, n, elem, cmp, ctx);
            return;
        }
        limit--;
        /* choose pivot: median of 3, or ninther for long slices */
        u32 a = n / 4, b = n / 2, c = n / 4 * 3;
        if (n >= 50) {
            a = median3(base, a - 1, a, a + 1, elem, cmp, ctx);
            b = median3(base, b - 1, b, b + 1, elem, cmp, ctx);
            c = median3(base, c - 1, c, c + 1, elem, cmp, ctx);
        }
        u32 p = median3(base, a, b, c, elem, cmp, ctx);
        jz_mem_swap(base, AT(base, p), elem);
        /* Hoare partition around base[0] */
        u32 l = 1, r = n - 1;
        for (;;) {
            while (l <= r && cmp(AT(base, l), base, ctx) < 0) l++;
            while (l <= r && cmp(AT(base, r), base, ctx) > 0) r--;
            if (l >= r) break;
            jz_mem_swap(AT(base, l), AT(base, r), elem);
            l++;
            r--;
        }
        u32 mid = l - 1;
        jz_mem_swap(base, AT(base, mid), elem);
        /* recurse into the smaller side, loop on the larger */
        u32 left_n = mid, right_n = n - mid - 1;
        if (left_n < right_n) {
            introsort(base, left_n, elem, cmp, ctx, limit, tmp);
            base = AT(base, mid + 1);
            n = right_n;
        } else {
            introsort(AT(base, mid + 1), right_n, elem, cmp, ctx, limit, tmp);
            n = left_n;
        }
    }
    insertion_sort_from(base, 1, n, elem, cmp, ctx, tmp);
}

void jz_sort_unstable(void *base, u32 n, usize elem, jz_cmp_fn cmp, void *ctx) {
    if (n < 2 || elem == 0) return;
    u8 stack_tmp[64];
    u8 *tmp = elem <= sizeof(stack_tmp) ? stack_tmp : (u8 *)jz_alloc(elem);
    u32 limit = 2 * (32 - (u32)__builtin_clz(n));
    introsort((u8 *)base, n, elem, cmp, ctx, limit, tmp);
    if (tmp != stack_tmp) jz_free(tmp);
}

/* slice::binary_search_by: Ok(i) -> true, Err(insertion point) -> false; *idx set either way */
bool jz_bsearch(const void *base, u32 n, usize elem, const void *key, jz_cmp_fn cmp, void *ctx, u32 *idx) {
    u32 size = n, lo = 0;
    if (size == 0) {
        if (idx) *idx = 0;
        return false;
    }
    while (size > 1) {
        u32 half = size / 2;
        u32 mid = lo + half;
        if (cmp((const u8 *)base + (usize)mid * elem, key, ctx) <= 0) lo = mid;
        size -= half;
    }
    int c = cmp((const u8 *)base + (usize)lo * elem, key, ctx);
    if (c == 0) {
        if (idx) *idx = lo;
        return true;
    }
    if (idx) *idx = lo + (c < 0);
    return false;
}

int jz_cmp_u32(const void *a, const void *b, void *ctx) {
    u32 x = *(const u32 *)a, y = *(const u32 *)b;
    return (x > y) - (x < y);
}

int jz_cmp_u64(const void *a, const void *b, void *ctx) {
    u64 x = *(const u64 *)a, y = *(const u64 *)b;
    return (x > y) - (x < y);
}

int jz_cmp_str(const void *a, const void *b, void *ctx) { return jz_str_cmp((const jz_str *)a, (const jz_str *)b); }

int jz_cmp_bytes(const void *a, const void *b, void *ctx) { return jz_bytes_cmp((const jz_bytes *)a, (const jz_bytes *)b); }

int jz_cmp_uuid(const void *a, const void *b, void *ctx) { return jz_uuid_cmp((const jz_uuid *)a, (const jz_uuid *)b); }
