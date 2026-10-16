/* jz_rt.h: shared runtime for the napkin C port of the Jazz client.
 *
 * This plays the role Rust's core/alloc/std (+ serde, postcard, serde_json,
 * hashbrown, internment, futures) play in the Rust build. Everything here is
 * written ONCE and type-erased, the way a C codebase would do it: containers
 * take element sizes and comparator/hash function pointers instead of being
 * monomorphised per type.
 *
 * Conventions for ported code (see ../PORTING.md):
 *  - every fallible function returns jz_err (0 = ok) and writes results via out-params
 *  - every owning struct has a <type>_drop(T*) that frees what it owns
 *  - Rust traits become vtable structs of function pointers
 *  - Rust generic functions become one function taking a vtable / elem size
 *  - async fns become plain synchronous functions (see PORTING.md)
 */
#ifndef JZ_RT_H
#define JZ_RT_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <string.h>
#include <stdlib.h>

typedef uint8_t u8;
typedef uint16_t u16;
typedef uint32_t u32;
typedef uint64_t u64;
typedef int8_t i8;
typedef int16_t i16;
typedef int32_t i32;
typedef int64_t i64;
typedef double f64;
typedef float f32;
typedef size_t usize;

/* ---------- panics / errors ---------- */
_Noreturn void jz_panic(const char *msg);
_Noreturn void jz_unwrap_failed(void);
_Noreturn void jz_oom(void);
#define JZ_ASSERT(c, msg) do { if (!(c)) jz_panic(msg); } while (0)
#define JZ_UNREACHABLE() jz_panic("unreachable")

typedef struct jz_str { char *ptr; u32 len; u32 cap; } jz_str;     /* owned UTF-8, not NUL-terminated */
typedef struct jz_bytes { u8 *ptr; u32 len; u32 cap; } jz_bytes;  /* owned bytes */
typedef struct jz_slice { const u8 *ptr; u32 len; } jz_slice;     /* borrowed bytes / str */

/* Error value: code (crate-defined enum discriminant) + optional formatted message.
 * 0 is success. Functions return jz_err by value; JZ_TRY propagates. */
typedef struct jz_err { u32 code; jz_str *msg; } jz_err;
#define JZ_OK ((jz_err){0, NULL})
#define JZ_IS_ERR(e) ((e).code != 0)
#define JZ_TRY(expr) do { jz_err _e = (expr); if (_e.code) return _e; } while (0)
jz_err jz_err_new(u32 code, const char *msg);
jz_err jz_err_fmt(u32 code, const char *fmt, ...);
void jz_err_drop(jz_err *e);
jz_err jz_err_context(jz_err e, const char *ctx); /* map_err(|e| format!("{ctx}: {e}")) */

/* ---------- allocation ---------- */
void *jz_alloc(usize n);
void *jz_zalloc(usize n);
void *jz_realloc(void *p, usize n);
void jz_free(void *p);
#define JZ_NEW(T) ((T *)jz_zalloc(sizeof(T)))
void *jz_memdup(const void *p, usize n);

/* ---------- strings / bytes ---------- */
jz_str jz_str_from(const char *s);
jz_str jz_str_from_slice(jz_slice s);
jz_str jz_str_clone(const jz_str *s);
void jz_str_drop(jz_str *s);
void jz_str_push(jz_str *s, const char *p, u32 n);
void jz_str_push_cstr(jz_str *s, const char *p);
void jz_str_push_char(jz_str *s, char c);
bool jz_str_eq(const jz_str *a, const jz_str *b);
bool jz_str_eq_cstr(const jz_str *a, const char *b);
int jz_str_cmp(const jz_str *a, const jz_str *b);
jz_slice jz_str_slice(const jz_str *s);
u64 jz_str_hash(const jz_str *s);
jz_str jz_format(const char *fmt, ...);            /* format!() */
void jz_str_appendf(jz_str *s, const char *fmt, ...); /* write!() */
bool jz_utf8_valid(const u8 *p, u32 n);

jz_bytes jz_bytes_from(const u8 *p, u32 n);
jz_bytes jz_bytes_clone(const jz_bytes *b);
void jz_bytes_drop(jz_bytes *b);
void jz_bytes_push(jz_bytes *b, const u8 *p, u32 n);
void jz_bytes_push_u8(jz_bytes *b, u8 v);
int jz_bytes_cmp(const jz_bytes *a, const jz_bytes *b);
int jz_slice_cmp(jz_slice a, jz_slice b);
bool jz_bytes_eq(const jz_bytes *a, const jz_bytes *b);
jz_slice jz_bytes_slice(const jz_bytes *b);
static inline jz_slice jz_slice_of(const void *p, u32 n) { jz_slice s = {(const u8 *)p, n}; return s; }
#define JZ_LIT(s) jz_slice_of((s), (u32)(sizeof(s) - 1))

/* ---------- Vec<T> (type-erased) ----------
 * Declare a typed vec with JZ_VEC(T) name; operate via jz_vec_* with sizeof(T). */
#define JZ_VEC(T) struct { T *ptr; u32 len; u32 cap; }
typedef JZ_VEC(void) jz_vec;
void jz_vec_reserve(void *vec, u32 extra, usize elem);
void *jz_vec_push_slot(void *vec, usize elem);           /* returns pointer to new uninit slot */
void jz_vec_push(void *vec, const void *item, usize elem);
void jz_vec_insert(void *vec, u32 idx, const void *item, usize elem);
void jz_vec_remove(void *vec, u32 idx, void *out, usize elem);
void jz_vec_swap_remove(void *vec, u32 idx, void *out, usize elem);
void jz_vec_truncate(void *vec, u32 len, usize elem, void (*drop)(void *));
void jz_vec_drop(void *vec, usize elem, void (*drop)(void *));
void jz_vec_clone(void *dst, const void *src, usize elem, void (*clone)(void *dst, const void *src));
void jz_vec_extend(void *vec, const void *items, u32 n, usize elem);
void jz_vec_dedup(void *vec, usize elem, int (*cmp)(const void *, const void *), void (*drop)(void *));
void jz_vec_retain(void *vec, usize elem, bool (*keep)(void *item, void *ctx), void *ctx, void (*drop)(void *));
#define JZ_VEC_AT(v, i) (&(v).ptr[(i)])
#define JZ_VEC_PUSH(v, x) do { __typeof__(x) _t = (x); jz_vec_push(&(v), &_t, sizeof(*(v).ptr)); } while (0)
#define JZ_VEC_FOR(T, it, v) for (T *it = (v).ptr; it < (v).ptr + (v).len; ++it)

/* ---------- sorting / searching (one copy for all element types) ---------- */
typedef int (*jz_cmp_fn)(const void *a, const void *b, void *ctx);
void jz_sort(void *base, u32 n, usize elem, jz_cmp_fn cmp, void *ctx);          /* stable merge sort */
void jz_sort_unstable(void *base, u32 n, usize elem, jz_cmp_fn cmp, void *ctx); /* introsort */
bool jz_bsearch(const void *base, u32 n, usize elem, const void *key, jz_cmp_fn cmp, void *ctx, u32 *idx);
int jz_cmp_u32(const void *a, const void *b, void *ctx);
int jz_cmp_u64(const void *a, const void *b, void *ctx);
int jz_cmp_str(const void *a, const void *b, void *ctx);
int jz_cmp_bytes(const void *a, const void *b, void *ctx);
int jz_cmp_uuid(const void *a, const void *b, void *ctx);

/* ---------- BTreeMap / BTreeSet (type-erased B-tree, B=6 like std) ----------
 * One implementation for every K,V. Keys/values are stored inline (memcpy'd).
 * A set is a map with val_size 0. */
typedef struct jz_btree_node jz_btree_node;
typedef struct jz_btree_desc {
    u16 key_size, val_size;
    jz_cmp_fn cmp;            /* key comparator */
    void (*key_drop)(void *); /* may be NULL */
    void (*val_drop)(void *); /* may be NULL */
} jz_btree_desc;
typedef struct jz_btree { jz_btree_node *root; u32 height; u32 len; const jz_btree_desc *d; } jz_btree;
typedef struct jz_btree_iter { jz_btree_node *stack[16]; u16 idx[16]; i32 depth; const jz_btree *t; const void *hi; bool hi_incl; } jz_btree_iter;
void jz_btree_init(jz_btree *t, const jz_btree_desc *d);
void jz_btree_drop(jz_btree *t);
void jz_btree_clear(jz_btree *t);
void *jz_btree_get(const jz_btree *t, const void *key);              /* -> value ptr or NULL */
bool jz_btree_contains(const jz_btree *t, const void *key);
/* insert: moves key & val in. Returns true if replaced (old value dropped/moved into old_val if non-NULL). */
bool jz_btree_insert(jz_btree *t, const void *key, const void *val, void *old_val);
void *jz_btree_entry(jz_btree *t, const void *key, bool *inserted); /* entry().or_insert_with(zeroed) */
bool jz_btree_remove(jz_btree *t, const void *key, void *out_key, void *out_val);
bool jz_btree_first(const jz_btree *t, void **key, void **val);
bool jz_btree_last(const jz_btree *t, void **key, void **val);
bool jz_btree_pop_first(jz_btree *t, void *out_key, void *out_val);
void jz_btree_iter_begin(jz_btree_iter *it, const jz_btree *t);
void jz_btree_range(jz_btree_iter *it, const jz_btree *t, const void *lo, bool lo_incl, const void *hi, bool hi_incl);
bool jz_btree_next(jz_btree_iter *it, void **key, void **val);
void jz_btree_clone(jz_btree *dst, const jz_btree *src, void (*kclone)(void *, const void *), void (*vclone)(void *, const void *));
void jz_btree_retain(jz_btree *t, bool (*keep)(const void *k, void *v, void *ctx), void *ctx);
void jz_btree_append(jz_btree *dst, jz_btree *src);
bool jz_btree_eq(const jz_btree *a, const jz_btree *b, bool (*veq)(const void *, const void *));
/* bulk build from a vec of (k,v) pairs: sorts once with jz_sort then inserts (std's collect()) */
void jz_btree_from_pairs(jz_btree *t, const jz_btree_desc *d, void *pairs, u32 n);
/* (added) from_pairs with explicit pair layout: stride = sizeof(pair), val_off = offsetof(pair, v).
 * jz_btree_from_pairs assumes a packed pair (stride = key_size + val_size, val_off = key_size). */
void jz_btree_from_pairs_ex(jz_btree *t, const jz_btree_desc *d, void *pairs, u32 n, usize stride, usize val_off);
#define JZ_BTREE_FOR(t, kp, vp) for (jz_btree_iter _it_##kp = ((void)jz_btree_iter_begin(&_it_##kp, (t)), _it_##kp); jz_btree_next(&_it_##kp, (void **)&(kp), (void **)&(vp));)

/* ---------- HashMap / HashSet (type-erased open addressing, hashbrown-like) ---------- */
typedef struct jz_hmap_desc {
    u16 key_size, val_size;
    u64 (*hash)(const void *key);
    bool (*eq)(const void *a, const void *b);
    void (*key_drop)(void *);
    void (*val_drop)(void *);
} jz_hmap_desc;
typedef struct jz_hmap { u8 *ctrl; u8 *slots; u32 mask; u32 len; u32 growth_left; const jz_hmap_desc *d; } jz_hmap;
void jz_hmap_init(jz_hmap *m, const jz_hmap_desc *d);
void jz_hmap_drop(jz_hmap *m);
void jz_hmap_clear(jz_hmap *m);
void *jz_hmap_get(const jz_hmap *m, const void *key);
bool jz_hmap_insert(jz_hmap *m, const void *key, const void *val, void *old_val);
void *jz_hmap_entry(jz_hmap *m, const void *key, bool *inserted);
bool jz_hmap_remove(jz_hmap *m, const void *key, void *out_key, void *out_val);
typedef struct jz_hmap_iter { const jz_hmap *m; u32 i; } jz_hmap_iter;
void jz_hmap_iter_begin(jz_hmap_iter *it, const jz_hmap *m);
bool jz_hmap_next(jz_hmap_iter *it, void **key, void **val);
void jz_hmap_retain(jz_hmap *m, bool (*keep)(const void *k, void *v, void *ctx), void *ctx);
u64 jz_hash_bytes(const void *p, usize n); /* foldhash/fxhash-like */
u64 jz_hash_u64(u64 v);
u64 jz_hash_combine(u64 h, u64 v);
#define JZ_HMAP_FOR(m, kp, vp) for (jz_hmap_iter _hit_##kp = ((void)jz_hmap_iter_begin(&_hit_##kp, (m)), _hit_##kp); jz_hmap_next(&_hit_##kp, (void **)&(kp), (void **)&(vp));)

/* ---------- VecDeque ---------- */
typedef struct jz_deque { u8 *buf; u32 head; u32 len; u32 cap; u32 elem; } jz_deque;
void jz_deque_init(jz_deque *q, u32 elem);
void jz_deque_push_back(jz_deque *q, const void *item);
void jz_deque_push_front(jz_deque *q, const void *item);
bool jz_deque_pop_front(jz_deque *q, void *out);
bool jz_deque_pop_back(jz_deque *q, void *out);
void *jz_deque_at(const jz_deque *q, u32 i);
void jz_deque_drop(jz_deque *q, void (*drop)(void *));

/* ---------- Rc / Arc (refcounted box with drop fn) ---------- */
typedef struct jz_rc_hdr { u32 strong; u32 weak; void (*drop)(void *); } jz_rc_hdr;
void *jz_rc_new(usize size, void (*drop)(void *)); /* returns payload ptr */
void *jz_rc_clone(void *p);
void jz_rc_release(void *p);
u32 jz_rc_strong(const void *p);
void *jz_rc_make_mut(void **pp, usize size, void (*clone)(void *dst, const void *src)); /* Rc::make_mut */

/* ---------- Interning (internment::Intern) ---------- */
typedef struct jz_interner jz_interner;
/* (added) like jz_intern, reports whether val was moved into the pool (shallow copy).
 * When *inserted is false the caller still owns val and should drop it (Intern::new drops it). */
const void *jz_intern_ex(jz_interner **pool, const void *val, usize size, u64 (*hash)(const void *), bool (*eq)(const void *, const void *), bool *inserted);
const void *jz_intern(jz_interner **pool, const void *val, usize size, u64 (*hash)(const void *), bool (*eq)(const void *, const void *));

/* ---------- UUID / ids ---------- */
typedef struct jz_uuid { u8 b[16]; } jz_uuid;
int jz_uuid_cmp(const jz_uuid *a, const jz_uuid *b);
bool jz_uuid_eq(const jz_uuid *a, const jz_uuid *b);
u64 jz_uuid_hash(const void *u);
bool jz_uuid_eq_v(const void *a, const void *b);
jz_uuid jz_uuid_v7(u64 unix_ms, const u8 rand[10]);
jz_uuid jz_uuid_v5(const jz_uuid *ns, const u8 *name, u32 len);
jz_uuid jz_uuid_v4(void);
void jz_uuid_to_string(const jz_uuid *u, char out[37]);
jz_err jz_uuid_parse(jz_slice s, jz_uuid *out);
enum { JZ_UUID_ERR = 0xffd0, JZ_BASE64_ERR, JZ_HEX_ERR, JZ_ZSTD_ERR }; /* (added) error codes of rt helpers */
void jz_random_bytes(u8 *out, u32 n); /* imported from host (getrandom) */
u64 jz_now_ms(void);                  /* imported from host (web-time) */

/* ---------- postcard (serde wire format) ---------- */
typedef struct jz_pc_w { jz_bytes *out; } jz_pc_w;
typedef struct jz_pc_r { const u8 *p; const u8 *end; u32 depth; } jz_pc_r;
void jz_pc_u8(jz_pc_w *w, u8 v);
void jz_pc_bool(jz_pc_w *w, bool v);
void jz_pc_varint(jz_pc_w *w, u64 v);
void jz_pc_zigzag(jz_pc_w *w, i64 v);
void jz_pc_f64(jz_pc_w *w, f64 v);
void jz_pc_f32(jz_pc_w *w, f32 v);
void jz_pc_bytes(jz_pc_w *w, const u8 *p, u32 n);   /* length-prefixed */
void jz_pc_str(jz_pc_w *w, const jz_str *s);
void jz_pc_raw(jz_pc_w *w, const u8 *p, u32 n);     /* fixed-size, e.g. [u8;16] */
void jz_pc_uuid(jz_pc_w *w, const jz_uuid *u);
void jz_pc_option(jz_pc_w *w, bool some);
void jz_pc_seq(jz_pc_w *w, u32 n);
void jz_pc_variant(jz_pc_w *w, u32 idx);
jz_err jz_pc_r_u8(jz_pc_r *r, u8 *v);
jz_err jz_pc_r_bool(jz_pc_r *r, bool *v);
jz_err jz_pc_r_varint(jz_pc_r *r, u64 *v);
jz_err jz_pc_r_u32(jz_pc_r *r, u32 *v);
jz_err jz_pc_r_u16(jz_pc_r *r, u16 *v);
jz_err jz_pc_r_zigzag(jz_pc_r *r, i64 *v);
jz_err jz_pc_r_f64(jz_pc_r *r, f64 *v);
jz_err jz_pc_r_bytes(jz_pc_r *r, jz_bytes *out);
jz_err jz_pc_r_bytes_ref(jz_pc_r *r, jz_slice *out);
jz_err jz_pc_r_str(jz_pc_r *r, jz_str *out);
jz_err jz_pc_r_raw(jz_pc_r *r, u8 *out, u32 n);
jz_err jz_pc_r_uuid(jz_pc_r *r, jz_uuid *out);
jz_err jz_pc_r_option(jz_pc_r *r, bool *some);
jz_err jz_pc_r_seq(jz_pc_r *r, u32 *n);
jz_err jz_pc_r_variant(jz_pc_r *r, u32 *idx, u32 nvariants);
jz_err jz_pc_r_finish(jz_pc_r *r); /* error on trailing bytes */
jz_err jz_pc_r_enter(jz_pc_r *r);  /* recursion depth guard */
void jz_pc_r_leave(jz_pc_r *r);
enum { JZ_PC_EOF = 0xfff0, JZ_PC_BAD_VARINT, JZ_PC_BAD_VARIANT, JZ_PC_BAD_UTF8, JZ_PC_TRAILING, JZ_PC_DEPTH, JZ_PC_BAD_BOOL };

/* ---------- JSON (serde_json::Value + parser/serializer) ---------- */
typedef enum { JZ_JSON_NULL, JZ_JSON_BOOL, JZ_JSON_U64, JZ_JSON_I64, JZ_JSON_F64, JZ_JSON_STR, JZ_JSON_ARR, JZ_JSON_OBJ } jz_json_kind;
typedef struct jz_json jz_json;
typedef struct jz_json_member { jz_str key; jz_json *val; } jz_json_member;
struct jz_json {
    jz_json_kind kind;
    union {
        bool b; u64 u; i64 i; f64 f; jz_str s;
        JZ_VEC(jz_json *) arr;
        jz_btree obj; /* jz_str -> jz_json* (sorted, serde_json default) */
    } v;
};
jz_err jz_json_parse(jz_slice text, jz_json **out);
void jz_json_write(const jz_json *v, jz_str *out);
void jz_json_write_pretty(const jz_json *v, jz_str *out);
void jz_json_free(jz_json *v);
jz_json *jz_json_clone(const jz_json *v);
bool jz_json_eq(const jz_json *a, const jz_json *b);
jz_json *jz_json_null(void);
jz_json *jz_json_bool(bool b);
jz_json *jz_json_u64(u64 v);
jz_json *jz_json_i64(i64 v);
jz_json *jz_json_f64(f64 v);
jz_json *jz_json_str(const char *s);
jz_json *jz_json_str_owned(jz_str s);
jz_json *jz_json_arr(void);
jz_json *jz_json_obj(void);
void jz_json_arr_push(jz_json *arr, jz_json *v);
void jz_json_obj_set(jz_json *obj, const char *key, jz_json *v);
jz_json *jz_json_obj_get(const jz_json *obj, const char *key);
const jz_str *jz_json_as_str(const jz_json *v);
bool jz_json_as_u64(const jz_json *v, u64 *out);
bool jz_json_as_i64(const jz_json *v, i64 *out);
bool jz_json_as_f64(const jz_json *v, f64 *out);
/* streaming JSON writer used by hand-written Serialize impls */
typedef struct jz_json_w { jz_str *out; u32 depth; bool need_comma[64]; } jz_json_w;
void jz_jw_begin_obj(jz_json_w *w);
void jz_jw_end_obj(jz_json_w *w);
void jz_jw_begin_arr(jz_json_w *w);
void jz_jw_end_arr(jz_json_w *w);
void jz_jw_key(jz_json_w *w, const char *k);
void jz_jw_str(jz_json_w *w, const char *p, u32 n);
void jz_jw_u64(jz_json_w *w, u64 v);
void jz_jw_i64(jz_json_w *w, i64 v);
void jz_jw_f64(jz_json_w *w, f64 v);
void jz_jw_bool(jz_json_w *w, bool v);
void jz_jw_null(jz_json_w *w);
enum { JZ_JSON_ERR_SYNTAX = 0xffe0, JZ_JSON_ERR_TYPE, JZ_JSON_ERR_MISSING_FIELD, JZ_JSON_ERR_UNKNOWN_VARIANT };
/* helpers for hand-written Deserialize impls */
jz_err jz_json_expect_obj(const jz_json *v, const char *what);
jz_err jz_json_field_str(const jz_json *obj, const char *key, jz_str *out);
jz_err jz_json_field_u64(const jz_json *obj, const char *key, u64 *out);
jz_err jz_json_field_bool(const jz_json *obj, const char *key, bool *out);
jz_err jz_json_variant(const jz_json *v, const char *const *names, u32 n, u32 *idx, const jz_json **payload); /* externally tagged enum */

/* ---------- base64 / hex ---------- */
void jz_base64_encode(const u8 *p, u32 n, bool url, bool pad, jz_str *out);
jz_err jz_base64_decode(jz_slice s, bool url, jz_bytes *out);
void jz_hex_encode(const u8 *p, u32 n, jz_str *out);
jz_err jz_hex_decode(jz_slice s, jz_bytes *out);

/* ---------- crypto (vendored C libs, see ../third_party) ---------- */
void jz_sha256(const u8 *p, usize n, u8 out[32]);
void jz_sha512(const u8 *p, usize n, u8 out[64]);
void jz_sha1(const u8 *p, usize n, u8 out[20]);
void jz_blake3(const u8 *p, usize n, u8 out[32]);
typedef struct jz_blake3_hasher jz_blake3_hasher;
jz_blake3_hasher *jz_blake3_new(void);
void jz_blake3_update(jz_blake3_hasher *h, const u8 *p, usize n);
void jz_blake3_finalize(jz_blake3_hasher *h, u8 out[32]);
void jz_blake3_free(jz_blake3_hasher *h);
void jz_ed25519_public_key(const u8 seed[32], u8 pk[32]);
void jz_ed25519_sign(const u8 seed[32], const u8 pk[32], const u8 *msg, usize n, u8 sig[64]);
bool jz_ed25519_verify(const u8 pk[32], const u8 *msg, usize n, const u8 sig[64]);
/* zstd frame decompression (ruzstd equivalent) */
jz_err jz_zstd_decompress(const u8 *src, u32 n, jz_bytes *out, u32 max_out);

/* ---------- tasks / channels (futures equivalent) ----------
 * The port is synchronous; spawned tasks are queued closures run by jz_task_run_ready(). */
typedef void (*jz_task_fn)(void *ctx);
void jz_task_spawn(jz_task_fn f, void *ctx, void (*ctx_drop)(void *));
void jz_task_run_ready(void);
typedef struct jz_chan jz_chan; /* unbounded mpsc of fixed-size items */
jz_chan *jz_chan_new(u32 elem);
void jz_chan_send(jz_chan *c, const void *item);
bool jz_chan_try_recv(jz_chan *c, void *out);
void jz_chan_close(jz_chan *c);
bool jz_chan_is_closed(const jz_chan *c);
void jz_chan_release(jz_chan *c);
typedef struct jz_waker { void (*wake)(void *ctx); void *ctx; } jz_waker;
void jz_waker_wake(const jz_waker *w);

/* ---------- tracing (spans + counters, ~0 cost when unobserved) ---------- */
extern u32 jz_trace_level;
void jz_trace_event(u32 level, const char *target, const char *fmt, ...);
u32 jz_trace_span_enter(const char *name);
void jz_trace_span_exit(u32 id);
void jz_counter_add(const char *name, u64 v);
#define JZ_TRACE(level, target, ...) do { if (jz_trace_level >= (level)) jz_trace_event((level), (target), __VA_ARGS__); } while (0)

/* ---------- host imports (what wasm-bindgen/js-sys provide in Rust) ---------- */
#define JZ_IMPORT(name) __attribute__((import_module("env"), import_name(name)))
#define JZ_EXPORT(name) __attribute__((export_name(name)))
typedef u32 jz_js; /* handle into a host-side JS object table */
JZ_IMPORT("js_drop") void jz_js_drop(jz_js h);
JZ_IMPORT("js_call") jz_js jz_js_call(jz_js fn, jz_js this_, const u8 *args, u32 args_len);
JZ_IMPORT("js_log") void jz_js_log(u32 level, const char *p, u32 n);
JZ_IMPORT("js_throw") void jz_js_throw(const char *p, u32 n);

#endif
