/* rt_probe.c: size-measurement harness for the runtime (not part of the port).
 * Compiles to nothing unless -DJZ_RT_PROBE; then -DP_ALL or one of -DP_CONTAINERS, -DP_STRINGS,
 * -DP_UUID, -DP_POSTCARD, -DP_JSON, -DP_CRYPTO, -DP_ZSTD, -DP_TASK, -DP_BASE_ALLOC, -DP_BASE_FMT
 * selects which exports exist. Built with -fno-lto (rt/probe/cflags.txt) so LTO cannot inline
 * runtime functions into the harness. See reports/runtime.md for the commands. */
#ifdef JZ_RT_PROBE
/* Probe: references a broad set of runtime functions through exports so nothing is gc'd. */
#include "jz_rt.h"
typedef struct ruzstd_FrameDecoder ruzstd_FrameDecoder;
ruzstd_FrameDecoder *ruzstd__decoding__frame_decoder__FrameDecoder__default(void);
void ruzstd__decoding__frame_decoder__FrameDecoder__drop(ruzstd_FrameDecoder *d);
jz_err ruzstd__decoding__frame_decoder__FrameDecoder__init(ruzstd_FrameDecoder *d, const u8 *p, u32 n);
jz_err ruzstd__decoding__frame_decoder__FrameDecoder__set_max_block_output(ruzstd_FrameDecoder *d, u32 limit);
jz_err ruzstd__decoding__frame_decoder__FrameDecoder__decode_blocks_upto(ruzstd_FrameDecoder *d, const u8 *p, u32 n, u32 blocks);
u32 ruzstd__decoding__frame_decoder__FrameDecoder__read_streaming(ruzstd_FrameDecoder *d, u8 *out, u32 n);
u32 ruzstd__decoding__frame_decoder__FrameDecoder__streaming_available(const ruzstd_FrameDecoder *d);
void ruzstd__decoding__errors__Display__fmt(jz_str *out, const jz_err *e);
typedef struct ruzstd_StreamingDecoder ruzstd_StreamingDecoder;
jz_err ruzstd__decoding__streaming_decoder__StreamingDecoder__new(const u8 *p, u32 n, ruzstd_StreamingDecoder **out);
jz_err ruzstd__decoding__streaming_decoder__StreamingDecoder__read_to_end_take(ruzstd_StreamingDecoder *d, u64 limit, jz_bytes *out);
void ruzstd__decoding__streaming_decoder__StreamingDecoder__drop(ruzstd_StreamingDecoder *d);
static u64 hk(const void *k) { return jz_str_hash((const jz_str *)k); }
static bool ek(const void *a, const void *b) { return jz_str_eq((const jz_str *)a, (const jz_str *)b); }
static void sd(void *p) { jz_str_drop((jz_str *)p); }
static void bd(void *p) { jz_bytes_drop((jz_bytes *)p); }
static void sc(void *d, const void *s) { *(jz_str *)d = jz_str_clone((const jz_str *)s); }
static const jz_btree_desc BD = {sizeof(jz_str), sizeof(jz_bytes), jz_cmp_str, sd, bd};
static const jz_btree_desc UD = {16, 8, jz_cmp_uuid, NULL, NULL};
static const jz_hmap_desc HD = {sizeof(jz_str), 8, hk, ek, sd, NULL};
static bool keep(const void *k, void *v, void *c) { return ((const jz_str *)k)->len & 1; }
static bool keepv(void *it, void *c) { return *(u32 *)it & 1; }
static int cmp2(const void *a, const void *b) { return jz_cmp_u32(a, b, NULL); }
static jz_interner *pool;

#if defined(P_CONTAINERS) || defined(P_ALL)
JZ_EXPORT("p_containers") u32 p_containers(const u8 *p, u32 n) {
    jz_btree t; jz_btree_init(&t, &BD);
    jz_hmap m; jz_hmap_init(&m, &HD);
    for (u32 i = 0; i + 4 <= n; i += 4) {
        jz_str k = jz_str_from_slice(jz_slice_of(p + i, 4));
        jz_bytes v = jz_bytes_from(p, i);
        jz_btree_insert(&t, &k, &v, NULL);
        jz_str k2 = jz_str_clone(&k);
        u64 c = i;
        jz_hmap_insert(&m, &k2, &c, NULL);
        bool ins; jz_str k3 = jz_str_clone(&k); *(u64 *)jz_hmap_entry(&m, &k3, &ins) += 1;
        jz_str k4 = jz_str_clone(&k); jz_btree_entry(&t, &k4, &ins);
    }
    u32 acc = t.len + m.len;
    jz_str *k; jz_bytes *v; u64 *hv;
    JZ_BTREE_FOR(&t, k, v) acc += v->len;
    JZ_HMAP_FOR(&m, k, hv) acc += (u32)*hv;
    jz_btree_iter it; jz_str lo = jz_str_from("a"), hi = jz_str_from("q");
    jz_btree_range(&it, &t, &lo, true, &hi, false);
    while (jz_btree_next(&it, (void **)&k, (void **)&v)) acc++;
    jz_btree c; jz_btree_clone(&c, &t, sc, NULL); /* shallow val clone fine for probe */
    acc += jz_btree_eq(&c, &t, NULL);
    jz_btree_retain(&t, keep, NULL);
    jz_hmap_retain(&m, keep, NULL);
    if (jz_btree_get(&t, &lo)) acc++;
    if (jz_btree_contains(&t, &hi)) acc++;
    if (jz_hmap_get(&m, &lo)) acc++;
    jz_str ok; jz_bytes ov; u64 oh;
    if (jz_btree_remove(&t, &lo, &ok, &ov)) { jz_str_drop(&ok); jz_bytes_drop(&ov); }
    if (jz_hmap_remove(&m, &lo, &ok, &oh)) jz_str_drop(&ok);
    void *fk, *fv;
    if (jz_btree_first(&t, &fk, &fv)) acc++;
    if (jz_btree_last(&t, &fk, &fv)) acc++;
    if (jz_btree_pop_first(&t, &ok, &ov)) { jz_str_drop(&ok); jz_bytes_drop(&ov); }
    jz_btree u; jz_btree_init(&u, &UD);
    struct { jz_uuid k; u64 v; } pairs[4];
    for (int i = 0; i < 4; i++) { memset(&pairs[i], 0, sizeof pairs[i]); pairs[i].k.b[0] = p[i]; }
    jz_btree_from_pairs(&u, &UD, pairs, 4);
    jz_btree u2; jz_btree_init(&u2, &UD); jz_btree_from_pairs_ex(&u2, &UD, pairs, 4, sizeof pairs[0], 16);
    jz_btree_append(&u, &u2);
    acc += u.len;
    jz_btree_clear(&u);
    jz_hmap_clear(&m);
    jz_btree_drop(&t); jz_hmap_drop(&m); jz_btree_drop(&u2);
    /* vec */
    JZ_VEC(u32) vv = {0};
    for (u32 i = 0; i < n; i++) JZ_VEC_PUSH(vv, p[i]);
    u32 x = 5; jz_vec_insert(&vv, 0, &x, 4); jz_vec_remove(&vv, 0, &x, 4); jz_vec_swap_remove(&vv, 0, &x, 4);
    jz_sort(vv.ptr, vv.len, 4, jz_cmp_u32, NULL);
    jz_sort_unstable(vv.ptr, vv.len, 4, jz_cmp_u32, NULL);
    u32 idx; acc += jz_bsearch(vv.ptr, vv.len, 4, &x, jz_cmp_u32, NULL, &idx);
    jz_vec_dedup(&vv, 4, cmp2, NULL);
    jz_vec_retain(&vv, 4, keepv, NULL, NULL);
    JZ_VEC(u32) v2; jz_vec_clone(&v2, &vv, 4, NULL); jz_vec_extend(&v2, vv.ptr, vv.len, 4); jz_vec_truncate(&v2, 1, 4, NULL);
    acc += v2.len + *(u32 *)jz_vec_push_slot(&v2, 4);
    jz_vec_drop(&vv, 4, NULL); jz_vec_drop(&v2, 4, NULL);
    /* deque */
    jz_deque q; jz_deque_init(&q, 4);
    for (u32 i = 0; i < n; i++) { u32 y = p[i]; if (y & 1) jz_deque_push_back(&q, &y); else jz_deque_push_front(&q, &y); }
    acc += *(u32 *)jz_deque_at(&q, 0);
    while (jz_deque_pop_front(&q, &x)) acc += x;
    jz_deque_pop_back(&q, &x);
    jz_deque_drop(&q, NULL);
    /* rc / intern */
    jz_str *r = jz_rc_new(sizeof(jz_str), sd); *r = jz_str_from("rc");
    jz_str *r2 = jz_rc_clone(r); r2 = jz_rc_make_mut((void **)&r2, sizeof(jz_str), sc);
    acc += jz_rc_strong(r); jz_rc_release(r); jz_rc_release(r2);
    jz_str is = jz_str_from_slice(jz_slice_of(p, n));
    bool ins2; const jz_str *ip = jz_intern_ex(&pool, &is, sizeof is, hk, ek, &ins2);
    if (!ins2) jz_str_drop(&is);
    acc += (u32)(uintptr_t)jz_intern(&pool, ip, sizeof *ip, hk, ek);
    return acc;
}

#endif
#if defined(P_STRINGS) || defined(P_ALL)
JZ_EXPORT("p_strings") u32 p_strings(const u8 *p, u32 n, jz_str *out) {
    jz_str s = jz_str_from("x");
    jz_str_push(&s, (const char *)p, n);
    jz_str_push_cstr(&s, "abc");
    jz_str_push_char(&s, '!');
    jz_str f = jz_format("%s %u %llu %.*s %f", "a", n, (unsigned long long)n, (int)n, (const char *)p, (double)n);
    jz_str_appendf(&f, "%d", (int)n);
    u32 acc = jz_str_eq(&s, &f) + jz_str_eq_cstr(&s, "q") + jz_str_cmp(&s, &f) + (u32)jz_str_hash(&s) + jz_utf8_valid(p, n);
    jz_bytes b = jz_bytes_from(p, n); jz_bytes_push_u8(&b, 1); jz_bytes_push(&b, p, n);
    jz_bytes b2 = jz_bytes_clone(&b);
    acc += jz_bytes_cmp(&b, &b2) + jz_bytes_eq(&b, &b2) + jz_slice_cmp(jz_bytes_slice(&b), jz_str_slice(&s));
    jz_base64_encode(p, n, p[0] & 1, p[0] & 2, out);
    jz_hex_encode(p, n, out);
    jz_bytes d = {0};
    jz_err e = jz_base64_decode(jz_slice_of(p, n), p[0] & 1, &d);
    if (e.code) { acc += e.code; jz_err_drop(&e); }
    e = jz_hex_decode(jz_slice_of(p, n), &d);
    e = jz_err_context(e, "ctx");
    jz_err_drop(&e);
    jz_bytes_drop(&b); jz_bytes_drop(&b2); jz_bytes_drop(&d);
    jz_str_drop(&f);
    *out = jz_str_clone(&s);
    jz_str_drop(&s);
    void *m = jz_alloc(n); m = jz_realloc(m, n * 2); jz_free(m); jz_free(jz_memdup(p, n));
    if (n == 7) jz_unwrap_failed();
    if (n == 8) jz_oom();
    if (n == 9) jz_panic("probe");
    return acc;
}

#endif
#if defined(P_UUID) || defined(P_ALL)
JZ_EXPORT("p_uuid") u32 p_uuid(const u8 *p, u32 n, char *out) {
    jz_uuid a = jz_uuid_v4(), b = jz_uuid_v7(jz_now_ms(), p), c = jz_uuid_v5(&a, p, n);
    jz_uuid_to_string(&c, out);
    jz_uuid d;
    jz_err e = jz_uuid_parse(jz_slice_of(p, n), &d);
    u32 acc = e.code + jz_uuid_cmp(&a, &b) + jz_uuid_eq(&b, &c) + (u32)jz_uuid_hash(&d) + jz_uuid_eq_v(&a, &d);
    jz_err_drop(&e);
    u8 rb[8]; jz_random_bytes(rb, 8);
    return acc + rb[0];
}

#endif
#if defined(P_POSTCARD) || defined(P_ALL)
JZ_EXPORT("p_postcard") u32 p_postcard(const u8 *p, u32 n, jz_bytes *out) {
    jz_pc_w w = {out};
    jz_str s = jz_str_from("hi"); jz_uuid u = {{0}};
    jz_pc_u8(&w, 1); jz_pc_bool(&w, true); jz_pc_varint(&w, n); jz_pc_zigzag(&w, -(i64)n); jz_pc_f64(&w, 1.5); jz_pc_f32(&w, 2.5f);
    jz_pc_bytes(&w, p, n); jz_pc_str(&w, &s); jz_pc_raw(&w, p, 2); jz_pc_uuid(&w, &u); jz_pc_option(&w, true); jz_pc_seq(&w, 3); jz_pc_variant(&w, 2);
    jz_pc_r r = {p, p + n, 0};
    u8 a; bool b; u64 v; u32 v32; u16 v16; i64 z; f64 f; jz_bytes by; jz_slice sl; jz_str st; u8 raw[4]; jz_uuid uu; u32 sq, var;
    jz_err e;
#define T(x) e = (x); if (e.code) { u32 c = e.code; jz_err_drop(&e); return c; }
    T(jz_pc_r_enter(&r)); T(jz_pc_r_u8(&r, &a)); T(jz_pc_r_bool(&r, &b)); T(jz_pc_r_varint(&r, &v)); T(jz_pc_r_u32(&r, &v32)); T(jz_pc_r_u16(&r, &v16));
    T(jz_pc_r_zigzag(&r, &z)); T(jz_pc_r_f64(&r, &f)); T(jz_pc_r_bytes(&r, &by)); T(jz_pc_r_bytes_ref(&r, &sl)); T(jz_pc_r_str(&r, &st));
    T(jz_pc_r_raw(&r, raw, 4)); T(jz_pc_r_uuid(&r, &uu)); T(jz_pc_r_option(&r, &b)); T(jz_pc_r_seq(&r, &sq)); T(jz_pc_r_variant(&r, &var, 5));
    jz_pc_r_leave(&r); T(jz_pc_r_finish(&r));
    return a + b + (u32)v + v32 + v16 + (u32)z + (u32)f + by.len + sl.len + st.len + uu.b[0] + sq + var;
}

#endif
#if defined(P_JSON) || defined(P_ALL)
JZ_EXPORT("p_json") u32 p_json(const u8 *p, u32 n, jz_str *out) {
    jz_json *j;
    jz_err e = jz_json_parse(jz_slice_of(p, n), &j);
    if (e.code) { u32 c = e.code; jz_err_drop(&e); return c; }
    jz_json_write(j, out);
    jz_json_write_pretty(j, out);
    jz_json *c = jz_json_clone(j);
    u32 acc = jz_json_eq(j, c);
    jz_json *o = jz_json_obj(), *arr = jz_json_arr();
    jz_json_arr_push(arr, jz_json_null()); jz_json_arr_push(arr, jz_json_bool(true)); jz_json_arr_push(arr, jz_json_u64(1));
    jz_json_arr_push(arr, jz_json_i64(-1)); jz_json_arr_push(arr, jz_json_f64(0.5)); jz_json_arr_push(arr, jz_json_str("s"));
    jz_json_arr_push(arr, jz_json_str_owned(jz_str_from("t")));
    jz_json_obj_set(o, "arr", arr);
    jz_json_obj_set(o, "j", c);
    const jz_json *g = jz_json_obj_get(j, "k");
    u64 u; i64 i; f64 f;
    acc += jz_json_as_u64(g, &u) + jz_json_as_i64(g, &i) + jz_json_as_f64(g, &f) + (jz_json_as_str(g) != NULL);
    jz_str s; bool b;
    e = jz_json_expect_obj(j, "struct X"); jz_err_drop(&e);
    e = jz_json_field_str(j, "a", &s); if (!e.code) jz_str_drop(&s); jz_err_drop(&e);
    e = jz_json_field_u64(j, "b", &u); jz_err_drop(&e);
    e = jz_json_field_bool(j, "c", &b); jz_err_drop(&e);
    static const char *const names[] = {"A", "B", "C"};
    u32 idx; const jz_json *pl;
    e = jz_json_variant(j, names, 3, &idx, &pl); jz_err_drop(&e);
    jz_json_w w = {out};
    jz_jw_begin_obj(&w); jz_jw_key(&w, "a"); jz_jw_begin_arr(&w); jz_jw_str(&w, "x", 1); jz_jw_u64(&w, 1); jz_jw_i64(&w, -1);
    jz_jw_f64(&w, 1.25); jz_jw_bool(&w, true); jz_jw_null(&w); jz_jw_end_arr(&w); jz_jw_end_obj(&w);
    jz_json_free(o); jz_json_free(j);
    return acc;
}

#endif
#if defined(P_CRYPTO) || defined(P_ALL)
JZ_EXPORT("p_crypto") u32 p_crypto(const u8 *p, u32 n, u8 *out) {
    jz_sha256(p, n, out); jz_sha512(p, n, out + 32); jz_sha1(p, n, out + 96); jz_blake3(p, n, out + 116);
    jz_blake3_hasher *h = jz_blake3_new(); jz_blake3_update(h, p, n); jz_blake3_finalize(h, out + 148); jz_blake3_free(h);
    u8 pk[32]; jz_ed25519_public_key(p, pk); jz_ed25519_sign(p, pk, p, n, out + 180);
    return jz_ed25519_verify(pk, p, n, out + 180);
}

#endif
#if defined(P_ZSTD) || defined(P_ALL)
JZ_EXPORT("p_zstd") u32 p_zstd(const u8 *p, u32 n, jz_bytes *out) {
    jz_err e = jz_zstd_decompress(p, n, out, 1 << 20);
    u32 c = e.code; jz_err_drop(&e);
    ruzstd_FrameDecoder *d = ruzstd__decoding__frame_decoder__FrameDecoder__default();
    jz_str s = {0};
    e = ruzstd__decoding__frame_decoder__FrameDecoder__init(d, p, 6);
    if (!e.code) e = ruzstd__decoding__frame_decoder__FrameDecoder__set_max_block_output(d, 1 << 17);
    if (!e.code) e = ruzstd__decoding__frame_decoder__FrameDecoder__decode_blocks_upto(d, p + 6, n - 6, 1);
    if (e.code) { ruzstd__decoding__errors__Display__fmt(&s, &e); jz_err_drop(&e); }
    c += ruzstd__decoding__frame_decoder__FrameDecoder__read_streaming(d, out->ptr, out->len);
    c += ruzstd__decoding__frame_decoder__FrameDecoder__streaming_available(d);
    ruzstd__decoding__frame_decoder__FrameDecoder__drop(d);
    ruzstd_StreamingDecoder *sd;
    e = ruzstd__decoding__streaming_decoder__StreamingDecoder__new(p, n, &sd);
    if (!e.code) { e = ruzstd__decoding__streaming_decoder__StreamingDecoder__read_to_end_take(sd, 1000, out); ruzstd__decoding__streaming_decoder__StreamingDecoder__drop(sd); }
    c += e.code + s.len; jz_err_drop(&e); jz_str_drop(&s);
    return c;
}

#endif
#if defined(P_TASK) || defined(P_ALL)
static void tf(void *c) { (*(u32 *)c)++; }
static void wk(void *c) { jz_task_spawn(tf, c, NULL); }
JZ_EXPORT("p_task") u32 p_task(u32 lvl) {
    static u32 cnt;
    jz_trace_level = lvl;
    u32 sp = jz_trace_span_enter("probe");
    JZ_TRACE(3, "probe", "hello %u", lvl);
    jz_counter_add("probe.calls", 1);
    jz_task_spawn(tf, &cnt, NULL);
    jz_chan *c = jz_chan_new(4);
    jz_chan_send(c, &lvl);
    u32 x = 0; jz_chan_try_recv(c, &x);
    jz_waker w = {wk, &cnt}; jz_waker_wake(&w);
    jz_chan_close(c); x += jz_chan_is_closed(c); jz_chan_release(c); jz_chan_release(c);
    jz_task_run_ready();
    jz_trace_span_exit(sp);
    return cnt + x;
}
#endif
#if defined(P_BASE_ALLOC)
JZ_EXPORT("p_base") u32 p_base(u32 n) { void *p = jz_alloc(n); p = jz_realloc(p, 2 * n); jz_free(p); return n; }
#endif
#if defined(P_BASE_FMT)
JZ_EXPORT("p_base") u32 p_base(u32 n) { jz_str s = jz_format("%u", n); u32 l = s.len; jz_str_drop(&s); return l; }
#endif
#endif /* JZ_RT_PROBE */
