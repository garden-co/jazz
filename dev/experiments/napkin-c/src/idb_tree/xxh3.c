/* xxhash-rust 0.8 xxh3::xxh3_64 (scalar path), the only dependency of
 * idb_tree beyond std. Ported here because the shared runtime has no xxh3. */
#include "idb_tree/idb_tree.h"

#define STRIPE_LEN 64u
#define SECRET_CONSUME_RATE 8u
#define ACC_NB 8u
#define SECRET_MERGEACCS_START 11u
#define SECRET_LASTACC_START 7u
#define MID_SIZE_MAX 240u
#define SECRET_SIZE_MIN 136u
#define DEFAULT_SECRET_SIZE 192u

#define P32_1 0x9E3779B1u
#define P32_2 0x85EBCA77u
#define P32_3 0xC2B2AE3Du
#define P64_1 0x9E3779B185EBCA87ull
#define P64_2 0xC2B2AE3D27D4EB4Full
#define P64_3 0x165667B19E3779F9ull
#define P64_4 0x85EBCA77C2B2AE63ull
#define P64_5 0x27D4EB2F165667C5ull

static const u8 DEFAULT_SECRET[DEFAULT_SECRET_SIZE] = {
    0xb8, 0xfe, 0x6c, 0x39, 0x23, 0xa4, 0x4b, 0xbe, 0x7c, 0x01, 0x81, 0x2c, 0xf7, 0x21, 0xad, 0x1c,
    0xde, 0xd4, 0x6d, 0xe9, 0x83, 0x90, 0x97, 0xdb, 0x72, 0x40, 0xa4, 0xa4, 0xb7, 0xb3, 0x67, 0x1f,
    0xcb, 0x79, 0xe6, 0x4e, 0xcc, 0xc0, 0xe5, 0x78, 0x82, 0x5a, 0xd0, 0x7d, 0xcc, 0xff, 0x72, 0x21,
    0xb8, 0x08, 0x46, 0x74, 0xf7, 0x43, 0x24, 0x8e, 0xe0, 0x35, 0x90, 0xe6, 0x81, 0x3a, 0x26, 0x4c,
    0x3c, 0x28, 0x52, 0xbb, 0x91, 0xc3, 0x00, 0xcb, 0x88, 0xd0, 0x65, 0x8b, 0x1b, 0x53, 0x2e, 0xa3,
    0x71, 0x64, 0x48, 0x97, 0xa2, 0x0d, 0xf9, 0x4e, 0x38, 0x19, 0xef, 0x46, 0xa9, 0xde, 0xac, 0xd8,
    0xa8, 0xfa, 0x76, 0x3f, 0xe3, 0x9c, 0x34, 0x3f, 0xf9, 0xdc, 0xbb, 0xc7, 0xc7, 0x0b, 0x4f, 0x1d,
    0x8a, 0x51, 0xe0, 0x4b, 0xcd, 0xb4, 0x59, 0x31, 0xc8, 0x9f, 0x7e, 0xc9, 0xd9, 0x78, 0x73, 0x64,
    0xea, 0xc5, 0xac, 0x83, 0x34, 0xd3, 0xeb, 0xc3, 0xc5, 0x81, 0xa0, 0xff, 0xfa, 0x13, 0x63, 0xeb,
    0x17, 0x0d, 0xdd, 0x51, 0xb7, 0xf0, 0xda, 0x49, 0xd3, 0x16, 0x55, 0x26, 0x29, 0xd4, 0x68, 0x9e,
    0x2b, 0x16, 0xbe, 0x58, 0x7d, 0x47, 0xa1, 0xfc, 0x8f, 0xf8, 0xb8, 0xd1, 0x7a, 0xd0, 0x31, 0xce,
    0x45, 0xcb, 0x3a, 0x8f, 0x95, 0x16, 0x04, 0x28, 0xaf, 0xd7, 0xfb, 0xca, 0xbb, 0x4b, 0x40, 0x7e,
};

static inline u32 rd32(const u8 *p) { u32 v; memcpy(&v, p, 4); return v; }
static inline u64 rd64(const u8 *p) { u64 v; memcpy(&v, p, 8); return v; }
static inline u64 rotl64(u64 v, u32 r) { return (v << r) | (v >> (64 - r)); }
static inline u64 xorshift64(u64 v, u32 s) { return v ^ (v >> s); }

static u64 avalanche(u64 v) {
    v = xorshift64(v, 37);
    v *= 0x165667919E3779F9ull;
    return xorshift64(v, 32);
}

static u64 strong_avalanche(u64 v, u64 len) {
    v ^= rotl64(v, 49) ^ rotl64(v, 24);
    v *= 0x9FB21C651E98DF25ull;
    v ^= (v >> 35) + len;
    v *= 0x9FB21C651E98DF25ull;
    return xorshift64(v, 28);
}

static u64 xxh64_avalanche(u64 v) {
    v ^= v >> 33;
    v *= P64_2;
    v ^= v >> 29;
    v *= P64_3;
    v ^= v >> 32;
    return v;
}

static u64 mul128_fold64(u64 a, u64 b) {
    unsigned __int128 p = (unsigned __int128)a * b;
    return (u64)p ^ (u64)(p >> 64);
}

static u64 mix16_b(const u8 *input, const u8 *secret, u64 seed) {
    u64 lo = rd64(input), hi = rd64(input + 8);
    lo ^= rd64(secret) + seed;
    hi ^= rd64(secret + 8) - seed;
    return mul128_fold64(lo, hi);
}

static void accumulate_512(u64 acc[ACC_NB], const u8 *input, const u8 *secret) {
    for (u32 i = 0; i < ACC_NB; i++) {
        u64 data_val = rd64(input + 8 * i);
        u64 data_key = data_val ^ rd64(secret + 8 * i);
        acc[i ^ 1] += data_val;
        acc[i] += (u64)(u32)(data_key & 0xFFFFFFFFu) * (u64)(u32)(data_key >> 32);
    }
}

static void scramble_acc(u64 acc[ACC_NB], const u8 *secret) {
    for (u32 i = 0; i < ACC_NB; i++) {
        u64 key = rd64(secret + 8 * i);
        u64 a = xorshift64(acc[i], 47);
        a ^= key;
        acc[i] = a * (u64)P32_1;
    }
}

static void accumulate_loop(u64 acc[ACC_NB], const u8 *input, const u8 *secret, u32 nb_stripes) {
    for (u32 i = 0; i < nb_stripes; i++) accumulate_512(acc, input + i * STRIPE_LEN, secret + i * SECRET_CONSUME_RATE);
}

static void hash_long_internal_loop(u64 acc[ACC_NB], const u8 *input, u32 len, const u8 *secret, u32 secret_len) {
    u32 nb_stripes = (secret_len - STRIPE_LEN) / SECRET_CONSUME_RATE;
    u32 block_len = STRIPE_LEN * nb_stripes;
    u32 nb_blocks = (len - 1) / block_len;
    for (u32 i = 0; i < nb_blocks; i++) {
        accumulate_loop(acc, input + i * block_len, secret, nb_stripes);
        scramble_acc(acc, secret + secret_len - STRIPE_LEN);
    }
    nb_stripes = ((len - 1) - (block_len * nb_blocks)) / STRIPE_LEN;
    accumulate_loop(acc, input + nb_blocks * block_len, secret, nb_stripes);
    accumulate_512(acc, input + len - STRIPE_LEN, secret + secret_len - STRIPE_LEN - SECRET_LASTACC_START);
}

static u64 xxh3_64_1to3(const u8 *in, u32 len, u64 seed, const u8 *secret) {
    u8 c1 = in[0], c2 = in[len >> 1], c3 = in[len - 1];
    u32 combo = (u32)c1 << 16 | (u32)c2 << 24 | (u32)c3 | len << 8;
    u64 flip = (u64)(rd32(secret) ^ rd32(secret + 4)) + seed;
    return xxh64_avalanche((u64)combo ^ flip);
}

static u64 xxh3_64_4to8(const u8 *in, u32 len, u64 seed, const u8 *secret) {
    seed ^= (u64)__builtin_bswap32((u32)seed) << 32;
    u32 in1 = rd32(in), in2 = rd32(in + len - 4);
    u64 flip = (rd64(secret + 8) ^ rd64(secret + 16)) - seed;
    u64 in64 = (u64)in2 + ((u64)in1 << 32);
    return strong_avalanche(in64 ^ flip, len);
}

static u64 xxh3_64_9to16(const u8 *in, u32 len, u64 seed, const u8 *secret) {
    u64 flip1 = (rd64(secret + 24) ^ rd64(secret + 32)) + seed;
    u64 flip2 = (rd64(secret + 40) ^ rd64(secret + 48)) - seed;
    u64 lo = rd64(in) ^ flip1;
    u64 hi = rd64(in + len - 8) ^ flip2;
    u64 acc = (u64)len + __builtin_bswap64(lo) + hi + mul128_fold64(lo, hi);
    return avalanche(acc);
}

static u64 xxh3_64_0to16(const u8 *in, u32 len, u64 seed, const u8 *secret) {
    if (len > 8) return xxh3_64_9to16(in, len, seed, secret);
    if (len >= 4) return xxh3_64_4to8(in, len, seed, secret);
    if (len > 0) return xxh3_64_1to3(in, len, seed, secret);
    return xxh64_avalanche(seed ^ (rd64(secret + 56) ^ rd64(secret + 64)));
}

static u64 xxh3_64_7to128(const u8 *in, u32 len, u64 seed, const u8 *secret) {
    u64 acc = (u64)len * P64_1;
    if (len > 32) {
        if (len > 64) {
            if (len > 96) {
                acc += mix16_b(in + 48, secret + 96, seed);
                acc += mix16_b(in + len - 64, secret + 112, seed);
            }
            acc += mix16_b(in + 32, secret + 64, seed);
            acc += mix16_b(in + len - 48, secret + 80, seed);
        }
        acc += mix16_b(in + 16, secret + 32, seed);
        acc += mix16_b(in + len - 32, secret + 48, seed);
    }
    acc += mix16_b(in, secret, seed);
    acc += mix16_b(in + len - 16, secret + 16, seed);
    return avalanche(acc);
}

__attribute__((noinline)) static u64 xxhash_rust__xxh3__xxh3_64_129to240(const u8 *in, u32 len, u64 seed, const u8 *secret) {
    u64 acc = (u64)len * P64_1;
    u32 nb_rounds = len / 16;
    u32 i = 0;
    while (i < 8) {
        acc += mix16_b(in + 16 * i, secret + 16 * i, seed);
        i++;
    }
    acc = avalanche(acc);
    while (i < nb_rounds) {
        acc += mix16_b(in + 16 * i, secret + 16 * (i - 8) + 3, seed);
        i++;
    }
    acc += mix16_b(in + len - 16, secret + SECRET_SIZE_MIN - 17, seed);
    return avalanche(acc);
}

__attribute__((noinline)) static u64 xxhash_rust__xxh3__xxh3_64_long_default(const u8 *in, u32 len) {
    u64 acc[ACC_NB] = {P32_3, P64_1, P64_2, P64_3, P64_4, P32_2, P64_5, P32_1};
    hash_long_internal_loop(acc, in, len, DEFAULT_SECRET, DEFAULT_SECRET_SIZE);
    const u8 *secret = DEFAULT_SECRET + SECRET_MERGEACCS_START;
    u64 result = (u64)len * P64_1;
    for (u32 i = 0; i < 4; i++)
        result += mul128_fold64(acc[2 * i] ^ rd64(secret + 16 * i), acc[2 * i + 1] ^ rd64(secret + 16 * i + 8));
    return avalanche(result);
}

u64 xxhash_rust__xxh3__xxh3_64(const u8 *in, u32 len) {
    if (len <= 16) return xxh3_64_0to16(in, len, 0, DEFAULT_SECRET);
    if (len <= 128) return xxh3_64_7to128(in, len, 0, DEFAULT_SECRET);
    if (len <= MID_SIZE_MAX) return xxhash_rust__xxh3__xxh3_64_129to240(in, len, 0, DEFAULT_SECRET);
    return xxhash_rust__xxh3__xxh3_64_long_default(in, len);
}
