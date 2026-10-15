// Compact XXH3-64 (seed 0, default secret): the xxhash-rust dependency of idb-tree.
const M = (1n << 64n) - 1n;
const P32_1 = 0x9e3779b1n;
const P32_2 = 0x85ebca77n;
const P32_3 = 0xc2b2ae3dn;
const P64_1 = 0x9e3779b185ebca87n;
const P64_2 = 0xc2b2ae3d27d4eb4fn;
const P64_3 = 0x165667b19e3779f9n;
const P64_4 = 0x85ebca77c2b2ae63n;
const P64_5 = 0x27d4eb2f165667c5n;

const SECRET = Uint8Array.from(
  "b8fe6c3923a44bbe7c01812cf721ad1cded46de9839097db7240a4a4b7b3671fcb79e64eccc0e578825ad07dccff7221b8084674f743248ee03590e6813a264c3c2852bb91c300cb88d0658b1b532ea371644897a20df94e3819ef46a9deacd8a8fa763fe39c343ff9dcbbc7c70b4f1d8a51e04bcdb45931c89f7ec9d9787364eac5ac8334d3ebc3c581a0fffa1363eb170ddd51b7f0da49d3165526 29d4689e2b16be587d47a1fc8ff8b8d17ad031ce45cb3a8f95160428afd7fbcabb4b407e"
    .replace(/ /g, "")
    .match(/../g)!
    .map((h) => parseInt(h, 16)),
);

const r32 = (b: Uint8Array, o: number) =>
  BigInt((b[o] | (b[o + 1] << 8) | (b[o + 2] << 16) | (b[o + 3] << 24)) >>> 0);
const r64 = (b: Uint8Array, o: number) => r32(b, o) | (r32(b, o + 4) << 32n);
const mul = (a: bigint, b: bigint) => (a * b) & M;
const rotl = (v: bigint, n: bigint) => ((v << n) | (v >> (64n - n))) & M;
const swap64 = (v: bigint) => {
  let out = 0n;
  for (let i = 0; i < 8; i++) out = (out << 8n) | ((v >> BigInt(i * 8)) & 0xffn);
  return out;
};
const fold = (a: bigint, b: bigint) => {
  const p = a * b;
  return (p & M) ^ (p >> 64n);
};
const avalanche = (v: bigint) => {
  v = mul(v ^ (v >> 37n), 0x165667919e3779f9n);
  return v ^ (v >> 32n);
};
const avalanche64 = (h: bigint) => {
  h = mul(h ^ (h >> 33n), P64_2);
  h = mul(h ^ (h >> 29n), P64_3);
  return h ^ (h >> 32n);
};
const mix16 = (input: Uint8Array, i: number, s: number) =>
  fold(r64(input, i) ^ r64(SECRET, s), r64(input, i + 8) ^ r64(SECRET, s + 8));

function accumulate(acc: bigint[], input: Uint8Array, i: number, s: number): void {
  for (let k = 0; k < 8; k++) {
    const v = r64(input, i + 8 * k);
    const key = v ^ r64(SECRET, s + 8 * k);
    acc[k ^ 1] = (acc[k ^ 1] + v) & M;
    acc[k] = (acc[k] + (key & 0xffffffffn) * (key >> 32n)) & M;
  }
}

function hashLong(input: Uint8Array): bigint {
  const acc = [P32_3, P64_1, P64_2, P64_3, P64_4, P32_2, P64_5, P32_1];
  const stripes = (SECRET.length - 64) / 8;
  const blockLen = 64 * stripes;
  const blocks = Math.floor((input.length - 1) / blockLen);
  for (let b = 0; b < blocks; b++) {
    for (let s = 0; s < stripes; s++) accumulate(acc, input, b * blockLen + s * 64, s * 8);
    for (let k = 0; k < 8; k++) {
      const a = acc[k] ^ (acc[k] >> 47n) ^ r64(SECRET, SECRET.length - 64 + 8 * k);
      acc[k] = mul(a, P32_1);
    }
  }
  const last = Math.floor((input.length - 1 - blockLen * blocks) / 64);
  for (let s = 0; s < last; s++) accumulate(acc, input, blocks * blockLen + s * 64, s * 8);
  accumulate(acc, input, input.length - 64, SECRET.length - 64 - 7);
  let result = mul(BigInt(input.length), P64_1);
  for (let k = 0; k < 4; k++)
    result =
      (result +
        fold(acc[2 * k] ^ r64(SECRET, 11 + 16 * k), acc[2 * k + 1] ^ r64(SECRET, 19 + 16 * k))) &
      M;
  return avalanche(result);
}

export function xxh3_64(input: Uint8Array): bigint {
  const n = input.length;
  const len = BigInt(n);
  if (n === 0) return avalanche64(r64(SECRET, 56) ^ r64(SECRET, 64));
  if (n <= 3) {
    const combo = (input[0] << 16) | (input[n >> 1] << 24) | input[n - 1] | (n << 8);
    return avalanche64(BigInt(combo >>> 0) ^ (r32(SECRET, 0) ^ r32(SECRET, 4)));
  }
  if (n <= 8) {
    const flip = r64(SECRET, 8) ^ r64(SECRET, 16);
    let v = ((r32(input, n - 4) + (r32(input, 0) << 32n)) & M) ^ flip;
    v ^= rotl(v, 49n) ^ rotl(v, 24n);
    v = mul(v, 0x9fb21c651e98df25n);
    v ^= ((v >> 35n) + len) & M;
    v = mul(v, 0x9fb21c651e98df25n);
    return v ^ (v >> 28n);
  }
  if (n <= 16) {
    const lo = r64(input, 0) ^ r64(SECRET, 24) ^ r64(SECRET, 32);
    const hi = r64(input, n - 8) ^ r64(SECRET, 40) ^ r64(SECRET, 48);
    return avalanche((len + swap64(lo) + hi + fold(lo, hi)) & M);
  }
  if (n <= 128) {
    let acc = mul(len, P64_1);
    const pairs: [number, number][] = [[0, 0]];
    if (n > 32) pairs.push([16, 32]);
    if (n > 64) pairs.push([32, 64]);
    if (n > 96) pairs.push([48, 96]);
    for (const [i, s] of pairs)
      acc = (acc + mix16(input, i, s) + mix16(input, n - 16 - i, s + 16)) & M;
    return avalanche(acc);
  }
  if (n <= 240) {
    let acc = mul(len, P64_1);
    const rounds = Math.floor(n / 16);
    for (let i = 0; i < 8; i++) acc = (acc + mix16(input, 16 * i, 16 * i)) & M;
    acc = avalanche(acc);
    for (let i = 8; i < rounds; i++) acc = (acc + mix16(input, 16 * i, 16 * (i - 8) + 3)) & M;
    return avalanche((acc + mix16(input, n - 16, 136 - 17)) & M);
  }
  return hashLong(input);
}
