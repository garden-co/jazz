/**
 * Deterministic row ids (RFC 4122 version 5, name-based SHA-1).
 *
 * The catalogue seed, a shopper's cart, each cart line and each order derive
 * their ids from stable names. Writing "the same thing" twice (a reseed, two
 * devices adding one product offline, a retried checkout) therefore addresses
 * the same row instead of creating a duplicate. Synchronous and dependency-free
 * so browser event handlers and server routes compute identical ids.
 */

const JAMAZON_NAMESPACE = "6a2b1f4e-4d0c-4c7e-9a51-3f7a0e5d2c11";

export const ids = {
  category: (slug: string) => uuidV5(`category:${slug}`),
  product: (sku: string) => uuidV5(`product:${sku}`),
  stock: (sku: string) => uuidV5(`stock:${sku}`),
  cart: (account: string) => uuidV5(`cart:${account}`),
  cartLine: (cartId: string, productId: string) => uuidV5(`cart-line:${cartId}:${productId}`),
  /** Scoped by account, so one shopper's key can never address another's order. */
  order: (account: string, idempotencyKey: string) => uuidV5(`order:${account}:${idempotencyKey}`),
  orderLine: (orderId: string, productId: string) => uuidV5(`order-line:${orderId}:${productId}`),
  orderEvent: (orderId: string, status: string) => uuidV5(`order-event:${orderId}:${status}`),
  payment: (orderId: string) => uuidV5(`payment:${orderId}`),
};

/** Short, human-readable order reference derived from the order id. */
export function orderCode(orderId: string): string {
  return `J-${orderId.replaceAll("-", "").slice(0, 8).toUpperCase()}`;
}

export function uuidV5(name: string, namespace = JAMAZON_NAMESPACE): string {
  const ns = hexToBytes(namespace.replaceAll("-", ""));
  const bytes = sha1(concat(ns, new TextEncoder().encode(name))).slice(0, 16);
  bytes[6] = (bytes[6]! & 0x0f) | 0x50;
  bytes[8] = (bytes[8]! & 0x3f) | 0x80;
  const hex = Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function hexToBytes(hex: string): Uint8Array {
  const out = new Uint8Array(hex.length / 2);
  for (let i = 0; i < out.length; i++) out[i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  return out;
}

function concat(a: Uint8Array, b: Uint8Array): Uint8Array {
  const out = new Uint8Array(a.length + b.length);
  out.set(a);
  out.set(b, a.length);
  return out;
}

function sha1(message: Uint8Array): Uint8Array {
  const bitLength = message.length * 8;
  const padded = new Uint8Array(((message.length + 9 + 63) >> 6) << 6);
  padded.set(message);
  padded[message.length] = 0x80;
  const view = new DataView(padded.buffer);
  view.setUint32(padded.length - 4, bitLength >>> 0);
  view.setUint32(padded.length - 8, Math.floor(bitLength / 2 ** 32));

  let h0 = 0x67452301,
    h1 = 0xefcdab89,
    h2 = 0x98badcfe,
    h3 = 0x10325476,
    h4 = 0xc3d2e1f0;
  const w = new Uint32Array(80);
  for (let offset = 0; offset < padded.length; offset += 64) {
    for (let i = 0; i < 16; i++) w[i] = view.getUint32(offset + i * 4);
    for (let i = 16; i < 80; i++) w[i] = rotl(w[i - 3]! ^ w[i - 8]! ^ w[i - 14]! ^ w[i - 16]!, 1);
    let a = h0,
      b = h1,
      c = h2,
      d = h3,
      e = h4;
    for (let i = 0; i < 80; i++) {
      const [f, k] =
        i < 20
          ? [(b & c) | (~b & d), 0x5a827999]
          : i < 40
            ? [b ^ c ^ d, 0x6ed9eba1]
            : i < 60
              ? [(b & c) | (b & d) | (c & d), 0x8f1bbcdc]
              : [b ^ c ^ d, 0xca62c1d6];
      const temp = (rotl(a, 5) + f + e + k + w[i]!) >>> 0;
      e = d;
      d = c;
      c = rotl(b, 30);
      b = a;
      a = temp;
    }
    h0 = (h0 + a) >>> 0;
    h1 = (h1 + b) >>> 0;
    h2 = (h2 + c) >>> 0;
    h3 = (h3 + d) >>> 0;
    h4 = (h4 + e) >>> 0;
  }
  const out = new Uint8Array(20);
  const outView = new DataView(out.buffer);
  [h0, h1, h2, h3, h4].forEach((h, i) => outView.setUint32(i * 4, h));
  return out;
}

function rotl(value: number, bits: number): number {
  return ((value << bits) | (value >>> (32 - bits))) >>> 0;
}
