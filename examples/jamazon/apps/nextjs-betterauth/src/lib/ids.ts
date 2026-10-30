/**
 * Deterministic row ids (RFC 4122 version 5, name-based SHA-1).
 *
 * The catalogue seed, a shopper's cart, each cart line and each order derive
 * their ids from stable names. Writing "the same thing" twice (a reseed, two
 * devices adding one product offline, a retried checkout) therefore addresses
 * the same row instead of creating a duplicate. Synchronous, so browser
 * event handlers and server routes compute identical ids.
 */
import { sha1 } from "@noble/hashes/legacy.js";
import { hexToBytes, utf8ToBytes } from "@noble/hashes/utils.js";

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
  const bytes = sha1(new Uint8Array([...ns, ...utf8ToBytes(name)])).slice(0, 16);
  bytes[6] = (bytes[6]! & 0x0f) | 0x50;
  bytes[8] = (bytes[8]! & 0x3f) | 0x80;
  const hex = Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}
