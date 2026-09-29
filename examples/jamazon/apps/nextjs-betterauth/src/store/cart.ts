"use client";

import type { Db } from "jazz-tools";
import { useAll, useDb } from "jazz-tools/react";
import { useMemo } from "react";
import { MAX_LINE_QUANTITY } from "@/permissions";
import { app, type Cart, type CartLine, type Product, type Stock } from "@/schema";
import { ids } from "@/src/lib/ids";
import { shippingCents } from "./pricing";

/**
 * The shopper's cart, straight from the local Jazz database: every edit is a
 * local write that works offline and syncs to the shopper's other devices.
 *
 * Merging: each (cart, product) pair has one row with a deterministic id, and
 * the quantity is its only mutable field. Two devices that add the same
 * product converge on one line; if they set different quantities, the later
 * edit wins. Lines are never deleted, only set to zero, so removing and
 * re-adding a product addresses the same row on every device.
 */
export type CartItem = { line: CartLine; product: Product; stock?: Stock };

export function useCart(account: string) {
  const db = useDb<Db>();
  const cartId = ids.cart(account);
  const { data: carts } = useAll(app.carts.where({ id: cartId }));
  const { data: lines } = useAll(app.cartLines.where({ cartId }));
  const { data: products } = useAll(app.products);
  const { data: stock } = useAll(app.stock);
  const cart: Cart | undefined = carts?.[0];

  const items = useMemo<CartItem[]>(() => {
    const out: CartItem[] = [];
    for (const line of lines ?? []) {
      if (line.quantity <= 0) continue;
      const product = products?.find((p) => p.id === line.productId);
      if (!product) continue;
      out.push({ line, product, stock: stock?.find((s) => s.productId === product.id) });
    }
    return out.sort((a, b) => a.product.position - b.product.position);
  }, [lines, products, stock]);

  const count = items.reduce((sum, item) => sum + item.line.quantity, 0);
  const subtotalCents = items.reduce((sum, i) => sum + i.line.quantity * i.product.priceCents, 0);
  const method = cart?.shippingMethod ?? "standard";

  return {
    cartId,
    cart,
    items,
    count,
    subtotalCents,
    shippingCents: shippingCents(method, subtotalCents),
    isLoading: lines === undefined || products === undefined,
    quantityOf: (productId: string) =>
      items.find((item) => item.product.id === productId)?.line.quantity ?? 0,
    setQuantity: (productId: string, quantity: number) =>
      setLineQuantity(db, account, productId, quantity),
    /** Shipping details live on the cart, so they also sync between devices. */
    updateCheckout: (fields: Partial<Omit<Cart, "id" | "shopper">>) => {
      ensureCart(db, account);
      db.update(app.carts, cartId, fields);
    },
  };
}

export function clampQuantity(quantity: number): number {
  return Math.max(0, Math.min(MAX_LINE_QUANTITY, Math.floor(quantity)));
}

function ensureCart(db: Db, account: string) {
  // Upsert with only the owner: creates the cart on first use and is a no-op
  // afterwards, whichever device gets there first.
  db.upsert(app.carts, ids.cart(account), { shopper: account });
}

export function setLineQuantity(db: Db, account: string, productId: string, quantity: number) {
  const cartId = ids.cart(account);
  ensureCart(db, account);
  db.upsert(app.cartLines, ids.cartLine(cartId, productId), {
    cartId,
    productId,
    quantity: clampQuantity(quantity),
  });
  // A reviewed checkout no longer matches the cart: ask for a fresh review.
  db.update(app.carts, cartId, { checkoutKey: null });
}

export type GuestCartLine = { productId: string; quantity: number };

export async function readGuestCart(db: Db, account: string): Promise<GuestCartLine[]> {
  const lines = await db.all(app.cartLines.where({ cartId: ids.cart(account) }));
  return lines
    .filter((line) => line.quantity > 0)
    .map(({ productId, quantity }) => ({ productId, quantity }));
}

/**
 * Claim a guest cart into a signed-in account's cart. A product in both keeps
 * the larger quantity rather than the sum: the same shopper adding the same
 * item before and after signing in usually meant one purchase, not two.
 */
export async function claimGuestCart(db: Db, account: string, guest: GuestCartLine[]) {
  const cartId = ids.cart(account);
  // Read what the account already holds at the server, not just this device.
  const existing = await db.all(app.cartLines.where({ cartId }), { tier: "global" });
  for (const line of guest) {
    const current = existing.find((e) => e.productId === line.productId)?.quantity ?? 0;
    if (line.quantity > current) setLineQuantity(db, account, line.productId, line.quantity);
  }
}
