import type { Db } from "jazz-tools";
import type { PolicyTestApp, TestDb } from "jazz-tools/testing";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { app } from "../schema";
import { PRODUCTS } from "../src/catalogue/catalogue";
import { ids, uuidV5 } from "../src/lib/ids";
import {
  placeOrder,
  recordPayment,
  settlePayment,
  shipOrder,
  startPayment,
} from "../src/server/orders";
import type { PaymentProvider } from "../src/server/payments";
import { sandboxProvider } from "../src/server/payments/sandbox";
import { seedCatalogue } from "../src/server/seed";
import { shopper, startStore } from "./helpers";

let testApp: PolicyTestApp;
let backend: Db;

beforeEach(async () => {
  ({ testApp, backend } = await startStore());
  await seedCatalogue(backend);
});
afterEach(async () => testApp.shutdown());

const STRINGS = PRODUCTS.find((p) => p.sku === "JAM-001")!;
const CABLE = PRODUCTS.find((p) => p.sku === "JAM-002")!;

/** Fill a shopper's cart and review it, as the checkout UI does. */
async function reviewedCart(db: TestDb, account: string): Promise<string> {
  const cartId = ids.cart(account);
  const key = crypto.randomUUID();
  await db.upsert(app.carts, cartId, { shopper: account }).wait({ tier: "global" });
  for (const [product, quantity] of [
    [STRINGS, 2],
    [CABLE, 1],
  ] as const) {
    const productId = ids.product(product.sku);
    await db
      .upsert(app.cartLines, ids.cartLine(cartId, productId), { cartId, productId, quantity })
      .wait({ tier: "global" });
  }
  await db
    .update(app.carts, cartId, {
      shippingMethod: "standard",
      shipName: "Ada Lovelace",
      shipLine1: "12 Analytical Row",
      shipCity: "London",
      shipPostcode: "N1 9GU",
      shipCountry: "United Kingdom",
      checkoutKey: key,
    })
    .wait({ tier: "global" });
  return key;
}

async function onHand(sku: string) {
  const [row] = await backend.all(app.stock.where({ productId: ids.product(sku) }), {
    tier: "global",
  });
  return row!.onHand;
}

/** Counts provider calls so the test can see what reached "the processor". */
function countingProvider(): PaymentProvider & { creates: string[] } {
  const creates: string[] = [];
  return {
    ...sandboxProvider,
    creates,
    async createPayment(input) {
      creates.push(input.idempotencyKey);
      return sandboxProvider.createPayment(input);
    },
  };
}

describe("checkout", () => {
  it("turns retries of one checkout into exactly one order, one charge and one timeline", async () => {
    const ada = shopper(testApp, "ada");
    const key = await reviewedCart(ada.db, ada.account);

    // A double click, a retry after a timeout and a third request, racing.
    const results = await Promise.all([
      placeOrder(backend, { account: ada.account, idempotencyKey: key }),
      placeOrder(backend, { account: ada.account, idempotencyKey: key }),
      placeOrder(backend, { account: ada.account, idempotencyKey: key }),
    ]);
    const orderId = results[0]!.orderId;
    expect(new Set(results.map((r) => r.orderId))).toEqual(new Set([orderId]));
    expect(results.filter((r) => r.created)).toHaveLength(1);
    // And a late retry after the cart was emptied still answers with the order.
    expect(await placeOrder(backend, { account: ada.account, idempotencyKey: key })).toEqual({
      orderId,
      created: false,
    });

    const orders = await ada.db.all(app.orders, { tier: "global" });
    expect(orders).toHaveLength(1);
    expect(orders[0]).toMatchObject({
      status: "placed",
      subtotalCents: 2 * STRINGS.priceCents + CABLE.priceCents,
      shippingCents: 500,
      totalCents: 2 * STRINGS.priceCents + CABLE.priceCents + 500,
    });
    expect(await onHand("JAM-001")).toBe(STRINGS.onHand - 2);
    expect(await onHand("JAM-002")).toBe(CABLE.onHand - 1);
    // The cart is emptied and its key retired.
    const lines = await ada.db.all(app.cartLines.where({ cartId: ids.cart(ada.account) }), {
      tier: "global",
    });
    expect(lines.every((line) => line.quantity === 0)).toBe(true);

    // Payment: created once, with one idempotency key, however often asked.
    const provider = countingProvider();
    await Promise.all([
      startPayment(backend, provider, orderId),
      startPayment(backend, provider, orderId),
    ]);
    await startPayment(backend, provider, orderId);
    expect(new Set(provider.creates)).toEqual(new Set([`jamazon-payment-${orderId}`]));
    expect(await ada.db.all(app.payments, { tier: "global" })).toHaveLength(1);

    // A decline, then an approval, then duplicate and late reports.
    expect(await settlePayment(backend, provider, { orderId, sandboxOutcome: "decline" })).toBe(
      "payment_failed",
    );
    expect(await settlePayment(backend, provider, { orderId, sandboxOutcome: "approve" })).toBe(
      "paid",
    );
    expect(await settlePayment(backend, provider, { orderId, sandboxOutcome: "approve" })).toBe(
      "paid",
    );
    expect(
      await recordPayment(backend, orderId, { status: "failed", reason: "late webhook" }),
    ).toBe("paid");

    // Two fulfilment workers race to ship it; it ships once.
    const shipped = await Promise.all([shipOrder(backend, orderId), shipOrder(backend, orderId)]);
    expect(shipped.filter(Boolean)).toHaveLength(1);

    const events = await ada.db.all(app.orderEvents.where({ orderId }).orderBy("at", "asc"), {
      tier: "global",
    });
    expect(events.map((e) => e.status).sort()).toEqual(
      ["paid", "payment_failed", "placed", "shipped"].sort(),
    );
    const [order] = await ada.db.all(app.orders.where({ id: orderId }), { tier: "global" });
    expect(order!.status).toBe("shipped");
  });

  it("refuses a checkout the shopper did not review, and scopes keys to the shopper", async () => {
    const ada = shopper(testApp, "ada");
    const eve = shopper(testApp, "eve");
    const key = await reviewedCart(ada.db, ada.account);

    await expect(
      placeOrder(backend, { account: ada.account, idempotencyKey: crypto.randomUUID() }),
    ).rejects.toThrow(/changed since you reviewed/);
    // Eve replaying Ada's key reaches Eve's (empty) cart, never Ada's order.
    await expect(placeOrder(backend, { account: eve.account, idempotencyKey: key })).rejects.toThrow(
      /empty/,
    );
    expect(ids.order(eve.account, key)).not.toBe(ids.order(ada.account, key));
  });

  it("does not oversell", async () => {
    const ada = shopper(testApp, "ada");
    const cartId = ids.cart(ada.account);
    const key = await reviewedCart(ada.db, ada.account);
    const picks = PRODUCTS.find((p) => p.sku === "JAM-003")!;
    const productId = ids.product(picks.sku);
    await ada.db
      .upsert(app.cartLines, ids.cartLine(cartId, productId), {
        cartId,
        productId,
        quantity: picks.onHand + 1,
      })
      .wait({ tier: "global" });
    await expect(placeOrder(backend, { account: ada.account, idempotencyKey: key })).rejects.toThrow(
      /left in stock/,
    );
    expect(await onHand("JAM-003")).toBe(picks.onHand);
    expect(await ada.db.all(app.orders, { tier: "global" })).toEqual([]);
  });
});

describe("deterministic ids", () => {
  it("are RFC 4122 version 5 UUIDs", () => {
    // Published test vector: uuid v5 of "www.example.com" in the DNS namespace.
    expect(uuidV5("www.example.com", "6ba7b810-9dad-11d1-80b4-00c04fd430c8")).toBe(
      "2ed6657d-e927-568b-95e1-2665a8aea6a2",
    );
  });
});
