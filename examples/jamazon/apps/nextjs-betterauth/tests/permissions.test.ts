import type { Db } from "jazz-tools";
import type { PolicyTestApp } from "jazz-tools/testing";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { MAX_LINE_QUANTITY } from "../permissions";
import { app } from "../schema";
import { ids } from "../src/lib/ids";
import { seedCatalogue } from "../src/server/seed";
import { shopper, startStore } from "./helpers";

let testApp: PolicyTestApp;
let backend: Db;

beforeEach(async () => {
  ({ testApp, backend } = await startStore());
  await seedCatalogue(backend);
});
afterEach(async () => testApp.shutdown());

const strings = ids.product("JAM-001");

describe("catalogue", () => {
  it("is readable by any account, and writable by none", async () => {
    // A local-first guest cannot be modelled with PolicyTestApp yet (see the
    // PR's "Core bugs found"); a second, unrelated account stands in for "anyone".
    const guest = shopper(testApp, "someone-else").db;
    const alice = shopper(testApp, "alice");
    for (const db of [guest, alice.db]) {
      const products = await db.all(app.products.where({ id: strings }), { tier: "global" });
      expect(products.map((p) => p.sku)).toEqual(["JAM-001"]);
      expect(
        await db.all(app.stock.where({ productId: strings }), { tier: "global" }),
      ).toHaveLength(1);
    }
    const [stock] = await alice.db.all(app.stock.where({ productId: strings }), { tier: "global" });
    await alice.db.expectDenied((db) => db.update(app.stock, stock!.id, { onHand: 9999 }));
    await alice.db.expectDenied((db) => db.update(app.products, strings, { priceCents: 1 }));
    await guest.expectDenied((db) =>
      db.insert(app.categories, { slug: "free", name: "Free stuff", blurb: "", position: 0 }),
    );
  });
});

describe("carts", () => {
  it("belong to one account: others can neither read nor add to them", async () => {
    const alice = shopper(testApp, "alice");
    const mallory = shopper(testApp, "mallory");
    const cartId = ids.cart(alice.account);
    await alice.db.upsert(app.carts, cartId, { shopper: alice.account }).wait({ tier: "global" });
    await alice.db
      .upsert(app.cartLines, ids.cartLine(cartId, strings), {
        cartId,
        productId: strings,
        quantity: 2,
      })
      .wait({ tier: "global" });

    expect(await mallory.db.all(app.carts.where({ id: cartId }), { tier: "global" })).toEqual([]);
    expect(await mallory.db.all(app.cartLines.where({ cartId }), { tier: "global" })).toEqual([]);
    // Mallory cannot slip a line into Alice's cart, or take over the cart.
    await mallory.db.expectDenied((db) =>
      db.insert(app.cartLines, { cartId, productId: ids.product("JAM-002"), quantity: 1 }),
    );
    await mallory.db.expectDenied((db) =>
      db.insert(app.carts, { shopper: alice.account, shippingMethod: "express" }),
    );
  });

  it("keep lines in the owner's cart and quantities in range", async () => {
    const alice = shopper(testApp, "alice");
    const bob = shopper(testApp, "bob");
    const aliceCart = ids.cart(alice.account);
    const bobCart = ids.cart(bob.account);
    await alice.db
      .upsert(app.carts, aliceCart, { shopper: alice.account })
      .wait({ tier: "global" });
    await bob.db.upsert(app.carts, bobCart, { shopper: bob.account }).wait({ tier: "global" });
    const line = await alice.db
      .insert(app.cartLines, { cartId: aliceCart, productId: strings, quantity: 1 })
      .wait({ tier: "global" });

    await alice.db.expectDenied((db) => db.update(app.cartLines, line.id, { cartId: bobCart }));
    await alice.db.expectDenied((db) =>
      db.update(app.cartLines, line.id, { quantity: MAX_LINE_QUANTITY + 1 }),
    );
    await alice.db.expectDenied((db) => db.update(app.cartLines, line.id, { quantity: -1 }));
    await alice.db.update(app.cartLines, line.id, { quantity: 0 }).wait({ tier: "global" });
  });
});

describe("orders", () => {
  it("are visible only to their shopper and are never written by clients", async () => {
    const alice = shopper(testApp, "alice");
    const mallory = shopper(testApp, "mallory");
    const orderId = ids.order(alice.account, crypto.randomUUID());
    await testApp.seed((db) =>
      db.insert(
        app.orders,
        {
          shopper: alice.account,
          code: "J-TEST",
          status: "placed",
          idempotencyKey: "k",
          subtotalCents: 2500,
          shippingCents: 500,
          totalCents: 3000,
          shippingMethod: "standard",
          shipName: "Alice",
          shipLine1: "1 Main St",
          shipCity: "Springfield",
          shipPostcode: "12345",
          shipCountry: "US",
          placedAt: new Date(0),
        },
        { id: orderId },
      ),
    );
    await testApp.seed((db) =>
      db.insert(
        app.payments,
        {
          orderId,
          provider: "sandbox",
          providerRef: "sandbox_k",
          status: "requires_payment",
          amountCents: 3000,
        },
        { id: ids.payment(orderId) },
      ),
    );

    expect(await alice.db.all(app.orders.where({ id: orderId }), { tier: "global" })).toHaveLength(
      1,
    );
    expect(await alice.db.all(app.payments.where({ orderId }), { tier: "global" })).toHaveLength(1);
    expect(await mallory.db.all(app.orders.where({ id: orderId }), { tier: "global" })).toEqual([]);
    expect(await mallory.db.all(app.payments.where({ orderId }), { tier: "global" })).toEqual([]);

    // Only the backend marks an order paid or shipped.
    await alice.db.expectDenied((db) => db.update(app.orders, orderId, { status: "paid" }));
    await alice.db.expectDenied((db) =>
      db.update(app.payments, ids.payment(orderId), { status: "succeeded" }),
    );
    await alice.db.expectDenied((db) =>
      db.insert(app.orderEvents, { orderId, status: "shipped", note: "", at: new Date() }),
    );
    await alice.db.expectDenied((db) =>
      db.insert(app.orders, {
        shopper: alice.account,
        code: "J-FREE",
        status: "paid",
        idempotencyKey: "free",
        subtotalCents: 0,
        shippingCents: 0,
        totalCents: 0,
        shippingMethod: "standard",
        shipName: "Alice",
        shipLine1: "1 Main St",
        shipCity: "Springfield",
        shipPostcode: "12345",
        shipCountry: "US",
        placedAt: new Date(),
      }),
    );
  });
});
