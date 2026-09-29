import { randomUUID } from "node:crypto";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import type { Db } from "jazz-tools";
import { createPolicyTestApp, type PolicyTestApp, type TestDb } from "jazz-tools/testing";
import permissions from "../../permissions.js";
import { app, REORDER_LEVEL_CAP } from "../../schema.js";
import {
  deliverBatch,
  InsufficientStockError,
  ORDER_STATUS,
  purchase,
  recordPayment,
} from "../../src/warehouse.js";

const issuer = "https://jamazon-warehouse.test";

let testApp: PolicyTestApp;
beforeEach(async () => {
  testApp = await createPolicyTestApp(app, permissions, expect);
});
afterEach(async () => testApp.shutdown());

function actor(userId: string): { db: TestDb; account: string } {
  const account = randomUUID();
  const db = testApp.as({
    issuer,
    user_id: userId,
    account_id: account,
    claims: {},
    authMode: "external",
  });
  return { db, account };
}

/** One warehouse with a district, a customer and stock of one item, built through its manager. */
async function buildWarehouse(manager: { db: TestDb; account: string }, name: string) {
  const warehouse = await manager.db
    .insert(app.warehouses, { name, region: name, operator_id: manager.account })
    .wait({ tier: "global" });
  const district = await manager.db
    .insert(app.districts, { warehouse_id: warehouse.id, name: "Central", next_order_number: 1 })
    .wait({ tier: "global" });
  const customer = await manager.db
    .insert(app.customers, {
      warehouse_id: warehouse.id,
      district_id: district.id,
      name: `${name} buyer`,
      balance_cents: 0,
    })
    .wait({ tier: "global" });
  const item = await manager.db
    .insert(app.items, {
      sku: `${name}-amp`,
      name: "Vintage tube amp",
      unit_price_cents: 1_000,
      operator_id: manager.account,
    })
    .wait({ tier: "global" });
  const stock = await manager.db
    .insert(app.stock, {
      warehouse_id: warehouse.id,
      item_id: item.id,
      on_hand: 5,
      reorder_level: 2,
    })
    .wait({ tier: "global" });
  return { warehouse, district, customer, item, stock };
}

async function staff(
  manager: { db: TestDb },
  warehouseId: string,
  operator: { account: string },
  name = "Operator",
) {
  return await manager.db
    .insert(app.warehouse_operators, {
      warehouse_id: warehouseId,
      account_id: operator.account,
      name,
    })
    .wait({ tier: "global" });
}

const asDb = (db: TestDb) => db as unknown as Db;

describe("warehouse ownership covers operational rows (#1899)", () => {
  it("lets staffed operators check out and rejects everyone else", async () => {
    const manager = actor("east-manager");
    const operator = actor("east-operator");
    const outsider = actor("outsider");
    const east = await buildWarehouse(manager, "east");

    await outsider.db.expectDenied((db) =>
      db.insert(app.warehouse_operators, {
        warehouse_id: east.warehouse.id,
        account_id: outsider.account,
        name: "Self-appointed",
      }),
    );
    await staff(manager, east.warehouse.id, operator);

    const receipt = await purchase(asDb(operator.db), {
      warehouseId: east.warehouse.id,
      districtId: east.district.id,
      customerId: east.customer.id,
      lines: [{ itemId: east.item.id, quantity: 2 }],
      idempotencyKey: "operator-checkout",
    });
    expect(receipt).toMatchObject({ orderNumber: 1, totalCents: 2_000 });
    expect(receipt.lines).toEqual([
      { lineNumber: 1, itemId: east.item.id, quantity: 2, amountCents: 2_000 },
    ]);

    await outsider.db.expectDenied((db) => db.update(app.stock, east.stock.id, { on_hand: 99 }));
    await outsider.db.expectDenied((db) =>
      db.update(app.orders, receipt.orderId, { status: "delivered" }),
    );
    await expect(
      purchase(asDb(outsider.db), {
        warehouseId: east.warehouse.id,
        districtId: east.district.id,
        customerId: east.customer.id,
        lines: [{ itemId: east.item.id, quantity: 1 }],
        idempotencyKey: "outsider-checkout",
      }),
    ).rejects.toThrow(/AuthorizationDenied|Write rejected|permission/i);
  });

  it("revokes a former manager on transfer but keeps staffed operators", async () => {
    const manager = actor("first-manager");
    const nextManager = actor("next-manager");
    const operator = actor("staffed-operator");
    const east = await buildWarehouse(manager, "east");
    await staff(manager, east.warehouse.id, operator);
    // A handover goes to someone already staffed on the warehouse.
    await staff(manager, east.warehouse.id, nextManager, "Next manager");

    await manager.db.update(app.stock, east.stock.id, { on_hand: 6 }).wait({ tier: "global" });
    await manager.db
      .update(app.warehouses, east.warehouse.id, { operator_id: nextManager.account })
      .wait({ tier: "global" });

    await manager.db.expectDenied((db) => db.update(app.stock, east.stock.id, { on_hand: 7 }));
    await manager.db.expectDenied((db) =>
      db.update(app.customers, east.customer.id, { balance_cents: 1 }),
    );
    await nextManager.db.update(app.stock, east.stock.id, { on_hand: 8 }).wait({ tier: "global" });
    await operator.db.update(app.stock, east.stock.id, { on_hand: 9 }).wait({ tier: "global" });
  });

  it("refuses to hand a warehouse to a stranger", async () => {
    const manager = actor("handing-manager");
    const stranger = actor("stranger");
    const east = await buildWarehouse(manager, "east");

    await manager.db.expectDenied((db) =>
      db.update(app.warehouses, east.warehouse.id, { operator_id: stranger.account }),
    );
    // The manager keeps full authority, including over the warehouse row.
    await manager.db
      .update(app.warehouses, east.warehouse.id, { region: "still-east" })
      .wait({ tier: "global" });
    await stranger.db.expectDenied((db) => db.update(app.stock, east.stock.id, { on_hand: 0 }));
  });

  it("revokes an operator who leaves the warehouse", async () => {
    const manager = actor("manager");
    const operator = actor("leaving-operator");
    const east = await buildWarehouse(manager, "east");
    const membership = await staff(manager, east.warehouse.id, operator);
    await operator.db.update(app.stock, east.stock.id, { on_hand: 4 }).wait({ tier: "global" });
    await operator.db.delete(app.warehouse_operators, membership.id).wait({ tier: "global" });
    await operator.db.expectDenied((db) => db.update(app.stock, east.stock.id, { on_hand: 3 }));
  });

  it("keeps an operator of one warehouse out of another", async () => {
    const eastManager = actor("east-manager");
    const westManager = actor("west-manager");
    const eastOperator = actor("east-operator");
    const east = await buildWarehouse(eastManager, "east");
    const west = await buildWarehouse(westManager, "west");
    await staff(eastManager, east.warehouse.id, eastOperator);

    await eastOperator.db.expectDenied((db) => db.update(app.stock, west.stock.id, { on_hand: 0 }));
    await expect(deliverBatch(asDb(eastOperator.db), west.warehouse.id)).resolves.toEqual([]);
  });
});

describe("checkout rows belong to one warehouse (#1898)", () => {
  it("rejects operational rows that reference another warehouse", async () => {
    // One manager runs both warehouses, so only the integrity rules can refuse.
    const manager = actor("two-warehouse-manager");
    const east = await buildWarehouse(manager, "east");
    const west = await buildWarehouse(manager, "west");

    await manager.db.expectDenied((db) =>
      db.insert(app.customers, {
        warehouse_id: east.warehouse.id,
        district_id: west.district.id,
        name: "Misfiled",
        balance_cents: 0,
      }),
    );
    await manager.db.expectDenied((db) =>
      db.insert(app.orders, {
        warehouse_id: east.warehouse.id,
        district_id: west.district.id,
        customer_id: west.customer.id,
        order_number: 1,
        status: "pending",
        total_cents: 0,
        idempotency_key: "cross-district",
      }),
    );
    await manager.db.expectDenied((db) =>
      db.insert(app.orders, {
        warehouse_id: east.warehouse.id,
        district_id: east.district.id,
        customer_id: west.customer.id,
        order_number: 1,
        status: "pending",
        total_cents: 0,
        idempotency_key: "cross-customer",
      }),
    );

    const eastOrder = await manager.db
      .insert(app.orders, {
        warehouse_id: east.warehouse.id,
        district_id: east.district.id,
        customer_id: east.customer.id,
        order_number: 1,
        status: "pending",
        total_cents: 0,
        idempotency_key: "coherent",
      })
      .wait({ tier: "global" });
    await manager.db.expectDenied((db) =>
      db.insert(app.order_lines, {
        warehouse_id: west.warehouse.id,
        order_id: eastOrder.id,
        line_number: 1,
        item_id: west.item.id,
        quantity: 1,
        amount_cents: 0,
      }),
    );
    await manager.db.expectDenied((db) =>
      db.insert(app.order_lines, {
        warehouse_id: east.warehouse.id,
        order_id: eastOrder.id,
        line_number: 1,
        item_id: west.item.id,
        quantity: 1,
        amount_cents: 0,
      }),
    );
    await manager.db.expectDenied((db) =>
      db.insert(app.payments, {
        warehouse_id: east.warehouse.id,
        customer_id: west.customer.id,
        amount_cents: 100,
        idempotency_key: "cross-payment",
      }),
    );
    await manager.db.expectDenied((db) =>
      db.insert(app.deliveries, {
        warehouse_id: west.warehouse.id,
        district_id: west.district.id,
        order_id: eastOrder.id,
        status: "delivered",
      }),
    );

    await expect(
      purchase(asDb(manager.db), {
        warehouseId: east.warehouse.id,
        districtId: west.district.id,
        customerId: west.customer.id,
        lines: [{ itemId: east.item.id, quantity: 1 }],
        idempotencyKey: "mixed-checkout",
      }),
    ).rejects.toThrow("checkout rows belong to different warehouses or districts");
    await expect(
      recordPayment(asDb(manager.db), {
        warehouseId: east.warehouse.id,
        districtId: east.district.id,
        customerId: west.customer.id,
        amountCents: 100,
        idempotencyKey: "mixed-payment",
      }),
    ).rejects.toThrow("customer belongs to another warehouse or district");
  });

  it("keeps stock non-negative and reorder levels under the report cap", async () => {
    const manager = actor("stock-manager");
    const east = await buildWarehouse(manager, "east");
    await manager.db.expectDenied((db) => db.update(app.stock, east.stock.id, { on_hand: -1 }));
    await manager.db.expectDenied((db) =>
      db.update(app.stock, east.stock.id, { reorder_level: REORDER_LEVEL_CAP + 1 }),
    );
  });
});

describe("stock contention", () => {
  it("accepts one of two concurrent orders for the last units and rejects the other", async () => {
    const manager = actor("contention-manager");
    const first = actor("first-operator");
    const second = actor("second-operator");
    const east = await buildWarehouse(manager, "east");
    await staff(manager, east.warehouse.id, first, "First");
    await staff(manager, east.warehouse.id, second, "Second");

    const order = (operator: { db: TestDb }, key: string) =>
      purchase(asDb(operator.db), {
        warehouseId: east.warehouse.id,
        districtId: east.district.id,
        customerId: east.customer.id,
        lines: [{ itemId: east.item.id, quantity: 4 }],
        idempotencyKey: key,
      });
    const outcomes = await Promise.allSettled([order(first, "first"), order(second, "second")]);

    const accepted = outcomes.filter((outcome) => outcome.status === "fulfilled");
    const rejected = outcomes.filter((outcome) => outcome.status === "rejected");
    expect(accepted).toHaveLength(1);
    expect(rejected).toHaveLength(1);
    const reason = (rejected[0] as PromiseRejectedResult).reason;
    expect(reason).toBeInstanceOf(InsufficientStockError);
    expect(reason).toMatchObject({ onHand: 1, requested: 4 });

    const [stock] = await manager.db.all(app.stock.where({ id: east.stock.id }).limit(1), {
      tier: "global",
    });
    expect(stock?.on_hand).toBe(1);
    const orders = await manager.db.all(app.orders.where({ warehouse_id: east.warehouse.id }), {
      tier: "global",
    });
    expect(orders).toHaveLength(1);
  });

  it("returns the first receipt when a checkout is retried with the same key", async () => {
    const manager = actor("retry-manager");
    const east = await buildWarehouse(manager, "east");
    const request = {
      warehouseId: east.warehouse.id,
      districtId: east.district.id,
      customerId: east.customer.id,
      lines: [{ itemId: east.item.id, quantity: 2 }],
      idempotencyKey: "retried",
    };
    const first = await purchase(asDb(manager.db), request);
    const again = await purchase(asDb(manager.db), request);
    expect(again).toEqual(first);
    const [stock] = await manager.db.all(app.stock.where({ id: east.stock.id }).limit(1), {
      tier: "global",
    });
    expect(stock?.on_hand).toBe(3);
  });

  it("reserves a draft, then places its lines and payment against the committed order", async () => {
    const manager = actor("two-phase-manager");
    const operator = actor("two-phase-operator");
    const east = await buildWarehouse(manager, "east");
    await staff(manager, east.warehouse.id, operator);

    const receipt = await purchase(asDb(operator.db), {
      warehouseId: east.warehouse.id,
      districtId: east.district.id,
      customerId: east.customer.id,
      lines: [{ itemId: east.item.id, quantity: 3 }],
      idempotencyKey: "two-phase",
    });

    const read = { tier: "global" } as const;
    const [order] = await manager.db.all(app.orders.where({ id: receipt.orderId }).limit(1), read);
    expect(order).toMatchObject({
      status: ORDER_STATUS.pending,
      total_cents: 3_000,
      order_number: 1,
    });
    expect(
      await manager.db.all(app.order_lines.where({ order_id: receipt.orderId }).limit(5), read),
    ).toMatchObject([{ warehouse_id: east.warehouse.id, item_id: east.item.id, quantity: 3 }]);
    expect(
      await manager.db.all(app.payments.where({ order_id: receipt.orderId }).limit(5), read),
    ).toMatchObject([{ customer_id: east.customer.id, amount_cents: 3_000 }]);
    const [stock] = await manager.db.all(app.stock.where({ id: east.stock.id }).limit(1), read);
    expect(stock?.on_hand).toBe(2);
    const [customer] = await manager.db.all(
      app.customers.where({ id: east.customer.id }).limit(1),
      read,
    );
    expect(customer?.balance_cents).toBe(-3_000);
  });

  it("places a draft left by an interrupted checkout when the request is resubmitted", async () => {
    const manager = actor("recovering-manager");
    const east = await buildWarehouse(manager, "east");
    // Phase one landed but phase two never ran: a reserved draft, with stock
    // and balance already taken, and no lines or payment yet.
    await manager.db.update(app.stock, east.stock.id, { on_hand: 3 }).wait({ tier: "global" });
    await manager.db
      .update(app.customers, east.customer.id, { balance_cents: -2_000 })
      .wait({ tier: "global" });
    await manager.db
      .update(app.districts, east.district.id, { next_order_number: 2 })
      .wait({ tier: "global" });
    const draft = await manager.db
      .insert(app.orders, {
        warehouse_id: east.warehouse.id,
        district_id: east.district.id,
        customer_id: east.customer.id,
        order_number: 1,
        status: ORDER_STATUS.draft,
        total_cents: 2_000,
        idempotency_key: "interrupted",
      })
      .wait({ tier: "global" });

    const receipt = await purchase(asDb(manager.db), {
      warehouseId: east.warehouse.id,
      districtId: east.district.id,
      customerId: east.customer.id,
      lines: [{ itemId: east.item.id, quantity: 2 }],
      idempotencyKey: "interrupted",
    });
    expect(receipt).toMatchObject({ orderId: draft.id, orderNumber: 1, totalCents: 2_000 });

    const read = { tier: "global" } as const;
    const [order] = await manager.db.all(app.orders.where({ id: draft.id }).limit(1), read);
    expect(order?.status).toBe(ORDER_STATUS.pending);
    const [stock] = await manager.db.all(app.stock.where({ id: east.stock.id }).limit(1), read);
    expect(stock?.on_hand).toBe(3);
  });

  it("delivers the oldest pending order per district once", async () => {
    const manager = actor("delivery-manager");
    const east = await buildWarehouse(manager, "east");
    const place = (key: string) =>
      purchase(asDb(manager.db), {
        warehouseId: east.warehouse.id,
        districtId: east.district.id,
        customerId: east.customer.id,
        lines: [{ itemId: east.item.id, quantity: 1 }],
        idempotencyKey: key,
      });
    const firstOrder = await place("a");
    const secondOrder = await place("b");
    expect(await deliverBatch(asDb(manager.db), east.warehouse.id)).toEqual([
      { districtId: east.district.id, orderId: firstOrder.orderId, orderNumber: 1 },
    ]);
    expect(await deliverBatch(asDb(manager.db), east.warehouse.id)).toEqual([
      { districtId: east.district.id, orderId: secondOrder.orderId, orderNumber: 2 },
    ]);
    expect(await deliverBatch(asDb(manager.db), east.warehouse.id)).toEqual([]);
  });
});
