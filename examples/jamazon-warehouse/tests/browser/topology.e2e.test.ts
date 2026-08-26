import { afterEach, describe, expect, it } from "vitest";
import { createDb, type Db } from "../../../../packages/jazz-tools/src/runtime/db.js";
import { deploy } from "../../../../packages/jazz-tools/src/dev/catalogue.js";
import {
  TestCleanup,
  uniqueDbName,
  waitForQuery,
  withTimeout,
} from "../../../../packages/jazz-tools/tests/browser/support.js";
import {
  browserTopologyReporter,
  runTopologyScenario,
  TopologyEnvelopeScheduler,
} from "../../../../packages/jazz-tools/tests/browser/topology-harness.js";
import {
  getJazzServerInfo,
  getJazzServerJwtForUser,
} from "../../../../packages/jazz-tools/tests/browser/testing-server.js";
import permissions from "../../permissions.js";
import { app } from "../../schema.js";
import { purchase, warehouseQueries, type PurchaseReceipt } from "../../src/warehouse.js";
import type { Db as PublicDb } from "jazz-tools";

const ctx = new TestCleanup();
afterEach(async () => ctx.cleanup());

describe("Jamazon Warehouse browser, edge, and core workflow", () => {
  it("preserves an atomic checkout through a duplicate retry, reconnect, persistent reopen, and ownership revocation", async () => {
    let server: Awaited<ReturnType<typeof getJazzServerInfo>>;
    let owner: Db;
    let observer: Db;
    let nextOperator: Db;
    let ownerToken: string;
    let ownerDbName: string;
    let warehouse: { id: string };
    let district: { id: string };
    let customer: { id: string };
    let item: { id: string };
    let stock: { id: string };
    const seed = topologySeed();
    const scheduler = new TopologyEnvelopeScheduler(seed);

    const receipt = await runTopologyScenario(
      {
        id: "jamazon-warehouse.checkout-retry-reopen",
        topology: ["browser", "edge", "core"],
        seed,
        phaseTimeoutMs: 25_000,
        faultTimeoutMs: 15_000,
        replay: `JAZZ_EXAMPLE_TOPOLOGY_SEED=${seed} pnpm --dir examples/jamazon-warehouse test:browser -- topology.e2e.test.ts`,
        envelopeSchedulers: [scheduler],
        targets: {
          owner: {
            disconnect: async () => owner.disconnect(),
            reconnect: async () => owner.reconnect(),
            restart: async () => {
              await owner.shutdown();
              ctx.untrack(owner);
              owner = await openClient(server, "owner-reopened", ownerToken, ownerDbName);
            },
          },
          authorization: {
            failure: async () => {
              const outsider = await openClient(
                server,
                "outsider",
                await getJazzServerJwtForUser("jamazon-outsider", undefined, server.appId),
              );
              await expect(
                outsider
                  .insert(app.warehouses, {
                    name: "Forged warehouse",
                    region: "outside",
                    operator_id: "jamazon-owner",
                  })
                  .wait({ tier: "edge" }),
              ).rejects.toThrow(/AuthorizationDenied|Write rejected/);
            },
          },
        },
        phases: [
          {
            name: "bootstrap the warehouse and its operational rows",
            run: async () => {
              server = await getJazzServerInfo(uniqueDbName("jamazon-warehouse-topology"));
              await deploy({
                appId: server.appId,
                serverUrl: server.serverUrl,
                adminSecret: server.adminSecret,
                schema: app,
                permissions,
              });
              ownerToken = await getJazzServerJwtForUser("jamazon-owner", undefined, server.appId);
              ownerDbName = uniqueDbName("jamazon-owner-persistent");
              owner = await openClient(server, "owner", ownerToken, ownerDbName);
              observer = await openClient(
                server,
                "observer",
                await getJazzServerJwtForUser("jamazon-observer", undefined, server.appId),
              );
              nextOperator = await openClient(
                server,
                "next-operator",
                await getJazzServerJwtForUser("jamazon-next-operator", undefined, server.appId),
              );
              warehouse = await owner
                .insert(app.warehouses, {
                  name: "East instruments",
                  region: "east",
                  operator_id: "jamazon-owner",
                })
                .wait({ tier: "edge" });
              district = await owner
                .insert(app.districts, {
                  warehouse_id: warehouse.id,
                  name: "A",
                  next_order_number: 17,
                })
                .wait({ tier: "edge" });
              customer = await owner
                .insert(app.customers, {
                  warehouse_id: warehouse.id,
                  district_id: district.id,
                  name: "Demo buyer",
                  balance_cents: 0,
                })
                .wait({ tier: "edge" });
              item = await owner
                .insert(app.items, {
                  sku: "JAM-001",
                  name: "Jazzmaster strings",
                  unit_price_cents: 2_500,
                })
                .wait({ tier: "edge" });
              stock = await owner
                .insert(app.stock, {
                  warehouse_id: warehouse.id,
                  item_id: item.id,
                  on_hand: 10,
                  reorder_level: 12,
                })
                .wait({ tier: "edge" });
            },
            faultsAfter: [{ kind: "failure", target: "authorization" }],
          },
          {
            name: "exercise the bounded operational reads used by the console",
            run: async () => {
              const queries = warehouseQueries({
                warehouseId: warehouse.id,
                districtId: district.id,
              });
              await Promise.all([
                waitForQuery(
                  observer,
                  queries.districts,
                  (rows) => rows.length === 1 && rows[0]?.id === district.id,
                  "observer warehouse districts",
                  15_000,
                  "edge",
                ),
                waitForQuery(
                  observer,
                  queries.customers,
                  (rows) => rows.length === 1 && rows[0]?.id === customer.id,
                  "observer district customers",
                  15_000,
                  "edge",
                ),
              ]);
              expect(await observer.all(queries.pendingOrders, { tier: "edge" })).toEqual([]);
            },
          },
          {
            name: "duplicate a checkout request and recover a dropped edge-to-core handoff",
            run: async () => {
              const duplicateCheckouts: Array<ReturnType<typeof purchase>> = [];
              scheduler.duplicateNext();
              await scheduler.intercept(
                { from: "browser", to: "edge", label: "checkout-request" },
                undefined,
                () => {
                  // Do not await here: the scheduler deterministically starts
                  // both duplicated deliveries before either preflight or
                  // exclusive transaction can settle. This is the real
                  // same-key overlap that a double submit or retry can cause.
                  duplicateCheckouts.push(
                    purchase(publicDb(owner), {
                      warehouseId: warehouse.id,
                      districtId: district.id,
                      customerId: customer.id,
                      itemId: item.id,
                      quantity: 3,
                      idempotencyKey: "checkout-17",
                    }),
                  );
                },
              );
              expect(duplicateCheckouts).toHaveLength(2);
              const receipts = await Promise.all(
                (await Promise.all(duplicateCheckouts)).map((checkout) =>
                  withTimeout(checkout.wait(), 15_000, "exclusive checkout did not settle"),
                ),
              );
              expect(receipts).toHaveLength(2);
              expect(receipts[0]).toEqual(receipts[1]);
              expect(receipts[0]).toMatchObject({ orderNumber: 17, totalCents: 7_500 });

              const queries = warehouseQueries({
                warehouseId: warehouse.id,
                districtId: district.id,
              });
              await waitForQuery(
                observer,
                queries.pendingOrders,
                (rows) => rows.length === 1 && rows[0]?.id === receipts[0]?.orderId,
                "observer sees exactly one concurrently duplicated checkout",
                20_000,
                "edge",
              );
              const afterConcurrentDuplicate = await checkoutSnapshot(observer, {
                warehouseId: warehouse.id,
                districtId: district.id,
                customerId: customer.id,
                stockId: stock.id,
              });
              expect(afterConcurrentDuplicate).toEqual({
                orders: [
                  {
                    id: receipts[0]!.orderId,
                    orderNumber: 17,
                    status: "pending",
                    totalCents: 7_500,
                    idempotencyKey: "checkout-17",
                  },
                ],
                orderLines: [
                  {
                    orderId: receipts[0]!.orderId,
                    itemId: item.id,
                    quantity: 3,
                    amountCents: 7_500,
                  },
                ],
                payments: [
                  {
                    orderId: receipts[0]!.orderId,
                    customerId: customer.id,
                    amountCents: 7_500,
                    idempotencyKey: "checkout-17",
                  },
                ],
                stockOnHand: 7,
                nextOrderNumber: 18,
                customerBalanceCents: -7_500,
              });

              // This is an app-owned checkout relay boundary, not an attempt to
              // instrument Jazz's transport.  The first handoff is dropped and
              // the retry reaches the same exclusive checkout with the same
              // request key.  It models the important adopter-visible edge ↔
              // core failure mode without coupling this example to runtime
              // protocol details.
              let recoveredReceipt: PurchaseReceipt | undefined;
              scheduler.dropNextThenRetry();
              await scheduler.intercept(
                { from: "edge", to: "core", label: "checkout-authority-handoff" },
                undefined,
                async () => {
                  const checkout = await purchase(publicDb(owner), {
                    warehouseId: warehouse.id,
                    districtId: district.id,
                    customerId: customer.id,
                    itemId: item.id,
                    quantity: 1,
                    idempotencyKey: "checkout-edge-core-loss",
                  });
                  recoveredReceipt = await withTimeout(
                    checkout.wait(),
                    15_000,
                    "retried edge-to-core checkout did not settle",
                  );
                },
              );
              expect(recoveredReceipt).toBeUndefined();
              // The scheduler has dropped the app-owned handoff, so the
              // observer must still see the sole accepted duplicate checkout.
              // This catches accidentally starting the authority write before
              // the relay has delivered its retry.
              expect(
                await checkoutSnapshot(observer, {
                  warehouseId: warehouse.id,
                  districtId: district.id,
                  customerId: customer.id,
                  stockId: stock.id,
                }),
              ).toEqual(afterConcurrentDuplicate);
              await scheduler.advance();
              expect(recoveredReceipt).toMatchObject({ orderNumber: 18, totalCents: 2_500 });

              const orders = await waitForQuery(
                observer,
                queries.pendingOrders,
                (rows) =>
                  rows.length === 2 &&
                  rows.map((row) => row.id).join(",") ===
                    [receipts[0]?.orderId, recoveredReceipt?.orderId].join(","),
                "observer sees both completed checkout retries in order",
                20_000,
                "edge",
              );
              expect(orders.map((order) => [order.order_number, order.total_cents])).toEqual([
                [17, 7_500],
                [18, 2_500],
              ]);
              expect(
                await checkoutSnapshot(observer, {
                  warehouseId: warehouse.id,
                  districtId: district.id,
                  customerId: customer.id,
                  stockId: stock.id,
                }),
              ).toEqual({
                orders: [
                  {
                    id: receipts[0]!.orderId,
                    orderNumber: 17,
                    status: "pending",
                    totalCents: 7_500,
                    idempotencyKey: "checkout-17",
                  },
                  {
                    id: recoveredReceipt!.orderId,
                    orderNumber: 18,
                    status: "pending",
                    totalCents: 2_500,
                    idempotencyKey: "checkout-edge-core-loss",
                  },
                ],
                orderLines: [
                  {
                    orderId: receipts[0]!.orderId,
                    itemId: item.id,
                    quantity: 3,
                    amountCents: 7_500,
                  },
                  {
                    orderId: recoveredReceipt!.orderId,
                    itemId: item.id,
                    quantity: 1,
                    amountCents: 2_500,
                  },
                ],
                payments: [
                  {
                    orderId: receipts[0]!.orderId,
                    customerId: customer.id,
                    amountCents: 7_500,
                    idempotencyKey: "checkout-17",
                  },
                  {
                    orderId: recoveredReceipt!.orderId,
                    customerId: customer.id,
                    amountCents: 2_500,
                    idempotencyKey: "checkout-edge-core-loss",
                  },
                ],
                stockOnHand: 6,
                nextOrderNumber: 19,
                customerBalanceCents: -10_000,
              });

              // Fill one page plus one row through the public operational
              // model. This makes the console query's order and `limit(20)`
              // observable from a separate browser rather than merely
              // inspecting its query-builder construction.
              await Promise.all(
                Array.from({ length: 19 }, (_, offset) =>
                  owner
                    .insert(app.orders, {
                      warehouse_id: warehouse.id,
                      district_id: district.id,
                      customer_id: customer.id,
                      order_number: 100 + offset,
                      status: "pending",
                      total_cents: 0,
                      idempotency_key: `bounded-operational-order-${offset}`,
                    })
                    .wait({ tier: "edge" }),
                ),
              );
              const boundedOrders = await waitForQuery(
                observer,
                queries.pendingOrders,
                (rows) =>
                  rows.length === 20 &&
                  rows.map((row) => row.order_number).join(",") ===
                    [17, 18, ...Array.from({ length: 18 }, (_, offset) => 100 + offset)].join(","),
                "observer sees the first bounded pending-order page in order",
                20_000,
                "edge",
              );
              expect(boundedOrders.at(-1)?.order_number).toBe(117);
              expect(boundedOrders.some((order) => order.order_number === 118)).toBe(false);
              await waitForQuery(
                observer,
                queries.allOrders,
                (rows) => rows.length === 21 && rows.at(-1)?.order_number === 118,
                "observer converges the complete concurrently seeded operational set",
                20_000,
                "edge",
              );
              expect(
                await observer.all(app.order_lines.where({ order_id: receipts[0]!.orderId }), {
                  tier: "edge",
                }),
              ).toMatchObject([{ item_id: item.id, quantity: 3, amount_cents: 7_500 }]);
              expect(
                await observer.all(app.payments.where({ order_id: receipts[0]!.orderId }), {
                  tier: "edge",
                }),
              ).toMatchObject([
                { customer_id: customer.id, amount_cents: 7_500, idempotency_key: "checkout-17" },
              ]);
              expect(
                await observer.all(app.stock.where({ id: stock.id }).limit(1), { tier: "edge" }),
              ).toMatchObject([{ on_hand: 6 }]);
              expect(
                await observer.all(app.districts.where({ id: district.id }).limit(1), {
                  tier: "edge",
                }),
              ).toMatchObject([{ next_order_number: 19 }]);
              expect(
                await observer.all(app.customers.where({ id: customer.id }).limit(1), {
                  tier: "edge",
                }),
              ).toMatchObject([{ balance_cents: -10_000 }]);

              const beforeRejectedCheckout = await checkoutSnapshot(observer, {
                warehouseId: warehouse.id,
                districtId: district.id,
                customerId: customer.id,
                stockId: stock.id,
              });
              await expect(
                purchase(publicDb(owner), {
                  warehouseId: warehouse.id,
                  districtId: district.id,
                  customerId: customer.id,
                  itemId: item.id,
                  quantity: 8,
                  idempotencyKey: "insufficient-stock",
                }),
              ).rejects.toThrow("insufficient stock");
              expect(
                await checkoutSnapshot(observer, {
                  warehouseId: warehouse.id,
                  districtId: district.id,
                  customerId: customer.id,
                  stockId: stock.id,
                }),
              ).toEqual(beforeRejectedCheckout);
            },
            faultsAfter: [{ kind: "disconnect", target: "owner" }],
          },
          {
            name: "keep a permitted warehouse edit local while disconnected",
            run: async () => {
              const localEdit = owner.update(app.warehouses, warehouse.id, {
                region: "offline-east",
              });
              await withTimeout(
                localEdit.wait({ tier: "local" }),
                5_000,
                "offline edit did not settle locally",
              );
              expect(
                await owner.all(app.warehouses.where({ id: warehouse.id }).limit(1), {
                  tier: "local",
                }),
              ).toMatchObject([{ region: "offline-east" }]);
            },
            faultsAfter: [
              { kind: "reconnect", target: "owner" },
              { kind: "restart", target: "owner" },
            ],
          },
          {
            name: "converge the local edit after persistent reopen",
            run: async () => {
              await waitForQuery(
                owner,
                app.warehouses.where({ id: warehouse.id }).limit(1),
                (rows) => rows[0]?.region === "offline-east",
                "persistent owner reopen retains local edit",
                20_000,
                "edge",
              );
              const observed = await waitForQuery(
                observer,
                app.warehouses.where({ id: warehouse.id }).limit(1),
                (rows) => rows[0]?.region === "offline-east",
                "observer receives reconnected local edit",
                20_000,
                "edge",
              );
              expect(observed[0]?.operator_id).toBe("jamazon-owner");
              const allOrders = await owner.all(
                warehouseQueries({ warehouseId: warehouse.id, districtId: district.id }).allOrders,
                {
                  tier: "edge",
                },
              );
              expect(allOrders).toHaveLength(21);
              expect(allOrders).toMatchObject([
                { order_number: 17, total_cents: 7_500 },
                { order_number: 18, total_cents: 2_500 },
              ]);
            },
          },
          {
            name: "transfer warehouse authority and reject the revoked operator",
            run: async () => {
              await owner
                .update(app.warehouses, warehouse.id, { operator_id: "jamazon-next-operator" })
                .wait({ tier: "edge" });
              await expect(
                owner
                  .update(app.warehouses, warehouse.id, { region: "revoked-owner-write" })
                  .wait({ tier: "edge" }),
              ).rejects.toThrow(/AuthorizationDenied|Write rejected/);
              await nextOperator
                .update(app.warehouses, warehouse.id, { region: "next-operator-write" })
                .wait({ tier: "edge" });
              await waitForQuery(
                observer,
                app.warehouses.where({ id: warehouse.id }).limit(1),
                (rows) => rows[0]?.region === "next-operator-write",
                "new operator update reaches observer",
                20_000,
                "edge",
              );
            },
          },
        ],
        cleanup: async () => ctx.cleanup(),
        cleanupTimeoutMs: 10_000,
      },
      browserTopologyReporter,
    );
    expect(receipt.status).toBe("passed");
    expect(receipt.faults.map((fault) => [fault.kind, fault.status])).toEqual([
      ["failure", "completed"],
      ["disconnect", "completed"],
      ["reconnect", "completed"],
      ["restart", "completed"],
    ]);
    expect(receipt.envelopes[0]?.activities.map(({ action }) => action)).toEqual(
      expect.arrayContaining(["duplicated", "dropped", "retried", "delivered"]),
    );
  }, 90_000);
});

async function openClient(
  server: { appId: string; serverUrl: string },
  label: string,
  jwtToken: string,
  dbName = uniqueDbName(`jamazon-${label}`),
): Promise<Db> {
  return ctx.track(
    await createDb({
      appId: server.appId,
      serverUrl: server.serverUrl,
      jwtToken,
      driver: { type: "persistent", dbName },
    }),
  );
}

function publicDb(db: Db): PublicDb {
  // Browser support utilities import Jazz source for Vite's current WASM
  // bundle, while the application workflow imports the public package. Both
  // paths resolve to the same database instance at runtime.
  return db as unknown as PublicDb;
}

/**
 * Read the complete mutable checkout footprint from a separate browser. The
 * unique deployed app makes unscoped child reads safe here, and deliberately
 * keeps an orphaned line or payment observable after a rejected transaction.
 */
async function checkoutSnapshot(
  db: Db,
  {
    warehouseId,
    districtId,
    customerId,
    stockId,
  }: {
    warehouseId: string;
    districtId: string;
    customerId: string;
    stockId: string;
  },
) {
  const [orders, orderLines, payments, stockRows, districtRows, customerRows] = await Promise.all([
    db.all(app.orders.where({ warehouse_id: warehouseId }), { tier: "edge" }),
    db.all(app.order_lines.where({}), { tier: "edge" }),
    db.all(app.payments.where({}), { tier: "edge" }),
    db.all(app.stock.where({ id: stockId }).limit(1), { tier: "edge" }),
    db.all(app.districts.where({ id: districtId }).limit(1), { tier: "edge" }),
    db.all(app.customers.where({ id: customerId }).limit(1), { tier: "edge" }),
  ]);
  const [stock] = stockRows;
  const [district] = districtRows;
  const [customer] = customerRows;
  if (!stock || !district || !customer) throw new Error("checkout state rows are missing");
  const orderNumberById = new Map(orders.map((order) => [order.id, order.order_number]));
  const orderPosition = (orderId: string | null) =>
    orderNumberById.get(orderId ?? "") ?? Number.MAX_SAFE_INTEGER;

  return {
    orders: orders
      .map((order) => ({
        id: order.id,
        orderNumber: order.order_number,
        status: order.status,
        totalCents: order.total_cents,
        idempotencyKey: order.idempotency_key,
      }))
      .sort((left, right) => left.orderNumber - right.orderNumber),
    orderLines: orderLines
      .map((line) => ({
        orderId: line.order_id,
        itemId: line.item_id,
        quantity: line.quantity,
        amountCents: line.amount_cents,
      }))
      .sort((left, right) => orderPosition(left.orderId) - orderPosition(right.orderId)),
    payments: payments
      .map((payment) => ({
        orderId: payment.order_id,
        customerId: payment.customer_id,
        amountCents: payment.amount_cents,
        idempotencyKey: payment.idempotency_key,
      }))
      .sort((left, right) => {
        const byOrder = orderPosition(left.orderId) - orderPosition(right.orderId);
        return byOrder || left.idempotencyKey.localeCompare(right.idempotencyKey);
      }),
    stockOnHand: stock.on_hand,
    nextOrderNumber: district.next_order_number,
    customerBalanceCents: customer.balance_cents,
  };
}

function topologySeed(): number {
  const value = (import.meta as ImportMeta & { env?: Record<string, string | undefined> }).env
    ?.JAZZ_EXAMPLE_TOPOLOGY_SEED;
  const seed = Number(value ?? 71);
  return Number.isSafeInteger(seed) ? seed : 71;
}
