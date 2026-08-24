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
            name: "duplicate a checkout request and make the retry idempotent",
            run: async () => {
              const receipts: PurchaseReceipt[] = [];
              scheduler.duplicateNext();
              await scheduler.intercept(
                { from: "browser", to: "edge", label: "checkout-request" },
                undefined,
                async () => {
                  const checkout = await purchase(publicDb(owner), {
                    warehouseId: warehouse.id,
                    districtId: district.id,
                    customerId: customer.id,
                    itemId: item.id,
                    quantity: 3,
                    idempotencyKey: "checkout-17",
                  });
                  receipts.push(
                    await withTimeout(checkout.wait(), 15_000, "exclusive checkout did not settle"),
                  );
                },
              );
              expect(receipts).toHaveLength(2);
              expect(receipts[0]).toEqual(receipts[1]);
              expect(receipts[0]).toMatchObject({ orderNumber: 17, totalCents: 7_500 });

              const queries = warehouseQueries({
                warehouseId: warehouse.id,
                districtId: district.id,
              });
              const orders = await waitForQuery(
                observer,
                queries.pendingOrders,
                (rows) => rows.length === 1 && rows[0]?.id === receipts[0]?.orderId,
                "observer sees exactly one retried order",
                20_000,
                "edge",
              );
              expect(orders.map((order) => [order.order_number, order.total_cents])).toEqual([
                [17, 7_500],
              ]);
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
              ).toMatchObject([{ on_hand: 7 }]);
              expect(
                await observer.all(app.districts.where({ id: district.id }).limit(1), {
                  tier: "edge",
                }),
              ).toMatchObject([{ next_order_number: 18 }]);
              expect(
                await observer.all(app.customers.where({ id: customer.id }).limit(1), {
                  tier: "edge",
                }),
              ).toMatchObject([{ balance_cents: -7_500 }]);

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
              const [
                ordersAfterRejectedCheckout,
                stockAfterRejectedCheckout,
                districtAfterRejectedCheckout,
                customerAfterRejectedCheckout,
                linesAfterRejectedCheckout,
                paymentsAfterRejectedCheckout,
              ] = await Promise.all([
                observer.all(app.orders.where({ warehouse_id: warehouse.id }), { tier: "edge" }),
                observer.all(app.stock.where({ id: stock.id }).limit(1), { tier: "edge" }),
                observer.all(app.districts.where({ id: district.id }).limit(1), {
                  tier: "edge",
                }),
                observer.all(app.customers.where({ id: customer.id }).limit(1), {
                  tier: "edge",
                }),
                observer.all(app.order_lines.where({ order_id: receipts[0]!.orderId }), {
                  tier: "edge",
                }),
                observer.all(app.payments.where({ order_id: receipts[0]!.orderId }), {
                  tier: "edge",
                }),
              ]);
              expect(ordersAfterRejectedCheckout).toHaveLength(1);
              expect(stockAfterRejectedCheckout).toMatchObject([{ on_hand: 7 }]);
              expect(districtAfterRejectedCheckout).toMatchObject([{ next_order_number: 18 }]);
              expect(customerAfterRejectedCheckout).toMatchObject([{ balance_cents: -7_500 }]);
              expect(linesAfterRejectedCheckout).toHaveLength(1);
              expect(paymentsAfterRejectedCheckout).toHaveLength(1);
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
              expect(
                await owner.all(
                  warehouseQueries({ warehouseId: warehouse.id, districtId: district.id })
                    .allOrders,
                  {
                    tier: "edge",
                  },
                ),
              ).toMatchObject([{ order_number: 17, total_cents: 7_500 }]);
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
      expect.arrayContaining(["duplicated", "delivered"]),
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

function topologySeed(): number {
  const value = (import.meta as ImportMeta & { env?: Record<string, string | undefined> }).env
    ?.JAZZ_EXAMPLE_TOPOLOGY_SEED;
  const seed = Number(value ?? 71);
  return Number.isSafeInteger(seed) ? seed : 71;
}
