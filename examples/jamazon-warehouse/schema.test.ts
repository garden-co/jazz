import { describe, expect, it } from "vitest";

import { app } from "./schema.js";
import { seedRows } from "./src/seed.js";
import { belowReorderLevel, consoleQueries, warehouseQueries } from "./src/warehouse.js";

describe("Jamazon Warehouse operational indexes", () => {
  it("emits the production query and policy indexes into the runtime schema", () => {
    expect(app.wasmSchema.warehouses?.indexed_columns).toEqual(["operator_id"]);
    expect(app.wasmSchema.items?.indexed_columns).toEqual(["operator_id"]);
    expect(app.wasmSchema.districts?.indexed_columns).toEqual(["warehouse_id", "name"]);
    expect(app.wasmSchema.stock?.indexed_columns).toEqual(["warehouse_id", "item_id", "on_hand"]);
    expect(app.wasmSchema.customers?.indexed_columns).toEqual([
      "warehouse_id",
      "district_id",
      "name",
    ]);
    expect(app.wasmSchema.orders?.indexed_columns).toEqual([
      "warehouse_id",
      "district_id",
      "customer_id",
      "status",
      "order_number",
      "idempotency_key",
    ]);
    expect(app.wasmSchema.order_lines?.indexed_columns).toEqual(["warehouse_id", "order_id"]);
    expect(app.wasmSchema.payments?.indexed_columns).toEqual([
      "warehouse_id",
      "order_id",
      "idempotency_key",
    ]);
    expect(app.wasmSchema.deliveries?.indexed_columns).toEqual(["warehouse_id", "order_id"]);
    expect(app.wasmSchema.warehouse_operators?.indexed_columns).toEqual([
      "warehouse_id",
      "account_id",
    ]);
  });

  it("keeps every public console order read ordered and bounded", () => {
    const queries = warehouseQueries({ warehouseId: "warehouse", districtId: "district" });

    expect(JSON.parse(queries.orders._build())).toMatchObject({
      orderBy: [["order_number", "asc"]],
      limit: 20,
    });
    expect(JSON.parse(queries.pendingOrders._build())).toMatchObject({
      orderBy: [["order_number", "asc"]],
      limit: 20,
    });
  });

  it("bounds every console read the UI subscribes to", () => {
    const scope = { warehouseId: "warehouse", districtId: "district" };
    const bounded = [
      consoleQueries.warehouses,
      consoleQueries.items,
      consoleQueries.districtsOf("warehouse"),
      consoleQueries.membershipsOf("account"),
      consoleQueries.ordersSince("warehouse", 0),
      consoleQueries.pendingInWarehouse("warehouse"),
      consoleQueries.pendingQueue(scope),
      consoleQueries.orderByNumber(scope, 1),
      consoleQueries.recentOrdersOf("customer"),
      consoleQueries.linesOf("order"),
      consoleQueries.deliveriesOf("warehouse", "order"),
      consoleQueries.stockOf("warehouse"),
      consoleQueries.stockLevelCandidates("warehouse"),
    ];
    for (const query of bounded) {
      expect(JSON.parse(query._build()).limit).toBeGreaterThan(0);
    }
  });
});

describe("Jamazon Warehouse seed profile", () => {
  it("is deterministic and internally consistent", () => {
    const rows = seedRows();
    expect(seedRows()).toEqual(rows);
    expect(rows.warehouses).toHaveLength(2);
    expect(rows.districts).toHaveLength(6);
    expect(rows.customers).toHaveLength(24);
    expect(rows.stock).toHaveLength(32);
    expect(rows.orders).toHaveLength(30);

    const ids = Object.values(rows).flatMap((list) => list.map((row) => row.id));
    expect(new Set(ids).size).toBe(ids.length);

    // Every reference stays inside its own warehouse (#1898).
    const byId = new Map(ids.map((id, index) => [id, index]));
    const district = new Map(rows.districts.map((row) => [row.id, row]));
    const customer = new Map(rows.customers.map((row) => [row.id, row]));
    const order = new Map(rows.orders.map((row) => [row.id, row]));
    for (const row of rows.customers) {
      expect(district.get(row.district_id)?.warehouse_id).toBe(row.warehouse_id);
    }
    for (const row of rows.orders) {
      expect(customer.get(row.customer_id)).toMatchObject({
        warehouse_id: row.warehouse_id,
        district_id: row.district_id,
      });
    }
    for (const row of [...rows.orderLines, ...rows.payments, ...rows.deliveries]) {
      expect(byId.has(row.order_id)).toBe(true);
      expect(order.get(row.order_id)?.warehouse_id).toBe(row.warehouse_id);
    }

    // Balances, totals and counters agree with the seeded orders.
    for (const row of rows.orders) {
      const lines = rows.orderLines.filter((line) => line.order_id === row.id);
      expect(lines.reduce((sum, line) => sum + line.amount_cents, 0)).toBe(row.total_cents);
    }
    for (const row of rows.customers) {
      const owed = rows.orders
        .filter((candidate) => candidate.customer_id === row.id)
        .reduce((sum, candidate) => sum + candidate.total_cents, 0);
      expect(row.balance_cents).toBe(-owed);
    }
    for (const row of rows.districts) {
      const numbers = rows.orders
        .filter((candidate) => candidate.district_id === row.id)
        .map((candidate) => candidate.order_number);
      expect(row.next_order_number).toBe(Math.max(...numbers) + 1);
    }
    expect(rows.stock.every((row) => row.on_hand >= 0)).toBe(true);
    expect(belowReorderLevel(rows.stock).length).toBeGreaterThan(0);
  });
});
