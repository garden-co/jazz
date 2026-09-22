import { describe, expect, it } from "vitest";

import { app } from "./schema.js";
import { warehouseQueries } from "./src/warehouse.js";

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
      "status",
      "order_number",
      "idempotency_key",
    ]);
    expect(app.wasmSchema.order_lines?.indexed_columns).toEqual(["warehouse_id", "order_id"]);
    expect(app.wasmSchema.payments?.indexed_columns).toEqual(["warehouse_id", "order_id"]);
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
});
