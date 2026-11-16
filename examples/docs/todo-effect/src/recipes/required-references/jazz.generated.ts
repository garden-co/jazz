// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Output 8af8cc16fec51581. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type Customer = {
  readonly id: string;
  name: string;
};

export type CustomerInsert = {
  name: string;
};

export type Order = {
  readonly id: string;
  customerId: string;
};

export type OrderInsert = {
  customerId: string;
};

interface Types extends AppTypes {
  customers: {
    row: Customer;
    insert: CustomerInsert;
    columns: {
      name: { kind: "string"; nullable: false; value: string };
    };
    relations: {
      orders: { many: "orders" };
    };
  };
  orders: {
    row: Order;
    insert: OrderInsert;
    columns: {
      customerId: { kind: "uuid"; nullable: false; value: string };
    };
    relations: {
      customer: { one: "customers"; nullable: false };
    };
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:7a0d80dd326c03183ea22f3aca4db17a53749db5bb5751226bceb28ba9209383",
  "schema": {
    "current": "b53f5ec01a265343fe53f10c51409731b032ae4cf42e92a50737617803152c68",
    "schemas": [
      {
        "hash": "b53f5ec01a265343fe53f10c51409731b032ae4cf42e92a50737617803152c68",
        "schema": {
          "tables": {
            "customers": {
              "columns": [{ "column_type": { "type": "Text" }, "name": "name", "nullable": false }],
            },
            "orders": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "customerId",
                  "nullable": false,
                  "references": "customers",
                },
              ],
            },
          },
        },
      },
    ],
    "migrations": [],
  },
  "relations": {
    "customers": { "orders": { "many": "orders", "relation": "customer" } },
    "orders": { "customer": { "one": "customers", "column": "customerId" } },
  },
});
