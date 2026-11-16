// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Output cb8717d3d23f7076. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type BetterAuthAccount = {
  readonly id: string;
  accountId: string;
  providerId: string;
  userId: string;
  password: string | null;
  createdAt: Date;
  updatedAt: Date;
};

export type BetterAuthAccountInsert = {
  accountId: string;
  providerId: string;
  userId: string;
  password?: string | null;
  createdAt: Date;
  updatedAt: Date;
};

export type BetterAuthJwk = {
  readonly id: string;
  publicKey: string;
  privateKey: string;
  createdAt: Date;
  expiresAt: Date | null;
  alg: string | null;
  crv: string | null;
};

export type BetterAuthJwkInsert = {
  publicKey: string;
  privateKey: string;
  createdAt: Date;
  expiresAt?: Date | null;
  alg?: string | null;
  crv?: string | null;
};

export type BetterAuthSession = {
  readonly id: string;
  expiresAt: Date;
  token: string;
  createdAt: Date;
  updatedAt: Date;
  ipAddress: string | null;
  userAgent: string | null;
  userId: string;
};

export type BetterAuthSessionInsert = {
  expiresAt: Date;
  token: string;
  createdAt: Date;
  updatedAt: Date;
  ipAddress?: string | null;
  userAgent?: string | null;
  userId: string;
};

export type BetterAuthUser = {
  readonly id: string;
  name: string;
  email: string;
  emailVerified: boolean;
  image: string | null;
  createdAt: Date;
  updatedAt: Date;
};

export type BetterAuthUserInsert = {
  name: string;
  email: string;
  emailVerified: boolean;
  image?: string | null;
  createdAt: Date;
  updatedAt: Date;
};

export type BetterAuthVerification = {
  readonly id: string;
  identifier: string;
  value: string;
  expiresAt: Date;
  createdAt: Date;
  updatedAt: Date;
};

export type BetterAuthVerificationInsert = {
  identifier: string;
  value: string;
  expiresAt: Date;
  createdAt: Date;
  updatedAt: Date;
};

export type Customer = {
  readonly id: string;
  warehouse_id: string;
  district_id: string;
  name: string;
  balance_cents: number;
};

export type CustomerInsert = {
  warehouse_id: string;
  district_id: string;
  name: string;
  balance_cents: number;
};

export type Delivery = {
  readonly id: string;
  warehouse_id: string;
  district_id: string;
  order_id: string;
  status: string;
};

export type DeliveryInsert = {
  warehouse_id: string;
  district_id: string;
  order_id: string;
  status: string;
};

export type District = {
  readonly id: string;
  warehouse_id: string;
  name: string;
  next_order_number: number;
};

export type DistrictInsert = {
  warehouse_id: string;
  name: string;
  next_order_number: number;
};

export type Item = {
  readonly id: string;
  sku: string;
  name: string;
  unit_price_cents: number;
  operator_id: string;
};

export type ItemInsert = {
  sku: string;
  name: string;
  unit_price_cents: number;
  operator_id: string;
};

export type OrderLine = {
  readonly id: string;
  warehouse_id: string;
  order_id: string;
  line_number: number;
  item_id: string;
  quantity: number;
  amount_cents: number;
};

export type OrderLineInsert = {
  warehouse_id: string;
  order_id: string;
  line_number: number;
  item_id: string;
  quantity: number;
  amount_cents: number;
};

export type Order = {
  readonly id: string;
  warehouse_id: string;
  district_id: string;
  customer_id: string;
  order_number: number;
  status: string;
  total_cents: number;
  idempotency_key: string;
  reserved_lines: string | null;
};

export type OrderInsert = {
  warehouse_id: string;
  district_id: string;
  customer_id: string;
  order_number: number;
  status: string;
  total_cents: number;
  idempotency_key: string;
  reserved_lines?: string | null;
};

export type Payment = {
  readonly id: string;
  warehouse_id: string;
  customer_id: string;
  order_id: string | null;
  amount_cents: number;
  idempotency_key: string;
};

export type PaymentInsert = {
  warehouse_id: string;
  customer_id: string;
  order_id?: string | null;
  amount_cents: number;
  idempotency_key: string;
};

export type Stock = {
  readonly id: string;
  warehouse_id: string;
  item_id: string;
  on_hand: number;
  reorder_level: number;
};

export type StockInsert = {
  warehouse_id: string;
  item_id: string;
  on_hand: number;
  reorder_level: number;
};

export type WarehouseOperator = {
  readonly id: string;
  warehouse_id: string;
  account_id: string;
  name: string;
};

export type WarehouseOperatorInsert = {
  warehouse_id: string;
  account_id: string;
  name: string;
};

export type Warehouse = {
  readonly id: string;
  name: string;
  region: string;
  operator_id: string;
};

export type WarehouseInsert = {
  name: string;
  region: string;
  operator_id: string;
};

interface Types extends AppTypes {
  better_auth_account: {
    row: BetterAuthAccount;
    insert: BetterAuthAccountInsert;
    columns: {
      accountId: { kind: "string"; nullable: false; value: string };
      providerId: { kind: "string"; nullable: false; value: string };
      userId: { kind: "uuid"; nullable: false; value: string };
      password: { kind: "string"; nullable: true; value: string };
      createdAt: { kind: "timestamp"; nullable: false; value: Date };
      updatedAt: { kind: "timestamp"; nullable: false; value: Date };
    };
    relations: {
      user: { one: "better_auth_user"; nullable: false };
    };
  };
  better_auth_jwks: {
    row: BetterAuthJwk;
    insert: BetterAuthJwkInsert;
    columns: {
      publicKey: { kind: "string"; nullable: false; value: string };
      privateKey: { kind: "string"; nullable: false; value: string };
      createdAt: { kind: "timestamp"; nullable: false; value: Date };
      expiresAt: { kind: "timestamp"; nullable: true; value: Date };
      alg: { kind: "string"; nullable: true; value: string };
      crv: { kind: "string"; nullable: true; value: string };
    };
    relations: {};
  };
  better_auth_session: {
    row: BetterAuthSession;
    insert: BetterAuthSessionInsert;
    columns: {
      expiresAt: { kind: "timestamp"; nullable: false; value: Date };
      token: { kind: "string"; nullable: false; value: string };
      createdAt: { kind: "timestamp"; nullable: false; value: Date };
      updatedAt: { kind: "timestamp"; nullable: false; value: Date };
      ipAddress: { kind: "string"; nullable: true; value: string };
      userAgent: { kind: "string"; nullable: true; value: string };
      userId: { kind: "uuid"; nullable: false; value: string };
    };
    relations: {
      user: { one: "better_auth_user"; nullable: false };
    };
  };
  better_auth_user: {
    row: BetterAuthUser;
    insert: BetterAuthUserInsert;
    columns: {
      name: { kind: "string"; nullable: false; value: string };
      email: { kind: "string"; nullable: false; value: string };
      emailVerified: { kind: "boolean"; nullable: false; value: boolean };
      image: { kind: "string"; nullable: true; value: string };
      createdAt: { kind: "timestamp"; nullable: false; value: Date };
      updatedAt: { kind: "timestamp"; nullable: false; value: Date };
    };
    relations: {
      better_auth_accountViaUser: { many: "better_auth_account" };
      better_auth_sessionViaUser: { many: "better_auth_session" };
    };
  };
  better_auth_verification: {
    row: BetterAuthVerification;
    insert: BetterAuthVerificationInsert;
    columns: {
      identifier: { kind: "string"; nullable: false; value: string };
      value: { kind: "string"; nullable: false; value: string };
      expiresAt: { kind: "timestamp"; nullable: false; value: Date };
      createdAt: { kind: "timestamp"; nullable: false; value: Date };
      updatedAt: { kind: "timestamp"; nullable: false; value: Date };
    };
    relations: {};
  };
  customers: {
    row: Customer;
    insert: CustomerInsert;
    columns: {
      warehouse_id: { kind: "uuid"; nullable: false; value: string };
      district_id: { kind: "uuid"; nullable: false; value: string };
      name: { kind: "string"; nullable: false; value: string };
      balance_cents: { kind: "int"; nullable: false; value: number };
    };
    relations: {
      district: { one: "districts"; nullable: false };
      ordersViaCustomer: { many: "orders" };
      paymentsViaCustomer: { many: "payments" };
      warehouse: { one: "warehouses"; nullable: false };
    };
  };
  deliveries: {
    row: Delivery;
    insert: DeliveryInsert;
    columns: {
      warehouse_id: { kind: "uuid"; nullable: false; value: string };
      district_id: { kind: "uuid"; nullable: false; value: string };
      order_id: { kind: "uuid"; nullable: false; value: string };
      status: { kind: "string"; nullable: false; value: string };
    };
    relations: {
      district: { one: "districts"; nullable: false };
      order: { one: "orders"; nullable: false };
      warehouse: { one: "warehouses"; nullable: false };
    };
  };
  districts: {
    row: District;
    insert: DistrictInsert;
    columns: {
      warehouse_id: { kind: "uuid"; nullable: false; value: string };
      name: { kind: "string"; nullable: false; value: string };
      next_order_number: { kind: "int"; nullable: false; value: number };
    };
    relations: {
      customersViaDistrict: { many: "customers" };
      deliveriesViaDistrict: { many: "deliveries" };
      ordersViaDistrict: { many: "orders" };
      warehouse: { one: "warehouses"; nullable: false };
    };
  };
  items: {
    row: Item;
    insert: ItemInsert;
    columns: {
      sku: { kind: "string"; nullable: false; value: string };
      name: { kind: "string"; nullable: false; value: string };
      unit_price_cents: { kind: "int"; nullable: false; value: number };
      operator_id: { kind: "uuid"; nullable: false; value: string };
    };
    relations: {
      order_linesViaItem: { many: "order_lines" };
      stockViaItem: { many: "stock" };
    };
  };
  order_lines: {
    row: OrderLine;
    insert: OrderLineInsert;
    columns: {
      warehouse_id: { kind: "uuid"; nullable: false; value: string };
      order_id: { kind: "uuid"; nullable: false; value: string };
      line_number: { kind: "int"; nullable: false; value: number };
      item_id: { kind: "uuid"; nullable: false; value: string };
      quantity: { kind: "int"; nullable: false; value: number };
      amount_cents: { kind: "int"; nullable: false; value: number };
    };
    relations: {
      item: { one: "items"; nullable: false };
      order: { one: "orders"; nullable: false };
      warehouse: { one: "warehouses"; nullable: false };
    };
  };
  orders: {
    row: Order;
    insert: OrderInsert;
    columns: {
      warehouse_id: { kind: "uuid"; nullable: false; value: string };
      district_id: { kind: "uuid"; nullable: false; value: string };
      customer_id: { kind: "uuid"; nullable: false; value: string };
      order_number: { kind: "int"; nullable: false; value: number };
      status: { kind: "string"; nullable: false; value: string };
      total_cents: { kind: "int"; nullable: false; value: number };
      idempotency_key: { kind: "string"; nullable: false; value: string };
      reserved_lines: { kind: "string"; nullable: true; value: string };
    };
    relations: {
      customer: { one: "customers"; nullable: false };
      deliveriesViaOrder: { many: "deliveries" };
      district: { one: "districts"; nullable: false };
      order_linesViaOrder: { many: "order_lines" };
      paymentsViaOrder: { many: "payments" };
      warehouse: { one: "warehouses"; nullable: false };
    };
  };
  payments: {
    row: Payment;
    insert: PaymentInsert;
    columns: {
      warehouse_id: { kind: "uuid"; nullable: false; value: string };
      customer_id: { kind: "uuid"; nullable: false; value: string };
      order_id: { kind: "uuid"; nullable: true; value: string };
      amount_cents: { kind: "int"; nullable: false; value: number };
      idempotency_key: { kind: "string"; nullable: false; value: string };
    };
    relations: {
      customer: { one: "customers"; nullable: false };
      order: { one: "orders"; nullable: true };
      warehouse: { one: "warehouses"; nullable: false };
    };
  };
  stock: {
    row: Stock;
    insert: StockInsert;
    columns: {
      warehouse_id: { kind: "uuid"; nullable: false; value: string };
      item_id: { kind: "uuid"; nullable: false; value: string };
      on_hand: { kind: "int"; nullable: false; value: number };
      reorder_level: { kind: "int"; nullable: false; value: number };
    };
    relations: {
      item: { one: "items"; nullable: false };
      warehouse: { one: "warehouses"; nullable: false };
    };
  };
  warehouse_operators: {
    row: WarehouseOperator;
    insert: WarehouseOperatorInsert;
    columns: {
      warehouse_id: { kind: "uuid"; nullable: false; value: string };
      account_id: { kind: "uuid"; nullable: false; value: string };
      name: { kind: "string"; nullable: false; value: string };
    };
    relations: {
      warehouse: { one: "warehouses"; nullable: false };
    };
  };
  warehouses: {
    row: Warehouse;
    insert: WarehouseInsert;
    columns: {
      name: { kind: "string"; nullable: false; value: string };
      region: { kind: "string"; nullable: false; value: string };
      operator_id: { kind: "uuid"; nullable: false; value: string };
    };
    relations: {
      customersViaWarehouse: { many: "customers" };
      deliveriesViaWarehouse: { many: "deliveries" };
      districtsViaWarehouse: { many: "districts" };
      operatorsViaWarehouse: { many: "warehouse_operators" };
      order_linesViaWarehouse: { many: "order_lines" };
      ordersViaWarehouse: { many: "orders" };
      paymentsViaWarehouse: { many: "payments" };
      stockViaWarehouse: { many: "stock" };
    };
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:206063292366c9759a9d2db437f02d33d4a75fe5f5e62f5f668bdf98195bb320",
  "schema": {
    "current": "36b03f261e65be21093aa294715b621b753dbc531e003f1487436c1946f174f9",
    "schemas": [
      {
        "hash": "7ce960ad34dc4f77c92e07727acc3113ccf803934671a1fe2f46cf736d2641ce",
        "schema": {
          "tables": {
            "better_auth_account": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "issuer", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "accountId", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "providerId", "nullable": false },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "userId",
                  "nullable": false,
                  "references": "better_auth_user",
                },
                { "column_type": { "type": "Text" }, "name": "password", "nullable": true },
                { "column_type": { "type": "Timestamp" }, "name": "createdAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "updatedAt", "nullable": false },
              ],
            },
            "better_auth_jwks": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "publicKey", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "privateKey", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "createdAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "expiresAt", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "alg", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "crv", "nullable": true },
              ],
            },
            "better_auth_session": {
              "columns": [
                { "column_type": { "type": "Timestamp" }, "name": "expiresAt", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "token", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "createdAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "updatedAt", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "ipAddress", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "userAgent", "nullable": true },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "userId",
                  "nullable": false,
                  "references": "better_auth_user",
                },
              ],
            },
            "better_auth_user": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "email", "nullable": false },
                { "column_type": { "type": "Boolean" }, "name": "emailVerified", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "image", "nullable": true },
                { "column_type": { "type": "Timestamp" }, "name": "createdAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "updatedAt", "nullable": false },
              ],
            },
            "better_auth_verification": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "identifier", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "value", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "expiresAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "createdAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "updatedAt", "nullable": false },
              ],
            },
            "customers": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "warehouse_id",
                  "nullable": false,
                  "references": "warehouses",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "district_id",
                  "nullable": false,
                  "references": "districts",
                },
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "balance_cents", "nullable": false },
              ],
              "indexed_columns": ["warehouse_id", "district_id", "name"],
            },
            "deliveries": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "warehouse_id",
                  "nullable": false,
                  "references": "warehouses",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "district_id",
                  "nullable": false,
                  "references": "districts",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "order_id",
                  "nullable": false,
                  "references": "orders",
                },
                { "column_type": { "type": "Text" }, "name": "status", "nullable": false },
              ],
              "indexed_columns": ["warehouse_id", "order_id"],
            },
            "districts": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "warehouse_id",
                  "nullable": false,
                  "references": "warehouses",
                },
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                {
                  "column_type": { "type": "Integer" },
                  "name": "next_order_number",
                  "nullable": false,
                },
              ],
              "indexed_columns": ["warehouse_id", "name"],
            },
            "items": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "sku", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                {
                  "column_type": { "type": "Integer" },
                  "name": "unit_price_cents",
                  "nullable": false,
                },
                { "column_type": { "type": "Uuid" }, "name": "operator_id", "nullable": false },
              ],
              "indexed_columns": ["operator_id"],
            },
            "order_lines": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "warehouse_id",
                  "nullable": false,
                  "references": "warehouses",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "order_id",
                  "nullable": false,
                  "references": "orders",
                },
                { "column_type": { "type": "Integer" }, "name": "line_number", "nullable": false },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "item_id",
                  "nullable": false,
                  "references": "items",
                },
                { "column_type": { "type": "Integer" }, "name": "quantity", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "amount_cents", "nullable": false },
              ],
              "indexed_columns": ["warehouse_id", "order_id"],
            },
            "orders": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "warehouse_id",
                  "nullable": false,
                  "references": "warehouses",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "district_id",
                  "nullable": false,
                  "references": "districts",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "customer_id",
                  "nullable": false,
                  "references": "customers",
                },
                { "column_type": { "type": "Integer" }, "name": "order_number", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "status", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "total_cents", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "idempotency_key", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "reserved_lines", "nullable": true },
              ],
              "indexed_columns": [
                "warehouse_id",
                "district_id",
                "customer_id",
                "status",
                "order_number",
                "idempotency_key",
              ],
            },
            "payments": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "warehouse_id",
                  "nullable": false,
                  "references": "warehouses",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "customer_id",
                  "nullable": false,
                  "references": "customers",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "order_id",
                  "nullable": true,
                  "references": "orders",
                },
                { "column_type": { "type": "Integer" }, "name": "amount_cents", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "idempotency_key", "nullable": false },
              ],
              "indexed_columns": ["warehouse_id", "order_id", "idempotency_key"],
            },
            "stock": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "warehouse_id",
                  "nullable": false,
                  "references": "warehouses",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "item_id",
                  "nullable": false,
                  "references": "items",
                },
                { "column_type": { "type": "Integer" }, "name": "on_hand", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "reorder_level", "nullable": false },
              ],
              "indexed_columns": ["warehouse_id", "item_id", "on_hand"],
            },
            "warehouse_operators": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "warehouse_id",
                  "nullable": false,
                  "references": "warehouses",
                },
                { "column_type": { "type": "Uuid" }, "name": "account_id", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
              ],
              "indexed_columns": ["warehouse_id", "account_id"],
            },
            "warehouses": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "region", "nullable": false },
                { "column_type": { "type": "Uuid" }, "name": "operator_id", "nullable": false },
              ],
              "indexed_columns": ["operator_id"],
            },
          },
        },
      },
      {
        "hash": "36b03f261e65be21093aa294715b621b753dbc531e003f1487436c1946f174f9",
        "schema": {
          "tables": {
            "better_auth_account": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "accountId", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "providerId", "nullable": false },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "userId",
                  "nullable": false,
                  "references": "better_auth_user",
                },
                { "column_type": { "type": "Text" }, "name": "password", "nullable": true },
                { "column_type": { "type": "Timestamp" }, "name": "createdAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "updatedAt", "nullable": false },
              ],
            },
            "better_auth_jwks": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "publicKey", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "privateKey", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "createdAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "expiresAt", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "alg", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "crv", "nullable": true },
              ],
            },
            "better_auth_session": {
              "columns": [
                { "column_type": { "type": "Timestamp" }, "name": "expiresAt", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "token", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "createdAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "updatedAt", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "ipAddress", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "userAgent", "nullable": true },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "userId",
                  "nullable": false,
                  "references": "better_auth_user",
                },
              ],
            },
            "better_auth_user": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "email", "nullable": false },
                { "column_type": { "type": "Boolean" }, "name": "emailVerified", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "image", "nullable": true },
                { "column_type": { "type": "Timestamp" }, "name": "createdAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "updatedAt", "nullable": false },
              ],
            },
            "better_auth_verification": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "identifier", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "value", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "expiresAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "createdAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "updatedAt", "nullable": false },
              ],
            },
            "customers": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "warehouse_id",
                  "nullable": false,
                  "references": "warehouses",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "district_id",
                  "nullable": false,
                  "references": "districts",
                },
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "balance_cents", "nullable": false },
              ],
              "indexed_columns": ["warehouse_id", "district_id", "name"],
            },
            "deliveries": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "warehouse_id",
                  "nullable": false,
                  "references": "warehouses",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "district_id",
                  "nullable": false,
                  "references": "districts",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "order_id",
                  "nullable": false,
                  "references": "orders",
                },
                { "column_type": { "type": "Text" }, "name": "status", "nullable": false },
              ],
              "indexed_columns": ["warehouse_id", "order_id"],
            },
            "districts": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "warehouse_id",
                  "nullable": false,
                  "references": "warehouses",
                },
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                {
                  "column_type": { "type": "Integer" },
                  "name": "next_order_number",
                  "nullable": false,
                },
              ],
              "indexed_columns": ["warehouse_id", "name"],
            },
            "items": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "sku", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                {
                  "column_type": { "type": "Integer" },
                  "name": "unit_price_cents",
                  "nullable": false,
                },
                { "column_type": { "type": "Uuid" }, "name": "operator_id", "nullable": false },
              ],
              "indexed_columns": ["operator_id"],
            },
            "order_lines": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "warehouse_id",
                  "nullable": false,
                  "references": "warehouses",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "order_id",
                  "nullable": false,
                  "references": "orders",
                },
                { "column_type": { "type": "Integer" }, "name": "line_number", "nullable": false },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "item_id",
                  "nullable": false,
                  "references": "items",
                },
                { "column_type": { "type": "Integer" }, "name": "quantity", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "amount_cents", "nullable": false },
              ],
              "indexed_columns": ["warehouse_id", "order_id"],
            },
            "orders": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "warehouse_id",
                  "nullable": false,
                  "references": "warehouses",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "district_id",
                  "nullable": false,
                  "references": "districts",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "customer_id",
                  "nullable": false,
                  "references": "customers",
                },
                { "column_type": { "type": "Integer" }, "name": "order_number", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "status", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "total_cents", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "idempotency_key", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "reserved_lines", "nullable": true },
              ],
              "indexed_columns": [
                "warehouse_id",
                "district_id",
                "customer_id",
                "status",
                "order_number",
                "idempotency_key",
              ],
            },
            "payments": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "warehouse_id",
                  "nullable": false,
                  "references": "warehouses",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "customer_id",
                  "nullable": false,
                  "references": "customers",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "order_id",
                  "nullable": true,
                  "references": "orders",
                },
                { "column_type": { "type": "Integer" }, "name": "amount_cents", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "idempotency_key", "nullable": false },
              ],
              "indexed_columns": ["warehouse_id", "order_id", "idempotency_key"],
            },
            "stock": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "warehouse_id",
                  "nullable": false,
                  "references": "warehouses",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "item_id",
                  "nullable": false,
                  "references": "items",
                },
                { "column_type": { "type": "Integer" }, "name": "on_hand", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "reorder_level", "nullable": false },
              ],
              "indexed_columns": ["warehouse_id", "item_id", "on_hand"],
            },
            "warehouse_operators": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "warehouse_id",
                  "nullable": false,
                  "references": "warehouses",
                },
                { "column_type": { "type": "Uuid" }, "name": "account_id", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
              ],
              "indexed_columns": ["warehouse_id", "account_id"],
            },
            "warehouses": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "region", "nullable": false },
                { "column_type": { "type": "Uuid" }, "name": "operator_id", "nullable": false },
              ],
              "indexed_columns": ["operator_id"],
            },
          },
        },
      },
    ],
    "migrations": [
      {
        "forward": [
          {
            "added": false,
            "operations": [
              {
                "column": "issuer",
                "column_type": { "type": "Text" },
                "type": "drop",
                "value": { "type": "Text", "value": "" },
              },
            ],
            "removed": false,
            "renamedFrom": null,
            "table": "better_auth_account",
          },
        ],
        "fromHash": "7ce960ad34dc4f77c92e07727acc3113ccf803934671a1fe2f46cf736d2641ce",
        "toHash": "36b03f261e65be21093aa294715b621b753dbc531e003f1487436c1946f174f9",
      },
    ],
  },
  "relations": {
    "better_auth_account": { "user": { "one": "better_auth_user", "column": "userId" } },
    "better_auth_jwks": {},
    "better_auth_session": { "user": { "one": "better_auth_user", "column": "userId" } },
    "better_auth_user": {
      "better_auth_accountViaUser": { "many": "better_auth_account", "relation": "user" },
      "better_auth_sessionViaUser": { "many": "better_auth_session", "relation": "user" },
    },
    "better_auth_verification": {},
    "customers": {
      "district": { "one": "districts", "column": "district_id" },
      "ordersViaCustomer": { "many": "orders", "relation": "customer" },
      "paymentsViaCustomer": { "many": "payments", "relation": "customer" },
      "warehouse": { "one": "warehouses", "column": "warehouse_id" },
    },
    "deliveries": {
      "district": { "one": "districts", "column": "district_id" },
      "order": { "one": "orders", "column": "order_id" },
      "warehouse": { "one": "warehouses", "column": "warehouse_id" },
    },
    "districts": {
      "customersViaDistrict": { "many": "customers", "relation": "district" },
      "deliveriesViaDistrict": { "many": "deliveries", "relation": "district" },
      "ordersViaDistrict": { "many": "orders", "relation": "district" },
      "warehouse": { "one": "warehouses", "column": "warehouse_id" },
    },
    "items": {
      "order_linesViaItem": { "many": "order_lines", "relation": "item" },
      "stockViaItem": { "many": "stock", "relation": "item" },
    },
    "order_lines": {
      "item": { "one": "items", "column": "item_id" },
      "order": { "one": "orders", "column": "order_id" },
      "warehouse": { "one": "warehouses", "column": "warehouse_id" },
    },
    "orders": {
      "customer": { "one": "customers", "column": "customer_id" },
      "deliveriesViaOrder": { "many": "deliveries", "relation": "order" },
      "district": { "one": "districts", "column": "district_id" },
      "order_linesViaOrder": { "many": "order_lines", "relation": "order" },
      "paymentsViaOrder": { "many": "payments", "relation": "order" },
      "warehouse": { "one": "warehouses", "column": "warehouse_id" },
    },
    "payments": {
      "customer": { "one": "customers", "column": "customer_id" },
      "order": { "one": "orders", "column": "order_id" },
      "warehouse": { "one": "warehouses", "column": "warehouse_id" },
    },
    "stock": {
      "item": { "one": "items", "column": "item_id" },
      "warehouse": { "one": "warehouses", "column": "warehouse_id" },
    },
    "warehouse_operators": { "warehouse": { "one": "warehouses", "column": "warehouse_id" } },
    "warehouses": {
      "customersViaWarehouse": { "many": "customers", "relation": "warehouse" },
      "deliveriesViaWarehouse": { "many": "deliveries", "relation": "warehouse" },
      "districtsViaWarehouse": { "many": "districts", "relation": "warehouse" },
      "operatorsViaWarehouse": { "many": "warehouse_operators", "relation": "warehouse" },
      "order_linesViaWarehouse": { "many": "order_lines", "relation": "warehouse" },
      "ordersViaWarehouse": { "many": "orders", "relation": "warehouse" },
      "paymentsViaWarehouse": { "many": "payments", "relation": "warehouse" },
      "stockViaWarehouse": { "many": "stock", "relation": "warehouse" },
    },
  },
});
