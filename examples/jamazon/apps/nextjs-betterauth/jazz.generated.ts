// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Output 11407cf0cbbe42b4. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes, type JsonValue } from "jazz-tools";

export type CartShippingMethod = "standard" | "express";
export type OrderEventStatus = "placed" | "paid" | "payment_failed" | "shipped";
export type OrderStatus = "placed" | "paid" | "payment_failed" | "shipped";
export type OrderShippingMethod = "standard" | "express";
export type PaymentProvider = "sandbox" | "stripe";
export type PaymentStatus = "requires_payment" | "succeeded" | "failed";
export type ProductHue =
  | "gray"
  | "blue"
  | "cyan"
  | "green"
  | "orange"
  | "pink"
  | "purple"
  | "red"
  | "teal"
  | "yellow";
export type ProductArt =
  | "guitar"
  | "bass"
  | "keys"
  | "synth"
  | "drum"
  | "cymbal"
  | "mic"
  | "headphones"
  | "amp"
  | "pedal"
  | "cable"
  | "strings"
  | "picks";

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

export type CartLine = {
  readonly id: string;
  cartId: string;
  productId: string;
  quantity: number;
};

export type CartLineInsert = {
  cartId: string;
  productId: string;
  quantity: number;
};

export type Cart = {
  readonly id: string;
  shopper: string;
  shippingMethod: CartShippingMethod;
  shipName: string | null;
  shipLine1: string | null;
  shipLine2: string | null;
  shipCity: string | null;
  shipPostcode: string | null;
  shipCountry: string | null;
  checkoutKey: string | null;
};

export type CartInsert = {
  shopper: string;
  shippingMethod?: CartShippingMethod;
  shipName?: string | null;
  shipLine1?: string | null;
  shipLine2?: string | null;
  shipCity?: string | null;
  shipPostcode?: string | null;
  shipCountry?: string | null;
  checkoutKey?: string | null;
};

export type Category = {
  readonly id: string;
  slug: string;
  name: string;
  blurb: string;
  position: number;
};

export type CategoryInsert = {
  slug: string;
  name: string;
  blurb: string;
  position: number;
};

export type OrderEvent = {
  readonly id: string;
  orderId: string;
  status: OrderEventStatus;
  note: string;
  at: Date;
};

export type OrderEventInsert = {
  orderId: string;
  status: OrderEventStatus;
  note: string;
  at: Date;
};

export type OrderLine = {
  readonly id: string;
  orderId: string;
  productId: string;
  productName: string;
  unitPriceCents: number;
  quantity: number;
};

export type OrderLineInsert = {
  orderId: string;
  productId: string;
  productName: string;
  unitPriceCents: number;
  quantity: number;
};

export type Order = {
  readonly id: string;
  shopper: string;
  code: string;
  status: OrderStatus;
  idempotencyKey: string;
  subtotalCents: number;
  shippingCents: number;
  totalCents: number;
  shippingMethod: OrderShippingMethod;
  shipName: string;
  shipLine1: string;
  shipLine2: string | null;
  shipCity: string;
  shipPostcode: string;
  shipCountry: string;
  placedAt: Date;
};

export type OrderInsert = {
  shopper: string;
  code: string;
  status: OrderStatus;
  idempotencyKey: string;
  subtotalCents: number;
  shippingCents: number;
  totalCents: number;
  shippingMethod: OrderShippingMethod;
  shipName: string;
  shipLine1: string;
  shipLine2?: string | null;
  shipCity: string;
  shipPostcode: string;
  shipCountry: string;
  placedAt: Date;
};

export type Payment = {
  readonly id: string;
  orderId: string;
  provider: PaymentProvider;
  providerRef: string;
  clientSecret: string | null;
  status: PaymentStatus;
  amountCents: number;
  failureReason: string | null;
};

export type PaymentInsert = {
  orderId: string;
  provider: PaymentProvider;
  providerRef: string;
  clientSecret?: string | null;
  status: PaymentStatus;
  amountCents: number;
  failureReason?: string | null;
};

export type Product = {
  readonly id: string;
  sku: string;
  slug: string;
  name: string;
  brand: string;
  categoryId: string;
  priceCents: number;
  summary: string;
  description: string;
  specs: JsonValue;
  hue: ProductHue;
  art: ProductArt;
  searchText: string;
  position: number;
};

export type ProductInsert = {
  sku: string;
  slug: string;
  name: string;
  brand: string;
  categoryId: string;
  priceCents: number;
  summary: string;
  description: string;
  specs: JsonValue;
  hue: ProductHue;
  art: ProductArt;
  searchText: string;
  position: number;
};

export type Stock = {
  readonly id: string;
  productId: string;
  onHand: number;
};

export type StockInsert = {
  productId: string;
  onHand: number;
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
  cartLines: {
    row: CartLine;
    insert: CartLineInsert;
    columns: {
      cartId: { kind: "uuid"; nullable: false; value: string };
      productId: { kind: "uuid"; nullable: false; value: string };
      quantity: { kind: "int"; nullable: false; value: number };
    };
    relations: {
      cart: { one: "carts"; nullable: false };
      product: { one: "products"; nullable: false };
    };
  };
  carts: {
    row: Cart;
    insert: CartInsert;
    columns: {
      shopper: { kind: "uuid"; nullable: false; value: string };
      shippingMethod: { kind: "enum"; nullable: false; value: CartShippingMethod };
      shipName: { kind: "string"; nullable: true; value: string };
      shipLine1: { kind: "string"; nullable: true; value: string };
      shipLine2: { kind: "string"; nullable: true; value: string };
      shipCity: { kind: "string"; nullable: true; value: string };
      shipPostcode: { kind: "string"; nullable: true; value: string };
      shipCountry: { kind: "string"; nullable: true; value: string };
      checkoutKey: { kind: "string"; nullable: true; value: string };
    };
    relations: {
      cartLinesViaCart: { many: "cartLines" };
    };
  };
  categories: {
    row: Category;
    insert: CategoryInsert;
    columns: {
      slug: { kind: "string"; nullable: false; value: string };
      name: { kind: "string"; nullable: false; value: string };
      blurb: { kind: "string"; nullable: false; value: string };
      position: { kind: "int"; nullable: false; value: number };
    };
    relations: {
      productsViaCategory: { many: "products" };
    };
  };
  orderEvents: {
    row: OrderEvent;
    insert: OrderEventInsert;
    columns: {
      orderId: { kind: "uuid"; nullable: false; value: string };
      status: { kind: "enum"; nullable: false; value: OrderEventStatus };
      note: { kind: "string"; nullable: false; value: string };
      at: { kind: "timestamp"; nullable: false; value: Date };
    };
    relations: {
      order: { one: "orders"; nullable: false };
    };
  };
  orderLines: {
    row: OrderLine;
    insert: OrderLineInsert;
    columns: {
      orderId: { kind: "uuid"; nullable: false; value: string };
      productId: { kind: "uuid"; nullable: false; value: string };
      productName: { kind: "string"; nullable: false; value: string };
      unitPriceCents: { kind: "int"; nullable: false; value: number };
      quantity: { kind: "int"; nullable: false; value: number };
    };
    relations: {
      order: { one: "orders"; nullable: false };
      product: { one: "products"; nullable: false };
    };
  };
  orders: {
    row: Order;
    insert: OrderInsert;
    columns: {
      shopper: { kind: "uuid"; nullable: false; value: string };
      code: { kind: "string"; nullable: false; value: string };
      status: { kind: "enum"; nullable: false; value: OrderStatus };
      idempotencyKey: { kind: "string"; nullable: false; value: string };
      subtotalCents: { kind: "int"; nullable: false; value: number };
      shippingCents: { kind: "int"; nullable: false; value: number };
      totalCents: { kind: "int"; nullable: false; value: number };
      shippingMethod: { kind: "enum"; nullable: false; value: OrderShippingMethod };
      shipName: { kind: "string"; nullable: false; value: string };
      shipLine1: { kind: "string"; nullable: false; value: string };
      shipLine2: { kind: "string"; nullable: true; value: string };
      shipCity: { kind: "string"; nullable: false; value: string };
      shipPostcode: { kind: "string"; nullable: false; value: string };
      shipCountry: { kind: "string"; nullable: false; value: string };
      placedAt: { kind: "timestamp"; nullable: false; value: Date };
    };
    relations: {
      orderEventsViaOrder: { many: "orderEvents" };
      orderLinesViaOrder: { many: "orderLines" };
      paymentsViaOrder: { many: "payments" };
    };
  };
  payments: {
    row: Payment;
    insert: PaymentInsert;
    columns: {
      orderId: { kind: "uuid"; nullable: false; value: string };
      provider: { kind: "enum"; nullable: false; value: PaymentProvider };
      providerRef: { kind: "string"; nullable: false; value: string };
      clientSecret: { kind: "string"; nullable: true; value: string };
      status: { kind: "enum"; nullable: false; value: PaymentStatus };
      amountCents: { kind: "int"; nullable: false; value: number };
      failureReason: { kind: "string"; nullable: true; value: string };
    };
    relations: {
      order: { one: "orders"; nullable: false };
    };
  };
  products: {
    row: Product;
    insert: ProductInsert;
    columns: {
      sku: { kind: "string"; nullable: false; value: string };
      slug: { kind: "string"; nullable: false; value: string };
      name: { kind: "string"; nullable: false; value: string };
      brand: { kind: "string"; nullable: false; value: string };
      categoryId: { kind: "uuid"; nullable: false; value: string };
      priceCents: { kind: "int"; nullable: false; value: number };
      summary: { kind: "string"; nullable: false; value: string };
      description: { kind: "string"; nullable: false; value: string };
      specs: { kind: "json"; nullable: false; value: JsonValue };
      hue: { kind: "enum"; nullable: false; value: ProductHue };
      art: { kind: "enum"; nullable: false; value: ProductArt };
      searchText: { kind: "string"; nullable: false; value: string };
      position: { kind: "int"; nullable: false; value: number };
    };
    relations: {
      cartLinesViaProduct: { many: "cartLines" };
      category: { one: "categories"; nullable: false };
      orderLinesViaProduct: { many: "orderLines" };
      stockViaProduct: { many: "stock" };
    };
  };
  stock: {
    row: Stock;
    insert: StockInsert;
    columns: {
      productId: { kind: "uuid"; nullable: false; value: string };
      onHand: { kind: "int"; nullable: false; value: number };
    };
    relations: {
      product: { one: "products"; nullable: false };
    };
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:a2f29e401bffee70a5a816e936b6613727b8619150b7c7d93b1c524054f90334",
  "schema": {
    "current": "f7318f0be5fcbf2065324e4f545c96323edfa4e007186fb89eabff519ca370d8",
    "schemas": [
      {
        "hash": "5806d5c6e98c33ae41c6ba5b3da6cca0ca72d69467521c5381c1babe56adf6b0",
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
            "cartLines": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "cartId",
                  "nullable": false,
                  "references": "carts",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "productId",
                  "nullable": false,
                  "references": "products",
                },
                { "column_type": { "type": "Integer" }, "name": "quantity", "nullable": false },
              ],
              "indexed_columns": ["cartId"],
            },
            "carts": {
              "columns": [
                { "column_type": { "type": "Uuid" }, "name": "shopper", "nullable": false },
                {
                  "column_type": { "type": "Enum", "variants": ["standard", "express"] },
                  "default": { "type": "Text", "value": "standard" },
                  "name": "shippingMethod",
                  "nullable": false,
                },
                { "column_type": { "type": "Text" }, "name": "shipName", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "shipLine1", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "shipLine2", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "shipCity", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "shipPostcode", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "shipCountry", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "checkoutKey", "nullable": true },
              ],
              "indexed_columns": ["shopper"],
            },
            "categories": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "slug", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "blurb", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "position", "nullable": false },
              ],
              "indexed_columns": ["slug", "position"],
            },
            "orderEvents": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "orderId",
                  "nullable": false,
                  "references": "orders",
                },
                {
                  "column_type": {
                    "type": "Enum",
                    "variants": ["placed", "paid", "payment_failed", "shipped"],
                  },
                  "name": "status",
                  "nullable": false,
                },
                { "column_type": { "type": "Text" }, "name": "note", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "at", "nullable": false },
              ],
              "indexed_columns": ["orderId", "at"],
            },
            "orderLines": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "orderId",
                  "nullable": false,
                  "references": "orders",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "productId",
                  "nullable": false,
                  "references": "products",
                },
                { "column_type": { "type": "Text" }, "name": "productName", "nullable": false },
                {
                  "column_type": { "type": "Integer" },
                  "name": "unitPriceCents",
                  "nullable": false,
                },
                { "column_type": { "type": "Integer" }, "name": "quantity", "nullable": false },
              ],
              "indexed_columns": ["orderId"],
            },
            "orders": {
              "columns": [
                { "column_type": { "type": "Uuid" }, "name": "shopper", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "code", "nullable": false },
                {
                  "column_type": {
                    "type": "Enum",
                    "variants": ["placed", "paid", "payment_failed", "shipped"],
                  },
                  "name": "status",
                  "nullable": false,
                },
                { "column_type": { "type": "Text" }, "name": "idempotencyKey", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "subtotalCents", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "shippingCents", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "totalCents", "nullable": false },
                {
                  "column_type": { "type": "Enum", "variants": ["standard", "express"] },
                  "name": "shippingMethod",
                  "nullable": false,
                },
                { "column_type": { "type": "Text" }, "name": "shipName", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "shipLine1", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "shipLine2", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "shipCity", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "shipPostcode", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "shipCountry", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "placedAt", "nullable": false },
              ],
              "indexed_columns": ["shopper", "status", "placedAt"],
            },
            "payments": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "orderId",
                  "nullable": false,
                  "references": "orders",
                },
                {
                  "column_type": { "type": "Enum", "variants": ["sandbox", "stripe"] },
                  "name": "provider",
                  "nullable": false,
                },
                { "column_type": { "type": "Text" }, "name": "providerRef", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "clientSecret", "nullable": true },
                {
                  "column_type": {
                    "type": "Enum",
                    "variants": ["requires_payment", "succeeded", "failed"],
                  },
                  "name": "status",
                  "nullable": false,
                },
                { "column_type": { "type": "Integer" }, "name": "amountCents", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "failureReason", "nullable": true },
              ],
              "indexed_columns": ["orderId"],
            },
            "products": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "sku", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "slug", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "brand", "nullable": false },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "categoryId",
                  "nullable": false,
                  "references": "categories",
                },
                { "column_type": { "type": "Integer" }, "name": "priceCents", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "summary", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "description", "nullable": false },
                { "column_type": { "type": "Json" }, "name": "specs", "nullable": false },
                {
                  "column_type": {
                    "type": "Enum",
                    "variants": [
                      "gray",
                      "blue",
                      "cyan",
                      "green",
                      "orange",
                      "pink",
                      "purple",
                      "red",
                      "teal",
                      "yellow",
                    ],
                  },
                  "name": "hue",
                  "nullable": false,
                },
                {
                  "column_type": {
                    "type": "Enum",
                    "variants": [
                      "guitar",
                      "bass",
                      "keys",
                      "synth",
                      "drum",
                      "cymbal",
                      "mic",
                      "headphones",
                      "amp",
                      "pedal",
                      "cable",
                      "strings",
                      "picks",
                    ],
                  },
                  "name": "art",
                  "nullable": false,
                },
                { "column_type": { "type": "Text" }, "name": "searchText", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "position", "nullable": false },
              ],
              "indexed_columns": ["slug", "categoryId", "position"],
            },
            "stock": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "productId",
                  "nullable": false,
                  "references": "products",
                },
                { "column_type": { "type": "Integer" }, "name": "onHand", "nullable": false },
              ],
              "indexed_columns": ["productId"],
            },
          },
        },
      },
      {
        "hash": "f7318f0be5fcbf2065324e4f545c96323edfa4e007186fb89eabff519ca370d8",
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
            "cartLines": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "cartId",
                  "nullable": false,
                  "references": "carts",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "productId",
                  "nullable": false,
                  "references": "products",
                },
                { "column_type": { "type": "Integer" }, "name": "quantity", "nullable": false },
              ],
              "indexed_columns": ["cartId"],
            },
            "carts": {
              "columns": [
                { "column_type": { "type": "Uuid" }, "name": "shopper", "nullable": false },
                {
                  "column_type": { "type": "Enum", "variants": ["standard", "express"] },
                  "default": { "type": "Text", "value": "standard" },
                  "name": "shippingMethod",
                  "nullable": false,
                },
                { "column_type": { "type": "Text" }, "name": "shipName", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "shipLine1", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "shipLine2", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "shipCity", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "shipPostcode", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "shipCountry", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "checkoutKey", "nullable": true },
              ],
              "indexed_columns": ["shopper"],
            },
            "categories": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "slug", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "blurb", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "position", "nullable": false },
              ],
              "indexed_columns": ["slug", "position"],
            },
            "orderEvents": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "orderId",
                  "nullable": false,
                  "references": "orders",
                },
                {
                  "column_type": {
                    "type": "Enum",
                    "variants": ["placed", "paid", "payment_failed", "shipped"],
                  },
                  "name": "status",
                  "nullable": false,
                },
                { "column_type": { "type": "Text" }, "name": "note", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "at", "nullable": false },
              ],
              "indexed_columns": ["orderId", "at"],
            },
            "orderLines": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "orderId",
                  "nullable": false,
                  "references": "orders",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "productId",
                  "nullable": false,
                  "references": "products",
                },
                { "column_type": { "type": "Text" }, "name": "productName", "nullable": false },
                {
                  "column_type": { "type": "Integer" },
                  "name": "unitPriceCents",
                  "nullable": false,
                },
                { "column_type": { "type": "Integer" }, "name": "quantity", "nullable": false },
              ],
              "indexed_columns": ["orderId"],
            },
            "orders": {
              "columns": [
                { "column_type": { "type": "Uuid" }, "name": "shopper", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "code", "nullable": false },
                {
                  "column_type": {
                    "type": "Enum",
                    "variants": ["placed", "paid", "payment_failed", "shipped"],
                  },
                  "name": "status",
                  "nullable": false,
                },
                { "column_type": { "type": "Text" }, "name": "idempotencyKey", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "subtotalCents", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "shippingCents", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "totalCents", "nullable": false },
                {
                  "column_type": { "type": "Enum", "variants": ["standard", "express"] },
                  "name": "shippingMethod",
                  "nullable": false,
                },
                { "column_type": { "type": "Text" }, "name": "shipName", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "shipLine1", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "shipLine2", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "shipCity", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "shipPostcode", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "shipCountry", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "placedAt", "nullable": false },
              ],
              "indexed_columns": ["shopper", "status", "placedAt"],
            },
            "payments": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "orderId",
                  "nullable": false,
                  "references": "orders",
                },
                {
                  "column_type": { "type": "Enum", "variants": ["sandbox", "stripe"] },
                  "name": "provider",
                  "nullable": false,
                },
                { "column_type": { "type": "Text" }, "name": "providerRef", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "clientSecret", "nullable": true },
                {
                  "column_type": {
                    "type": "Enum",
                    "variants": ["requires_payment", "succeeded", "failed"],
                  },
                  "name": "status",
                  "nullable": false,
                },
                { "column_type": { "type": "Integer" }, "name": "amountCents", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "failureReason", "nullable": true },
              ],
              "indexed_columns": ["orderId"],
            },
            "products": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "sku", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "slug", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "brand", "nullable": false },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "categoryId",
                  "nullable": false,
                  "references": "categories",
                },
                { "column_type": { "type": "Integer" }, "name": "priceCents", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "summary", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "description", "nullable": false },
                { "column_type": { "type": "Json" }, "name": "specs", "nullable": false },
                {
                  "column_type": {
                    "type": "Enum",
                    "variants": [
                      "gray",
                      "blue",
                      "cyan",
                      "green",
                      "orange",
                      "pink",
                      "purple",
                      "red",
                      "teal",
                      "yellow",
                    ],
                  },
                  "name": "hue",
                  "nullable": false,
                },
                {
                  "column_type": {
                    "type": "Enum",
                    "variants": [
                      "guitar",
                      "bass",
                      "keys",
                      "synth",
                      "drum",
                      "cymbal",
                      "mic",
                      "headphones",
                      "amp",
                      "pedal",
                      "cable",
                      "strings",
                      "picks",
                    ],
                  },
                  "name": "art",
                  "nullable": false,
                },
                { "column_type": { "type": "Text" }, "name": "searchText", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "position", "nullable": false },
              ],
              "indexed_columns": ["slug", "categoryId", "position"],
            },
            "stock": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "productId",
                  "nullable": false,
                  "references": "products",
                },
                { "column_type": { "type": "Integer" }, "name": "onHand", "nullable": false },
              ],
              "indexed_columns": ["productId"],
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
        "fromHash": "5806d5c6e98c33ae41c6ba5b3da6cca0ca72d69467521c5381c1babe56adf6b0",
        "toHash": "f7318f0be5fcbf2065324e4f545c96323edfa4e007186fb89eabff519ca370d8",
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
    "cartLines": {
      "cart": { "one": "carts", "column": "cartId" },
      "product": { "one": "products", "column": "productId" },
    },
    "carts": { "cartLinesViaCart": { "many": "cartLines", "relation": "cart" } },
    "categories": { "productsViaCategory": { "many": "products", "relation": "category" } },
    "orderEvents": { "order": { "one": "orders", "column": "orderId" } },
    "orderLines": {
      "order": { "one": "orders", "column": "orderId" },
      "product": { "one": "products", "column": "productId" },
    },
    "orders": {
      "orderEventsViaOrder": { "many": "orderEvents", "relation": "order" },
      "orderLinesViaOrder": { "many": "orderLines", "relation": "order" },
      "paymentsViaOrder": { "many": "payments", "relation": "order" },
    },
    "payments": { "order": { "one": "orders", "column": "orderId" } },
    "products": {
      "cartLinesViaProduct": { "many": "cartLines", "relation": "product" },
      "category": { "one": "categories", "column": "categoryId" },
      "orderLinesViaProduct": { "many": "orderLines", "relation": "product" },
      "stockViaProduct": { "many": "stock", "relation": "product" },
    },
    "stock": { "product": { "one": "products", "column": "productId" } },
  },
});
