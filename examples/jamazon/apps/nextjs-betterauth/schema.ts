import { schema as s } from "jazz-tools";
import { tables as betterAuthSchema } from "./schema-better-auth/schema";
import { ART, HUES } from "./app-extras";

export const schema = s.defineSchema({
  ...betterAuthSchema,

  // ── Catalogue: public, written only by the store backend ──────────────
  categories: s
    .table(
      { slug: s.string(), name: s.string(), blurb: s.string(), position: s.int() },
      { productsViaCategory: s.reverse("products", "category") },
    )
    .indexOnly(["slug", "position"]),
  products: s
    .table(
      {
        // SKUs are shared with Jamazon Warehouse (JAM-001 is the same strings).
        sku: s.string(),
        slug: s.string(),
        name: s.string(),
        brand: s.string(),
        categoryId: s.uuid(),
        priceCents: s.int(),
        summary: s.string(),
        description: s.string(),
        specs: s.json(),
        hue: s.enum(...HUES),
        art: s.enum(...ART),
        // Lower-case name, brand and category: `contains` is case-sensitive.
        searchText: s.string(),
        position: s.int(),
      },
      {
        category: s.rel("categories", "categoryId"),
        stockViaProduct: s.reverse("stock", "product"),
        cartLinesViaProduct: s.reverse("cartLines", "product"),
        orderLinesViaProduct: s.reverse("orderLines", "product"),
      },
    )
    .indexOnly(["slug", "categoryId", "position"]),
  // Stock lives beside the product so an order changes one small row, not
  // the catalogue entry every shopper is subscribed to.
  stock: s
    .table({ productId: s.uuid(), onHand: s.int() }, { product: s.rel("products", "productId") })
    .indexOnly(["productId"]),

  // ── Carts: private to one shopper account, editable offline ───────────
  carts: s
    .table(
      {
        shopper: s.uuid(),
        shippingMethod: s.enum("standard", "express").default("standard"),
        shipName: s.string().optional(),
        shipLine1: s.string().optional(),
        shipLine2: s.string().optional(),
        shipCity: s.string().optional(),
        shipPostcode: s.string().optional(),
        shipCountry: s.string().optional(),
        // Minted when the shopper reaches the review step and reused for every
        // retry of "Place order", so a retry cannot create a second order.
        checkoutKey: s.string().optional(),
      },
      { cartLinesViaCart: s.reverse("cartLines", "cart") },
    )
    .indexOnly(["shopper"]),
  // One row per (cart, product). Its id is derived from both, so two devices
  // that add the same product offline write to the same row and converge.
  cartLines: s
    .table(
      { cartId: s.uuid(), productId: s.uuid(), quantity: s.int() },
      { cart: s.rel("carts", "cartId"), product: s.rel("products", "productId") },
    )
    .indexOnly(["cartId"]),

  // ── Orders: readable by their shopper, written only by the backend ────
  orders: s
    .table(
      {
        shopper: s.uuid(),
        code: s.string(),
        status: s.enum("placed", "paid", "payment_failed", "shipped"),
        idempotencyKey: s.string(),
        subtotalCents: s.int(),
        shippingCents: s.int(),
        totalCents: s.int(),
        shippingMethod: s.enum("standard", "express"),
        shipName: s.string(),
        shipLine1: s.string(),
        shipLine2: s.string().optional(),
        shipCity: s.string(),
        shipPostcode: s.string(),
        shipCountry: s.string(),
        placedAt: s.timestamp(),
      },
      {
        orderLinesViaOrder: s.reverse("orderLines", "order"),
        orderEventsViaOrder: s.reverse("orderEvents", "order"),
        paymentsViaOrder: s.reverse("payments", "order"),
      },
    )
    .indexOnly(["shopper", "status", "placedAt"]),
  orderLines: s
    .table(
      {
        orderId: s.uuid(),
        productId: s.uuid(),
        // Snapshots: later catalogue edits must not rewrite a past order.
        productName: s.string(),
        unitPriceCents: s.int(),
        quantity: s.int(),
      },
      { order: s.rel("orders", "orderId"), product: s.rel("products", "productId") },
    )
    .indexOnly(["orderId"]),
  orderEvents: s
    .table(
      {
        orderId: s.uuid(),
        status: s.enum("placed", "paid", "payment_failed", "shipped"),
        note: s.string(),
        at: s.timestamp(),
      },
      { order: s.rel("orders", "orderId") },
    )
    .indexOnly(["orderId", "at"]),
  payments: s
    .table(
      {
        orderId: s.uuid(),
        provider: s.enum("sandbox", "stripe"),
        providerRef: s.string(),
        // Stripe's PaymentIntent client secret is meant for the paying
        // browser; row permissions keep it with the order's shopper.
        clientSecret: s.string().optional(),
        status: s.enum("requires_payment", "succeeded", "failed"),
        amountCents: s.int(),
        failureReason: s.string().optional(),
      },
      { order: s.rel("orders", "orderId") },
    )
    .indexOnly(["orderId"]),
});

const { definePermissions, allowedTo, session, allOf, gte, lte } = schema;

const shopperIsMe = { shopper: session.user };

export const permissions = definePermissions({
  better_auth_account: {
    read: false,
    insert: false,
    update: false,
    delete: false,
  },
  better_auth_jwks: {
    read: false,
    insert: false,
    update: false,
    delete: false,
  },
  better_auth_session: {
    read: false,
    insert: false,
    update: false,
    delete: false,
  },
  better_auth_user: {
    read: false,
    insert: false,
    update: false,
    delete: false,
  },
  better_auth_verification: {
    read: false,
    insert: false,
    update: false,
    delete: false,
  },
  cartLines: {
    read: (cartLine) => allowedTo.read(cartLine.cart),
    insert: (cartLine) =>
      allOf(allowedTo.update(cartLine.cart), allOf({ quantity: gte(0) }, { quantity: lte(20) })),
    updateIf: (cartLine) => allowedTo.update(cartLine.cart),
    updateTo: (cartLine) =>
      allOf(allowedTo.update(cartLine.cart), allOf({ quantity: gte(0) }, { quantity: lte(20) })),
    delete: (cartLine) => allowedTo.update(cartLine.cart),
  },
  carts: {
    read: shopperIsMe,
    insert: shopperIsMe,
    update: shopperIsMe,
    delete: shopperIsMe,
  },
  categories: {
    read: true,
    insert: false,
    update: false,
    delete: false,
  },
  orderEvents: {
    read: (orderEvent) => allowedTo.read(orderEvent.order),
    insert: false,
    update: false,
    delete: false,
  },
  orderLines: {
    read: (orderLine) => allowedTo.read(orderLine.order),
    insert: false,
    update: false,
    delete: false,
  },
  orders: {
    read: shopperIsMe,
    insert: false,
    update: false,
    delete: false,
  },
  payments: {
    read: (payment) => allowedTo.read(payment.order),
    insert: false,
    update: false,
    delete: false,
  },
  products: {
    read: true,
    insert: false,
    update: false,
    delete: false,
  },
  stock: {
    read: true,
    insert: false,
    update: false,
    delete: false,
  },
});
