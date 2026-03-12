import { createMiddleware, createServerFn } from "@tanstack/react-start";
import { getServerDb } from "@/lib/server-db";
import { deriveLocalPrincipalId } from "jazz-tools/backend";
import { app } from "../../schema/app";
import { APP_ID } from "@/config";

interface CheckoutItem {
  cart_item_id: string;
  product_id: string;
  quantity: number;
}

interface CheckoutInput {
  items: CheckoutItem[];
}

const localAuthRequestMiddleware = createMiddleware({ type: "request" }).server(
  ({ request, next }) => next({ context: { request } }),
);

const userDbMiddleware = createMiddleware({ type: "request" })
  .middleware([localAuthRequestMiddleware])
  .server(async ({ request, next }) => {
    const localMode = request.headers.get("X-Jazz-Local-Mode");
    const localToken = request.headers.get("X-Jazz-Local-Token");
    if ((localMode !== "anonymous" && localMode !== "demo") || !localToken) {
      throw new Error("Missing local auth headers");
    }
    const requesterId = await deriveLocalPrincipalId(APP_ID, localMode, localToken);

    const requesterDb = await getServerDb(localToken, localMode);

    return next({ context: { requesterId, requesterDb } });
  });

export const processCheckout = createServerFn({ method: "POST" })
  .middleware([userDbMiddleware])
  .inputValidator((input: unknown) => input as CheckoutInput)
  .handler(async ({ data, context: { requesterId, requesterDb } }) => {
    // Server identity for privileged writes (orders/stock)
    const db = await getServerDb();

    if (data.items.length === 0) {
      throw new Error("Cart is empty");
    }

    // Server reads the products table (globally readable) to validate stock + get prices
    const products = await db.all(app.products, { tier: "global" });
    const productMap = new Map(products.map((p) => [p.id, p]));

    console.log("[jamazon] products", products);

    const outOfStock: string[] = [];
    let totalCents = 0;
    let itemCount = 0;

    for (const item of data.items) {
      const product = productMap.get(item.product_id);
      if (!product) {
        throw new Error(`Product ${item.product_id} not found`);
      }
      if (product.in_stock < item.quantity) {
        outOfStock.push(product.name);
      }
      // Use server-side price (not client-supplied) to prevent tampering
      totalCents += item.quantity * product.price_cents;
      itemCount += item.quantity;
    }

    if (outOfStock.length > 0) {
      throw new Error(`Insufficient stock for: ${outOfStock.join(", ")}`);
    }

    // Create order
    const orderId = await db.insert(
      app.orders,
      {
        owner_id: requesterId,
        created_at: new Date(),
        total_cents: totalCents,
        item_count: itemCount,
      },
      { tier: "global" },
    );

    // Create order items + decrement stock
    for (const item of data.items) {
      const product = productMap.get(item.product_id)!;

      await db.insert(
        app.order_items,
        {
          order: orderId,
          product: item.product_id,
          quantity: item.quantity,
          unit_price_cents: product.price_cents,
        },
        { tier: "global" },
      );

      await db.update(
        app.products,
        item.product_id,
        { in_stock: product.in_stock - item.quantity },
        { tier: "global" },
      );
    }

    // Clear authoritative edge-visible cart rows for this requester.
    const edgeCartItems = await requesterDb.all(app.cart_items.where({ owner_id: requesterId }), {
      tier: "global",
    });
    for (const edgeItem of edgeCartItems) {
      await requesterDb.deleteFrom(app.cart_items, edgeItem.id, { tier: "global" });
    }

    return { order_id: orderId, total_cents: totalCents, item_count: itemCount };
  });
