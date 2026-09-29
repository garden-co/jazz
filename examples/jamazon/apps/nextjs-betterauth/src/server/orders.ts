import type { Db } from "jazz-tools";
import { app, type Order, type OrderStatus, type Payment } from "@/schema";
import { ids, orderCode } from "@/src/lib/ids";
import { shippingCents } from "@/src/store/pricing";
import type { PaymentProvider, PaymentResolution, SandboxOutcome } from "./payments";

/**
 * The store's backend workflow. Every function here runs with backend
 * authority and is safe to call more than once with the same input:
 *
 * - an order's id is derived from (shopper account, idempotency key), and the
 *   exclusive transaction that creates it first looks for that id;
 * - a payment's id is derived from its order, and the provider call carries an
 *   idempotency key derived from the order as well;
 * - status events have one deterministic id per (order, status).
 *
 * So a double-clicked "Place order", a retried request after a timeout or two
 * racing workers all converge on one order, one charge and one timeline.
 */

export class CheckoutError extends Error {
  constructor(
    message: string,
    readonly status = 409,
  ) {
    super(message);
  }
}

export async function placeOrder(
  db: Db,
  input: { account: string; idempotencyKey: string; now?: Date },
): Promise<{ orderId: string; created: boolean }> {
  const { account, idempotencyKey } = input;
  if (!/^[0-9a-f-]{36}$/.test(idempotencyKey))
    throw new CheckoutError("Invalid idempotency key", 400);
  const orderId = ids.order(account, idempotencyKey);
  const cartId = ids.cart(account);

  return await retryConflicts(async () => {
    const write = await db.exclusiveTransaction(async (tx) => {
      // A retry of an accepted checkout returns the order it already made.
      if (await tx.one(app.orders.where({ id: orderId }))) return { orderId, created: false };

      const cart = await tx.one(app.carts.where({ id: cartId }));
      if (!cart || cart.shopper !== account) throw new CheckoutError("Your cart is empty.");
      // The key is minted when the shopper reviews the order. If the cart has
      // since moved on (another device placed it, or it was reviewed again),
      // this request no longer describes what the shopper saw.
      if (cart.checkoutKey !== idempotencyKey)
        throw new CheckoutError("Your cart changed since you reviewed it. Review it again.");
      const address = {
        shipName: cart.shipName?.trim(),
        shipLine1: cart.shipLine1?.trim(),
        shipLine2: cart.shipLine2?.trim() || undefined,
        shipCity: cart.shipCity?.trim(),
        shipPostcode: cart.shipPostcode?.trim(),
        shipCountry: cart.shipCountry?.trim(),
      };
      if (
        !address.shipName ||
        !address.shipLine1 ||
        !address.shipCity ||
        !address.shipPostcode ||
        !address.shipCountry
      )
        throw new CheckoutError("Add a shipping address before placing the order.");

      const lines = (await tx.all(app.cartLines.where({ cartId }))).filter((l) => l.quantity > 0);
      if (lines.length === 0) throw new CheckoutError("Your cart is empty.");
      const productIds = lines.map((line) => line.productId);
      const [products, stock] = await Promise.all([
        tx.all(app.products.where({ id: { in: productIds } })),
        tx.all(app.stock.where({ productId: { in: productIds } })),
      ]);

      let subtotalCents = 0;
      const priced = lines.map((line) => {
        const product = products.find((p) => p.id === line.productId);
        const level = stock.find((s) => s.productId === line.productId);
        if (!product || !level) throw new CheckoutError("An item in your cart is no longer sold.");
        if (line.quantity > level.onHand)
          throw new CheckoutError(
            level.onHand === 0
              ? `${product.name} is out of stock.`
              : `Only ${level.onHand} × ${product.name} left in stock.`,
          );
        // Prices come from the catalogue at the authority, never from the client.
        subtotalCents += product.priceCents * line.quantity;
        return { line, product, level };
      });
      const shipping = shippingCents(cart.shippingMethod, subtotalCents);
      const placedAt = input.now ?? new Date();

      for (const { line, level } of priced)
        tx.update(app.stock, level.id, { onHand: level.onHand - line.quantity });
      tx.insert(
        app.orders,
        {
          shopper: account,
          code: orderCode(orderId),
          status: "placed",
          idempotencyKey,
          subtotalCents,
          shippingCents: shipping,
          totalCents: subtotalCents + shipping,
          shippingMethod: cart.shippingMethod,
          shipName: address.shipName,
          shipLine1: address.shipLine1,
          shipLine2: address.shipLine2,
          shipCity: address.shipCity,
          shipPostcode: address.shipPostcode,
          shipCountry: address.shipCountry,
          placedAt,
        },
        { id: orderId },
      );
      for (const { line, product } of priced)
        tx.insert(
          app.orderLines,
          {
            orderId,
            productId: product.id,
            productName: product.name,
            unitPriceCents: product.priceCents,
            quantity: line.quantity,
          },
          { id: ids.orderLine(orderId, product.id) },
        );
      tx.insert(
        app.orderEvents,
        { orderId, status: "placed", note: "Order placed", at: placedAt },
        { id: ids.orderEvent(orderId, "placed") },
      );
      // Empty the cart. Lines are zeroed rather than deleted so that adding the
      // same product again later reuses the same deterministic row.
      for (const { line } of priced) tx.update(app.cartLines, line.id, { quantity: 0 });
      tx.update(app.carts, cartId, { checkoutKey: null });
      return { orderId, created: true };
    });
    return await write.wait();
  });
}

/** Create the order's payment with the configured provider, once. */
export async function startPayment(
  db: Db,
  provider: PaymentProvider,
  orderId: string,
): Promise<Payment> {
  const paymentId = ids.payment(orderId);
  const existing = await db.one(app.payments.where({ id: paymentId }), { tier: "global" });
  if (existing) return existing;
  const order = await db.one(app.orders.where({ id: orderId }), { tier: "global" });
  if (!order) throw new CheckoutError("Order not found", 404);

  // The provider sees the same key on every attempt: Stripe returns the same
  // PaymentIntent for a repeated Idempotency-Key, so a crash between this call
  // and the insert below cannot create a second charge.
  const created = await provider.createPayment({
    orderId,
    amountCents: order.totalCents,
    description: `Jamazon order ${order.code}`,
    idempotencyKey: `jamazon-payment-${orderId}`,
  });
  return await retryConflicts(async () => {
    const write = await db.exclusiveTransaction(async (tx) => {
      const raced = await tx.one(app.payments.where({ id: paymentId }));
      if (raced) return raced;
      return tx.insert(
        app.payments,
        {
          orderId,
          provider: provider.name,
          providerRef: created.providerRef,
          clientSecret: created.clientSecret,
          status: "requires_payment",
          amountCents: order.totalCents,
        },
        { id: paymentId },
      );
    });
    return await write.wait();
  });
}

/** Ask the provider what happened, then write the result back idempotently. */
export async function settlePayment(
  db: Db,
  provider: PaymentProvider,
  input: { orderId: string; sandboxOutcome?: SandboxOutcome; now?: Date },
): Promise<OrderStatus> {
  const payment = await db.one(app.payments.where({ id: ids.payment(input.orderId) }), {
    tier: "global",
  });
  if (!payment) throw new CheckoutError("Payment not started", 404);
  if (payment.status === "succeeded") return currentStatus(db, input.orderId);
  if (payment.provider !== provider.name)
    throw new CheckoutError(`This order is paid through ${payment.provider}.`);
  const resolution = await provider.resolvePayment({
    providerRef: payment.providerRef,
    sandboxOutcome: input.sandboxOutcome,
  });
  return await recordPayment(db, input.orderId, resolution, input.now);
}

export async function recordPayment(
  db: Db,
  orderId: string,
  resolution: PaymentResolution,
  now = new Date(),
): Promise<OrderStatus> {
  if (resolution.status === "pending") return currentStatus(db, orderId);
  return await retryConflicts(async () => {
    const write = await db.exclusiveTransaction(async (tx) => {
      const [order, payment] = await Promise.all([
        tx.one(app.orders.where({ id: orderId })),
        tx.one(app.payments.where({ id: ids.payment(orderId) })),
      ]);
      if (!order || !payment) throw new CheckoutError("Order not found", 404);
      // A succeeded payment is final: a late failure report or a duplicate
      // success changes nothing.
      if (payment.status === "succeeded") return order.status;
      if (resolution.status === "succeeded") {
        tx.update(app.payments, payment.id, { status: "succeeded", failureReason: null });
        tx.update(app.orders, orderId, { status: "paid" });
        await addEvent(tx, order, "paid", "Payment received", now);
        return "paid" as const;
      }
      tx.update(app.payments, payment.id, { status: "failed", failureReason: resolution.reason });
      tx.update(app.orders, orderId, { status: "payment_failed" });
      await addEvent(tx, order, "payment_failed", resolution.reason, now);
      return "payment_failed" as const;
    });
    return await write.wait();
  });
}

/** Mark a paid order shipped. Called by the fulfilment worker. */
export async function shipOrder(db: Db, orderId: string, now = new Date()): Promise<boolean> {
  return await retryConflicts(async () => {
    const write = await db.exclusiveTransaction(async (tx) => {
      const order = await tx.one(app.orders.where({ id: orderId }));
      if (!order || order.status !== "paid") return false;
      tx.update(app.orders, orderId, { status: "shipped" });
      await addEvent(tx, order, "shipped", "Left the Jamazon fulfilment centre", now);
      return true;
    });
    return await write.wait();
  });
}

type Tx = Parameters<Parameters<Db["exclusiveTransaction"]>[0]>[0];

async function addEvent(tx: Tx, order: Order, status: OrderStatus, note: string, at: Date) {
  const id = ids.orderEvent(order.id, status);
  // One event per (order, status): a payment that fails twice shows once.
  if (await tx.one(app.orderEvents.where({ id }))) return;
  tx.insert(app.orderEvents, { orderId: order.id, status, note, at }, { id });
}

async function currentStatus(db: Db, orderId: string): Promise<OrderStatus> {
  const order = await db.one(app.orders.where({ id: orderId }), { tier: "global" });
  if (!order) throw new CheckoutError("Order not found", 404);
  return order.status;
}

/**
 * Exclusive transactions are rejected when a concurrent write touched what
 * they read. Re-running is safe: every function above re-reads first.
 */
async function retryConflicts<T>(run: () => Promise<T>): Promise<T> {
  for (let attempt = 0; ; attempt++) {
    try {
      return await run();
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      if (
        attempt >= 20 ||
        !/exclusive_conflict|transaction_conflict|cascade_rejected/.test(message)
      )
        throw error;
    }
  }
}
