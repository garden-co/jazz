import { app } from "@/schema";
import { backend } from "@/src/server/backend";
import { CheckoutError, settlePayment, startPayment } from "@/src/server/orders";
import { configuredPaymentProvider } from "@/src/server/payments";
import { jsonRoute, requireShopper } from "@/src/server/shopper";
import { ensureStore } from "@/src/server/store";

export const runtime = "nodejs";

/**
 * Settle an order's payment: ask the provider what happened and record it.
 * Stripe is asked about the PaymentIntent the browser confirmed; the sandbox
 * applies the outcome the shopper picked. Safe to repeat.
 */
export async function POST(request: Request, context: { params: Promise<{ orderId: string }> }) {
  return jsonRoute(async () => {
    const { account } = await requireShopper(request);
    const { orderId } = await context.params;
    const body = (await request.json().catch(() => ({}))) as { sandboxOutcome?: unknown };
    const sandboxOutcome =
      body.sandboxOutcome === "approve" || body.sandboxOutcome === "decline"
        ? body.sandboxOutcome
        : undefined;
    await ensureStore();
    const { db } = await backend();
    const order = await db.one(app.orders.where({ id: orderId }), { tier: "global" });
    // Someone else's order is indistinguishable from a missing one.
    if (!order || order.shopper !== account) throw new CheckoutError("Order not found", 404);
    const provider = configuredPaymentProvider();
    await startPayment(db, provider, orderId);
    return { status: await settlePayment(db, provider, { orderId, sandboxOutcome }) };
  });
}
