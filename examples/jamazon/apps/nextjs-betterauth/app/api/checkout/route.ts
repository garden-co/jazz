import { backend } from "@/src/server/backend";
import { CheckoutError, placeOrder, startPayment } from "@/src/server/orders";
import { configuredPaymentProvider } from "@/src/server/payments";
import { jsonRoute, requireShopper } from "@/src/server/shopper";
import { ensureStore } from "@/src/server/store";

export const runtime = "nodejs";

/**
 * Place the shopper's cart as an order. The body names only the idempotency
 * key; the cart, address and prices are read at the authority. Repeating the
 * request returns the same order and the same payment.
 */
export async function POST(request: Request) {
  return jsonRoute(async () => {
    const { account } = await requireShopper(request);
    const body = (await request.json().catch(() => ({}))) as { idempotencyKey?: unknown };
    if (typeof body.idempotencyKey !== "string")
      throw new CheckoutError("Missing idempotency key", 400);
    await ensureStore();
    const { db } = await backend();
    const { orderId } = await placeOrder(db, { account, idempotencyKey: body.idempotencyKey });
    await startPayment(db, configuredPaymentProvider(), orderId);
    return { orderId };
  });
}
