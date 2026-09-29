import type { PaymentProvider } from "./provider";

const STRIPE_API = "https://api.stripe.com/v1";

type PaymentIntent = {
  id: string;
  client_secret: string;
  status: string;
  last_payment_error?: { message?: string } | null;
};

/**
 * Stripe in test mode, over its REST API. The server creates a PaymentIntent
 * with an Idempotency-Key; the browser confirms it with Stripe Elements; the
 * server then reads the PaymentIntent back and records the result.
 */
export function stripeProvider(secretKey: string): PaymentProvider {
  if (!secretKey.startsWith("sk_test_")) throw new Error("Jamazon only runs Stripe in test mode");
  async function call(path: string, init: RequestInit & { idempotencyKey?: string } = {}) {
    const headers = new Headers(init.headers);
    headers.set("authorization", `Bearer ${secretKey}`);
    if (init.idempotencyKey) headers.set("idempotency-key", init.idempotencyKey);
    const response = await fetch(`${STRIPE_API}${path}`, { ...init, headers });
    const body = (await response.json()) as PaymentIntent & { error?: { message?: string } };
    if (!response.ok) throw new Error(body.error?.message ?? `Stripe request failed (${response.status})`);
    return body;
  }
  return {
    name: "stripe",
    async createPayment({ orderId, amountCents, description, idempotencyKey }) {
      const form = new URLSearchParams({
        amount: String(amountCents),
        currency: "usd",
        description,
        "automatic_payment_methods[enabled]": "true",
        "metadata[order_id]": orderId,
      });
      const intent = await call("/payment_intents", {
        method: "POST",
        body: form,
        headers: { "content-type": "application/x-www-form-urlencoded" },
        idempotencyKey,
      });
      return { providerRef: intent.id, clientSecret: intent.client_secret };
    },
    async resolvePayment({ providerRef }) {
      const intent = await call(`/payment_intents/${encodeURIComponent(providerRef)}`);
      if (intent.status === "succeeded") return { status: "succeeded" };
      if (intent.last_payment_error)
        return {
          status: "failed",
          reason: intent.last_payment_error.message ?? "The card was declined.",
        };
      return { status: "pending" };
    },
  };
}
