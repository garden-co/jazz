import type { PaymentProvider } from "./provider";

/**
 * A clearly labelled stand-in for a card processor, for local runs and tests.
 * It moves no money and holds no card data: the shopper picks the outcome.
 * Selected only through PAYMENT_PROVIDER (or the documented local default).
 */
export const sandboxProvider: PaymentProvider = {
  name: "sandbox",
  async createPayment({ idempotencyKey }) {
    // The reference is a pure function of the idempotency key, so creating the
    // same payment twice yields the same reference.
    return { providerRef: `sandbox_${idempotencyKey}` };
  },
  async resolvePayment({ sandboxOutcome }) {
    if (sandboxOutcome === "approve") return { status: "succeeded" };
    if (sandboxOutcome === "decline")
      return { status: "failed", reason: "The sandbox card was declined." };
    return { status: "pending" };
  },
};
