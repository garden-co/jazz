/**
 * The seam between checkout and a payment service. Every call carries an
 * idempotency key derived from the order, so a retried request can never
 * create a second charge.
 */
export interface PaymentProvider {
  readonly name: "sandbox" | "stripe";
  /** Create (or, for a repeated key, return) the payment for one order. */
  createPayment(input: {
    orderId: string;
    amountCents: number;
    description: string;
    idempotencyKey: string;
  }): Promise<{ providerRef: string; clientSecret?: string }>;
  /**
   * Resolve what happened to a payment. Stripe is asked for the PaymentIntent
   * the browser confirmed; the sandbox applies the shopper's chosen outcome.
   */
  resolvePayment(input: {
    providerRef: string;
    sandboxOutcome?: SandboxOutcome;
  }): Promise<PaymentResolution>;
}

export type SandboxOutcome = "approve" | "decline";

export type PaymentResolution =
  | { status: "succeeded" }
  | { status: "failed"; reason: string }
  | { status: "pending" };
