import { serverConfig } from "../config";
import type { PaymentProvider } from "./provider";
import { sandboxProvider } from "./sandbox";
import { stripeProvider } from "./stripe";

export type { PaymentProvider, PaymentResolution, SandboxOutcome } from "./provider";

/** The provider named by configuration. There is no fallback between them. */
export function configuredPaymentProvider(): PaymentProvider {
  return serverConfig.paymentProvider === "stripe"
    ? stripeProvider(serverConfig.stripeSecretKey!)
    : sandboxProvider;
}
