import type { ShippingMethod } from "@/schema";

/** Shared by the checkout UI (estimates) and the backend (the charged total). */
export const SHIPPING: Record<
  ShippingMethod,
  { label: string; description: string; cents: number }
> = {
  standard: { label: "Standard", description: "3–5 working days", cents: 500 },
  express: { label: "Express", description: "Next working day", cents: 1500 },
};

/** Standard shipping is free from this subtotal. */
export const FREE_STANDARD_FROM_CENTS = 15000;

export function shippingCents(method: ShippingMethod, subtotalCents: number): number {
  if (method === "standard" && subtotalCents >= FREE_STANDARD_FROM_CENTS) return 0;
  return SHIPPING[method].cents;
}

const money = new Intl.NumberFormat("en-US", { style: "currency", currency: "USD" });
export function formatMoney(cents: number): string {
  return money.format(cents / 100);
}
