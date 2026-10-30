const currency = new Intl.NumberFormat("en-US", { style: "currency", currency: "USD" });

export function formatCents(cents: number): string {
  return currency.format(cents / 100);
}

/** A bounded count: "500+" when the read hit its cap. */
export function formatCount(count: number, cap: number): string {
  return count >= cap ? `${cap}+` : String(count);
}

export function newRequestKey(): string {
  return crypto.randomUUID();
}
