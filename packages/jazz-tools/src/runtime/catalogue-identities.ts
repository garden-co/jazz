/** @internal Names to resolve during one schema/session preparation operation. */
export type CatalogueIdentityRequest = {
  readonly table: string;
  readonly columns?: readonly string[];
};

/** @internal Structural identities, not a reusable freshness/permission token. */
export type PreparedCatalogueIdentities = {
  readonly table: string;
  readonly identity: string | null;
  readonly columns: Readonly<Record<string, string | null>>;
};

/** @internal The host ABI admits only JavaScript-safe millisecond budgets. */
export function validateCatalogueTimeout(timeoutMs: number): void {
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 0)
    throw new RangeError("Catalogue timeout must be a nonnegative safe integer");
}
