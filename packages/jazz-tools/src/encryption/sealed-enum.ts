/** An enum type as `jazz.app.json` and its encryption sidecar write it. */
export interface SealedEnumType {
  enum?: unknown;
  optional?: unknown;
}

/**
 * Whether `after` only appends variants to the enum `before`, with the same
 * nullability: the one change to an encrypted enum that `jazz-tools generate`
 * pins, keeping `before` as the column's `sealedType`.
 */
export function appendsEnumVariants(before: SealedEnumType, after: SealedEnumType): boolean {
  const sealed = before.enum;
  const declared = after.enum;
  return (
    Array.isArray(sealed) &&
    Array.isArray(declared) &&
    (before.optional ?? false) === (after.optional ?? false) &&
    sealed.length < declared.length &&
    sealed.every((variant, index) => variant === declared[index])
  );
}
