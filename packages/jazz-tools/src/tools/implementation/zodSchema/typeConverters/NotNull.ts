/**
 * Similar to {@link NonNullable}, but removes only `null` and preserves `undefined`.
 */
export type NotNull<T> = T extends null ? never : T;
