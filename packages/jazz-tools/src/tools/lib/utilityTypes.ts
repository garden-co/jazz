/**
 * Make any property optional if its type includes `undefined`, preserving the type as-is
 */
export type PartialOnUndefined<T> = {
  [K in keyof T as undefined extends T[K] ? never : K]: T[K];
} & {
  [K in keyof T as undefined extends T[K] ? K : never]?: T[K];
};

/**
 * Useful to flatten the type output to improve type hints shown in editors.
 * And also to transform an interface into a type to aide with assignability.
 *
 * Taken from https://github.com/sindresorhus/type-fest/blob/main/source/simplify.d.ts
 */
export type Simplify<T> = { [KeyType in keyof T]: T[KeyType] } & {};

/**
 * Similar to {@link NonNullable}, but removes only `null` and preserves `undefined`.
 */
export type NotNull<T> = Exclude<T, null>;

/**
 * Used to check if T is a union type.
 *
 * If T is a union type, the left hand side of the extends becomes a union of function types.
 * The right hand side is always a single function type.
 */
export type IsUnion<T, U = T> = (
  T extends any
    ? (x: T) => void
    : never
) extends (x: U) => void
  ? false
  : true;
