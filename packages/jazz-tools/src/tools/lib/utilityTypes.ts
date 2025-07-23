/**
 * Make any property optional if its type includes `undefined`, preserving the type as-is
 */
export type OptionalizeUndefinedKeys<T> = {
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
