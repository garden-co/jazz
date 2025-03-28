export type optionalKeys<T extends object> = {
  [k in keyof T]: undefined extends T[k] ? k : never;
}[keyof T];

export type requiredKeys<T extends object> = {
  [k in keyof T]: undefined extends T[k] ? never : k;
}[keyof T];

export type addQuestionMarks<T extends object, _O = any> = {
  [K in requiredKeys<T>]: T[K];
} & {
  [K in optionalKeys<T>]?: T[K];
} & { [k in keyof T]?: unknown };

export type identity<T> = T;
export type flatten<T> = T extends string
  ? T
  : identity<{ [k in keyof T]: T[k] }>;

type DEPTH_LIMIT = 5;

export type IsDepthLimit<CurrentDepth extends number[]> =
  DEPTH_LIMIT extends CurrentDepth["length"] ? true : false;

export type simplifyResolveQuery<R> = keyof R extends never ? true : flatten<R>;

export type readonly<T> = {
  readonly [K in keyof T]: T[K];
};
