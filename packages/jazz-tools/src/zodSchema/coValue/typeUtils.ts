export type optionalKeys<T extends object> = {
  [k in keyof T]: undefined extends T[k] ? k : never;
}[keyof T];

export type requiredKeys<T extends object> = {
  [k in keyof T]: undefined extends T[k] ? never : k;
}[keyof T];

export type addQuestionMarks<T extends object> = {
  [K in requiredKeys<T>]: T[K];
} & {
  [K in optionalKeys<T>]?: T[K];
} & { [k in keyof T]?: unknown };

export type identity<T> = T;
export type flatten<T> = T extends string
  ? T
  : identity<{ [k in keyof T]: T[k] }>;

// type DEPTH_LIMIT = 10;
type DEPTH_LIMIT = 5;

export type IsDepthLimit<CurrentDepth extends number[]> =
  DEPTH_LIMIT extends CurrentDepth["length"] ? true : false;

export type validResolveKeys<T> = T extends true
  ? never
  : {
      [K in keyof T]: T[K] extends never ? never : K;
    }[keyof T];

export type simplifyResolveQuery<R> = validResolveKeys<R> extends never
  ? true
  : identity<{ [K in keyof R]: simplifyResolveQuery<R[K]> }>;

export type extensibleResolveQuery<R> = R extends true
  ? {}
  : identity<{ [K in keyof R]: extensibleResolveQuery<R[K]> }>;

export type SchemaOf<T extends { _schema: any }> = T["_schema"];
export type ResolveQueryOf<T extends { $jazz: { _resolveQuery: any } }> =
  simplifyResolveQuery<T["$jazz"]["_resolveQuery"]>;
