import { CoValueUniqueness } from "cojson";
import { TypeOf, ZodTypeAny, z } from "zod";
import type { Account } from "../coValues/account.js";
import type { Group } from "../coValues/group.js";
import { parseCoValueCreateOptions } from "../internal.js";
import { CoMapInstanceClass } from "./coMap.js";

export type CoMapInnerSchema = {
  [key: string]: CoMap<any> | ZodTypeAny | "self";
};

export type CoMapSchema<D extends CoMap<any>> = D extends CoMap<infer S>
  ? S
  : never;

export type CoValueDefinition<S extends CoMapInnerSchema> = CoMap<S>;

export type Relations<D extends CoValueDefinition<any>> = D extends CoMap<
  infer S
>
  ? {
      [K in keyof S]: S[K] extends CoMap<any>
        ? S[K]
        : S[K] extends "self"
          ? S
          : never;
    }
  : never;

export type RelationsKeys<D extends CoValueDefinition<any>> =
  keyof Relations<D> & (string | number);

export type CoValue<
  D extends CoValueDefinition<any>,
  R extends RelationsToResolve<D>,
> = LoadedCoMap<D, R>;

export type RelationsToResolveStrict<
  T extends CoValueDefinition<any>,
  V,
> = V extends RelationsToResolve<T> ? RelationsToResolve<T> : V;

export type RelationsToResolve<
  D extends CoValueDefinition<any>,
  DepthLimit extends number = 5,
  CurrentDepth extends number[] = [],
> =
  | boolean
  | (DepthLimit extends CurrentDepth["length"]
      ? boolean
      : D extends CoMap<infer S>
        ?
            | {
                [K in keyof S]?: S[K] extends "self"
                  ? RelationsToResolve<D, DepthLimit, [0, ...CurrentDepth]>
                  : S[K] extends CoValueDefinition<any>
                    ? RelationsToResolve<S[K], DepthLimit, [0, ...CurrentDepth]>
                    : never;
              }
            | boolean
        : boolean);

type IsDepthLimit<
  Depth,
  DepthLimit extends number,
  CurrentDepth extends number[],
> = DepthLimit extends CurrentDepth["length"]
  ? true
  : Depth extends boolean | undefined
    ? true
    : keyof Depth extends never // Depth = {}
      ? true
      : false;

export type Loaded<
  D extends CoValueDefinition<any>,
  Depth extends RelationsToResolve<D>,
  DepthLimit extends number = 5,
  CurrentDepth extends number[] = [],
> = Depth extends never
  ? undefined
  : D extends CoMap<infer S>
    ? LoadedCoMap<CoMap<S>, Depth, DepthLimit, CurrentDepth>
    : undefined;

export type LoadedCoMap<
  D extends CoValueDefinition<any>,
  Depth extends RelationsToResolve<D>,
  DepthLimit extends number = 5,
  CurrentDepth extends number[] = [],
> = CoMapInstanceClass<D, Depth> &
  (D extends CoMap<infer S>
    ? IsDepthLimit<Depth, DepthLimit, CurrentDepth> extends true
      ? {
          [K in keyof S]: S[K] extends ZodTypeAny ? TypeOf<S[K]> : null;
        }
      : {
          [K in keyof S]: K extends keyof Depth
            ? S[K] extends "self"
              ? Depth[K] extends RelationsToResolve<D>
                ? Loaded<D, Depth[K], DepthLimit, [0, ...CurrentDepth]>
                : null
              : S[K] extends CoValueDefinition<any>
                ? Depth[K] extends RelationsToResolve<S[K]>
                  ? Loaded<S[K], Depth[K], DepthLimit, [0, ...CurrentDepth]>
                  : null
                : null
            : S[K] extends ZodTypeAny
              ? TypeOf<S[K]>
              : null;
        }
    : {});

export type CoMapInit<D extends CoMap<any>> = D extends CoMap<infer S>
  ? {
      [K in keyof S]?: S[K] extends ZodTypeAny
        ? TypeOf<S[K]>
        : S[K] extends CoMap<any>
          ? CoMapInit<S[K]> | CoMapInstanceClass<S[K], any> | undefined
          : never;
    }
  : {};

export type CoMapInitStrict<D extends CoMap<any>> = D extends CoMap<infer S>
  ? {
      [K in keyof S]: S[K] extends ZodTypeAny ? TypeOf<S[K]> : never;
    }
  : {};

type CoMapInitToRelationsToResolve<
  D,
  I,
  DepthLimit extends number = 5,
  CurrentDepth extends number[] = [],
> = DepthLimit extends CurrentDepth["length"]
  ? true
  : D extends CoMap<infer S>
    ? I extends CoMapInit<D>
      ? {
          [K in keyof S]: S[K] extends CoMap<any>
            ? I[K] extends undefined
              ? never
              : I[K] extends CoMapInit<S[K]> | CoMapInstanceClass<S[K], any>
                ? true
                : never
            : never;
        }
      : true
    : true;

export class CoMap<S extends CoMapInnerSchema> {
  protected schema: S;
  optional = true;

  constructor(schema: S) {
    this.schema = schema;
  }

  get(key: keyof S) {
    return this.schema[key];
  }

  keys() {
    return Object.keys(this.schema) as (keyof S)[];
  }

  create<I extends CoMapInit<CoMap<S>>>(
    init: I,
    options?:
      | {
          owner: Account | Group;
          unique?: CoValueUniqueness["uniqueness"];
        }
      | Account
      | Group,
  ) {
    const { owner, uniqueness } = parseCoValueCreateOptions(options);
    return CoMapInstanceClass.fromInit<CoMap<S>>(
      this,
      init,
      owner,
      uniqueness,
    ) as Loaded<CoMap<S>, CoMapInitToRelationsToResolve<CoMap<S>, I>>;
  }
}

function map<S extends CoMapInnerSchema>(schema: S) {
  return new CoMap(schema);
}

export function isRelationRef(
  descriptor: CoMap<any> | ZodTypeAny | "self",
): descriptor is CoMap<any> | "self" {
  return descriptor instanceof CoMap || descriptor === "self";
}

export const co = {
  map,
  string: z.string,
  number: z.number,
  boolean: z.boolean,
  object: z.object,
  union: z.union,
  intersection: z.intersection,
  tuple: z.tuple,
};
