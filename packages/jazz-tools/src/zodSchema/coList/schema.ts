import { CoValueUniqueness, RawCoList } from "cojson";
import { TypeOf, ZodString, ZodTypeAny, z } from "zod";
import { Account } from "../../coValues/account.js";
import { Group } from "../../coValues/group.js";
import { parseCoValueCreateOptions } from "../../internal.js";
import { LazySchema } from "../coValue/lazy.js";
import {
  IsDepthLimit,
  SchemaOf,
  simplifyResolveQuery,
} from "../coValue/typeUtils.js";
import {
  CoValueInit,
  Loaded,
  LoadedCoList,
  ReolveQueryForCoInitChild,
  addNullable,
} from "../coValue/types.js";
import { CoValueResolutionNode } from "../subscribe.js";
import { CoValueSchema } from "../types.js";
import { createCoList, createCoListFromRaw } from "./instance.js";

export type CoListItem = CoValueSchema | ZodTypeAny | LazySchema<any>;

export type CoListInit<
  S extends AnyCoListSchema,
  CurrentDepth extends number[] = [],
> = IsDepthLimit<CurrentDepth> extends true
  ? "TOO DEEP"
  : Array<
      | (S["items"] extends ZodTypeAny
          ? TypeOf<S["items"]>
          : CoValueInit<SchemaOf<S["items"]>, [0, ...CurrentDepth]>)
      | addNullable<"nullable", S["items"]>
    >;

export type ResolveQueryForCoListInit<
  S extends AnyCoListSchema,
  I,
  CurrentDepth extends number[] = [],
> = IsDepthLimit<CurrentDepth> extends true
  ? true
  : S["items"] extends ZodTypeAny
    ? true
    : I extends CoListInit<S>
      ? {
          $each: simplifyResolveQuery<
            ReolveQueryForCoInitChild<
              S["items"],
              I[number],
              [0, ...CurrentDepth]
            >
          >;
        }
      : true;

export type CoListSchema<I extends CoListItem, O extends boolean = boolean> = {
  items: I;
  isOptional: O;

  _schema: CoListSchema<I, O>;
  _ID: { items: I }; // Used to identify the schema through the ID type

  get(): I;
};

export type AnyCoListSchema = CoListSchema<any, boolean>;

export class CoListSchemaClass<
  V extends CoListItem,
  O extends boolean = boolean,
> implements CoListSchema<V, O>
{
  items: V;
  isOptional: O;

  declare _schema: CoListSchema<V, O>;
  declare _ID: { items: V };
  declare _optionalType: CoListSchema<V, true>;

  constructor(items: V, isOptional: O) {
    this.items = items;
    this.isOptional = isOptional;
  }

  optional(): CoListSchemaClass<V, true> {
    return new CoListSchemaClass(this.items, true);
  }

  get(): V {
    return this.items;
  }

  create<I extends CoListInit<CoListSchema<V>>>(
    init: I,
    options?:
      | {
          owner: Account | Group;
          unique?: CoValueUniqueness["uniqueness"];
        }
      | Account
      | Group,
  ): LoadedCoList<
    CoListSchema<V, false>,
    ResolveQueryForCoListInit<CoListSchema<V>, I>,
    "non-nullable" // We want the loaded type to reflect the init input as we know for sure if values are available or not
  > {
    const { owner, uniqueness } = parseCoValueCreateOptions(options);
    return createCoList(this, init, owner, uniqueness) as any;
  }

  fromRaw(
    raw: RawCoList,
    refs?: any,
    resolutionNode?: CoValueResolutionNode<CoListSchema<V>>,
  ): any {
    return createCoListFromRaw(this, raw as any, refs, resolutionNode) as any;
  }
}
