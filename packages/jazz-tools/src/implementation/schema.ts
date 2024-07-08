import type { JsonValue, RawCoValue } from "cojson";
import {
    type CoValue,
    type CoValueClass,
    isCoValueClass,
    CoValueFromRaw,
} from "../internal.js";
import type { Schema as EffectSchema, TypeId } from "@effect/schema/Schema";

export type CoMarker = { readonly __co: unique symbol };
/** @category Schema definition */
export type co<T> = T | (T & CoMarker);
export type IfCo<C, R> = C extends infer _A | infer B
    ? B extends CoMarker
        ? R
        : never
    : never;
export type UnCo<T> = T extends co<infer A> ? A : T;

/** @category Schema definition */
export const co = {
    string: {
        [SchemaInit]: "json" satisfies Schema,
    } as unknown as co<string>,
    number: {
        [SchemaInit]: "json" satisfies Schema,
    } as unknown as co<number>,
    boolean: {
        [SchemaInit]: "json" satisfies Schema,
    } as unknown as co<boolean>,
    null: {
        [SchemaInit]: "json" satisfies Schema,
    } as unknown as co<null>,
    literal<T extends (string | number | boolean)[]>(
        ..._lit: T
    ): co<T[number]> {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        return { [SchemaInit]: "json" satisfies Schema } as any;
    },
    json<T extends JsonValue>(): co<T> {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        return { [SchemaInit]: "json" satisfies Schema } as any;
    },
    encoded<T>(arg: Encoder<T>): co<T> {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        return { [SchemaInit]: { encoded: arg } satisfies Schema } as any;
    },
    ref,
    items: ItemsSym as ItemsSym,
    members: MembersSym as MembersSym,
};

function ref<C extends CoValueClass>(
    arg: C | ((_raw: InstanceType<C>["_raw"]) => C),
): co<InstanceType<C> | null>;
function ref<C extends CoValueClass>(
    arg: C | ((_raw: InstanceType<C>["_raw"]) => C),
    options: { optional: true },
): co<InstanceType<C> | null | undefined>;
function ref<
    C extends CoValueClass,
    Options extends { optional?: boolean } | undefined,
>(
    arg: C | ((_raw: InstanceType<C>["_raw"]) => C),
    options?: Options,
): Options extends { optional: true }
    ? co<InstanceType<C> | null | undefined>
    : co<InstanceType<C> | null> {
    return {
        [SchemaInit]: {
            ref: arg,
            optional: options?.optional || false,
        } satisfies Schema,
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
    } as any;
}

export type JsonEncoded = "json";
export type EncodedAs<V> = { encoded: Encoder<V> };
export type RefEncoded<V extends CoValue> = {
    ref: CoValueClass<V> | ((raw: RawCoValue) => CoValueClass<V>);
    optional: boolean;
};

export function isRefEncoded<V extends CoValue>(
    schema: Schema,
): schema is RefEncoded<V> {
    return (
        typeof schema === "object" &&
        "ref" in schema &&
        "optional" in schema &&
        typeof schema.ref === "function"
    );
}

export function instantiateRefEncoded<V extends CoValue>(
    schema: RefEncoded<V>,
    raw: RawCoValue,
): V {
    return isCoValueClass<V>(schema.ref)
        ? schema.ref.fromRaw(raw)
        : (
              schema.ref as (
                  raw: RawCoValue,
              ) => CoValueClass<V> & CoValueFromRaw<V>
          )(raw).fromRaw(raw);
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type Schema = JsonEncoded | RefEncoded<CoValue> | EncodedAs<any>;

export type SchemaFor<Field> = NonNullable<Field> extends CoValue
    ? RefEncoded<NonNullable<Field>>
    : NonNullable<Field> extends JsonValue
      ? JsonEncoded
      : EncodedAs<NonNullable<Field>>;

export type EffectSchemaWithInputAndOutput<A, I = A> = EffectSchema<
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    any,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    any,
    never
> & {
    [TypeId]: {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        _A: (_: any) => A;
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        _I: (_: any) => I;
    };
};

export type Encoder<V> = EffectSchemaWithInputAndOutput<
    V,
    JsonValue | undefined
>;

import { Date } from "@effect/schema/Schema";
import { SchemaInit, ItemsSym, MembersSym } from "./symbols.js";

/** @category Schema definition */
export const Encoders = {
    Date,
};
