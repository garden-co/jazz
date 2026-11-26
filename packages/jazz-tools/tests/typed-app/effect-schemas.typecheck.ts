// jazz.effect.generated.ts's Schemas have exactly the types of
// jazz.generated.ts's, for an app with every column kind, typed JSON,
// enums, transforms and computed fields (apps/effect-schema). Typechecked
// by `build:test`; never run.
import type { Schema } from "effect";
import type * as Ts from "./apps/effect-schema/jazz.generated.js";
import * as Effect from "./apps/effect-schema/jazz.effect.generated.js";

/** `true` when each of `A` and `B` is assignable to the other. */
type Mutual<A, B> = [A] extends [B] ? ([B] extends [A] ? true : false) : false;

/** A Schema's `Type` and `Encoded` (rows decode as they are). */
type Both<S extends Schema.Top> = [Schema.Schema.Type<S>, Schema.Codec.Encoded<S>];

function assert<T extends true>(): T | void {}

/** Whether `S`'s Type and Encoded are both `T`. */
type Fits<S extends Schema.Top, T> =
  Mutual<Both<S>[0], T> extends true ? Mutual<Both<S>[1], T> : false;

assert<Fits<typeof Effect.Owner, Ts.Owner>>();
assert<Fits<typeof Effect.OwnerInsert, Ts.OwnerInsert>>();
assert<Fits<typeof Effect.Thing, Ts.Thing>>();
assert<Fits<typeof Effect.ThingInsert, Ts.ThingInsert>>();
assert<Fits<typeof Effect.ThingStatus, Ts.ThingStatus>>();
assert<Fits<typeof Effect.ThingMaybeStatus, Ts.ThingMaybeStatus>>();
assert<Fits<typeof Effect.ThingPoint, Ts.ThingPoint>>();
assert<Fits<typeof Effect.ThingPair, Ts.ThingPair>>();
assert<Fits<typeof Effect.ThingEither, Ts.ThingEither>>();
assert<Fits<typeof Effect.ThingCounts, Ts.ThingCounts>>();
assert<Fits<typeof Effect.ThingOpen, Ts.ThingOpen>>();
assert<Fits<typeof Effect.ThingLevel, Ts.ThingLevel>>();
assert<Fits<typeof Effect.ThingAnything, Ts.ThingAnything>>();
assert<Fits<typeof Effect.ThingMaybeLabel, Ts.ThingMaybeLabel>>();
assert<Fits<typeof Effect.ThingShape, Ts.ThingShape>>();

// Spot checks that the comparison has teeth.
assert<Mutual<Schema.Schema.Type<typeof Effect.Thing>["at"], Date>>();
assert<Mutual<Schema.Schema.Type<typeof Effect.Thing>["stamp"], string>>();
// @ts-expect-error a row has a required `id`; an insert has none.
assert<Mutual<Schema.Schema.Type<typeof Effect.Thing>, Ts.ThingInsert>>();
// @ts-expect-error a readonly array is not the bindings' mutable one.
assert<Mutual<ReadonlyArray<string>, Ts.Thing["tags"]>>();
