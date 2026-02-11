import { JsonValue, RawCoMap } from "cojson";
import {
  Account,
  AnonymousJazzAgent,
  CoMapInit_DEPRECATED,
  CoValue,
  CoValueBase,
  CoValueClass,
  CoValueFromRaw,
  CoValueJazzApi,
  Group,
  ID,
  Settled,
  RefsToResolve,
  RefsToResolveStrict,
  Resolved,
  Simplify,
  SubscribeListenerOptions,
  SubscribeRestArgs,
  loadCoValueWithoutMe,
  parseSubscribeRestArgs,
  subscribeToCoValueWithoutMe,
} from "../internal.js";

/**
 * Extends `SchemaUnion` with a non-abstract constructor.
 */
export type SchemaUnionConcreteSubclass<V extends CoValue> =
  typeof SchemaUnion & CoValueClass<V>;

export type SchemaUnionDiscriminator<V extends CoValue> = (discriminable: {
  get(key: string): JsonValue | undefined;
}) => CoValueClass<V> & CoValueFromRaw<V>;

/**
 * SchemaUnion allows you to create union types of CoValues that can be discriminated at runtime.
 *
 * @category CoValues
 */
export abstract class SchemaUnion extends CoValueBase implements CoValue {
  static create<V extends CoValue>(
    this: CoValueClass<V>,
    init: Simplify<CoMapInit_DEPRECATED<V>>,
    owner: Account | Group,
  ): V {
    throw new Error("Not implemented");
  }

  /**
   * Create an instance from raw data. This is called internally and should not be used directly.
   * Use `co.discriminatedUnion(...)` to create union schemas instead.
   *
   * @internal
   */
  static fromRaw<V extends CoValue>(
    this: CoValueClass<V>,
    raw: V["$jazz"]["raw"],
  ): V {
    throw new Error("Not implemented");
  }

  /**
   * Load a `SchemaUnion` with a given ID, as a given account.
   *
   * @category Subscription & Loading
   */
  static load<M extends SchemaUnion, const R extends RefsToResolve<M> = true>(
    this: CoValueClass<M>,
    id: ID<M>,
    options?: {
      resolve?: RefsToResolveStrict<M, R>;
      loadAs?: Account | AnonymousJazzAgent;
      skipRetry?: boolean;
    },
  ): Promise<Settled<Resolved<M, R>>> {
    return loadCoValueWithoutMe(this, id, options);
  }

  /**
   * Load and subscribe to a `CoMap` with a given ID, as a given account.
   *
   * Automatically also subscribes to updates to all referenced/nested CoValues as soon as they are accessed in the listener.
   *
   * Returns an unsubscribe function that you should call when you no longer need updates.
   *
   * Also see the `useCoState` hook to reactively subscribe to a CoValue in a React component.
   *
   * @category Subscription & Loading
   */
  static subscribe<
    M extends SchemaUnion,
    const R extends RefsToResolve<M> = true,
  >(
    this: CoValueClass<M>,
    id: ID<M>,
    listener: (value: Resolved<M, R>, unsubscribe: () => void) => void,
  ): () => void;
  static subscribe<
    M extends SchemaUnion,
    const R extends RefsToResolve<M> = true,
  >(
    this: CoValueClass<M>,
    id: ID<M>,
    options: SubscribeListenerOptions<M, R>,
    listener: (value: Resolved<M, R>, unsubscribe: () => void) => void,
  ): () => void;
  static subscribe<M extends SchemaUnion, const R extends RefsToResolve<M>>(
    this: CoValueClass<M>,
    id: ID<M>,
    ...args: SubscribeRestArgs<M, R>
  ): () => void {
    const { options, listener } = parseSubscribeRestArgs(args);
    return subscribeToCoValueWithoutMe<M, R>(this, id, options, listener);
  }
}

/**
 * @internal
 * Create a SchemaUnion subclass from a discriminator function.
 */
export function schemaUnionClassFromDiscriminator<V extends CoValue>(
  discriminator: SchemaUnionDiscriminator<V>,
): SchemaUnionConcreteSubclass<V> {
  return class SchemaUnionClass extends SchemaUnion {
    declare $jazz: CoValueJazzApi<this>;

    static override create<V extends CoValue>(
      this: CoValueClass<V>,
      init: Simplify<CoMapInit_DEPRECATED<V>>,
      owner: Account | Group,
    ): V {
      const ResolvedClass = discriminator(new Map(Object.entries(init)));
      // @ts-expect-error - create is a static method in the CoMap class
      return ResolvedClass.create(init, owner);
    }

    static override fromRaw<T extends CoValue>(
      this: CoValueClass<T> & CoValueFromRaw<T>,
      raw: T["$jazz"]["raw"],
    ): T {
      const ResolvedClass = discriminator(
        raw as RawCoMap,
      ) as unknown as CoValueClass<T> & CoValueFromRaw<T>;
      return ResolvedClass.fromRaw(raw);
    }
  } as unknown as SchemaUnionConcreteSubclass<V>;
}
