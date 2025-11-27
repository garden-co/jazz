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
  MaybeLoaded,
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
 * @categoryDescription Declaration
 * Declare your union types by extending `SchemaUnion.Of(...)` and passing a discriminator function that determines which concrete type to use based on the raw data.
 *
 * ```ts
 * import { SchemaUnion, CoMap } from "jazz-tools";
 *
 * class BaseWidget extends CoMap {
 *   type = coField.string;
 * }
 *
 * class ButtonWidget extends BaseWidget {
 *   type = coField.literal("button");
 *   label = coField.string;
 * }
 *
 * class SliderWidget extends BaseWidget {
 *   type = coField.literal("slider");
 *   min = coField.number;
 *   max = coField.number;
 * }
 *
 * const WidgetUnion = SchemaUnion.Of<BaseWidget>((raw) => {
 *   switch (raw.get("type")) {
 *     case "button": return ButtonWidget;
 *     case "slider": return SliderWidget;
 *     default: throw new Error("Unknown widget type");
 *   }
 * });
 * ```
 *
 * @category CoValues
 */
export abstract class SchemaUnion extends CoValueBase implements CoValue {
  /**
   * Create a new union type from a discriminator function.
   *
   * The discriminator function receives the raw data and should return the appropriate concrete class to use for that data.
   *
   * When loading a SchemaUnion, the correct subclass will be instantiated based on the discriminator.
   *
   * @param discriminator - Function that determines which concrete type to use
   * @returns A new class that can create/load instances of the union type
   *
   * @example
   * ```ts
   * const WidgetUnion = SchemaUnion.Of<BaseWidget>((raw) => {
   *   switch (raw.get("type")) {
   *     case "button": return ButtonWidget;
   *     case "slider": return SliderWidget;
   *     default: throw new Error("Unknown widget type");
   *   }
   * });
   *
   * const widget = await loadCoValue(WidgetUnion, id, me, {});
   *
   * // You can narrow the returned instance to a subclass by using `instanceof`
   * if (widget instanceof ButtonWidget) {
   *   console.log(widget.label);
   * } else if (widget instanceof SliderWidget) {
   *   console.log(widget.min, widget.max);
   * }
   * ```
   *
   * @category Declaration
   **/
  static Of<V extends CoValue>(
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

  static create<V extends CoValue>(
    this: CoValueClass<V>,
    init: Simplify<CoMapInit_DEPRECATED<V>>,
    owner: Account | Group,
  ): V {
    throw new Error("Not implemented");
  }

  /**
   * Create an instance from raw data. This is called internally and should not be used directly.
   * Use {@link SchemaUnion.Of} to create a union type instead.
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
  ): Promise<MaybeLoaded<Resolved<M, R>>> {
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
