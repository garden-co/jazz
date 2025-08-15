import {
  ControlledAccount,
  LocalNode,
  RawAccount,
  type RawCoValue,
} from "cojson";
import { CoreCoValueSchema } from "../implementation/zodSchema/schemaTypes/CoValueSchema.js";
import {
  AnonymousJazzAgent,
  CoValue,
  CoValueClass,
  CoValueFromRaw,
  ID,
  RegisteredSchemas,
  accessChildById,
  coValueClassFromCoValueClassOrSchema,
  coValuesCache,
  inspect,
} from "../internal.js";
import {
  Account,
  CoValueClassOrSchema,
  Group,
  InstanceOfSchemaCoValuesNullable,
  TypeSym,
} from "../internal.js";

/** @internal */
export abstract class CoValueBase implements CoValue {
  declare [TypeSym]: string;

  declare abstract $jazz: CoValueJazzApi<this>;

  /** @category Internals */
  static fromRaw<V extends CoValue>(this: CoValueClass<V>, raw: RawCoValue): V {
    return new this({ fromRaw: raw });
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  toJSON(): object | any[] | string {
    return {
      id: this.$jazz.id,
      type: this[TypeSym],
      error: "unknown CoValue class",
    };
  }

  [inspect]() {
    return this.toJSON();
  }
}

export abstract class CoValueJazzApi<V extends CoValue> {
  /** @category Internals */
  declare _instanceID: string;

  constructor(private coValue: V) {
    Object.defineProperty(this, "_instanceID", {
      value: `instance-${Math.random().toString(36).slice(2)}`,
      enumerable: false,
    });
  }

  abstract get id(): ID<V>;
  abstract get raw(): RawCoValue;

  get owner(): Account | Group {
    const schema =
      this.raw.group instanceof RawAccount
        ? RegisteredSchemas["Account"]
        : RegisteredSchemas["Group"];

    return accessChildById(this.coValue, this.raw.group.id, {
      ref: schema,
      optional: false,
    });
  }

  /** @internal */
  get localNode(): LocalNode {
    return this.raw.core.node;
  }

  /** @private */
  get loadedAs() {
    const agent = this.localNode.getCurrentAgent();

    if (agent instanceof ControlledAccount) {
      return coValuesCache.get(agent.account, () =>
        coValueClassFromCoValueClassOrSchema(
          RegisteredSchemas["Account"],
        ).fromRaw(agent.account),
      );
    }

    return new AnonymousJazzAgent(this.localNode);
  }

  /** @category Type Helpers */
  castAs<S extends CoValueClassOrSchema>(
    schema: S,
  ): S extends CoValueClass
    ? InstanceType<S>
    : S extends CoreCoValueSchema
      ? NonNullable<InstanceOfSchemaCoValuesNullable<S>>
      : never {
    const cl = coValueClassFromCoValueClassOrSchema(schema);

    if (this.coValue.constructor === cl) {
      return this.coValue as any;
    }

    return (cl as unknown as CoValueFromRaw<CoValue>).fromRaw(this.raw) as any;
  }
}
