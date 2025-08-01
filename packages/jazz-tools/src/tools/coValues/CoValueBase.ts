import { ControlledAccount, RawAccount, type RawCoValue } from "cojson";
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
  isCoValueSchema,
} from "../internal.js";
import type {
  Account,
  CoValueClassOrSchema,
  Group,
  InstanceOfSchemaCoValuesNullable,
} from "../internal.js";

/** @internal */

export abstract class CoValueBase implements CoValue {
  declare id: ID<this>;
  declare _type: string;
  declare _raw: RawCoValue;
  /** @category Internals */
  declare _instanceID: string;

  declare abstract $jazz: CoValueJazzApi<this>;

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  constructor(..._args: any) {
    Object.defineProperty(this, "_instanceID", {
      value: `instance-${Math.random().toString(36).slice(2)}`,
      enumerable: false,
    });
  }

  /** @category Internals */
  static fromRaw<V extends CoValue>(this: CoValueClass<V>, raw: RawCoValue): V {
    return new this({ fromRaw: raw });
  }
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  toJSON(): object | any[] | string {
    return {
      id: this.id,
      type: this._type,
      error: "unknown CoValue class",
    };
  }

  [inspect]() {
    return this.toJSON();
  }

  /** @category Type Helpers */
  castAs<S extends CoValueClassOrSchema>(
    schema: S,
  ): S extends CoValueClass
    ? InstanceType<S>
    : S extends CoreCoValueSchema
      ? NonNullable<InstanceOfSchemaCoValuesNullable<S>>
      : never {
    const cl = isCoValueSchema(schema) ? schema.getCoValueClass() : schema;

    if (this.constructor === cl) {
      return this as any;
    }

    return (cl as unknown as CoValueFromRaw<CoValue>).fromRaw(this._raw) as any;
  }
}

export class CoValueJazzApi<V extends CoValue> {
  constructor(private coValue: V) {}

  get owner(): Account | Group {
    const schema =
      this._raw.group instanceof RawAccount
        ? RegisteredSchemas["Account"]
        : RegisteredSchemas["Group"];

    return accessChildById(this.coValue, this._raw.group.id, {
      ref: schema,
      optional: false,
    });
  }

  /** @private */
  get loadedAs() {
    const agent = this._raw.core.node.getCurrentAgent();

    if (agent instanceof ControlledAccount) {
      return coValuesCache.get(agent.account, () =>
        coValueClassFromCoValueClassOrSchema(
          RegisteredSchemas["Account"],
        ).fromRaw(agent.account),
      );
    }

    return new AnonymousJazzAgent(this._raw.core.node);
  }

  get _raw() {
    return this.coValue._raw;
  }
}
