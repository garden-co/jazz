import { ControlledAccount, RawAccount, type RawCoValue } from "cojson";
import { CoreCoValueSchema } from "../implementation/zodSchema/schemaTypes/CoValueSchema.js";
import type {
  Account,
  CoValueClassOrSchema,
  Group,
  InstanceOfSchemaCoValuesNullable,
} from "../internal.js";
import {
  AnonymousJazzAgent,
  accessChildById,
  CoValue,
  CoValueClass,
  CoValueFromRaw,
  coValueClassFromCoValueClassOrSchema,
  coValuesCache,
  ID,
  inspect,
  isCoValueSchema,
  RegisteredSchemas,
} from "../internal.js";

/** @internal */

export class CoValueBase implements CoValue {
  declare id: ID<this>;
  declare _type: string;
  declare _raw: RawCoValue;
  /** @category Internals */
  declare _instanceID: string;

  get _owner(): Account | Group {
    const schema =
      this._raw.group instanceof RawAccount
        ? RegisteredSchemas["Account"]
        : RegisteredSchemas["Group"];

    return accessChildById(this, this._raw.group.id, {
      ref: schema,
      optional: false,
    });
  }

  /** @private */
  get _loadedAs() {
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
