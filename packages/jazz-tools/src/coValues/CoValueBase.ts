import { ControlledAccount, RawAccount, type RawCoValue } from "cojson";
import { z } from "../implementation/zodSchema/zodReExport.js";
import {
  AnonymousJazzAgent,
  CoValue,
  CoValueClass,
  CoValueFromRaw,
  ID,
  RegisteredSchemas,
  accessChildById,
  anySchemaToCoSchema,
  coValuesCache,
  inspect,
} from "../internal.js";
import type {
  Account,
  CoValueClassFromZodSchema,
  Group,
  InstanceOfSchemaCoValuesNullable,
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
        anySchemaToCoSchema(RegisteredSchemas["Account"]).fromRaw(
          agent.account,
        ),
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
  castAs<
    S extends
      | CoValueClass
      | z.core.$ZodType
      | (z.core.$ZodObject<any, any> & {
          builtin: "Account";
          migration?: (account: any, creationProps?: { name: string }) => void;
        })
      | (z.core.$ZodCustom<any, any> & { builtin: "FileStream" })
      | (z.core.$ZodCustom<any, any> & {
          builtin: "CoFeed";
          element: z.core.$ZodType;
        }),
  >(
    schema: S,
  ): S extends CoValueClass
    ? InstanceType<S>
    : S extends z.core.$ZodType
      ? NonNullable<InstanceOfSchemaCoValuesNullable<S>>
      : never {
    const cl = "getCoSchema" in schema ? (schema as any).getCoSchema() : schema;

    if (this.constructor === cl) {
      return this as any;
    }

    return (cl as unknown as CoValueFromRaw<CoValue>).fromRaw(this._raw) as any;
  }
}
