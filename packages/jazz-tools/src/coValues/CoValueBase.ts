import { ControlledAccount, RawAccount, type RawCoValue } from "cojson";
import {
  AnonymousJazzAgent,
  CoValue,
  CoValueClass,
  CoValueFromRaw,
  ID,
  RegisteredSchemas,
  anySchemaToCoSchema,
  coValuesCache,
  inspect,
} from "../internal.js";
import type { Account, Group } from "../internal.js";

/** @internal */

export class CoValueBase implements CoValue {
  declare id: ID<this>;
  declare _type: string;
  declare _raw: RawCoValue;
  /** @category Internals */
  declare _instanceID: string;

  get _owner(): Account | Group {
    const owner = coValuesCache.get(this._raw.group, () =>
      this._raw.group instanceof RawAccount
        ? anySchemaToCoSchema(RegisteredSchemas["Account"]).fromRaw(
            this._raw.group,
          )
        : RegisteredSchemas["Group"].fromRaw(this._raw.group),
    );

    return owner;
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
  castAs<Cl extends CoValueClass & CoValueFromRaw<CoValue>>(
    cl: Cl,
  ): InstanceType<Cl> {
    return cl.fromRaw(this._raw) as InstanceType<Cl>;
  }
}
