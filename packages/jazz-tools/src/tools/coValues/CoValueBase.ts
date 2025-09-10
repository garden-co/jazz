import { ControlledAccount, LocalNode, type RawCoValue } from "cojson";
import { CoreCoValueSchema } from "../implementation/zodSchema/schemaTypes/CoValueSchema.js";
import {
  AnonymousJazzAgent,
  CoValue,
  CoValueClass,
  CoValueFromRaw,
  ID,
  RegisteredSchemas,
  coValueClassFromCoValueClassOrSchema,
  coValuesCache,
  inspect,
} from "../internal.js";
import {
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
  abstract get owner(): Group | undefined;

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

  /**
   * The timestamp of the creation time of the CoValue
   *
   * @category Content
   */
  get createdAt(): number {
    const createdAt = this.raw.core.verified.header.meta?.createdAt;

    if (typeof createdAt === "string") {
      return new Date(createdAt).getTime();
    }

    return this.raw.core.earliestTxMadeAt;
  }

  /**
   * The timestamp of the last updated time of the CoValue
   *
   * Returns the creation time if there are no updates.
   *
   * @category Content
   */
  get lastUpdatedAt(): number {
    const value = this.raw.core.latestTxMadeAt;

    if (value === 0) {
      return this.createdAt;
    }

    return value;
  }
}
