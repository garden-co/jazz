import {
  CoValueUniqueness,
  CojsonInternalTypes,
  RawAccount,
  RawCoValue,
} from "cojson";
import { RegisteredSchemas } from "../coValues/registeredSchemas.js";
import { Account, Group } from "../exports.js";
import { activeAccountContext } from "../implementation/activeAccountContext.js";
import {
  AnonymousJazzAgent,
  RefsToResolve,
  RefsToResolveStrict,
  Resolved,
} from "../internal.js";
import { coValuesCache } from "../lib/cache.js";

/** @category Abstract interfaces */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export interface CoValue {
  /** @category Content */
  readonly id: ID<this>;
  /** @category Type Helpers */
  $type: string;
  /** @category Collaboration */
  $owner: Account | Group;
  $loadedAs: Account | AnonymousJazzAgent;

  /** @category Internals */
  $raw: RawCoValue;
}

export type ID<T> = CojsonInternalTypes.RawCoID & IDMarker<T>;

type IDMarker<out T> = { __type(_: never): T };

/** @internal */
export class CoValuePrototype implements CoValue {
  declare id: ID<this>;
  declare $type: string;
  declare $raw: RawCoValue;
  declare $schema: any;

  get $loadedAs(): Account | AnonymousJazzAgent {
    const rawAccount = this.$raw.core.node.account;

    if (rawAccount instanceof RawAccount) {
      return coValuesCache.get(rawAccount, () =>
        RegisteredSchemas["Account"].fromRaw(rawAccount),
      );
    }

    return new AnonymousJazzAgent(this.$raw.core.node);
  }

  get $owner(): Account | Group {
    return coValuesCache.get(this.$raw.group, () =>
      this.$raw.group instanceof RawAccount
        ? RegisteredSchemas["Account"].fromRaw(this.$raw.group)
        : RegisteredSchemas["Group"].fromRaw(this.$raw.group),
    );
  }
}
