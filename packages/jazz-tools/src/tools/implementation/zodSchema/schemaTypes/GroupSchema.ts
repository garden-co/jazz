import { Group } from "../../../coValues/group.js";
import {
  CoValueClass,
  ID,
  SubscribeListenerOptions,
  SubscribeRestArgs,
} from "../../../coValues/interfaces.js";
import {
  Account,
  Settled,
  ResolveQuery,
  ResolveQueryStrict,
  Loaded,
  loadCoValueWithoutMe,
  parseGroupCreateOptions,
  parseSubscribeRestArgs,
  subscribeToCoValueWithoutMe,
  AnonymousJazzAgent,
  co,
  isControlledAccount,
  TypeSym,
  RegisteredSchemas,
  CoreAccountSchema,
} from "../../../internal.js";
import { RawGroup } from "cojson";
import { CoreCoValueSchema } from "./CoValueSchema.js";
import { coOptionalDefiner } from "../zodCo.js";
import { CoOptionalSchema } from "./CoOptionalSchema.js";
import type { AccountRole, InviteSecret } from "cojson";

export interface CoreGroupSchema extends CoreCoValueSchema {
  builtin: "Group";
}

export function createCoreGroupSchema(): CoreGroupSchema {
  return {
    collaborative: true as const,
    builtin: "Group" as const,
    resolveQuery: true as const,
  };
}

export class GroupSchema implements CoreGroupSchema {
  readonly collaborative = true as const;
  readonly builtin = "Group" as const;
  readonly resolveQuery = true as const;

  optional(): CoOptionalSchema<this> {
    return coOptionalDefiner(this);
  }

  create(
    options?:
      | { owner: Loaded<CoreAccountSchema, true> }
      | Loaded<CoreAccountSchema, true>,
  ): Loaded<CoreGroupSchema> {
    const initOwner = parseGroupCreateOptions(options).owner;
    if (!initOwner) throw new Error("No owner provided");
    if (initOwner[TypeSym] === "Account" && isControlledAccount(initOwner)) {
      const rawOwner = initOwner.$jazz.raw;
      const raw = rawOwner.core.node.createGroup();
      return new Group(raw, this);
    } else {
      throw new Error("Can only construct group as a controlled account");
    }
  }

  fromRaw(raw: RawGroup): Group {
    return new Group(raw, this);
  }

  load<R extends ResolveQuery<CoreGroupSchema>>(
    id: ID<CoreGroupSchema>,
    options?: {
      loadAs?: Loaded<CoreAccountSchema, true> | AnonymousJazzAgent;
      resolve?: ResolveQueryStrict<CoreGroupSchema, R>;
    },
  ): Promise<Settled<CoreGroupSchema, R>> {
    return loadCoValueWithoutMe(this, id, options);
  }
  async createInvite<R extends ResolveQuery<CoreGroupSchema>>(
    id: ID<CoreGroupSchema>,
    options?: { role?: AccountRole; loadAs?: Loaded<CoreAccountSchema, true> },
  ): Promise<InviteSecret> {
    const group = await loadCoValueWithoutMe(this, id, {
      loadAs: options?.loadAs,
    });
    if (!group.$isLoaded) {
      throw new Error(`Group with id ${id} not found`);
    }
    return group.$jazz.createInvite(options?.role ?? "reader");
  }
  subscribe<R extends ResolveQuery<CoreGroupSchema>>(
    id: ID<CoreGroupSchema>,
    listener: (
      value: Loaded<CoreGroupSchema, R>,
      unsubscribe: () => void,
    ) => void,
  ): () => void;
  subscribe<R extends ResolveQuery<CoreGroupSchema>>(
    id: ID<CoreGroupSchema>,
    options: SubscribeListenerOptions<CoreGroupSchema, R>,
    listener: (
      value: Loaded<CoreGroupSchema, R>,
      unsubscribe: () => void,
    ) => void,
  ): () => void;
  subscribe<R extends ResolveQuery<CoreGroupSchema>>(
    id: ID<CoreGroupSchema>,
    ...args: SubscribeRestArgs<CoreGroupSchema, R>
  ): () => void {
    const { options, listener } = parseSubscribeRestArgs(args);
    return subscribeToCoValueWithoutMe(this, id, options, listener as any);
  }
}

RegisteredSchemas["Group"] = new GroupSchema();
