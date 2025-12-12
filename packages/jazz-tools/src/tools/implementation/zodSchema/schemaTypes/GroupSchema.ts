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
  RefsToResolve,
  RefsToResolveStrict,
  Resolved,
  ResolveQuery,
  loadCoValueWithoutMe,
  parseSubscribeRestArgs,
  subscribeToCoValueWithoutMe,
  AnonymousJazzAgent,
  co,
} from "../../../internal.js";
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

  getCoValueClass(): typeof Group {
    return Group;
  }

  optional(): CoOptionalSchema<this> {
    return coOptionalDefiner(this);
  }

  create(options?: { owner: Account } | Account): Group {
    return Group.create(options);
  }

  load<G extends Group, R extends ResolveQuery<GroupSchema>>(
    id: ID<G>,
    options?: {
      loadAs?: Account | AnonymousJazzAgent;
      resolve?: RefsToResolveStrict<Group, R>;
    },
  ): Promise<Settled<Group>> {
    return loadCoValueWithoutMe(Group, id, options) as Promise<Settled<Group>>;
  }
  async createInvite<G extends Group>(
    id: ID<G>,
    options?: { role?: AccountRole; loadAs?: Account },
  ): Promise<InviteSecret> {
    const group = await loadCoValueWithoutMe(Group, id, {
      loadAs: options?.loadAs,
    });
    if (!group.$isLoaded) {
      throw new Error(`Group with id ${id} not found`);
    }
    return group.$jazz.createInvite(options?.role ?? "reader");
  }
  subscribe<G extends Group, const R extends RefsToResolve<G>>(
    id: ID<G>,
    listener: (value: Resolved<G, R>, unsubscribe: () => void) => void,
  ): () => void;
  subscribe<G extends Group, const R extends RefsToResolve<G>>(
    id: ID<G>,
    options: SubscribeListenerOptions<G, R>,
    listener: (value: Resolved<G, R>, unsubscribe: () => void) => void,
  ): () => void;
  subscribe<G extends Group, const R extends RefsToResolve<G>>(
    id: ID<G>,
    ...args: SubscribeRestArgs<G, R>
  ): () => void {
    const { options, listener } = parseSubscribeRestArgs(args);
    return subscribeToCoValueWithoutMe(
      Group as unknown as CoValueClass<G>,
      id,
      options,
      listener as any,
    );
  }
}
