import { Group } from "../../../coValues/group.js";
import {
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
} from "../../../internal.js";
import { CoreCoValueSchema } from "./CoValueSchema.js";
import { coOptionalDefiner } from "../zodCo.js";
import { CoOptionalSchema } from "./CoOptionalSchema.js";
import type { AccountRole, InviteSecret } from "cojson";
import { z } from "../zodReExport.js";

export interface CoreGroupSchema extends CoreCoValueSchema {
  builtin: "Group";
}

export function createCoreGroupSchema(): CoreGroupSchema {
  return {
    collaborative: true as const,
    builtin: "Group" as const,
    resolveQuery: true as const,
    getValidationSchema: () => z.any(),
  };
}

export class GroupSchema implements CoreGroupSchema {
  readonly collaborative = true as const;
  readonly builtin = "Group" as const;
  readonly resolveQuery = true as const;

  #validationSchema: z.ZodType | undefined = undefined;

  getValidationSchema = () => {
    if (this.#validationSchema) {
      return this.#validationSchema;
    }

    this.#validationSchema = z.instanceof(Group);
    return this.#validationSchema;
  };

  getCoValueClass(): typeof Group {
    return Group;
  }

  optional(): CoOptionalSchema<this> {
    return coOptionalDefiner(this);
  }

  create(options?: { owner: Account } | Account) {
    return Group.create(options);
  }

  load<G extends Group, R extends ResolveQuery<GroupSchema>>(
    id: ID<G>,
    options?: {
      loadAs?: Account;
      resolve?: RefsToResolveStrict<Group, R>;
    },
  ): Promise<Settled<Group>> {
    return Group.load(id, options);
  }
  createInvite<G extends Group>(
    id: ID<G>,
    options?: { role?: AccountRole; loadAs?: Account },
  ): Promise<InviteSecret> {
    return Group.createInvite(id, options);
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
    // @ts-expect-error
    return Group.subscribe<G, R>(id, ...args);
  }
}
