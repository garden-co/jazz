import { CoID, RawCoValue } from "../coValue.js";
import {
  AvailableCoValueCore,
  CoValueCore,
} from "../coValueCore/coValueCore.js";
import {
  CoValueHeader,
  CoValueUniqueness,
} from "../coValueCore/verifiedState.js";
import {
  AgentSecret,
  CryptoProvider,
  SealerID,
  SealerSecret,
  SignerID,
  SignerSecret,
} from "../crypto/crypto.js";
import { AgentID } from "../ids.js";
import { JsonObject } from "../jsonValue.js";
import { LocalNode } from "../localNode.js";
import { logger } from "../logger.js";
import type { AccountRole } from "../permissions.js";
import { RawCoMap } from "./coMap.js";
import { InviteSecret, RawGroup } from "./group.js";

export function accountHeaderForInitialAgentSecret(
  agentSecret: AgentSecret,
  crypto: CryptoProvider,
): CoValueHeader {
  const agent = crypto.getAgentID(agentSecret);
  return {
    type: "comap",
    ruleset: { type: "group", initialAdmin: agent },
    meta: {
      type: "account",
    },
    createdAt: null,
    uniqueness: null,
  };
}

export type InvalidAccountAgentIDError = {
  type: "InvalidAccountAgentID";
  reason: string;
};

export class RawAccount<
  Meta extends AccountMeta = AccountMeta,
> extends RawGroup<Meta> {
  _cachedCurrentAgentID: AgentID | undefined;

  currentAgentID(): AgentID {
    if (this._cachedCurrentAgentID) {
      return this._cachedCurrentAgentID;
    }

    const agents = this.keys()
      .filter((k): k is AgentID => k.startsWith("sealer_"))
      .sort(
        (a, b) =>
          (this.lastEditAt(a)?.at.getTime() || 0) -
          (this.lastEditAt(b)?.at.getTime() || 0),
      );

    if (agents.length !== 1) {
      logger.warn("Account has " + agents.length + " agents", { id: this.id });
    }

    this._cachedCurrentAgentID = agents[0];

    return agents[0]!;
  }

  createInvite(_: AccountRole): InviteSecret {
    throw new Error("Cannot create invite from an account");
  }
}

export interface ControlledAccountOrAgent {
  id: RawAccountID | AgentID;
  agentSecret: AgentSecret;

  currentAgentID: () => AgentID;
  currentSignerID: () => SignerID;
  currentSignerSecret: () => SignerSecret;
  currentSealerID: () => SealerID;
  currentSealerSecret: () => SealerSecret;
}

/** @hidden */
export class ControlledAccount implements ControlledAccountOrAgent {
  account: RawAccount<AccountMeta>;
  agentSecret: AgentSecret;
  _cachedCurrentAgentID: AgentID | undefined;
  crypto: CryptoProvider;

  constructor(account: RawAccount<AccountMeta>, agentSecret: AgentSecret) {
    this.account = account;
    this.agentSecret = agentSecret;
    this.crypto = account.core.node.crypto;
  }

  get id(): RawAccountID {
    return this.account.id;
  }

  currentAgentID(): AgentID {
    if (this._cachedCurrentAgentID) {
      return this._cachedCurrentAgentID;
    }
    const agentID = this.crypto.getAgentID(this.agentSecret);
    this._cachedCurrentAgentID = agentID;
    return agentID;
  }

  currentSignerID() {
    return this.crypto.getAgentSignerID(this.currentAgentID());
  }

  currentSignerSecret(): SignerSecret {
    return this.crypto.getAgentSignerSecret(this.agentSecret);
  }

  currentSealerID() {
    return this.crypto.getAgentSealerID(this.currentAgentID());
  }

  currentSealerSecret(): SealerSecret {
    return this.crypto.getAgentSealerSecret(this.agentSecret);
  }
}

export class ControlledAgent implements ControlledAccountOrAgent {
  constructor(
    public agentSecret: AgentSecret,
    public crypto: CryptoProvider,
  ) {}

  get id(): AgentID {
    return this.crypto.getAgentID(this.agentSecret);
  }

  currentAgentID() {
    return this.crypto.getAgentID(this.agentSecret);
  }

  currentSignerID() {
    return this.crypto.getAgentSignerID(this.currentAgentID());
  }

  currentSignerSecret(): SignerSecret {
    return this.crypto.getAgentSignerSecret(this.agentSecret);
  }

  currentSealerID() {
    return this.crypto.getAgentSealerID(this.currentAgentID());
  }

  currentSealerSecret(): SealerSecret {
    return this.crypto.getAgentSealerSecret(this.agentSecret);
  }
}

export type AccountMeta = { type: "account" };
export type RawAccountID = CoID<RawAccount>;

export type ProfileShape = {
  name: string;
};

export class RawProfile<
  Shape extends ProfileShape = ProfileShape,
  Meta extends JsonObject | null = JsonObject | null,
> extends RawCoMap<Shape, Meta> {}

export type InboxShape = {
  inbox: string;
};

export class RawInbox<
  Shape extends InboxShape = InboxShape,
  Meta extends JsonObject | null = JsonObject | null,
> extends RawCoMap<Shape, Meta> {}

export type RawAccountMigration<Meta extends AccountMeta = AccountMeta> = (
  account: RawAccount<Meta>,
  localNode: LocalNode,
  creationProps?: { name: string },
) => void | Promise<void>;

export function expectAccount(content: RawCoValue): RawAccount {
  if (!(content instanceof RawAccount)) {
    throw new Error("Expected an account");
  }
  return content;
}
