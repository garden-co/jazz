import { CoID, RawCoValue } from "../coValue.js";
import { CoValueHeader } from "../coValueCore/verifiedState.js";
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
import type { AccountRole, Role } from "../permissions.js";
import { RawCoMap } from "./coMap.js";
import { Everyone, EVERYONE, InviteSecret, RawGroup } from "./group.js";

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

  override createInvite(_: AccountRole): InviteSecret {
    throw new Error("Cannot create invite from an account");
  }

  override roleOfInternal(
    accountID: RawAccountID | AgentID | typeof EVERYONE,
  ): Role | undefined {
    if (accountID === this.id) {
      return "admin";
    }
    return super.roleOfInternal(accountID);
  }

  override addMember(
    account: RawAccount | ControlledAccountOrAgent | Everyone,
    role: Role,
  ) {
    throw new Error("Cannot add a member to an account");
  }

  override removeMember(
    account: RawAccount | ControlledAccountOrAgent | Everyone,
  ) {
    throw new Error("Cannot remove a member from an account");
  }

  override extend(
    parent: RawGroup,
    role: "reader" | "writer" | "admin" | "inherit" = "inherit",
  ) {
    throw new Error("Cannot extend an account");
  }

  override revokeExtend(parent: RawGroup) {
    throw new Error("Cannot unextend an account");
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
  _cachedCurrentSignerID: SignerID | undefined;
  _cachedCurrentSignerSecret: SignerSecret | undefined;
  _cachedCurrentSealerID: SealerID | undefined;
  _cachedCurrentSealerSecret: SealerSecret | undefined;
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
    if (this._cachedCurrentSignerID) {
      return this._cachedCurrentSignerID;
    }
    const signerID = this.crypto.getAgentSignerID(this.currentAgentID());
    this._cachedCurrentSignerID = signerID;
    return signerID;
  }

  currentSignerSecret(): SignerSecret {
    if (this._cachedCurrentSignerSecret) {
      return this._cachedCurrentSignerSecret;
    }
    const signerSecret = this.crypto.getAgentSignerSecret(this.agentSecret);
    this._cachedCurrentSignerSecret = signerSecret;
    return signerSecret;
  }

  currentSealerID() {
    if (this._cachedCurrentSealerID) {
      return this._cachedCurrentSealerID;
    }
    const sealerID = this.crypto.getAgentSealerID(this.currentAgentID());
    this._cachedCurrentSealerID = sealerID;
    return sealerID;
  }

  currentSealerSecret(): SealerSecret {
    if (this._cachedCurrentSealerSecret) {
      return this._cachedCurrentSealerSecret;
    }
    const sealerSecret = this.crypto.getAgentSealerSecret(this.agentSecret);
    this._cachedCurrentSealerSecret = sealerSecret;
    return sealerSecret;
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
