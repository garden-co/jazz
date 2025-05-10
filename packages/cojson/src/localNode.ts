import { Result, err, ok } from "neverthrow";
import { CoID } from "./coValue.js";
import { RawCoValue } from "./coValue.js";
import {
  AvailableCoValueCore,
  CoValueCore,
  idforHeader,
} from "./coValueCore/coValueCore.js";
import {
  CoValueHeader,
  CoValueUniqueness,
  VerifiedState,
} from "./coValueCore/verifiedState.js";
import {
  AccountMeta,
  ControlledAccount,
  ControlledAccountOrAgent,
  ControlledAgent,
  InvalidAccountAgentIDError,
  RawProfile as Profile,
  RawAccount,
  RawAccountID,
  RawAccountMigration,
  RawProfile,
  accountHeaderForInitialAgentSecret,
  expectAccount,
} from "./coValues/account.js";
import {
  InviteSecret,
  RawGroup,
  secretSeedFromInviteSecret,
} from "./coValues/group.js";
import { AgentSecret, CryptoProvider } from "./crypto/crypto.js";
import { AgentID, RawCoID, SessionID, isAgentID } from "./ids.js";
import { logger } from "./logger.js";
import { Peer, PeerID, SyncManager } from "./sync.js";
import { accountOrAgentIDfromSessionID } from "./typeUtils/accountOrAgentIDfromSessionID.js";
import { expectGroup } from "./typeUtils/expectGroup.js";

/** A `LocalNode` represents a local view of a set of loaded `CoValue`s, from the perspective of a particular account (or primitive cryptographic agent).

A `LocalNode` can have peers that it syncs to, for example some form of local persistence, or a sync server, such as `cloud.jazz.tools` (Jazz Cloud).

@example
You typically get hold of a `LocalNode` using `jazz-react`'s `useJazz()`:

```typescript
const { localNode } = useJazz();
```
*/
export class LocalNode {
  /** @internal */
  crypto: CryptoProvider;
  /** @internal */
  private readonly coValues = new Map<RawCoID, CoValueCore>();

  /** @category 3. Low-level */
  readonly currentSessionID: SessionID;
  readonly agentSecret: AgentSecret;

  /** @category 3. Low-level */
  syncManager = new SyncManager(this);

  crashed: Error | undefined = undefined;

  /** @category 3. Low-level */
  constructor(
    agentSecret: AgentSecret,
    currentSessionID: SessionID,
    crypto: CryptoProvider,
  ) {
    this.agentSecret = agentSecret;
    this.currentSessionID = currentSessionID;
    this.crypto = crypto;
  }

  getCoValue(id: RawCoID) {
    let entry = this.coValues.get(id);

    if (!entry) {
      entry = CoValueCore.fromID(id, this);
      this.coValues.set(id, entry);
    }

    return entry;
  }

  allCoValues() {
    return this.coValues.values();
  }

  private putCoValue(
    id: RawCoID,
    verified: VerifiedState,
    { forceOverwrite = false }: { forceOverwrite?: boolean } = {},
  ): AvailableCoValueCore {
    const entry = this.getCoValue(id);
    entry.internalMarkMagicallyAvailable(verified, { forceOverwrite });
    return entry as AvailableCoValueCore;
  }

  internalDeleteCoValue(id: RawCoID) {
    this.coValues.delete(id);
  }

  getCurrentAgent(): ControlledAccountOrAgent {
    const accountOrAgent = accountOrAgentIDfromSessionID(this.currentSessionID);
    if (isAgentID(accountOrAgent)) {
      return new ControlledAgent(this.agentSecret, this.crypto);
    }
    return new ControlledAccount(
      expectAccount(
        this.expectCoValueLoaded(accountOrAgent).getCurrentContent(),
      ),
      this.agentSecret,
    );
  }

  expectCurrentAccountID(reason: string): RawAccountID {
    const accountOrAgent = accountOrAgentIDfromSessionID(this.currentSessionID);
    if (isAgentID(accountOrAgent)) {
      throw new Error(
        "Current account is an agent, but expected an account: " + reason,
      );
    }
    return accountOrAgent;
  }

  expectCurrentAccount(reason: string): RawAccount {
    const accountID = this.expectCurrentAccountID(reason);
    return expectAccount(
      this.expectCoValueLoaded(accountID).getCurrentContent(),
    );
  }

  static internalCreateAccount(opts: {
    crypto: CryptoProvider;
    initialAgentSecret?: AgentSecret;
    peersToLoadFrom?: Peer[];
  }): RawAccount {
    const {
      crypto,
      initialAgentSecret = crypto.newRandomAgentSecret(),
      peersToLoadFrom = [],
    } = opts;
    const accountHeader = accountHeaderForInitialAgentSecret(
      initialAgentSecret,
      crypto,
    );
    const accountID = idforHeader(accountHeader, crypto);

    const node = new LocalNode(
      initialAgentSecret,
      crypto.newRandomSessionID(accountID as RawAccountID),
      crypto,
    );

    for (const peer of peersToLoadFrom) {
      node.syncManager.addPeer(peer);
    }

    const accountAgentID = crypto.getAgentID(initialAgentSecret);

    const rawAccount = expectGroup(
      node.createCoValue(accountHeader).getCurrentContent(),
    );

    rawAccount.set(accountAgentID, "admin", "trusting");

    const readKey = crypto.newRandomKeySecret();

    const sealed = crypto.seal({
      message: readKey.secret,
      from: crypto.getAgentSealerSecret(initialAgentSecret),
      to: crypto.getAgentSealerID(accountAgentID),
      nOnceMaterial: {
        in: rawAccount.id,
        tx: rawAccount.core.nextTransactionID(),
      },
    });

    rawAccount.set(`${readKey.id}_for_${accountAgentID}`, sealed, "trusting");

    rawAccount.set("readKey", readKey.id, "trusting");

    return node.expectCurrentAccount("after creation");
  }

  /** @category 2. Node Creation */
  static async withNewlyCreatedAccount({
    creationProps,
    peersToLoadFrom,
    migration,
    crypto,
    initialAgentSecret = crypto.newRandomAgentSecret(),
  }: {
    creationProps: { name: string };
    peersToLoadFrom?: Peer[];
    migration?: RawAccountMigration<AccountMeta>;
    crypto: CryptoProvider;
    initialAgentSecret?: AgentSecret;
  }): Promise<{
    node: LocalNode;
    accountID: RawAccountID;
    accountSecret: AgentSecret;
    sessionID: SessionID;
  }> {
    const account = LocalNode.internalCreateAccount({
      crypto,
      initialAgentSecret,
      peersToLoadFrom,
    });
    const node = account.core.node;

    if (migration) {
      await migration(account, node, creationProps);
    } else {
      const profileGroup = node.createGroup();
      profileGroup.addMember("everyone", "reader");
      const profile = profileGroup.createMap<Profile>({
        name: creationProps.name,
      });
      account.set("profile", profile.id, "trusting");
    }

    const profileId = account.get("profile");

    if (!profileId) {
      throw new Error("Must set account profile in initial migration");
    }

    if (node.syncManager.hasStoragePeers()) {
      await Promise.all([
        node.syncManager.waitForStorageSync(account.id),
        node.syncManager.waitForStorageSync(profileId),
      ]);
    }

    return {
      node,
      accountID: account.id,
      accountSecret: initialAgentSecret,
      sessionID: node.currentSessionID,
    };
  }

  /** @category 2. Node Creation */
  static async withLoadedAccount({
    accountID,
    accountSecret,
    sessionID,
    peersToLoadFrom,
    crypto,
    migration,
  }: {
    accountID: RawAccountID;
    accountSecret: AgentSecret;
    sessionID: SessionID | undefined;
    peersToLoadFrom: Peer[];
    crypto: CryptoProvider;
    migration?: RawAccountMigration<AccountMeta>;
  }): Promise<LocalNode> {
    try {
      const node = new LocalNode(
        accountSecret,
        sessionID || crypto.newRandomSessionID(accountID),
        crypto,
      );

      for (const peer of peersToLoadFrom) {
        node.syncManager.addPeer(peer);
      }

      const account = await node.load(accountID);

      if (account === "unavailable") {
        throw new Error("Account unavailable from all peers");
      }

      const profileID = account.get("profile");
      if (!profileID) {
        throw new Error("Account has no profile");
      }

      // Preload the profile
      await node.load(profileID);

      if (migration) {
        await migration(account, node);
      }

      return node;
    } catch (e) {
      logger.error("Error withLoadedAccount", { err: e });
      throw e;
    }
  }

  /** @internal */
  createCoValue(header: CoValueHeader): AvailableCoValueCore {
    if (this.crashed) {
      throw new Error("Trying to create CoValue after node has crashed", {
        cause: this.crashed,
      });
    }

    const id = idforHeader(header, this.crypto);

    const coValue = this.putCoValue(
      id,
      new VerifiedState(id, this.crypto, header, new Map()),
    );

    void this.syncManager.requestCoValueSync(coValue);

    return coValue;
  }

  /** @internal */
  async loadCoValueCore(
    id: RawCoID,
    skipLoadingFromPeer?: PeerID,
  ): Promise<CoValueCore> {
    if (this.crashed) {
      throw new Error("Trying to load CoValue after node has crashed", {
        cause: this.crashed,
      });
    }

    let retries = 0;

    while (true) {
      const coValue = this.getCoValue(id);

      if (
        coValue.loadingState === "unknown" ||
        coValue.loadingState === "unavailable"
      ) {
        const peers =
          this.syncManager.getServerAndStoragePeers(skipLoadingFromPeer);

        if (peers.length === 0) {
          return coValue;
        }

        coValue.loadFromPeers(peers).catch((e) => {
          logger.error("Error loading from peers", {
            id,
            err: e,
          });
        });
      }

      const result = await coValue.waitForAvailableOrUnavailable();
      if (result.isAvailable() || retries >= 1) {
        return result;
      }

      await new Promise((resolve) => setTimeout(resolve, 300));

      retries++;
    }
  }

  /**
   * Loads a CoValue's content, syncing from peers as necessary and resolving the returned
   * promise once a first version has been loaded. See `coValue.subscribe()` and `node.useTelepathicData()`
   * for listening to subsequent updates to the CoValue.
   *
   * @category 3. Low-level
   */
  async load<T extends RawCoValue>(id: CoID<T>): Promise<T | "unavailable"> {
    if (!id) {
      throw new Error("Trying to load CoValue with undefined id");
    }

    if (!id.startsWith("co_z")) {
      throw new Error(`Trying to load CoValue with invalid id ${id}`);
    }

    const core = await this.loadCoValueCore(id);

    if (!core.isAvailable()) {
      return "unavailable";
    }

    return core.getCurrentContent() as T;
  }

  getLoaded<T extends RawCoValue>(id: CoID<T>): T | undefined {
    const coValue = this.getCoValue(id);

    if (coValue.isAvailable()) {
      return coValue.getCurrentContent() as T;
    }

    return undefined;
  }

  /** @category 3. Low-level */
  subscribe<T extends RawCoValue>(
    id: CoID<T>,
    callback: (update: T | "unavailable") => void,
  ): () => void {
    let stopped = false;
    let unsubscribe!: () => void;

    this.load(id)
      .then((coValue) => {
        if (stopped) {
          return;
        }
        if (coValue === "unavailable") {
          callback("unavailable");
          return;
        }
        unsubscribe = coValue.subscribe(callback);
      })
      .catch((e) => {
        logger.error("Subscription error", {
          id,
          err: e,
        });
      });

    return () => {
      stopped = true;
      unsubscribe?.();
    };
  }

  async acceptInvite<T extends RawCoValue>(
    groupOrOwnedValueID: CoID<T>,
    inviteSecret: InviteSecret,
  ): Promise<void> {
    const value = await this.load(groupOrOwnedValueID);

    if (value === "unavailable") {
      throw new Error(
        "Trying to accept invite: Group/owned value unavailable from all peers",
      );
    }

    const ruleset = value.core.verified.header.ruleset;

    let group: RawGroup;

    if (ruleset.type === "unsafeAllowAll") {
      throw new Error("Can only accept invites to values owned by groups");
    }

    if (ruleset.type === "ownedByGroup") {
      const owner = await this.load(ruleset.group as CoID<RawGroup>);

      if (owner === "unavailable") {
        throw new Error(
          "Trying to accept invite: CoValue owner unavailable from all peers",
        );
      }

      group = expectGroup(owner);
    } else {
      group = expectGroup(value);
    }

    if (group.core.verified.header.meta?.type === "account") {
      throw new Error("Can't accept invites to values owned by accounts");
    }

    const inviteAgentSecret = this.crypto.agentSecretFromSecretSeed(
      secretSeedFromInviteSecret(inviteSecret),
    );

    const inviteAgentID = this.crypto.getAgentID(inviteAgentSecret);

    const inviteRole = await new Promise((resolve, reject) => {
      group.subscribe((groupUpdate) => {
        const role = groupUpdate.get(inviteAgentID);
        if (role) {
          resolve(role);
        }
      });
      setTimeout(
        () => reject(new Error("Couldn't find invite before timeout")),
        2000,
      );
    });

    if (!inviteRole) {
      throw new Error("No invite found");
    }

    const account = this.getCurrentAgent();
    const existingRole = group.get(account.id);

    if (
      existingRole === "admin" ||
      (existingRole === "writer" && inviteRole === "writerInvite") ||
      (existingRole === "writer" && inviteRole === "reader") ||
      (existingRole === "reader" && inviteRole === "readerInvite") ||
      (existingRole && inviteRole === "writeOnlyInvite")
    ) {
      logger.debug("Not accepting invite that would replace or downgrade role");
      return;
    }

    const groupAsInvite = expectGroup(
      this.loadCoValueAsDifferentAgent(
        group.id,
        inviteAgentSecret,
      ).getCurrentContent(),
    );

    groupAsInvite.addMemberInternal(
      account,
      inviteRole === "adminInvite"
        ? "admin"
        : inviteRole === "writerInvite"
          ? "writer"
          : inviteRole === "writeOnlyInvite"
            ? "writeOnly"
            : "reader",
    );

    group.core.internalShamefullyCloneVerifiedStateFrom(
      groupAsInvite.core.verified,
      { forceOverwrite: true },
    );
    group.core.internalShamefullyResetCachedContent();

    group.core.notifyUpdate("immediate");
  }

  /** @internal */
  expectCoValueLoaded(id: RawCoID, expectation?: string): AvailableCoValueCore {
    const coValue = this.getCoValue(id);

    if (!coValue.isAvailable()) {
      throw new Error(
        `${expectation ? expectation + ": " : ""}CoValue ${id} not yet loaded.`,
      );
    }
    return coValue;
  }

  /** @internal */
  expectProfileLoaded(id: RawAccountID, expectation?: string): RawProfile {
    const account = this.expectCoValueLoaded(id, expectation);
    const profileID = expectGroup(account.getCurrentContent()).get("profile");
    if (!profileID) {
      throw new Error(
        `${expectation ? expectation + ": " : ""}Account ${id} has no profile`,
      );
    }
    return this.expectCoValueLoaded(
      profileID,
      expectation,
    ).getCurrentContent() as RawProfile;
  }

  /** @internal */
  resolveAccountAgent(
    id: RawAccountID | AgentID,
    expectation?: string,
  ): Result<AgentID, ResolveAccountAgentError> {
    if (isAgentID(id)) {
      return ok(id);
    }

    let coValue: AvailableCoValueCore;

    try {
      coValue = this.expectCoValueLoaded(id, expectation);
    } catch (e) {
      return err({
        type: "ErrorLoadingCoValueCore",
        expectation,
        id,
        error: e,
      } satisfies LoadCoValueCoreError);
    }

    if (
      coValue.verified.header.type !== "comap" ||
      coValue.verified.header.ruleset.type !== "group" ||
      !coValue.verified.header.meta ||
      !("type" in coValue.verified.header.meta) ||
      coValue.verified.header.meta.type !== "account"
    ) {
      return err({
        type: "UnexpectedlyNotAccount",
        expectation,
        id,
      } satisfies UnexpectedlyNotAccountError);
    }

    return ok((coValue.getCurrentContent() as RawAccount).currentAgentID());
  }

  createGroup(
    uniqueness: CoValueUniqueness = this.crypto.createdNowUnique(),
  ): RawGroup {
    const account = this.getCurrentAgent();

    const groupCoValue = this.createCoValue({
      type: "comap",
      ruleset: { type: "group", initialAdmin: account.id },
      meta: null,
      ...uniqueness,
    });

    const group = expectGroup(groupCoValue.getCurrentContent());

    group.set(account.id, "admin", "trusting");

    const readKey = this.crypto.newRandomKeySecret();

    group.set(
      `${readKey.id}_for_${account.id}`,
      this.crypto.seal({
        message: readKey.secret,
        from: account.currentSealerSecret(),
        to: account.currentSealerID(),
        nOnceMaterial: {
          in: groupCoValue.id,
          tx: groupCoValue.nextTransactionID(),
        },
      }),
      "trusting",
    );

    group.set("readKey", readKey.id, "trusting");

    return group;
  }

  loadCoValueAsDifferentAgent(
    id: RawCoID,
    secret: AgentSecret,
    accountId?: RawAccountID | AgentID,
  ) {
    const agent = new ControlledAgent(secret, this.crypto);

    const newNode = new LocalNode(
      secret,
      this.crypto.newRandomSessionID(accountId || agent.id),
      this.crypto,
    );

    newNode.cloneVerifiedStateFrom(this, id);

    return newNode.expectCoValueLoaded(id);
  }

  /** @internal */
  cloneVerifiedStateFrom(otherNode: LocalNode, id: RawCoID) {
    const coValuesIdsToCopy = [id];

    // Scan all the dependencies and add them to the list
    for (let i = 0; i < coValuesIdsToCopy.length; i++) {
      const coValueID = coValuesIdsToCopy[i]!;
      const coValue = otherNode.getCoValue(coValueID);

      if (!coValue.isAvailable()) {
        continue;
      }

      for (const dep of coValue.getDependedOnCoValues()) {
        coValuesIdsToCopy.push(dep);
      }
    }

    // Copy the coValue all the dependencies by following the dependency order
    while (coValuesIdsToCopy.length > 0) {
      const coValueID = coValuesIdsToCopy.pop()!;
      const coValue = otherNode.getCoValue(coValueID);

      if (!coValue.isAvailable()) {
        continue;
      }

      if (this.coValues.get(coValueID)?.isAvailable()) {
        continue;
      }

      this.putCoValue(coValueID, coValue.verified);
    }
  }

  gracefulShutdown() {
    this.syncManager.gracefulShutdown();
  }
}

export type LoadCoValueCoreError = {
  type: "ErrorLoadingCoValueCore";
  error: unknown;
  expectation?: string;
  id: RawAccountID;
};

export type AccountUnavailableFromAllPeersError = {
  type: "AccountUnavailableFromAllPeers";
  expectation?: string;
  id: RawAccountID;
};

export type UnexpectedlyNotAccountError = {
  type: "UnexpectedlyNotAccount";
  expectation?: string;
  id: RawAccountID;
};

export type ResolveAccountAgentError =
  | InvalidAccountAgentIDError
  | LoadCoValueCoreError
  | AccountUnavailableFromAllPeersError
  | UnexpectedlyNotAccountError;
