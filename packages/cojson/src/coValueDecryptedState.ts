import { CoValueCore } from "./coValueCore.js";
import { CoValueState } from "./coValueState.js";
import { RawAccountID } from "./coValues/account.js";
import { RawGroup } from "./coValues/group.js";
import {
  CryptoProvider,
  Encrypted,
  KeyID,
  KeySecret,
} from "./crypto/crypto.js";
import {
  RawCoID,
  SessionID,
  TransactionID,
  getGroupDependentKeyList,
} from "./ids.js";
import { RawCoValue } from "./index.js";
import { parseJSON } from "./jsonStringify.js";
import { JsonValue } from "./jsonValue.js";
import { LocalNode } from "./localNode.js";
import { logger } from "./logger.js";
import { determineValidTransactions } from "./permissions.js";
import { CoValueKnownState } from "./sync.js";
import { accountOrAgentIDfromSessionID } from "./typeUtils/accountOrAgentIDfromSessionID.js";
import { expectGroup } from "./typeUtils/expectGroup.js";
import { isAccountID } from "./typeUtils/isAccountID.js";

export type DecryptedTransaction = {
  txID: TransactionID;
  changes: JsonValue[];
  madeAt: number;
};

const readKeyCache = new WeakMap<CoValueCore, { [id: KeyID]: KeySecret }>();

export class CoValueDecryptedState {
  private state: CoValueState;
  private crypto: CryptoProvider;
  private node: LocalNode;
  private _decryptionCache: {
    [key: Encrypted<JsonValue[], JsonValue>]: JsonValue[] | undefined;
  } = {};
  private _cachedDependentOn?: RawCoID[];
  private _cachedContent?: RawCoValue;

  private listeners: Set<
    (state: CoValueDecryptedState, unsub: () => void) => void
  > = new Set();

  unsubFromGroup?: () => void;

  subscribeToGroupInvalidation() {
    if (this.unsubFromGroup) {
      return;
    }

    const header = this.header;

    if (header.ruleset.type == "ownedByGroup") {
      const groupId = header.ruleset.group;
      const entry = this.node.coValuesStore.get(groupId);

      if (entry.isAvailable()) {
        this.unsubFromGroup = entry.addListener((_groupUpdate) => {
          this._cachedContent = undefined;
          this.notifyUpdate("immediate");
        }, false);
      } else {
        logger.error("CoValueCore: Owner group not available", {
          id: this.id,
          groupId,
        });
      }
    }
  }

  subscribe(
    listener: (state: CoValueDecryptedState, unsub: () => void) => void,
  ) {
    if (this.listeners.size === 0) {
      this.state.addListener((state) => {
        if (state.highLevelState === "available") {
          this._cachedDependentOn = undefined;
          if (
            this._cachedContent &&
            "processNewTransactions" in this._cachedContent &&
            typeof this._cachedContent.processNewTransactions === "function"
          ) {
            this._cachedContent.processNewTransactions();
          } else {
            this._cachedContent = undefined;
          }
        }
      });
    }
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  private unsubscribe(
    listener: (state: CoValueDecryptedState, unsub: () => void) => void,
  ) {
    this.listeners.delete(listener);
    if (this.listeners.size === 0) {
      this.unsubFromGroup?.();
      this.unsubFromGroup = undefined;
    }
  }

  constructor(state: CoValueState) {
    this.state = state;
  }

  getGroup(): RawGroup {
    if (this.core.header.ruleset.type !== "ownedByGroup") {
      throw new Error("Only values owned by groups have groups");
    }

    return expectGroup(
      this.node
        .expectCoValueLoaded(this.core.header.ruleset.group)
        .getCurrentContent(),
    );
  }

  getValidTransactions(options?: {
    ignorePrivateTransactions: boolean;
    knownTransactions?: CoValueKnownState["sessions"];
  }): DecryptedTransaction[] {
    const validTransactions = determineValidTransactions(
      this.core,
      options?.knownTransactions,
    );

    const allTransactions: DecryptedTransaction[] = [];

    for (const { txID, tx } of validTransactions) {
      if (options?.knownTransactions?.[txID.sessionID]! >= txID.txIndex) {
        continue;
      }

      if (tx.privacy === "trusting") {
        allTransactions.push({
          txID,
          madeAt: tx.madeAt,
          changes: parseJSON(tx.changes),
        });
        continue;
      }

      if (options?.ignorePrivateTransactions) {
        continue;
      }

      const readKey = this.getReadKey(tx.keyUsed);

      if (!readKey) {
        continue;
      }

      let decryptedChanges = this._decryptionCache[tx.encryptedChanges];

      if (!decryptedChanges) {
        const decryptedString = this.crypto.decryptRawForTransaction(
          tx.encryptedChanges,
          readKey,
          {
            in: this.id,
            tx: txID,
          },
        );
        decryptedChanges = decryptedString && parseJSON(decryptedString);
        this._decryptionCache[tx.encryptedChanges] = decryptedChanges;
      }

      if (!decryptedChanges) {
        logger.error("Failed to decrypt transaction despite having key", {
          err: new Error("Failed to decrypt transaction despite having key"),
        });
        continue;
      }

      allTransactions.push({
        txID,
        madeAt: tx.madeAt,
        changes: decryptedChanges,
      });
    }

    return allTransactions;
  }

  getValidSortedTransactions(options?: {
    ignorePrivateTransactions: boolean;
  }): DecryptedTransaction[] {
    const allTransactions = this.getValidTransactions(options);

    allTransactions.sort(this.compareTransactions);

    return allTransactions;
  }

  getCurrentReadKey(): { secret: KeySecret | undefined; id: KeyID } {
    if (this.header.ruleset.type === "group") {
      const content = expectGroup(this.getCurrentContent());

      const currentKeyId = content.getCurrentReadKeyId();

      if (!currentKeyId) {
        throw new Error("No readKey set");
      }

      const secret = this.getReadKey(currentKeyId);

      return {
        secret: secret,
        id: currentKeyId,
      };
    } else if (this.header.ruleset.type === "ownedByGroup") {
      return this.node
        .expectCoValueLoaded(this.header.ruleset.group)
        .getCurrentReadKey();
    } else {
      throw new Error(
        "Only groups or values owned by groups have read secrets",
      );
    }
  }

  getReadKey(keyID: KeyID): KeySecret | undefined {
    let key = readKeyCache.get(this)?.[keyID];
    if (!key) {
      key = this.getUncachedReadKey(keyID);
      if (key) {
        let cache = readKeyCache.get(this);
        if (!cache) {
          cache = {};
          readKeyCache.set(this, cache);
        }
        cache[keyID] = key;
      }
    }
    return key;
  }

  getUncachedReadKey(keyID: KeyID): KeySecret | undefined {
    if (this.header.ruleset.type === "group") {
      const content = expectGroup(
        this.getCurrentContent({ ignorePrivateTransactions: true }),
      );

      const keyForEveryone = content.get(`${keyID}_for_everyone`);
      if (keyForEveryone) return keyForEveryone;

      // Try to find key revelation for us
      const lookupAccountOrAgentID =
        this.header.meta?.type === "account"
          ? this.node.account.currentAgentID()
          : this.node.account.id;

      const lastReadyKeyEdit = content.lastEditAt(
        `${keyID}_for_${lookupAccountOrAgentID}`,
      );

      if (lastReadyKeyEdit?.value) {
        const revealer = lastReadyKeyEdit.by;
        const revealerAgent = this.node
          .resolveAccountAgent(revealer, "Expected to know revealer")
          ._unsafeUnwrap({ withStackTrace: true });

        const secret = this.crypto.unseal(
          lastReadyKeyEdit.value,
          this.node.account.currentSealerSecret(),
          this.crypto.getAgentSealerID(revealerAgent),
          {
            in: this.id,
            tx: lastReadyKeyEdit.tx,
          },
        );

        if (secret) {
          return secret as KeySecret;
        }
      }

      // Try to find indirect revelation through previousKeys

      for (const co of content.keys()) {
        if (isKeyForKeyField(co) && co.startsWith(keyID)) {
          const encryptingKeyID = co.split("_for_")[1] as KeyID;
          const encryptingKeySecret = this.getReadKey(encryptingKeyID);

          if (!encryptingKeySecret) {
            continue;
          }

          const encryptedPreviousKey = content.get(co)!;

          const secret = this.crypto.decryptKeySecret(
            {
              encryptedID: keyID,
              encryptingID: encryptingKeyID,
              encrypted: encryptedPreviousKey,
            },
            encryptingKeySecret,
          );

          if (secret) {
            return secret as KeySecret;
          } else {
            logger.warn(
              `Encrypting ${encryptingKeyID} key didn't decrypt ${keyID}`,
            );
          }
        }
      }

      // try to find revelation to parent group read keys
      for (const co of content.keys()) {
        if (isParentGroupReference(co)) {
          const parentGroupID = getParentGroupId(co);
          const parentGroup = this.node.expectCoValueLoaded(
            parentGroupID,
            "Expected parent group to be loaded",
          );

          const parentKeys = this.findValidParentKeys(
            keyID,
            content,
            parentGroup,
          );

          for (const parentKey of parentKeys) {
            const revelationForParentKey = content.get(
              `${keyID}_for_${parentKey.id}`,
            );

            if (revelationForParentKey) {
              const secret = parentGroup.crypto.decryptKeySecret(
                {
                  encryptedID: keyID,
                  encryptingID: parentKey.id,
                  encrypted: revelationForParentKey,
                },
                parentKey.secret,
              );

              if (secret) {
                return secret as KeySecret;
              } else {
                logger.warn(
                  `Encrypting parent ${parentKey.id} key didn't decrypt ${keyID}`,
                );
              }
            }
          }
        }
      }

      return undefined;
    } else if (this.header.ruleset.type === "ownedByGroup") {
      return this.node
        .expectCoValueLoaded(this.header.ruleset.group)
        .getReadKey(keyID);
    } else {
      throw new Error(
        "Only groups or values owned by groups have read secrets",
      );
    }
  }

  findValidParentKeys(keyID: KeyID, group: RawGroup, parentGroup: CoValueCore) {
    const validParentKeys: { id: KeyID; secret: KeySecret }[] = [];

    for (const co of group.keys()) {
      if (isKeyForKeyField(co) && co.startsWith(keyID)) {
        const encryptingKeyID = co.split("_for_")[1] as KeyID;
        const encryptingKeySecret = parentGroup.getReadKey(encryptingKeyID);

        if (!encryptingKeySecret) {
          continue;
        }

        validParentKeys.push({
          id: encryptingKeyID,
          secret: encryptingKeySecret,
        });
      }
    }

    return validParentKeys;
  }

  getDependedOnCoValues(): RawCoID[] {
    if (this._cachedDependentOn) {
      return this._cachedDependentOn;
    } else {
      const dependentOn = this.getDependedOnCoValuesUncached();
      this._cachedDependentOn = dependentOn;
      return dependentOn;
    }
  }

  /** @internal */
  getDependedOnCoValuesUncached(): RawCoID[] {
    return this.core.header.ruleset.type === "group"
      ? getGroupDependentKeyList(expectGroup(this.getCurrentContent()).keys())
      : this.core.header.ruleset.type === "ownedByGroup"
        ? [
            this.core.header.ruleset.group,
            ...new Set(
              [...this.core.sessionLogs.keys()]
                .map((sessionID) =>
                  accountOrAgentIDfromSessionID(sessionID as SessionID),
                )
                .filter(
                  (session): session is RawAccountID =>
                    isAccountID(session) && session !== this.id,
                ),
            ),
          ]
        : [];
  }

  nextTransactionID(): TransactionID {
    // This is an ugly hack to get a unique but stable session ID for editing the current account
    const sessionID =
      this.core.header.meta?.type === "account"
        ? (this.node.currentSessionID.replace(
            this.node.account.id,
            this.node.account.currentAgentID(),
          ) as SessionID)
        : this.node.currentSessionID;

    return {
      sessionID,
      txIndex: this.sessionLogs.get(sessionID)?.transactions.length || 0,
    };
  }

  makeTransaction(
    changes: JsonValue[],
    privacy: "private" | "trusting",
  ): boolean {
    const madeAt = Date.now();

    let transaction: Transaction;

    if (privacy === "private") {
      const { secret: keySecret, id: keyID } = this.getCurrentReadKey();

      if (!keySecret) {
        throw new Error("Can't make transaction without read key secret");
      }

      const encrypted = this.crypto.encryptForTransaction(changes, keySecret, {
        in: this.id,
        tx: this.nextTransactionID(),
      });

      this._decryptionCache[encrypted] = changes;

      transaction = {
        privacy: "private",
        madeAt,
        keyUsed: keyID,
        encryptedChanges: encrypted,
      };
    } else {
      transaction = {
        privacy: "trusting",
        madeAt,
        changes: stableStringify(changes),
      };
    }

    // This is an ugly hack to get a unique but stable session ID for editing the current account
    const sessionID =
      this.header.meta?.type === "account"
        ? (this.node.currentSessionID.replace(
            this.node.account.id,
            this.node.account.currentAgentID(),
          ) as SessionID)
        : this.node.currentSessionID;

    const { expectedNewHash, newStreamingHash } = this.expectedNewHashAfter(
      sessionID,
      [transaction],
    );

    const signature = this.crypto.sign(
      this.node.account.currentSignerSecret(),
      expectedNewHash,
    );

    const success = this.int_tryAddTransactions(
      sessionID,
      [transaction],
      expectedNewHash,
      signature,
      true,
      newStreamingHash,
    )._unsafeUnwrap({ withStackTrace: true });

    if (success) {
      this.node.syncManager.recordTransactionsSize([transaction], "local");
      void this.node.syncManager.syncCoValue(this);
    }

    return success;
  }

  getCurrentContent(options?: {
    ignorePrivateTransactions: true;
  }): RawCoValue {
    if (!options?.ignorePrivateTransactions && this._cachedContent) {
      return this._cachedContent;
    }

    this.subscribeToGroupInvalidation();

    const newContent = coreToCoValue(this, options);

    if (!options?.ignorePrivateTransactions) {
      this._cachedContent = newContent;
    }

    return newContent;
  }
}

function compareTransactions(
  a: Pick<DecryptedTransaction, "madeAt" | "txID">,
  b: Pick<DecryptedTransaction, "madeAt" | "txID">,
) {
  return (
    a.madeAt - b.madeAt ||
    (a.txID.sessionID === b.txID.sessionID
      ? 0
      : a.txID.sessionID < b.txID.sessionID
        ? -1
        : 1) ||
    a.txID.txIndex - b.txID.txIndex
  );
}
