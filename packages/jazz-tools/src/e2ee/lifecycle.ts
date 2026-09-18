import { exclusiveE2eeTransaction } from "../runtime/db.js";
import { configureAcceptedHistory } from "./accepted-history.js";
import type { AccountStore } from "../accounts/persistence.js";
import type { WasmSchema } from "../drivers/types.js";
import { encryptedSchemas } from "./encrypted-schema.js";
import { accountRegistry, exportLocalFirstSecret } from "../accounts/enrollment.js";
import { parseAuthSecret } from "../runtime/auth-secret-codec.js";
import { openRecoveryMaterial, protectRecoveryMaterial } from "./recovery-protection.js";
import { E2eeRecoveryError } from "./recovery-error.js";
import { E2eeDataError } from "./data-error.js";
import type { AccountHandle } from "../accounts/state.js";
import { beginDbTransactionAfter, prepareDbTransaction } from "../runtime/db.js";
import type { Db, TableProxy, Transaction, E2eeTransactionScope } from "../runtime/db.js";
import { deviceRequestApp, deviceRequestSchema } from "./device-requests.js";
import type { DeviceTables } from "./device-requests.js";
import { encodeEnvelope } from "./envelope.js";
import type { CryptoMechanism } from "./envelope.js";
import { localDevice } from "./local-device.js";
import { firstAccountEpoch } from "./first-epoch.js";
import { DeviceApproval } from "./device-approval.js";
import {
  prefetchPublicMembershipHistory,
  readPublicMembershipHistory,
  replayAccountMembership,
} from "./public-membership.js";
import { Groups } from "./group-lifecycle.js";
import type { GroupRecoveryPath } from "./group-lifecycle.js";
import type { GroupTables } from "./groups.js";
import { Spaces } from "./space-lifecycle.js";
import type { SpaceRecoveryPath } from "./space-lifecycle.js";
import type { SpaceTables } from "./spaces.js";
import type { JazzCrypto, CellCipher, EqualityIndex } from "./types.js";

const equalityCrypto = new WeakMap<Db, () => Promise<EqualityIndex>>();

/** @internal Index adapters never decide what a candidate is allowed to match. */
export async function equalityCryptoForDb(db: Db): Promise<EqualityIndex> {
  e2eeForDb(db);
  return equalityCrypto.get(db)!();
}

export type E2eeConfig = {
  /** Application returned by defineApp; encrypted apps include managed bindings automatically. */
  app?: DeviceTables | { readonly wasmSchema: WasmSchema };
  /** Dedicated local key storage. Do not reuse the account-selection store value. */
  store: AccountStore;
  crypto?: JazzCrypto;
  /** Known-stale offline writes warn by default; revoked devices are always refused. */
  staleWrites?: "warn" | "reject";
};
export type DeviceInfo = Readonly<{
  id: string;
  /** Verified approval membership, independent of this caller's key access. */
  state: "pending" | "active" | "revoked";
  /** This caller verified the current key-delivery chain; not proof of remote key possession. */
  keyReadiness: "verified" | "not-verified";
  publicKey: Uint8Array;
  mechanism: CryptoMechanism;
}>;

const contexts = new WeakMap<Db, E2ee>();
const cellCrypto = new WeakMap<Db, () => Promise<{ cipher: CellCipher; application: string }>>();

/** @internal Common cell framing owns identity selection, not the crypto adapter. */
export async function cellCryptoForDb(
  db: Db,
): Promise<{ cipher: CellCipher; application: string }> {
  e2eeForDb(db);
  return cellCrypto.get(db)!();
}
const configuredSchemas = new WeakMap<Db, WasmSchema>();
const configuredAccounts = new WeakMap<Db, string>();

/** @internal Dependency observation follows the caller, not the space author. */
export function e2eeAccountForDb(db: Db): string {
  e2eeForDb(db);
  return configuredAccounts.get(db)!;
}

/** @internal The configured application can bind a cold transaction's schema. */
export function e2eeSchemaForDb(db: Db): WasmSchema | undefined {
  return configuredSchemas.get(db);
}
const initialSpacePreparers = new WeakMap<Db, Spaces["prepareInitial"]>();
const currentSpaceKeys = new WeakMap<Db, Spaces["withKeys"]>();
const initialSpacePrerequisites = new WeakMap<
  Db,
  (recipientIds?: readonly string[]) => Promise<void>
>();

/** @internal Exclusive transactions on encrypted schemas prepare before opening. */
export function e2eeInitialPreparationForDb(
  db: Db,
): ((recipientIds?: readonly string[]) => Promise<void>) | undefined {
  const schema = configuredSchemas.get(db);
  return schema && encryptedSchemas.has(schema) ? initialSpacePrerequisites.get(db) : undefined;
}

/** @internal Cell operations borrow a verified key; never exported by the public API. */
export async function withSpaceKeys<T, Init>(
  db: Db,
  scope: TableProxy<T, Init>,
  identifier: string,
  use: Parameters<Spaces["withKeys"]>[2],
  includeHistory = false,
  forWrite = false,
): Promise<void> {
  e2eeForDb(db);
  let callbackFailure: { error: unknown } | undefined;
  const operation: typeof use = async (...args) => {
    try {
      await use(...args);
    } catch (error) {
      callbackFailure = { error };
      throw error;
    }
  };
  const state = await currentSpaceKeys.get(db)!(
    scope,
    identifier,
    operation,
    includeHistory,
    undefined,
    forWrite,
  ).catch((error: unknown) => {
    // Key adapters can include secret material in their exceptions.
    // The operation owns its own crypto diagnostics and ordinary runtime errors.
    if (error instanceof E2eeDataError || (callbackFailure && callbackFailure.error === error))
      throw error;
    throw new E2eeDataError("key-unavailable");
  });
  if (state.state !== "ready") {
    throw new E2eeDataError(
      state.state === "refused"
        ? "key-not-shared"
        : state.state === "maintenance-required"
          ? "maintenance-required"
          : "key-unavailable",
    );
  }
}

/** @internal Cold recipient discovery precedes the immutable transaction snapshot. */
export function beginInitialSpaceTransaction(
  db: Db,
  recipientIds?: readonly string[],
): Transaction<"exclusive"> {
  const recipients = recipientIds?.slice();
  return beginDbTransactionAfter(db, async () => {
    e2eeForDb(db);
    await initialSpacePrerequisites.get(db)!(recipients);
  });
}

/** @internal Not exported from the package API; used by transaction preparation. */
export async function prepareInitialSpaceForTransaction<T, Init>(
  db: Db,
  tx: Transaction<"exclusive">,
  scope: TableProxy<T, Init>,
  identifier: string,
  prepareData: Parameters<Spaces["prepareInitial"]>[3],
  recipientIds?: readonly string[],
): Promise<void> {
  const recipients = recipientIds?.slice();
  try {
    await prepareDbTransaction(tx, async (prepared) => {
      await prepareInitialSpaceRows(db, prepared, scope, identifier, prepareData, recipients);
    });
  } catch (error) {
    try {
      await tx.rollback();
    } catch {
      // Preserve the preparation error, including failures before keys are loaded.
    }
    throw error;
  }
}

/** @internal Reuses an already-prepared transaction; never queues behind itself. */
export async function prepareInitialSpaceRows<T, Init>(
  db: Db,
  tx: E2eeTransactionScope,
  scope: TableProxy<T, Init>,
  identifier: string,
  prepareData: Parameters<Spaces["prepareInitial"]>[3],
  recipientIds?: readonly string[],
): Promise<void> {
  e2eeForDb(db);
  await initialSpacePreparers.get(db)!(tx, scope, identifier, prepareData, recipientIds);
}

export type RecoveryStatus = Readonly<{
  configured: boolean;
  account: {
    epochId: string | null;
    /** Accepted active registrations, not evidence that a device is online. */
    activeDeviceIds: string[];
    recoveryRootIds: string[];
    validation: "not-checked" | "validated";
    validatedRootId?: string;
  };
  groups: { validation: "not-checked" } | { validation: "checked"; paths: GroupRecoveryPath[] };
  spaces: { validation: "not-checked" } | { validation: "checked"; paths: SpaceRecoveryPath[] };
}>;

/** @internal Only verified account-context creation binds lifecycle state. */
export function attachE2ee(db: Db, account: AccountHandle, config: E2eeConfig, env: string): void {
  contexts.set(db, new E2ee(db, account, config, env));
  configuredAccounts.set(db, account.id);
}

/** @internal Schema objects never own account keys or lifecycle state. */
export function e2eeForDb(db: Db): E2ee {
  const e2ee = contexts.get(db);
  if (!e2ee) throw new Error("E2EE requires an authenticated Db with dedicated local key storage");
  return e2ee;
}

/** First-device activation; approval and rotation are separate lifecycle operations. */
export class E2ee {
  private approval: DeviceApproval | undefined;
  private groupLifecycle: Groups | undefined;
  private spaceLifecycle: Spaces | undefined;
  private closed = false;
  private preparation: Promise<void> | undefined;
  private localPreparation: Promise<void> | undefined;
  private readonly scope: string;
  private readonly app: DeviceTables;
  readonly groups = {
    leave: (groupId: string): { wait(): Promise<void> } =>
      this.groups.remove(groupId, this.account.id),
    remove: (groupId: string, memberId: string): { wait(): Promise<void> } => {
      const completion = (async () => {
        await this.prepare();
        return this.requireGroups().remove(groupId, memberId);
      })();
      completion.catch(() => {});
      return { wait: () => completion };
    },
    add: (groupId: string, memberId: string): { wait(): Promise<void> } => {
      const completion = (async () => {
        await this.prepare();
        return this.requireGroups().add(groupId, memberId);
      })();
      completion.catch(() => {});
      return { wait: () => completion };
    },
    create: (): { id: string; wait(): Promise<{ id: string }> } => {
      const id = crypto.randomUUID();
      const completion = (async () => {
        await this.prepare();
        return this.requireGroups().create(id);
      })();
      completion.catch(() => {});
      return { id, wait: () => completion };
    },
  };

  readonly spaces = {
    revoke: <T, Init>(
      scope: TableProxy<T, Init>,
      identifier: string,
      recipientId: string,
    ): { wait(): Promise<void> } => {
      const completion = (async () => {
        await this.prepare();
        return this.requireSpaces().revoke(scope, identifier, recipientId);
      })();
      completion.catch(() => {});
      return { wait: () => completion };
    },
    grant: <T, Init>(
      scope: TableProxy<T, Init>,
      identifier: string,
      recipientId: string,
    ): { wait(): Promise<void> } => {
      const completion = (async () => {
        await this.prepare();
        return this.requireSpaces().grant(scope, identifier, recipientId);
      })();
      completion.catch(() => {});
      return { wait: () => completion };
    },
  };

  async explain<T, Init>(
    target: { groupId: string } | { scope: TableProxy<T, Init>; identifier: string },
  ) {
    await this.prepare();
    if ("scope" in target) return this.requireSpaces().explain(target.scope, target.identifier);
    return this.requireGroups().explain(target.groupId);
  }

  private requireSpaces(): Spaces {
    if (!this.spaceLifecycle) throw new Error("E2EE spaces require spaceSchema in the application");
    return this.spaceLifecycle;
  }

  private requireGroups(): Groups {
    if (!this.groupLifecycle) throw new Error("E2EE groups require groupSchema in the application");
    return this.groupLifecycle;
  }
  readonly recovery = {
    status: async (material?: string): Promise<RecoveryStatus> => {
      this.assertOpen();
      if (material !== undefined) return this.inspectRecovery(material);
      await prefetchPublicMembershipHistory(this.db, this.account.id, this.app);
      const read = await exclusiveE2eeTransaction(this.db, (tx) =>
        readPublicMembershipHistory(tx, this.account.id, this.app),
      );
      const history = await read.wait({ tier: "global" });
      this.assertOpen();
      let membership: Awaited<ReturnType<typeof replayAccountMembership>> | undefined;
      if (history.roots.rows.length) {
        const signer =
          this.config.crypto?.deviceSigner ??
          (await (await import("./browser.js")).createBrowserDeviceSigner());
        membership = await replayAccountMembership(history, this.scope, signer);
        this.assertOpen();
      }
      if (
        membership?.recoveryRoots.length &&
        this.account.identity.issuer === "urn:jazz:local-first"
      )
        return this.withProtectedRecovery((value) => this.inspectRecovery(value));
      // Registration is evidence of configuration, never of retained recovery secrets.
      return {
        configured: !!membership?.recoveryRoots.length,
        account: {
          epochId: membership?.epochId ?? null,
          activeDeviceIds: [...(membership?.active ?? [])].sort(),
          recoveryRootIds: membership?.recoveryRoots.map((root) => root.id).sort() ?? [],
          validation: "not-checked",
        },
        groups: { validation: "not-checked" },
        spaces: { validation: "not-checked" },
      };
    },
    use: (material?: string): { wait(): Promise<void> } => {
      const completion = (async () => {
        await this.prepare();
        if (material !== undefined) {
          await this.approval!.useRecovery(material);
          await this.groupLifecycle?.restoreRecovery(material);
          await this.spaceLifecycle?.restoreRecovery(material);
        } else await this.restoreProtectedRecovery();
      })();
      completion.catch(() => {});
      return { wait: () => completion };
    },
    create: (): { wait(): Promise<{ material: string }> } => {
      const completion = (async () => {
        await this.prepare();
        const result = await this.approval!.createRecovery();
        await this.groupLifecycle?.protectRecovery(result.material);
        await this.spaceLifecycle?.protectRecovery(result.material);
        if (this.account.identity.issuer === "urn:jazz:local-first") {
          const secret = parseAuthSecret(exportLocalFirstSecret(this.account));
          try {
            const cipher =
              this.config.crypto?.cellCipher ??
              (await (await import("./browser.js")).createBrowserCellCipher());
            const rootId: string = JSON.parse(result.material).rootId;
            const target = { application: this.scope, accountId: this.account.id, rootId };
            const material = await protectRecoveryMaterial(cipher, secret, target, result.material);
            if ((await openRecoveryMaterial(cipher, secret, target, material)) !== result.material)
              throw new Error("Invalid E2EE recovery protector output");
            this.assertOpen();
            await this.db
              .insert(this.app.__e2ee_recovery_protectors, { rootId, material })
              .wait({ tier: "global" });
            this.assertOpen();
          } finally {
            secret.fill(0);
          }
        }
        return result;
      })();
      completion.catch(() => {});
      return { wait: () => completion };
    },
  };

  private async inspectRecovery(material: string): Promise<RecoveryStatus> {
    const keys =
      this.config.crypto?.keyEnvelope ??
      (await (await import("./browser.js")).createBrowserKeyEnvelope());
    const signer =
      this.config.crypto?.deviceSigner ??
      (await (await import("./browser.js")).createBrowserDeviceSigner());
    this.assertOpen();
    const reader = new DeviceApproval(
      this.db,
      this.account.id,
      this.scope,
      keys,
      signer,
      () => this.assertOpen(),
      this.app,
    );
    const account = await reader.inspectRecovery(material);
    this.assertOpen();
    let groups: Groups | undefined;
    let groupCoverage: RecoveryStatus["groups"] = { validation: "not-checked" };
    if ("__e2ee_groups" in this.app && "__e2ee_group_recovery_deliveries" in this.app) {
      groups = new Groups(
        this.db,
        this.account.id,
        this.scope,
        this.app as DeviceTables & GroupTables,
        keys,
        signer,
        () => this.assertOpen(),
        (accountId) => JSON.stringify([accountRegistry(this.account), this.env, accountId]),
      );
      const paths = await groups.inspectRecovery(material, account.epochId);
      this.assertOpen();
      groupCoverage = { validation: "checked", paths };
    }
    let spaceCoverage: RecoveryStatus["spaces"] = { validation: "not-checked" };
    if ("__e2ee_spaces" in this.app && "__e2ee_space_recovery_deliveries" in this.app) {
      const spaces = new Spaces(
        this.db,
        this.account.id,
        this.app as DeviceTables & SpaceTables,
        keys,
        signer,
        (accountId) => JSON.stringify([accountRegistry(this.account), this.env, accountId]),
        () => this.assertOpen(),
        undefined,
        groups,
      );
      const paths = await spaces.inspectRecovery(material, account.epochId);
      this.assertOpen();
      spaceCoverage = { validation: "checked", paths };
    }
    return { configured: true, account, groups: groupCoverage, spaces: spaceCoverage };
  }

  private restoreProtectedRecovery(): Promise<void> {
    return this.withProtectedRecovery(async (material) => {
      await this.approval!.useRecovery(material);
      this.assertOpen();
      await this.groupLifecycle?.restoreRecovery(material);
      await this.spaceLifecycle?.restoreRecovery(material);
    });
  }

  private async withProtectedRecovery<T>(consume: (material: string) => Promise<T>): Promise<T> {
    const secret = parseAuthSecret(exportLocalFirstSecret(this.account));
    try {
      const cipher =
        this.config.crypto?.cellCipher ??
        (await (await import("./browser.js")).createBrowserCellCipher());
      const rows = await this.db.all(this.app.__e2ee_recovery_protectors, { tier: "edge" });
      let failure: unknown = new E2eeRecoveryError("recovery-protector-missing");
      for (const row of rows) {
        this.assertOpen();
        let material: string;
        try {
          material = await openRecoveryMaterial(
            cipher,
            secret,
            { application: this.scope, accountId: this.account.id, rootId: row.rootId },
            row.material,
          );
          if (JSON.parse(material).rootId !== row.rootId)
            throw new Error("E2EE recovery protector root mismatch");
        } catch {
          this.assertOpen();
          failure = new E2eeRecoveryError("recovery-protector-unusable");
          continue;
        }
        try {
          return await consume(material);
        } catch (error) {
          this.assertOpen();
          failure = error;
        }
      }
      throw failure;
    } finally {
      secret.fill(0);
    }
  }

  readonly devices = {
    list: async (): Promise<DeviceInfo[]> => {
      await this.prepare();
      const { approved, verified, revoked } = await this.approval!.deviceStates();
      const rows = await this.db.all(this.app.__e2ee_device_requests, { tier: "edge" });
      this.assertOpen();
      return rows.map((row) => ({
        id: row.id,
        state: revoked.has(row.id) ? "revoked" : approved.has(row.id) ? "active" : "pending",
        keyReadiness: verified.has(row.id) && !revoked.has(row.id) ? "verified" : "not-verified",
        publicKey: Uint8Array.from(row.publicKey),
        mechanism: { id: row.mechanism, version: row.version },
      }));
    },
    approve: (deviceId: string): { wait(): Promise<void> } => {
      const completion = (async () => {
        await this.prepare();
        await this.approval!.approve(deviceId);
      })();
      // The returned wait handle owns deferred errors, even when attached later.
      completion.catch(() => {});
      return { wait: () => completion };
    },
    revoke: (deviceId: string): { wait(): Promise<void> } => {
      const completion = (async () => {
        await this.prepare();
        await this.approval!.revoke(deviceId);
      })();
      completion.catch(() => {});
      return { wait: () => completion };
    },
  };

  constructor(
    private readonly db: Db,
    private readonly account: AccountHandle,
    private readonly config: E2eeConfig,
    private readonly env: string,
  ) {
    const app = config.app ?? deviceRequestApp;
    for (const name of Object.keys(deviceRequestSchema)) {
      if (!(name in app)) {
        throw new Error(`E2EE application is missing managed table "${name}"`);
      }
    }
    this.app = app as DeviceTables;
    configuredSchemas.set(db, this.app.__e2ee_device_requests._schema);
    this.scope = JSON.stringify([accountRegistry(account), env, account.id]);
    configureAcceptedHistory(db, {
      store: config.store,
      scope: this.scope,
      assertOpen: () => this.assertOpen(),
    });
    let cellCipher: Promise<CellCipher> | undefined;
    let equalityIndex: Promise<EqualityIndex> | undefined;
    equalityCrypto.set(db, async () => {
      this.assertOpen();
      equalityIndex ??= this.config.crypto?.equalityIndex
        ? Promise.resolve(this.config.crypto.equalityIndex)
        : import("./browser.js").then((module) => module.createBrowserEqualityIndex());
      const index = await equalityIndex;
      this.assertOpen();
      return index;
    });
    cellCrypto.set(db, async () => {
      this.assertOpen();
      cellCipher ??= this.config.crypto?.cellCipher
        ? Promise.resolve(this.config.crypto.cellCipher)
        : import("./browser.js").then((module) => module.createBrowserCellCipher());
      const cipher = await cellCipher;
      this.assertOpen();
      return { cipher, application: JSON.stringify([accountRegistry(this.account), this.env]) };
    });
    initialSpacePrerequisites.set(db, async (recipientIds) => {
      await this.prepare();
      await this.requireSpaces().warmInitialRecipients(recipientIds);
    });
    initialSpacePreparers.set(db, async (...args) => {
      await this.prepare();
      await this.requireSpaces().prepareInitial(...args);
    });
    currentSpaceKeys.set(
      db,
      async (scope, identifier, use, includeHistory, _prepareOnline, forWrite) => {
        await this.prepare(true);
        return this.requireSpaces().withKeys(
          scope,
          identifier,
          use,
          includeHistory,
          () => this.prepare(),
          forWrite,
        );
      },
    );
    db.onShutdown(() => {
      this.closed = true;
    });
  }

  private assertOpen(): void {
    if (this.closed) throw new Error("Cannot operate on a closed E2EE context");
    accountRegistry(this.account); // Reject logout, including during asynchronous preparation.
  }

  private async prepare(localOnly = false): Promise<void> {
    this.assertOpen();
    await (this.localPreparation ??= this.prepareRequest(true).catch((error) => {
      this.localPreparation = undefined;
      throw error;
    }));
    this.assertOpen();
    // Local key use must not join an unrelated, stalled online enrolment check.
    if (localOnly || (await this.db.e2eeIsExplicitlyOffline())) return;
    await (this.preparation ??= this.prepareRequest(false).catch((error) => {
      this.preparation = undefined;
      throw error;
    }));
    this.assertOpen();
  }

  private async prepareRequest(localOnly = false): Promise<void> {
    const envelope =
      this.config.crypto?.keyEnvelope ??
      (await (await import("./browser.js")).createBrowserKeyEnvelope());
    encodeEnvelope(envelope.mechanism, new Uint8Array());
    const signer =
      this.config.crypto?.deviceSigner ??
      (await (await import("./browser.js")).createBrowserDeviceSigner());
    encodeEnvelope(signer.mechanism, new Uint8Array());
    const device = await localDevice(this.config.store, this.scope, envelope, signer, () =>
      this.assertOpen(),
    );
    try {
      this.assertOpen();
      const requests = this.app.__e2ee_device_requests;
      // Once online preparation starts, complete it or reject so it can retry.
      // A later disconnect must not cache skipped enrolment as successful.
      if (!localOnly) {
        const query = requests.where({ id: device.id });
        let row = await this.db.one(query, { tier: "edge" });
        if (!row) {
          this.assertOpen();
          try {
            row = await this.db
              .insert(
                requests,
                {
                  publicKey: device.publicKey,
                  mechanism: envelope.mechanism.id,
                  version: envelope.mechanism.version,
                  challenge: device.challenge,
                  signingPublicKey: device.signing.publicKey,
                  signingMechanism: signer.mechanism.id,
                  signingVersion: signer.mechanism.version,
                },
                { id: device.id },
              )
              .wait({ tier: "edge" });
          } catch (error) {
            // Another context may have published this same durable proposal.
            row = await this.db.one(query, { tier: "edge" });
            if (!row) throw error;
          }
        }
        this.assertOpen();
        if (
          row.mechanism !== envelope.mechanism.id ||
          row.version !== envelope.mechanism.version ||
          row.signingMechanism !== signer.mechanism.id ||
          row.signingVersion !== signer.mechanism.version ||
          !sameBytes(row.signingPublicKey, device.signing.publicKey) ||
          !sameBytes(row.publicKey, device.publicKey) ||
          !sameBytes(row.challenge, device.challenge)
        )
          throw new Error("E2EE device request does not match the locally retained key");
        const publicKeys = this.app.__e2ee_device_keys;
        const publicQuery = publicKeys.where({ deviceId: device.id });
        if (!(await this.db.one(publicQuery, { tier: "edge" }))) {
          this.assertOpen();
          try {
            await this.db
              .insert(publicKeys, {
                deviceId: device.id,
                publicKey: row.publicKey,
                mechanism: row.mechanism,
                version: row.version,
                signingPublicKey: row.signingPublicKey,
                signingMechanism: row.signingMechanism,
                signingVersion: row.signingVersion,
              })
              .wait({ tier: "edge" });
          } catch (error) {
            // Concurrent contexts may publish the same immutable, policy-checked keys.
            if (!(await this.db.one(publicQuery, { tier: "edge" }))) throw error;
          }
        }
        this.assertOpen();
        await firstAccountEpoch(
          this.db,
          this.account.id,
          this.scope,
          device,
          envelope,
          () => this.assertOpen(),
          this.app,
        );
      }
      // Reconnection enrols once without adding another responder or key owner.
      if (this.approval) return;
      const retainedKey = device.privateKey.slice();
      const retainedSigningKey = device.signing.privateKey.slice();
      this.db.onShutdown(() => {
        retainedKey.fill(0);
        retainedSigningKey.fill(0);
      });
      const loadDevice = async () => ({
        ...device,
        privateKey: retainedKey.slice(),
        signing: { ...device.signing, privateKey: retainedSigningKey.slice() },
      });
      this.approval = new DeviceApproval(
        this.db,
        this.account.id,
        this.scope,
        envelope,
        signer,
        () => this.assertOpen(),
        this.app,
        { id: device.id, load: loadDevice },
      );
      if ("__e2ee_groups" in this.app && "__e2ee_group_deliveries" in this.app) {
        this.groupLifecycle = new Groups(
          this.db,
          this.account.id,
          this.scope,
          this.app as DeviceTables & GroupTables,
          envelope,
          signer,
          () => this.assertOpen(),
          (accountId) => JSON.stringify([accountRegistry(this.account), this.env, accountId]),
          {
            store: this.config.store,
            isKnownRevoked: () => this.approval!.isKnownRevoked(),
            load: loadDevice,
            states: (transaction) => this.approval!.deviceStates(transaction),
          },
        );
      }
      if (
        "__e2ee_spaces" in this.app &&
        "__e2ee_space_grants" in this.app &&
        "__e2ee_space_successors" in this.app &&
        "__e2ee_space_deliveries" in this.app
      ) {
        this.spaceLifecycle = new Spaces(
          this.db,
          this.account.id,
          this.app as DeviceTables & SpaceTables,
          envelope,
          signer,
          (accountId) => JSON.stringify([accountRegistry(this.account), this.env, accountId]),
          () => this.assertOpen(),
          {
            store: this.config.store,
            isKnownRevoked: () => this.approval!.isKnownRevoked(),
            load: loadDevice,
            states: (transaction) => this.approval!.deviceStates(transaction),
          },
          this.groupLifecycle,
          this.config.staleWrites,
        );
      }
    } finally {
      device.privateKey.fill(0);
      device.signing.privateKey.fill(0);
    }
  }
}

function sameBytes(left: Uint8Array, right: Uint8Array): boolean {
  return left.length === right.length && left.every((byte, index) => byte === right[index]);
}
