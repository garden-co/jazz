import { acceptedDeviceTransaction } from "./accepted-membership.js";
import type { AccountStore } from "../accounts/persistence.js";
import type { WasmSchema } from "../drivers/types.js";
import { encryptedSchemas } from "./encrypted-schema.js";
import { accountRegistry, exportLocalFirstSecret } from "../accounts/enrollment.js";
import { parseAuthSecret } from "../runtime/auth-secret-codec.js";
import { openRecoveryMaterial, protectRecoveryMaterial } from "./recovery-protection.js";
import { E2eeRecoveryError, RecoveryCandidateError } from "./recovery-error.js";
import { recoveryMaterialRootId } from "./recovery-format.js";
import { E2eeDataError } from "./data-error.js";
import type { AccountHandle } from "../accounts/state.js";
import {
  beginDbTransactionAfter,
  prepareDbTransaction,
  transactionHasRegisteredInsert,
} from "../runtime/db.js";
import type {
  Db,
  TableProxy,
  Transaction,
  PreparedTransactionScope,
  QueryBuilder,
} from "../runtime/db.js";
import type { AcceptedRows } from "../runtime/accepted-read.js";
import { resolveSchemaSource } from "../schema-source.js";
import { TypedTableQueryBuilder } from "../typed-app.js";
import { deviceRequestApp, deviceRequestSchema } from "./device-requests.js";
import type { DeviceTables } from "./device-requests.js";
import { encodeEnvelope } from "./envelope.js";
import type { CryptoMechanism } from "./envelope.js";
import { DeviceKeyLifetime, localDevice, retainedLocalDevice } from "./local-device.js";
import type { LocalDeviceProvider } from "./local-device.js";
import { E2eeHistoryUnavailable } from "./history-reader.js";
import { RetainedContentCache } from "./retained-content-cache.js";
import { firstAccountEpoch } from "./first-epoch.js";
import { DeviceApproval } from "./device-approval.js";
import {
  prefetchPublicMembershipHistory,
  readPublicMembershipHistory,
  replayAccountMembership,
} from "./public-membership.js";
import { Groups } from "./group-lifecycle.js";
import type { GroupRecoveryPath } from "./group-lifecycle.js";
import { groupSchema, type GroupMember, type GroupTables } from "./groups.js";
import { Spaces } from "./space-lifecycle.js";
import type { SpaceDataPreparation, SpaceRecoveryPath } from "./space-lifecycle.js";
import { spaceSchema, type SpaceTables, type SpaceRoot } from "./spaces.js";
import type { CellCipher, DeviceSigner, JazzCrypto, KeyEnvelope } from "./types.js";

export type E2eeConfig = {
  /** Application returned by defineApp; encrypted apps include managed bindings automatically. */
  app?: DeviceTables | { readonly wasmSchema: WasmSchema };
  /** Dedicated local key storage. Do not reuse the account-selection store value. */
  store: AccountStore;
  crypto?: JazzCrypto;
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
const startupPreparers = new WeakMap<Db, () => Promise<void>>();

/** @internal Encrypted application factories finish readiness before exposing the Db. */
export async function prepareE2eeStartup(db: Db): Promise<void> {
  const schema = configuredSchemas.get(db);
  if (schema && encryptedSchemas.has(schema)) await startupPreparers.get(db)!();
}
const cellCrypto = new WeakMap<Db, () => Promise<{ cipher: CellCipher; application: string }>>();

/** @internal Common cell framing owns identity selection, not the crypto adapter. */
export async function cellCryptoForDb(
  db: Db,
): Promise<{ cipher: CellCipher; application: string }> {
  e2eeForDb(db);
  return cellCrypto.get(db)!();
}
const configuredSchemas = new WeakMap<Db, WasmSchema>();
/** @internal The configured application can bind a cold transaction's schema. */
export function e2eeSchemaForDb(db: Db): WasmSchema | undefined {
  return configuredSchemas.get(db);
}
type BorrowSpaceKeys = <T, Init>(
  scope: TableProxy<T, Init>,
  identifier: string,
  use: Parameters<Spaces["withKeys"]>[2],
  includeHistory?: boolean,
  contentRead?: boolean,
) => ReturnType<Spaces["withKeys"]>;
const currentSpaceKeys = new WeakMap<Db, BorrowSpaceKeys>();
const retainedCaches = new WeakMap<Db, () => Promise<RetainedContentCache | undefined>>();

const initialSpacePreparers = new WeakMap<Db, Spaces["prepareInitial"]>();
const initialSpaceAuthorityPreparers = new WeakMap<Db, Spaces["prepareInitialAuthority"]>();
type InitialAuthorityMemo = WeakMap<WasmSchema, Map<string, AcceptedRows<unknown>>>;
const initialAuthorityMemos = new WeakMap<
  Transaction<"serializable">,
  { recipients: string; reads: InitialAuthorityMemo }
>();
const initialSpacePrerequisites = new WeakMap<
  Db,
  (recipientIds?: readonly string[]) => Promise<void>
>();

/** @internal Cell operations borrow a verified key; never exported by the public API. */
export async function withSpaceKeys<T, Init>(
  db: Db,
  scope: TableProxy<T, Init>,
  identifier: string,
  use: Parameters<Spaces["withKeys"]>[2],
  includeHistory = false,
): Promise<void> {
  return borrowSpaceKeys(db, scope, identifier, use, includeHistory, false);
}

/** @internal Only decryption may use a prior online-validated private key offline. */
export async function withContentSpaceKeys<T, Init>(
  db: Db,
  scope: TableProxy<T, Init>,
  identifier: string,
  use: Parameters<Spaces["withKeys"]>[2],
  includeHistory = true,
): Promise<void> {
  return borrowSpaceKeys(db, scope, identifier, use, includeHistory, true);
}

async function borrowSpaceKeys<T, Init>(
  db: Db,
  scope: TableProxy<T, Init>,
  identifier: string,
  use: Parameters<Spaces["withKeys"]>[2],
  includeHistory: boolean,
  contentRead: boolean,
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
    contentRead,
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

/** @internal Cached catalogue IDs bind content only, never structural freshness. */
export async function contentIdentitiesForDb(
  db: Db,
  table: TableProxy<unknown, unknown>,
  column: string,
  root: Readonly<SpaceRoot>,
): Promise<{ table: string; column: string }> {
  e2eeForDb(db);
  const cache = await retainedCaches.get(db)!();
  if (await db.e2eeIsExplicitlyOffline()) {
    try {
      if (!cache) throw new E2eeHistoryUnavailable("Missing private content cache");
      return cache.identities(root, table._table, column);
    } catch {
      throw new E2eeDataError("key-unavailable");
    }
  }
  const tableId = await db.tableIdentity(table);
  const columnId = await db.columnIdentity(table, column);
  if (!tableId || !columnId) throw new E2eeDataError("key-unavailable");
  cache?.rememberIdentities(root, table._table, column, tableId, columnId);
  return { table: tableId, column: columnId };
}

/** @internal Finish initial-space maintenance only after the actual Global wait. */
export async function retainCommittedSpaceContent(
  db: Db,
  roots: readonly { root: Readonly<SpaceRoot>; scopeTable: string }[],
): Promise<void> {
  const e2ee = e2eeForDb(db);
  for (const { root, scopeTable } of roots) await e2ee.retainCommittedContent(root, scopeTable);
}

/** @internal Cold recipient discovery precedes the immutable transaction snapshot. */
export function beginInitialSpaceTransaction(
  db: Db,
  recipientIds?: readonly string[],
): Transaction<"serializable"> {
  const recipients = recipientIds?.slice();
  const transaction = beginDbTransactionAfter(
    db,
    async () => {
      e2eeForDb(db);
      await initialSpacePrerequisites.get(db)!(recipients);
    },
    "accepted-evidence",
  );
  registerInitialSpaceAuthority(db, transaction, recipients);
  return transaction;
}

/** @internal Register before the automatic compiler stages its first scope row. */
export function registerInitialSpaceAuthority(
  db: Db,
  transaction: Transaction<"serializable">,
  recipientIds?: readonly string[],
): void {
  const recipients = recipientIds?.slice();
  const previous = initialAuthorityMemos.get(transaction);
  if (previous) {
    if (previous.recipients !== JSON.stringify(recipients ?? null))
      throw new Error("Initial space recipients changed after authority registration");
    return;
  }
  const memo: InitialAuthorityMemo = new WeakMap();
  initialAuthorityMemos.set(transaction, {
    recipients: JSON.stringify(recipients ?? null),
    reads: memo,
  });
  // Register before returning: even an immediate caller insert remains behind
  // this authority-only prefix in the normal FIFO preparation queue.
  const preparing = prepareDbTransaction(transaction, async (scope) => {
    await initialSpaceAuthorityPreparers.get(db)!(
      {
        ...scope,
        async readAccepted(query) {
          const result = await scope.readAccepted(query);
          scope.assertActive();
          let queries = memo.get(query._schema);
          if (!queries) memo.set(query._schema, (queries = new Map()));
          queries.set(query._build(), structuredClone(result));
          return result;
        },
      },
      recipients,
    );
  });
  // The registered queue owns this error and poisons commit if the prefix fails.
  preparing.catch(() => {});
}

/** @internal Not exported from the package API; used by transaction preparation. */
export async function prepareInitialSpaceForTransaction<T, Init>(
  db: Db,
  tx: Transaction<"serializable">,
  scope: TableProxy<T, Init>,
  identifier: string,
  prepareData: SpaceDataPreparation,
  recipientIds?: readonly string[],
): Promise<void> {
  const recipients = recipientIds?.slice();
  const ownScopeInsert = transactionHasRegisteredInsert(tx, scope, identifier);
  try {
    await prepareDbTransaction(tx, async (prepared) => {
      await prepareRegisteredInitialSpaceRows(
        db,
        tx,
        prepared,
        scope,
        identifier,
        prepareData,
        recipients,
        ownScopeInsert,
      );
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

/** @internal Reuse the same exact pre-write predicates inside registered preparation. */
export async function prepareRegisteredInitialSpaceRows<T, Init>(
  db: Db,
  transaction: Transaction<"serializable">,
  prepared: PreparedTransactionScope,
  scope: TableProxy<T, Init>,
  identifier: string,
  prepareData: SpaceDataPreparation,
  recipients?: readonly string[],
  ownScopeInsert = false,
): Promise<void> {
  const authority = initialAuthorityMemos.get(transaction);
  if (!authority) throw new Error("Initial space authority was not registered before staging");
  if (authority.recipients !== JSON.stringify(recipients ?? null))
    throw new Error("Initial space recipients changed after authority registration");
  const memo = authority.reads;
  await prepareInitialSpaceRows(
    db,
    {
      ...prepared,
      async readAccepted<T>(query: QueryBuilder<T>): Promise<AcceptedRows<T>> {
        prepared.assertActive();
        const result = memo.get(query._schema)?.get(query._build());
        // A no-write preparation can still request a real same-cut scope
        // predicate. After any staged operation the existing core guard
        // rejects an uncaptured read; never synthesize a cached answer.
        if (!result) return prepared.readAccepted(query);
        // These are exact responses from this transaction's authority
        // prefix, not local-cache evidence or a later independent cut.
        const reply = await Promise.resolve(structuredClone(result) as AcceptedRows<T>);
        prepared.assertActive();
        return reply;
      },
    },
    scope,
    identifier,
    prepareData,
    recipients,
    ownScopeInsert,
  );
}

/** @internal Reuses an already-prepared transaction; never queues behind itself. */
export async function prepareInitialSpaceRows<T, Init>(
  db: Db,
  tx: PreparedTransactionScope,
  scope: TableProxy<T, Init>,
  identifier: string,
  prepareData: SpaceDataPreparation,
  recipientIds?: readonly string[],
  ownScopeInsert = false,
): Promise<void> {
  e2eeForDb(db);
  await initialSpacePreparers.get(db)!(
    tx,
    scope,
    identifier,
    prepareData,
    recipientIds,
    ownScopeInsert,
  );
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
  private recoveryReader:
    | { approval: DeviceApproval; keys: KeyEnvelope; signer: DeviceSigner }
    | undefined;
  private groupLifecycle: Groups | undefined;
  private spaceLifecycle: Spaces | undefined;
  private closed = false;
  private readonly deviceKeys = new DeviceKeyLifetime();
  private preparation: Promise<boolean> | undefined;
  private locallyPrepared = false;
  private contentCache: RetainedContentCache | undefined;
  private currentDeviceId: string | undefined;
  private readonly scope: string;
  private readonly app: DeviceTables;
  readonly groups = {
    leave: (groupId: string): { wait(): Promise<void> } =>
      this.groups.remove(groupId, { kind: "account", id: this.account.id }),
    remove: (groupId: string, member: GroupMember): { wait(): Promise<void> } => {
      const completion = (async () => {
        await this.prepare();
        const result = await this.requireGroups().remove(groupId, member);
        await this.contentCache?.clearContent();
        return result;
      })();
      completion.catch(() => {});
      return { wait: () => completion };
    },
    add: (groupId: string, member: GroupMember): { wait(): Promise<void> } => {
      const completion = (async () => {
        await this.prepare();
        return this.requireGroups().add(groupId, member);
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
        const result = await this.requireSpaces().revoke(scope, identifier, recipientId);
        if (recipientId === this.account.id)
          await this.contentCache?.denySpace(scope._table, identifier);
        return result;
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

  /**
   * Scope readiness concerns usable content keys. Explicitly offline, ready
   * means only retained, previously validated content can be decrypted; it
   * does not certify current membership, absent revocation or write authority.
   * Group membership readiness always requires online accepted validation.
   */
  async explain<T, Init>(
    target: { groupId: string } | { scope: TableProxy<T, Init>; identifier: string },
  ) {
    if (await this.db.e2eeIsExplicitlyOffline()) {
      if (!("scope" in target))
        throw new E2eeHistoryUnavailable("Offline content does not certify group membership");
      const cache = await this.prepareRetainedContent();
      const available = await cache.withKeys(target.scope, target.identifier, async () => {});
      return available
        ? { state: "ready" as const }
        : { state: "unavailable" as const, reason: "retained-content-unavailable" };
    }
    await this.prepare();
    if ("scope" in target) {
      const state = await this.requireSpaces().explain(target.scope, target.identifier);
      if (state.state === "ready" && this.contentCache)
        return currentSpaceKeys.get(this.db)!(
          target.scope,
          target.identifier,
          async () => {},
          true,
        );
      if (state.reason === "device-not-active") await this.contentCache?.denyDevice();
      else if (state.state === "refused" || state.state === "maintenance-required")
        await this.contentCache?.denySpace(target.scope._table, target.identifier);
      return state;
    }
    return this.requireGroups().explain(target.groupId);
  }

  /** @internal Only invoked after the compiler's actual Global acceptance floor. */
  async retainCommittedContent(root: Readonly<SpaceRoot>, scopeTable: string): Promise<void> {
    const schema = configuredSchemas.get(this.db);
    if (!schema || !this.contentCache) return;
    const scope = new TypedTableQueryBuilder(scopeTable, schema);
    await this.explain({ scope, identifier: root.identifier });
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
      const read = await acceptedDeviceTransaction(this.db, (tx) =>
        readPublicMembershipHistory(tx, this.account.id, this.app),
      );
      const history = await read.wait();
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
            const rootId = recoveryMaterialRootId(result.material);
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
    revoke: (rootId: string): { wait(): Promise<void> } => {
      const completion = (async () => {
        await this.prepare();
        await this.approval!.retireRecoveryRoot(rootId);
      })();
      completion.catch(() => {});
      return { wait: () => completion };
    },
  };

  private async inspectRecovery(material: string): Promise<RecoveryStatus> {
    let reader = this.recoveryReader;
    if (!reader) {
      const keys =
        this.config.crypto?.keyEnvelope ??
        (await (await import("./browser.js")).createBrowserKeyEnvelope());
      const signer =
        this.config.crypto?.deviceSigner ??
        (await (await import("./browser.js")).createBrowserDeviceSigner());
      this.assertOpen();
      // No device, responder or retained recovery key. Avoid repeating the initial prefetch;
      // every inspection still reads and accepts a fresh transaction snapshot.
      const approval = new DeviceApproval(
        this.db,
        this.account.id,
        this.scope,
        keys,
        signer,
        () => this.assertOpen(),
        this.app,
      );
      reader = this.recoveryReader = { approval, keys, signer };
    }
    const account = await reader.approval.inspectRecovery(material);
    this.assertOpen();
    let groups: Groups | undefined;
    let groupCoverage: RecoveryStatus["groups"] = { validation: "not-checked" };
    if ("__e2ee_groups" in this.app && "__e2ee_group_recovery_deliveries" in this.app) {
      groups = new Groups(
        this.db,
        this.account.id,
        this.scope,
        this.app as DeviceTables & GroupTables,
        reader.keys,
        reader.signer,
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
        reader.keys,
        reader.signer,
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
      const rows = await this.db.all(this.app.__e2ee_recovery_protectors, { tier: "remote" });
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
          if (recoveryMaterialRootId(material) !== row.rootId)
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
          if (
            !(error instanceof RecoveryCandidateError) ||
            (error.code !== "recovery-material-unusable" &&
              error.code !== "recovery-root-mismatch" &&
              error.code !== "recovery-delivery-missing" &&
              error.code !== "recovery-delivery-unusable" &&
              error.code !== "recovery-group-delivery-unavailable" &&
              error.code !== "recovery-space-delivery-unavailable")
          )
            throw error;
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
      const requests = this.app.__e2ee_device_requests.where({
        "$createdBy.account": this.account.id,
      });
      const {
        state: { approved, verified, revoked },
        rows,
      } = {
        state: await this.approval!.deviceStates(),
        rows: await this.db.all(requests, { tier: "remote" }),
      };
      this.assertOpen();
      if (this.currentDeviceId && revoked.has(this.currentDeviceId))
        await this.contentCache?.denyDevice();
      else if (
        this.currentDeviceId &&
        approved.has(this.currentDeviceId) &&
        verified.has(this.currentDeviceId)
      )
        await this.contentCache?.retainDevice();
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
        if (deviceId === this.currentDeviceId) await this.contentCache?.denyDevice();
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
    const schema = "wasmSchema" in app ? resolveSchemaSource(app) : undefined;
    const tables: Record<string, unknown> = {};
    for (const name of Object.keys(deviceRequestSchema) as (keyof DeviceTables)[]) {
      if (schema ? !(name in schema) : !(name in app)) {
        throw new Error(`E2EE application is missing managed table "${name}"`);
      }
      // Keep supplied query builders and their transforms. A schema-only app
      // uses the same builders against its full schema, including user tables.
      tables[name] =
        name in app ? (app as DeviceTables)[name] : new TypedTableQueryBuilder(name, schema!);
    }
    for (const name of [...Object.keys(groupSchema), ...Object.keys(spaceSchema)] as (
      | keyof GroupTables
      | keyof SpaceTables
    )[]) {
      if (schema ? name in schema : name in app) {
        tables[name] =
          name in app
            ? (app as DeviceTables & Partial<GroupTables & SpaceTables>)[name]
            : new TypedTableQueryBuilder(name, schema!);
      }
    }
    const groupTables = Object.keys(groupSchema);
    if (groupTables.some((name) => name in tables)) {
      for (const name of groupTables) {
        if (!(name in tables))
          throw new Error(`E2EE application is missing managed table "${name}"`);
      }
    }
    this.app = tables as DeviceTables;
    configuredSchemas.set(db, this.app.__e2ee_device_requests._schema);
    this.scope = JSON.stringify([accountRegistry(account), env, account.id]);
    startupPreparers.set(db, async () => {
      if (await this.prepareRequest(true)) return;
      await this.prepare();
    });
    db.onE2eeReconnect(() => {
      this.preparation = undefined;
    });
    let cellCipher: Promise<CellCipher> | undefined;
    cellCrypto.set(db, async () => {
      this.assertOpen();
      cellCipher ??= this.config.crypto?.cellCipher
        ? Promise.resolve(this.config.crypto.cellCipher)
        : import("./browser.js").then((module) => module.createBrowserCellCipher());
      const cipher = await cellCipher;
      this.assertOpen();
      return { cipher, application: JSON.stringify([accountRegistry(this.account), this.env]) };
    });
    retainedCaches.set(db, async () => {
      if (await db.e2eeIsExplicitlyOffline()) return this.prepareRetainedContent();
      await this.prepare();
      return this.contentCache;
    });
    currentSpaceKeys.set(db, async (scope, identifier, use, includeHistory, contentRead) => {
      if (await db.e2eeIsExplicitlyOffline()) {
        if (!contentRead)
          throw new E2eeHistoryUnavailable(
            "Retained content does not authorise an encrypted write",
          );
        const cache = await this.prepareRetainedContent();
        const available = await cache.withKeys(scope, identifier, use);
        return available
          ? { state: "ready" }
          : { state: "unavailable", reason: "retained-content-unavailable" };
      }
      await this.prepare();
      const cache = this.contentCache;
      const revision = await cache?.revision();
      const state = await this.requireSpaces().withKeys(
        scope,
        identifier,
        async (secret, root) => {
          await use(secret, root);
          if (cache && revision !== undefined) {
            // Space key access already completed its accepted device/recipient
            // predicate checks and Global wait. This marker is content-only.
            await cache.retainDevice();
            await cache.retainSpace(scope._table, root, secret, revision);
          }
        },
        includeHistory,
      );
      if (state.reason === "device-not-active") await cache?.denyDevice();
      else if (state.state === "refused" || state.state === "maintenance-required")
        await cache?.denySpace(scope._table, identifier);
      return state;
    });
    initialSpacePrerequisites.set(db, async (recipientIds) => {
      await this.prepare();
      await this.requireSpaces().warmInitialRecipients(recipientIds);
    });
    initialSpacePreparers.set(db, async (...args) => {
      await this.prepare();
      await this.requireSpaces().prepareInitial(...args);
    });
    initialSpaceAuthorityPreparers.set(db, async (...args) => {
      await this.prepare();
      await this.requireSpaces().prepareInitialAuthority(...args);
    });
    db.onShutdown(() => {
      this.closed = true;
      this.recoveryReader = undefined;
      this.deviceKeys.close();
    });
  }

  private assertOpen(): void {
    if (this.closed) throw new Error("Cannot operate on a closed E2EE context");
    accountRegistry(this.account); // Reject logout, including during asynchronous preparation.
  }

  private async prepare(): Promise<void> {
    this.assertOpen();
    if (this.approval?.isKnownRevoked()) {
      await this.contentCache?.denyDevice();
      throw new E2eeHistoryUnavailable("E2EE device is revoked");
    }
    if (await this.db.e2eeIsExplicitlyOffline()) {
      throw new E2eeHistoryUnavailable(
        "Online authority is required; private offline keys are content-only",
      );
    }
    await (this.preparation ??= this.prepareRequest().catch((error) => {
      this.preparation = undefined;
      throw error;
    }));
    this.assertOpen();
  }

  private async prepareRetainedContent(): Promise<RetainedContentCache> {
    this.assertOpen();
    if ((!this.locallyPrepared || !this.contentCache) && !(await this.prepareRequest(true)))
      throw new E2eeHistoryUnavailable("Missing retained E2EE content readiness");
    if (this.approval?.isKnownRevoked()) {
      await this.contentCache?.denyDevice();
      throw new E2eeHistoryUnavailable("Retained content owner is revoked");
    }
    if (!this.contentCache || !(await this.contentCache.available()))
      throw new E2eeHistoryUnavailable("Missing bound retained E2EE content");
    return this.contentCache;
  }

  private async prepareRequest(retainedOnly = false): Promise<boolean> {
    const envelope =
      this.config.crypto?.keyEnvelope ??
      (await (await import("./browser.js")).createBrowserKeyEnvelope());
    encodeEnvelope(envelope.mechanism, new Uint8Array());
    const signer =
      this.config.crypto?.deviceSigner ??
      (await (await import("./browser.js")).createBrowserDeviceSigner());
    encodeEnvelope(signer.mechanism, new Uint8Array());
    const device = await (retainedOnly ? retainedLocalDevice : localDevice)(
      this.config.store,
      this.scope,
      envelope,
      signer,
      this.deviceKeys,
      () => this.assertOpen(),
    );
    if (!device) return false;
    this.currentDeviceId = device.id;
    try {
      this.assertOpen();
      const requests = this.app.__e2ee_device_requests;
      // Once online preparation starts, complete it or reject so it can retry.
      // A later disconnect must not cache skipped enrolment as successful.
      if (!retainedOnly) {
        const query = requests.where({ id: device.id });
        let row = await this.db.one(query, { tier: "remote" });
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
              .wait({ tier: "global" });
          } catch (error) {
            // Another context may have published this same durable proposal.
            row = await this.db.one(query, { tier: "remote" });
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
        if (!(await this.db.one(publicQuery, { tier: "remote" }))) {
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
              .wait({ tier: "global" });
          } catch (error) {
            // Concurrent contexts may publish the same immutable, policy-checked keys.
            if (!(await this.db.one(publicQuery, { tier: "remote" }))) throw error;
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
      // Reconnection reuses its existing responder and key owner.
      if (!this.approval) {
        this.assertOpen();
        const retainedKey = this.deviceKeys.own(device.privateKey.slice());
        let retainedSigningKey: Uint8Array | undefined;
        try {
          const signingKey = this.deviceKeys.own(device.signing.privateKey.slice());
          retainedSigningKey = signingKey;
          const provider: LocalDeviceProvider = {
            id: device.id,
            load: async () => {
              this.assertOpen();
              const privateKey = this.deviceKeys.own(retainedKey.slice());
              try {
                return {
                  ...device,
                  privateKey,
                  signing: {
                    ...device.signing,
                    privateKey: this.deviceKeys.own(signingKey.slice()),
                  },
                };
              } catch (error) {
                this.deviceKeys.release(privateKey);
                throw error;
              }
            },
            release: (loaded) => {
              this.deviceKeys.release(loaded.privateKey);
              this.deviceKeys.release(loaded.signing.privateKey);
            },
          };
          this.approval = new DeviceApproval(
            this.db,
            this.account.id,
            this.scope,
            envelope,
            signer,
            () => this.assertOpen(),
            this.app,
            provider,
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
                id: provider.id,
                store: this.config.store,
                isKnownRevoked: () => this.approval!.isKnownRevoked(),
                load: provider.load,
                release: provider.release,
                keyLifetime: this.deviceKeys,
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
                id: provider.id,
                store: this.config.store,
                isKnownRevoked: () => this.approval!.isKnownRevoked(),
                load: provider.load,
                release: provider.release,
                keyLifetime: this.deviceKeys,
                states: (transaction) => this.approval!.deviceStates(transaction),
              },
              this.groupLifecycle,
            );
            const schema = configuredSchemas.get(this.db)!;
            if (encryptedSchemas.has(schema))
              this.contentCache = new RetainedContentCache(
                this.db,
                this.config.store,
                this.scope,
                schema,
                this.app as DeviceTables & SpaceTables,
                provider,
                this.deviceKeys,
                envelope,
                signer,
                () => this.assertOpen(),
              );
          }
        } catch (error) {
          this.deviceKeys.release(retainedKey);
          if (retainedSigningKey) this.deviceKeys.release(retainedSigningKey);
          throw error;
        }
      }
      if (retainedOnly) {
        this.locallyPrepared = !!this.contentCache && (await this.contentCache.available());
        return this.locallyPrepared;
      }
      if (this.contentCache) {
        const state = await this.approval.deviceStates();
        if (state.revoked.has(device.id)) await this.contentCache.denyDevice();
        else if (state.active.has(device.id) && state.verified.has(device.id))
          await this.contentCache.retainDevice();
      }
      this.locallyPrepared = true;
      this.approval.startResponder();
      return true;
    } finally {
      this.deviceKeys.release(device.privateKey);
      this.deviceKeys.release(device.signing.privateKey);
    }
  }
}

function sameBytes(left: Uint8Array, right: Uint8Array): boolean {
  return left.length === right.length && left.every((byte, index) => byte === right[index]);
}
