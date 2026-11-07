import { sha256 } from "@noble/hashes/sha2.js";
import { bytesToHex } from "@noble/hashes/utils.js";
import type { AccountStore } from "../accounts/persistence.js";
import type { WasmSchema } from "../drivers/types.js";
import type { Db, QueryBuilder, TableProxy } from "../runtime/db.js";
import type { LocalObservationSnapshot } from "../runtime/client.js";
import type { DeviceSigner, KeyEnvelope } from "./types.js";
import type { DeviceTables } from "./device-requests.js";
import type { SpaceRoot, SpaceTables } from "./spaces.js";
import {
  withLocalDeviceStore,
  type LocalDeviceProvider,
  type DeviceKeyLifetime,
} from "./local-device.js";
import { E2eeHistoryUnavailable } from "./history-reader.js";
import { acceptedDeviceTransaction, acceptedContentPositions } from "./accepted-membership.js";
import { getDbInternalSession } from "../runtime/db-internal-session.js";
import { runtimeRandomBytes } from "../runtime/runtime-entropy.js";
import { nativeCoreErrorCode } from "../runtime/native-runtime/native-error-code.js";

const FORMAT = "jazz-e2ee-retained-content-v1";
const WRITE_FORMAT = "jazz-e2ee-provisional-key-v1";
type ProvisionalKey = {
  format: typeof WRITE_FORMAT;
  binding: Binding;
  scopeTable: string;
  identifier: string;
  content: string;
  knownStale: boolean;
  signature: number[];
};
const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
type Origin = LocalObservationSnapshot["evidence"][number];
const PRIVATE_ANCHOR = "jazz-e2ee-private-online-anchor-v1";
type PrivateBinding = {
  format: typeof PRIVATE_ANCHOR;
  owner: string;
  session: string;
  account: string;
  namespaceTables: [string, string];
  acceptedSchema: string;
  through: string;
  table: string;
};
type Anchor = {
  rowId: string;
  body: string;
  origin: Origin;
  provenance: string;
  privateBinding?: PrivateBinding;
};
type Identities = Record<string, { table: string; column: string }>;
type StoredRoot = Omit<SpaceRoot, "verification" | "authorEnvelope" | "signature"> & {
  verification: number[];
  authorEnvelope: number[];
  signature: number[];
};
type Binding = {
  application: string;
  schema: string;
  device: string;
  keys: string;
  anchor: Anchor;
};
type DeviceRecord = { format: typeof FORMAT; binding: Binding; signature: number[] };
type ContentRecord = {
  format: typeof FORMAT;
  binding: Binding;
  scopeTable: string;
  root: StoredRoot;
  anchor: Anchor;
  identities: Identities;
  envelope: number[];
  signature: number[];
};
type Cache = {
  format: typeof FORMAT;
  revision: number;
  deniedDevices: string[];
  devices: DeviceRecord[];
  content: ContentRecord[];
  provisionalWritesV1?: ProvisionalKey[];
};

function canonical(value: unknown): unknown {
  if (value instanceof Uint8Array) return Array.from(value);
  if (value instanceof Date) return value.toISOString();
  if (typeof value === "bigint") return value.toString();
  if (Array.isArray(value)) return value.map(canonical);
  if (value && typeof value === "object")
    return Object.fromEntries(
      Object.entries(value)
        .filter(([, entry]) => entry !== undefined)
        .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
        .map(([name, entry]) => [name, canonical(entry)]),
    );
  return value;
}
function bytes(value: unknown): Uint8Array {
  return new TextEncoder().encode(JSON.stringify(canonical(value)));
}
function hash(value: unknown): string {
  return bytesToHex(sha256(bytes(value)));
}
function unsigned<T extends { signature: number[] }>(record: T) {
  const { signature: _signature, ...body } = record;
  return body;
}
/** @internal Versioned local signing/envelope context; not a wire certificate. */
export function retainedContentRecordBytes(record: object): Uint8Array {
  return bytes([FORMAT, record]);
}
const message = retainedContentRecordBytes;
/** @internal Separate versioned local context: provisional encryption, NOT authority. */
export function provisionalWriteRecordBytes(record: object): Uint8Array {
  return bytes([WRITE_FORMAT, record]);
}
function byteArray(value: unknown): value is number[] {
  return (
    Array.isArray(value) &&
    value.length > 0 &&
    value.length <= 65536 &&
    value.every((entry) => Number.isInteger(entry) && entry >= 0 && entry <= 255)
  );
}
function sameAnchor(a: Anchor, b: Anchor): boolean {
  const positive = (kind: string) => kind === "authority-read" || kind === "local-submission";
  // Retain each observed proof kind in the signed record. A normal change in
  // positive provenance classification grants no additional capability here.
  return (
    !!a &&
    !!b &&
    !!a.origin &&
    !!b.origin &&
    ((positive(a.provenance) && positive(b.provenance)) ||
      (a.provenance === PRIVATE_ANCHOR &&
        b.provenance === PRIVATE_ANCHOR &&
        !!a.privateBinding &&
        !!b.privateBinding &&
        hash({ ...a.privateBinding, through: undefined }) ===
          hash({ ...b.privateBinding, through: undefined }))) &&
    a.rowId === b.rowId &&
    a.body === b.body &&
    hash(a.origin) === hash(b.origin)
  );
}
function cache(value: unknown): Cache {
  if (value === undefined)
    return { format: FORMAT, revision: 0, deniedDevices: [], devices: [], content: [] };
  const result = value as Cache;
  if (
    !result ||
    result.format !== FORMAT ||
    !Number.isSafeInteger(result.revision) ||
    result.revision < 0 ||
    !Array.isArray(result.deniedDevices) ||
    result.deniedDevices.some((id) => typeof id !== "string") ||
    !Array.isArray(result.devices) ||
    !Array.isArray(result.content) ||
    (result.provisionalWritesV1 !== undefined && !Array.isArray(result.provisionalWritesV1))
  )
    throw new E2eeHistoryUnavailable("Invalid private retained-content cache");
  return result;
}
function decodeRoot(root: StoredRoot): SpaceRoot {
  if (
    !root ||
    ![
      root.id,
      root.scopeId,
      root.identifier,
      root.epochId,
      root.creatorAccountId,
      root.deviceId,
      root.accountEpochId,
      root.initialGrantId,
    ].every((id) => typeof id === "string" && uuid.test(id)) ||
    typeof root.mechanism !== "string" ||
    !Number.isSafeInteger(root.version) ||
    !byteArray(root.verification) ||
    !byteArray(root.authorEnvelope) ||
    !byteArray(root.signature)
  )
    throw new E2eeHistoryUnavailable("Invalid retained-content coordinates");
  return {
    ...root,
    verification: Uint8Array.from(root.verification),
    authorEnvelope: Uint8Array.from(root.authorEnvelope),
    signature: Uint8Array.from(root.signature),
  };
}

/**
 * Private, device-signed content capability recorded only after online validation.
 * Local anchors bind retained bytes to this store; they never prove membership,
 * latest epoch, absent revocation, SELECT coverage or permission to write.
 */
/** Owner-local validation receipt; never part of the persisted cache format. */
type RetentionRevision = Readonly<{ stored: number; generation: symbol }>;

export class RetainedContentCache {
  private readonly schema: string;
  private readonly pendingIdentities = new Map<string, Identities>();
  private readonly borrowed = new WeakMap<Readonly<SpaceRoot>, Identities>();
  private volatile?: Cache;
  private readonly deniedScopes = new Set<string>();
  private readonly staleScopes = new Set<string>();
  private deniedOwner = false;
  private retentionGeneration = Symbol("retained-content invalidation");
  // Neither these capabilities nor their owner token are exported as C proof.
  private readonly privateOwner = bytesToHex(runtimeRandomBytes(32));
  private readonly privateAnchors = new Map<string, Anchor>();

  private privateQueryKey(query: QueryBuilder<{ id: string }>): string {
    return hash([query._table, query._build(), this.schema]);
  }
  private privateSession(): { account: string; fingerprint: string } {
    this.assertOpen();
    const session = getDbInternalSession(this.db);
    let scope: unknown;
    try {
      scope = JSON.parse(this.application);
    } catch {
      /* Refuse non-account scopes. */
    }
    if (!session?.account_id || !Array.isArray(scope) || scope[2] !== session.account_id)
      throw new E2eeHistoryUnavailable(
        "Private retention requires the actual authenticated account",
      );
    return { account: session.account_id, fingerprint: hash(session) };
  }
  private rootMatches(row: Record<string, unknown>, root: Readonly<SpaceRoot>): boolean {
    return (
      row.scopeId === root.scopeId &&
      row.identifier === root.identifier &&
      row.creatorAccountId === root.creatorAccountId &&
      row.deviceId === root.deviceId &&
      row.accountEpochId === root.accountEpochId &&
      row.initialGrantId === root.initialGrantId
    );
  }

  /** PRIVATE post-validation retention only; never C import or an authority grant.
   * Both rows are genuine paired AcceptedRows at ONE cut, then Global-confirmed.
   * The live Db and its catalogue IDs bind the actual namespace/owner lifetime.
   */
  async captureOnlineBinding(root: Readonly<SpaceRoot>): Promise<void> {
    this.assertOpen();
    const knownDevice = await this.deviceBinding(false, false);
    const knownRoot = await this.anchor(
      this.tables.__e2ee_spaces.where({ id: root.id }),
      (row) => this.rootMatches(row, root),
      false,
      false,
    );
    if (
      knownDevice &&
      knownRoot &&
      knownDevice.anchor.provenance !== PRIVATE_ANCHOR &&
      knownRoot.provenance !== PRIVATE_ANCHOR
    )
      return;
    if (!this.db.e2eeHasConnectedTransportForKeys())
      throw new E2eeHistoryUnavailable("Private binding requires online accepted validation");
    const session = this.privateSession();
    const queries = [
      this.tables.__e2ee_device_keys.where({ deviceId: this.device.id }),
      this.tables.__e2ee_spaces.where({ id: root.id }),
    ] as const;
    const tableIds = await Promise.all([
      this.db.tableIdentity(queries[0]),
      this.db.tableIdentity(queries[1]),
    ]);
    if (tableIds.some((id) => typeof id !== "string" || !uuid.test(id)))
      throw new E2eeHistoryUnavailable("Missing private namespace catalogue binding");
    const read = await acceptedDeviceTransaction(this.db, async (tx) => {
      const device = await tx.readAccepted(queries[0]);
      const space = await tx.readAccepted(queries[1]);
      return { device, space };
    });
    const snapshots = await read.wait();
    this.assertOpen();
    if (this.privateSession().fingerprint !== session.fingerprint)
      throw new E2eeHistoryUnavailable("Private binding session changed");
    const afterIds = await Promise.all([
      this.db.tableIdentity(queries[0]),
      this.db.tableIdentity(queries[1]),
    ]);
    if (hash(afterIds) !== hash(tableIds))
      throw new E2eeHistoryUnavailable("Private binding namespace changed");
    const { device, space } = snapshots;
    acceptedContentPositions(device);
    acceptedContentPositions(space);
    if (
      device.rows.length !== 1 ||
      space.rows.length !== 1 ||
      !device.schema ||
      device.schema !== space.schema ||
      device.through !== space.through
    )
      throw new E2eeHistoryUnavailable("Private binding requires one exact same-cut row pair");
    const loaded = await this.device.load();
    try {
      const row = device.rows[0]!;
      if (
        row.deviceId !== this.device.id ||
        hash([row.publicKey, row.signingPublicKey]) !==
          hash([loaded.publicKey, loaded.signing.publicKey]) ||
        row.mechanism !== this.envelope.mechanism.id ||
        row.version !== this.envelope.mechanism.version ||
        row.signingMechanism !== this.signer.mechanism.id ||
        row.signingVersion !== this.signer.mechanism.version ||
        !this.rootMatches(space.rows[0]!, root)
      )
        throw new E2eeHistoryUnavailable("Private binding key or scope mismatch");
      this.assertOpen();
      if (this.privateSession().fingerprint !== session.fingerprint)
        throw new E2eeHistoryUnavailable("Private binding owner changed");
      for (const [index, snapshot] of [device, space].entries()) {
        const row = snapshot.rows[0]!;
        const anchor: Anchor = {
          rowId: row.id,
          body: hash(row),
          origin: structuredClone(snapshot.evidence[0]!),
          provenance: PRIVATE_ANCHOR,
          privateBinding: {
            format: PRIVATE_ANCHOR,
            owner: this.privateOwner,
            session: session.fingerprint,
            account: session.account,
            namespaceTables: tableIds as [string, string],
            acceptedSchema: snapshot.schema,
            through: snapshot.through,
            table: queries[index]!._table,
          },
        };
        this.privateAnchors.set(this.privateQueryKey(queries[index]!), anchor);
      }
    } finally {
      this.device.release(loaded);
    }
  }

  private async privateAnchor(
    query: QueryBuilder<{ id: string }>,
    matches?: (row: Record<string, unknown>) => boolean,
  ): Promise<Anchor | undefined> {
    const anchor = this.privateAnchors.get(this.privateQueryKey(query));
    if (!anchor) return;
    const binding = anchor.privateBinding!;
    if (
      binding.owner !== this.privateOwner ||
      binding.session !== this.privateSession().fingerprint
    )
      throw new E2eeHistoryUnavailable("Private retained owner binding changed");
    // Local body equality is content binding only, NOT current authority or origins.
    // The signed origin pair remains the exact prior online accepted observation.
    const rows = await this.db.all(query, { tier: "local-first" });
    this.assertOpen();
    if (binding.session !== this.privateSession().fingerprint)
      throw new E2eeHistoryUnavailable("Private retained owner changed during read");
    if (
      rows.length !== 1 ||
      rows[0]!.id !== anchor.rowId ||
      hash(rows[0]) !== anchor.body ||
      (matches && !matches(rows[0]!))
    )
      return;
    return structuredClone(anchor);
  }

  private scopeKey(table: string, identifier: string): string {
    return JSON.stringify([table, identifier]);
  }
  private mergeVolatile(current: Cache): Cache {
    if (!this.volatile || current.revision !== this.volatile.revision) return current;
    const merge = <T>(rows: T[], extra: T[], key: (value: T) => string) => [
      ...new Map([...rows, ...extra].map((row) => [key(row), row])).values(),
    ];
    return {
      ...current,
      deniedDevices: [...new Set([...current.deniedDevices, ...this.volatile.deniedDevices])],
      devices: merge(current.devices, this.volatile.devices, (row) => hash(row.binding)),
      content: merge(current.content, this.volatile.content, (row) =>
        hash([row.binding, row.root.id, row.root.epochId]),
      ),
      provisionalWritesV1: merge(
        current.provisionalWritesV1 ?? [],
        this.volatile.provisionalWritesV1 ?? [],
        (row) => hash([row.binding, row.scopeTable, row.identifier]),
      ),
    };
  }

  constructor(
    private readonly db: Db,
    private readonly store: AccountStore,
    private readonly application: string,
    schema: WasmSchema,
    private readonly tables: DeviceTables & SpaceTables,
    private readonly device: LocalDeviceProvider,
    private readonly lifetime: DeviceKeyLifetime,
    private readonly envelope: KeyEnvelope,
    private readonly signer: DeviceSigner,
    private readonly assertOpen: () => void,
  ) {
    this.schema = hash(schema);
  }

  private owner() {
    return JSON.stringify([this.application, this.device.id]);
  }
  private matches(binding: Binding): boolean {
    return (
      binding?.application === this.application &&
      binding.schema === this.schema &&
      binding.device === this.device.id
    );
  }
  private read(): Promise<Cache> {
    return this.store.read().then((value) => {
      this.assertOpen();
      return withLocalDeviceStore(value, this.lifetime, (state) =>
        this.mergeVolatile(
          structuredClone(
            cache((state as typeof state & { retainedContentV1?: Cache }).retainedContentV1),
          ),
        ),
      );
    });
  }
  private async update(change: (current: Cache) => void, optionalRetention = false): Promise<void> {
    let candidate: Cache | undefined;
    let persisted = false;
    const previousVolatile = this.volatile;
    try {
      await this.store.update((value) => {
        this.assertOpen();
        return withLocalDeviceStore(value, this.lifetime, (state) => {
          const stored = state as typeof state & { retainedContentV1?: Cache };
          const current = this.mergeVolatile(cache(stored.retainedContentV1));
          change(current);
          candidate = structuredClone(current);
          stored.retainedContentV1 = current;
          return JSON.stringify(stored);
        });
      });
      persisted = true;
    } catch (error) {
      this.assertOpen();
      // Only optional retention may fall back to an owner-local signed bundle.
      // Invalidation/policy errors still fail; a new owner cannot assume durability.
      if (!optionalRetention || !candidate) throw error;
    }
    if (persisted) {
      if (this.volatile === previousVolatile) this.volatile = undefined;
    } else if (candidate) this.volatile = candidate;
    this.assertOpen();
  }
  async revision(): Promise<RetentionRevision> {
    // Capture before the asynchronous store read and before caller validation.
    const generation = this.retentionGeneration;
    return Object.freeze({ stored: (await this.read()).revision, generation });
  }

  private async anchor(
    query: QueryBuilder<{ id: string }>,
    matches?: (row: Record<string, unknown>) => boolean,
    privateOnly = false,
    allowPrivate = true,
  ): Promise<Anchor | undefined> {
    let observations: Awaited<ReturnType<Db["observeE2eeHistory"]>>;
    try {
      observations = await this.db.observeE2eeHistory([query]);
    } catch (error) {
      // Preserve actual native owner/policy failures. No C evidence is created.
      if (nativeCoreErrorCode(error) === "not_observed") observations = [];
      else throw error;
    }
    this.assertOpen();
    const [observed] = observations;
    if (
      !privateOnly &&
      observed &&
      observed.rows.length === 1 &&
      observed.settlements.length === 1 &&
      observed.provenance.length === 1
    ) {
      const row = observed.rows[0]!,
        origin = observed.settlements[0]!;
      if (
        (!matches || matches(row)) &&
        origin.rowId === row.id &&
        ["authority-read", "local-submission"].includes(observed.provenance[0]!)
      )
        return { rowId: row.id, body: hash(row), origin, provenance: observed.provenance[0]! };
    }
    return allowPrivate ? this.privateAnchor(query, matches) : undefined;
  }
  private async deviceBinding(
    privateOnly = false,
    allowPrivate = true,
  ): Promise<Binding | undefined> {
    const loaded = await this.device.load();
    try {
      this.assertOpen();
      const anchor = await this.anchor(
        this.tables.__e2ee_device_keys.where({ deviceId: this.device.id }),
        (row) =>
          hash([row.publicKey, row.signingPublicKey]) ===
            hash([loaded.publicKey, loaded.signing.publicKey]) &&
          row.mechanism === this.envelope.mechanism.id &&
          row.version === this.envelope.mechanism.version &&
          row.signingMechanism === this.signer.mechanism.id &&
          row.signingVersion === this.signer.mechanism.version,
        privateOnly,
        allowPrivate,
      );
      if (!anchor) return;
      return {
        application: this.application,
        schema: this.schema,
        device: this.device.id,
        keys: hash([
          loaded.publicKey,
          loaded.signing.publicKey,
          this.envelope.mechanism,
          this.signer.mechanism,
        ]),
        anchor,
      };
    } finally {
      this.device.release(loaded);
    }
  }
  private async sign(record: object): Promise<number[]> {
    const loaded = await this.device.load();
    try {
      const signature = await this.signer.sign(loaded.signing.privateKey, message(record));
      this.assertOpen();
      if (!(await this.signer.verify(loaded.signing.publicKey, message(record), signature)))
        throw new E2eeHistoryUnavailable("Invalid private retained-content signature");
      this.assertOpen();
      return Array.from(signature);
    } finally {
      this.device.release(loaded);
    }
  }
  private async valid(record: DeviceRecord | ContentRecord, binding: Binding): Promise<boolean> {
    if (
      !record ||
      record.format !== FORMAT ||
      !this.matches(record.binding) ||
      record.binding.keys !== binding.keys ||
      !sameAnchor(record.binding.anchor, binding.anchor) ||
      !byteArray(record.signature)
    )
      return false;
    if ("anchor" in record && record.anchor.provenance === PRIVATE_ANCHOR) {
      const deviceCut = record.binding.anchor.privateBinding;
      const rootCut = record.anchor.privateBinding;
      if (
        !deviceCut ||
        !rootCut ||
        deviceCut.through !== rootCut.through ||
        deviceCut.acceptedSchema !== rootCut.acceptedSchema ||
        deviceCut.owner !== rootCut.owner ||
        deviceCut.session !== rootCut.session ||
        hash(deviceCut.namespaceTables) !== hash(rootCut.namespaceTables)
      )
        return false;
    }
    const loaded = await this.device.load();
    try {
      const result = await this.signer.verify(
        loaded.signing.publicKey,
        message(unsigned(record)),
        Uint8Array.from(record.signature),
      );
      this.assertOpen();
      return result;
    } finally {
      this.device.release(loaded);
    }
  }

  /** Caller has just completed real accepted device membership and Global checks. */
  async retainDevice(): Promise<void> {
    const binding = await this.deviceBinding();
    if (!binding) return;
    const body: Omit<DeviceRecord, "signature"> = { format: FORMAT, binding };
    const record: DeviceRecord = { ...body, signature: await this.sign(body) };
    await this.update((current) => {
      if (current.deniedDevices.includes(this.owner()) || this.deniedOwner) return;
      current.devices = [...current.devices.filter((row) => !this.matches(row.binding)), record];
    }, true);
  }
  async available(): Promise<boolean> {
    if (this.deniedOwner) return false;
    const current = await this.read();
    if (current.deniedDevices.includes(this.owner())) return false;
    const binding = await this.deviceBinding();
    if (!binding) return false;
    for (const record of current.devices) if (await this.valid(record, binding)) return true;
    if (this.privateAnchors.size && binding.anchor.provenance !== PRIVATE_ANCHOR) {
      const privateBinding = await this.deviceBinding(true);
      if (privateBinding)
        for (const record of current.devices)
          if (await this.valid(record, privateBinding)) return true;
    }
    return false;
  }
  async denyDevice(): Promise<void> {
    this.retentionGeneration = Symbol("retained-content invalidation");
    this.deniedOwner = true;
    await this.update((current) => {
      current.revision++;
      if (!current.deniedDevices.includes(this.owner())) current.deniedDevices.push(this.owner());
      current.devices = current.devices.filter((row) => !this.matches(row.binding));
      current.content = current.content.filter((row) => !this.matches(row.binding));
      current.provisionalWritesV1 = (current.provisionalWritesV1 ?? []).filter(
        (row) => !this.matches(row.binding),
      );
    });
  }
  async denySpace(scopeTable: string, identifier: string): Promise<void> {
    this.retentionGeneration = Symbol("retained-content invalidation");
    this.deniedScopes.add(this.scopeKey(scopeTable, identifier));
    await this.update((current) => {
      current.revision++;
      current.content = current.content.filter(
        (row) =>
          !this.matches(row.binding) ||
          row.scopeTable !== scopeTable ||
          row.root.identifier !== identifier,
      );
      current.provisionalWritesV1 = (current.provisionalWritesV1 ?? []).filter(
        (row) =>
          !this.matches(row.binding) ||
          row.scopeTable !== scopeTable ||
          row.identifier !== identifier,
      );
    });
  }
  async clearContent(): Promise<void> {
    this.retentionGeneration = Symbol("retained-content invalidation");
    await this.update((current) => {
      current.revision++;
      current.content = current.content.filter((row) => !this.matches(row.binding));
      current.provisionalWritesV1 = (current.provisionalWritesV1 ?? []).filter(
        (row) => !this.matches(row.binding),
      );
    });
  }

  rememberIdentities(
    root: Readonly<SpaceRoot>,
    table: string,
    column: string,
    tableId: string,
    columnId: string,
  ): void {
    this.assertOpen();
    if (!uuid.test(tableId) || !uuid.test(columnId))
      throw new E2eeHistoryUnavailable("Invalid content catalogue identity");
    const key = JSON.stringify([root.id, root.epochId]);
    let entries = this.pendingIdentities.get(key);
    if (!entries) this.pendingIdentities.set(key, (entries = {}));
    entries[JSON.stringify([table, column])] = { table: tableId, column: columnId };
  }
  /** Structural identities are available only during this owner's verified key borrow. */
  isBorrowing(root: Readonly<SpaceRoot>): boolean {
    this.assertOpen();
    return this.borrowed.has(root);
  }

  identities(root: Readonly<SpaceRoot>, table: string, column: string) {
    this.assertOpen();
    const result = this.borrowed.get(root)?.[JSON.stringify([table, column])];
    if (!result || !uuid.test(result.table) || !uuid.test(result.column))
      throw new E2eeHistoryUnavailable("Missing retained content catalogue binding");
    return result;
  }

  /** Never call this for a provisional key or before its authority wait completes. */
  async retainSpace(
    scopeTable: string,
    root: Readonly<SpaceRoot>,
    secret: Uint8Array,
    revision: RetentionRevision,
    provisionalWrite = false,
  ): Promise<void> {
    if (
      revision.generation !== this.retentionGeneration ||
      secret.length !== 32 ||
      !(await this.available())
    )
      return;
    const anchor = await this.anchor(
      this.tables.__e2ee_spaces.where({ id: root.id }),
      (row) =>
        row.scopeId === root.scopeId &&
        row.identifier === root.identifier &&
        row.creatorAccountId === root.creatorAccountId &&
        row.deviceId === root.deviceId &&
        row.accountEpochId === root.accountEpochId &&
        row.initialGrantId === root.initialGrantId,
    );
    if (!anchor) return;
    const binding = await this.deviceBinding(anchor.provenance === PRIVATE_ANCHOR);
    if (!binding) return;
    if (anchor.provenance === PRIVATE_ANCHOR || binding.anchor.provenance === PRIVATE_ANCHOR) {
      const deviceCut = binding.anchor.privateBinding;
      const rootCut = anchor.privateBinding;
      if (
        !deviceCut ||
        !rootCut ||
        deviceCut.through !== rootCut.through ||
        deviceCut.acceptedSchema !== rootCut.acceptedSchema ||
        deviceCut.owner !== rootCut.owner ||
        deviceCut.session !== rootCut.session ||
        hash(deviceCut.namespaceTables) !== hash(rootCut.namespaceTables)
      )
        throw new E2eeHistoryUnavailable(
          "Private retained rows do not share the same accepted cut",
        );
    }
    const storedRoot: StoredRoot = {
      ...root,
      verification: Array.from(root.verification),
      authorEnvelope: Array.from(root.authorEnvelope),
      signature: Array.from(root.signature),
    };
    decodeRoot(storedRoot);
    const body: Omit<ContentRecord, "envelope" | "signature"> = {
      format: FORMAT,
      binding,
      scopeTable,
      root: storedRoot,
      anchor,
      identities: structuredClone(
        this.pendingIdentities.get(JSON.stringify([root.id, root.epochId])) ?? {},
      ),
    };
    const loaded = await this.device.load();
    let sealed: Uint8Array;
    try {
      sealed = await this.envelope.seal(loaded.publicKey, message(body), secret);
      this.assertOpen();
    } finally {
      this.device.release(loaded);
    }
    const unsignedRecord = { ...body, envelope: Array.from(sealed) };
    const record: ContentRecord = { ...unsignedRecord, signature: await this.sign(unsignedRecord) };
    const writeBody: Omit<ProvisionalKey, "signature"> = {
      format: WRITE_FORMAT,
      binding,
      scopeTable,
      identifier: root.identifier,
      content: hash(record),
      knownStale: false,
    };
    const write = provisionalWrite
      ? { ...writeBody, signature: await this.signWrite(writeBody) }
      : undefined;
    let retained = false;
    await this.update((current) => {
      if (
        revision.generation !== this.retentionGeneration ||
        current.revision !== revision.stored ||
        current.deniedDevices.includes(this.owner()) ||
        this.deniedOwner
      )
        return;
      current.content = [
        ...current.content.filter(
          (row) =>
            !this.matches(row.binding) ||
            row.root.id !== root.id ||
            row.root.epochId !== root.epochId,
        ),
        record,
      ];
      if (write)
        current.provisionalWritesV1 = [
          ...(current.provisionalWritesV1 ?? []).filter(
            (row) =>
              !this.matches(row.binding) ||
              row.scopeTable !== scopeTable ||
              row.identifier !== root.identifier,
          ),
          write,
        ];
      retained = true;
    }, true);
    // Persistence itself may yield across a newer failed invalidation.
    if (retained && revision.generation === this.retentionGeneration) {
      this.deniedScopes.delete(this.scopeKey(scopeTable, root.identifier));
      if (write) this.staleScopes.delete(this.scopeKey(scopeTable, root.identifier));
    }
  }

  private async signWrite(record: object): Promise<number[]> {
    const loaded = await this.device.load();
    try {
      const signature = await this.signer.sign(
        loaded.signing.privateKey,
        provisionalWriteRecordBytes(record),
      );
      this.assertOpen();
      if (
        !(await this.signer.verify(
          loaded.signing.publicKey,
          provisionalWriteRecordBytes(record),
          signature,
        ))
      )
        throw new E2eeHistoryUnavailable("Invalid provisional-key signature");
      return Array.from(signature);
    } finally {
      this.device.release(loaded);
    }
  }

  /** One-way prior accepted maintenance knowledge; this never supplies a key. */
  isKnownStale(scopeTable: string, identifier: string): boolean {
    this.assertOpen();
    return this.staleScopes.has(this.scopeKey(scopeTable, identifier));
  }

  /** Records a genuinely observed maintenance requirement, not an offline inference. */
  async markStale(scopeTable: string, identifier: string): Promise<void> {
    this.retentionGeneration = Symbol("retained-content invalidation");
    const wasStale = this.staleScopes.has(this.scopeKey(scopeTable, identifier));
    this.staleScopes.add(this.scopeKey(scopeTable, identifier));
    const before = await this.read();
    const originals = (before.provisionalWritesV1 ?? []).filter(
      (row) =>
        this.matches(row.binding) && row.scopeTable === scopeTable && row.identifier === identifier,
    );
    if (wasStale && originals.every((row) => row.knownStale)) return;
    const changed = await Promise.all(
      originals.map(async (row) => {
        const body = { ...unsigned(row), knownStale: true };
        return { ...body, signature: await this.signWrite(body) };
      }),
    );
    await this.update((current) => {
      current.revision++;
      current.provisionalWritesV1 = (current.provisionalWritesV1 ?? []).map(
        (row) => changed.find((next, i) => hash(originals[i]) === hash(row)) ?? row,
      );
    });
  }

  /** Borrow ONE prior-online-validated key for a pending local write, never membership. */
  async withProvisionalKey<T, Init>(
    scope: TableProxy<T, Init>,
    identifier: string,
    use: (secret: Uint8Array, root: Readonly<SpaceRoot>) => Promise<void>,
    staleWrites: "warn" | "reject",
  ): Promise<"used" | "unavailable" | "stale"> {
    if (
      hash(scope._schema) !== this.schema ||
      this.deniedScopes.has(this.scopeKey(scope._table, identifier)) ||
      !(await this.available())
    )
      return "unavailable";
    const current = await this.read();
    const candidates = (current.provisionalWritesV1 ?? []).filter(
      (row) =>
        this.matches(row.binding) &&
        row.scopeTable === scope._table &&
        row.identifier === identifier,
    );
    if (candidates.length !== 1) return "unavailable";
    const selection = candidates[0]!;
    const binding = await this.deviceBinding(
      selection.binding?.anchor?.provenance === PRIVATE_ANCHOR,
    );
    if (!binding) return "unavailable";
    const loaded = await this.device.load();
    try {
      if (
        selection.format !== WRITE_FORMAT ||
        !byteArray(selection.signature) ||
        typeof selection.knownStale !== "boolean" ||
        selection.binding.keys !== binding.keys ||
        !sameAnchor(selection.binding.anchor, binding.anchor) ||
        !(await this.signer.verify(
          loaded.signing.publicKey,
          provisionalWriteRecordBytes(unsigned(selection)),
          Uint8Array.from(selection.signature),
        ))
      )
        throw new E2eeHistoryUnavailable("Invalid provisional-key record");
    } finally {
      this.device.release(loaded);
    }
    this.assertOpen();
    const stale =
      selection.knownStale || this.staleScopes.has(this.scopeKey(scope._table, identifier));
    if (stale && staleWrites === "reject") return "stale";
    const matches = current.content.filter((row) => hash(row) === selection.content);
    if (matches.length !== 1) return "unavailable";
    if (stale)
      console.warn(
        "E2EE: writing offline with a known-stale encryption epoch; removed recipients may still decrypt this write.",
      );
    const scopeKey = this.scopeKey(scope._table, identifier);
    const guardedUse: typeof use = async (secret, root) => {
      if (staleWrites === "reject" && this.staleScopes.has(scopeKey))
        throw new E2eeHistoryUnavailable("Provisional key became known-stale during use");
      await use(secret, root);
      if (staleWrites === "reject" && this.staleScopes.has(scopeKey))
        throw new E2eeHistoryUnavailable("Provisional key became known-stale during use");
      if (!stale && this.staleScopes.has(scopeKey))
        console.warn(
          "E2EE: writing offline with a known-stale encryption epoch; removed recipients may still decrypt this write.",
        );
    };
    return (await this.borrowRecord(matches[0]!, current, binding, guardedUse))
      ? "used"
      : "unavailable";
  }

  async withKeys<T, Init>(
    scope: TableProxy<T, Init>,
    identifier: string,
    use: (secret: Uint8Array, root: Readonly<SpaceRoot>) => Promise<void>,
  ): Promise<boolean> {
    if (
      hash(scope._schema) !== this.schema ||
      this.deniedScopes.has(this.scopeKey(scope._table, identifier)) ||
      !(await this.available())
    )
      return false;
    const current = await this.read();
    const binding = await this.deviceBinding();
    if (!binding) return false;
    let found = false;
    for (const record of current.content) {
      if (
        !this.matches(record.binding) ||
        record.scopeTable !== scope._table ||
        record.root.identifier !== identifier
      )
        continue;
      if (await this.borrowRecord(record, current, binding, use)) found = true;
    }
    return found;
  }
  private async borrowRecord(
    record: ContentRecord,
    current: Cache,
    binding: Binding,
    use: (secret: Uint8Array, root: Readonly<SpaceRoot>) => Promise<void>,
  ): Promise<boolean> {
    if (
      this.deniedOwner ||
      this.deniedScopes.has(this.scopeKey(record.scopeTable, record.root.identifier))
    )
      return false;
    const recordBinding = await this.deviceBinding(record.anchor?.provenance === PRIVATE_ANCHOR);
    if (!recordBinding) return false;
    binding = recordBinding;
    if (!(await this.valid(record, binding)) || !byteArray(record.envelope))
      throw new E2eeHistoryUnavailable("Invalid private retained-content record");
    const root = Object.freeze(decodeRoot(record.root));
    const anchor = await this.anchor(
      this.tables.__e2ee_spaces.where({ id: root.id }),
      undefined,
      record.anchor.provenance === PRIVATE_ANCHOR,
    );
    if (!anchor || !sameAnchor(anchor, record.anchor)) return false;
    const { envelope: _envelope, signature: _signature, ...body } = record;
    const loaded = await this.device.load();
    let secret: Uint8Array | undefined;
    try {
      secret = this.lifetime.own(
        await this.envelope.open(loaded, message(body), Uint8Array.from(record.envelope)),
      );
      this.assertOpen();
      if (secret.length !== 32) throw new E2eeHistoryUnavailable("Invalid retained content key");
      if (
        this.deniedOwner ||
        this.deniedScopes.has(this.scopeKey(record.scopeTable, root.identifier))
      )
        throw new E2eeHistoryUnavailable("Retained key owner was invalidated");
      const beforeUse = await this.read();
      if (
        beforeUse.revision !== current.revision ||
        this.deniedOwner ||
        this.deniedScopes.has(this.scopeKey(record.scopeTable, root.identifier))
      )
        throw new E2eeHistoryUnavailable("Retained key was invalidated before use");
      this.borrowed.set(root, record.identities);
      await use(secret, root);
      this.assertOpen();
      if (record.anchor.provenance === PRIVATE_ANCHOR) {
        // Recheck the real native owner fence after awaited crypto/use too.
        const afterAnchor = await this.anchor(
          this.tables.__e2ee_spaces.where({ id: root.id }),
          undefined,
          true,
        );
        if (!afterAnchor || !sameAnchor(afterAnchor, record.anchor))
          throw new E2eeHistoryUnavailable("Private retained owner or content changed during use");
      }
      const after = await this.read();
      if (
        after.revision !== current.revision ||
        after.deniedDevices.includes(this.owner()) ||
        this.deniedOwner ||
        this.deniedScopes.has(this.scopeKey(record.scopeTable, root.identifier))
      )
        throw new E2eeHistoryUnavailable("Retained content was invalidated during use");
      return true;
    } finally {
      this.borrowed.delete(root);
      if (secret) this.lifetime.release(secret);
      this.device.release(loaded);
    }
  }
}
