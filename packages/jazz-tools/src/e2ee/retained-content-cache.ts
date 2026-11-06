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
import { nativeCoreErrorCode } from "../runtime/native-runtime/native-error-code.js";

const FORMAT = "jazz-e2ee-retained-content-v1";
const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
type Origin = LocalObservationSnapshot["evidence"][number];
type Anchor = { rowId: string; body: string; origin: Origin; provenance: string };
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
    positive(a.provenance) &&
    positive(b.provenance) &&
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
    !Array.isArray(result.content)
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
export class RetainedContentCache {
  private readonly schema: string;
  private readonly pendingIdentities = new Map<string, Identities>();
  private readonly borrowed = new WeakMap<Readonly<SpaceRoot>, Identities>();

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
        structuredClone(
          cache((state as typeof state & { retainedContentV1?: Cache }).retainedContentV1),
        ),
      );
    });
  }
  private async update(change: (current: Cache) => void): Promise<void> {
    await this.store.update((value) => {
      this.assertOpen();
      return withLocalDeviceStore(value, this.lifetime, (state) => {
        const stored = state as typeof state & { retainedContentV1?: Cache };
        const current = cache(stored.retainedContentV1);
        change(current);
        stored.retainedContentV1 = current;
        return JSON.stringify(stored);
      });
    });
    this.assertOpen();
  }
  async revision(): Promise<number> {
    return (await this.read()).revision;
  }

  private async anchor(
    query: QueryBuilder<{ id: string }>,
    matches?: (row: Record<string, unknown>) => boolean,
  ): Promise<Anchor | undefined> {
    let observations: Awaited<ReturnType<Db["observeE2eeHistory"]>>;
    try {
      observations = await this.db.observeE2eeHistory([query]);
    } catch (error) {
      // A fresh store without an admitted namespace has no positive binding.
      // Do not reinterpret owner retirement, storage or other failures.
      if (nativeCoreErrorCode(error) === "not_observed") return undefined;
      throw error;
    }
    const [observed] = observations;
    this.assertOpen();
    if (
      !observed ||
      observed.rows.length !== 1 ||
      observed.settlements.length !== 1 ||
      observed.provenance.length !== 1
    )
      return;
    const row = observed.rows[0]!;
    if (matches && !matches(row)) return;
    const origin = observed.settlements[0]!;
    if (
      origin.rowId !== row.id ||
      !["authority-read", "local-submission"].includes(observed.provenance[0]!)
    )
      return;
    return { rowId: row.id, body: hash(row), origin, provenance: observed.provenance[0]! };
  }
  private async deviceBinding(): Promise<Binding | undefined> {
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
      if (current.deniedDevices.includes(this.owner())) return;
      current.devices = [...current.devices.filter((row) => !this.matches(row.binding)), record];
    });
  }
  async available(): Promise<boolean> {
    const current = await this.read();
    if (current.deniedDevices.includes(this.owner())) return false;
    const binding = await this.deviceBinding();
    if (!binding) return false;
    for (const record of current.devices) if (await this.valid(record, binding)) return true;
    return false;
  }
  async denyDevice(): Promise<void> {
    await this.update((current) => {
      current.revision++;
      if (!current.deniedDevices.includes(this.owner())) current.deniedDevices.push(this.owner());
      current.devices = current.devices.filter((row) => !this.matches(row.binding));
      current.content = current.content.filter((row) => !this.matches(row.binding));
    });
  }
  async denySpace(scopeTable: string, identifier: string): Promise<void> {
    await this.update((current) => {
      current.revision++;
      current.content = current.content.filter(
        (row) =>
          !this.matches(row.binding) ||
          row.scopeTable !== scopeTable ||
          row.root.identifier !== identifier,
      );
    });
  }
  async clearContent(): Promise<void> {
    await this.update((current) => {
      current.revision++;
      current.content = current.content.filter((row) => !this.matches(row.binding));
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
    revision: number,
  ): Promise<void> {
    if (secret.length !== 32 || !(await this.available())) return;
    const binding = await this.deviceBinding();
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
    if (!binding || !anchor) return;
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
    await this.update((current) => {
      if (current.revision !== revision || current.deniedDevices.includes(this.owner())) return;
      current.content = [
        ...current.content.filter(
          (row) =>
            !this.matches(row.binding) ||
            row.root.id !== root.id ||
            row.root.epochId !== root.epochId,
        ),
        record,
      ];
    });
  }

  async withKeys<T, Init>(
    scope: TableProxy<T, Init>,
    identifier: string,
    use: (secret: Uint8Array, root: Readonly<SpaceRoot>) => Promise<void>,
  ): Promise<boolean> {
    if (hash(scope._schema) !== this.schema || !(await this.available())) return false;
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
      if (!(await this.valid(record, binding)) || !byteArray(record.envelope))
        throw new E2eeHistoryUnavailable("Invalid private retained-content record");
      const root = Object.freeze(decodeRoot(record.root));
      const anchor = await this.anchor(this.tables.__e2ee_spaces.where({ id: root.id }));
      if (!anchor || !sameAnchor(anchor, record.anchor)) continue;
      const { envelope: _envelope, signature: _signature, ...body } = record;
      const loaded = await this.device.load();
      let secret: Uint8Array | undefined;
      try {
        secret = this.lifetime.own(
          await this.envelope.open(loaded, message(body), Uint8Array.from(record.envelope)),
        );
        this.assertOpen();
        if (secret.length !== 32) throw new E2eeHistoryUnavailable("Invalid retained content key");
        this.borrowed.set(root, record.identities);
        await use(secret, root);
        this.assertOpen();
        const after = await this.read();
        if (after.revision !== current.revision || after.deniedDevices.includes(this.owner()))
          throw new E2eeHistoryUnavailable("Retained content was invalidated during use");
        found = true;
      } finally {
        this.borrowed.delete(root);
        if (secret) this.lifetime.release(secret);
        this.device.release(loaded);
      }
    }
    return found;
  }
}
