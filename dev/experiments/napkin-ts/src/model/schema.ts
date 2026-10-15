// Jazz schema metadata and lowering to groove storage schemas.
import { SortedMap, SortedSet, cmpStr } from "../support/collections.js";
import { utf8, type Bytes } from "../support/bytes.js";
import { uuidV5, uuidFromString } from "../support/hash.js";
import {
  RecordDescriptor,
  EnumSchema,
  EnumCase,
  ScalarEnumSchema,
  SystemVariantRegistry,
  GrooveDatabaseSchema,
  DirectRecordStoreSchema,
  GrooveIndexSchema,
  GrooveTableSchema,
  GrooveColumnSchema,
  PrimaryKey,
  PrimaryKeyColumn,
  StorageLayout,
  physicalStorageValueType,
  isInternalStorageType,
  valueTypeEquals,
  RowAuthor,
  AuthorSubject,
  SchemaVersionId,
  type GValue,
  type GValueType,
  type Query,
} from "../_extern/model-core.js";
import { BranchColumnValue, BranchKey, BranchSelector } from "./model/branch.js";
import { type Schema as PublicSchema, newSchema } from "./model/public_api/types/schema.js";
import { defaultTablePolicies } from "./model/public_api/types/policy.js";
import { policyBindingDirectoryClaimsValueType } from "./model/policy_directory.js";
import { RejectionReason } from "./tx.js";
import { convertPublicSchema } from "./model/public_schema_convert.js";
export { SchemaConversionError } from "./model/public_schema_convert.js";

export const SCHEMA_VERSION_NAMESPACE = uuidFromString("61b9ef21-3195-50e8-87fc-2aa83a6f74e3");
export const AUTHORITY_POLICY_BINDINGS_STORE = "jazz_authority_policy_bindings";
export const SUBSCRIPTION_WATERMARKS_STORE = "jazz_subscription_watermarks_v1";
export const LOCAL_ROW_AVAILABILITY_STORE = "jazz_local_row_availability_v1";
export const SUPERSEDED_WRITES_STORE = "jazz_superseded_writes_v1";

// ---- groove value-type helpers ----
const T = (kind: string): GValueType => ({ kind });
const nullable = (inner: GValueType): GValueType => ({ kind: "Nullable", inner });
const arrayOf = (inner: GValueType): GValueType => ({ kind: "Array", inner });
const record = (fields: [string, GValueType][]): GValueType => ({
  kind: "Record",
  descriptor: new RecordDescriptor(fields),
});
const U8 = T("U8"),
  U32 = T("U32"),
  U64 = T("U64"),
  BYTES = T("Bytes"),
  UUID = T("Uuid"),
  STRING = T("String"),
  BOOL = T("Bool");

/** Source-backed Jazz schema accepted by public database APIs. */
export class JazzSchema {
  constructor(
    public public_schema: PublicSchema,
    public runtime: RuntimeSchema,
  ) {}

  static empty(): JazzSchema {
    return JazzSchema.compile(newSchema());
  }
  static compile(schema: PublicSchema): JazzSchema {
    return convertPublicSchema(schema);
  }
  static fromRuntime(public_schema: PublicSchema, runtime: RuntimeSchema): JazzSchema {
    return new JazzSchema(public_schema, runtime);
  }
  equals(o: JazzSchema): boolean {
    return this.runtime.equals(o.runtime);
  }
  get tables(): TableSchema[] {
    return this.runtime.tables;
  }

  clone(): JazzSchema {
    return new JazzSchema(
      newSchema([...this.public_schema].map(([k, v]) => [k, { ...v }])),
      new RuntimeSchema(this.runtime.tables.map(cloneTable)),
    );
  }

  /** Structural catalogue payload, independent of the active permissions. */
  withoutPermissions(): JazzSchema {
    const s = this.clone();
    for (const t of s.public_schema.valuesArray()) t.policies = defaultTablePolicies();
    for (const t of s.runtime.tables) {
      t.read_policy = undefined;
      t.write_policies = {};
    }
    return s;
  }

  hasSameLineageStructure(other: JazzSchema): boolean {
    const normalize = (s: JazzSchema) =>
      new RuntimeSchema(
        s.runtime.tables.map((t) => {
          const c = cloneTable(t);
          c.read_policy = undefined;
          c.write_policies = {};
          c.indexed_columns = new SortedSet(cmpStr);
          c.columns = c.columns.map((col) =>
            c.branch_by.includes(col.name) ? col : { ...col, default: undefined },
          );
          return c;
        }),
      );
    return normalize(this).equals(normalize(other));
  }

  withPermissionsFrom(selected: JazzSchema): JazzSchema {
    if (!this.versionId().equals(selected.versionId()))
      throw new Error("assertion `left == right` failed");
    const s = this.clone();
    for (const [name, t] of s.public_schema) {
      const sel = selected.public_schema.get(name);
      if (!sel) throw new Error("same schema tables");
      t.policies = sel.policies;
    }
    for (const t of s.runtime.tables) {
      const sel = selected.runtime.tables.find((o) => o.name === t.name);
      if (!sel) throw new Error("same schema tables");
      t.read_policy = sel.read_policy;
      t.write_policies = sel.write_policies;
    }
    return s;
  }

  versionId(): any {
    return this.runtime.versionId();
  }
}

function cloneTable(t: TableSchema): TableSchema {
  return {
    ...t,
    columns: t.columns.slice(),
    branch_by: t.branch_by.slice(),
    write_policies: { ...t.write_policies },
  };
}

/** Compiled logical schema used internally by the Jazz engine. */
export class RuntimeSchema {
  constructor(public tables: TableSchema[]) {}

  equals(o: RuntimeSchema): boolean {
    return (
      this.tables.length === o.tables.length &&
      this.tables.every((t, i) => tableSchemaEquals(t, o.tables[i]))
    );
  }

  projectBranchSelector(
    table: TableSchema,
    selector: BranchSelector,
  ): [BranchKey, SortedMap<string, GValue>] {
    if (selector.values.size !== table.branch_by.length)
      throw new Error(
        `branch selector for ${table.name} must provide exactly ${table.branch_by.length} values`,
      );
    const values: [string, BranchColumnValue][] = [];
    const cells = new SortedMap<string, GValue>(cmpStr);
    for (const name of table.branch_by) {
      const column = table.columns.find((c) => c.name === name);
      if (!column)
        throw new Error(`unknown branch column ${JSON.stringify(name)} on ${table.name}`);
      const encoded = selector.values.get(name);
      if (!encoded) throw new Error(`missing branch column ${name}`);
      let value: GValue;
      try {
        value = encoded.decode();
      } catch {
        throw new Error(`invalid branch column ${name} encoding`);
      }
      let reencoded: BranchColumnValue;
      try {
        new RecordDescriptor([["value", column.column_type]]).create([value]);
        reencoded = BranchColumnValue.encodeTyped(value, column.column_type);
      } catch {
        throw new Error(`invalid branch column ${name} value`);
      }
      values.push([name, reencoded]);
      cells.set(name, value);
    }
    values.sort((a, b) => cmpStr(a[0], b[0]));
    return [new BranchKey(values), cells];
  }

  projectBranchViewSelector(
    table: TableSchema,
    selector: BranchSelector,
  ): [BranchKey, SortedMap<string, GValue>] {
    const all = new SortedSet(
      cmpStr,
      this.tables.flatMap((t) => t.branch_by),
    );
    const keys = selector.values.keysArray();
    if (keys.length !== all.size || keys.some((k) => !all.has(k)))
      throw new Error("branch view selector must provide every schema branch column exactly once");
    const projected = new BranchSelector();
    for (const c of table.branch_by) projected.values.set(c, selector.values.get(c)!);
    return this.projectBranchSelector(table, projected);
  }

  validateAuthoredBranchKey(table: TableSchema, key: BranchKey) {
    return RuntimeSchema.validateBranchKeyForTable(table, key);
  }

  static decodePersistedBranchKey(table: TableSchema, bytes: Bytes): BranchKey {
    let key: BranchKey;
    try {
      key = BranchKey.fromCanonicalBytes(bytes);
    } catch {
      throw new Error(`invalid persisted branch key for ${table.name}`);
    }
    RuntimeSchema.validateBranchKeyForTable(table, key);
    return key;
  }

  static validateBranchKeyForTable(table: TableSchema, key: BranchKey): SortedMap<string, GValue> {
    const bindings = table.branch_by.slice().sort(cmpStr);
    if (key.values.length !== bindings.length)
      throw new Error(
        `branch key for ${table.name} must contain exactly ${bindings.length} values`,
      );
    const cells = new SortedMap<string, GValue>(cmpStr);
    bindings.forEach((name, i) => {
      const [actual, encoded] = key.values[i];
      if (actual !== name)
        throw new Error(`branch key for ${table.name} is not in canonical column order`);
      const column = table.columns.find((c) => c.name === name);
      if (!column) throw new Error(`unknown branch column on ${table.name}`);
      let value: GValue;
      try {
        value = encoded.decodeAs(column.column_type);
      } catch {
        throw new Error(`invalid branch column ${name} encoding`);
      }
      let re: BranchColumnValue;
      try {
        re = BranchColumnValue.encodeTyped(value, column.column_type);
      } catch {
        throw new Error(`invalid branch column ${name} value`);
      }
      if (re.bytes.join() !== encoded.bytes.join())
        throw new Error(`non-canonical branch column ${name} encoding`);
      try {
        new RecordDescriptor([["value", column.column_type]]).create([value]);
      } catch {
        throw new Error(`invalid branch column ${name} value`);
      }
      cells.set(name, value);
    });
    return cells;
  }

  lowerToGroove(): any {
    return this.withJazzDirectRecordStores(new GrooveDatabaseSchema(this.storageTables()));
  }
  lowerCatalogueMetaToGroove(): any {
    return this.withJazzDirectRecordStores(
      new GrooveDatabaseSchema(this.catalogueMetaStorageTables()),
    );
  }
  columnFamilies(): string[] {
    return StorageLayout.jazzClassV2().physicalColumnFamilies([
      ...this.lowerToGroove().columnFamilies(),
      "indices",
      "jazz_physical_history",
      "jazz_physical_global_current",
      "jazz_physical_ahead_current",
    ]);
  }
  physicalColumnFamilies(): string[] {
    return this.columnFamilies();
  }
  storageTables(): any[] {
    return [
      nodesTable(),
      schemaVersionsTable(),
      catalogueTable(),
      transactionsTable(),
      txTouchedRowsTable(),
      rejectedTransactionsTable(),
    ];
  }
  catalogueMetaStorageTables(): any[] {
    return [nodesTable(), schemaVersionsTable(), catalogueTable()];
  }
  canonicalBytes(): Bytes {
    return canonicalSchemaBytes(this);
  }
  versionId(): any {
    return new SchemaVersionId(uuidV5(SCHEMA_VERSION_NAMESPACE, this.canonicalBytes()));
  }

  private withJazzDirectRecordStores(schema: any): any {
    return schema
      .withDirectRecordStore(
        new DirectRecordStoreSchema(
          AUTHORITY_POLICY_BINDINGS_STORE,
          new RecordDescriptor([["policy_binding_digest", BYTES]]),
          new RecordDescriptor([
            ["subject", STRING],
            ["claims_v1", policyBindingDirectoryClaimsValueType()],
          ]),
        ),
      )
      .withDirectRecordStore(
        new DirectRecordStoreSchema(
          SUBSCRIPTION_WATERMARKS_STORE,
          subscriptionWatermarkKeyDescriptor(),
          subscriptionWatermarkValueDescriptor(),
        ),
      )
      .withDirectRecordStore(
        new DirectRecordStoreSchema(
          LOCAL_ROW_AVAILABILITY_STORE,
          new RecordDescriptor([
            ["policy_binding_digest", BYTES],
            ["global_table", UUID],
            ["row", UUID],
          ]),
          localAvailabilityRecordDescriptor(),
        ),
      )
      .withDirectRecordStore(
        new DirectRecordStoreSchema(
          SUPERSEDED_WRITES_STORE,
          new RecordDescriptor([
            ["physical_table_id", U64],
            ["branch_key", BYTES],
            ["row", UUID],
            ["node", UUID],
          ]),
          new RecordDescriptor([["superseded_through", U64]]),
        ),
      );
  }
}

export type MergeStrategy = "Lww" | "Counter" | "GSet";

export type ColumnTransformSemantics = {
  bijective: boolean;
  canonical_equality_preserving: boolean;
};
export function registeredColumnTransform(key: string): ColumnTransformSemantics | undefined {
  return key === "identity" || key === "jazz.identity"
    ? { bijective: true, canonical_equality_preserving: true }
    : undefined;
}

export type LargeValueSemanticKind = "NotLarge" | "Bytes" | "String" | "Json";

/** Application column declaration. */
export type ColumnSchema = {
  name: string;
  column_type: GValueType;
  large_value_kind: LargeValueSemanticKind;
  default?: GValue;
};

export function newColumnSchema(
  name: string,
  column_type: GValueType,
  defaultValue?: GValue,
): ColumnSchema {
  if (containsInternalStorageType(column_type))
    throw new Error(
      "raw and stored-scalar value types are physical-only and cannot be declared in a Jazz schema",
    );
  return {
    name,
    column_type,
    large_value_kind: largeValueKindForType(column_type),
    default: defaultValue,
  };
}

/** serde Deserialize with validation. */
export function columnSchemaFromJson(json: any): ColumnSchema {
  if (containsInternalStorageType(json.column_type))
    throw new Error("internal Groove storage types cannot appear in a Jazz schema");
  const derived = largeValueKindForType(json.column_type);
  const kind: LargeValueSemanticKind = json.large_value_kind ?? derived;
  if (!(kind === derived || (kind === "Json" && derived === "String")))
    throw new Error("large-value semantic kind does not match the declared column type");
  return {
    name: json.name,
    column_type: json.column_type,
    large_value_kind: kind,
    default: json.default ?? undefined,
  };
}

export const isNullableJson = (c: ColumnSchema) =>
  c.large_value_kind === "Json" && c.column_type.kind === "Nullable";
export const columnStorageValue = (c: ColumnSchema, v: GValue): GValue =>
  v.kind === "Nullable" && v.value !== undefined && isNullableJson(c) ? v.value : v;

function containsInternalStorageType(t: GValueType): boolean {
  if (isInternalStorageType(t)) return true;
  switch (t.kind) {
    case "Nullable":
    case "Array":
      return containsInternalStorageType(t.inner);
    case "Tuple":
      return t.members.some(containsInternalStorageType);
    case "Record":
      return t.descriptor.fields().some((f: any) => containsInternalStorageType(f.value_type));
    case "Enum":
      return t.schema.cases.some((c: any) =>
        c.payload.fields().some((f: any) => containsInternalStorageType(f.value_type)),
      );
    default:
      return false;
  }
}

function largeValueKindForType(t: GValueType): LargeValueSemanticKind {
  if (t.kind === "String") return "String";
  if (t.kind === "Bytes") return "Bytes";
  if (t.kind === "Nullable") return largeValueKindForType(t.inner);
  return "NotLarge";
}

export function storageColumnType(c: ColumnSchema): GValueType {
  return c.large_value_kind === "Json" ? physicalStorageValueType("Json") : c.column_type;
}

/** Operation-specific write policy clauses for an application table. */
export type WritePolicies = {
  insert_check?: Query;
  update_using?: Query;
  update_check?: Query;
  delete_using?: Query;
};
export const legacyWritePolicies = (p?: Query): WritePolicies => ({
  insert_check: p,
  update_using: p,
  update_check: p,
  delete_using: p,
});
export function writePolicyEntries(w: WritePolicies): [string, Query][] {
  return (["insert_check", "update_using", "update_check", "delete_using"] as const)
    .filter((k) => w[k] !== undefined)
    .map((k) => [k, w[k]!]);
}
export const anyWritePolicy = (w: WritePolicies) =>
  w.insert_check ?? w.update_check ?? w.update_using ?? w.delete_using;
export const hasAnyWritePolicy = (w: WritePolicies) => anyWritePolicy(w) !== undefined;

/** Application table whose rows are stored as immutable history versions. */
export type TableSchema = {
  name: string;
  columns: ColumnSchema[];
  branch_by: string[];
  references: SortedMap<string, string>;
  read_policy?: Query;
  write_policies: WritePolicies;
  indexed_columns: SortedSet<string>;
  composite_indexes: SortedSet<string[]>;
  merge_strategies: SortedMap<string, MergeStrategy>;
};

const cmpStrList = (a: string[], b: string[]) => {
  for (let i = 0; i < Math.min(a.length, b.length); i++) {
    const c = cmpStr(a[i], b[i]);
    if (c) return c;
  }
  return a.length - b.length;
};

export function newTableSchema(name: string, columns: ColumnSchema[]): TableSchema {
  return {
    name,
    columns,
    branch_by: [],
    references: new SortedMap(cmpStr),
    write_policies: {},
    indexed_columns: new SortedSet(cmpStr),
    composite_indexes: new SortedSet(cmpStrList),
    merge_strategies: new SortedMap(cmpStr),
  };
}
export const newCompositeIndexSet = () => new SortedSet<string[]>(cmpStrList);

function tableSchemaEquals(a: TableSchema, b: TableSchema): boolean {
  const ser = (t: TableSchema) =>
    JSON.stringify(
      [
        t.name,
        t.columns,
        t.branch_by,
        [...t.references],
        t.read_policy,
        t.write_policies,
        t.indexed_columns.toArray(),
        t.composite_indexes.toArray(),
        [...t.merge_strategies],
      ],
      (_k, v) => (typeof v === "bigint" ? v.toString() : v instanceof Uint8Array ? [...v] : v),
    );
  return ser(a) === ser(b);
}

export const hasAnyPolicy = (t: TableSchema) =>
  t.read_policy !== undefined || hasAnyWritePolicy(t.write_policies);
export const mergeStrategyOf = (t: TableSchema, column: string): MergeStrategy =>
  t.merge_strategies.get(column) ?? "Lww";

export const globalCurrentIndexedColumns = (t: TableSchema) =>
  new SortedSet(cmpStr, [...t.references.keysArray(), ...t.indexed_columns]);

const col = (name: string, t: GValueType) => new GrooveColumnSchema(name, t);
const userColumns = (t: TableSchema) =>
  t.columns.map((c) => col(appStorageColumnName(c.name), nullable(storageColumnType(c))));

export function rejectedVersionsStorageTable(t: TableSchema): any {
  return new GrooveTableSchema(`jazz_${t.name}_rejected_versions`, [
    col("tx_time", U64),
    col("tx_node_id", U64),
    col("row_uuid", UUID),
    col("_deletion", nullable(deletionColumn())),
    ...userColumns(t),
  ]).withPrimaryKey(
    PrimaryKey.composite([
      PrimaryKeyColumn.integer("tx_time", "U64"),
      PrimaryKeyColumn.integer("tx_node_id", "U64"),
      PrimaryKeyColumn.uuid("row_uuid"),
    ]),
  );
}

export const historyStorageTable = (t: TableSchema) =>
  historyStorageTableNamed(t, `jazz_${t.name}_history`);
export const authoredHistoryStorageTable = historyStorageTable;

function historyStorageTableNamed(t: TableSchema, name: string): any {
  const columns = [
    col("branch_key", BYTES),
    col("row_uuid", UUID),
    col("tx_time", U64),
    col("tx_node_id", U64),
    col("schema_version", U64),
    col("created_by", RowAuthor.valueType()),
    col("created_at", U64),
    col("updated_by", nullable(RowAuthor.valueType())),
    col("updated_at", U64),
    col("_deletion", nullable(deletionColumn())),
    ...userColumns(t),
    col("authored_columns", nullable(arrayOf(U64))),
    col(COUNTER_SIGNS_FIELD, BYTES),
    col(SEQ_FIELD, U64),
    col(BASE_SEQ_FIELD, nullable(U64)),
    col(BASE_PENDING_FIELD, nullable(txIdColumn())),
  ];
  return new GrooveTableSchema(name, columns).withPrimaryKey(
    PrimaryKey.composite([
      PrimaryKeyColumn.bytes("branch_key"),
      PrimaryKeyColumn.uuid("row_uuid"),
      PrimaryKeyColumn.integer(SEQ_FIELD, "U64"),
      PrimaryKeyColumn.integer("tx_time", "U64"),
      PrimaryKeyColumn.integer("tx_node_id", "U64"),
    ]),
  );
}

function currentColumns(t: TableSchema): any[] {
  return [
    col("branch_key", BYTES),
    col("row_uuid", UUID),
    col("tx_time", U64),
    col("tx_node_id", U64),
    col("schema_version", U64),
    col("created_by", RowAuthor.valueType()),
    col("created_at", U64),
    col("updated_by", RowAuthor.valueType()),
    col("updated_at", U64),
    col("global_time", nullable(U64)),
    col("_deletion", nullable(deletionColumn())),
    ...userColumns(t),
    col("authored_columns", nullable(arrayOf(U64))),
  ];
}

export function globalCurrentStorageTable(t: TableSchema): any {
  let table = new GrooveTableSchema(
    `jazz_${t.name}_global_current`,
    currentColumns(t),
  ).withPrimaryKey(
    PrimaryKey.composite([PrimaryKeyColumn.bytes("branch_key"), PrimaryKeyColumn.uuid("row_uuid")]),
  );
  for (const indexed of globalCurrentIndexedColumns(t))
    table = table.withIndex(
      new GrooveIndexSchema(globalCurrentIndexName(indexed), [
        "branch_key",
        appStorageColumnName(indexed),
      ]),
    );
  for (const columns of t.composite_indexes)
    table = table.withIndex(
      new GrooveIndexSchema(globalCurrentCompositeIndexName(columns), [
        "branch_key",
        ...columns.map(appStorageColumnName),
      ]),
    );
  return table.withIndex(
    new GrooveIndexSchema(GLOBAL_CURRENT_BY_SEQ_INDEX, ["branch_key", "global_time", "row_uuid"]),
  );
}

export function aheadCurrentStorageTable(t: TableSchema): any {
  return new GrooveTableSchema(`jazz_${t.name}_ahead_current`, currentColumns(t)).withPrimaryKey(
    PrimaryKey.composite([PrimaryKeyColumn.bytes("branch_key"), PrimaryKeyColumn.uuid("row_uuid")]),
  );
}

/** Wire descriptor for replicated immutable row payloads. */
export function wireRecordDescriptor(t: TableSchema): any {
  return new RecordDescriptor([
    ["row_uuid", UUID],
    ["created_by", RowAuthor.valueType()],
    ["created_at", U64],
    ["updated_by", RowAuthor.valueType()],
    ["updated_at", U64],
    ["_deletion", nullable(deletionColumn())],
    ...t.columns.map((c): [string, GValueType] => [
      appStorageColumnName(c.name),
      nullable(storageColumnType(c)),
    ]),
  ]);
}

export const subscriptionWatermarkKeyDescriptor = () =>
  new RecordDescriptor([
    ["shape_id", UUID],
    ["binding_id", UUID],
    ["read_view_id", UUID],
    ["policy_scope", U8],
    ["policy_binding_digest", BYTES],
  ]);

export const subscriptionWatermarkValueDescriptor = () =>
  new RecordDescriptor([
    ["format_v1", U8],
    ["settled_through", U64],
    ["supporting_revision", BYTES],
  ]);

const schemaVersionsTable = () =>
  new GrooveTableSchema("jazz_schema_versions", [
    col("id", U64),
    col("uuid", UUID),
    col("physical_mapping", BYTES),
  ]).withPrimaryKey(new PrimaryKey("id", "U64"));

const catalogueTable = () =>
  new GrooveTableSchema("jazz_catalogue", [
    col("kind", U64),
    col("id", UUID),
    col("payload", BYTES),
  ]).withPrimaryKey(
    PrimaryKey.composite([PrimaryKeyColumn.integer("kind", "U64"), PrimaryKeyColumn.uuid("id")]),
  );

const storageEnum = (name: string, variants: string[]): GValueType => ({
  kind: "EnumTag",
  schema: new ScalarEnumSchema(name, variants),
});
const txKindColumn = () => storageEnum("jazz_tx_kind", ["mergeable", "exclusive"]);
const fateColumn = () => storageEnum("jazz_fate", ["pending", "accepted", "rejected"]);
const durabilityColumn = () => storageEnum("jazz_durability", ["none", "local", "global"]);
const deletionColumn = (): GValueType => ({
  kind: "EnumTag",
  schema: new ScalarEnumSchema("jazz_deletion", ["deleted", "restored"]).withSystemRegistry(
    SystemVariantRegistry.deletionState(),
  ),
});
const rejectionReasonColumn = () => RejectionReason.storageValueType();
const txIdColumn = (): GValueType => ({ kind: "Tuple", members: [U64, UUID] });

export const SEQ_FIELD = "seq";
export const BASE_SEQ_FIELD = "base_seq";
export const BASE_PENDING_FIELD = "base_pending";
export const COUNTER_SIGNS_FIELD = "counter_signs";
export const APP_COLUMN_PREFIX = "_app_";
export const appStorageColumnName = (column: string) => `${APP_COLUMN_PREFIX}${column}`;
export const GLOBAL_CURRENT_BY_SEQ_INDEX = "by_seq";
export const globalCurrentIndexName = (column: string) => `by_app_${column}`;
export const globalCurrentCompositeIndexName = (columns: string[]) =>
  `by_composite_${columns.map((c) => `${c.length}_${c}`).join("_")}`;

const nodesTable = () =>
  new GrooveTableSchema("jazz_nodes", [col("id", U64), col("uuid", UUID)]).withPrimaryKey(
    new PrimaryKey("id", "U64"),
  );

const timeNodeKey = (a: string, b: string) =>
  PrimaryKey.composite([PrimaryKeyColumn.integer(a, "U64"), PrimaryKeyColumn.integer(b, "U64")]);

const transactionsTable = () =>
  new GrooveTableSchema("jazz_transactions", [
    col("time", U64),
    col("node_id", U64),
    col("kind", txKindColumn()),
    col("n_total_writes", U32),
    col("made_by", RowAuthor.valueType()),
    col("base_snapshot", nullable(BYTES)),
    col("row_read_set", nullable(BYTES)),
    col("absent_read_set", nullable(BYTES)),
    col("predicate_read_set", nullable(BYTES)),
    col("user_metadata", nullable(STRING)),
    col("contribution_merge", contributionMergeColumn()),
    col("permission_subject", nullable(AuthorSubject.valueType())),
    col("is_partial", BOOL),
    col("fate", fateColumn()),
    col("global_time", nullable(U64)),
    col("rejection_reason", nullable(rejectionReasonColumn())),
    col("durability", durabilityColumn()),
  ])
    .withPrimaryKey(timeNodeKey("time", "node_id"))
    .withIndex(new GrooveIndexSchema("by_global_time", ["global_time"]));

const txTouchedRowsTable = () =>
  new GrooveTableSchema("jazz_tx_touched_rows", [
    col("tx_time", U64),
    col("tx_node_id", U64),
    col(
      "touched_rows",
      arrayOf(
        record([
          ["physical_table_id", U64],
          ["branch_key", BYTES],
          ["row_uuids", arrayOf(UUID)],
        ]),
      ),
    ),
  ]).withPrimaryKey(timeNodeKey("tx_time", "tx_node_id"));

const enumType = (name: string, cases: [string, [string, GValueType][]][]): GValueType => ({
  kind: "Enum",
  schema: new EnumSchema(
    name,
    cases.map(([n, f]) => new EnumCase(n, new RecordDescriptor(f))),
  ),
});

const contributionComponentColumn = () =>
  enumType("jazz_contribution_component", [
    ["column", [["physical_column_id", U64]]],
    [
      "operation",
      [
        ["physical_column_id", U64],
        ["identity", BYTES],
      ],
    ],
    ["register", []],
  ]);

const contributionCoordinateColumn = () =>
  record([
    ["branch_key", BYTES],
    ["physical_table_id", U64],
    ["row_uuid", UUID],
    ["layer", storageEnum("jazz_contribution_layer", ["content", "deletion"])],
    ["component", contributionComponentColumn()],
  ]);

const contributionDotColumn = () =>
  record([
    ["tx_time", U64],
    ["tx_node", UUID],
    ["coordinate", contributionCoordinateColumn()],
  ]);

function contributionMergeColumn(): GValueType {
  return nullable(
    record([
      ["source", BYTES],
      ["target", BYTES],
      [
        "substitutions",
        arrayOf(
          record([
            ["target", contributionCoordinateColumn()],
            ["sources", arrayOf(contributionDotColumn())],
          ]),
        ),
      ],
      [
        "branch_view_copy_v1",
        arrayOf(
          record([
            ["version", U8],
            ["head", BYTES],
            [
              "base",
              enumType("jazz_branch_view_copy_base_v1", [
                ["current", [["branch", BYTES]]],
                [
                  "snapshot",
                  [
                    ["branch", BYTES],
                    ["owner", UUID],
                    ["global_base", U64],
                    ["local_base", U64],
                    [
                      "dots",
                      arrayOf(
                        record([
                          ["time", U64],
                          ["node", UUID],
                        ]),
                      ),
                    ],
                  ],
                ],
              ]),
            ],
            ["table", STRING],
            ["row_uuid", UUID],
            ["source_time", U64],
            ["source_node", UUID],
          ]),
        ),
      ],
      [
        "branch_write_intent_v1",
        arrayOf(
          record([
            ["version", U8],
            ["physical_table_id", U64],
            ["authored_schema", UUID],
            ["row_uuid", UUID],
            ["head", BYTES],
            [
              "operation",
              enumType("jazz_branch_write_operation_v1", [
                ["exact_head_insert", []],
                ["exact_head_update", []],
                ["view_update_copy", [["evidence_index", U32]]],
              ]),
            ],
          ]),
        ),
      ],
    ]),
  );
}

/** Bound physical type used by the transaction codec. */
export function contributionMergeStorageType(): GValueType {
  const t = new GrooveTableSchema("jazz_transactions", [
    col("contribution_merge", contributionMergeColumn()),
  ]);
  const c = t.columns[0];
  if (!c) throw new Error("contribution merge storage column");
  return c.column_type;
}

const SCHEMA_ID_DOMAIN_V1 = "jazz-schema-v1-large-value-kinds";
const SCHEMA_ID_DOMAIN_V2_COMPOSITE_INDEXES = "jazz-schema-v2-composite-indexes";

class ByteSink {
  out: number[] = [];
  u8(v: number) {
    this.out.push(v);
  }
  u64(v: number) {
    const b = new Uint8Array(8);
    new DataView(b.buffer).setBigUint64(0, BigInt(v), false);
    this.out.push(...b);
  }
  str(s: string) {
    const b = utf8(s);
    this.u64(b.length);
    this.out.push(...b);
  }
}

function canonicalSchemaBytes(schema: RuntimeSchema): Bytes {
  const withComposite = schema.tables.some((t) => t.composite_indexes.size > 0);
  const s = new ByteSink();
  s.str(withComposite ? SCHEMA_ID_DOMAIN_V2_COMPOSITE_INDEXES : SCHEMA_ID_DOMAIN_V1);
  const tables = schema.tables.slice().sort((a, b) => cmpStr(a.name, b.name));
  s.u64(tables.length);
  for (const t of tables) {
    s.str(t.name);
    s.u64(t.columns.length);
    for (const c of t.columns) {
      s.str(c.name);
      putColumnType(s, c.column_type);
      s.u8(["NotLarge", "Bytes", "String", "Json"].indexOf(c.large_value_kind));
      s.u8(["Lww", "Counter", "GSet"].indexOf(mergeStrategyOf(t, c.name)));
    }
    s.u64(t.references.size);
    for (const [c, target] of t.references) {
      s.str(c);
      s.str(target);
    }
    const branchBy = t.branch_by.slice().sort(cmpStr);
    s.u64(branchBy.length);
    for (const c of branchBy) s.str(c);
    if (withComposite) {
      s.u64(t.composite_indexes.size);
      for (const cols of t.composite_indexes) {
        s.u64(cols.length);
        for (const c of cols) s.str(c);
      }
    }
  }
  return Uint8Array.from(s.out);
}

const SCALAR_CODES: Record<string, number> = {
  U8: 0,
  U16: 1,
  U32: 2,
  U64: 3,
  I32: 4,
  I64: 5,
  F64: 6,
  Bool: 7,
  String: 8,
  Bytes: 9,
  Uuid: 10,
};

function putFields(s: ByteSink, fields: any[]) {
  s.u64(fields.length);
  for (const f of fields) {
    if (f.name !== undefined) {
      s.u8(1);
      s.str(f.name);
    } else s.u8(0);
    putColumnType(s, f.value_type);
  }
}

function putColumnType(s: ByteSink, t: GValueType): void {
  const code = SCALAR_CODES[t.kind];
  if (code !== undefined) return s.u8(code);
  switch (t.kind) {
    case "EnumTag":
      s.u8(11);
      s.str(t.schema.name);
      s.u64(t.schema.variants.length);
      for (const v of t.schema.variants) s.str(v);
      return;
    case "Tuple":
      s.u8(12);
      s.u64(t.members.length);
      for (const m of t.members) putColumnType(s, m);
      return;
    case "Array":
      s.u8(13);
      return putColumnType(s, t.inner);
    case "Nullable":
      s.u8(14);
      return putColumnType(s, t.inner);
    case "Record":
      s.u8(15);
      return putFields(s, t.descriptor.fields());
    case "Enum":
      s.u8(16);
      s.str(t.schema.name);
      s.u64(t.schema.cases.length);
      for (const c of t.schema.cases) {
        s.str(c.name);
        putFields(s, c.payload.fields());
      }
      return;
    default:
      throw new Error("raw stored-scalar backing types cannot appear in a Jazz schema");
  }
}

const rejectedTransactionsTable = () =>
  new GrooveTableSchema("jazz_rejected_transactions", [
    col("time", U64),
    col("node_id", U64),
    col("kind", txKindColumn()),
    col("made_by", RowAuthor.valueType()),
    col("rejection_reason", rejectionReasonColumn()),
    col("user_metadata", nullable(STRING)),
  ]).withPrimaryKey(timeNodeKey("time", "node_id"));

export function physicalColumnEpochIsCompatible(
  sourceTable: TableSchema,
  sourceColumn: string,
  targetTable: TableSchema,
  targetColumn: string,
): boolean {
  const s = sourceTable.columns.find((c) => c.name === sourceColumn);
  const t = targetTable.columns.find((c) => c.name === targetColumn);
  if (!s || !t) return false;
  return (
    physicalValueEpochIsCompatible(s.column_type, t.column_type) &&
    mergeStrategyOf(sourceTable, sourceColumn) === mergeStrategyOf(targetTable, targetColumn)
  );
}

export function physicalValueEpochIsCompatible(source: GValueType, target: GValueType): boolean {
  if (source.kind !== target.kind) return false;
  switch (source.kind) {
    case "EnumTag": {
      const l: string[] = source.schema.variants;
      const r: string[] = target.schema.variants;
      return r.length >= l.length && l.every((v, i) => r[i] === v);
    }
    case "Enum": {
      const l = source.schema.cases;
      const r = target.schema.cases;
      return (
        r.length >= l.length &&
        l.every(
          (a: any, i: number) =>
            a.name === r[i].name && physicalRecordEpochIsCompatible(a.payload, r[i].payload),
        )
      );
    }
    case "Tuple":
      return (
        source.members.length === target.members.length &&
        source.members.every((m: GValueType, i: number) =>
          physicalValueEpochIsCompatible(m, target.members[i]),
        )
      );
    case "Array":
    case "Nullable":
      return physicalValueEpochIsCompatible(source.inner, target.inner);
    case "Record":
      return physicalRecordEpochIsCompatible(source.descriptor, target.descriptor);
    default:
      return valueTypeEquals(source, target);
  }
}

function physicalRecordEpochIsCompatible(source: any, target: any): boolean {
  const a = source.fields();
  const b = target.fields();
  return (
    a.length === b.length &&
    a.every(
      (f: any, i: number) =>
        f.name === b[i].name && physicalValueEpochIsCompatible(f.value_type, b[i].value_type),
    )
  );
}

export const localAvailabilityRecordDescriptor = () =>
  new RecordDescriptor([
    ["format_v1", U8],
    ["unavailable", BOOL],
    ["core", UUID],
    ["core_epoch", U64],
    ["claims_revision", U64],
    ["policy_epoch", U64],
    ["settled_through", U64],
    ["evaluation_seq", U64],
  ]);
