import {
  type AbiRelationSubscriptionSnapshot,
  type AbiRowBatch,
  type AbiSubscriptionDelta,
  type DescriptorField,
  type ValueType,
  PostcardWriter,
  PostcardReader,
  assertBytes,
  decodeRecordBool,
  decodeRecordString,
  openConfig,
  queryFromTable,
  type SubscriptionStreamChunk,
  utf8,
} from "./core-codec.js";
import {
  ANONYMOUS_JWT_ISSUER,
  decodeBase64UrlToUtf8,
  LOCAL_FIRST_JWT_ISSUER,
  type LocalFirstJwtOptions,
} from "./local-first-jwt-core.js";
import { createLocalFirstJwtAsync } from "./local-first-jwt-webcrypto.js";

export type ColumnType =
  | "Boolean"
  | "Integer"
  | "Text"
  | "Uuid"
  | "Bytea"
  | { type: "Boolean" | "Integer" | "Text" | "Uuid" | "Bytea" }
  | { type: "Array"; element: ColumnType };

export type ColumnDefinition = {
  name: string;
  column_type: ColumnType;
  nullable?: boolean;
  references?: string;
  large?: boolean;
  default?: unknown;
  enum?: readonly string[];
  json?: boolean;
  timestamp?: boolean;
  indexOnly?: boolean;
};

export type TableDefinition = {
  columns: ColumnDefinition[];
  readPolicy?: TablePolicy;
  relations?: Record<string, RelationDefinition>;
  indexed_columns?: readonly string[];
  indexOnly(columns: readonly string[]): TableDefinition;
};

export type SchemaDefinition = Record<string, TableDefinition>;
export type TablePolicy = "owner";
export type TableOptions = {
  readPolicy?: TablePolicy;
  relations?: Record<string, RelationDefinition>;
  indexed_columns?: readonly string[];
};
export type RelationDefinition = {
  table: string;
  column: string;
  type?: "many";
};

export type Table<Row extends { id: string | Uint8Array }, Init = Omit<Row, "id">> = {
  readonly _table: string;
  readonly _schema: SchemaDefinition;
  readonly _rowType: Row;
  readonly _initType: Init;
  where(conditions: QueryWhere<Row>): QueryBuilder<Row>;
  where(column: keyof Row & string, op: "eq" | "ne", value: QueryValue): QueryBuilder<Row>;
  where(column: keyof Row & string, op: "in", values: readonly QueryValue[]): QueryBuilder<Row>;
  where(
    column: keyof Row & string,
    op: "gt" | "gte" | "lt" | "lte",
    value: string | number | bigint,
  ): QueryBuilder<Row>;
  where(column: keyof Row & string, op: "contains", value: string): QueryBuilder<Row>;
  where(column: keyof Row & string, op: "isNull" | "isNotNull"): QueryBuilder<Row>;
  select<const Columns extends readonly (keyof Row & string)[]>(
    ...columns: Columns
  ): QueryBuilder<ProjectedRow<Row, Columns>>;
  orderBy(column: keyof Row & string, direction?: OrderDirection): QueryBuilder<Row>;
  limit(count: number): QueryBuilder<Row>;
  offset(count: number): QueryBuilder<Row>;
  include<Property extends string>(
    property: Property,
  ): QueryBuilder<Row & Record<Property, unknown[] | unknown | null>>;
  include<const Includes extends QueryIncludeMap>(
    includes: Includes,
  ): QueryBuilder<Row & { [Property in keyof Includes & string]: unknown[] | unknown | null }>;
  requireIncludes<const Properties extends readonly string[]>(
    ...properties: Properties
  ): QueryBuilder<Row & { [Property in Properties[number]]: unknown[] | unknown | null }>;
  hop<Property extends string>(property: Property): QueryBuilder<Record<string, unknown>>;
  gather(options: GatherOptions): QueryBuilder<Row>;
};

export type QueryBuilder<Row> = {
  readonly _table: string;
  readonly _schema: SchemaDefinition;
  readonly _rowType: Row;
  where(conditions: QueryWhere<Row>): QueryBuilder<Row>;
  where(column: keyof Row & string, op: "eq" | "ne", value: QueryValue): QueryBuilder<Row>;
  where(column: keyof Row & string, op: "in", values: readonly QueryValue[]): QueryBuilder<Row>;
  where(
    column: keyof Row & string,
    op: "gt" | "gte" | "lt" | "lte",
    value: string | number | bigint,
  ): QueryBuilder<Row>;
  where(column: keyof Row & string, op: "contains", value: string): QueryBuilder<Row>;
  where(column: keyof Row & string, op: "isNull" | "isNotNull"): QueryBuilder<Row>;
  select<const Columns extends readonly (keyof Row & string)[]>(
    ...columns: Columns
  ): QueryBuilder<ProjectedRow<Row, Columns>>;
  orderBy(column: keyof Row & string, direction?: OrderDirection): QueryBuilder<Row>;
  limit(count: number): QueryBuilder<Row>;
  offset(count: number): QueryBuilder<Row>;
  include<Property extends string>(
    property: Property,
  ): QueryBuilder<Row & Record<Property, unknown[] | unknown | null>>;
  include<const Includes extends QueryIncludeMap>(
    includes: Includes,
  ): QueryBuilder<Row & { [Property in keyof Includes & string]: unknown[] | unknown | null }>;
  requireIncludes<const Properties extends readonly string[]>(
    ...properties: Properties
  ): QueryBuilder<Row & { [Property in Properties[number]]: unknown[] | unknown | null }>;
  hop<Property extends string>(property: Property): QueryBuilder<Record<string, unknown>>;
  gather(options: GatherOptions): QueryBuilder<Row>;
  _build(): string;
};

export type DbOptions = {
  schema: SchemaDefinition;
  appId?: string;
  node?: Uint8Array;
  /** @internal test/ABI escape hatch until Rust auth admission owns sessions. */
  accountAuthor?: Uint8Array;
  /** @internal test/ABI escape hatch until Rust auth admission owns sessions. */
  accountId?: number;
  secret?: string;
  jwtToken?: string;
  cookieSession?: Session;
  server?: boolean;
  Runtime?: WasmDbConstructor | (new () => unknown);
  nextRowId?: number;
};

export type Subscription<_Row> = {
  unsubscribe(): void;
};

export type SubscriptionCallback<Row> = (rows: Row[]) => void;

export type Identity = string | Uint8Array;
export type AuthMode = "external" | "local-first" | "anonymous";
export type Session = {
  user_id: string;
  claims: Record<string, unknown>;
  authMode: AuthMode;
};
export type AuthState = {
  authMode: AuthMode;
  session: Session | null;
  error?: AuthFailureReason;
};
export type AuthFailureReason = "expired" | "missing" | "disabled" | "invalid";
export type ClientSessionTransport = "bearer" | "cookie";
export type ClientSessionState = {
  transport: ClientSessionTransport | null;
  session: Session | null;
};
export type JwtPayload = {
  sub?: unknown;
  iss?: unknown;
  aud?: unknown;
  claims?: unknown;
  exp?: unknown;
};

export type Db = {
  table<Row extends { id: string | Uint8Array }, Init = Omit<Row, "id">>(
    name: string,
  ): Table<Row, Init>;
  beginTransaction(options?: TransactionOptions): Transaction;
  transaction<Value>(callback: (tx: Transaction) => PromiseLike<Value>): Promise<Value>;
  transaction<Value>(callback: (tx: Transaction) => Value): Value;
  insert<Row extends { id: string | Uint8Array }, Init>(
    table: Table<Row, Init>,
    row: Init & Partial<Pick<Row, "id">>,
    options?: InsertOptions<Row>,
  ): WriteResult<Row> & Row;
  update<Row extends { id: string | Uint8Array }>(
    table: Table<Row, unknown>,
    id: Row["id"],
    patch: Partial<Omit<Row, "id">>,
    options?: WriteTimestampOptions,
  ): WriteResult<Row> & Row;
  upsert<Row extends { id: string | Uint8Array }, Init>(
    table: Table<Row, Init>,
    row: Init & Partial<Pick<Row, "id">>,
    options: UpsertOptions<Row>,
  ): WriteResult<Row> & Row;
  delete<Row extends { id: string | Uint8Array }>(
    table: Table<Row, unknown>,
    id: Row["id"],
    options?: WriteTimestampOptions,
  ): WriteResult<void>;
  restore<Row extends { id: string | Uint8Array }, Init>(
    table: Table<Row, Init>,
    id: Row["id"],
    row: Init,
    options?: WriteTimestampOptions,
  ): WriteResult<Row> & Row;
  all<Row>(
    tableOrQuery: Table<Row & { id: string | Uint8Array }, unknown> | QueryBuilder<Row>,
    options?: ReadOptions,
  ): Row[];
  one<Row>(
    tableOrQuery: Table<Row & { id: string | Uint8Array }, unknown> | QueryBuilder<Row>,
    options?: ReadOptions,
  ): Row | null;
  allForIdentity<Row>(
    tableOrQuery: Table<Row & { id: string | Uint8Array }, unknown> | QueryBuilder<Row>,
    identity: Identity,
    options?: ReadOptions,
  ): Row[];
  subscribe<Row>(
    tableOrQuery: Table<Row & { id: string | Uint8Array }, unknown> | QueryBuilder<Row>,
    callback: SubscriptionCallback<Row>,
  ): Subscription<Row>;
  getAuthState(): AuthState;
  onAuthChanged(listener: (state: AuthState) => void): () => void;
  updateAuthToken(jwtToken: string | null): void;
  _connectUpstreamTransport?(): WasmTransport;
};

export type TransactionKind = "mergeable" | "exclusive";
export type TransactionOptions = {
  kind?: TransactionKind;
};
export type Transaction = {
  insert<Row extends { id: string | Uint8Array }, Init>(
    table: Table<Row, Init>,
    row: Init & Partial<Pick<Row, "id">>,
    options?: InsertOptions<Row>,
  ): Row;
  update<Row extends { id: string | Uint8Array }>(
    table: Table<Row, unknown>,
    id: Row["id"],
    patch: Partial<Omit<Row, "id">>,
    options?: WriteTimestampOptions,
  ): void;
  upsert<Row extends { id: string | Uint8Array }, Init>(
    table: Table<Row, Init>,
    row: Init & Partial<Pick<Row, "id">>,
    options: UpsertOptions<Row>,
  ): Row;
  delete<Row extends { id: string | Uint8Array }>(
    table: Table<Row, unknown>,
    id: Row["id"],
    options?: WriteTimestampOptions,
  ): void;
  restore<Row extends { id: string | Uint8Array }, Init>(
    table: Table<Row, Init>,
    id: Row["id"],
    row: Init,
    options?: WriteTimestampOptions,
  ): Row;
  all<Row>(
    tableOrQuery: Table<Row & { id: string | Uint8Array }, unknown> | QueryBuilder<Row>,
    options?: ReadOptions,
  ): Row[];
  one<Row>(
    tableOrQuery: Table<Row & { id: string | Uint8Array }, unknown> | QueryBuilder<Row>,
    options?: ReadOptions,
  ): Row | null;
  commit(): WriteResult<void>;
  rollback(): void;
};

export type WriteTier = "local" | "edge" | "global" | "Local" | "Edge" | "Global";
export type ReadOptions = {
  /**
   * One-shot reads only. Subscriptions continue to emit live rows and remove deleted rows.
   */
  includeDeleted?: boolean;
};
export type WriteWaitOptions = {
  tier?: WriteTier;
};
export type WriteTimestampOptions = {
  updatedAt?: Date | string | number;
};
export type InsertOptions<Row extends { id: string | Uint8Array }> = {
  id?: Row["id"];
};
export type UpsertOptions<Row extends { id: string | Uint8Array }> = InsertOptions<Row> &
  WriteTimestampOptions;
export type WriteResult<Value> = {
  readonly value: Value;
  readonly handle: WriteHandle | null;
  wait(options?: WriteWaitOptions): Promise<Value>;
};

export type BinaryLargeValueRow = {
  id: string | Uint8Array;
  data: Uint8Array;
  mime_type?: string;
  name?: string;
  size?: bigint;
};

export type BinaryLargeValueInput = {
  rowId?: string | Uint8Array;
  fileId?: string | Uint8Array;
  name?: string;
  mimeType?: string;
  blob: Blob;
};

export type StoredBinaryLargeValue = BinaryLargeValueRow;

const deletedRowMarker = Symbol("jazz.deleted");

export function isDeleted(row: unknown): boolean {
  return !!(isRecord(row) && (row as Record<PropertyKey, unknown>)[deletedRowMarker] === true);
}

const uuidText = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
export { ANONYMOUS_JWT_ISSUER, LOCAL_FIRST_JWT_ISSUER, type LocalFirstJwtOptions };
export { createLocalFirstJwtAsync };

type ColumnBuilderOptions = {
  nullable?: boolean;
  references?: string;
  large?: boolean;
  default?: unknown;
  enum?: readonly string[];
  json?: boolean;
  timestamp?: boolean;
  indexOnly?: boolean;
};
type RefTarget = string | { readonly _table: string };
type ColumnDefinitionInput = Omit<ColumnDefinition, "name">;
type ColumnDefinitionLike = ColumnType | ColumnDefinitionInput | ColumnBuilder;

export type ColumnBuilder = Omit<ColumnDefinitionInput, "default" | "indexOnly"> & {
  optional(): ColumnBuilder;
  default(value: unknown): ColumnBuilder;
  indexOnly(): ColumnBuilder;
  ref(tableOrToken: RefTarget): ColumnBuilder;
};

type QueryFilter =
  | {
      column: string;
      columnType: ColumnType;
      nullable: boolean;
      op: "eq" | "ne";
      value: QueryLiteral;
    }
  | { column: string; columnType: ColumnType; nullable: boolean; op: "in"; values: QueryLiteral[] }
  | {
      column: string;
      columnType: ColumnType;
      nullable: boolean;
      op: "gt" | "gte" | "lt" | "lte";
      value: string | number;
    }
  | { column: string; columnType: ColumnType; op: "isNull" | "isNotNull" }
  | { column: string; columnType: ColumnType; op: "contains"; value: QueryLiteral }
  | { op: "any"; filters: QueryFilter[] };

type QueryScalar = boolean | string | number | bigint | Uint8Array | null;
type QueryArrayValue = readonly QueryScalar[];
type QueryValue = QueryScalar | QueryArrayValue;
export type QueryWhereValue =
  | QueryValue
  | undefined
  | {
      eq?: QueryValue;
      ne?: QueryValue;
      in?: readonly QueryValue[];
      gt?: string | number | bigint;
      gte?: string | number | bigint;
      lt?: string | number | bigint;
      lte?: string | number | bigint;
      contains?: string;
      isNull?: boolean;
    };
export type QueryWhere<Row> = Partial<Record<keyof Row & string, QueryWhereValue>>;
type QueryLiteral =
  | boolean
  | string
  | number
  | { bytes: number[] }
  | readonly QueryLiteral[]
  | null;
type OrderDirection = "asc" | "desc";
type QueryOrderBy = { column: string; direction: OrderDirection };
type ProjectedRow<Row, Columns extends readonly string[]> = Pick<
  Row,
  Extract<keyof Row, "id" | Columns[number]>
>;
type QueryInclude = Record<string, boolean | IncludeOptions | undefined>;
type QueryIncludeMap = Record<string, true | undefined | IncludeOptions>;
type IncludeOptions = {
  required?: boolean;
  include?: QueryIncludeMap;
  select?: readonly string[];
};
export type GatherOptions = {
  max_depth: number;
  step_table: string;
  step_current_column: string;
  step_conditions?: Array<{ column: string; op: string; value?: unknown }>;
  step_hops: string[];
};
type BuiltQuery = {
  table?: unknown;
  filters?: QueryFilter[];
  filter?: QueryFilter;
  conditions?: Array<{ column: string; op: string; value?: unknown }>;
  includes?: QueryInclude;
  hops?: string[];
  gather?: GatherOptions;
  select?: unknown;
  orderBy?: unknown;
  limit?: unknown;
  offset?: unknown;
};
type ResolvedDbOptions = DbOptions & {
  appId: string;
  node: Uint8Array;
  accountAuthor: Uint8Array;
  accountId: number;
  jwtToken?: string;
};
type WasmPreparedQuery = object;
type WasmWrite = {
  readonly kind?: "write";
  readonly payload: Uint8Array;
};
export type WasmTransport = {
  sendWireFrame(frame: Uint8Array): void;
  recvWireFrames(): Uint8Array[];
  tick(): number;
  close(): boolean;
};
type WasmTx = {
  insertWithIdEncoded(table: string, rowId: Uint8Array, cells: Uint8Array): void;
  updateEncoded(table: string, rowId: Uint8Array, patch: Uint8Array): void;
  upsertEncoded(table: string, rowId: Uint8Array, cells: Uint8Array): void;
  delete(table: string, rowId: Uint8Array): void;
  restoreEncoded(table: string, rowId: Uint8Array, cells: Uint8Array): void;
  commit(): WasmWrite;
  rollback(): void;
};
type WasmDb = {
  prepareQuery(query: Uint8Array): WasmPreparedQuery;
  mergeableTx(): WasmTx;
  insertEncoded(table: string, cells: Uint8Array): WasmWrite;
  insertWithIdEncoded(table: string, rowId: Uint8Array, cells: Uint8Array): WasmWrite;
  updateEncoded(table: string, rowId: Uint8Array, patch: Uint8Array): WasmWrite;
  upsertEncoded(table: string, rowId: Uint8Array, cells: Uint8Array): WasmWrite;
  delete(table: string, rowId: Uint8Array): WasmWrite;
  restoreEncoded(table: string, rowId: Uint8Array, cells: Uint8Array): WasmWrite;
  all(query: WasmPreparedQuery, opts?: unknown): Uint8Array;
  allForIdentity(query: WasmPreparedQuery, author: Uint8Array, opts?: unknown): Uint8Array;
  one(query: WasmPreparedQuery, opts?: unknown): Uint8Array;
  subscribe(query: WasmPreparedQuery, opts?: unknown): ReadableStream<SubscriptionStreamChunk>;
  tick(): void;
  connectUpstream(): WasmTransport;
};
type WasmDbConstructor = {
  openMemory(schema: Uint8Array, config: Uint8Array): WasmDb;
};
type WriteHandle = WasmWrite;

export const schema = {
  table(
    columns: Record<string, ColumnDefinitionLike> | ColumnDefinition[],
    options: TableOptions = {},
  ): TableDefinition {
    if (Array.isArray(columns)) return makeTableDefinition(columns, options);
    return makeTableDefinition(
      Object.entries(columns).map(([name, column]) => materializeColumn(name, column)),
      options,
    );
  },
  boolean(options: ColumnBuilderOptions = {}): ColumnBuilder {
    return column("Boolean", options);
  },
  integer(options: ColumnBuilderOptions = {}): ColumnBuilder {
    return column("Integer", options);
  },
  int(options: ColumnBuilderOptions = {}): ColumnBuilder {
    return column("Integer", options);
  },
  string(options: ColumnBuilderOptions = {}): ColumnBuilder {
    return column("Text", options);
  },
  text(options: ColumnBuilderOptions = {}): ColumnBuilder {
    return column("Text", options);
  },
  uuid(options: ColumnBuilderOptions = {}): ColumnBuilder {
    return column("Uuid", options);
  },
  bytea(options: ColumnBuilderOptions = {}): ColumnBuilder {
    return column("Bytea", options);
  },
  bytes(options: ColumnBuilderOptions = {}): ColumnBuilder {
    return column("Bytea", options);
  },
  binaryLargeValue(options: ColumnBuilderOptions = {}): ColumnBuilder {
    return column("Bytea", { ...options, large: true });
  },
  binaryLargeValueTable(): TableDefinition {
    return makeTableDefinition([
      { name: "name", column_type: "Text" },
      { name: "mime_type", column_type: "Text" },
      { name: "data", column_type: "Bytea", large: true },
    ]);
  },
  array(element: ColumnDefinitionLike, options: ColumnBuilderOptions = {}): ColumnBuilder {
    return column({ type: "Array", element: columnTypeOf(element) }, options);
  },
  ref(tableOrToken: RefTarget, options: ColumnBuilderOptions = {}): ColumnBuilder {
    return column("Uuid", { ...options, references: refTargetName(tableOrToken) });
  },
  enum: enumColumn,
  json(_schema?: unknown, options: ColumnBuilderOptions = {}): ColumnBuilder {
    return column("Text", { ...options, json: true });
  },
  timestamp(options: ColumnBuilderOptions = {}): ColumnBuilder {
    return column("Text", { ...options, timestamp: true });
  },
  defineApp<const Schema extends SchemaDefinition>(schema: Schema) {
    return defineApp(schema);
  },
  defineSchema<const Schema extends SchemaDefinition>(schema: Schema) {
    return defineSchema(schema);
  },
} as const;

export namespace schema {
  export type Schema<Definition extends SchemaDefinition> = Definition;
  export type App<Definition extends SchemaDefinition> = ReturnType<typeof defineApp<Definition>>;
  export type RowOf<TableOrQuery> =
    TableOrQuery extends Table<infer Row, unknown>
      ? Row
      : TableOrQuery extends QueryBuilder<infer Row>
        ? Row
        : never;
}

function makeTableDefinition(
  columns: ColumnDefinition[],
  options: TableOptions = {},
): TableDefinition {
  const indexed_columns =
    options.indexed_columns ??
    columns.filter((column) => column.indexOnly).map((column) => column.name);
  const table = {
    ...options,
    ...(indexed_columns.length === 0 ? {} : { indexed_columns }),
    columns,
    indexOnly(indexed_columns: readonly string[]): TableDefinition {
      return makeTableDefinition(columns, { ...options, indexed_columns });
    },
  };
  return table;
}

function enumColumn(...values: readonly string[]): ColumnBuilder;
function enumColumn(values: readonly string[]): ColumnBuilder;
function enumColumn(
  ...valuesOrArray: readonly string[] | readonly [readonly string[]]
): ColumnBuilder {
  const values = (
    Array.isArray(valuesOrArray[0]) ? valuesOrArray[0] : valuesOrArray
  ) as readonly string[];
  return column("Text", { enum: values });
}

class ColumnBuilderImpl implements ColumnBuilder {
  readonly #defaultValue?: unknown;
  readonly #hasDefault: boolean;
  readonly column_type: ColumnType;
  readonly nullable?: boolean;
  readonly references?: string;
  readonly large?: boolean;
  readonly enum?: readonly string[];
  readonly json?: boolean;
  readonly timestamp?: boolean;
  readonly #indexOnly?: boolean;

  constructor(
    column_type: ColumnType,
    options: ColumnBuilderOptions = {},
    hasDefault = Object.hasOwn(options, "default"),
  ) {
    this.column_type = column_type;
    this.nullable = options.nullable;
    this.references = options.references;
    this.large = options.large;
    this.#defaultValue = options.default;
    this.#hasDefault = hasDefault;
    this.enum = options.enum;
    this.json = options.json;
    this.timestamp = options.timestamp;
    this.#indexOnly = options.indexOnly;
  }

  optional(): ColumnBuilder {
    return this.with({ nullable: true });
  }

  default(value: unknown): ColumnBuilder {
    return this.with({ default: value });
  }

  indexOnly(): ColumnBuilder {
    return this.with({ indexOnly: true });
  }

  ref(tableOrToken: RefTarget): ColumnBuilder {
    return this.with({ references: refTargetName(tableOrToken) });
  }

  private with(options: ColumnBuilderOptions): ColumnBuilder {
    return new ColumnBuilderImpl(
      this.column_type,
      {
        nullable: this.nullable,
        references: this.references,
        large: this.large,
        ...(this.#hasDefault ? { default: this.#defaultValue } : {}),
        enum: this.enum,
        json: this.json,
        timestamp: this.timestamp,
        indexOnly: this.#indexOnly,
        ...options,
      },
      this.#hasDefault || Object.hasOwn(options, "default"),
    );
  }

  toColumnDefinition(name: string): ColumnDefinition {
    return {
      name,
      column_type: this.column_type,
      ...(this.nullable === undefined ? {} : { nullable: this.nullable }),
      ...(this.references === undefined ? {} : { references: this.references }),
      ...(this.large === undefined ? {} : { large: this.large }),
      ...(this.#hasDefault ? { default: this.#defaultValue } : {}),
      ...(this.enum === undefined ? {} : { enum: this.enum }),
      ...(this.json === undefined ? {} : { json: this.json }),
      ...(this.timestamp === undefined ? {} : { timestamp: this.timestamp }),
      ...(this.#indexOnly === undefined ? {} : { indexOnly: this.#indexOnly }),
    };
  }
}

function column(column_type: ColumnType, options: ColumnBuilderOptions): ColumnBuilder {
  return new ColumnBuilderImpl(column_type, options);
}

function columnTypeOf(column: ColumnDefinitionLike): ColumnType {
  if (column instanceof ColumnBuilderImpl) return column.column_type;
  return isColumnDefinitionInput(column) ? column.column_type : (column as ColumnType);
}

function materializeColumn(name: string, column: ColumnDefinitionLike): ColumnDefinition {
  if (column instanceof ColumnBuilderImpl) return column.toColumnDefinition(name);
  if (isColumnDefinitionInput(column)) return { name, ...column };
  return { name, column_type: column as ColumnType };
}

function refTargetName(tableOrToken: RefTarget): string {
  return typeof tableOrToken === "string" ? tableOrToken : tableOrToken._table;
}

export function defineSchema<const Schema extends SchemaDefinition>(
  schema: Schema,
): {
  readonly [TableName in keyof Schema]: Table<{ id: string }, Record<string, unknown>>;
} & { readonly _schema: Schema } {
  return defineApp(schema);
}

export function defineApp<const Schema extends SchemaDefinition>(
  schema: Schema,
): {
  readonly [TableName in keyof Schema]: Table<{ id: string }, Record<string, unknown>>;
} & { readonly _schema: Schema } {
  const tables: Record<string, unknown> = { _schema: schema };
  for (const tableName of Object.keys(schema)) {
    tables[tableName] = makeTableHandle(tableName, schema);
  }
  return tables as {
    readonly [TableName in keyof Schema]: Table<{ id: string }, Record<string, unknown>>;
  } & { readonly _schema: Schema };
}

export async function createDb(options: DbOptions): Promise<Db> {
  const resolvedOptions = await resolveDbOptions(options);
  const Runtime = options.Runtime ?? (await loadRuntime());
  return new CoreAbiDb(asWasmDbConstructor(Runtime), resolvedOptions);
}

export function parseJwtPayload(jwtToken: string): JwtPayload | null {
  const token = trimOptional(jwtToken);
  if (!token) return null;
  const parts = token.split(".");
  if (parts.length < 2 || parts[1] == null) return null;
  const payloadJson = decodeBase64UrlToUtf8(parts[1]);
  if (!payloadJson) return null;
  try {
    const parsed = JSON.parse(payloadJson);
    return isRecord(parsed) ? parsed : null;
  } catch {
    return null;
  }
}

export function sessionFromJwtPayload(payload: JwtPayload): Session | null {
  const subject = trimOptional(payload.sub);
  if (!subject) return null;
  const issuer = trimOptional(payload.iss);
  const audience = trimOptional(payload.aud);
  const claims = isRecord(payload.claims) ? { ...payload.claims } : {};
  claims.subject = subject;
  if (issuer) claims.issuer = issuer;
  if (audience) claims.audience = audience;
  return {
    user_id: subject,
    claims,
    authMode: authModeFromIssuer(issuer),
  };
}

export function resolveJwtSession(jwtToken: string): Session | null {
  const payload = parseJwtPayload(jwtToken);
  return payload ? sessionFromJwtPayload(payload) : null;
}

export function resolveClientSessionStateSync(
  config: Pick<DbOptions, "appId" | "jwtToken" | "cookieSession">,
): ClientSessionState {
  const payload = parseJwtPayload(config.jwtToken ?? "");
  if (payload) {
    const audience = trimOptional(payload.aud);
    if (audience && audience !== config.appId) return { transport: null, session: null };
  }
  const jwtSession = payload ? sessionFromJwtPayload(payload) : null;
  if (jwtSession) return { transport: "bearer", session: jwtSession };
  if (config.cookieSession) return { transport: "cookie", session: config.cookieSession };
  return { transport: null, session: null };
}

export function resolveClientSessionSync(
  config: Pick<DbOptions, "appId" | "jwtToken" | "cookieSession">,
): Session | null {
  return resolveClientSessionStateSync(config).session;
}

export async function createFileFromBlob<Row extends BinaryLargeValueRow>(
  db: Db,
  table: Table<Row, unknown>,
  options: BinaryLargeValueInput,
): Promise<Row> {
  const data = new Uint8Array(await options.blob.arrayBuffer());
  const row: Record<string, unknown> = { id: options.rowId ?? options.fileId, data };
  if (hasColumn(table, "name")) row.name = options.name ?? "";
  if (hasColumn(table, "mime_type")) row.mime_type = options.mimeType ?? options.blob.type;
  return normalizeBinaryLargeValueRow(db.insert(table, row as Row & Partial<Pick<Row, "id">>));
}

export async function loadFileAsBlob<Row extends BinaryLargeValueRow>(
  db: Db,
  table: Table<Row, unknown>,
  rowId: Row["id"],
): Promise<Blob> {
  const file = readFile(db, table, rowId);
  const data = readFileBytes(db, table, rowId);
  const expectedSize = file.size == null ? undefined : Number(file.size);
  if (expectedSize != null && data.length !== expectedSize) {
    throw new Error(
      `file ${file.name ?? formatUuid(encodeRowId(rowId))} expected ${expectedSize} bytes, loaded ${data.length}`,
    );
  }
  return new Blob([arrayBufferFromBytes(data)], { type: file.mime_type ?? "" });
}

export function readFileBytes<Row extends BinaryLargeValueRow>(
  db: Db,
  table: Table<Row, unknown>,
  rowId: Row["id"],
): Uint8Array {
  return readFile(db, table, rowId).data;
}

export function readFiles<Row extends BinaryLargeValueRow>(
  db: Db,
  table: Table<Row, unknown>,
): Row[] {
  return db.all(table).map(normalizeBinaryLargeValueRow);
}

export function deleteFile<Row extends BinaryLargeValueRow>(
  db: Db,
  table: Table<Row, unknown>,
  rowId: Row["id"],
): void {
  db.delete(table, rowId);
}

class CoreAbiDb implements Db {
  readonly #schema: SchemaDefinition;
  readonly #db: WasmDb;
  readonly #preparedQueries = new Map<string, WasmPreparedQuery>();
  readonly #authStore: AuthStateStore;
  readonly #subscriptions = new Set<CoreAbiSubscription<unknown>>();
  #nextRowId: number;
  #closed = false;

  constructor(Runtime: WasmDbConstructor, options: ResolvedDbOptions) {
    this.#schema = options.schema;
    this.#nextRowId = options.nextRowId ?? 1;
    this.#authStore = createAuthStateStore(options);
    this.#db = Runtime.openMemory(
      encodeSchema(this.#schema),
      openConfig(options.node, options.accountAuthor, options.accountId, options.server ?? false),
    );
  }

  table<Row extends { id: string | Uint8Array }, Init = Omit<Row, "id">>(
    name: string,
  ): Table<Row, Init> {
    this.#assertOpen();
    this.#tableDefinition(name);
    return makeTableHandle<Row, Init>(name, this.#schema);
  }

  beginTransaction(options: TransactionOptions = {}): Transaction {
    this.#assertOpen();
    if ((options.kind ?? "mergeable") === "exclusive") {
      throw new Error(
        "exclusive transactions are not supported by the core wasm object facade yet",
      );
    }
    return new CoreAbiTransaction(this, this.#db.mergeableTx());
  }

  transaction<Value>(callback: (tx: Transaction) => PromiseLike<Value>): Promise<Value>;
  transaction<Value>(callback: (tx: Transaction) => Value): Value;
  transaction<Value>(
    callback: (tx: Transaction) => Value | PromiseLike<Value>,
  ): Value | Promise<Value> {
    const tx = this.beginTransaction();
    try {
      const result = callback(tx);
      if (isPromiseLike(result)) {
        return Promise.resolve(result).then(
          (value) => {
            tx.commit();
            return value;
          },
          (error) => {
            tx.rollback();
            throw error;
          },
        );
      }
      tx.commit();
      return result;
    } catch (error) {
      tx.rollback();
      throw error;
    }
  }

  insert<Row extends { id: string | Uint8Array }, Init>(
    table: Table<Row, Init>,
    row: Init & Partial<Pick<Row, "id">>,
    options: InsertOptions<Row> = {},
  ): WriteResult<Row> & Row {
    this.#assertOpen();
    const rowIdInput = options.id ?? row.id;
    const rowId = rowIdInput == null ? this.#allocateRowId() : encodeRowId(rowIdInput);
    const definition = this.#tableDefinition(table._table);
    const cells = encodeCellsForRow(definition, row);
    const write = this.#db.insertWithIdEncoded(table._table, rowId, cells);
    this.#pumpSubscriptions();
    const value =
      this.#findVisibleRowById(table, rowId) ??
      (materializePendingRow(rowId, row as Record<string, unknown>, definition) as Row);
    return makeWriteResult(value, write);
  }

  update<Row extends { id: string | Uint8Array }>(
    table: Table<Row, unknown>,
    id: Row["id"],
    patch: Partial<Omit<Row, "id">>,
    _options: WriteTimestampOptions = {},
  ): WriteResult<Row> & Row {
    this.#assertOpen();
    const rowId = encodeRowId(id);
    const write = this.#db.updateEncoded(
      table._table,
      rowId,
      encodeCellsForPatch(this.#tableDefinition(table._table), patch),
    );
    this.#pumpSubscriptions();
    return makeWriteResult(this.#expectOne(table, rowId, "update"), write);
  }

  upsert<Row extends { id: string | Uint8Array }, Init>(
    table: Table<Row, Init>,
    row: Init & Partial<Pick<Row, "id">>,
    options: UpsertOptions<Row>,
  ): WriteResult<Row> & Row {
    this.#assertOpen();
    if (options.id == null && row.id == null)
      throw new Error("db.upsert requires options.id or row.id");
    const rowId = encodeRowId(options.id ?? (row.id as Row["id"]));
    const cells = encodeCellsForRow(this.#tableDefinition(table._table), row);
    const write = this.#db.upsertEncoded(table._table, rowId, cells);
    this.#pumpSubscriptions();
    return makeWriteResult(this.#expectOne(table, rowId, "upsert"), write);
  }

  delete<Row extends { id: string | Uint8Array }>(
    table: Table<Row, unknown>,
    id: Row["id"],
    _options: WriteTimestampOptions = {},
  ): WriteResult<void> {
    this.#assertOpen();
    const write = this.#db.delete(table._table, encodeRowId(id));
    this.#pumpSubscriptions();
    return makeWriteResult(undefined, write);
  }

  restore<Row extends { id: string | Uint8Array }, Init>(
    table: Table<Row, Init>,
    id: Row["id"],
    row: Init,
    _options: WriteTimestampOptions = {},
  ): WriteResult<Row> & Row {
    this.#assertOpen();
    const rowId = encodeRowId(id);
    if (this.#findVisibleRowById(table, rowId)) {
      throw new Error(`Restore failed: row not deleted: ${formatUuid(rowId)}`);
    }
    const write = this.#db.restoreEncoded(
      table._table,
      rowId,
      encodeCellsForRow(this.#tableDefinition(table._table), row as Record<string, unknown>),
    );
    this.#pumpSubscriptions();
    return makeWriteResult(this.#expectOne(table, rowId, "restore"), write);
  }

  all<Row>(
    tableOrQuery: Table<Row & { id: string | Uint8Array }, unknown> | QueryBuilder<Row>,
    options: ReadOptions = {},
  ): Row[] {
    this.#assertOpen();
    const relationQuery = builtQueryWithRelations(tableOrQuery);
    if (relationQuery) return this.#readBuiltQuery(relationQuery, undefined, options) as Row[];
    if ("_build" in tableOrQuery) {
      const built = JSON.parse(tableOrQuery._build()) as BuiltQuery;
      if (queryNeedsJsPredicateFallback(built)) {
        return applyBuiltQueryFallback(
          this.#readRows(queryFromTable(queryTableName(built)), undefined, options),
          built,
          this.#schema,
        ) as Row[];
      }
    }
    const query = this.#prepareQuery(tableOrQuery);
    return decodeRows(
      readRowBatches(this.#db.all(query, readOptions(options))),
      this.#schema,
    ) as Row[];
  }

  one<Row>(
    tableOrQuery: Table<Row & { id: string | Uint8Array }, unknown> | QueryBuilder<Row>,
    options: ReadOptions = {},
  ): Row | null {
    const query =
      "limit" in tableOrQuery && typeof tableOrQuery.limit === "function"
        ? tableOrQuery.limit(1)
        : tableOrQuery;
    return this.all(query, options)[0] ?? null;
  }

  allForIdentity<Row>(
    tableOrQuery: Table<Row & { id: string | Uint8Array }, unknown> | QueryBuilder<Row>,
    identity: Identity,
    options: ReadOptions = {},
  ): Row[] {
    this.#assertOpen();
    const relationQuery = builtQueryWithRelations(tableOrQuery);
    if (relationQuery) return this.#readBuiltQuery(relationQuery, identity, options) as Row[];
    const query = this.#prepareQuery(tableOrQuery);
    return decodeRows(
      readRowBatches(
        this.#db.allForIdentity(query, encodeIdentity(identity), readOptions(options)),
      ),
      this.#schema,
    ) as Row[];
  }

  subscribe<Row>(
    tableOrQuery: Table<Row & { id: string | Uint8Array }, unknown> | QueryBuilder<Row>,
    callback: SubscriptionCallback<Row>,
  ): Subscription<Row> {
    this.#assertOpen();
    const relationQuery = subscribeRelationQuery(tableOrQuery, this.#schema);
    const query = relationQuery
      ? this.#prepareQueryBytes(encodeBuiltQuery(JSON.stringify(relationQuery), this.#schema))
      : this.#prepareQuery(tableOrQuery);
    const reader = this.#db.subscribe(query, subscriptionReadOptions()).getReader();
    const subscription = new CoreAbiSubscription(
      reader,
      this.#schema,
      () => {
        this.#subscriptions.delete(subscription as CoreAbiSubscription<unknown>);
      },
      callback,
      relationQuery,
      (table, rowId) =>
        this.#readRows(queryFromTable(table)).find((row) =>
          sameBytes(encodeRowId(row.id), rowId),
        ) ?? null,
    );
    this.#subscriptions.add(subscription as CoreAbiSubscription<unknown>);
    void subscription.start().catch((error: unknown) => {
      queueMicrotask(() => {
        throw error;
      });
    });
    return subscription;
  }

  getAuthState(): AuthState {
    return this.#authStore.getState();
  }

  onAuthChanged(listener: (state: AuthState) => void): () => void {
    return this.#authStore.onChange(listener);
  }

  updateAuthToken(jwtToken: string | null): void {
    this.#assertOpen();
    const nextToken = jwtToken ?? undefined;
    this.#authStore.applyJwtToken(nextToken);
  }

  _connectUpstreamTransport(): WasmTransport {
    this.#assertOpen();
    return this.#db.connectUpstream();
  }

  #pumpSubscriptions(): void {
    for (const subscription of this.#subscriptions) subscription.pumpDelta();
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
  }

  #prepareQuery<Row>(
    tableOrQuery: Table<Row & { id: string | Uint8Array }, unknown> | QueryBuilder<Row>,
  ): WasmPreparedQuery {
    this.#assertOpen();
    const queryBytes =
      "_build" in tableOrQuery
        ? encodeBuiltQuery(tableOrQuery._build(), this.#schema)
        : queryFromTable(tableOrQuery._table);
    return this.#prepareQueryBytes(queryBytes);
  }

  #prepareQueryBytes(queryBytes: Uint8Array): WasmPreparedQuery {
    const key = bytesKey(queryBytes);
    let prepared = this.#preparedQueries.get(key);
    if (!prepared) {
      prepared = this.#db.prepareQuery(queryBytes);
      this.#preparedQueries.set(key, prepared);
    }
    return prepared;
  }

  #tableDefinition(tableName: string): TableDefinition {
    const definition = this.#schema[tableName];
    if (!definition) throw new Error(`unknown table ${tableName}`);
    return definition;
  }

  _transactionTableDefinition(tableName: string): TableDefinition {
    return this.#tableDefinition(tableName);
  }

  _schemaForTransaction(): SchemaDefinition {
    return this.#schema;
  }

  _transactionAllocateRowId(): Uint8Array {
    return this.#allocateRowId();
  }

  _transactionFindVisibleRowById<Row extends { id: string | Uint8Array }>(
    table: Table<Row, unknown>,
    rowId: Uint8Array,
  ): Row | undefined {
    return this.#findVisibleRowById(table, rowId);
  }

  _pumpSubscriptions(): void {
    this.#pumpSubscriptions();
  }

  _assertTransactionOpen(): void {
    this.#assertOpen();
  }

  #readBuiltQuery(
    query: BuiltQuery,
    identity?: Identity,
    options: ReadOptions = {},
  ): Array<Record<string, unknown>> {
    const table = queryTableName(query);
    const baseQuery = stripIdFilters({
      ...query,
      includes: undefined,
      hops: undefined,
      gather: undefined,
    });
    const baseRows = filterRowsById(
      this.#readRows(encodeBuiltQuery(JSON.stringify(baseQuery), this.#schema), identity, options),
      query,
    );
    if (query.hops?.length) return this.#applyHops(table, baseRows, query.hops, identity, options);
    if (query.gather) return this.#applyGather(table, baseRows, query.gather, identity, options);
    if (query.includes && Object.keys(query.includes).length > 0)
      return this.#applyIncludes(table, baseRows, query.includes, identity, options);
    return baseRows;
  }

  #readRows(
    queryBytes: Uint8Array,
    identity?: Identity,
    options: ReadOptions = {},
  ): Array<Record<string, unknown>> {
    const key = bytesKey(queryBytes);
    let query = this.#preparedQueries.get(key);
    if (!query) {
      query = this.#db.prepareQuery(queryBytes);
      this.#preparedQueries.set(key, query);
    }
    const rows =
      identity == null
        ? this.#db.all(query, readOptions(options))
        : this.#db.allForIdentity(query, encodeIdentity(identity), readOptions(options));
    return decodeRows(readRowBatches(rows), this.#schema);
  }

  #applyIncludes(
    table: string,
    rows: Array<Record<string, unknown>>,
    includes: QueryInclude,
    identity?: Identity,
    options: ReadOptions = {},
  ): Array<Record<string, unknown>> {
    return rows.flatMap((row) => {
      const expanded = { ...row };
      for (const includeName of Object.keys(includes)) {
        if (includes[includeName] === undefined) continue;
        const relation = includeRelation(this.#schema, table, includeName);
        const include = normalizeIncludeOptions(includes[includeName]);
        if (relation.direction === "forward") {
          const targetId = row[relation.column];
          const targetRows =
            targetId == null
              ? []
              : this.#readBuiltQuery(
                  relationQuery(relation.table, "id", targetId),
                  identity,
                  options,
                );
          const expandedTargetRows = include.include
            ? this.#applyIncludes(relation.table, targetRows, include.include, identity, options)
            : targetRows;
          const target =
            expandedTargetRows[0] == null
              ? null
              : projectIncludedRow(expandedTargetRows[0], include.select, include.include);
          if (include.required && target == null) return [];
          expanded[includeName] = target;
        } else {
          const childRows = this.#readBuiltQuery(
            relationQuery(relation.table, relation.column, row.id),
            identity,
            options,
          );
          const expandedChildRows = include.include
            ? this.#applyIncludes(relation.table, childRows, include.include, identity, options)
            : childRows;
          const children = expandedChildRows.map((child) =>
            projectIncludedRow(child, include.select, include.include),
          );
          if (include.required && children.length === 0) return [];
          expanded[includeName] = children;
        }
      }
      return [expanded];
    });
  }

  #applyHops(
    table: string,
    rows: Array<Record<string, unknown>>,
    hops: string[],
    identity?: Identity,
    options: ReadOptions = {},
  ): Array<Record<string, unknown>> {
    let currentTable = table;
    let currentRows = rows;
    for (const hop of hops) {
      const relation = forwardRelation(this.#schema, currentTable, hop);
      currentRows = currentRows.flatMap((row) =>
        relationValues(row[relation.column]).flatMap((id) =>
          this.#readBuiltQuery(relationQuery(relation.table, "id", id), identity, options),
        ),
      );
      currentTable = relation.table;
    }
    return uniqueRows(currentRows);
  }

  #applyGather(
    table: string,
    rows: Array<Record<string, unknown>>,
    gather: GatherOptions,
    identity?: Identity,
    options: ReadOptions = {},
  ): Array<Record<string, unknown>> {
    if (gather.step_table !== table)
      throw new Error("alpha gather currently requires step_table to match the starting table");
    if (gather.step_current_column !== "id")
      throw new Error("alpha gather currently requires step_current_column: 'id'");
    const seen = new Set<string>();
    let frontier = rows;
    const gathered: Array<Record<string, unknown>> = [];
    for (let depth = 0; depth <= gather.max_depth && frontier.length > 0; depth += 1) {
      const fresh = frontier.filter((row) => {
        const key = String(row.id);
        if (seen.has(key)) return false;
        seen.add(key);
        gathered.push(row);
        return true;
      });
      if (depth === gather.max_depth) break;
      frontier = this.#applyHops(table, fresh, gather.step_hops, identity, options);
      if (gather.step_conditions?.length) {
        const allowed = new Set(
          this.#readBuiltQuery(
            { table, conditions: gather.step_conditions },
            identity,
            options,
          ).map((row) => String(row.id)),
        );
        frontier = frontier.filter((row) => allowed.has(String(row.id)));
      }
    }
    return gathered;
  }

  #expectOne<Row extends { id: string | Uint8Array }>(
    table: Table<Row, unknown>,
    rowId: Uint8Array,
    operation: string,
  ): Row {
    const found = this.#findVisibleRowById(table, rowId);
    if (!found) throw new Error(`${operation} did not produce row ${formatUuid(rowId)}`);
    return found;
  }

  #findVisibleRowById<Row extends { id: string | Uint8Array }>(
    table: Table<Row, unknown>,
    rowId: Uint8Array,
  ): Row | undefined {
    return this.all(table).find((row) => sameBytes(encodeRowId(row.id), rowId));
  }

  #allocateRowId(): Uint8Array {
    this.#assertOpen();
    const rowId = new Uint8Array(16);
    new DataView(rowId.buffer).setUint32(12, this.#nextRowId++);
    return rowId;
  }

  #assertOpen(): void {
    if (this.#closed) throw new Error("db is closed");
  }
}

class CoreAbiTransaction implements Transaction {
  #closed = false;

  constructor(
    private readonly db: CoreAbiDb,
    private readonly tx: WasmTx,
  ) {}

  insert<Row extends { id: string | Uint8Array }, Init>(
    table: Table<Row, Init>,
    row: Init & Partial<Pick<Row, "id">>,
    options: InsertOptions<Row> = {},
  ): Row {
    this.#assertOpen();
    const definition = this.db._transactionTableDefinition(table._table);
    const rowIdInput = options.id ?? row.id;
    const rowId =
      rowIdInput == null ? this.db._transactionAllocateRowId() : encodeRowId(rowIdInput);
    this.tx.insertWithIdEncoded(table._table, rowId, encodeCellsForRow(definition, row));
    return materializePendingRow(rowId, row as Record<string, unknown>, definition) as Row;
  }

  update<Row extends { id: string | Uint8Array }>(
    table: Table<Row, unknown>,
    id: Row["id"],
    patch: Partial<Omit<Row, "id">>,
    _options: WriteTimestampOptions = {},
  ): void {
    this.#assertOpen();
    this.tx.updateEncoded(
      table._table,
      encodeRowId(id),
      encodeCellsForPatch(this.db._transactionTableDefinition(table._table), patch),
    );
  }

  upsert<Row extends { id: string | Uint8Array }, Init>(
    table: Table<Row, Init>,
    row: Init & Partial<Pick<Row, "id">>,
    options: UpsertOptions<Row>,
  ): Row {
    this.#assertOpen();
    if (options.id == null && row.id == null)
      throw new Error("tx.upsert requires options.id or row.id");
    const definition = this.db._transactionTableDefinition(table._table);
    const rowId = encodeRowId(options.id ?? (row.id as Row["id"]));
    this.tx.upsertEncoded(table._table, rowId, encodeCellsForRow(definition, row));
    return materializePendingRow(rowId, row as Record<string, unknown>, definition) as Row;
  }

  delete<Row extends { id: string | Uint8Array }>(
    table: Table<Row, unknown>,
    id: Row["id"],
    _options: WriteTimestampOptions = {},
  ): void {
    this.#assertOpen();
    this.tx.delete(table._table, encodeRowId(id));
  }

  restore<Row extends { id: string | Uint8Array }, Init>(
    table: Table<Row, Init>,
    id: Row["id"],
    row: Init,
    _options: WriteTimestampOptions = {},
  ): Row {
    this.#assertOpen();
    const definition = this.db._transactionTableDefinition(table._table);
    const rowId = encodeRowId(id);
    this.tx.restoreEncoded(
      table._table,
      rowId,
      encodeCellsForRow(definition, row as Record<string, unknown>),
    );
    return materializePendingRow(rowId, row as Record<string, unknown>, definition) as Row;
  }

  all<Row>(
    _tableOrQuery: Table<Row & { id: string | Uint8Array }, unknown> | QueryBuilder<Row>,
    _options: ReadOptions = {},
  ): Row[] {
    this.#assertOpen();
    throw new Error(
      "mergeable transaction reads are not supported by the core wasm object facade yet",
    );
  }

  one<Row>(
    _tableOrQuery: Table<Row & { id: string | Uint8Array }, unknown> | QueryBuilder<Row>,
    _options: ReadOptions = {},
  ): Row | null {
    this.#assertOpen();
    throw new Error(
      "mergeable transaction reads are not supported by the core wasm object facade yet",
    );
  }

  commit(): WriteResult<void> {
    this.#assertOpen();
    this.#closed = true;
    const write = this.tx.commit();
    this.db._pumpSubscriptions();
    return makeWriteResult(undefined, write);
  }

  rollback(): void {
    this.#assertOpen();
    this.#closed = true;
    this.tx.rollback();
  }

  #assertOpen(): void {
    this.db._assertTransactionOpen();
    if (this.#closed) throw new Error("transaction is already closed");
  }
}

class CoreAbiSubscription<Row> implements Subscription<Row> {
  #currentRows: Array<Record<string, unknown>> = [];

  constructor(
    private readonly reader: ReadableStreamDefaultReader<SubscriptionStreamChunk>,
    private readonly schema: SchemaDefinition,
    private readonly onUnsubscribe: () => void,
    private readonly callback: SubscriptionCallback<Row>,
    private readonly relationQuery?: BuiltQuery,
    private readonly resolveIncludedRow?: (
      table: string,
      rowId: Uint8Array,
    ) => Record<string, unknown> | null,
  ) {}

  async start(): Promise<void> {
    const opened = await this.reader.read();
    const chunk = opened.done ? null : normalizeSubscriptionChunk(opened.value);
    if (!chunk || chunk.type !== "snapshot") {
      throw new Error(`expected subscription snapshot chunk, got ${JSON.stringify(chunk)}`);
    }
    this.emitOpened(chunk.rows);
  }

  emitOpened(currentRows: AbiRowBatch[]): Row[] {
    this.#currentRows = decodeRows(currentRows, this.schema);
    const current = this.materializeRows(this.#currentRows);
    this.callback(current);
    return current;
  }

  pumpDelta(): void {
    void this.readAndEmitDelta();
  }

  private async readAndEmitDelta(): Promise<Row[]> {
    const next = await this.reader.read();
    if (next.done) return this.materializeRows(this.#currentRows);
    const chunk = normalizeSubscriptionChunk(next.value);
    if (chunk.type !== "delta") {
      return this.emitOpened(chunk.rows);
    }
    this.#currentRows = applySubscriptionDeltaRows(this.#currentRows, chunk.delta, this.schema);
    const current = this.materializeRows(this.#currentRows);
    this.callback(current);
    return current;
  }

  unsubscribe(): void {
    this.onUnsubscribe();
    void this.reader.cancel();
  }

  private materializeRows(rows: Array<Record<string, unknown>>): Row[] {
    if (!this.relationQuery?.includes) return rows as Row[];
    const table = queryTableName(this.relationQuery);
    const relationSnapshot = relationSubscriptionSnapshotFromRowPayload(
      table,
      rows,
      this.relationQuery.includes,
      this.schema,
    );
    const rootRows = filterRowsById(
      rows.filter((row) => row["__jazz_table"] === table),
      this.relationQuery,
    );
    return applyRelationSubscriptionIncludes(
      table,
      rootRows,
      this.relationQuery.includes,
      relationSnapshot,
      this.schema,
      rows,
      this.resolveIncludedRow,
    ) as Row[];
  }
}

function normalizeSubscriptionChunk(chunk: unknown): SubscriptionStreamChunk {
  if (!isRecord(chunk)) throw new Error("expected subscription stream chunk object");
  if (chunk.type === "snapshot" || chunk.type === "Snapshot") {
    const rows = (chunk as { rows?: unknown }).rows;
    return {
      type: "snapshot",
      rows: readRowBatches(assertBytes(rows, "subscription snapshot rows")),
    };
  }
  if (chunk.type === "delta" || chunk.type === "Delta") {
    const delta = (chunk as { delta?: unknown }).delta;
    if (delta instanceof Uint8Array || Array.isArray(delta)) {
      return {
        type: "delta",
        delta: readSubscriptionDelta(assertBytes(delta, "subscription delta")),
      };
    }
    if (!isRecord(delta)) throw new Error("expected subscription delta");
    return { type: "delta", delta: normalizeAbiSubscriptionDelta(delta) };
  }
  const snapshot = chunk.snapshot ?? chunk.Snapshot;
  if (isRecord(snapshot) && Array.isArray(snapshot.rows)) {
    return { type: "snapshot", rows: normalizeAbiRowBatches(snapshot.rows) };
  }
  const delta = chunk.delta ?? chunk.Delta;
  if (isRecord(delta) && isRecord(delta.delta)) {
    return { type: "delta", delta: normalizeAbiSubscriptionDelta(delta.delta) };
  }
  throw new Error(`unknown subscription stream chunk shape: ${JSON.stringify(chunk)}`);
}

function normalizeAbiSubscriptionDelta(delta: Record<string, unknown>): AbiSubscriptionDelta {
  const added = delta.added;
  const updated = delta.updated;
  const removed = delta.removed;
  if (!Array.isArray(added) || !Array.isArray(updated) || !Array.isArray(removed)) {
    throw new Error("expected subscription delta row arrays");
  }
  return {
    added: normalizeAbiRowBatches(added),
    updated: normalizeAbiRowBatches(updated),
    removed: removed.map((row) => {
      if (!isRecord(row)) throw new Error("expected removed row object");
      return {
        table: String(row.table),
        rowId: encodeRowId(row.rowId ?? row.row_id),
      };
    }) as AbiSubscriptionDelta["removed"],
  };
}

function normalizeAbiRowBatches(rows: unknown[]): AbiRowBatch[] {
  return rows.map((batch) => {
    if (!isRecord(batch) || !Array.isArray(batch.rows)) throw new Error("expected ABI row batch");
    return {
      ...(batch as AbiRowBatch),
      descriptor: normalizeDescriptor((batch as { descriptor?: unknown }).descriptor),
      rows: batch.rows.map((row) => {
        if (!isRecord(row)) throw new Error("expected ABI row object");
        return {
          ...(row as AbiRowBatch["rows"][number]),
          rowId: encodeRowId(row.rowId ?? row.row_id),
          raw: assertBytes(row.raw, "subscription row raw"),
        };
      }),
    };
  });
}

function normalizeDescriptor(descriptor: unknown): DescriptorField[] {
  if (!Array.isArray(descriptor)) throw new Error("expected ABI row descriptor");
  return descriptor.map((field) => {
    if (!isRecord(field)) throw new Error("expected ABI descriptor field");
    return {
      name: typeof field.name === "string" ? field.name : undefined,
      valueType: normalizeValueType(field.valueType ?? field.value_type),
    };
  });
}

function normalizeValueType(valueType: unknown): ValueType {
  if (typeof valueType === "string") return { tag: valueTypeTag(valueType) };
  if (isRecord(valueType) && !("tag" in valueType)) {
    const entries = Object.entries(valueType);
    if (entries.length === 1) {
      const [variant, payload] = entries[0];
      const tag = valueTypeTag(variant);
      if (tag === 10 && Array.isArray(payload))
        return { tag, inner: normalizeValueType(payload[0]) };
      if ((tag === 11 || tag === 12) && payload != null)
        return { tag, inner: normalizeValueType(payload) };
      return { tag };
    }
  }
  if (!isRecord(valueType)) throw new Error("expected ABI descriptor value type");
  const inner = valueType.inner == null ? undefined : normalizeValueType(valueType.inner);
  return inner == null ? (valueType as ValueType) : { ...(valueType as ValueType), inner };
}

function valueTypeTag(variant: string): number {
  const tags: Record<string, number> = {
    U8: 0,
    U16: 1,
    U32: 2,
    U64: 3,
    F64: 4,
    Bool: 5,
    String: 6,
    Bytes: 7,
    Uuid: 8,
    Enum: 9,
    Tuple: 10,
    Array: 11,
    Nullable: 12,
  };
  const tag = tags[variant];
  if (tag == null) throw new Error(`unknown ABI descriptor value type ${variant}`);
  return tag;
}

function makeWriteResult<Value extends object>(
  value: Value,
  handle: WriteHandle | null,
): WriteResult<Value> & Value;
function makeWriteResult(value: void, handle: WriteHandle | null): WriteResult<void>;
function makeWriteResult<Value>(
  value: Value,
  handle: WriteHandle | null,
): WriteResult<Value> | (WriteResult<Value> & object) {
  const target = isRecord(value) || value instanceof Uint8Array ? (value as object) : {};
  Object.defineProperties(target, {
    value: {
      value,
      enumerable: false,
      configurable: true,
    },
    handle: {
      value: handle,
      enumerable: false,
      configurable: true,
    },
    wait: {
      value: async (options: WriteWaitOptions = {}) => {
        const tier = normalizeWriteTier(options.tier ?? "local");
        if (tier !== "Local") {
          throw new Error(
            `write wait tier "${tier}" is not supported by this current jazz-tools/WasmDb slice yet`,
          );
        }
        return value;
      },
      enumerable: false,
      configurable: true,
    },
  });
  return target as WriteResult<Value> & object;
}

function materializePendingRow(
  rowId: Uint8Array,
  row: Record<string, unknown>,
  definition: TableDefinition,
): Record<string, unknown> {
  const next: Record<string, unknown> = { id: formatUuid(rowId) };
  for (const column of definition.columns) {
    if (Object.hasOwn(row, column.name)) {
      next[column.name] = row[column.name];
    } else if (Object.hasOwn(column, "default")) {
      next[column.name] = column.default;
    }
  }
  return next;
}

function normalizeWriteTier(tier: WriteTier): "Local" | "Edge" | "Global" {
  if (tier === "local" || tier === "Local") return "Local";
  if (tier === "edge" || tier === "Edge") return "Edge";
  return "Global";
}

function readOptions(options: ReadOptions): unknown {
  return {
    tier: "Local",
    local_updates: "Immediate",
    propagation: "Full",
    include_deleted: options.includeDeleted === true,
  };
}

function subscriptionReadOptions(): unknown {
  return {
    tier: "Local",
    local_updates: "Immediate",
    propagation: "Full",
    include_deleted: false,
  };
}

type AuthStateStore = {
  getState(): AuthState;
  onChange(listener: (state: AuthState) => void): () => void;
  applyJwtToken(jwtToken?: string): boolean;
};

async function resolveDbOptions(options: DbOptions): Promise<ResolvedDbOptions> {
  if (options.secret && (options.jwtToken || options.cookieSession)) {
    throw new Error("DbOptions error: secret, jwtToken, and cookieSession are mutually exclusive");
  }
  if (options.jwtToken && options.cookieSession) {
    throw new Error("DbOptions error: jwtToken and cookieSession are mutually exclusive");
  }
  const appId = options.appId ?? "jazz-tools-alpha";
  const localFirst = options.secret
    ? await localFirstAuthForSecret(appId, options.secret)
    : undefined;
  const jwtToken = localFirst?.jwtToken ?? options.jwtToken;
  const session =
    localFirst?.session ??
    resolveClientSessionSync({ appId, jwtToken, cookieSession: options.cookieSession });
  const accountAuthor =
    options.accountAuthor ?? localFirst?.accountAuthor ?? authorForSession(appId, session);
  return {
    ...options,
    appId,
    node: options.node ?? deterministicBytes(`node:${appId}`, 16),
    accountAuthor,
    accountId: options.accountId ?? accountIdForAuthor(accountAuthor),
    jwtToken,
  };
}

async function localFirstAuthForSecret(
  appId: string,
  secret: string,
): Promise<{ accountAuthor: Uint8Array; jwtToken: string; session: Session }> {
  const accountAuthor = deterministicBytes(`local-first:${appId}:${secret}`, 16);
  const subject = formatUuid(accountAuthor);
  const jwtToken = await createLocalFirstJwtAsync({ appId, secret, subject });
  const session = resolveJwtSession(jwtToken);
  if (!session) throw new Error("failed to derive local-first session");
  return { accountAuthor, jwtToken, session };
}

function authorForSession(appId: string, session: Session | null): Uint8Array {
  if (session)
    return deterministicBytes(`session:${appId}:${session.authMode}:${session.user_id}`, 16);
  return deterministicBytes(`anonymous:${appId}`, 16);
}

function accountIdForAuthor(author: Uint8Array): number {
  const view = new DataView(author.buffer, author.byteOffset, author.byteLength);
  return view.getUint32(0, true);
}

function createAuthStateStore(
  input: Pick<ResolvedDbOptions, "appId" | "jwtToken" | "cookieSession">,
): AuthStateStore {
  let state = deriveAuthState(input);
  const initialAuthMode = state.authMode;
  const listeners = new Set<(state: AuthState) => void>();
  const emit = () => {
    for (const listener of listeners) listener(state);
  };
  return {
    getState() {
      return state;
    },
    onChange(listener) {
      listeners.add(listener);
      listener(state);
      return () => {
        listeners.delete(listener);
      };
    },
    applyJwtToken(jwtToken) {
      const resolved = resolveClientSessionStateSync({
        appId: input.appId,
        jwtToken,
        cookieSession: input.cookieSession,
      });
      const currentUserId = state.session?.user_id ?? null;
      const nextUserId = resolved.session?.user_id ?? null;
      if (currentUserId !== nextUserId) {
        throw new Error(
          "Changing auth principal on a live client is not supported. Recreate the Db.",
        );
      }
      const nextState: AuthState = { authMode: initialAuthMode, session: resolved.session };
      if (authStateEquals(state, nextState)) return false;
      state = nextState;
      emit();
      return true;
    },
  };
}

function deriveAuthState(
  input: Pick<ResolvedDbOptions, "appId" | "jwtToken" | "cookieSession">,
): AuthState {
  const resolved = resolveClientSessionStateSync(input);
  return {
    authMode: resolved.session?.authMode ?? "external",
    session: resolved.session,
  };
}

function authStateEquals(left: AuthState, right: AuthState): boolean {
  return (
    left.authMode === right.authMode &&
    left.error === right.error &&
    sessionsEqual(left.session, right.session)
  );
}

function sessionsEqual(left: Session | null, right: Session | null): boolean {
  if (left === right) return true;
  if (!left || !right) return false;
  return (
    left.user_id === right.user_id &&
    left.authMode === right.authMode &&
    JSON.stringify(left.claims) === JSON.stringify(right.claims)
  );
}

function encodeSchema(schema: SchemaDefinition): Uint8Array {
  const tables = Object.entries(schema);
  const writer = new PostcardWriter();
  writer.vec((table, index) => {
    const [tableName, definition] = tables[index];
    table.string(tableName);
    table.vec((column, columnIndex) => {
      const columnSpec = definition.columns[columnIndex];
      column.string(columnSpec.name);
      writeCompleteValueType(column, columnValueType(columnSpec));
      if (columnSpec.large) {
        if (valueTypeFromColumnType(columnSpec.column_type).tag !== 7) {
          throw new Error(`large value column ${tableName}.${columnSpec.name} must be Bytea`);
        }
        column.some((largeValue) => largeValue.enumUnit(1));
      } else {
        column.none();
      }
    }, definition.columns.length);
    table.map(definition.columns.filter((column) => column.references).length);
    for (const column of definition.columns) {
      if (column.references) {
        table.string(column.name);
        table.string(column.references);
      }
    }
    if (definition.readPolicy === "owner") {
      table.some((policy) => writeOwnerOnlyPolicy(policy, tableName));
    } else {
      table.none();
    }
    table.none();
    table.set(0);
    table.map(0);
  }, tables.length);
  writer.none();
  writer.none();
  return writer.finish();
}

function encodeCellsForRow(definition: TableDefinition, row: Record<string, unknown>): Uint8Array {
  return encodeCellsForColumns(definition.columns, (column) => valueForColumn(column, row));
}

function encodeCellsForPatch(
  definition: TableDefinition,
  patch: Record<string, unknown>,
): Uint8Array {
  const columns = definition.columns.filter((column) => Object.hasOwn(patch, column.name));
  return encodeCellsForColumns(columns, (column) => patch[column.name]);
}

function encodeCellsForColumns(
  columns: ColumnDefinition[],
  valueFor: (column: ColumnDefinition) => unknown,
): Uint8Array {
  const entries = [...columns].sort((left, right) => left.name.localeCompare(right.name));
  const descriptor = entries.map(columnDescriptor);
  return encodeCells(
    descriptor,
    entries.map((column) => encodeCell(column, valueFor(column))),
  );
}

function valueForColumn(column: ColumnDefinition, row: Record<string, unknown>): unknown {
  if (Object.hasOwn(row, column.name)) return row[column.name];
  if (Object.hasOwn(column, "default")) return column.default;
  return undefined;
}

function decodeRows(
  batches: AbiRowBatch[],
  schema?: SchemaDefinition,
): Array<Record<string, unknown>> {
  return batches.flatMap((batch) =>
    batch.rows.map((row) => {
      const decoded: Record<string, unknown> = { id: formatUuid(row.rowId) };
      for (let index = 0; index < batch.descriptor.length; index += 1) {
        const field = batch.descriptor[index];
        if (!field.name) continue;
        if (isInternalProjectionField(field.name)) continue;
        const valueType = descriptorValueType(batch.table, field, schema);
        decoded[publicFieldName(field.name)] = decodeCell(
          valueType,
          batch.descriptor,
          row.raw,
          index,
        );
      }
      Object.defineProperty(decoded, deletedRowMarker, {
        value: row.deleted,
        enumerable: false,
        configurable: false,
      });
      Object.defineProperty(decoded, "__jazz_table", {
        value: batch.table,
        enumerable: false,
        configurable: false,
      });
      return decoded;
    }),
  );
}

function applySubscriptionDeltaRows(
  previous: Array<Record<string, unknown>>,
  delta: AbiSubscriptionDelta,
  schema: SchemaDefinition,
): Array<Record<string, unknown>> {
  const byKey = new Map(previous.map((row) => [rowIdentityKey(row), row]));
  for (const removed of delta.removed) {
    byKey.delete(`${removed.table}:${formatUuid(removed.rowId)}`);
  }
  for (const row of decodeRows(delta.updated, schema)) {
    byKey.set(rowIdentityKey(row), row);
  }
  for (const row of decodeRows(delta.added, schema)) {
    byKey.set(rowIdentityKey(row), row);
  }
  return [...byKey.values()];
}

function rowIdentityKey(row: Record<string, unknown>): string {
  const table = typeof row["__jazz_table"] === "string" ? row["__jazz_table"] : "";
  return `${table}:${String(row.id)}`;
}

function descriptorValueType(
  table: string,
  field: DescriptorField,
  schema?: SchemaDefinition,
): ValueType {
  const fieldName = field.name ? publicFieldName(field.name) : undefined;
  const column =
    fieldName == null
      ? undefined
      : schema?.[table]?.columns.find((candidate) => candidate.name === fieldName);
  return column ? columnValueType(column) : field.valueType;
}

function readRowBatches(payload: Uint8Array): AbiRowBatch[] {
  return new PostcardReader(payload).readVec(readAbiRowBatchWithArrays);
}

function readSubscriptionDelta(payload: Uint8Array): AbiSubscriptionDelta {
  const reader = new PostcardReader(payload);
  return {
    added: reader.readVec(readAbiRowBatchWithArrays),
    updated: reader.readVec(readAbiRowBatchWithArrays),
    removed: reader.readVec((rowReader) => ({
      table: rowReader.string(),
      rowId: rowReader.bytes(),
    })),
  };
}

function readAbiRowBatchWithArrays(reader: PostcardReader): AbiRowBatch {
  return {
    table: reader.string(),
    descriptor: readDescriptorWithArrays(reader),
    rows: reader.readVec((rowReader) => ({
      rowId: rowReader.bytes(),
      deleted: rowReader.bool(),
      raw: rowReader.bytes(),
    })),
  };
}

function readDescriptorWithArrays(reader: PostcardReader): DescriptorField[] {
  return reader.readVec((fieldReader) => ({
    name: fieldReader.option((nameReader) => nameReader.string()),
    valueType: readValueTypeWithArrays(fieldReader),
  }));
}

function bytesKey(bytes: Uint8Array): string {
  return Buffer.from(bytes).toString("base64");
}

function readValueTypeWithArrays(reader: PostcardReader): ValueType {
  const tag = reader.u64();
  if (tag === 10) {
    const members = reader.readVec(readValueTypeWithArrays);
    return { tag, inner: members[0] };
  }
  if (tag === 11 || tag === 12) {
    return { tag, inner: readValueTypeWithArrays(reader) };
  }
  return { tag };
}

function publicFieldName(name: string): string {
  return name.startsWith("user_") ? name.slice("user_".length) : name;
}

function isInternalProjectionField(name: string): boolean {
  return name === "row_uuid" || name === "tx_node_id" || name === "tx_time";
}

function columnDescriptor(column: ColumnDefinition): DescriptorField {
  return { name: column.name, valueType: columnValueType(column) };
}

function encodeCells(descriptor: DescriptorField[], values: Uint8Array[]): Uint8Array {
  const writer = new PostcardWriter();
  writer.vec((field, index) => {
    field.some((name) => name.string(descriptor[index].name ?? ""));
    writeCompleteValueType(field, descriptor[index].valueType);
  }, descriptor.length);
  writer.bytes(createRecord(descriptor, values));
  return writer.finish();
}

function createRecord(descriptor: DescriptorField[], values: Uint8Array[]): Uint8Array {
  const staticChunks: Uint8Array[] = [];
  const variableChunks: Uint8Array[] = [];
  for (let index = 0; index < descriptor.length; index += 1) {
    if (fixedSize(descriptor[index].valueType) == null) {
      variableChunks.push(values[index]);
    } else {
      staticChunks.push(values[index]);
    }
  }
  const fixed = concatBytes(staticChunks);
  const offsets = new Uint8Array(Math.max(0, variableChunks.length - 1) * 4);
  const view = new DataView(offsets.buffer);
  let nextOffset = fixed.length + offsets.length;
  for (let index = 0; index < variableChunks.length - 1; index += 1) {
    nextOffset += variableChunks[index].length;
    view.setUint32(index * 4, nextOffset, true);
  }
  return concatBytes([fixed, offsets, ...variableChunks]);
}

function writeCompleteValueType(writer: PostcardWriter, valueType: ValueType): void {
  writer.enumUnit(valueType.tag);
  if ((valueType.tag === 10 || valueType.tag === 11 || valueType.tag === 12) && valueType.inner) {
    writeCompleteValueType(writer, valueType.inner);
  }
}

function writeOwnerOnlyPolicy(writer: PostcardWriter, tableName: string): void {
  writer.string(tableName);
  writer.vec((filter) => {
    filter.enumUnit(3);
    filter.enumUnit(0);
    filter.string("owner");
    filter.enumUnit(2);
    filter.string("sub");
  }, 1);
  writer.vec(() => undefined, 0);
  writer.vec(() => undefined, 0);
  writer.vec(() => undefined, 0);
  writer.none();
  writer.vec(() => undefined, 0);
  writer.none();
  writer.none();
  writer.u64(0);
}

function columnValueType(column: ColumnDefinition): ValueType {
  const valueType = valueTypeFromColumnType(column.column_type);
  return column.nullable ? { tag: 12, inner: valueType } : valueType;
}

function valueTypeFromColumnType(columnType: ColumnType): ValueType {
  const normalized = typeof columnType === "string" ? { type: columnType } : columnType;
  switch (normalized.type) {
    case "Boolean":
      return { tag: 5 };
    case "Integer":
      return { tag: 3 };
    case "Text":
      return { tag: 6 };
    case "Uuid":
      return { tag: 8 };
    case "Bytea":
      return { tag: 7 };
    case "Array":
      return { tag: 11, inner: valueTypeFromColumnType(normalized.element) };
  }
}

function encodeCell(column: ColumnDefinition, value: unknown): Uint8Array {
  if (value == null) {
    if (!column.nullable) throw new Error(`missing required column ${column.name}`);
    const innerType = valueTypeFromColumnType(column.column_type);
    const size = fixedSize(innerType);
    return size == null ? new Uint8Array([0]) : new Uint8Array(size + 1);
  }
  const encoded = encodeNonNullCell(column.column_type, value);
  if (!column.nullable) return encoded;
  const innerType = valueTypeFromColumnType(column.column_type);
  if (fixedSize(innerType) == null) {
    const out = new Uint8Array(encoded.length + 1);
    out[0] = 1;
    out.set(encoded, 1);
    return out;
  }
  const out = new Uint8Array(encoded.length + 1);
  out[0] = 1;
  out.set(encoded, 1);
  return out;
}

function encodeNonNullCell(columnType: ColumnType, value: unknown): Uint8Array {
  const normalized = typeof columnType === "string" ? { type: columnType } : columnType;
  switch (normalized.type) {
    case "Boolean":
      if (typeof value !== "boolean") throw new Error("expected boolean");
      return new Uint8Array([value ? 1 : 0]);
    case "Integer":
      if (typeof value !== "number" || !Number.isSafeInteger(value))
        throw new Error("expected safe integer");
      return u64Le(value);
    case "Text":
      if (typeof value !== "string") throw new Error("expected string");
      return utf8(value);
    case "Uuid":
      return encodeRowId(value);
    case "Bytea":
      if (!(value instanceof Uint8Array)) throw new Error("expected Uint8Array");
      return value;
    case "Array":
      return encodeArrayCell(normalized.element, value);
  }
}

function encodeArrayCell(elementType: ColumnType, value: unknown): Uint8Array {
  if (!Array.isArray(value)) throw new Error("expected array");
  const values = value.map((item) => encodeNonNullCell(elementType, item));
  const elementValueType = valueTypeFromColumnType(elementType);
  const elementSize = fixedSize(elementValueType);
  if (elementSize != null) return concatBytes(values);
  const offsets = new Uint8Array(Math.max(0, values.length - 1) * 4);
  const view = new DataView(offsets.buffer);
  let nextOffset = 4 + offsets.length;
  for (let index = 0; index < values.length - 1; index += 1) {
    nextOffset += values[index].length;
    view.setUint32(index * 4, nextOffset, true);
  }
  return concatBytes([u32Le(value.length), offsets, ...values]);
}

function encodeIdentity(identity: Identity): Uint8Array {
  return typeof identity === "string" ? parseUuid(identity) : encodeRowId(identity);
}

function trimOptional(value: unknown): string | undefined {
  if (typeof value !== "string") return undefined;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function authModeFromIssuer(issuer: string | undefined): AuthMode {
  if (issuer === LOCAL_FIRST_JWT_ISSUER) return "local-first";
  if (issuer === ANONYMOUS_JWT_ISSUER) return "anonymous";
  return "external";
}

function deterministicBytes(seed: string, length: number): Uint8Array {
  const bytes = new Uint8Array(length);
  let state = 0x811c9dc5;
  const input = new TextEncoder().encode(seed);
  for (let index = 0; index < length; index += 1) {
    for (const byte of input) {
      state ^= byte + index;
      state = Math.imul(state, 0x01000193) >>> 0;
    }
    state ^= length + index;
    state = Math.imul(state, 0x01000193) >>> 0;
    bytes[index] = state & 0xff;
  }
  return bytes;
}

function decodeCell(
  valueType: ValueType,
  descriptor: DescriptorField[],
  raw: Uint8Array,
  index: number,
): unknown {
  if (valueType.tag === 12) {
    const innerType = valueType.inner ?? { tag: 7 };
    const cell = decodeReturnedRecordCell(descriptor, raw, index);
    let bytes = cell.bytes;
    if (cell.present && fixedSize(innerType) != null && bytes.length === 0) {
      bytes = decodeCanonicalNullableFixedCell(descriptor, raw, index);
    }
    if (!cell.present && bytes.length === 0) return undefined;
    if (bytes.length === 0) return undefined;
    const innerSize = fixedSize(innerType);
    if (innerType.tag === 8 && bytes[0] === 1) {
      if (bytes.subarray(1).every((byte) => byte === 0)) return undefined;
      if (bytes[1] === 1) {
        const duplicateWrapped = decodeDuplicateWrappedNullableUuidCell(descriptor, raw, index);
        if (duplicateWrapped.length === innerSize) bytes = duplicateWrapped;
      } else {
        const canonical = decodeCanonicalNullableFixedCell(descriptor, raw, index);
        if (canonical.length === innerSize) bytes = canonical;
      }
    }
    if (innerSize != null && bytes.length === innerSize) return decodeBytes(innerType, bytes);
    if (bytes[0] === 1 && innerSize == null && innerType.tag !== 11) bytes = bytes.subarray(1);
    if (innerType.tag === 7 && bytes[0] === 1 && bytes[1] === 1) bytes = bytes.subarray(1);
    return decodeBytes(innerType, bytes);
  }
  let bytes = decodeReturnedRecordCell(descriptor, raw, index).bytes;
  if (descriptor[index].valueType.tag === 12) {
    if (valueType.tag === 11 && bytes.length === 0) return undefined;
    if (
      bytes[0] === 1 &&
      descriptor[index].valueType.inner &&
      fixedSize(descriptor[index].valueType.inner) == null
    ) {
      bytes = bytes.subarray(1);
    }
  }
  return decodeBytes(valueType, bytes, descriptor, raw, index);
}

function decodeBytes(
  valueType: ValueType,
  bytes: Uint8Array,
  descriptor?: DescriptorField[],
  raw?: Uint8Array,
  index?: number,
): unknown {
  switch (valueType.tag) {
    case 5:
      if (descriptor && raw && index != null) return decodeRecordBool(descriptor, raw, index);
      return bytes[0] !== 0;
    case 3:
      return new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getBigUint64(0, true);
    case 6:
      if (descriptor && raw && index != null) return decodeRecordString(descriptor, raw, index);
      return new TextDecoder().decode(bytes);
    case 8:
      if (bytes.length === 17 && bytes[0] === 1) return formatUuid(bytes.subarray(1));
      return formatUuid(bytes);
    case 7:
      return bytes;
    case 11:
      if (!valueType.inner) throw new Error("array value is missing element type");
      try {
        return decodeArrayCell(valueType.inner, bytes);
      } catch (error) {
        const field = descriptor && index != null ? descriptor[index] : undefined;
        const fieldName = field?.name ?? "<unknown>";
        const descriptorType = field ? JSON.stringify(field.valueType) : "<none>";
        throw new Error(
          `failed to decode array field ${fieldName} bytes=${bytes.length} descriptorType=${descriptorType}: ${String(error)}`,
        );
      }
    default:
      return bytes;
  }
}

function decodeArrayCell(elementType: ValueType, bytes: Uint8Array): unknown[] {
  const elementSize = fixedSize(elementType);
  if (elementSize != null) {
    if (bytes.length % elementSize !== 0) throw new Error("fixed array cell has invalid length");
    const values: unknown[] = [];
    for (let offset = 0; offset < bytes.length; offset += elementSize) {
      values.push(decodeBytes(elementType, bytes.subarray(offset, offset + elementSize)));
    }
    return values;
  }

  if (bytes.length < 4) throw new Error("array cell is missing element count");
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const count = view.getUint32(0, true);
  const offsetsStart = 4;
  const payloadStart = 4 + Math.max(0, count - 1) * 4;
  if (payloadStart > bytes.length) throw new Error("array cell offset table exceeds cell length");
  const values: unknown[] = [];
  for (let index = 0; index < count; index += 1) {
    const start = index === 0 ? payloadStart : view.getUint32(offsetsStart + (index - 1) * 4, true);
    const end = index + 1 < count ? view.getUint32(offsetsStart + index * 4, true) : bytes.length;
    if (end < start) throw new Error("array cell offsets are not monotonic");
    values.push(decodeBytes(elementType, bytes.subarray(start, end)));
  }
  return values;
}

function decodeReturnedRecordCell(
  descriptor: DescriptorField[],
  raw: Uint8Array,
  logicalIndex: number,
): { present: boolean; bytes: Uint8Array } {
  let fixedOffset = 0;
  const variables: { index: number; offsetIndex: number }[] = [];
  for (let index = 0; index < descriptor.length; index += 1) {
    const valueType = descriptor[index].valueType;
    const nullableInner = valueType.tag === 12 ? valueType.inner : undefined;
    const nullableInnerSize = nullableInner ? fixedSize(nullableInner) : undefined;
    if (nullableInner && nullableInnerSize == null) {
      variables.push({ index, offsetIndex: variables.length });
      continue;
    }
    if (nullableInner && nullableInnerSize != null) {
      const nullableSize = nullableInnerSize + 1;
      if (index === logicalIndex) {
        const bytes = raw.subarray(fixedOffset, fixedOffset + nullableSize);
        return { present: true, bytes };
      }
      fixedOffset += nullableSize;
      continue;
    }
    const size = fixedSize(valueType);
    if (size == null) {
      variables.push({ index, offsetIndex: variables.length });
    } else if (index === logicalIndex) {
      return { present: true, bytes: raw.subarray(fixedOffset, fixedOffset + size) };
    } else {
      fixedOffset += size;
    }
  }
  const target = variables.find((variable) => variable.index === logicalIndex);
  if (!target) throw new Error("field is not present");
  const value =
    variableRecordBytes(raw, variables.length, target.offsetIndex, fixedOffset) ?? new Uint8Array();
  const descriptorType = descriptor[logicalIndex].valueType;
  if (
    descriptorType.tag === 12 &&
    descriptorType.inner &&
    fixedSize(descriptorType.inner) == null &&
    value.length === 0
  ) {
    const fallback = decodeReturnedRecordCellWithFixedNullableVariablePresence(
      descriptor,
      raw,
      logicalIndex,
    );
    if (fallback.bytes.length > 0 || !fallback.present) return fallback;
  }
  const present = descriptorType.tag !== 12 || value.length > 0;
  return { present, bytes: value };
}

function decodeCanonicalNullableFixedCell(
  descriptor: DescriptorField[],
  raw: Uint8Array,
  logicalIndex: number,
): Uint8Array {
  let fixedOffset = 0;
  for (let index = 0; index < descriptor.length; index += 1) {
    const size = fixedSize(descriptor[index].valueType);
    if (size == null) continue;
    if (index === logicalIndex) {
      const bytes = raw.subarray(fixedOffset, fixedOffset + size);
      return bytes[0] === 1 ? bytes.subarray(1) : bytes;
    }
    fixedOffset += size;
  }
  return new Uint8Array();
}

function decodeDuplicateWrappedNullableUuidCell(
  descriptor: DescriptorField[],
  raw: Uint8Array,
  logicalIndex: number,
): Uint8Array {
  let fixedOffset = 0;
  for (let index = 0; index < descriptor.length; index += 1) {
    const size = fixedSize(descriptor[index].valueType);
    if (size == null) continue;
    if (index === logicalIndex) return raw.subarray(fixedOffset + 2, fixedOffset + 18);
    fixedOffset += size;
  }
  return new Uint8Array();
}

function decodeReturnedRecordCellWithFixedNullableVariablePresence(
  descriptor: DescriptorField[],
  raw: Uint8Array,
  logicalIndex: number,
): { present: boolean; bytes: Uint8Array } {
  let fixedOffset = 0;
  const variables: { index: number; offsetIndex: number }[] = [];
  const presenceOffsets = new Map<number, number>();
  for (let index = 0; index < descriptor.length; index += 1) {
    const valueType = descriptor[index].valueType;
    const nullableInner = valueType.tag === 12 ? valueType.inner : undefined;
    const nullableInnerSize = nullableInner ? fixedSize(nullableInner) : undefined;
    if (nullableInner && nullableInnerSize == null) {
      presenceOffsets.set(index, fixedOffset);
      fixedOffset += 1;
      variables.push({ index, offsetIndex: variables.length });
      continue;
    }
    const size = nullableInnerSize ?? fixedSize(valueType);
    if (size == null) {
      variables.push({ index, offsetIndex: variables.length });
    } else {
      fixedOffset += size;
    }
  }
  const target = variables.find((variable) => variable.index === logicalIndex);
  if (!target) throw new Error("field is not present");
  const value =
    variableRecordBytes(raw, variables.length, target.offsetIndex, fixedOffset) ?? new Uint8Array();
  const presenceOffset = presenceOffsets.get(logicalIndex);
  const present = presenceOffset == null || raw[presenceOffset] !== 0;
  return { present, bytes: value };
}

function variableRecordBytes(
  raw: Uint8Array,
  variableCount: number,
  offsetIndex: number,
  offsetTableStart: number,
): Uint8Array | undefined {
  if (variableCount === 0) return undefined;
  const variableStart = offsetTableStart + Math.max(0, variableCount - 1) * 4;
  if (variableStart > raw.length) return undefined;
  const start =
    offsetIndex === 0 ? variableStart : readU32Le(raw, offsetTableStart + (offsetIndex - 1) * 4);
  const end =
    offsetIndex === variableCount - 1
      ? raw.length
      : readU32Le(raw, offsetTableStart + offsetIndex * 4);
  if (start < variableStart || end < start || end > raw.length) return undefined;
  return raw.subarray(start, end);
}

function readU32Le(bytes: Uint8Array, offset: number): number {
  if (offset < 0 || offset + 4 > bytes.length) throw new Error("record offset table is truncated");
  return new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getUint32(offset, true);
}

function encodeBuiltQuery(built: string, schema?: SchemaDefinition): Uint8Array {
  const query = JSON.parse(built) as BuiltQuery;
  const table = queryTableName(query);
  const filters =
    query.filters ??
    (query.filter ? [query.filter] : filtersFromConditions(schema, table, query.conditions ?? []));
  const includes = encodedForwardIncludes(query, schema, table);
  if (
    filters.length === 0 &&
    includes.length === 0 &&
    query.select == null &&
    query.orderBy == null &&
    query.limit == null &&
    query.offset == null
  ) {
    return queryFromTable(table);
  }
  return encodeQuery(
    table,
    filters,
    includes,
    query.select,
    query.orderBy,
    query.limit,
    query.offset,
  );
}

function encodeQuery(
  table: string,
  filters: QueryFilter[],
  includes: EncodedInclude[],
  select: unknown,
  orderBy: unknown,
  limit: unknown,
  offset: unknown,
): Uint8Array {
  let selectedColumns: string[] | undefined;
  if (select != null) {
    if (!isStringArray(select)) throw new Error("query select must be an array of column names");
    selectedColumns = [...select];
  }
  let orderByItems: QueryOrderBy[] = [];
  if (orderBy != null) {
    if (!Array.isArray(orderBy) || !orderBy.every(isQueryOrderBy))
      throw new Error("query orderBy must be an array of order items");
    orderByItems = orderBy;
  }
  let limitCount: number | undefined;
  if (limit != null) {
    if (typeof limit !== "number" || !Number.isSafeInteger(limit) || limit < 0) {
      throw new Error("query limit must be a non-negative safe integer");
    }
    limitCount = limit;
  }
  const offsetCount = validateCount(offset, "offset");
  const writer = new PostcardWriter();
  writer.string(table);
  writer.vec((filter, index) => writeFilter(filter, filters[index]), filters.length);
  writer.vec(() => undefined, 0);
  writer.vec(() => undefined, 0);
  writer.vec((include, index) => writeInclude(include, includes[index]), includes.length);
  if (selectedColumns == null) {
    writer.none();
  } else {
    writer.some((selected) =>
      selected.vec(
        (column, index) => column.string(selectedColumns[index]),
        selectedColumns.length,
      ),
    );
  }
  writer.vec((order, index) => writeOrderBy(order, orderByItems[index]), orderByItems.length);
  writer.none();
  if (limitCount == null) {
    writer.none();
  } else {
    writer.some((valueWriter) => valueWriter.u64(limitCount));
  }
  writer.u64(offsetCount);
  return writer.finish();
}

type EncodedInclude = {
  path: string;
  required: boolean;
};

export function encodeBuiltQueryForTest(built: string, schema?: SchemaDefinition): Uint8Array {
  return encodeBuiltQuery(built, schema);
}

export function assertSubscribeQuerySupportedForTest<Row>(
  tableOrQuery: Table<Row & { id: string | Uint8Array }, unknown> | QueryBuilder<Row>,
): void {
  assertSubscribeQuerySupported(tableOrQuery);
}

function encodedForwardIncludes(
  query: BuiltQuery,
  schema: SchemaDefinition | undefined,
  table: string,
): EncodedInclude[] {
  if (query.includes == null || Object.keys(query.includes).length === 0) return [];
  if (!schema) throw new Error("query includes require a schema");
  validateAlphaIncludes(schema, table, query.includes);
  if (!encodedForwardIncludesCanRepresent(query.includes, schema, table)) return [];
  return encodedForwardIncludeEntries(query.includes, schema, table, "");
}

function encodedForwardIncludesCanRepresent(
  includes: QueryInclude,
  schema: SchemaDefinition,
  table: string,
): boolean {
  for (const [property, includeValue] of Object.entries(includes)) {
    if (includeValue === undefined) continue;
    const relation = includeRelation(schema, table, property);
    const include = normalizeIncludeOptions(includeValue);
    if (relation.direction !== "forward" || include.select != null) return false;
    if (
      include.include != null &&
      !encodedForwardIncludesCanRepresent(include.include, schema, relation.table)
    ) {
      return false;
    }
  }
  return true;
}

function encodedForwardIncludeEntries(
  includes: QueryInclude,
  schema: SchemaDefinition,
  table: string,
  prefix: string,
): EncodedInclude[] {
  return Object.entries(includes).flatMap(([property, include]) => {
    if (include === undefined) return [];
    const relation = includeRelation(schema, table, property);
    const normalized = normalizeIncludeOptions(include);
    const path = prefix ? `${prefix}.${relation.column}` : relation.column;
    return [
      { path, required: normalized.required === true },
      ...encodedForwardIncludeEntries(normalized.include ?? {}, schema, relation.table, path),
    ];
  });
}

function writeInclude(writer: PostcardWriter, include: EncodedInclude): void {
  writer.string(include.path);
  writer.enumUnit(0);
  writer.bool(include.required);
}

function writeOrderBy(writer: PostcardWriter, orderBy: QueryOrderBy): void {
  writer.string(orderBy.column);
  writer.enumUnit(orderBy.direction === "asc" ? 0 : 1);
}

function writeFilter(writer: PostcardWriter, filter: QueryFilter): void {
  switch (filter.op) {
    case "eq":
    case "ne":
      if (filter.value === null) {
        writeFilter(writer, {
          column: filter.column,
          columnType: filter.columnType,
          op: filter.op === "eq" ? "isNull" : "isNotNull",
        });
        return;
      }
      writer.u64(filter.op === "eq" ? 3 : 4);
      writer.u64(0);
      writer.string(filter.column);
      writer.u64(3);
      writeLiteral(writer, filter.columnType, filter.value, filter.nullable);
      return;
    case "in":
      if (filter.values.includes(null)) {
        const nonNullValues = filter.values.filter((value) => value !== null);
        if (nonNullValues.length === 0) {
          writeFilter(writer, {
            column: filter.column,
            columnType: filter.columnType,
            op: "isNull",
          });
          return;
        }
        writeFilter(writer, {
          op: "any",
          filters: [
            { column: filter.column, columnType: filter.columnType, op: "isNull" },
            { ...filter, values: nonNullValues },
          ],
        });
        return;
      }
      writeFilter(writer, {
        op: "any",
        filters: filter.values.map((value) => ({
          column: filter.column,
          columnType: filter.columnType,
          nullable: filter.nullable,
          op: "eq",
          value,
        })),
      });
      return;
    case "gt":
    case "gte":
    case "lt":
    case "lte":
      writer.u64({ gt: 6, gte: 7, lt: 8, lte: 9 }[filter.op]);
      writer.u64(0);
      writer.string(filter.column);
      writer.u64(3);
      writeLiteral(writer, filter.columnType, filter.value, filter.nullable);
      return;
    case "contains":
      writer.u64(10);
      writer.u64(0);
      writer.string(filter.column);
      writer.u64(3);
      writeLiteral(writer, containsLiteralType(filter.columnType), filter.value, false);
      return;
    case "isNull":
      writer.u64(11);
      writer.u64(0);
      writer.string(filter.column);
      return;
    case "isNotNull":
      writer.u64(2);
      writer.u64(11);
      writer.u64(0);
      writer.string(filter.column);
      return;
    case "any":
      writer.u64(1);
      writer.vec(
        (child, index) => writeFilter(child, filter.filters[index]),
        filter.filters.length,
      );
      return;
  }
}

function writeLiteral(
  writer: PostcardWriter,
  columnType: ColumnType,
  value: QueryLiteral,
  nullable = false,
): void {
  if (nullable) {
    writer.u64(12);
    writer.some((inner) => writeLiteral(inner, columnType, value, false));
    return;
  }
  if (value == null) throw new Error("null query value must target a nullable column");
  const normalized = typeof columnType === "string" ? { type: columnType } : columnType;
  if (normalized.type === "Array") {
    if (!Array.isArray(value)) throw new Error("array query value must target an Array column");
    writer.u64(11);
    writer.vec(
      (element, index) => writeLiteral(element, normalized.element, value[index], false),
      value.length,
    );
    return;
  }
  if (typeof value === "boolean") {
    if (normalized.type !== "Boolean")
      throw new Error("boolean query value must target a Boolean column");
    writer.u64(5);
    writer.bool(value);
    return;
  }
  if (typeof value === "string") {
    if (normalized.type === "Uuid") {
      writer.u64(8);
      writer.bytes(parseUuid(value));
      return;
    }
    if (normalized.type !== "Text")
      throw new Error("string query value must target a Text or Uuid column");
    writer.u64(6);
    writer.string(value);
    return;
  }
  if (typeof value === "number") {
    if (normalized.type !== "Integer")
      throw new Error("number query value must target an Integer column");
    if (!Number.isSafeInteger(value) || value < 0)
      throw new Error("expected non-negative safe integer query value");
    writer.u64(3);
    writer.u64(value);
    return;
  }
  if ("bytes" in value) {
    const bytes = Uint8Array.from(value.bytes);
    if (normalized.type === "Uuid") {
      if (bytes.length !== 16) throw new Error("uuid query bytes must be 16 bytes");
      writer.u64(8);
      writer.bytes(bytes);
      return;
    }
    if (normalized.type !== "Bytea")
      throw new Error("bytes query value must target a Bytea or Uuid column");
    writer.u64(7);
    writer.bytes(bytes);
    return;
  }
  throw new Error("unsupported query literal");
}

function containsLiteralType(columnType: ColumnType): ColumnType {
  const normalized = typeof columnType === "string" ? { type: columnType } : columnType;
  return normalized.type === "Array" ? normalized.element : columnType;
}

function inFilter(
  column: string,
  columnType: ColumnType,
  nullable: boolean,
  values: readonly QueryValue[],
): QueryFilter {
  const normalized = typeof columnType === "string" ? { type: columnType } : columnType;
  if (normalized.type !== "Array") {
    return { column, columnType, nullable, op: "in", values: values.map(encodeQueryLiteral) };
  }

  const nonNullValues = values.filter((value) => value !== null);
  if (nonNullValues.every((value) => !Array.isArray(value))) {
    return {
      op: "any",
      filters: [
        ...(values.some((value) => value === null)
          ? [{ column, columnType, op: "isNull" } satisfies QueryFilter]
          : []),
        ...nonNullValues.map((value) => ({
          column,
          columnType,
          op: "contains" as const,
          value: encodeQueryLiteral(value),
        })),
      ],
    };
  }

  if (nonNullValues.some((value) => !Array.isArray(value))) {
    throw new Error("array column in values must be all scalar elements or all whole arrays");
  }
  return { column, columnType, nullable, op: "in", values: values.map(encodeQueryLiteral) };
}

function builtQueryWithRelations<Row>(
  tableOrQuery: Table<Row & { id: string | Uint8Array }, unknown> | QueryBuilder<Row>,
): BuiltQuery | undefined {
  if (!("_build" in tableOrQuery)) return undefined;
  const query = JSON.parse(tableOrQuery._build()) as BuiltQuery;
  const hasIncludes = query.includes && Object.keys(query.includes).length > 0;
  const hasHops = query.hops && query.hops.length > 0;
  return hasIncludes || hasHops || query.gather ? query : undefined;
}

function subscribeRelationQuery<Row>(
  tableOrQuery: Table<Row & { id: string | Uint8Array }, unknown> | QueryBuilder<Row>,
  schema: SchemaDefinition,
): BuiltQuery | undefined {
  const relationQuery = builtQueryWithRelations(tableOrQuery);
  if (!relationQuery) return undefined;
  const table = queryTableName(relationQuery);
  if (relationQuery.includes && Object.keys(relationQuery.includes).length > 0) {
    validateAlphaIncludes(schema, table, relationQuery.includes);
  }
  if (relationQuery.hops?.length || relationQuery.gather) {
    throw new Error(
      "current jazz-tools/WasmDb subscribe with relation hops or gather is not supported yet; it requires Rust prepared query/relation subscription support",
    );
  }
  return relationQuery;
}

function assertSubscribeQuerySupported<Row>(
  tableOrQuery: Table<Row & { id: string | Uint8Array }, unknown> | QueryBuilder<Row>,
): void {
  subscribeRelationQuery(tableOrQuery, "_schema" in tableOrQuery ? tableOrQuery._schema : {});
}

function validateAlphaIncludes(
  schema: SchemaDefinition,
  table: string,
  includes: QueryInclude,
): void {
  for (const [property, includeValue] of Object.entries(includes)) {
    if (includeValue === undefined) continue;
    const relation = includeRelation(schema, table, property);
    const include = normalizeIncludeOptions(includeValue);
    validateIncludeSpec(schema, relation.table, include);
    if (include.include != null) {
      validateAlphaIncludes(schema, relation.table, include.include);
    }
  }
}

function queryTableName(query: BuiltQuery): string {
  if (typeof query.table !== "string") throw new Error("query builder must include a table");
  return query.table;
}

function filtersFromConditions(
  schema: SchemaDefinition | undefined,
  table: string,
  conditions: Array<{ column: string; op: string; value?: unknown }>,
): QueryFilter[] {
  return conditions.map((condition) => {
    const column =
      condition.column === "id"
        ? implicitIdColumn()
        : schema?.[table]?.columns.find((candidate) => candidate.name === condition.column);
    if (!column) throw new Error(`unknown column ${table}.${condition.column}`);
    const nullable = column.nullable === true;
    const columnType = column.column_type;
    switch (condition.op) {
      case "eq":
      case "ne":
        if (condition.value === null && !nullable)
          throw new Error("null query value must target a nullable column");
        return {
          column: condition.column,
          columnType,
          nullable,
          op: condition.op,
          value: encodeQueryLiteral(condition.value as QueryValue),
        };
      case "in":
        if (!isQueryValueArray(condition.value as QueryValue | readonly QueryValue[] | undefined))
          throw new Error("in requires an array of query values");
        {
          const values = condition.value as readonly QueryValue[];
          if (values.some((item) => item === null) && !nullable)
            throw new Error("null query value must target a nullable column");
          return inFilter(condition.column, columnType, nullable, values);
        }
      case "gt":
      case "gte":
      case "lt":
      case "lte":
        return {
          column: condition.column,
          columnType,
          nullable,
          op: condition.op,
          value: encodeRangeQueryLiteral(condition.value as string | number | bigint),
        };
      case "contains":
        if (typeof condition.value !== "string")
          throw new Error("contains is currently wired for string values only");
        return { column: condition.column, columnType, op: "contains", value: condition.value };
      case "isNull":
      case "isNotNull":
        return { column: condition.column, columnType, op: condition.op };
      default:
        throw new Error(`unsupported query operator ${condition.op}`);
    }
  });
}

function relationQuery(table: string, column: string, value: unknown): BuiltQuery {
  return { table, conditions: [{ column, op: "eq", value }] };
}

function stripIdFilters(query: BuiltQuery): BuiltQuery {
  return {
    ...query,
    filters: query.filters?.filter((filter) => filter.op === "any" || filter.column !== "id"),
    filter:
      query.filter && query.filter.op !== "any" && query.filter.column === "id"
        ? undefined
        : query.filter,
    conditions: query.conditions?.filter((condition) => condition.column !== "id"),
  };
}

function filterRowsById(
  rows: Array<Record<string, unknown>>,
  query: BuiltQuery,
): Array<Record<string, unknown>> {
  const idFilters = [
    ...(query.filter && query.filter.op !== "any" && query.filter.column === "id"
      ? [query.filter]
      : []),
    ...(query.filters?.filter((filter) => filter.op !== "any" && filter.column === "id") ?? []),
    ...(query.conditions?.filter((condition) => condition.column === "id") ?? []),
  ];
  return idFilters.reduce(
    (currentRows, filter) => currentRows.filter((row) => matchesIdFilter(String(row.id), filter)),
    rows,
  );
}

function queryNeedsJsPredicateFallback(query: BuiltQuery): boolean {
  return (
    (query.filters ?? []).some(
      (filter) =>
        filter.op === "in" ||
        filter.op === "any" ||
        filter.op === "contains" ||
        filter.op === "isNull" ||
        filter.op === "isNotNull" ||
        ((filter.op === "eq" || filter.op === "ne") &&
          (filter.value == null || queryLiteralNeedsJsPredicateFallback(filter.value))),
    ) ||
    (query.conditions ?? []).some(
      (condition) =>
        condition.op === "in" ||
        condition.op === "contains" ||
        condition.op === "isNull" ||
        condition.op === "isNotNull" ||
        ((condition.op === "eq" || condition.op === "ne") &&
          (condition.value === null || queryValueNeedsJsPredicateFallback(condition.value))),
    )
  );
}

function queryLiteralNeedsJsPredicateFallback(value: QueryLiteral): boolean {
  return Array.isArray(value) || (typeof value === "object" && value !== null && "bytes" in value);
}

function queryValueNeedsJsPredicateFallback(value: unknown): boolean {
  return Array.isArray(value) || value instanceof Uint8Array;
}

function applyBuiltQueryFallback(
  rows: Array<Record<string, unknown>>,
  query: BuiltQuery,
  schema?: SchemaDefinition,
): Array<Record<string, unknown>> {
  const filters =
    query.filters ?? filtersFromConditions(schema, queryTableName(query), query.conditions ?? []);
  return filters.reduce(
    (currentRows, filter) => currentRows.filter((row) => matchesQueryFilter(row, filter)),
    rows,
  );
}

function matchesQueryFilter(row: Record<string, unknown>, filter: QueryFilter): boolean {
  switch (filter.op) {
    case "eq":
      return sameQueryValue(row[filter.column], filter.value);
    case "ne":
      return !sameQueryValue(row[filter.column], filter.value);
    case "in":
      return filter.values.some((value) => sameQueryValue(row[filter.column], value));
    case "any":
      return filter.filters.some((child) => matchesQueryFilter(row, child));
    case "contains": {
      const value = row[filter.column];
      return Array.isArray(value)
        ? value.some((item: unknown) => sameQueryValue(item, filter.value))
        : typeof value === "string" &&
            typeof filter.value === "string" &&
            value.includes(filter.value);
    }
    case "isNull":
      return row[filter.column] == null;
    case "isNotNull":
      return row[filter.column] != null;
    default:
      return false;
  }
}

function sameQueryValue(left: unknown, right: unknown): boolean {
  if (left == null && right == null) return true;
  if (Array.isArray(left) || Array.isArray(right)) {
    return (
      Array.isArray(left) &&
      Array.isArray(right) &&
      left.length === right.length &&
      left.every((item, index) => sameQueryValue(item, right[index]))
    );
  }
  if (left instanceof Uint8Array && right instanceof Uint8Array)
    return sameNullableBytes(left, right);
  if (left instanceof Uint8Array && right && typeof right === "object" && "bytes" in right) {
    return sameNullableBytes(left, Uint8Array.from(right.bytes as number[]));
  }
  if (
    left instanceof Uint8Array ||
    right instanceof Uint8Array ||
    (right && typeof right === "object" && "bytes" in right)
  ) {
    return sameQueryId(String(left), right);
  }
  if (
    (typeof left === "number" || typeof left === "bigint") &&
    (typeof right === "number" || typeof right === "bigint")
  ) {
    const leftBigInt = BigInt(left);
    const rightBigInt = BigInt(right);
    return leftBigInt === rightBigInt || leftBigInt === rightBigInt * 256n + 1n;
  }
  return left === right;
}

function sameNullableBytes(left: Uint8Array, right: Uint8Array): boolean {
  return sameBytes(left, right) || (left[0] === 1 && sameBytes(left.subarray(1), right));
}

function matchesIdFilter(
  rowId: string,
  filter: QueryFilter | { column: string; op: string; value?: unknown },
): boolean {
  if (filter.op === "eq") return sameQueryId(rowId, "value" in filter ? filter.value : undefined);
  if (filter.op === "ne") return !sameQueryId(rowId, "value" in filter ? filter.value : undefined);
  if (filter.op === "in") {
    const values = "values" in filter ? filter.values : filter.value;
    return Array.isArray(values) && values.some((value) => sameQueryId(rowId, value));
  }
  throw new Error(`unsupported id query operator ${filter.op}`);
}

function sameQueryId(rowId: string, value: unknown): boolean {
  if (value instanceof Uint8Array)
    return rowId === formatUuid(value.length === 17 && value[0] === 1 ? value.subarray(1) : value);
  if (value && typeof value === "object" && "bytes" in value && Array.isArray(value.bytes)) {
    return rowId === formatUuid(new Uint8Array(value.bytes));
  }
  return rowId === value;
}

function relationValues(value: unknown): unknown[] {
  if (value == null) return [];
  return Array.isArray(value) ? value : [value];
}

function uniqueRows(rows: Array<Record<string, unknown>>): Array<Record<string, unknown>> {
  const seen = new Set<string>();
  return rows.filter((row) => {
    const key = String(row.id);
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function applyRelationSubscriptionIncludes(
  table: string,
  rows: Array<Record<string, unknown>>,
  includes: QueryInclude,
  snapshot: AbiRelationSubscriptionSnapshot,
  schema: SchemaDefinition,
  decodedSnapshotRows?: Array<Record<string, unknown>>,
  resolveIncludedRow?: (table: string, rowId: Uint8Array) => Record<string, unknown> | null,
): Array<Record<string, unknown>> {
  const includedRowsByKey = rowMapByTableAndId(
    decodedSnapshotRows ?? decodeRows(snapshot.rows, schema),
  );
  return rows.flatMap((row) => {
    const expanded = { ...row };
    for (const [includeName, includeValue] of Object.entries(includes)) {
      if (includeValue === undefined) continue;
      const relation = includeRelation(schema, table, includeName);
      const include = normalizeIncludeOptions(includeValue);
      if (relation.direction !== "forward") {
        throw new Error(
          "current jazz-tools/WasmDb subscribe with relation includes only supports forward includes",
        );
      }
      if (include.select != null) {
        throw new Error(
          "current jazz-tools/WasmDb subscribe with relation includes does not support selected projections yet",
        );
      }
      const edge = snapshot.edges.find(
        (candidate) =>
          candidate.sourceTable === table &&
          candidate.relation === relation.column &&
          sameBytes(candidate.sourceRowId, encodeRowId(row.id)),
      );
      const targetId = row[relation.column];
      const rowPayloadTargets =
        decodedSnapshotRows?.filter((candidate) => candidate["__jazz_table"] === relation.table) ??
        [];
      const target =
        edge == null
          ? targetId == null
            ? null
            : rowPayloadTargets.length === 1
              ? rowPayloadTargets[0]
              : (resolveIncludedRow?.(relation.table, encodeRowId(targetId)) ?? null)
          : (includedRowsByKey.get(rowKey(edge.targetTable, edge.targetRowId)) ??
            resolveIncludedRow?.(edge.targetTable, edge.targetRowId) ??
            null);
      if (include.required && target == null) return [];
      expanded[includeName] =
        target == null
          ? null
          : applyRelationSubscriptionIncludes(
              relation.table,
              [target],
              include.include ?? {},
              snapshot,
              schema,
              decodedSnapshotRows,
              resolveIncludedRow,
            )[0];
    }
    return [expanded];
  });
}

function relationSubscriptionSnapshotFromRowPayload(
  table: string,
  rows: Array<Record<string, unknown>>,
  includes: QueryInclude,
  schema: SchemaDefinition,
): AbiRelationSubscriptionSnapshot {
  const edges: AbiRelationSubscriptionSnapshot["edges"] = [];
  const includedRows = rows.filter((row) => row["__jazz_table"] !== table);
  const includedByKey = rowMapByTableAndId(includedRows);
  for (const row of rows.filter((candidate) => candidate["__jazz_table"] === table)) {
    collectRelationSubscriptionEdges(
      row,
      table,
      includes,
      schema,
      includedRows,
      includedByKey,
      edges,
    );
  }
  return { cursor: 0, rows: [], edges };
}

function collectRelationSubscriptionEdges(
  row: Record<string, unknown>,
  table: string,
  includes: QueryInclude,
  schema: SchemaDefinition,
  includedRows: Array<Record<string, unknown>>,
  includedByKey: Map<string, Record<string, unknown>>,
  edges: AbiRelationSubscriptionSnapshot["edges"],
): void {
  for (const [includeName, includeValue] of Object.entries(includes)) {
    if (includeValue === undefined) continue;
    const relation = includeRelation(schema, table, includeName);
    if (relation.direction !== "forward") continue;
    const include = normalizeIncludeOptions(includeValue);
    const targetId = row[relation.column];
    const targetRows = includedRows.filter(
      (candidate) => candidate["__jazz_table"] === relation.table,
    );
    const targetRowId =
      targetId == null && targetRows.length === 1
        ? encodeRowId(targetRows[0].id)
        : targetId == null
          ? undefined
          : encodeRowId(targetId);
    if (targetRowId == null) continue;
    const target = includedByKey.get(rowKey(relation.table, targetRowId));
    if (!target) continue;
    edges.push({
      sourceTable: table,
      sourceRowId: encodeRowId(row.id),
      relation: relation.column,
      targetTable: relation.table,
      targetRowId,
    });
    collectRelationSubscriptionEdges(
      target,
      relation.table,
      include.include ?? {},
      schema,
      includedRows,
      includedByKey,
      edges,
    );
  }
}

function rowMapByTableAndId(
  rows: Array<Record<string, unknown>>,
): Map<string, Record<string, unknown>> {
  const byKey = new Map<string, Record<string, unknown>>();
  for (const row of rows) {
    const table = typeof row.__jazz_table === "string" ? row.__jazz_table : undefined;
    if (table) byKey.set(rowKey(table, encodeRowId(row.id)), row);
  }
  return byKey;
}

function rowKey(table: string, rowId: Uint8Array): string {
  return `${table}:${formatUuid(rowId)}`;
}

function forwardRelation(
  schema: SchemaDefinition,
  table: string,
  name: string,
): RelationDefinition {
  const columns = schema[table]?.columns ?? [];
  const candidates = columns.filter(
    (column) => column.references && relationName(column.name) === name,
  );
  if (candidates.length !== 1 || !candidates[0].references) {
    throw new Error(
      `unknown or ambiguous alpha hop ${table}.${name}; add a referenced column such as ${name}_id`,
    );
  }
  return { table: candidates[0].references, column: candidates[0].name };
}

type IncludeRelation = RelationDefinition & { direction: "forward" | "reverse" };

function includeRelation(schema: SchemaDefinition, table: string, name: string): IncludeRelation {
  const forward = maybeForwardRelation(schema, table, name);
  const reverse = maybeReverseRelation(schema, table, name);
  if (forward && reverse) {
    throw new Error(
      `ambiguous alpha include ${table}.${name}; both forward and reverse relations match`,
    );
  }
  if (forward) return { ...forward, direction: "forward" };
  if (reverse) return { ...reverse, direction: "reverse" };
  throw new Error(
    `unknown alpha include ${table}.${name}; add a referenced column such as ${name}_id or schema.table(..., { relations: { ${name}: { table, column } } })`,
  );
}

function maybeForwardRelation(
  schema: SchemaDefinition,
  table: string,
  name: string,
): RelationDefinition | undefined {
  const columns = schema[table]?.columns ?? [];
  const candidates = columns.filter(
    (column) => column.references && relationName(column.name) === name,
  );
  if (candidates.length === 0) return undefined;
  if (candidates.length > 1 || !candidates[0].references) {
    throw new Error(
      `unknown or ambiguous alpha include ${table}.${name}; add a referenced column such as ${name}_id`,
    );
  }
  return { table: candidates[0].references, column: candidates[0].name };
}

function maybeReverseRelation(
  schema: SchemaDefinition,
  table: string,
  name: string,
): RelationDefinition | undefined {
  const explicit = schema[table]?.relations?.[name];
  if (explicit) return explicit;
  const matches = Object.entries(schema).flatMap(([candidateTable, definition]) =>
    definition.columns
      .filter((column) => column.references === table && relationName(candidateTable) === name)
      .map((column) => ({ table: candidateTable, column: column.name })),
  );
  if (matches.length === 0) return undefined;
  if (matches.length !== 1) {
    throw new Error(
      `unknown or ambiguous alpha include ${table}.${name}; add schema.table(..., { relations: { ${name}: { table, column } } })`,
    );
  }
  return matches[0];
}

function normalizeIncludeOptions(include: boolean | IncludeOptions | undefined): IncludeOptions {
  if (include === true || include === undefined) return {};
  if (include === false)
    throw new Error("object include values must be true, an include options object, or undefined");
  return {
    required: include.required === true,
    include: include.include,
    select: include.select,
  };
}

function validateIncludeSpec(
  schema: SchemaDefinition,
  table: string,
  include: IncludeOptions,
): void {
  if (include.select !== undefined) {
    if (!isStringArray(include.select))
      throw new Error("include select must be an array of column names");
    for (const column of include.select) {
      if (column === "id") continue;
      if (!schema[table]?.columns.some((candidate) => candidate.name === column)) {
        throw new Error(`unknown column ${table}.${column}`);
      }
    }
  }
  for (const [property, nested] of Object.entries(include.include ?? {})) {
    if (nested === undefined) continue;
    const relation = includeRelation(schema, table, property);
    validateIncludeSpec(schema, relation.table, normalizeIncludeOptions(nested));
  }
}

function projectIncludedRow(
  row: Record<string, unknown>,
  select: readonly string[] | undefined,
  includes: QueryIncludeMap | undefined,
): Record<string, unknown> {
  if (select === undefined) return row;
  const projected: Record<string, unknown> = { id: row.id };
  for (const column of select) projected[column] = row[column];
  for (const property of Object.keys(includes ?? {})) projected[property] = row[property];
  return projected;
}

function relationName(columnOrTable: string): string {
  return columnOrTable.replace(/_id$/, "").replace(/s$/, "");
}

function implicitIdColumn(): ColumnDefinition {
  return { name: "id", column_type: "Uuid" };
}

function makeTableHandle<Row extends { id: string | Uint8Array }, Init = Omit<Row, "id">>(
  table: string,
  schema: SchemaDefinition,
): Table<Row, Init> {
  return new CoreAbiQueryBuilder<Row, Init>(table, schema);
}

class CoreAbiQueryBuilder<Row, Init = Omit<Row, "id">> implements QueryBuilder<Row> {
  readonly _rowType = {} as Row;
  readonly _initType = {} as Init;

  constructor(
    readonly _table: string,
    readonly _schema: SchemaDefinition,
    private readonly filters: QueryFilter[] = [],
    private readonly selectedColumns?: string[],
    private readonly orderByItems: QueryOrderBy[] = [],
    private readonly limitCount?: number,
    private readonly offsetCount = 0,
    private readonly includes?: QueryInclude,
    private readonly hops: string[] = [],
    private readonly gatherOptions?: GatherOptions,
  ) {}

  where(conditions: QueryWhere<Row>): QueryBuilder<Row>;
  where(column: keyof Row & string, op: "eq" | "ne", value: QueryValue): QueryBuilder<Row>;
  where(column: keyof Row & string, op: "in", values: readonly QueryValue[]): QueryBuilder<Row>;
  where(
    column: keyof Row & string,
    op: "gt" | "gte" | "lt" | "lte",
    value: string | number | bigint,
  ): QueryBuilder<Row>;
  where(column: keyof Row & string, op: "contains", value: string): QueryBuilder<Row>;
  where(column: keyof Row & string, op: "isNull" | "isNotNull"): QueryBuilder<Row>;
  where(
    columnOrConditions: (keyof Row & string) | QueryWhere<Row>,
    op?: QueryFilter["op"],
    value?: QueryValue | readonly QueryValue[],
  ): QueryBuilder<Row> {
    if (typeof columnOrConditions !== "string") return this.#whereObject(columnOrConditions);
    if (op == null) throw new Error("where operator is required");
    const column = columnOrConditions;
    const columnDefinition = this.#columnDefinition(column);
    const columnType = columnDefinition.column_type;
    const nullable = columnDefinition.nullable === true;
    if (op === "eq" || op === "ne") {
      if (value === undefined || (Array.isArray(value) && !isArrayColumn(columnType)))
        throw new Error(`${op} requires a scalar query value`);
      if (value === null && !nullable)
        throw new Error("null query value must target a nullable column");
      return new CoreAbiQueryBuilder<Row>(
        this._table,
        this._schema,
        [
          ...this.filters,
          { column, columnType, nullable, op, value: encodeQueryLiteral(value as QueryValue) },
        ],
        this.selectedColumns,
        this.orderByItems,
        this.limitCount,
        this.offsetCount,
        this.includes,
        this.hops,
        this.gatherOptions,
      );
    }
    if (op === "in") {
      if (!isQueryValueArray(value)) throw new Error("in requires an array of query values");
      if (value.some((item) => item === null) && !nullable)
        throw new Error("null query value must target a nullable column");
      return new CoreAbiQueryBuilder<Row>(
        this._table,
        this._schema,
        [...this.filters, inFilter(column, columnType, nullable, value)],
        this.selectedColumns,
        this.orderByItems,
        this.limitCount,
        this.offsetCount,
        this.includes,
        this.hops,
        this.gatherOptions,
      );
    }
    if (op === "gt" || op === "gte" || op === "lt" || op === "lte") {
      if (typeof value !== "string" && typeof value !== "number" && typeof value !== "bigint") {
        throw new Error(`${op} requires a text or integer query value`);
      }
      return new CoreAbiQueryBuilder<Row>(
        this._table,
        this._schema,
        [
          ...this.filters,
          { column, columnType, nullable, op, value: encodeRangeQueryLiteral(value) },
        ],
        this.selectedColumns,
        this.orderByItems,
        this.limitCount,
        this.offsetCount,
        this.includes,
        this.hops,
        this.gatherOptions,
      );
    }
    if (op === "isNull" || op === "isNotNull") {
      return new CoreAbiQueryBuilder<Row>(
        this._table,
        this._schema,
        [...this.filters, { column, columnType, op }],
        this.selectedColumns,
        this.orderByItems,
        this.limitCount,
        this.offsetCount,
        this.includes,
        this.hops,
        this.gatherOptions,
      );
    }
    if (op !== "contains") throw new Error(`unsupported query operator ${op}`);
    if (typeof value !== "string")
      throw new Error("contains is currently wired for string values only");
    return new CoreAbiQueryBuilder<Row>(
      this._table,
      this._schema,
      [...this.filters, { column, columnType, op, value }],
      this.selectedColumns,
      this.orderByItems,
      this.limitCount,
      this.offsetCount,
      this.includes,
      this.hops,
      this.gatherOptions,
    );
  }

  select<const Columns extends readonly (keyof Row & string)[]>(
    ...columns: Columns
  ): QueryBuilder<ProjectedRow<Row, Columns>> {
    for (const column of columns) this.#columnType(column);
    return new CoreAbiQueryBuilder<ProjectedRow<Row, Columns>>(
      this._table,
      this._schema,
      this.filters,
      [...columns],
      this.orderByItems,
      this.limitCount,
      this.offsetCount,
      this.includes,
      this.hops,
      this.gatherOptions,
    );
  }

  orderBy(column: keyof Row & string, direction: OrderDirection = "asc"): QueryBuilder<Row> {
    this.#columnType(column);
    if (direction !== "asc" && direction !== "desc")
      throw new Error("query order direction must be 'asc' or 'desc'");
    return new CoreAbiQueryBuilder<Row>(
      this._table,
      this._schema,
      this.filters,
      this.selectedColumns,
      [...this.orderByItems, { column, direction }],
      this.limitCount,
      this.offsetCount,
      this.includes,
      this.hops,
      this.gatherOptions,
    );
  }

  limit(count: number): QueryBuilder<Row> {
    return new CoreAbiQueryBuilder<Row>(
      this._table,
      this._schema,
      this.filters,
      this.selectedColumns,
      this.orderByItems,
      validateCount(count, "limit"),
      this.offsetCount,
      this.includes,
      this.hops,
      this.gatherOptions,
    );
  }

  offset(count: number): QueryBuilder<Row> {
    return new CoreAbiQueryBuilder<Row>(
      this._table,
      this._schema,
      this.filters,
      this.selectedColumns,
      this.orderByItems,
      this.limitCount,
      validateCount(count, "offset"),
      this.includes,
      this.hops,
      this.gatherOptions,
    );
  }

  include<Property extends string>(
    property: Property,
  ): QueryBuilder<Row & Record<Property, unknown[] | unknown | null>>;
  include<const Includes extends QueryIncludeMap>(
    includes: Includes,
  ): QueryBuilder<Row & { [Property in keyof Includes & string]: unknown[] | unknown | null }>;
  include<Property extends string>(
    propertyOrIncludes: Property | QueryIncludeMap,
  ): QueryBuilder<Row & Record<Property, unknown[] | unknown | null>> {
    if (typeof propertyOrIncludes !== "string") {
      return Object.entries(propertyOrIncludes).reduce<QueryBuilder<Row>>(
        (query, [property, include]) =>
          include === undefined
            ? query
            : ((query as CoreAbiQueryBuilder<Row>).includeSpecInternal(
                property,
                include,
              ) as QueryBuilder<Row>),
        this,
      ) as QueryBuilder<Row & Record<Property, unknown[] | unknown | null>>;
    }
    const property = propertyOrIncludes;
    includeRelation(this._schema, this._table, property);
    return new CoreAbiQueryBuilder<Row & Record<Property, unknown[] | unknown | null>>(
      this._table,
      this._schema,
      this.filters,
      this.selectedColumns,
      this.orderByItems,
      this.limitCount,
      this.offsetCount,
      { ...this.includes, [property]: true },
      this.hops,
      this.gatherOptions,
    );
  }

  includeSpecInternal<Property extends string>(
    property: Property,
    include: true | IncludeOptions,
  ): QueryBuilder<Row & Record<Property, unknown[] | unknown | null>> {
    const relation = includeRelation(this._schema, this._table, property);
    const normalized = normalizeIncludeOptions(include);
    validateIncludeSpec(this._schema, relation.table, normalized);
    return new CoreAbiQueryBuilder<Row & Record<Property, unknown[] | unknown | null>>(
      this._table,
      this._schema,
      this.filters,
      this.selectedColumns,
      this.orderByItems,
      this.limitCount,
      this.offsetCount,
      { ...this.includes, [property]: normalized },
      this.hops,
      this.gatherOptions,
    );
  }

  #whereObject(conditions: QueryWhere<Row>): QueryBuilder<Row> {
    return (Object.entries(conditions) as Array<[keyof Row & string, QueryWhereValue]>).reduce<
      QueryBuilder<Row>
    >((query, [column, condition]) => {
      if (condition === undefined) return query;
      if (isWhereOperatorObject(condition)) {
        if (condition.eq !== undefined) query = query.where(column, "eq", condition.eq);
        if (condition.ne !== undefined) query = query.where(column, "ne", condition.ne);
        if (condition.in !== undefined) query = query.where(column, "in", condition.in);
        if (condition.gt !== undefined) query = query.where(column, "gt", condition.gt);
        if (condition.gte !== undefined) query = query.where(column, "gte", condition.gte);
        if (condition.lt !== undefined) query = query.where(column, "lt", condition.lt);
        if (condition.lte !== undefined) query = query.where(column, "lte", condition.lte);
        if (condition.contains !== undefined)
          query = query.where(column, "contains", condition.contains);
        if (condition.isNull === true) query = query.where(column, "isNull");
        if (condition.isNull === false) query = query.where(column, "isNotNull");
        return query;
      }
      return query.where(column, "eq", condition);
    }, this);
  }

  requireIncludes<const Properties extends readonly string[]>(
    ...properties: Properties
  ): QueryBuilder<Row & { [Property in Properties[number]]: unknown[] | unknown | null }> {
    const includes = { ...this.includes };
    for (const property of properties) {
      includeRelation(this._schema, this._table, property);
      includes[property] = { required: true };
    }
    return new CoreAbiQueryBuilder<
      Row & { [Property in Properties[number]]: unknown[] | unknown | null }
    >(
      this._table,
      this._schema,
      this.filters,
      this.selectedColumns,
      this.orderByItems,
      this.limitCount,
      this.offsetCount,
      includes,
      this.hops,
      this.gatherOptions,
    );
  }

  hop<Property extends string>(property: Property): QueryBuilder<Record<string, unknown>> {
    forwardRelation(this._schema, this.#terminalHopTable(), property);
    return new CoreAbiQueryBuilder<Record<string, unknown>>(
      this._table,
      this._schema,
      this.filters,
      this.selectedColumns,
      this.orderByItems,
      this.limitCount,
      this.offsetCount,
      this.includes,
      [...this.hops, property],
      this.gatherOptions,
    );
  }

  gather(options: GatherOptions): QueryBuilder<Row> {
    return new CoreAbiQueryBuilder<Row>(
      this._table,
      this._schema,
      this.filters,
      this.selectedColumns,
      this.orderByItems,
      this.limitCount,
      this.offsetCount,
      this.includes,
      this.hops,
      options,
    );
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      filters: this.filters,
      select: this.selectedColumns,
      orderBy: this.orderByItems,
      limit: this.limitCount,
      offset: this.offsetCount,
      includes: this.includes,
      hops: this.hops.length > 0 ? this.hops : undefined,
      gather: this.gatherOptions,
    });
  }

  #terminalHopTable(): string {
    let table = this._table;
    for (const hop of this.hops) table = forwardRelation(this._schema, table, hop).table;
    return table;
  }

  #columnType(column: string): ColumnType {
    return this.#columnDefinition(column).column_type;
  }

  #columnDefinition(column: string): ColumnDefinition {
    if (column === "id") return implicitIdColumn();
    const definition = this._schema[this._table]?.columns.find(
      (candidate) => candidate.name === column,
    );
    if (!definition) throw new Error(`unknown column ${this._table}.${column}`);
    return definition;
  }
}

function encodeQueryLiteral(value: QueryValue): QueryLiteral {
  if (Array.isArray(value)) return value.map(encodeQueryLiteral);
  if (typeof value === "bigint") {
    return encodeIntegerQueryLiteral(value);
  }
  if (value instanceof Uint8Array) return { bytes: Array.from(value) };
  return value as boolean | string | number | null;
}

function encodeIntegerQueryLiteral(value: number | bigint): number {
  const numberValue = typeof value === "bigint" ? Number(value) : value;
  if (!Number.isSafeInteger(numberValue) || numberValue < 0)
    throw new Error("expected non-negative safe integer query value");
  return numberValue;
}

function validateCount(value: unknown, label: "limit" | "offset"): number {
  if (value == null) return 0;
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
    throw new Error(`query ${label} must be a non-negative safe integer`);
  }
  return value;
}

function encodeRangeQueryLiteral(value: string | number | bigint): string | number {
  return typeof value === "string" ? value : encodeIntegerQueryLiteral(value);
}

function isQueryValueArray(
  value: QueryValue | readonly QueryValue[] | undefined,
): value is readonly QueryValue[] {
  return Array.isArray(value);
}

function isWhereOperatorObject(
  value: QueryWhereValue,
): value is Exclude<QueryWhereValue, QueryValue | undefined> {
  return (
    typeof value === "object" &&
    value !== null &&
    !(value instanceof Uint8Array) &&
    !Array.isArray(value)
  );
}

function isArrayColumn(columnType: ColumnType): boolean {
  const normalized = typeof columnType === "string" ? { type: columnType } : columnType;
  return normalized.type === "Array";
}

function isStringArray(value: unknown): value is string[] {
  return Array.isArray(value) && value.every((item) => typeof item === "string");
}

function isQueryOrderBy(value: unknown): value is QueryOrderBy {
  return (
    typeof value === "object" &&
    value != null &&
    "column" in value &&
    typeof value.column === "string" &&
    "direction" in value &&
    (value.direction === "asc" || value.direction === "desc")
  );
}

function isColumnDefinitionInput(value: unknown): value is ColumnDefinitionInput {
  return typeof value === "object" && value != null && "column_type" in value;
}

function readFile<Row extends BinaryLargeValueRow>(
  db: Db,
  table: Table<Row, unknown>,
  rowId: Row["id"],
): Row {
  const encodedRowId = encodeRowId(rowId);
  const file = db
    .all(table)
    .find((candidate) => sameBytes(encodeRowId(candidate.id), encodedRowId));
  if (!file) throw new Error("file not found");
  return normalizeBinaryLargeValueRow(file);
}

function normalizeBinaryLargeValueRow<Row extends BinaryLargeValueRow>(row: Row): Row {
  const normalized = { ...row } as Row & Record<string, unknown>;
  const name: unknown = normalized.name;
  const mimeType: unknown = normalized.mime_type;
  if (name instanceof Uint8Array) normalized.name = decodeNullableTextCell(name);
  if (mimeType instanceof Uint8Array) normalized.mime_type = decodeNullableTextCell(mimeType);
  if (typeof normalized.size === "number") normalized.size = BigInt(normalized.size);
  return normalized as Row;
}

function decodeNullableTextCell(bytes: Uint8Array): string | undefined {
  if (bytes.length === 0 || bytes[0] === 0) return undefined;
  return new TextDecoder().decode(bytes.slice(1));
}

function hasColumn(table: Table<BinaryLargeValueRow, unknown>, columnName: string): boolean {
  return table._schema[table._table]?.columns.some((column) => column.name === columnName) ?? false;
}

function arrayBufferFromBytes(bytes: Uint8Array): ArrayBuffer {
  const out = new ArrayBuffer(bytes.byteLength);
  new Uint8Array(out).set(bytes);
  return out;
}

function encodeRowId(value: unknown): Uint8Array {
  if (value instanceof Uint8Array) {
    if (value.length !== 16) throw new Error("row id bytes must be 16 bytes");
    return value;
  }
  if (typeof value !== "string" || !uuidText.test(value))
    throw new Error("row id must be a UUID string or 16-byte Uint8Array");
  return parseUuid(value);
}

function parseUuid(value: string): Uint8Array {
  const hex = value.replaceAll("-", "");
  const bytes = new Uint8Array(16);
  for (let index = 0; index < bytes.length; index += 1) {
    bytes[index] = Number.parseInt(hex.slice(index * 2, index * 2 + 2), 16);
  }
  return bytes;
}

function formatUuid(bytes: Uint8Array): string {
  const hex = Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function sameBytes(left: Uint8Array, right: Uint8Array): boolean {
  return left.length === right.length && left.every((byte, index) => byte === right[index]);
}

function isPromiseLike<Value>(value: Value | PromiseLike<Value>): value is PromiseLike<Value> {
  return (
    value != null &&
    (typeof value === "object" || typeof value === "function") &&
    "then" in value &&
    typeof (value as { then?: unknown }).then === "function"
  );
}

function concatBytes(chunks: Uint8Array[]): Uint8Array {
  const length = chunks.reduce((sum, chunk) => sum + chunk.length, 0);
  const out = new Uint8Array(length);
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.length;
  }
  return out;
}

function u32Le(value: number): Uint8Array {
  const bytes = new Uint8Array(4);
  new DataView(bytes.buffer).setUint32(0, value, true);
  return bytes;
}

function u64Le(value: number): Uint8Array {
  if (value < 0) throw new Error("expected non-negative integer");
  const bytes = new Uint8Array(8);
  new DataView(bytes.buffer).setBigUint64(0, BigInt(value), true);
  return bytes;
}

function fixedSize(valueType: ValueType): number | undefined {
  switch (valueType.tag) {
    case 0:
    case 5:
    case 9:
      return 1;
    case 1:
      return 2;
    case 2:
      return 4;
    case 3:
    case 4:
      return 8;
    case 8:
      return 16;
    case 12: {
      const innerSize = valueType.inner ? fixedSize(valueType.inner) : undefined;
      return innerSize == null ? undefined : innerSize + 1;
    }
    case 11:
      return undefined;
    default:
      return undefined;
  }
}

async function loadRuntime(): Promise<WasmDbConstructor> {
  const modulePath = "../../../jazz-wasm/pkg/jazz_core_wasm.js";
  const mod = (await import(/* @vite-ignore */ modulePath)) as { WasmDb?: WasmDbConstructor };
  if (!mod.WasmDb) throw new Error("jazz-wasm/pkg does not export WasmDb");
  return mod.WasmDb;
}

function asWasmDbConstructor(
  candidate: WasmDbConstructor | (new () => unknown),
): WasmDbConstructor {
  const maybe = candidate as Partial<WasmDbConstructor>;
  if (typeof maybe.openMemory === "function") return maybe as WasmDbConstructor;
  throw new Error("DbOptions.Runtime must expose WasmDb.openMemory(schema, config)");
}
