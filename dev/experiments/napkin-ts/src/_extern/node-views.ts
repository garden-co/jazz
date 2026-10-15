import { extern } from "../support/runtime.js";

// node/api_error (node-state)
export type ErrorCode = string;
export const ErrorCode = extern<Record<string, ErrorCode>>();
export const NodeError =
  extern<new (code: ErrorCode, message: string) => Error & { code: ErrorCode }>();
export const nodeError = (code: ErrorCode, message: string): Error => new NodeError(code, message);

// groove records / ivm
export type RecordDescriptor = any;
export type OwnedRecord = any;
export type Value = any;
export type ValueType = any;
export const OwnedRecord = extern<{
  new: (raw: Uint8Array, descriptor: RecordDescriptor) => OwnedRecord;
}>();
export const terminalOccurrenceKey =
  extern<(rowKey: Uint8Array, occurrence: number) => Uint8Array>();
export const descriptorEquals = extern<(a: RecordDescriptor, b: RecordDescriptor) => boolean>();
export const postcardValuesSize = extern<(values: Value[]) => number>();
export type TerminalEdit = any;
export type TerminalOperation = any;
export type TerminalPathSegment = any;

// object ids
export type ObjectId = any;
export type OutputOccurrenceId = any;
export const ObjectId = extern<{
  fromUuid(bytes: Uint8Array): ObjectId;
  equals(a: ObjectId, b: ObjectId): boolean;
}>();
export const OutputOccurrenceId = extern<{
  new_(root: ObjectId, joined: ObjectId[]): OutputOccurrenceId;
  withUnionArms(
    root: ObjectId,
    joined: ObjectId[],
    arms: [number, string][],
  ): OutputOccurrenceId | undefined;
}>();

// node mod (node-state)
export type CurrentRowPublicationField = any;
export const publicationPublicName =
  extern<(f: CurrentRowPublicationField) => string | undefined>();

// protocol
export type SupportingRow = any;
export const cmpSupportingRow = extern<(a: SupportingRow, b: SupportingRow) => number>();

// ---- currency.rs deps (node-state / node-physical-ingest / protocol / types) ----
export type NodeState = any;
export type VersionRow = any;
export type StoredTransaction = any;
export type TxId = any;
export type BranchKey = any;
export const NodeErrors = extern<{
  tableNotFound(table: string): Error;
  invalidStoredValue(message: string): Error;
  invalidInput(message: string): Error;
  protocol(message: string): Error;
  groove(error: unknown): Error;
  conflictingCommitUnit(tx: any): Error;
  malformedViewUpdate(reason: string): Error;
  invalidAuthoritySourceClosure(subscription: any, transition: string): Error;
  missingTransaction(tx: any): Error;
  maintainedViewMissingBundleWitness(message: string): Error;
}>();
/** Node `Error` variant tag + payload, as thrown by NodeErrors. */
export type NodeErrorLike = Error & { kind: string; reason?: string; message: string };
export const BranchKey = extern<{
  default_(): BranchKey;
  cmp(a: BranchKey, b: BranchKey): number;
}>();
export const HistoryRowRecord = extern<Record<string, number>>();
export const GlobalCurrentRowRecord = extern<Record<string, number>>();
export const TransactionRowRecord = extern<Record<string, number>>();
export const NodeAliasRowRecord = extern<Record<string, number>>();
export const physicalHistoryTableName = extern<(tableId: any) => string>();
export const physicalVersionTableId = extern<(storageTable: string) => any | undefined>();
export const versionTxIdFromAliases = extern<(version: VersionRow, aliases: any) => TxId>();
export const historySeqCandidates = extern<(globalTime: number | undefined) => number[]>();
export const historyKeyValues =
  extern<(branch: Uint8Array, row: any, seq: number, txTime: number, alias: number) => Value[]>();
export const fateFromEncodedFields = extern<(record: any) => any>();
export const durabilityFromDiscriminant = extern<(d: number) => any>();
export const txKindFromDiscriminant = extern<(d: number) => any>();
export const rowAuthorValue = extern<(subject: any) => Value>();
export const RowAuthor = extern<{ fromRecord(r: any): any; valueType(): ValueType }>();
export const decodeEvidenceSlots = extern<(...slots: (Uint8Array | undefined)[]) => any>();
export const decodePersistedBranchKey =
  extern<(tableSchema: any, bytes: Uint8Array) => BranchKey>();
export const txTimeFromPhysicalMs = extern<(ms: number) => number>();
export const txTimeSortKey = extern<(time: number, node: any) => any>();
export const cmpSortKey = extern<(a: any, b: any) => number>();
export const cmpRowUuid = extern<(a: any, b: any) => number>();
export const cmpTxId = extern<(a: TxId, b: TxId) => number>();
export const txIdKey = extern<(tx: TxId) => string>();
export const nodeUuidKey = extern<(n: any) => string>();
export const TxId = extern<{ new_(time: number, node: any): TxId }>();
export const RecordDescriptorApi = extern<{
  new_(fields: [string, ValueType][]): RecordDescriptor;
}>();
export const nullableType = extern<(vt: ValueType) => ValueType>();
export const readOptionalAuthorSubject = extern<(record: any, idx: number) => any>();
export const readOptionalRecord = extern<(record: any, idx: number) => OwnedRecord | undefined>();
export const SchemaFields = extern<{
  COUNTER_SIGNS_FIELD: string;
  SEQ_FIELD: string;
  BASE_SEQ_FIELD: string;
  BASE_PENDING_FIELD: string;
}>();
export const HISTORY_TX_AUTHOR_CACHE_MAX_ENTRIES = extern<number>();
export const internTable = extern<(name: string) => any>();

// ---- views.rs deps ----
export type ViewEvaluationContext = any;
export const ViewEvaluationContext = extern<{ default_(): ViewEvaluationContext }>();
export type VersionBundle = any;
export type VersionRecord = any;
export type SyncMessage = any;
export const CoveredInputCoordinate = extern<{
  from(row: SupportingRow): any;
  key(c: any): string;
}>();
export const coveredKey = (row: SupportingRow): string =>
  CoveredInputCoordinate.key(CoveredInputCoordinate.from(row));
export const SupportingRowsUpdate = extern<{ snapshot(rows: SupportingRow[]): any }>();
export const supportingRowEquals = extern<(a: SupportingRow, b: SupportingRow) => boolean>();
export const isWireValid = extern<(row: SupportingRow) => boolean>();
export const transactionWithoutPermissionSubject = extern<(tx: any) => any>();
export const knownTransactionPayloadMatches = extern<(stored: any, incoming: any) => boolean>();
export const transactionEquals = extern<(a: any, b: any) => boolean>();
export const versionRecordEquals = extern<(a: VersionRecord, b: VersionRecord) => boolean>();
export const cmpVersionRecord = extern<(a: VersionRecord, b: VersionRecord) => number>();
export const fateEquals = extern<(a: any, b: any) => boolean>();
export const rowUuidKey = extern<(row: any) => string>();
export const schemaVersionKey = extern<(s: any) => string>();
export const buildVersionCarriersFromSingletons = extern<(bundles: VersionBundle[]) => any[]>();
export const readPolicyAdmitsEveryRow = extern<(policy: any) => boolean>();
export const recordMaintainedViewStreamBAddBundle = extern<() => void>();
export const recordMaintainedViewRemovalStreamBundle = extern<() => void>();
export const validateReceivedViewBundleGlobalTimeDurability =
  extern<(globalTime: number | undefined, durability: any) => void>();
export const AuthorityResultState = extern<{ new_(...args: any[]): any }>();
export const authorityResultKeyString = extern<(key: any) => string>();
export const subscriptionKeyString = extern<(key: any) => string>();

// ---- maintained_subscription_view.rs deps ----
export type ResultMemberEntry = any;
export type NodeAliases = any;
export const cmpResultMember = extern<(a: ResultMemberEntry, b: ResultMemberEntry) => number>();
export const resultMemberEquals = extern<(a: ResultMemberEntry, b: ResultMemberEntry) => boolean>();
export const programFactKey = extern<(fact: any) => string>();
export const programSourceKey = extern<(source: any) => string>();
export const RealRowMemberEntry = extern<{ currentContent(row: [string, any, TxId]): any }>();
export const CurrentPayloadEncodePlan = extern<{
  new_(descriptor: RecordDescriptor, schema: any): any;
}>();
export const encodeAggregatePayloadRecord = extern<(record: any, schema: any) => any>();
export const SyntheticReplacementToken = extern<{ fromEncodedRecord(bytes: Uint8Array): any }>();
export const historyValuesFromParts = extern<(table: any, parts: any) => Value[]>();
export const authoredColumnIdsFromValue = extern<(value: Value) => any>();
export const deletionEventFromValue = extern<(value: Value) => any>();
export const nullableValue = extern<(value: Value) => Value | undefined>();
export const HISTORY_USER_CELLS = extern<number>();
export const encodePersistedRecordDescriptor = extern<(d: RecordDescriptor) => Uint8Array>();
export const decodePersistedRecordDescriptor = extern<(b: Uint8Array) => RecordDescriptor>();
export const RecordDescriptorFields = extern<{
  newWithFields(fields: any[]): RecordDescriptor;
  field(name: string, valueType: ValueType): any;
}>();
export const recordProjectorRegistryRebound =
  extern<
    (source: RecordDescriptor, target: RecordDescriptor, map: [number, number][]) => unknown
  >();
export const EnumValue = extern<{ new_(tag: number, record: OwnedRecord): Value }>();
export const valueTypeEquals = extern<(a: ValueType, b: ValueType) => boolean>();
export const debugCoveredInputTrace = extern<() => boolean>();
export const defaultBranchCanonicalBytes = extern<() => Uint8Array>();
export const VersionRowApi = extern<{
  new_(table: any, branchKey: BranchKey, record: OwnedRecord): VersionRow;
}>();
