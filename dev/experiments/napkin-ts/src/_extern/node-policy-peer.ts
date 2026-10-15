import { extern } from "../support/runtime.js";

// ---- types / ids / time ----
export type AuthorSubject = any;
export type TxId = any;
export type GlobalTime = any; // [seq, tiebreak]
export const AUTHOR_SYSTEM = extern<AuthorSubject>();
export const authorEquals = extern<(a: AuthorSubject, b: AuthorSubject) => boolean>();
export const cmpTxId = extern<(a: TxId, b: TxId) => number>();
export const cmpGlobalTime = extern<(a: GlobalTime, b: GlobalTime) => number>();
export const claimsEqual = extern<(a: Map<string, any>, b: Map<string, any>) => boolean>();
export const coveredInputTrace = extern<() => boolean>();
export const rehydrateTrace = extern<() => boolean>();
export const debugFmt = extern<(v: unknown) => string>();

// ---- protocol ----
export type SubscriptionKey = any;
export type ResultMemberEntry = any;
export type KnownStateDeclaration = any;
export type SyncMessage = any;
export type VersionBundle = any;
export type VersionCarrier = any;
export type AuthorityResultKey = any;
export const cmpSubscriptionKey = extern<(a: SubscriptionKey, b: SubscriptionKey) => number>();
export const cmpResultMemberEntry =
  extern<(a: ResultMemberEntry, b: ResultMemberEntry) => number>();
export const resultMemberEquals = extern<(a: ResultMemberEntry, b: ResultMemberEntry) => boolean>();
export const resultMemberKey = extern<(m: ResultMemberEntry) => string>();
export const rowKeyString = extern<(k: [any, string, any]) => string>();
export const expandVersionCarriers = extern<(c: VersionCarrier[]) => VersionBundle[]>();
export const defaultReadViewKey = extern<() => any>();
export const readViewKeyEquals = extern<(a: any, b: any) => boolean>();
export const policyBindingKeyFromCanonicalParts =
  extern<(identity: AuthorSubject, claims: Map<string, any>) => any>();
export const AuthorityResultKey = extern<{ unscoped(bindingView: any): AuthorityResultKey }>();
export const BindingViewKey = extern<{ fromCanonicalSubscriptionKey(s: SubscriptionKey): any }>();
export const PeerPayloadInventory = extern<{ default(): any }>();
export const SupportingRowsUpdate = extern<{
  snapshot(rows: any[]): any;
  delta(predecessor: Uint8Array, revision: Uint8Array, adds: any[], removes: any[]): any;
}>();
export const RegisterShapeOptions = extern<{ default(): any }>();
export const ShapeAst = extern<{ fromValidated(shape: any): any }>();
export const ProgramFactEntry = extern<{ isPeerSourceClosureFact(f: any): boolean }>();

// ---- node (other node partitions) ----
export const NodeErrorKind = extern<Record<string, string>>();
export const nodeInvalidStoredValue = extern<(message: string) => Error>();
export const isNodeError = extern<(e: unknown, kind: string) => boolean>();
export const LocalAuthorityReconciliation = extern<new () => any>();
export const emptyResultTransitions = extern<() => any>();
export const simpleScalarExitQuery = extern<(q: any) => boolean>();
export const nodeError = extern<(kind: string, message?: string, data?: unknown) => Error>();
export const blake3DeriveKey = extern<(context: string, material: Uint8Array) => Uint8Array>();

// ---- model / schema ----
export const columnSchemaEquals = extern<(a: any, b: any) => boolean>();
export const columnTypeEquals = extern<(a: any, b: any) => boolean>();
export const valueEquals = extern<(a: any, b: any) => boolean>();
export const physicalValueEpochIsCompatible = extern<(a: any, b: any) => boolean>();
export const validateRegisteredTransform = extern<(transform: string) => void>();

// ---- groove records ----
export const RecordDescriptor = extern<{
  new: (fields: [string, any][]) => any;
  newWithFields: (fields: any[]) => any;
}>();
export const Record_ = extern<{ new: (bytes: Uint8Array, descriptor: any) => any }>();
export const ValueType = extern<
  Record<string, any> & { arrayOf(t: any): any; record(d: any): any }
>();
export const V = extern<Record<string, (...args: any[]) => any>>();

// ---- tx / query (model) ----
export const Snapshot = extern<{
  exclusiveBase(owner: any, globalBase: any, localBase: any, dots: any[]): any;
}>();
export const txIdNew = extern<(time: any, node: any) => TxId>();
export const snapshotEquals = extern<(a: any, b: any) => boolean>();
export const rowReadsEqual = extern<(a: any[], b: any[]) => boolean>();
export const absentReadsEqual = extern<(a: any[], b: any[]) => boolean>();
export const predicateReadsEqual = extern<(a: any[], b: any[]) => boolean>();
export const QUERY_NAMESPACE = extern<Uint8Array>();
export const encodeQueryPostcard = extern<(q: any) => Uint8Array>();
export const takeQueryPostcard = extern<(b: Uint8Array) => [any, Uint8Array]>();
export const canonicalBindingBytesForValues = extern<(values: Map<string, any>) => Uint8Array>();
export const bindingValuesFromCanonicalBytes = extern<(b: Uint8Array) => Map<string, any>>();

// ---- node catalogue / physical (other node partitions) ----
export const deepEqual = extern<(a: unknown, b: unknown) => boolean>();
export const cloneCatalogue = extern<(c: any) => any>();
export const ActiveSchema = extern<{ new: (pointer: any, schema: any) => any }>();
export const SchemaVersion = extern<{ new: (schema: any) => any }>();
export const CatalogueRecordKind = extern<Record<string, number>>();
export const putCatalogue =
  extern<(batch: any, kind: number, id: string, bytes: Uint8Array) => void>();
export const deleteCatalogue = extern<(batch: any, kind: number, id: string) => void>();
export const encodeCatalogueBootstrapReady = extern<(r: any) => Uint8Array>();
export const encodeCatalogueSchema = extern<(s: any) => Uint8Array>();
export const nextGrooveRuntimeToken = extern<() => number>();
export const uuidSimple = extern<(id: string) => string>();
export const allocateProvisionalPhysicalMapping =
  extern<
    (
      schema: any,
      identities: any,
      ids: { nextPhysicalTableId: number; nextPhysicalColumnId: number },
    ) => any
  >();
export const validateDurablePhysicalIdentityBindings = extern<(...args: any[]) => void>();
export const validateScalarEnumCaseProvenance = extern<(mappings: any, aliases: any) => void>();
export const validatePayloadEnumCaseProvenance = extern<(mappings: any, aliases: any) => void>();
/** Associated (non-self) NodeState functions owned by node/mod.rs & friends. */
export const NodeStatics = extern<Record<string, (...args: any[]) => any>>();
export const DescriptorField = extern<{ new: (name: string, type: any) => any }>();
export const OwnedRecord = extern<{ new: (raw: Uint8Array, descriptor: any) => any }>();
export const RecordProjector = extern<{
  newRegistryRebound: (source: any, target: any, mapping: [number, number][]) => any;
}>();
export const encodePersistedRecordDescriptor = extern<(d: any) => Uint8Array>();
export const decodePersistedRecordDescriptor = extern<(b: Uint8Array) => any>();
export const valueTypeEquals = extern<(a: any, b: any) => boolean>();
export const runtimeResultIdentityBytes = extern<(value: any, type: any) => Uint8Array>();
export const CurrentRow = extern<{
  newWithPublicationFields: (table: string, record: any, publications: any[]) => any;
}>();

// ---- node history / branch views (other node partitions) ----
export const BranchColumnValue = extern<{ encodeTyped(value: any, type: any): any }>();
export const applyCompiledLensPath = extern<(path: any, cells: Map<string, any>) => string>();
export const branchViewStack =
  extern<
    (schema: any, table: any, head: any, base: any) => { value?: [any, any]; error?: string }
  >();
export const cmpSortKey = extern<(a: any, b: any) => number>();
export const currentRowFromMaterializedCellsWithLayerProvenance =
  extern<(table: any, content: any, source: any, updated: any, cells: Map<string, any>) => any>();
export const durabilityAtLeast = extern<(a: any, b: string) => boolean>();
export const isUnrepresentableEnumProjection = extern<(e: unknown) => boolean>();
export const sortCurrentRows = extern<(rows: any[]) => void>();
export const topWins = extern<(layers: any[], key: string) => any>();
export const txIdEquals = extern<(a: TxId, b: TxId) => boolean>();

// ---- policy evaluation (query_eval & friends) ----
export const RowAuthor = extern<{ fromSession(madeBy: any, node: any): any }>();
export const TransactionOverlayTable = extern<{ new: (rows: any) => any }>();
export const TransactionWriteOverlay = extern<{
  default(): any;
  acceptedState(): any;
  fromTables(tables: Map<string, any>): any;
}>();
export const VersionRecord = extern<{ fromCommit(commit: any, table: any, schema: string): any }>();
export const authorizationQueryFromReadPolicy = extern<(table: any) => any>();
export const currentRowFromCellsWithExplicitProvenance =
  extern<
    (table: any, rowUuid: string, cells: Map<string, any>, provenance: any, layer: any) => any
  >();
export const queryFrom = extern<(table: string) => any>();
