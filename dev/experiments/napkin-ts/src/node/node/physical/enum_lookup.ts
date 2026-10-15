// Durable enum case lookups and authored/physical tag remaps, written once over both case kinds.
import { SortedMap } from "../../../support/collections.js";
import {
  nodeError,
  RecursiveEnumRemaps,
  globalCasePath,
  type NodeState,
  type ValueType,
  type EnumOccurrenceRemaps,
  type Uuid,
} from "../../../_extern/node-physical-ingest.js";
import {
  type TablePhysicalMapping,
  type GlobalScalarEnumCaseId,
  type GlobalEnumCaseId,
  compareEnumCases,
  caseIdEquals,
  newCaseSet,
  bootstrapCopyEnumRemaps,
} from "./catalogue.js";

type CaseId = GlobalScalarEnumCaseId | GlobalEnumCaseId;

/** One kind of durable enum case identity: compact scalar (u8) or payload (u32) tags. */
type EnumCaseKind = {
  maxTag: number;
  missing: [string, string];
  targetTagExhausted: [string, string];
  copiedTagExhausted: [string, string];
  root(t: TablePhysicalMapping): SortedMap<number, CaseId[]>;
  nested(t: TablePhysicalMapping): SortedMap<number, SortedMap<string, CaseId[]>>;
  install(
    r: EnumOccurrenceRemaps,
    path: string,
    tags: (number | undefined)[],
    children: (CaseId | undefined)[],
  ): void;
  afterAuthoredRoot?(r: EnumOccurrenceRemaps): void;
};

export const SCALAR: EnumCaseKind = {
  maxTag: 0xff,
  missing: [
    "physical scalar enum registry identity mapping missing",
    "physical nested scalar enum registry identity mapping missing",
  ],
  targetTagExhausted: [
    "target scalar enum tag exhausted",
    "target nested scalar enum tag exhausted",
  ],
  copiedTagExhausted: [
    "copied scalar enum tag exhausted",
    "copied nested scalar enum tag exhausted",
  ],
  root: (t) => t.scalarEnumCases,
  nested: (t) => t.nestedScalarEnumCases,
  install: (r, path, tags) => void r.scalar.set(path, tags),
};

export const PAYLOAD: EnumCaseKind = {
  maxTag: 0xffffffff,
  missing: [
    "physical payload enum registry identity mapping missing",
    "physical nested payload enum registry identity mapping missing",
  ],
  targetTagExhausted: [
    "target payload enum tag exhausted",
    "target nested payload enum tag exhausted",
  ],
  copiedTagExhausted: [
    "copied payload enum tag exhausted",
    "copied nested payload enum tag exhausted",
  ],
  root: (t) => t.payloadEnumCases,
  nested: (t) => t.nestedPayloadEnumCases,
  install: (r, path, tags, children) => {
    r.payload.set(path, tags);
    r.payloadChildren.set(
      path,
      children.map((c) => (c ? globalCasePath(path, c) : undefined)),
    );
  },
  /** Payload values also receive a `root/nullable` alias for the current table's nullable carrier. */
  afterAuthoredRoot: (r) => {
    const tags = r.payload.get("root");
    if (tags) r.payload.set("root/nullable", tags.slice());
    const children = r.payloadChildren.get("root");
    if (children) r.payloadChildren.set("root/nullable", children.slice());
  },
};

const enumError = (messages: [string, string], path: string | undefined) =>
  nodeError("InvalidStoredValue", messages[path === undefined ? 0 : 1]);

function occurrence(
  kind: EnumCaseKind,
  table: TablePhysicalMapping,
  columnId: number,
  path: string | undefined,
): CaseId[] | undefined {
  return path === undefined
    ? kind.root(table).get(columnId)
    : kind.nested(table).get(columnId)?.get(path);
}

export const physicalScalarEnumCases = (node: NodeState, tableId: number, columnId: number) =>
  physicalEnumCases(node, SCALAR, tableId, columnId) as GlobalScalarEnumCaseId[];
export const physicalPayloadEnumCases = (node: NodeState, tableId: number, columnId: number) =>
  physicalEnumCases(node, PAYLOAD, tableId, columnId) as GlobalEnumCaseId[];
export const physicalNestedScalarEnumCases = (
  node: NodeState,
  tableId: number,
  columnId: number,
  path: string,
) => physicalEnumCases(node, SCALAR, tableId, columnId, path) as GlobalScalarEnumCaseId[];
export const physicalNestedPayloadEnumCases = (
  node: NodeState,
  tableId: number,
  columnId: number,
  path: string,
) => physicalEnumCases(node, PAYLOAD, tableId, columnId, path) as GlobalEnumCaseId[];

/** The union of one enum occurrence's registry across the physical table's mappings, in durable order. */
export function physicalEnumCases(
  node: NodeState,
  kind: EnumCaseKind,
  tableId: number,
  columnId: number,
  path?: string,
): CaseId[] {
  const cases = newCaseSet<CaseId>();
  for (const mapping of node.catalogue.physicalMappings.valuesArray())
    for (const table of mapping.tables.valuesArray() as TablePhysicalMapping[]) {
      if (table.tableId !== tableId) continue;
      for (const c of occurrence(kind, table, columnId, path) ?? []) cases.add(c);
    }
  if (cases.size === 0) throw enumError(kind.missing, path);
  const aliases: SortedMap<Uuid, number> = node.catalogue.schemaVersionAliases;
  return cases.toArray().sort((l, r) => compareEnumCases(aliases, l, r));
}

function physicalEnumCasesOr(
  node: NodeState,
  kind: EnumCaseKind,
  tableId: number,
  columnId: number,
  path: string | undefined,
  fallback: CaseId[],
): CaseId[] {
  try {
    return physicalEnumCases(node, kind, tableId, columnId, path);
  } catch {
    return fallback.slice();
  }
}

function occurrences(
  kind: EnumCaseKind,
  mapping: TablePhysicalMapping,
  columnId: number,
  skipEmptyNested: boolean,
): [string | undefined, CaseId[]][] {
  const out: [string | undefined, CaseId[]][] = [];
  const root = kind.root(mapping).get(columnId);
  if (root) out.push([undefined, root]);
  for (const [path, cases] of kind.nested(mapping).get(columnId) ?? [])
    if (!skipEmptyNested || cases.length) out.push([path, cases]);
  return out;
}

function checkedTag(
  kind: EnumCaseKind,
  tag: number,
  messages: [string, string],
  path: string | undefined,
): number {
  if (tag > kind.maxTag) throw enumError(messages, path);
  return tag;
}

/** Physical-to-authored side of the enum interning boundary for one user cell. */
export function physicalToAuthoredEnumRemaps(
  node: NodeState,
  targetMapping: TablePhysicalMapping,
  columnId: number,
): EnumOccurrenceRemaps {
  const remaps = RecursiveEnumRemaps.default_();
  for (const kind of [SCALAR, PAYLOAD]) {
    for (const [path, targetCases] of occurrences(kind, targetMapping, columnId, false)) {
      // At bootstrap the target's own registry is the complete physical registry.
      const physicalCases = physicalEnumCasesOr(
        node,
        kind,
        targetMapping.tableId,
        columnId,
        path,
        targetCases,
      );
      const tags = physicalCases.map((identity) => {
        const tag = targetCases.findIndex((c) => caseIdEquals(c, identity));
        return tag < 0 ? undefined : checkedTag(kind, tag, kind.targetTagExhausted, path);
      });
      kind.install(remaps, path ?? "root", tags, physicalCases);
      if (path === undefined) kind.afterAuthoredRoot?.(remaps);
    }
  }
  return remaps;
}

/** Re-encode a source physical enum occurrence into a distinct copied column's physical registry. */
export function physicalCopyEnumRemaps(
  node: NodeState,
  sourceMapping: TablePhysicalMapping,
  sourceColumnId: number,
  targetMapping: TablePhysicalMapping,
  targetColumnId: number,
  sourceColumnType: ValueType,
  targetColumnType: ValueType,
): EnumOccurrenceRemaps {
  const remaps = RecursiveEnumRemaps.default_();
  for (const kind of [SCALAR, PAYLOAD]) {
    for (const [path, fallback] of occurrences(kind, sourceMapping, sourceColumnId, true)) {
      const sourceCases = physicalEnumCasesOr(
        node,
        kind,
        sourceMapping.tableId,
        sourceColumnId,
        path,
        fallback,
      );
      const targetFallback = occurrence(kind, targetMapping, targetColumnId, path);
      const targetCases = targetFallback
        ? physicalEnumCasesOr(
            node,
            kind,
            targetMapping.tableId,
            targetColumnId,
            path,
            targetFallback,
          )
        : [];
      const tags = sourceCases.map((_, ordinal) =>
        ordinal < targetCases.length
          ? checkedTag(kind, ordinal, kind.copiedTagExhausted, path)
          : undefined,
      );
      kind.install(
        remaps,
        path ?? "root",
        tags,
        sourceCases.map((_, ordinal) => targetCases[ordinal]),
      );
    }
  }
  bootstrapCopyEnumRemaps(sourceColumnType, targetColumnType, "root", remaps);
  return remaps;
}
