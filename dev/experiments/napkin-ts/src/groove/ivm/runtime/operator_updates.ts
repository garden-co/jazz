// Stateless source and unary-operator delta application.
import { type Bytes, compareBytes, concatBytes } from "../../../support/bytes.js";
import {
  EnumValue,
  IvmRuntimeError,
  OwnedRecord,
  applyIndexBy,
  durableIndexKeyPrefix,
  emptyDeltas,
  encodeProjectionFieldValue,
  indexRecordFromStorage,
  keyMatchesStaticScan,
  persistedIndexScanBounds,
  primaryKeyFieldIndices,
  primaryKeyValueBytes,
  projectBindingSourceDeltas,
  projectFieldValue,
  projectRecordRaw,
  scanBounds,
  scanMaxItems,
  scanReversed,
  splitVariantRecord,
  variantProjectionTargetName,
  type DatabaseSchema,
  type NodeId,
  type OrderedKvStorage,
  type RecordDelta,
  type RecordDeltas,
  type RecordDescriptor,
  type Retainer,
  type TableDelta,
  type TableSchema,
} from "../../../_extern/groove-ivm-ops.js";
import type { StorageRequestKey } from "./evaluation_session.js";
import type { EvaluationInputs } from "./evaluation_session.js";
import type { TerminalLineage } from "./evaluator.js";
import type { PreparedPipeline } from "./pipeline.js";

export interface NodeRuntimeMeta {
  retainers: Map<string, Retainer>;
  lastUsedTick: number;
  dependsOnContext?: boolean;
  /** Immutable graph classification, not a proof that runtime state is ready. */
  hasHydrationStateAncestor?: boolean;
  readinessFrontier?: NodeId[];
  terminalLineage?: TerminalLineage;
  inputSignature?: any;
  inputGeneration: number;
  /** `undefined` = not computed; `null` = computed, no raw projection. */
  rawProjectionFields?: any;
  pipeline?: PreparedPipeline;
  joinLeftFields?: string[];
  joinRightFields?: string[];
  joinOutput?: any;
  aggregateGroupFields?: string[];
}

type VariantProjections = Map<string, any>;
const variantKey = (table: string, target: any) => `${table}\0${JSON.stringify(target)}`;
const INDICES = "indices";

function tableOf(schema: DatabaseSchema, table: string): TableSchema {
  const t = schema.table(table);
  if (!t) throw new IvmRuntimeError("TableNotFound", table);
  return t;
}

const isEnumAbsent = (error: any) =>
  error?.kind === "EnumTagProjectionAbsent" || error?.kind === "EnumProjectionAbsent";

/** Namespace for stateless operator helpers. */
export const NodeState = {
  defaultMeta(): NodeRuntimeMeta {
    return { retainers: new Map(), lastUsedTick: 0, inputGeneration: 0 };
  },

  tableSourceRequest(input: any): StorageRequestKey | undefined {
    const family = input.table;
    if (!input.scan) return { kind: "ScanPrefix", family, prefix: new Uint8Array() };
    const bounds = scanBounds(input.scan);
    if (bounds.kind === "Prefix") {
      const maxItems = scanMaxItems(input.scan);
      return maxItems !== undefined
        ? {
            kind: "ScanPrefixLimit",
            family,
            prefix: bounds.prefix,
            maxItems,
            reversed: scanReversed(input.scan),
          }
        : { kind: "ScanPrefix", family, prefix: bounds.prefix };
    }
    return compareBytes(bounds.start, bounds.end) < 0
      ? { kind: "ScanRange", family, start: bounds.start, end: bounds.end }
      : undefined;
  },

  indexSourceRequest(input: any): StorageRequestKey | undefined {
    if (input.candidateFilter && !input.rowProjection)
      throw new IvmRuntimeError("UnsupportedIndexCandidateFilter");
    const { table, index } = input;
    const prefixOf = (indexId: any, scan: any, error: string): Bytes => {
      const bounds = persistedIndexScanBounds(indexId, scan);
      if (bounds.kind !== "Prefix") throw new IvmRuntimeError(error);
      return bounds.prefix;
    };
    if (input.rowProjection) {
      const filter = input.candidateFilter;
      if (filter) {
        if (input.intersections.length)
          throw new IvmRuntimeError("UnsupportedIndexCandidateFilter");
        const prefix = prefixOf(
          input.durableIds.index,
          input.scan,
          "UnsupportedIndexCandidateFilter",
        );
        const candidateId = input.durableIds.candidate;
        if (candidateId === undefined) throw new IvmRuntimeError("UnsupportedIndexCandidateFilter");
        return {
          kind: "IndexedRowsCandidateFilter",
          table,
          index,
          prefix,
          candidateTable: filter.table,
          candidateIndex: filter.index,
          candidatePrefix: prefixOf(candidateId, filter.scan, "UnsupportedIndexCandidateFilter"),
          sourceColumn: filter.sourceColumn,
          candidateColumn: filter.candidateColumn,
        };
      }
      if (input.intersections.length) {
        const prefix = prefixOf(
          input.durableIds.index,
          input.scan,
          "UnsupportedIndexIntersectionScan",
        );
        const intersections = input.intersections.map(
          ([name, scan]: [string, any], i: number): [string, Bytes] => [
            name,
            prefixOf(input.durableIds.intersections[i], scan, "UnsupportedIndexIntersectionScan"),
          ],
        );
        return { kind: "IndexedRowsIntersection", table, index, prefix, intersections };
      }
      const maxItems = scanMaxItems(input.scan);
      const bounds = persistedIndexScanBounds(input.durableIds.index, input.scan);
      if (bounds.kind === "Prefix") {
        return maxItems !== undefined
          ? {
              kind: "IndexedRowsPrefixLimit",
              table,
              index,
              prefix: bounds.prefix,
              maxItems,
              reversed: scanReversed(input.scan),
            }
          : { kind: "IndexedRowsPrefix", table, index, prefix: bounds.prefix };
      }
      return { kind: "IndexedRowsRange", table, index, start: bounds.start, end: bounds.end };
    }
    const bounds = persistedIndexScanBounds(input.durableIds.index, input.scan);
    if (bounds.kind === "Prefix") {
      const maxItems = scanMaxItems(input.scan);
      return maxItems !== undefined
        ? {
            kind: "ScanPrefixLimit",
            family: INDICES,
            prefix: bounds.prefix,
            maxItems,
            reversed: scanReversed(input.scan),
          }
        : { kind: "ScanPrefix", family: INDICES, prefix: bounds.prefix };
    }
    return compareBytes(bounds.start, bounds.end) < 0
      ? { kind: "ScanRange", family: INDICES, start: bounds.start, end: bounds.end }
      : undefined;
  },

  /** Resolve each variant layout once for the batch, before projecting rows. */
  groupSourceRows(table: TableSchema, rows: Iterable<Bytes>): TableDelta[] {
    const grouped = new Map<number, TableDelta>();
    for (const stored of rows) {
      const [variantTag, payload] = splitVariantRecord(stored);
      let group = grouped.get(variantTag);
      if (!group) {
        const descriptor = table.recordSchemaForVariant(variantTag);
        if (!descriptor)
          throw new IvmRuntimeError("UnknownTableVariant", {
            table: table.name,
            version: variantTag,
          });
        grouped.set(
          variantTag,
          (group = { table: table.name, variantTag, descriptor, deltas: [] }),
        );
      }
      group.deltas.push({ record: payload.slice(), weight: 1 });
    }
    return [...grouped.values()];
  },

  updateTableSourceFromInputs(
    input: any,
    schema: DatabaseSchema,
    variantProjections: VariantProjections,
    outputDesc: RecordDescriptor,
    inputs: EvaluationInputs,
  ): RecordDeltas {
    const request = NodeState.tableSourceRequest(input);
    if (!request) return emptyDeltas(outputDesc);
    const tableDeltas = NodeState.groupSourceRows(
      tableOf(schema, input.table),
      inputs.rows(request).map(([, stored]) => stored),
    );
    return NodeState.updateTableSource(input, schema, variantProjections, outputDesc, tableDeltas);
  },

  updateIndexSourceFromInputs(
    input: any,
    schema: DatabaseSchema,
    variantProjections: VariantProjections,
    outputDesc: RecordDescriptor,
    inputs: EvaluationInputs,
  ): RecordDeltas {
    const request = NodeState.indexSourceRequest(input);
    if (!request) return emptyDeltas(outputDesc);
    const rows = inputs.rows(request);
    if (input.rowProjection) {
      const tableDeltas = NodeState.groupSourceRows(
        tableOf(schema, input.table),
        rows.map(([, stored]) => stored),
      );
      return NodeState.updateTableSource(
        { table: input.table, scan: undefined, variantProjection: input.rowProjection },
        schema,
        variantProjections,
        outputDesc,
        tableDeltas,
      );
    }
    const prefixLen = durableIndexKeyPrefix(input.durableIds.index).length;
    return {
      descriptor: outputDesc,
      deltas: rows.map(([key, value]) => ({
        record: indexRecordFromStorage(prefixLen, key, value),
        weight: 1,
      })),
    };
  },

  updateTableSource(
    input: any,
    schema: DatabaseSchema,
    variantProjections: VariantProjections,
    outputDesc: RecordDescriptor,
    tableDeltas: TableDelta[],
  ): RecordDeltas {
    const tableSchema = tableOf(schema, input.table);
    const primaryKeyFields = input.scan ? primaryKeyFieldIndices(tableSchema, outputDesc) : [];
    const deltas: RecordDelta[] = [];
    const targetName = () =>
      input.variantProjection ? variantProjectionTargetName(input.variantProjection) : "";
    let projection: any;
    if (input.variantProjection) {
      projection = variantProjections.get(variantKey(input.table, input.variantProjection));
      if (!projection)
        throw new IvmRuntimeError("VariantProjectionNotFound", {
          table: input.table,
          target: targetName(),
        });
    }
    for (const delta of tableDeltas) {
      if (delta.table !== input.table) continue;
      let sourceDeltas: RecordDelta[];
      if (projection) {
        if (!projection.output.registryCompatibleWith(outputDesc))
          throw new IvmRuntimeError("GraphOutputMismatch");
        const projectionCase = projection.cases.get(delta.variantTag);
        const detail = { table: input.table, target: targetName(), version: delta.variantTag };
        if (!projectionCase) throw new IvmRuntimeError("VariantProjectionCaseNotFound", detail);
        if (!projectionCase.source.registryCompatibleWith(delta.descriptor)) {
          throw new IvmRuntimeError("VariantProjectionSourceMismatch", detail);
        }
        const source = { descriptor: delta.descriptor, deltas: delta.deltas };
        switch (projectionCase.kind) {
          case "Project":
            sourceDeltas = NodeState.updateMapProject(
              projectionCase.project,
              outputDesc,
              source,
              projectionCase.rawProjection,
              projectionCase.omitUnrepresentableEnumRows,
            ).deltas;
            break;
          case "Enum":
            sourceDeltas = NodeState.updateVariantEnumProject(
              projectionCase.tag,
              projectionCase.payload,
              projectionCase.project,
              outputDesc,
              source,
              projectionCase.rawProjection,
            ).deltas;
            break;
          default:
            continue;
        }
      } else {
        if (!delta.descriptor.registryCompatibleWith(outputDesc))
          throw new IvmRuntimeError("GraphOutputMismatch");
        sourceDeltas = delta.deltas;
      }
      for (const recordDelta of sourceDeltas) {
        if (input.scan) {
          const key = primaryKeyValueBytes(outputDesc, recordDelta.record, primaryKeyFields);
          if (!keyMatchesStaticScan(key, input.scan)) continue;
        }
        deltas.push(recordDelta);
      }
    }
    return { descriptor: outputDesc, deltas };
  },

  async updateIndexSource(
    input: any,
    schema: DatabaseSchema,
    variantProjections: VariantProjections,
    outputDesc: RecordDescriptor,
    tableDeltas: TableDelta[],
    storage: OrderedKvStorage | undefined,
    evalMode: string,
  ): Promise<RecordDeltas> {
    if (evalMode === "Hydrate") {
      if (!storage) throw new IvmRuntimeError("StorageUnavailable");
      const maxItems = scanMaxItems(input.scan);
      const direction = scanReversed(input.scan) ? "Reverse" : "Forward";
      const bounds = persistedIndexScanBounds(input.durableIds.index, input.scan);
      if (bounds.kind === "Range" && compareBytes(bounds.start, bounds.end) >= 0)
        return emptyDeltas(outputDesc);
      const scan = await storage.scan({
        cf: INDICES,
        bounds: bounds.kind === "Prefix" ? { kind: "Prefix", prefix: bounds.prefix } : bounds,
        direction,
        maxItems,
      });
      const deltas: RecordDelta[] = [];
      const prefixLen = durableIndexKeyPrefix(input.durableIds.index).length;
      for (let batch = await scan.nextBatch(); batch; batch = await scan.nextBatch()) {
        for (const [key, value] of batch)
          deltas.push({ record: indexRecordFromStorage(prefixLen, key, value), weight: 1 });
      }
      return { descriptor: outputDesc, deltas };
    }
    const indexBy = {
      keyExpressions: [],
      valueExpressions: [],
      explicitIndex: undefined,
      keyFields: input.keyFields,
      valueFields: input.valueFields,
      unique: input.unique,
      appendValueToKey: input.appendValueToKey,
      storeValue: input.storeValue,
      scan: input.scan,
    };
    const sourceOp = {
      table: input.table,
      scan: undefined,
      variantProjection: input.variantProjection,
    };
    if (input.rowProjection) {
      const matching: RecordDelta[] = [];
      for (const tableDelta of tableDeltas) {
        if (tableDelta.table !== input.table) continue;
        for (const recordDelta of tableDelta.deltas) {
          const one = [{ ...tableDelta, deltas: [recordDelta] }];
          const indexInput = NodeState.updateTableSource(
            sourceOp,
            schema,
            variantProjections,
            input.inputDescriptor,
            one,
          );
          if (applyIndexBy(indexBy, input.inputDescriptor, indexInput.deltas).length === 0)
            continue;
          matching.push(
            ...NodeState.updateTableSource(
              { table: input.table, scan: undefined, variantProjection: input.rowProjection },
              schema,
              variantProjections,
              outputDesc,
              one,
            ).deltas,
          );
        }
      }
      return { descriptor: outputDesc, deltas: matching };
    }
    const source = NodeState.updateTableSource(
      sourceOp,
      schema,
      variantProjections,
      input.inputDescriptor,
      tableDeltas,
    );
    return {
      descriptor: outputDesc,
      deltas: applyIndexBy(indexBy, input.inputDescriptor, source.deltas),
    };
  },

  updateBindingSource(
    input: any,
    outputDesc: RecordDescriptor,
    bindingDeltas: any[],
    bindingSnapshots: any,
    mode: string,
  ): RecordDeltas {
    if (mode === "Replace") {
      const snapshot = bindingSnapshots.get(input.key);
      return snapshot ? projectBindingSourceDeltas(snapshot, outputDesc) : emptyDeltas(outputDesc);
    }
    const deltas: RecordDelta[] = [];
    for (const delta of bindingDeltas) {
      if (!delta.key.equals(input.key)) continue;
      deltas.push(
        ...projectBindingSourceDeltas(
          { descriptor: delta.descriptor, deltas: delta.deltas },
          outputDesc,
        ).deltas,
      );
    }
    return { descriptor: outputDesc, deltas };
  },

  updateFilter(filter: any, outputDesc: RecordDescriptor, input: RecordDeltas): RecordDeltas {
    const deltas = input.deltas.filter((delta) =>
      filter.predicate.matches(delta, input.descriptor, filter.comparison),
    );
    return { descriptor: outputDesc, deltas };
  },

  updateMapProject(
    project: any,
    outputDesc: RecordDescriptor,
    input: RecordDeltas,
    rawProjection: any,
    omitUnrepresentableEnumRows: boolean,
  ): RecordDeltas {
    return NodeState.updateMapProjectSlice(
      project,
      outputDesc,
      input.descriptor,
      input.deltas,
      rawProjection,
      omitUnrepresentableEnumRows,
    );
  },

  updateMapProjectSlice(
    project: any,
    outputDesc: RecordDescriptor,
    inputDesc: RecordDescriptor,
    input: RecordDelta[],
    rawProjection: any,
    omitUnrepresentableEnumRows: boolean,
  ): RecordDeltas {
    if (rawProjection?.reusesInput) return { descriptor: outputDesc, deltas: input.slice() };
    const omit =
      omitUnrepresentableEnumRows ||
      project.expressions.some(
        (e: any) => e.expression.kind === "RecursiveEnumRemap" && e.expression.omitUnrepresentable,
      );
    const deltas: RecordDelta[] = [];
    for (const delta of input) {
      const record = NodeState.projectRow(
        project,
        outputDesc,
        inputDesc,
        delta.record,
        rawProjection,
        omit,
      );
      if (record) deltas.push({ record, weight: delta.weight });
    }
    return { descriptor: outputDesc, deltas };
  },

  projectRow(
    project: any,
    outputDesc: RecordDescriptor,
    inputDesc: RecordDescriptor,
    raw: Bytes,
    rawProjection: any,
    omitUnrepresentableEnumRows: boolean,
  ): Bytes | undefined {
    try {
      if (rawProjection) {
        const parts: Bytes[] = [];
        outputDesc.projectRawFieldsInto(
          inputDesc,
          raw,
          rawProjection.fields,
          parts,
          (index: number, out: Bytes[]) => {
            const value = projectFieldValue(
              project.expressions[index],
              index,
              outputDesc,
              inputDesc,
              raw,
            );
            out.push(encodeProjectionFieldValue(outputDesc, index, value));
          },
        );
        return concatBytes(...parts);
      }
      return projectRecordRaw(project.expressions, project.mapping, outputDesc, inputDesc, raw);
    } catch (error) {
      if (isEnumAbsent(error) && omitUnrepresentableEnumRows) return undefined;
      throw error;
    }
  },

  updateVariantEnumProject(
    tag: number,
    payloadDesc: RecordDescriptor,
    project: any,
    outputDesc: RecordDescriptor,
    input: RecordDeltas,
    rawProjection: any,
  ): RecordDeltas {
    const payloads = NodeState.updateMapProject(project, payloadDesc, input, rawProjection, false);
    const deltas = payloads.deltas.map((payload) => ({
      record: outputDesc.create([
        { kind: "Enum", value: new EnumValue(tag, new OwnedRecord(payload.record, payloadDesc)) },
      ]),
      weight: payload.weight,
    }));
    return { descriptor: outputDesc, deltas };
  },

  updateVariantProject(
    variantProject: any,
    outputDesc: RecordDescriptor,
    input: RecordDeltas,
  ): RecordDeltas {
    const deltas: RecordDelta[] = [];
    for (const delta of input.deltas) {
      const value = input.descriptor.borrow(delta.record).getIdx(variantProject.fieldIdx);
      if (value.kind !== "Enum")
        throw new IvmRuntimeError("VariantProjectFieldTypeMismatch", {
          field: variantProject.field,
        });
      if (value.value.tag() !== variantProject.tag) continue;
      if (!value.value.record().descriptor().equals(outputDesc)) {
        throw new IvmRuntimeError("VariantProjectPayloadMismatch", { field: variantProject.field });
      }
      deltas.push({ record: value.value.record().raw().slice(), weight: delta.weight });
    }
    return { descriptor: outputDesc, deltas };
  },

  updateUnwrapNullable(
    unwrap: any,
    outputDesc: RecordDescriptor,
    input: RecordDeltas,
  ): RecordDeltas {
    const deltas: RecordDelta[] = [];
    const scratch = {};
    for (const delta of input.deltas) {
      let record: Bytes | undefined;
      try {
        record = outputDesc.unwrapNullableField(
          input.descriptor,
          delta.record,
          unwrap.fieldIdx,
          scratch,
        );
      } catch (error) {
        throw new IvmRuntimeError("RecordEncoding", error);
      }
      if (record) deltas.push({ record, weight: delta.weight });
    }
    return { descriptor: outputDesc, deltas };
  },

  updateUnnest(unnest: any, outputDesc: RecordDescriptor, input: RecordDeltas): RecordDeltas {
    const deltas: RecordDelta[] = [];
    for (const delta of input.deltas) {
      const values = input.descriptor.borrow(delta.record).toValues();
      const value = values[unnest.arrayFieldIdx];
      if (value === undefined)
        throw new IvmRuntimeError("GraphFieldIndexOutOfBounds", unnest.arrayFieldIdx);
      if (value.kind !== "Array") throw new IvmRuntimeError("UnsupportedOperator");
      for (const element of value.value) {
        deltas.push({ record: outputDesc.create([...values, element]), weight: delta.weight });
      }
    }
    return { descriptor: outputDesc, deltas };
  },

  updateUnion(outputDesc: RecordDescriptor, inputs: RecordDeltas[]): RecordDeltas {
    const deltas: RecordDelta[] = [];
    for (const input of inputs) {
      if (input.deltas.length === 0) continue;
      if (!outputDesc.registryCompatibleWith(input.descriptor))
        throw new IvmRuntimeError("GraphOutputMismatch");
      deltas.push(...input.deltas);
    }
    return { descriptor: outputDesc, deltas };
  },

  updateIndexBy(indexBy: any, outputDesc: RecordDescriptor, input: RecordDeltas): RecordDeltas {
    return {
      descriptor: outputDesc,
      deltas: applyIndexBy(indexBy, input.descriptor, input.deltas),
    };
  },
};
