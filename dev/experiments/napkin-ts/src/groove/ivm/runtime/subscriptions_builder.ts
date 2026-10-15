// Graph-builder structural identity and auto-direct literal-filter lifting
// (split out of subscriptions.rs).
import { IvmRuntimeError } from "./index.js";
import type { IvmRuntime } from "./index.js";
import {
  type GraphBuilder,
  type RecordDescriptor,
  type FieldRef,
  type ProjectField,
  type LiteralValue,
  type Value,
  type CollectByBuilder,
  type CollectBySlotBuilder,
  GB,
  PE,
  PF,
  FR,
  newRecordDescriptor,
  builderPostorder,
  stableKey,
  hashString64,
  resolveFieldRef,
  fieldRefName,
  literalValueType,
} from "../../../_extern/groove-ivm-tick.js";
import { inferBuilderOutput } from "./subscriptions.js";

/** Fields of a builder node that hold child builders. */
const CHILD_KEYS = [
  "input",
  "left",
  "right",
  "seed",
  "step",
  "stepWitness",
  "recursive",
  "keys",
  "inputs",
] as const;

function isChildKey(node: GraphBuilder, key: string): boolean {
  if (key === "keys") return node.kind === "Lookup";
  if (key === "inputs") return node.kind === "Union" || node.kind === "TypedTemplate";
  return (CHILD_KEYS as readonly string[]).includes(key);
}

function builderChildren(node: GraphBuilder): GraphBuilder[] {
  const out: GraphBuilder[] = [];
  for (const key of Object.keys(node)) {
    if (!isChildKey(node, key)) continue;
    const v = (node as any)[key];
    if (Array.isArray(v)) out.push(...v);
    else if (v) out.push(v);
  }
  return out;
}

function builderScalars(node: GraphBuilder): string {
  const scalars: Record<string, unknown> = {};
  for (const key of Object.keys(node).sort()) {
    if (isChildKey(node, key)) {
      const v = (node as any)[key];
      // Child presence/arity is part of identity; child contents are not.
      scalars[key] = Array.isArray(v) ? v.length : v ? 1 : 0;
    } else scalars[key] = (node as any)[key];
  }
  return stableKey(scalars);
}

/** Bounded structural fingerprint for auto-direct family lookup. */
export function graphBuilderFingerprint(graph: GraphBuilder): bigint {
  const hashes = new Map<GraphBuilder, bigint>();
  for (const node of builderPostorder(graph)) {
    if (hashes.has(node)) continue;
    let text = builderScalars(node);
    for (const child of builderChildren(node)) {
      const h = hashes.get(child);
      if (h === undefined) throw new Error("postorder visits graph children before their parent");
      text += `|${h}`;
    }
    hashes.set(node, hashString64(text));
  }
  const root = hashes.get(graph);
  if (root === undefined) throw new Error("postorder includes the graph root");
  return root;
}

export function graphBuildersEqual(left: GraphBuilder, right: GraphBuilder): boolean {
  return graphBuildersEqualWith(left, right, () => undefined);
}

/** Structural equality with a hook consulted before each pair. */
export function graphBuildersEqualWith(
  left: GraphBuilder,
  right: GraphBuilder,
  hook: (a: GraphBuilder, b: GraphBuilder) => boolean | undefined,
): boolean {
  const pending: [GraphBuilder, GraphBuilder][] = [[left, right]];
  while (pending.length) {
    const [a, b] = pending.pop()!;
    const hooked = hook(a, b);
    if (hooked !== undefined) {
      if (!hooked) return false;
      continue;
    }
    if (a.kind !== b.kind || builderScalars(a) !== builderScalars(b)) return false;
    const ac = builderChildren(a);
    const bc = builderChildren(b);
    if (ac.length !== bc.length) return false;
    for (let i = 0; i < ac.length; i++) pending.push([ac[i], bc[i]]);
  }
  return true;
}

export function countBuilderNodes(graph: GraphBuilder): number {
  return builderPostorder(graph).length;
}

export function builderContainsBindingSource(graph: GraphBuilder): boolean {
  return builderPostorder(graph).some((node) => node.kind === "BindingSource");
}

export function descriptorFieldNames(descriptor: RecordDescriptor): string[] {
  return descriptor.fields().map((field: any) => {
    if (field.name == null) throw IvmRuntimeError.of("GraphFieldNotFound", "<unnamed>");
    return field.name as string;
  });
}

export function validatePublicOutputFields(
  source: RecordDescriptor,
  publicOutput: RecordDescriptor,
): void {
  for (const field of publicOutput.fields()) {
    const name = field.logicalName() ?? field.name;
    if (name == null) throw IvmRuntimeError.of("GraphFieldNotFound", "<unnamed>");
    let index: number | undefined = field.identity
      ? source.fieldIndexByIdentity(field.identity)
      : undefined;
    index ??= source.fieldIndex(name);
    if (index === undefined && field.name != null) {
      const i = source.fields().findIndex((c: any) => c.name === field.name);
      if (i >= 0) index = i;
    }
    if (index === undefined) throw IvmRuntimeError.of("GraphFieldNotFound", name);
    const sourceField = source.fields()[index];
    if (!sourceField) throw IvmRuntimeError.of("GraphFieldIndexOutOfBounds", index);
    if (!sourceField.valueType || !registryCompatible(sourceField.valueType, field.valueType)) {
      throw IvmRuntimeError.of("GraphOutputMismatch");
    }
  }
}
const registryCompatible = (a: any, b: any): boolean =>
  a.registryCompatibleWith?.(b) ?? stableKey(a) === stableKey(b);

export function routePredicate(field: string, value: Value) {
  return value.kind === "Nullable" && value.v === null ? PE.isNull(field) : PE.eq(field, value);
}

export function boundRoutedMultisinkGraph(
  terminal: {
    graph: GraphBuilder;
    routeFields: string[];
    routeValueIndices: number[];
    publicFields: string[];
  },
  bindingValues: Value[],
  output: RecordDescriptor,
): GraphBuilder {
  const predicates = terminal.routeFields.map((field, i) =>
    routePredicate(field, bindingValues[terminal.routeValueIndices[i]]),
  );
  const predicate =
    predicates.length === 0
      ? undefined
      : predicates.length === 1
        ? predicates[0]
        : PE.canonicalize(PE.and(predicates));
  const graph = terminal.graph;
  if (graph.kind === "CollectBy") {
    // CollectBy is terminal-only: route its flat input and drop hidden route
    // columns from the collector's own projection and keys.
    const collect: CollectByBuilder = structuredClone(graph.collect);
    collect.parentFields = collect.parentFields.filter((f: any) =>
      terminal.publicFields.includes(f.outputName),
    );
    collect.tupleFields = collect.tupleFields.filter((f: any) =>
      terminal.publicFields.includes(f.outputName),
    );
    stripBoundRouteFieldsFromCollect(collect, terminal.routeFields);
    const input = predicate ? GB.filter(graph.input, predicate) : graph.input;
    return { kind: "CollectBy", input, collect };
  }
  const filtered = predicate ? GB.filter(graph, predicate) : graph;
  const fields = terminal.publicFields.map((name): ProjectField => {
    const source = FR.storedName(name);
    const index = resolveFieldRef(output, source);
    const field = output.fields()[index];
    return {
      expression: { kind: "Field", source },
      outputName: name,
      outputIdentity: field.identity ?? { kind: "Name", name },
    };
  });
  return GB.projectFields(filtered, fields);
}

function stripBoundRouteFieldsFromCollect(collect: CollectByBuilder, routeFields: string[]): void {
  const isRoute = (field: FieldRef) => field.kind === "Name" && routeFields.includes(field.name);
  collect.groupCols = collect.groupCols.filter((f: FieldRef) => !isRoute(f));
  collect.tieCols = collect.tieCols.filter((f: FieldRef) => !isRoute(f));
  collect.orderCols = collect.orderCols.filter((o: any) => !isRoute(o.field));
  for (const slot of collect.slots) stripBoundRouteFieldsFromSlot(slot, isRoute);
}

function stripBoundRouteFieldsFromSlot(
  slot: CollectBySlotBuilder,
  isRoute: (f: FieldRef) => boolean,
): void {
  slot.groupCols = slot.groupCols.filter((f: FieldRef) => !isRoute(f));
  slot.ownerKeyCols = slot.ownerKeyCols.filter((f: FieldRef) => !isRoute(f));
  slot.tieCols = slot.tieCols.filter((f: FieldRef) => !isRoute(f));
  slot.orderCols = slot.orderCols.filter((o: any) => !isRoute(o.field));
  for (const child of slot.slots) stripBoundRouteFieldsFromSlot(child, isRoute);
}

// ---- auto-direct literal-filter lifting ----

export type LiftedLiteralFilter = { graph: GraphBuilder; value: LiteralValue };

const AUTO_DIRECT_BINDING_PREFIX = "\0groove.auto_direct.binding.";

export function autoDirectBindingField(
  graph: GraphBuilder,
  output: RecordDescriptor,
  runtime: IvmRuntime,
): string {
  const occupied = new Set<string>();
  for (const node of builderPostorder(graph)) {
    for (const field of inferBuilderOutput(runtime, node).fields())
      if (field.name != null) occupied.add(field.name);
  }
  for (const field of output.fields()) if (field.name != null) occupied.add(field.name);
  for (let index = 0; ; index++) {
    const candidate = `${AUTO_DIRECT_BINDING_PREFIX}${index}`;
    if (!occupied.has(candidate)) return candidate;
  }
}

export function liftLiteralFilter(
  runtime: IvmRuntime,
  graph: GraphBuilder,
  bindingField: string,
): LiftedLiteralFilter | undefined {
  const lifted = new Map<GraphBuilder, LiftedLiteralFilter | undefined>();
  for (const node of builderPostorder(graph)) {
    if (lifted.has(node)) continue;
    lifted.set(node, liftLiteralFilterNode(runtime, node, bindingField, lifted));
  }
  if (!lifted.has(graph)) throw new Error("postorder includes the graph root");
  return lifted.get(graph);
}

/** Rewrite a projection field whose source moved to `mapSource(source)`. */
function remapProjectField(
  field: ProjectField,
  mapSource: (source: FieldRef) => string,
): ProjectField {
  const e: any = field.expression;
  const out = field.outputName;
  switch (e.kind) {
    case "TemplateArgument":
    case "Literal":
    case "TypedLiteral":
    case "Null":
      return field;
    case "RecordField":
      return {
        ...field,
        expression: { kind: "RecordField", source: FR.name(mapSource(e.source)), path: e.path },
      };
    case "Field":
      return PF.renamed(mapSource(e.source), out);
    case "Nullable":
      return PF.nullable(mapSource(e.source), out);
    case "NullableFlat":
      return PF.nullableFlat(mapSource(e.source), out);
    case "EnumTagRemap":
      return PF.enumTagRemap(mapSource(e.source), out, e.tags);
    case "EnumRemap":
      return PF.enumRemap(mapSource(e.source), out, e.tags);
    case "RecursiveEnumRemap":
      return e.omitUnrepresentable
        ? PF.recursiveEnumRemapOmittingUnrepresentable(mapSource(e.source), out, e.target, e.remaps)
        : PF.recursiveEnumRemap(mapSource(e.source), out, e.target, e.remaps);
  }
  throw new Error(`unknown projection ${e.kind}`);
}

function liftLiteralFilterNode(
  runtime: IvmRuntime,
  graph: GraphBuilder,
  bindingField: string,
  liftedChildren: Map<GraphBuilder, LiftedLiteralFilter | undefined>,
): LiftedLiteralFilter | undefined {
  const child = (c: GraphBuilder) => {
    if (!liftedChildren.has(c))
      throw new Error("postorder visits graph children before their parent");
    return liftedChildren.get(c);
  };
  const withInput = (
    lifted: LiftedLiteralFilter | undefined,
    make: (input: GraphBuilder) => GraphBuilder,
  ) => (lifted ? { graph: make(lifted.graph), value: lifted.value } : undefined);
  const groupedBy = (lifted: LiftedLiteralFilter | undefined) =>
    withInput(lifted, (input) => ({
      ...(graph as any),
      input,
      groupCols: [...(graph as any).groupCols, FR.name(bindingField)],
    }));
  switch (graph.kind) {
    case "Filter": {
      const predicate = graph.predicate;
      if (predicate.kind === "Eq") {
        const joined = literalFilterBindingJoin(
          graph.input,
          predicate.field,
          predicate.value,
          bindingField,
        );
        const fields = inferBuilderOutput(runtime, graph.input)
          .fields()
          .filter((f: any) => f.name != null)
          .map((f: any) => PF.renamed(`left.${f.name}`, f.name));
        fields.push(PF.renamed(`right.${bindingField}`, bindingField));
        return { graph: GB.projectFields(joined, fields), value: predicate.value };
      }
      return withInput(child(graph.input), (input) => ({ ...graph, input }));
    }
    case "Project": {
      const input = graph.input;
      if (input.kind === "Join") {
        for (const side of ["left", "right"] as const) {
          const lifted = child(input[side]);
          if (!lifted) continue;
          const joined: GraphBuilder = { ...input, [side]: lifted.graph };
          const fields = projectFieldsAgainstRewrittenInput(runtime, input, joined, graph.fields);
          appendBindingProjectField(
            fields,
            bindingField,
            bindingProjectSource(joined, bindingField),
          );
          return { graph: GB.projectFields(joined, fields), value: lifted.value };
        }
      }
      if (input.kind === "Filter" && input.predicate.kind === "Eq") {
        const { field, value } = input.predicate;
        const joined = literalFilterBindingJoin(input.input, field, value, bindingField);
        const inputOutput = inferBuilderOutput(runtime, input.input);
        const fields = graph.fields.map((f: ProjectField) =>
          remapProjectField(f, (source) => projectSourceFromJoinedFilterInput(inputOutput, source)),
        );
        fields.push(PF.renamed(`right.${bindingField}`, bindingField));
        return { graph: GB.projectFields(joined, fields), value };
      }
      const lifted = child(input);
      if (!lifted) return undefined;
      const fields = graph.fields.slice();
      appendBindingProjectField(
        fields,
        bindingField,
        bindingProjectSource(lifted.graph, bindingField),
      );
      return { graph: { kind: "Project", input: lifted.graph, fields }, value: lifted.value };
    }
    case "Join": {
      for (const side of ["left", "right"] as const) {
        const lifted = child(graph[side]);
        if (!lifted) continue;
        const originalOutput = inferBuilderOutput(runtime, graph);
        const joined: GraphBuilder = { ...graph, [side]: lifted.graph };
        return {
          graph: projectToOutputWithBinding(runtime, joined, originalOutput, bindingField),
          value: lifted.value,
        };
      }
      return undefined;
    }
    case "AntiJoin":
    case "SemiJoin":
      return withInput(child(graph.left), (left) => ({ ...graph, left }));
    case "UnwrapNullable":
    case "Unnest":
      return withInput(child(graph.input), (input) => ({ ...graph, input }));
    case "ArgMaxBy":
    case "ArgMinBy":
    case "TopBy":
    case "Aggregate":
      return groupedBy(child(graph.input));
    default:
      // Recursive, Union, VariantProject, CollectBy, templates, lookups and
      // sources are not lifted through.
      return undefined;
  }
}

function literalFilterBindingJoin(
  input: GraphBuilder,
  field: string,
  value: LiteralValue,
  bindingField: string,
): GraphBuilder {
  const valueType = literalValueType(value);
  if (!valueType) throw IvmRuntimeError.of("UnsupportedOperator");
  const binding = GB.bindingSource(
    "__auto_direct_shape",
    newRecordDescriptor([[bindingField, valueType]]),
  );
  return GB.join(input, binding, [field], [bindingField]);
}

function projectSourceFromJoinedFilterInput(
  inputOutput: RecordDescriptor,
  source: FieldRef,
): string {
  return `left.${fieldRefName(inputOutput, source)}`;
}

function rewrittenProjectionSource(
  original: RecordDescriptor,
  rewritten: RecordDescriptor,
  fieldRef: FieldRef,
): string {
  const originalIdx = resolveFieldRef(original, fieldRef);
  const originalField = original.fields()[originalIdx];
  if (!originalField) throw IvmRuntimeError.of("GraphFieldIndexOutOfBounds", originalIdx);
  let idx: number | undefined = originalField.identity
    ? rewritten.fieldIndexByIdentity(originalField.identity)
    : undefined;
  const logical = originalField.logicalName();
  if (idx === undefined && logical != null) idx = rewritten.fieldIndex(logical);
  if (idx === undefined && originalField.name != null) {
    const i = rewritten.fields().findIndex((c: any) => c.name === originalField.name);
    if (i >= 0) idx = i;
  }
  if (idx === undefined) {
    throw IvmRuntimeError.of("GraphFieldNotFound", logical ?? originalField.name ?? "<unnamed>");
  }
  const rewrittenField = rewritten.fields()[idx];
  if (!rewrittenField) throw IvmRuntimeError.of("GraphFieldIndexOutOfBounds", idx);
  return rewrittenField.logicalName() ?? rewrittenField.name ?? "<unnamed>";
}

function projectFieldsAgainstRewrittenInput(
  runtime: IvmRuntime,
  originalInput: GraphBuilder,
  rewrittenInput: GraphBuilder,
  fields: ProjectField[],
): ProjectField[] {
  const original = inferBuilderOutput(runtime, originalInput);
  const rewritten = inferBuilderOutput(runtime, rewrittenInput);
  return fields.map((field) => {
    const e: any = field.expression;
    const map = (source: FieldRef) => rewrittenProjectionSource(original, rewritten, source);
    switch (e.kind) {
      case "Field":
      case "Nullable":
      case "NullableFlat":
        // Plain field references keep their declared output identity.
        return { ...field, expression: { kind: e.kind, source: FR.name(map(e.source)) } };
      default:
        return remapProjectField(field, map);
    }
  });
}

function projectToOutputWithBinding(
  runtime: IvmRuntime,
  graph: GraphBuilder,
  originalOutput: RecordDescriptor,
  bindingField: string,
): GraphBuilder {
  const liftedOutput = inferBuilderOutput(runtime, graph);
  const fields: ProjectField[] = originalOutput.fields().map((field: any) => {
    const name = field.name;
    if (name == null) throw IvmRuntimeError.of("GraphFieldNotFound", "<unnamed>");
    const source = rewrittenProjectionSource(originalOutput, liftedOutput, FR.name(name));
    return {
      expression: { kind: "Field", source: FR.name(source) },
      outputName: name,
      outputIdentity: field.identity ?? { kind: "Name", name },
    };
  });
  appendBindingProjectField(fields, bindingField, bindingProjectSource(graph, bindingField));
  return { kind: "Project", input: graph, fields };
}

function appendBindingProjectField(
  fields: ProjectField[],
  bindingField: string,
  source: string,
): void {
  if (!fields.some((f) => f.outputName === bindingField))
    fields.push(PF.renamed(source, bindingField));
}

function bindingProjectSource(input: GraphBuilder, bindingField: string): string {
  if (input.kind === "Join" || input.kind === "SemiJoin" || input.kind === "AntiJoin") {
    if (graphOutputsBinding(input.left, bindingField)) return `left.${bindingField}`;
    if (graphOutputsBinding(input.right, bindingField)) return `right.${bindingField}`;
  }
  return bindingField;
}

function graphOutputsBinding(graph: GraphBuilder, bindingField: string): boolean {
  const outputs = new Map<GraphBuilder, boolean>();
  const child = (c: GraphBuilder) => {
    const v = outputs.get(c);
    if (v === undefined) throw new Error("postorder visits graph children before their parent");
    return v;
  };
  for (const node of builderPostorder(graph)) {
    if (outputs.has(node)) continue;
    let output: boolean;
    switch (node.kind) {
      case "TypedTemplate":
        output = node.program.output.fieldIndex(bindingField) !== undefined;
        break;
      case "TemplateInput":
      case "BindingSource":
      case "FrontierSource":
      case "InputSource":
      case "InlineRecords":
        output = node.output.fieldIndex(bindingField) !== undefined;
        break;
      case "Project":
        output = node.fields.some((f: ProjectField) => f.outputName === bindingField);
        break;
      case "StreamingChecksum":
        output =
          node.outputField === bindingField ||
          (!(node.field.kind === "Name" && node.field.name === bindingField) && child(node.input));
        break;
      case "Filter":
      case "UnwrapNullable":
      case "Unnest":
      case "ArgMaxBy":
      case "ArgMinBy":
      case "TopBy":
      case "CollectBy":
      case "Aggregate":
        output = child(node.input);
        break;
      case "Recursive":
        output = child(node.seed);
        break;
      case "RecursiveStepWitness":
        output =
          node.recursive.kind === "Recursive" && node.recursive.stepWitness
            ? child(node.recursive.stepWitness)
            : false;
        break;
      case "Join":
      case "SemiJoin":
      case "AntiJoin":
        output = child(node.left) || child(node.right);
        break;
      case "Union":
        output = node.inputs.some((i: GraphBuilder) => child(i));
        break;
      default:
        output = false;
    }
    outputs.set(node, output);
  }
  const root = outputs.get(graph);
  if (root === undefined) throw new Error("postorder includes the graph root");
  return root;
}

/** Rewrite every binding source to `shape`, iteratively in postorder. */
export function replaceBindingShape(graph: GraphBuilder, shape: string): GraphBuilder {
  const rewritten = new Map<GraphBuilder, GraphBuilder>();
  const child = (c: GraphBuilder) => {
    const r = rewritten.get(c);
    if (!r) throw new Error("postorder rewrites every graph child before its parent");
    return r;
  };
  const REWRITE_THROUGH = new Set([
    "Recursive",
    "RecursiveStepWitness",
    "Filter",
    "Project",
    "UnwrapNullable",
    "Unnest",
    "ArgMaxBy",
    "ArgMinBy",
    "TopBy",
    "Aggregate",
    "Union",
    "Join",
    "SemiJoin",
    "AntiJoin",
  ]);
  for (const node of builderPostorder(graph)) {
    if (rewritten.has(node)) continue;
    let replacement: GraphBuilder;
    if (node.kind === "BindingSource") replacement = GB.bindingSource(shape, node.output);
    else if (REWRITE_THROUGH.has(node.kind)) {
      // Operators auto-direct does not rewrite through keep their children.
      const next: any = { ...node };
      for (const key of Object.keys(node)) {
        if (!isChildKey(node, key)) continue;
        const v = (node as any)[key];
        next[key] = Array.isArray(v) ? v.map(child) : v ? child(v) : v;
      }
      replacement = next;
    } else replacement = node;
    rewritten.set(node, replacement);
  }
  const root = rewritten.get(graph);
  if (!root) throw new Error("postorder includes the graph root");
  return root;
}
