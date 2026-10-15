// Immutable operator topology with explicit, typed, instance-owned inputs.
import type { RecordDescriptor } from "../../_extern/groove-ivm-plan.js";
import { JazzError } from "../../support/runtime.js";
import {
  type GraphBuilder,
  type NodeDescriptor,
  graphInputs,
  graphMapInputs,
  graphPostorder,
} from "./graph.js";
import { type PredicateExpr, predicateAcceptsTemplateArgument, structEq } from "./op_types.js";
import {
  TemplateArguments,
  TemplateScalarArgument,
  bindProjectExpression,
  bindTemplateArguments,
} from "./template/arguments.js";
import { TypedGraphTemplateCache } from "./runtime/typed_template.js";

export { matchTemplateSources, splitTemplateSources } from "./template/sources.js";
export { TemplateScalarArgument, bindTemplateArguments, TypedGraphTemplateCache };

export type TemplateNodeRef = { kind: "Input"; index: number } | { kind: "Node"; index: number };

export interface TemplateNode {
  descriptor: NodeDescriptor;
  inputs: TemplateNodeRef[];
  predicate?: number;
}

/** One terminal's program plus the forest argument slots it consumes. */
export interface TemplateSlice {
  inputs: number[];
  predicates: number[];
}

export type CompiledForest = [TypedGraphTemplate, TemplateSlice][];

/** A typed, source-independent installation program. Process-local compiler state. */
export class TypedGraphTemplate {
  constructor(
    readonly identity: number,
    readonly nodes: TemplateNode[],
    readonly inputs: RecordDescriptor[],
    readonly output: RecordDescriptor,
    readonly root: TemplateNodeRef,
    readonly ordering: TemplateNodeRef | undefined,
    readonly terminal: boolean,
    readonly fallback: GraphBuilder,
    readonly predicateMarkers: PredicateExpr[],
  ) {}

  structKey(): string {
    return `tpl${this.identity}`;
  }

  outputDescriptor(): RecordDescriptor {
    return this.output;
  }

  bindDeclarative(inputs: GraphBuilder[], predicates: PredicateExpr[]): GraphBuilder {
    return this.bindDeclarativeWithArguments(inputs, predicates, []);
  }

  /** Reconstruct the declarative equivalent for diagnostics and compiler fallbacks. */
  bindDeclarativeWithArguments(
    inputs: GraphBuilder[],
    predicates: PredicateExpr[],
    scalars: TemplateScalarArgument[],
  ): GraphBuilder {
    if (predicates.length !== this.predicateMarkers.length)
      throw TemplateBindingError.predicateArity();
    this.predicateMarkers.forEach((marker, slot) => {
      if (!predicateAcceptsTemplateArgument(marker, predicates[slot]))
        throw TemplateBindingError.predicateArgument(slot);
    });
    const rewritten = new Map<GraphBuilder, GraphBuilder>();
    for (const node of graphPostorder(this.fallback)) {
      let bound = graphMapInputs(node, (c) => rewritten.get(c)!);
      if (bound.kind === "Filter") {
        const predicate = bound.predicate;
        const slot = this.predicateMarkers.findIndex((m) => structEq(m, predicate));
        if (slot >= 0) bound = { ...bound, predicate: predicates[slot] };
      }
      if (bound.kind === "Project") {
        bound = {
          ...bound,
          fields: bound.fields.map((f) => ({
            ...f,
            expression: bindProjectExpression(f.expression, scalars),
          })),
        };
      }
      rewritten.set(node, bound);
    }
    const fallback = rewritten.get(this.fallback)!;
    const typed = inputs.map((graph, i) =>
      TemplateGraphInput.withOutputContract(graph, this.inputs[i]),
    );
    return bindTemplateGraphsInner([fallback], typed, true)[0];
  }
}

/** Compile source-slot graphs once into typed operator definitions. */
export function compileTemplateGraphs(graphs: GraphBuilder[]): GraphBuilder[] {
  return new TypedGraphTemplateCache().compile(graphs);
}

/** A graph with an explicit output contract. */
export class TemplateGraphInput {
  constructor(
    readonly graph: GraphBuilder,
    readonly output: RecordDescriptor,
  ) {}
  descriptor(): RecordDescriptor {
    return this.output;
  }
  static withOutputContract(graph: GraphBuilder, output: RecordDescriptor): TemplateGraphInput {
    return new TemplateGraphInput(graph, output);
  }
}

export class TemplateBindingError extends JazzError {
  static scalarArgument = (n: number) =>
    new TemplateBindingError(
      "ScalarArgument",
      `template scalar argument ${n} is missing or has a different type`,
    );
  static predicateArgument = (n: number) =>
    new TemplateBindingError(
      "PredicateArgument",
      `template predicate argument ${n} is missing or remains unbound`,
    );
  static predicateArity = () =>
    new TemplateBindingError("PredicateArity", "template predicate argument count does not match");
  static missingInput = (n: number) =>
    new TemplateBindingError("MissingInput", `template input ${n} is missing`);
  static descriptorMismatch = (n: number) =>
    new TemplateBindingError(
      "DescriptorMismatch",
      `template input ${n} has a different record descriptor`,
    );
}

/** Bind an entire multisink forest in one iterative pass. */
export function bindTemplateGraphs(
  graphs: GraphBuilder[],
  inputs: TemplateGraphInput[],
): GraphBuilder[] {
  return bindTemplateGraphsInner(graphs, inputs, false);
}

/** Bind all source and scalar arguments in one forest pass. */
export function bindTemplateProgram(
  graphs: GraphBuilder[],
  inputs: TemplateGraphInput[],
  predicates: PredicateExpr[],
  scalars: TemplateScalarArgument[],
): GraphBuilder[] {
  return bindTemplateGraphsInner(graphs, inputs, false, new TemplateArguments(predicates, scalars));
}

export function bindTemplateGraphsInner(
  graphs: GraphBuilder[],
  inputs: TemplateGraphInput[],
  descendBound: boolean,
  args?: TemplateArguments,
): GraphBuilder[] {
  const rewritten = new Map<GraphBuilder, GraphBuilder>();
  for (const root of graphs) {
    const pending: [GraphBuilder, boolean][] = [[root, false]];
    while (pending.length) {
      const [node, expanded] = pending.pop()!;
      if (rewritten.has(node)) continue;
      if (node.kind === "TemplateInput") {
        if (node.input && !descendBound) {
          rewritten.set(node, node);
          continue;
        }
        if (!node.input) {
          const input = inputs[node.slot];
          if (!input) throw TemplateBindingError.missingInput(node.slot);
          if (input.output !== node.output)
            throw TemplateBindingError.descriptorMismatch(node.slot);
          rewritten.set(node, {
            kind: "TemplateInput",
            slot: node.slot,
            output: node.output,
            input: input.graph,
          });
          continue;
        }
      }
      if (!expanded) {
        pending.push([node, true]);
        for (const child of graphInputs(node)) pending.push([child, false]);
        continue;
      }
      let bound = graphMapInputs(node, (c) => rewritten.get(c)!);
      if (args) bound = args.bindNode(bound);
      rewritten.set(node, bound);
    }
  }
  return graphs.map((root) => rewritten.get(root)!);
}
