import type { SortedMap } from "../../../support/collections.js";
import {
  LiteralValue,
  matchTemplateSources,
  NIL_UUID,
  splitTemplateSources,
  TypedGraphTemplateCache,
  queryTemplateTrace,
  type GraphBuilder,
  type TemplateGraphInput,
  type ValueType,
} from "../../../_extern/engine.js";
import { structEq, structMap, type QueryProgramRequest, type SourceId } from "../input.js";
import { emptyExplain, tryGap, type ExplainPlan, type ResolvedSource } from "../resolver.js";
import { extractTemplateLiterals, ProgramArgumentRecipes } from "./arguments.js";
import {
  explainWithRequest,
  lowerResolvedQueryProgram,
  lowerResolvedQueryProgramWithSourceParameters,
  QueryProgramCompilation,
  type QueryProgram,
  type ResolvedQuerySources,
} from "../lowering.js";

function traceTemplate(event: string): void {
  if (queryTemplateTrace()) console.error(`JAZZ_QUERY_TEMPLATE_TRACE ${event}`);
}

type ParameterizedProgram = { program: QueryProgram; arguments: ProgramArgumentRecipes };

/** A family is promoted on its second sighting. */
type TemplateState =
  | { kind: "Seen" }
  | { kind: "Unsupported" }
  | { kind: "Ready"; template: ParameterizedProgram };

type TemplateEntry = {
  request: QueryProgramRequest;
  literalTypes: (ValueType | undefined)[];
  sources: ResolvedQuerySources;
  program: TemplateState;
};

function cloneTemplateRequest(request: QueryProgramRequest): QueryProgramRequest {
  const shape = request.input.shape;
  return {
    ...request,
    input: {
      shape: {
        ...shape,
        identity: { ...shape.identity },
        nodes: structMap([...shape.nodes].map(([id, node]) => [id, structuredClone(node)])),
      },
      binding: { ...request.input.binding, values: request.input.binding.values.clone() },
    },
  };
}

/** Only immutable, unbound descriptions live here. */
export class QueryProgramTemplateCache {
  private entries: TemplateEntry[] = [];
  private physical = new TypedGraphTemplateCache();

  clear(): void {
    this.entries = [];
    this.physical.clear();
  }

  private terminalGraphs(program: QueryProgram): GraphBuilder[] {
    const graphs = program.lowered.terminals.map((t) => t.graph);
    if (program.lowered.internalAppRowsGraph !== undefined)
      graphs.push(program.lowered.internalAppRowsGraph);
    return graphs;
  }

  private replaceGraphs(program: QueryProgram, graphs: GraphBuilder[]): void {
    let i = 0;
    for (const terminal of program.lowered.terminals) terminal.graph = graphs[i++];
    if (program.lowered.internalAppRowsGraph !== undefined)
      program.lowered.internalAppRowsGraph = graphs[i];
  }

  private typedProgram(program: QueryProgram): QueryProgram {
    let typed: GraphBuilder[] | undefined;
    try {
      typed = this.physical.compile(this.terminalGraphs(program));
    } catch {
      typed = undefined;
    }
    if (typed) {
      this.replaceGraphs(program, typed);
      traceTemplate("typed_bound");
    }
    return program;
  }

  lower(
    compilation: QueryProgramCompilation,
    sources: ResolvedQuerySources,
    explain: ExplainPlan,
    describe: (graph: GraphBuilder) => TemplateGraphInput,
  ): QueryProgram {
    const fallback = () => lowerResolvedQueryProgram(compilation, sources, explain);
    const templateRequest = cloneTemplateRequest(compilation.request);
    const literals = tryGap(() => extractTemplateLiterals(templateRequest));
    if (!literals) return fallback();
    const literalTypes = literals.map((value) => LiteralValue.from(value).valueType());
    templateRequest.input.binding.values.clear();
    templateRequest.input.binding.id = NIL_UUID;
    const sourceGraphs = sourceGraphList(sources);

    let found: [number, TemplateGraphInput[]] | undefined;
    for (let index = 0; index < this.entries.length; index++) {
      const entry = this.entries[index];
      if (
        !structEq(
          entry.request.input.shape.identity.shapeId,
          templateRequest.input.shape.identity.shapeId,
        ) ||
        !structEq(entry.literalTypes, literalTypes) ||
        !sourceMetadataEqual(entry.sources, sources) ||
        !structEq(entry.request, templateRequest)
      ) {
        continue;
      }
      let inputs: TemplateGraphInput[] | undefined;
      try {
        inputs = matchTemplateSources(sourceGraphs, sourceGraphList(entry.sources), describe);
      } catch {
        traceTemplate("descriptor_fallback");
        return fallback();
      }
      if (inputs) {
        found = [index, inputs];
        break;
      }
    }

    if (!found) {
      // First sighting: record the family's blueprint and take the concrete path.
      let blueprints: GraphBuilder[];
      try {
        blueprints = splitTemplateSources(sourceGraphs, describe)[0];
      } catch {
        traceTemplate("descriptor_fallback");
        return fallback();
      }
      const templateSources = structMap<SourceId, ResolvedSource>();
      let next = 0;
      for (const [id, source] of sources) {
        const copy: ResolvedSource = { ...source, graph: blueprints[next++] };
        if (copy.contentVersion)
          copy.contentVersion = { ...copy.contentVersion, graph: blueprints[next++] };
        if (copy.deletionRegister)
          copy.deletionRegister = { ...copy.deletionRegister, graph: blueprints[next++] };
        if (copy.authorizedDeletionPreimage) {
          copy.authorizedDeletionPreimage = {
            ...copy.authorizedDeletionPreimage,
            graph: blueprints[next++],
          };
        }
        templateSources.set(id, copy);
      }
      if (this.entries.length === 256) this.entries.shift();
      this.entries.push({
        request: templateRequest,
        literalTypes,
        sources: templateSources,
        program: { kind: "Seen" },
      });
      traceTemplate("seen");
      return fallback();
    }

    const [index, inputs] = found;
    const entry = this.entries[index];
    let template: ParameterizedProgram;
    switch (entry.program.kind) {
      case "Ready":
        template = entry.program.template;
        break;
      case "Unsupported":
        traceTemplate("unsupported_hit");
        return fallback();
      case "Seen": {
        const args = new ProgramArgumentRecipes(literalTypes);
        const program = tryGap(() =>
          lowerResolvedQueryProgramWithSourceParameters(
            QueryProgramCompilation.analyze(templateRequest),
            entry.sources,
            emptyExplain(),
            new Map(),
            args,
          ),
        );
        traceTemplate(program ? "compiled" : "unsupported");
        if (!program) {
          entry.program = { kind: "Unsupported" };
          return fallback();
        }
        template = { program: this.typedProgram(program), arguments: args };
        entry.program = { kind: "Ready", template };
      }
    }
    traceTemplate("bound");
    const program: QueryProgram = {
      ...template.program,
      lowered: {
        ...template.program.lowered,
        terminals: template.program.lowered.terminals.map((t) => ({ ...t })),
      },
    };
    const bound = tryGap(() =>
      template.arguments.bind(this.terminalGraphs(program), inputs, compilation.request, literals),
    );
    if (!bound) {
      traceTemplate("argument_fallback");
      return fallback();
    }
    this.replaceGraphs(program, bound);
    program.request = compilation.request;
    const capabilities = program.explain.capabilities;
    program.explain = explainWithRequest(program.request, explain);
    program.explain.capabilities.push(...capabilities);
    return program;
  }
}

/** Source graphs in the fixed order used for blueprint slot numbering. */
function sourceGraphList(sources: ResolvedQuerySources): GraphBuilder[] {
  const graphs: GraphBuilder[] = [];
  for (const source of sources.valuesArray()) {
    graphs.push(source.graph);
    if (source.contentVersion) graphs.push(source.contentVersion.graph);
    if (source.deletionRegister) graphs.push(source.deletionRegister.graph);
    if (source.authorizedDeletionPreimage) graphs.push(source.authorizedDeletionPreimage.graph);
  }
  return graphs;
}

/** Everything a resolved source contributes to a family key except its graphs. */
function sourceMetadataEqual(
  left: SortedMap<SourceId, ResolvedSource>,
  right: ResolvedQuerySources,
): boolean {
  if (left.size !== right.size) return false;
  const l = [...left];
  const r = [...right];
  return l.every(([leftId, a], i) => {
    const [rightId, b] = r[i];
    return (
      structEq(leftId, rightId) &&
      structEq(a.storedColumnIds, b.storedColumnIds) &&
      structEq(a.tableSchema, b.tableSchema) &&
      structEq(a.rowShape, b.rowShape) &&
      structEq(a.routingFields, b.routingFields) &&
      a.requiresResultPayload === b.requiresResultPayload &&
      a.contentVersion?.rowUuidField === b.contentVersion?.rowUuidField &&
      a.deletionRegister?.rowUuidField === b.deletionRegister?.rowUuidField &&
      structEq(
        a.authorizedDeletionPreimage?.routingFields,
        b.authorizedDeletionPreimage?.routingFields,
      )
    );
  });
}
