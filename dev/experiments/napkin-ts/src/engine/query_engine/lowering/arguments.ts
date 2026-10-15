import {
  bindTemplateProgram,
  decodeValue,
  GroovePredicateExpr,
  NIL_UUID,
  ProjectExpr,
  TemplateScalarArgument,
  type GraphBuilder,
  type TemplateGraphInput,
  type Value,
  type ValueType,
} from "../../../_extern/engine.js";
import { coercePreparedBindingValue } from "../binding_values.js";
import {
  forEachPredicateValue,
  structEq,
  type NormalizedValueRef,
  type PredicateExpr,
  type QueryProgramRequest,
  type SourceId,
} from "../input.js";
import { operatorGap, runtimeGap, UnsupportedError, type ResolvedSource } from "../resolver.js";
import { claimValue, lowerPredicate, sourceFields } from "./graph_lowering.js";
import { parameterDomainForRequest, type ParameterDomain } from "../lowering.js";

export class LoweringContext {
  private domain?: { ok: ParameterDomain } | { err: UnsupportedError };

  constructor(
    readonly request: QueryProgramRequest,
    private readonly argumentRecipes?: ProgramArgumentRecipes,
    private readonly literals: Value[] = [],
  ) {}

  static concrete(request: QueryProgramRequest): LoweringContext {
    return new LoweringContext(request);
  }
  static compiling(request: QueryProgramRequest, args: ProgramArgumentRecipes): LoweringContext {
    return new LoweringContext(request, args);
  }

  get input() {
    return this.request.input;
  }
  get output() {
    return this.request.output;
  }
  get policy() {
    return this.request.policy;
  }
  get reads() {
    return this.request.reads;
  }

  /** The request's parameter domain, derived once per lowering. */
  parameterDomain(): ParameterDomain {
    if (!this.domain) {
      try {
        this.domain = { ok: parameterDomainForRequest(this.request) };
      } catch (err) {
        if (!(err instanceof UnsupportedError)) throw err;
        this.domain = { err };
      }
    }
    if ("err" in this.domain) throw this.domain.err;
    return this.domain.ok;
  }

  predicateArgument(predicate: PredicateExpr, sourceId: SourceId, source: ResolvedSource): any {
    if (!predicateContainsArgument(predicate) || !this.argumentRecipes) return undefined;
    const slot = this.argumentRecipes.predicates.length;
    this.argumentRecipes.predicates.push({ predicate, sourceId, source });
    return GroovePredicateExpr.TemplateArgument({ slot, fields: [...sourceFields(source)] });
  }

  literal(slot: number): Value {
    if (slot >= this.literals.length) throw runtimeGap(`compiler literal ${slot} is not bound`);
    return this.literals[slot];
  }

  literalType(slot: number): ValueType | undefined {
    return this.argumentRecipes?.literalTypes[slot];
  }

  scalarArgument(value: NormalizedValueRef, ty: ValueType, coerce: boolean): any {
    const args = this.argumentRecipes;
    if (!args) return undefined;
    // Multiple witnesses often project the identical route; one recipe owns it.
    const recipe: ScalarArgument = { value, ty, coerce };
    let slot = args.scalars.findIndex((existing) => structEq(existing, recipe));
    if (slot < 0) {
      slot = args.scalars.length;
      args.scalars.push(recipe);
    }
    return ProjectExpr.TemplateArgument({ slot, valueType: ty });
  }
}

type PredicateArgument = { predicate: PredicateExpr; sourceId: SourceId; source: ResolvedSource };
type ScalarArgument = { value: NormalizedValueRef; ty: ValueType; coerce: boolean };

export class ProgramArgumentRecipes {
  predicates: PredicateArgument[] = [];
  scalars: ScalarArgument[] = [];
  constructor(readonly literalTypes: (ValueType | undefined)[] = []) {}

  bind(
    graphs: GraphBuilder[],
    inputs: TemplateGraphInput[],
    request: QueryProgramRequest,
    literals: Value[],
  ): GraphBuilder[] {
    const context = new LoweringContext(request, undefined, literals);
    const predicates = this.predicates.map((recipe) =>
      lowerPredicate(recipe.predicate, recipe.sourceId, recipe.source, context),
    );
    const scalars = this.scalars.map((recipe) => {
      let value: Value;
      switch (recipe.value.kind) {
        case "TemplateLiteral":
          value = context.literal(recipe.value.slot);
          break;
        case "Param": {
          const name = recipe.value.name;
          value = request.input.binding.values.get(name);
          if (value === undefined) throw operatorGap(`binding parameter '${name}' is not bound`);
          break;
        }
        case "Claim":
          value = claimValue(recipe.value.path, request.policy);
          break;
        case "Literal":
          try {
            value = decodeValue(recipe.value.bytes);
          } catch (err) {
            throw operatorGap(`literal decoding failed: ${err}`);
          }
          break;
        default:
          throw operatorGap("template scalar must be a parameter, claim or literal");
      }
      if (recipe.coerce) value = coercePreparedBindingValue(value, recipe.ty);
      try {
        return new TemplateScalarArgument(value, recipe.ty);
      } catch (err) {
        throw operatorGap(`template scalar binding failed: ${err}`);
      }
    });
    try {
      return bindTemplateProgram(graphs, inputs, predicates, scalars);
    } catch (err) {
      throw runtimeGap(`template argument binding failed: ${err}`);
    }
  }
}

function predicateContainsArgument(predicate: PredicateExpr): boolean {
  let found = false;
  forEachPredicateValue(predicate, (value) => {
    if (value.kind === "Param" || value.kind === "TemplateLiteral") found = true;
  });
  return found;
}

/** Lift ordinary literal operands into a compiler-local argument namespace. */
export function extractTemplateLiterals(request: QueryProgramRequest): Value[] {
  const literals: Value[] = [];
  const lift = (value: NormalizedValueRef): NormalizedValueRef | void => {
    if (value.kind !== "Literal") return;
    let decoded: Value;
    try {
      decoded = decodeValue(value.bytes);
    } catch (err) {
      throw operatorGap(`literal decoding failed: ${err}`);
    }
    literals.push(decoded);
    return { kind: "TemplateLiteral", slot: literals.length - 1 };
  };
  for (const node of request.input.shape.nodes.valuesArray()) {
    if (node.kind === "Filter") forEachPredicateValue(node.predicate, lift);
    else if (node.kind === "Project") {
      for (const column of node.columns) {
        const next = lift(column.value);
        if (next) column.value = next;
      }
    }
  }
  if (literals.length > 0) {
    request.input.shape.identity.shapeId = NIL_UUID;
    request.input.shape.identity.canonical = new Uint8Array(0);
  }
  return literals;
}
