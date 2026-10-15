// Typed bind-time arguments. Values never enter the reusable program.
import {
  type Value,
  type ValueType,
  encodeSingleFieldValue,
} from "../../../_extern/groove-ivm-plan.js";
import type { GraphBuilder, ProjectExpr } from "../graph.js";
import {
  type LiteralValue,
  type PredicateExpr,
  literalFromValue,
  predicateAcceptsTemplateArgument,
  structEq,
  structKey,
} from "../op_types.js";
import { TemplateBindingError } from "../template.js";
import { graphInputs, graphMapInputs } from "../graph.js";

/** A checked scalar value. This is not authority. */
export class TemplateScalarArgument {
  private constructor(
    readonly value: LiteralValue,
    readonly valueType: ValueType,
  ) {}
  static new(value: Value, valueType: ValueType): TemplateScalarArgument {
    encodeSingleFieldValue(value, valueType);
    return new TemplateScalarArgument(literalFromValue(value), valueType);
  }
  structKey(): string {
    return structKey([this.value, this.valueType]);
  }
}

export function bindProjectExpression(
  expression: ProjectExpr,
  args: readonly TemplateScalarArgument[],
): ProjectExpr {
  if (expression.kind !== "TemplateArgument") return expression;
  const argument = args[expression.slot];
  if (!argument || !structEq(argument.valueType, expression.valueType))
    throw TemplateBindingError.scalarArgument(expression.slot);
  return { kind: "TypedLiteral", value: argument.value, valueType: expression.valueType };
}

function bindPredicate(predicate: PredicateExpr, args: readonly PredicateExpr[]): PredicateExpr {
  switch (predicate.kind) {
    case "TemplateArgument": {
      const argument = args[predicate.slot];
      if (!argument || !predicateAcceptsTemplateArgument(predicate, argument))
        throw TemplateBindingError.predicateArgument(predicate.slot);
      return argument;
    }
    case "And":
    case "Or":
      return {
        kind: predicate.kind,
        children: predicate.children.map((c) => bindPredicate(c, args)),
      };
    case "EnumMatch":
      return { ...predicate, payload: bindPredicate(predicate.payload, args) };
    default:
      return predicate;
  }
}

export class TemplateArguments {
  constructor(
    readonly predicates: readonly PredicateExpr[],
    readonly scalars: TemplateScalarArgument[],
  ) {}

  bindNode(node: GraphBuilder): GraphBuilder {
    switch (node.kind) {
      case "Filter":
        return { ...node, predicate: bindPredicate(node.predicate, this.predicates) };
      case "Project":
        return {
          ...node,
          fields: node.fields.map((f) => ({
            ...f,
            expression: bindProjectExpression(f.expression, this.scalars),
          })),
        };
      case "TypedTemplate":
        return {
          ...node,
          predicates: node.predicates.map((p) => bindPredicate(p, this.predicates)),
          scalars: this.scalars,
        };
      default:
        return node;
    }
  }
}

/** Bind predicates and typed projected constants across one program forest. */
export function bindTemplateArguments(
  graphs: GraphBuilder[],
  predicates: PredicateExpr[],
  scalars: TemplateScalarArgument[],
): GraphBuilder[] {
  const args = new TemplateArguments(predicates, scalars);
  const rewritten = new Map<GraphBuilder, GraphBuilder>();
  for (const root of graphs) {
    const pending: [GraphBuilder, boolean][] = [[root, false]];
    while (pending.length) {
      const [node, expanded] = pending.pop()!;
      if (rewritten.has(node)) continue;
      if (node.kind === "TemplateInput" && node.input) {
        rewritten.set(node, node);
        continue;
      }
      if (!expanded) {
        pending.push([node, true]);
        for (const child of graphInputs(node)) pending.push([child, false]);
        continue;
      }
      rewritten.set(node, args.bindNode(graphMapInputs(node, (c) => rewritten.get(c)!)));
    }
  }
  return graphs.map((root) => rewritten.get(root)!);
}
