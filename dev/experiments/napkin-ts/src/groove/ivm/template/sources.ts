// Separate instance-owned leaves from immutable source operator context.
import { type GraphBuilder, graphInputs, graphMapInputs } from "../graph.js";
import type { TemplateGraphInput } from "../template.js";
import { graphBuildersEqualWith } from "../../../_extern/groove-ivm-plan.js";

type Describe = (g: GraphBuilder) => TemplateGraphInput;

class SourceBindings {
  inputs: TemplateGraphInput[] = [];
  private rewritten = new Map<GraphBuilder, GraphBuilder>();

  blueprint(root: GraphBuilder, describe: Describe): GraphBuilder {
    const pending: [GraphBuilder, boolean][] = [[root, false]];
    while (pending.length) {
      const [graph, expanded] = pending.pop()!;
      if (this.rewritten.has(graph)) continue;
      if (isExternalLeaf(graph)) {
        const input = describe(graph);
        this.rewritten.set(graph, {
          kind: "TemplateInput",
          slot: this.inputs.length,
          output: input.descriptor(),
        });
        this.inputs.push(input);
      } else if (!expanded) {
        pending.push([graph, true]);
        for (const child of graphInputs(graph)) pending.push([child, false]);
      } else {
        this.rewritten.set(
          graph,
          graphMapInputs(graph, (c) => this.rewritten.get(c)!),
        );
      }
    }
    return this.rewritten.get(root)!;
  }
}

/** Instance-owned leaves: identity or rows belong to one admitted request. */
function isExternalLeaf(graph: GraphBuilder): boolean {
  switch (graph.kind) {
    case "Table":
    case "Index":
    case "InputSource":
    case "InlineRecords":
    case "TypedTemplate":
    case "Lookup":
      return true;
    case "TemplateInput":
      return graph.input !== undefined;
    case "ArgMaxBy":
    case "ArgMinBy":
      return graph.input.kind === "Table";
    default:
      return false;
  }
}

/** Match fresh source graphs against blueprints from splitTemplateSources. */
export function matchTemplateSources(
  graphs: GraphBuilder[],
  blueprints: GraphBuilder[],
  describe: Describe,
): TemplateGraphInput[] | undefined {
  if (graphs.length !== blueprints.length) return undefined;
  const inputs: (TemplateGraphInput | undefined)[] = [];
  const leafSlots = new Map<GraphBuilder, number>();
  for (let i = 0; i < graphs.length; i++) {
    const matched = graphBuildersEqualWith(
      graphs[i],
      blueprints[i],
      (fresh: GraphBuilder, slot: GraphBuilder) => {
        if (!isExternalLeaf(fresh)) {
          // A blueprint slot can only stand for an external leaf.
          return slot.kind === "TemplateInput" && !slot.input ? false : undefined;
        }
        if (slot.kind !== "TemplateInput" || slot.input) return false;
        const existing = leafSlots.get(fresh);
        if (existing !== undefined) return existing === slot.slot;
        if (inputs[slot.slot] !== undefined) return false;
        const input = describe(fresh);
        if (input.descriptor() !== slot.output) return false;
        leafSlots.set(fresh, slot.slot);
        inputs[slot.slot] = input;
        return true;
      },
    );
    if (!matched) return undefined;
  }
  for (let i = 0; i < inputs.length; i++) if (inputs[i] === undefined) return undefined;
  return inputs as TemplateGraphInput[];
}

/** Split an immutable source forest into static blueprints and fresh typed leaf arguments. */
export function splitTemplateSources(
  graphs: GraphBuilder[],
  describe: Describe,
): [GraphBuilder[], TemplateGraphInput[]] {
  const bindings = new SourceBindings();
  const out = graphs.map((g) => bindings.blueprint(g, describe));
  return [out, bindings.inputs];
}
