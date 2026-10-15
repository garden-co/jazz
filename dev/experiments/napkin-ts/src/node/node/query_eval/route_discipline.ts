// Route discipline for prepared graphs shared across bindings (#4017).
import { blake3, toHex, utf8, concatBytes } from "../../../support/index.js";
import {
  type NodeState,
  appRowTerminalRouteEligibleFields,
  preparedRouteParamNames,
  terminalRouteEligibleFields,
  terminalRouteFields,
  routeParamField,
  coveredInputTrace,
  graphBuilderKey,
  debugFmt,
} from "../../../_extern/node-qe-a.js";

type Graph = any;

export class RouteViolation {
  constructor(readonly reason: string) {}
}

const fmtSet = (s: Iterable<string>) =>
  `{${[...s]
    .sort()
    .map((x) => JSON.stringify(x))
    .join(", ")}}`;
const fmtList = (s: Iterable<string>) => `[${[...s].map((x) => JSON.stringify(x)).join(", ")}]`;
const isSubset = (a: Set<string>, b: Set<string>) => [...a].every((x) => b.has(x));
const firstMissing = (a: Set<string>, b: Set<string>) => [...a].sort().find((x) => !b.has(x));

export function bindingSlotRoutes(domain: any): Map<string, string> {
  const out = new Map<string, string>();
  for (const param of domain.userParams.keys()) out.set(param, routeParamField(param));
  for (const claim of domain.claimParams.keys()) out.set(claim, claim);
  return out;
}

export function checkRouteDiscipline(
  terminals: [string, Graph, Set<string>][],
  slotRoutes: Map<string, string>,
  infer: (g: Graph) => any,
): void {
  const aliases = slotAliases(
    terminals.map(([, g]) => g),
    slotRoutes,
    infer,
  );
  const checker = new RouteChecker(slotRoutes, aliases, infer);
  for (const [sink, graph, delivered] of terminals) {
    const depends = checker.depends(graph);
    if (!isSubset(depends, delivered)) {
      throw new RouteViolation(
        `terminal ${sink} depends on binding routes ${fmtSet(depends)} but delivers by ${fmtSet(delivered)}`,
      );
    }
    checker.check(graph);
    const masked = checker.masked(graph);
    if (masked.size) {
      throw new RouteViolation(
        `terminal ${sink} reads binding-keyed lookup rows for routes ${fmtSet(masked)}, which shared programs do not support yet`,
      );
    }
  }
}

function slotAliases(
  graphs: Graph[],
  slotRoutes: Map<string, string>,
  infer: (g: Graph) => any,
): Map<string, string> {
  const stored = new Set<string>();
  const seen = new Set<Graph>();
  const pending = [...graphs];
  while (pending.length) {
    const node = pending.pop()!;
    if (seen.has(node)) continue;
    seen.add(node);
    const kids = children(node);
    if (node.kind !== "BindingSource" && (kids.length === 0 || node.kind === "Lookup")) {
      let output;
      try {
        output = infer(node);
      } catch {
        return new Map();
      }
      for (const f of output.fields()) if (f.name !== undefined) stored.add(f.name);
    }
    pending.push(...kids);
  }
  const out = new Map<string, string>();
  for (const [slot, route] of slotRoutes)
    if (slot !== route && !stored.has(slot)) out.set(slot, route);
  return out;
}

class RouteChecker {
  private routes: Set<string>;
  private deps = new Map<Graph, Set<string>>();
  private carriedMemo = new Map<Graph, Set<string>>();
  private masks = new Map<Graph, Set<string>>();
  private equivalentsMemo = new Map<Graph, Map<string, string>>();
  private checked = new Set<Graph>();

  constructor(
    private slotRoutes: Map<string, string>,
    private aliases: Map<string, string>,
    private infer: (g: Graph) => any,
  ) {
    this.routes = new Set(slotRoutes.values());
  }

  depends(node: Graph): Set<string> {
    const hit = this.deps.get(node);
    if (hit) return hit;
    const out = new Set<string>();
    if (node.kind === "BindingSource") {
      for (const f of node.output.fields()) {
        if (f.name !== undefined) out.add(this.slotRoutes.get(f.name) ?? f.name);
      }
    } else {
      for (const c of children(node)) for (const d of this.depends(c)) out.add(d);
    }
    this.deps.set(node, out);
    return out;
  }

  carried(node: Graph): Set<string> {
    const hit = this.carriedMemo.get(node);
    if (hit) return hit;
    const out = new Set<string>();
    for (const f of this.outputNames(node)) {
      const r = this.routeName(f);
      if (this.routes.has(r)) out.add(r);
    }
    this.carriedMemo.set(node, out);
    return out;
  }

  masked(node: Graph): Set<string> {
    const hit = this.masks.get(node);
    if (hit) return hit;
    let out: Set<string>;
    if (node.kind === "Lookup") out = this.depends(node.keys);
    else {
      out = new Set();
      for (const c of children(node)) for (const m of this.masked(c)) out.add(m);
    }
    this.masks.set(node, out);
    return out;
  }

  outputNames(node: Graph): string[] {
    let output;
    try {
      output = this.infer(node);
    } catch (error) {
      throw new RouteViolation(`cannot describe a binding-dependent input: ${debugFmt(error)}`);
    }
    return output.fields().map((f: any) => f.name ?? "");
  }

  keyRoute(input: Graph, field: any): string {
    const name = this.rawFieldName(input, field);
    return this.equivalents(input).get(name) ?? this.routeName(name);
  }

  equivalents(node: Graph): Map<string, string> {
    const hit = this.equivalentsMemo.get(node);
    if (hit) return hit;
    let out = new Map<string, string>();
    switch (node.kind) {
      case "Join": {
        for (const [prefix, side] of [
          ["left.", node.left],
          ["right.", node.right],
        ] as const) {
          for (const [f, r] of this.equivalents(side)) out.set(prefix + f, r);
        }
        const lc = this.carried(node.left);
        const rc = this.carried(node.right);
        node.leftOn.forEach((lk: any, i: number) => {
          const rk = node.rightOn[i];
          if (rk === undefined) return;
          const ln = this.rawFieldName(node.left, lk);
          const rn = this.rawFieldName(node.right, rk);
          const lr = this.keyRoute(node.left, lk);
          const rr = this.keyRoute(node.right, rk);
          if (rc.has(rr) || (this.routes.has(rr) && rr !== rn)) out.set(`left.${ln}`, rr);
          if (lc.has(lr) || (this.routes.has(lr) && lr !== ln)) out.set(`right.${rn}`, lr);
        });
        break;
      }
      case "Project": {
        const inputEq = this.equivalents(node.input);
        for (const field of node.fields) {
          const e = field.expression;
          if (e.kind !== "Field" && e.kind !== "Nullable" && e.kind !== "NullableFlat") continue;
          const source = this.rawFieldName(node.input, e.field);
          const route = inputEq.get(source);
          if (route !== undefined) out.set(field.outputName, route);
        }
        break;
      }
      case "Filter":
      case "UnwrapNullable":
      case "ArgMaxBy":
      case "ArgMinBy":
      case "TopBy":
        out = this.equivalents(node.input);
        break;
      case "SemiJoin":
      case "AntiJoin":
        out = this.equivalents(node.left);
        break;
    }
    this.equivalentsMemo.set(node, out);
    return out;
  }

  routeName(name: string): string {
    return this.aliases.get(name) ?? name;
  }

  fieldName(input: Graph, field: any): string {
    return this.routeName(this.rawFieldName(input, field));
  }

  rawFieldName(input: Graph, field: any): string {
    if (field.kind === "Name" || field.kind === "StoredName") return field.name;
    return this.outputNames(input)[field.index] ?? "";
  }

  check(root: Graph): void {
    const pending = [root];
    while (pending.length) {
      const node = pending.pop()!;
      if (this.checked.has(node)) continue;
      this.checked.add(node);
      pending.push(...children(node));
      this.checkNode(node);
    }
  }

  private checkNode(node: Graph): void {
    switch (node.kind) {
      case "Join":
      case "SemiJoin":
      case "AntiJoin": {
        const { left, right, leftOn, rightOn } = node;
        const ld = this.depends(left);
        const rd = this.depends(right);
        if (!ld.size && !rd.size) return;
        const lc = this.carried(left);
        const rc = this.carried(right);
        for (const [side, depends, carried] of [
          ["left", ld, lc],
          ["right", rd, rc],
        ] as const) {
          if (!isSubset(depends, carried)) {
            let reason = `${operatorName(node)} ${side} input depends on binding routes ${fmtSet(depends)} but carries ${fmtSet(carried)}`;
            if (coveredInputTrace()) {
              reason += ` (sketch ${sketch(node, 12)}; left names ${fmtList(this.outputNames(left))}, right names ${fmtList(this.outputNames(right))})`;
            }
            throw new RouteViolation(reason);
          }
        }
        const pairs: [any, any][] = leftOn
          .slice(0, rightOn.length)
          .map((l: any, i: number) => [l, rightOn[i]]);
        const keyed = new Set<string>();
        for (const [lk, rk] of pairs) {
          const l = this.keyRoute(left, lk);
          if (l === this.keyRoute(right, rk)) keyed.add(l);
        }
        const required = node.kind === "Join" ? new Set([...lc].filter((r) => rc.has(r))) : rd;
        const route = firstMissing(required, keyed);
        if (route !== undefined) {
          let reason = `join does not key on binding route ${route}`;
          if (coveredInputTrace()) {
            const keys = pairs.map(
              ([l, r]) =>
                `(${JSON.stringify(this.fieldName(left, l))}, ${JSON.stringify(this.fieldName(right, r))})`,
            );
            const leq = [...this.equivalents(left)].map(
              ([k, v]) => `${JSON.stringify(k)}: ${JSON.stringify(v)}`,
            );
            reason += ` (keys [${keys.join(", ")}], left carries ${fmtSet(lc)}, right carries ${fmtSet(rc)}, left equivalents {${leq.join(", ")}}, sketch ${sketch(node, 12)}; left names ${fmtList(this.outputNames(left))}, right names ${fmtList(this.outputNames(right))})`;
          }
          throw new RouteViolation(reason);
        }
        return;
      }
      case "Aggregate":
      case "ArgMaxBy":
      case "ArgMinBy":
      case "TopBy": {
        if (node.kind === "TopBy" && node.offset === 0 && node.limit.kind === "Unbounded") return;
        const depends = this.depends(node.input);
        if (!depends.size) return;
        const groups = new Set<string>(
          node.groupCols.map((f: any) => this.fieldName(node.input, f)),
        );
        const route = firstMissing(depends, groups);
        if (route !== undefined)
          throw new RouteViolation(`grouping does not group by binding route ${route}`);
        return;
      }
      case "CollectBy": {
        const depends = this.depends(node.input);
        if (!depends.size) return;
        const groupings: any[][] = [node.collect.groupCols];
        const slots = [...node.collect.slots];
        while (slots.length) {
          const slot = slots.pop();
          groupings.push(slot.groupCols);
          slots.push(...slot.slots);
        }
        for (const groupCols of groupings) {
          const groups = new Set<string>(groupCols.map((f) => this.fieldName(node.input, f)));
          const route = firstMissing(depends, groups);
          if (route !== undefined)
            throw new RouteViolation(`collection does not group by binding route ${route}`);
        }
        return;
      }
      case "Recursive":
      case "RecursiveStepWitness":
      case "StreamingChecksum":
      case "TypedTemplate":
      case "TemplateInput": {
        const depends = this.depends(node);
        if (depends.size) {
          throw new RouteViolation(
            `${operatorName(node)} is not route-aware but depends on binding routes ${fmtSet(depends)}`,
          );
        }
        return;
      }
    }
  }
}

function sketch(node: Graph, depth: number): string {
  const name: string = node.kind;
  if (depth === 0) return name;
  const kids = children(node).map((c) => sketch(c, depth - 1));
  return kids.length ? `${name}[${kids.join(", ")}]` : name;
}

function operatorName(node: Graph): string {
  switch (node.kind) {
    case "Recursive":
    case "RecursiveStepWitness":
    case "TypedTemplate":
    case "TemplateInput":
    case "StreamingChecksum":
      return node.kind;
    case "Join":
      return "join";
    case "SemiJoin":
      return "semi-join";
    case "AntiJoin":
      return "anti-join";
    default:
      return "operator";
  }
}

/** Child slots as [holder, key] so rewriting can replace inputs in place. */
function childSlots(node: Graph): [any, string | number][] {
  switch (node.kind) {
    case "TypedTemplate":
    case "Union":
      return node.inputs.map((_: any, i: number) => [node.inputs, i]);
    case "TemplateInput":
      return node.input ? [[node, "input"]] : [];
    case "Recursive":
      return [
        [node, "seed"],
        [node, "step"],
        ...(node.stepWitness ? [[node, "stepWitness"] as [any, string]] : []),
      ];
    case "RecursiveStepWitness":
      return [[node, "recursive"]];
    case "Filter":
    case "UnwrapNullable":
    case "Unnest":
    case "VariantProject":
    case "Project":
    case "StreamingChecksum":
    case "ArgMaxBy":
    case "ArgMinBy":
    case "TopBy":
    case "CollectBy":
    case "Aggregate":
      return [[node, "input"]];
    case "Lookup":
      return [[node, "keys"]];
    case "Join":
    case "SemiJoin":
    case "AntiJoin":
      return [
        [node, "left"],
        [node, "right"],
      ];
    default:
      return [];
  }
}

function children(node: Graph): Graph[] {
  return childSlots(node).map(([h, k]) => h[k]);
}

function programBindingSuffix(program: any): string {
  const parts: Uint8Array[] = [];
  for (const t of program.lowered.terminals)
    parts.push(utf8(t.sink), utf8(graphBuilderKey(t.graph)));
  parts.push(
    utf8(
      program.lowered.internalAppRowsGraph
        ? graphBuilderKey(program.lowered.internalAppRowsGraph)
        : "",
    ),
  );
  return `:program:${toHex(blake3(concatBytes(...parts), 8))}`;
}

function programBindingSourceNames(program: any): string[] {
  const names = new Set<string>();
  const seen = new Set<Graph>();
  const pending: Graph[] = program.lowered.terminals.map((t: any) => t.graph);
  if (program.lowered.internalAppRowsGraph) pending.push(program.lowered.internalAppRowsGraph);
  while (pending.length) {
    const node = pending.pop()!;
    if (seen.has(node)) continue;
    seen.add(node);
    if (node.kind === "BindingSource") names.add(node.shape);
    pending.push(...children(node));
  }
  return [...names].sort();
}

type RewrittenNodes = Map<Graph, Graph>;

export function isolateBindingSources(
  graph: Graph,
  suffix: string,
  rewritten: RewrittenNodes,
): Graph {
  return rewriteNode(
    graph,
    (node) =>
      node.kind === "BindingSource" ? { ...node, shape: `${node.shape}${suffix}` } : undefined,
    rewritten,
  );
}

function expandTemplates(graph: Graph): Graph {
  return expandTemplatesWithMemo(graph, new Map());
}

function expandTemplatesWithMemo(graph: Graph, rewritten: RewrittenNodes): Graph {
  return rewriteNode(
    graph,
    (node) => {
      if (node.kind === "TypedTemplate") {
        try {
          return expandTemplates(
            node.program.bindDeclarativeWithArguments(node.inputs, node.predicates, node.scalars),
          );
        } catch {
          return undefined;
        }
      }
      if (node.kind === "TemplateInput" && node.input) return node.input;
      return undefined;
    },
    rewritten,
  );
}

/** Applies `map` bottom-up, copying only changed nodes and their ancestors. */
function rewriteNode(
  node: Graph,
  map: (g: Graph) => Graph | undefined,
  rewritten: RewrittenNodes,
): Graph {
  let mapped: Graph | undefined;
  let changed = false;
  for (const child of children(node)) {
    if (!rewritten.has(child)) rewritten.set(child, rewriteNode(child, map, rewritten));
    if (rewritten.get(child) !== child) changed = true;
  }
  if (changed) {
    mapped = { ...node };
    if (Array.isArray(node.inputs)) mapped.inputs = [...node.inputs];
    for (const [holder, key] of childSlots(mapped)) holder[key] = rewritten.get(holder[key]);
  }
  const replacement = map(mapped ?? node);
  if (replacement !== undefined) mapped = replacement;
  return mapped ?? node;
}

export function scopeProgramBindingSource(st: NodeState, program: any): void {
  const readSources = programBindingSourceNames(program);
  const sourceShape = program.request.input.binding.sourceShape ?? readSources[0];
  if (sourceShape === undefined) return;
  if (program.request.policy.kind === "AuthorizationSubplan") return;
  let violation: RouteViolation | undefined;
  try {
    if (readSources.length > 1)
      throw new RouteViolation(`program reads several binding sources ${fmtSet(readSources)}`);
    checkProgramRouteDiscipline(st, program);
  } catch (e) {
    if (!(e instanceof RouteViolation)) throw e;
    violation = e;
  }
  let suffix: string;
  if (!violation) {
    if (program.request.authorizationMode === "ClientLocal") return;
    suffix = programBindingSuffix(program);
  } else {
    if (coveredInputTrace()) {
      console.error(
        `JAZZ_COVERED_INPUT_TRACE stage=isolate_binding_source source_shape=${sourceShape} reason=RouteViolation(${JSON.stringify(violation.reason)})`,
      );
    }
    suffix = isolatedBindingSuffix(st, program.request);
  }
  const rewritten = new Map();
  for (const terminal of program.lowered.terminals) {
    terminal.graph = isolateBindingSources(terminal.graph, suffix, rewritten);
  }
  if (program.lowered.internalAppRowsGraph) {
    program.lowered.internalAppRowsGraph = isolateBindingSources(
      program.lowered.internalAppRowsGraph,
      suffix,
      rewritten,
    );
  }
  program.request.input.binding.sourceShape = `${sourceShape}${suffix}`;
}

export function checkProgramRouteDiscipline(st: NodeState, program: any): void {
  const parameters = program.lowered.parameters;
  const routeParams: string[] = preparedRouteParamNames(parameters);
  const memo = new Map();
  const expanded = program.lowered.terminals.map((t: any) =>
    expandTemplatesWithMemo(t.graph, memo),
  );
  const expandedAppRows = program.lowered.internalAppRowsGraph
    ? expandTemplatesWithMemo(program.lowered.internalAppRowsGraph, memo)
    : undefined;
  const terminals: [string, Graph, Set<string>][] = [];
  program.lowered.terminals.forEach((terminal: any, i: number) => {
    let eligible;
    try {
      eligible = terminalRouteEligibleFields(terminal.output);
    } catch (error) {
      throw new RouteViolation(debugFmt(error));
    }
    terminals.push([
      terminal.sink,
      expanded[i],
      new Set(terminalRouteFields(routeParams, eligible)),
    ]);
  });
  if (expandedAppRows) {
    let eligible: Set<string>;
    try {
      eligible = appRowTerminalRouteEligibleFields(program.lowered.output);
    } catch (error) {
      throw new RouteViolation(debugFmt(error));
    }
    terminals.push([
      "internal-app-rows",
      expandedAppRows,
      new Set(routeParams.filter((f) => eligible.has(f))),
    ]);
  }
  checkRouteDiscipline(terminals, bindingSlotRoutes(parameters), (g) =>
    st.database.graphOutputDescriptor(g),
  );
}

function isolatedBindingSuffix(st: NodeState, request: any): string {
  const session =
    request.policy.kind === "Identity"
      ? (st.activeSessionClaimScopeKey(request.policy.permissionSubject) ??
        debugFmt(request.policy.permissionSubject))
      : "system";
  const data = concatBytes(
    utf8("jazz/isolated-binding-source/v1\0"),
    utf8(session),
    utf8(debugFmt(request.input.binding.id)),
  );
  return `:isolated:${toHex(blake3(data))}`;
}
