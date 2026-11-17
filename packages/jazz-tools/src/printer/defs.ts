/**
 * Expanding a hand-written `jazz.permissions.json`'s `defs` (alpha.63–65
 * documented them; alpha.66 refuses them): each call `name(arg, …)` becomes
 * the def's body in parentheses, its `$param`s replaced by the arguments,
 * recursively. Text in, text out, so the expanded file loads as the old
 * loader read the original: a call was its body's own node.
 */
import type { PermissionsJson, PolicyJson } from "./app-json.js";
import { tokens } from "./expr.js";

interface Def {
  params: string[];
  body: string;
}

const MAX_DEPTH = 64;

/** The file without `defs`, every call expanded. */
export function expandDefs(permissions: PermissionsJson): PermissionsJson {
  const raw = permissions.defs;
  if (!raw || Object.keys(raw).length === 0) {
    const { defs: _defs, ...rest } = permissions;
    return rest as PermissionsJson;
  }
  const defs = new Map<string, Def>();
  for (const [signature, body] of Object.entries(raw)) {
    const match = /^\s*([A-Za-z_][A-Za-z0-9_]*)\s*\(([^)]*)\)\s*$/.exec(signature);
    if (!match)
      throw new Error(
        `jazz.permissions.json: defs: \`${signature}\` is not a signature like \`name($param, …)\``,
      );
    const params = match[2]!
      .split(",")
      .map((p) => p.trim())
      .filter(Boolean)
      .map((p) => p.replace(/^\$/, ""));
    defs.set(match[1]!, { params, body: policyText(body) });
  }
  const expand = (text: string, depth: number): string => {
    if (depth > MAX_DEPTH)
      throw new Error("jazz.permissions.json: defs call each other too deeply (a cycle?)");
    const toks = tokens(text);
    let out = "";
    let at = 0;
    for (let i = 0; i < toks.length; i++) {
      const t = toks[i]!;
      const def = t.kind === "ident" && toks[i + 1]?.kind === "(" ? defs.get(t.value!) : undefined;
      // A def call: not `exists t(…)`'s table, `allowedTo.x(…)`, or a hop `.rel(…)`.
      const prev = toks[i - 1];
      if (
        !def ||
        prev?.kind === "." ||
        (prev?.kind === "ident" && (prev.value === "exists" || prev.value === "as"))
      )
        continue;
      // The arguments: operands between the parentheses, split at top-level commas.
      const args: string[] = [];
      let depthParen = 0;
      let argStart = toks[i + 1]!.end;
      let j = i + 1;
      for (; j < toks.length; j++) {
        const k = toks[j]!;
        if (k.kind === "(" || k.kind === "[") depthParen++;
        else if (k.kind === ")" || k.kind === "]") {
          depthParen--;
          if (depthParen === 0) {
            const arg = text.slice(argStart, k.start).trim();
            if (arg) args.push(arg);
            break;
          }
        } else if (k.kind === "," && depthParen === 1) {
          args.push(text.slice(argStart, k.start).trim());
          argStart = k.end;
        }
      }
      if (args.length !== def.params.length) {
        throw new Error(
          `jazz.permissions.json: \`${t.value}\` takes ${def.params.length} argument(s), not ${args.length}`,
        );
      }
      const body = substitute(def, args);
      out += `${text.slice(at, t.start)}(${expand(body, depth + 1)})`;
      at = toks[j]!.end;
      i = j;
    }
    return out + text.slice(at);
  };
  const policy = (p: PolicyJson): PolicyJson => {
    if (typeof p === "string") return expand(p, 0);
    if ("anyOf" in p) return { anyOf: p.anyOf.map(policy) };
    if ("allOf" in p) return { allOf: p.allOf.map(policy) };
    if ("where" in p) return { ...p, where: policy(p.where) };
    return { using: policy(p.using), withCheck: policy(p.withCheck) };
  };
  const tables: PermissionsJson["tables"] = {};
  for (const [table, rules] of Object.entries(permissions.tables)) {
    const out: Record<string, unknown> = {};
    for (const [op, rule] of Object.entries(rules)) {
      if (op === "$comment") out[op] = rule;
      else if (typeof rule === "object" && rule !== null && "old" in rule && "new" in rule) {
        out[op] = { old: policy(rule.old as PolicyJson), new: policy(rule.new as PolicyJson) };
      } else out[op] = policy(rule as PolicyJson);
    }
    tables[table] = out;
  }
  const { defs: _defs, ...rest } = permissions;
  return { ...(rest as PermissionsJson), tables };
}

/** A def's body as one expression: `anyOf`/`allOf` objects as `anyOf(…)` / `allOf(…)`. */
function policyText(p: PolicyJson): string {
  if (typeof p === "string") return p;
  if ("anyOf" in p) return `anyOf(${p.anyOf.map(policyText).join(", ")})`;
  if ("allOf" in p) return `allOf(${p.allOf.map(policyText).join(", ")})`;
  if ("where" in p) return policyText(p.where);
  throw new Error("jazz.permissions.json: a def can't have both `using` and `withCheck`");
}

function substitute(def: Def, args: string[]): string {
  const toks = tokens(def.body);
  let out = "";
  let at = 0;
  for (const t of toks) {
    const index = t.kind === "dollar" ? def.params.indexOf(t.value!) : -1;
    if (index < 0) continue;
    out += def.body.slice(at, t.start) + args[index];
    at = t.end;
  }
  return out + def.body.slice(at);
}
