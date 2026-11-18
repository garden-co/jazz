/**
 * alpha.69's device-account renames, as `jazz-tools upgrade` applies them
 * (jazz#6079, jazz#6077):
 *
 * - `session.authMode` is `"device-account"` where it was `"local-first"`.
 *   Only comparisons with `session.authMode` change: in jazz.permissions.json
 *   (`session.authMode = 'local-first'`, `!=`, `in [...]`), in the policy DSL
 *   (`eq` / `ne` / `isIn` on `session.authMode`), and in app code
 *   (`x.authMode === "local-first"`, `case "local-first":` on an `authMode`,
 *   `authMode: "local-first"` in an object such as
 *   `createPolicyTestApp().as({ … })`).
 * - `AccountAuthError` codes `local_first_*` become `device_account_*`.
 * - Deprecated names (they still work): `getLocalFirstIdentityProof` →
 *   `getDeviceAccountProof`, `LocalFirstGuest` (jazz-tools/backend) →
 *   `DeviceAccountsAnswer`, and the CLI's `--allow-local-first-auth` →
 *   `--allow-device-accounts`.
 *
 * Everything else called "local-first" is storage, not auth, and stays: the
 * read tier, `"local-first-unless-empty"`, `jazz-tools/local-first`,
 * `upgradeToLocalFirst`, the issuer `urn:jazz:local-first`, the
 * `X-Jazz-Local-First-Audience` header, `allowLocalFirstAuth` and the HTTP
 * wire codes.
 */
import type * as TS from "typescript";
import type { PermissionsJson, PolicyJson } from "../printer/app-json.js";
import { tokens } from "../printer/expr.js";
import { applyEdits, type TextEdit } from "./edits.js";

type Ts = typeof TS;

export const OLD_AUTH_MODE = "local-first";
export const NEW_AUTH_MODE = "device-account";

/** The renamed `AccountAuthError` codes. */
export const ERROR_CODES: Record<string, string> = {
  local_first_account_required: "device_account_required",
  local_first_recovery_unavailable: "device_account_recovery_unavailable",
  local_first_restore_unavailable: "device_account_restore_unavailable",
};

/**
 * A policy expression with `'local-first'` compared with `session.authMode`
 * rewritten (`=`, `!=`, either side, and `in [...]` lists).
 */
export function rewritePolicyText(text: string): string {
  let toks;
  try {
    toks = tokens(text);
  } catch {
    return text; // Not an expression: the loader reports it.
  }
  const edits: TextEdit[] = [];
  const isOld = (i: number) => toks[i]?.kind === "str" && toks[i]!.value === OLD_AUTH_MODE;
  const replace = (i: number) =>
    edits.push({ start: toks[i]!.start, end: toks[i]!.end, text: `'${NEW_AUTH_MODE}'` });
  /** Whether `session.authMode` starts at token `i` (and ends there: not `.authMode.x`). */
  const authModeAt = (i: number) =>
    toks[i]?.kind === "ident" &&
    toks[i]!.value === "session" &&
    toks[i + 1]?.kind === "." &&
    toks[i + 2]?.kind === "ident" &&
    toks[i + 2]!.value === "authMode" &&
    toks[i + 3]?.kind !== ".";
  for (let i = 0; i < toks.length; i++) {
    if (authModeAt(i)) {
      const next = toks[i + 3];
      if (next?.kind === "op" && isOld(i + 4)) replace(i + 4);
      if (next?.kind === "ident" && next.value === "in" && toks[i + 4]?.kind === "[") {
        for (let j = i + 5; j < toks.length && toks[j]!.kind !== "]"; j++) if (isOld(j)) replace(j);
      }
    }
    // `'local-first' = session.authMode`
    if (isOld(i) && toks[i + 1]?.kind === "op" && authModeAt(i + 2)) replace(i);
  }
  return edits.length ? applyEdits(text, edits) : text;
}

/** The permissions file with every policy expression rewritten; `count` says how many changed. */
export function rewritePermissions(permissions: PermissionsJson): {
  permissions: PermissionsJson;
  count: number;
} {
  let count = 0;
  const policy = (p: PolicyJson): PolicyJson => {
    if (typeof p === "string") {
      const next = rewritePolicyText(p);
      if (next !== p) count++;
      return next;
    }
    if ("anyOf" in p) return { ...p, anyOf: p.anyOf.map(policy) };
    if ("allOf" in p) return { ...p, allOf: p.allOf.map(policy) };
    if ("where" in p) return { ...p, where: policy(p.where) };
    return { ...p, using: policy(p.using), withCheck: policy(p.withCheck) };
  };
  const tables: PermissionsJson["tables"] = {};
  for (const [table, rules] of Object.entries(permissions.tables ?? {})) {
    const out: Record<string, unknown> = {};
    for (const [op, rule] of Object.entries(rules)) {
      if (op === "$comment") out[op] = rule;
      else if (typeof rule === "object" && rule !== null && "old" in rule && "new" in rule) {
        out[op] = { old: policy(rule.old as PolicyJson), new: policy(rule.new as PolicyJson) };
      } else out[op] = policy(rule as PolicyJson);
    }
    tables[table] = out;
  }
  const defs = permissions.defs
    ? Object.fromEntries(Object.entries(permissions.defs).map(([k, v]) => [k, policy(v)]))
    : undefined;
  return { permissions: { ...permissions, ...(defs ? { defs } : {}), tables }, count };
}

/** The JSON text of a permissions file, rewritten, or the same text when nothing changes. */
export function rewritePermissionsText(text: string): { text: string; count: number } {
  let parsed: PermissionsJson;
  try {
    parsed = JSON.parse(text) as PermissionsJson;
  } catch {
    return { text, count: 0 };
  }
  const { permissions, count } = rewritePermissions(parsed);
  return count === 0
    ? { text, count }
    : { text: `${JSON.stringify(permissions, null, 2)}\n`, count };
}

export interface SourceRewrite {
  text: string;
  /** One line per rewrite: `line: what`. */
  changes: { line: number; what: string }[];
}

/** A TS or JS module with the renames applied. */
export function rewriteSource(ts: Ts, file: string, text: string): SourceRewrite {
  const kind = /\.[jt]sx$/.test(file) ? ts.ScriptKind.TSX : ts.ScriptKind.TS;
  const source = ts.createSourceFile(file, text, ts.ScriptTarget.Latest, true, kind);
  const edits: TextEdit[] = [];
  const changes: SourceRewrite["changes"] = [];
  const line = (node: TS.Node) =>
    source.getLineAndCharacterOfPosition(node.getStart(source)).line + 1;
  const replaceString = (
    node: TS.StringLiteral | TS.NoSubstitutionTemplateLiteral,
    value: string,
    what: string,
  ) => {
    const quote = text[node.getStart(source)]!;
    edits.push({
      start: node.getStart(source),
      end: node.getEnd(),
      text: `${quote}${value}${quote}`,
    });
    changes.push({ line: line(node), what });
  };
  /** `x.authMode`, `x?.authMode`, `x["authMode"]`. */
  const isAuthMode = (node: TS.Node | undefined): boolean => {
    if (!node) return false;
    while (ts.isParenthesizedExpression(node) || ts.isNonNullExpression(node))
      node = node.expression;
    if (ts.isPropertyAccessExpression(node)) return node.name.text === "authMode";
    if (ts.isElementAccessExpression(node)) {
      return (
        ts.isStringLiteral(node.argumentExpression) && node.argumentExpression.text === "authMode"
      );
    }
    return false;
  };
  const isOld = (node: TS.Node): node is TS.StringLiteral | TS.NoSubstitutionTemplateLiteral =>
    (ts.isStringLiteral(node) || ts.isNoSubstitutionTemplateLiteral(node)) &&
    node.text === OLD_AUTH_MODE;
  const authMode = (node: TS.StringLiteral | TS.NoSubstitutionTemplateLiteral) =>
    replaceString(node, NEW_AUTH_MODE, `authMode "${OLD_AUTH_MODE}" → "${NEW_AUTH_MODE}"`);

  /** Imported `LocalFirstGuest` from jazz-tools/backend, under its local name when unaliased. */
  let guestLocal: string | undefined;
  for (const statement of source.statements) {
    if (!ts.isImportDeclaration(statement) || !ts.isStringLiteral(statement.moduleSpecifier))
      continue;
    if (statement.moduleSpecifier.text !== "jazz-tools/backend") continue;
    const named = statement.importClause?.namedBindings;
    if (!named || !ts.isNamedImports(named)) continue;
    for (const element of named.elements) {
      const imported = element.propertyName ?? element.name;
      if (imported.text !== "LocalFirstGuest") continue;
      if (element.propertyName) {
        edits.push({
          start: imported.getStart(source),
          end: imported.getEnd(),
          text: "DeviceAccountsAnswer",
        });
      } else guestLocal = "LocalFirstGuest";
      changes.push({
        line: line(element),
        what: "`LocalFirstGuest` → `DeviceAccountsAnswer` (deprecated name)",
      });
    }
  }

  const visit = (node: TS.Node): void => {
    // `x.authMode === "local-first"`, either side.
    if (ts.isBinaryExpression(node)) {
      const op = node.operatorToken.kind;
      const equality =
        op === ts.SyntaxKind.EqualsEqualsEqualsToken ||
        op === ts.SyntaxKind.ExclamationEqualsEqualsToken ||
        op === ts.SyntaxKind.EqualsEqualsToken ||
        op === ts.SyntaxKind.ExclamationEqualsToken;
      if (equality && isOld(node.right) && isAuthMode(node.left)) authMode(node.right);
      if (equality && isOld(node.left) && isAuthMode(node.right)) authMode(node.left);
    }
    // `eq(session.authMode, "local-first")`, `ne(…)`, `isIn(session.authMode, [...])`.
    if (
      ts.isCallExpression(node) &&
      ts.isIdentifier(node.expression) &&
      node.arguments.length === 2
    ) {
      const fn = node.expression.text;
      const [left, right] = node.arguments as unknown as [TS.Expression, TS.Expression];
      if ((fn === "eq" || fn === "ne") && isAuthMode(left) && isOld(right)) authMode(right);
      if ((fn === "eq" || fn === "ne") && isAuthMode(right) && isOld(left)) authMode(left);
      if (fn === "isIn" && isAuthMode(left) && ts.isArrayLiteralExpression(right)) {
        for (const element of right.elements) if (isOld(element)) authMode(element);
      }
    }
    // `switch (x.authMode) { case "local-first": … }`
    if (
      ts.isCaseClause(node) &&
      isOld(node.expression) &&
      isAuthMode(node.parent.parent.expression)
    ) {
      authMode(node.expression);
    }
    // `{ authMode: "local-first" }`, as in `createPolicyTestApp().as({ … })`.
    if (
      ts.isPropertyAssignment(node) &&
      isOld(node.initializer) &&
      ((ts.isIdentifier(node.name) && node.name.text === "authMode") ||
        (ts.isStringLiteral(node.name) && node.name.text === "authMode"))
    ) {
      authMode(node.initializer);
    }
    // `AccountAuthError` codes, wherever the string appears.
    if (
      (ts.isStringLiteral(node) || ts.isNoSubstitutionTemplateLiteral(node)) &&
      ERROR_CODES[node.text]
    ) {
      replaceString(
        node,
        ERROR_CODES[node.text]!,
        `error code ${node.text} → ${ERROR_CODES[node.text]}`,
      );
    }
    // `db.getLocalFirstIdentityProof(…)` (deprecated name).
    if (ts.isPropertyAccessExpression(node) && node.name.text === "getLocalFirstIdentityProof") {
      edits.push({
        start: node.name.getStart(source),
        end: node.name.getEnd(),
        text: "getDeviceAccountProof",
      });
      changes.push({
        line: line(node.name),
        what: "`getLocalFirstIdentityProof` → `getDeviceAccountProof` (deprecated name)",
      });
    }
    // `LocalFirstGuest`, imported unaliased: its uses (types and the import itself).
    if (guestLocal && ts.isIdentifier(node) && node.text === guestLocal) {
      const parent = node.parent;
      const isKey =
        (ts.isPropertyAccessExpression(parent) && parent.name === node) ||
        (ts.isPropertyAssignment(parent) && parent.name === node) ||
        (ts.isQualifiedName(parent) && parent.right === node);
      if (!isKey)
        edits.push({
          start: node.getStart(source),
          end: node.getEnd(),
          text: "DeviceAccountsAnswer",
        });
    }
    ts.forEachChild(node, visit);
  };
  visit(source);
  if (edits.length === 0) return { text, changes: [] };
  // One edit per span: the passes above may meet the same literal twice.
  const unique = [...new Map(edits.map((e) => [`${e.start}:${e.end}`, e])).values()];
  return { text: applyEdits(text, unique), changes: dedupe(changes) };
}

function dedupe(changes: SourceRewrite["changes"]): SourceRewrite["changes"] {
  const seen = new Set<string>();
  return changes.filter((c) => {
    const key = `${c.line}:${c.what}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

/** A package.json script with the CLI's deprecated `--allow-local-first-auth` renamed. */
export function rewriteScript(script: string): string {
  return script.replace(/(^|\s)--allow-local-first-auth(?=$|[\s=])/g, "$1--allow-device-accounts");
}

const SCRIPT_BLOCK = /(<script\b[^>]*>)([\s\S]*?)(<\/script>)/g;

/** A project file with the renames applied: a module, or the script blocks of a .svelte/.vue/.html file. */
export function rewriteFile(ts: Ts, file: string, text: string): SourceRewrite {
  if (!/\.(svelte|vue|html)$/.test(file)) return rewriteSource(ts, file, text);
  const edits: TextEdit[] = [];
  const changes: SourceRewrite["changes"] = [];
  for (const match of text.matchAll(SCRIPT_BLOCK)) {
    const offset = match.index! + match[1]!.length;
    const code = match[2]!;
    const out = rewriteSource(ts, `${file}.ts`, code);
    if (out.text === code) continue;
    edits.push({ start: offset, end: offset + code.length, text: out.text });
    const before = text.slice(0, offset).split("\n").length - 1;
    changes.push(...out.changes.map((c) => ({ line: c.line + before, what: c.what })));
  }
  return { text: edits.length ? applyEdits(text, edits) : text, changes };
}

/** Where a shell script or CI file passes the CLI's deprecated flag: reported, not edited. */
export function flagMentions(text: string): number[] {
  const lines: number[] = [];
  text.split("\n").forEach((l, i) => {
    if (/(^|\s)--allow-local-first-auth(?=$|[\s=])/.test(l)) lines.push(i + 1);
  });
  return lines;
}
