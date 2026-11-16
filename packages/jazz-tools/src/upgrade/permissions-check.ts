/**
 * Fail closed on permissions alpha.64 compiled from values that aren't in
 * the source. alpha.64's DSL ran permissions.ts when validate ran, so a
 * `process.env` read became whatever it was then, and a `where({ col:
 * undefined })` compiled to no condition at all: `"true"`. A policy that
 * allows more than its author wrote must not survive into the files, which
 * from alpha.65 are the only source.
 */
import { readFileSync } from "node:fs";
import { relative } from "node:path";
import type * as TS from "typescript";
import { isObject, parseJsonOrdered, entriesOf, type Json } from "./app-json.js";
import { UpgradeRefusal } from "./errors.js";
import type { Project } from "./project.js";

/**
 * Where schema.ts, permissions.ts or the project files they import read the
 * environment. The printed policies hold the values validate resolved them
 * to, so upgrade warns about each rather than refuse.
 */
export function environmentReads(project: Project, files: string[]): string[] {
  const { ts } = project;
  const found: string[] = [];
  for (const file of files) {
    const text = readFileSync(file, "utf8");
    if (!/\b(?:process\.env|import\.meta\.env)\b/.test(text)) continue;
    const source = ts.createSourceFile(file, text, ts.ScriptTarget.Latest, true);
    const visit = (node: TS.Node): void => {
      if (ts.isPropertyAccessExpression(node) && node.name.text === "env") {
        const target = node.expression;
        const isProcess = ts.isIdentifier(target) && target.text === "process";
        const isImportMeta = ts.isMetaProperty(target);
        if (isProcess || isImportMeta) {
          const parent = node.parent;
          const read =
            ts.isPropertyAccessExpression(parent) && parent.expression === node
              ? parent.getText(source)
              : ts.isElementAccessExpression(parent) && parent.expression === node
                ? parent.getText(source)
                : node.getText(source);
          const { line } = source.getLineAndCharacterOfPosition(node.getStart(source));
          found.push(`${relative(project.root, file) || file}:${line + 1}: reads \`${read}\``);
          return;
        }
      }
      ts.forEachChild(node, visit);
    };
    visit(source);
  }
  return found;
}

/** Refuse `"true"` inside `anyOf` / `allOf` of jazz.permissions.json: the trace of a condition that compiled to nothing. */
export function refuseDroppedConditions(project: Project): void {
  let permissions: Json;
  try {
    permissions = parseJsonOrdered(readFileSync(project.permissionsJsonFile, "utf8"));
  } catch {
    return; // the old validate already refused an unreadable file
  }
  const found: string[] = [];
  const walk = (value: Json, path: string, combined: boolean): void => {
    if (value === "true" && combined) found.push(path);
    else if (Array.isArray(value))
      value.forEach((item, i) => walk(item, `${path}[${i}]`, combined));
    else if (isObject(value)) {
      for (const [key, item] of entriesOf(value)) {
        if (key === "$comment") continue;
        walk(item, path ? `${path}.${key}` : key, combined || key === "anyOf" || key === "allOf");
      }
    }
  };
  if (isObject(permissions)) {
    for (const [key, value] of entriesOf(permissions)) {
      if (key === "tables") walk(value, key, false);
    }
  }
  if (found.length === 0) return;
  throw new UpgradeRefusal(
    `jazz.permissions.json has \`true\` inside anyOf/allOf at ${found.join(", ")}. alpha.64 writes that for a condition that ` +
      "compiled to nothing, typically `where({ column: value })` with `value` undefined when validate ran, so the policy " +
      "allows more than permissions.ts says. Fix the condition in permissions.ts (or, if the policy really is unconditional, " +
      "write it as `.always()` alone), run `npx jazz-tools validate` with the app's alpha.64, then upgrade again.",
    relative(project.root, project.permissionsJsonFile),
  );
}
