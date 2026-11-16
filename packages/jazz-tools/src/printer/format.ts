/**
 * Formatting printed TS: with the app's own prettier when it has one, and
 * otherwise with a small layout close to prettier's (two-space indent,
 * trailing commas, groups broken at 100 columns, objects written expanded
 * stay expanded).
 */
import { createRequire } from "node:module";
import { join } from "node:path";
import { pathToFileURL } from "node:url";

export const PRINT_WIDTH = 100;

interface Prettier {
  format(source: string, options: Record<string, unknown>): Promise<string> | string;
  resolveConfig?(file: string): Promise<Record<string, unknown> | null>;
}

/** The app's prettier, if it resolves from `projectDir`. */
export async function loadPrettier(projectDir: string): Promise<Prettier | undefined> {
  try {
    const require = createRequire(join(projectDir, "package.json"));
    const path = require.resolve("prettier");
    const mod = (await import(pathToFileURL(path).href)) as Prettier & { default?: Prettier };
    return typeof mod.format === "function" ? mod : mod.default;
  } catch {
    return undefined;
  }
}

/** Format `source` as `file` (absolute, for the app's prettier config). */
export async function formatTs(
  source: string,
  options: { file?: string; projectDir?: string; prettier?: Prettier | null } = {},
): Promise<string> {
  const prettier =
    options.prettier === null
      ? undefined
      : (options.prettier ??
        (options.projectDir ? await loadPrettier(options.projectDir) : undefined));
  if (prettier) {
    const file = options.file ?? join(options.projectDir ?? process.cwd(), "schema.ts");
    const config = (await prettier.resolveConfig?.(file)) ?? {};
    return prettier.format(source, {
      printWidth: PRINT_WIDTH,
      ...config,
      filepath: file,
      parser: "typescript",
    });
  }
  return layout(source);
}

// ---------------------------------------------------------------------------
// The fallback layout
// ---------------------------------------------------------------------------

type Node =
  | { kind: "text"; text: string }
  | { kind: "comment"; text: string }
  | { kind: "newline" }
  | {
      kind: "group";
      open: string;
      close: string;
      items: Node[][];
      expanded: boolean;
      trailingComma: boolean;
    };

const CLOSE: Record<string, string> = { "(": ")", "{": "}", "[": "]" };

/** Split into strings, comments, brackets, commas, newlines and other text. */
function tokens(source: string): string[] {
  const out: string[] = [];
  let at = 0;
  while (at < source.length) {
    const c = source[at]!;
    if (c === '"' || c === "'" || c === "`") {
      let end = at + 1;
      while (end < source.length && source[end] !== c) end += source[end] === "\\" ? 2 : 1;
      out.push(source.slice(at, end + 1));
      at = end + 1;
    } else if (source.startsWith("//", at)) {
      const end = source.indexOf("\n", at);
      out.push(source.slice(at, end < 0 ? source.length : end));
      at = end < 0 ? source.length : end;
    } else if (source.startsWith("=>", at)) {
      out.push(" => ");
      at += 2;
    } else if ("(){}[],;\n".includes(c)) {
      out.push(c);
      at++;
    } else {
      let end = at;
      while (
        end < source.length &&
        !"(){}[],;\n\"'`".includes(source[end]!) &&
        !source.startsWith("//", end) &&
        !source.startsWith("=>", end)
      )
        end++;
      out.push(source.slice(at, end));
      at = end;
    }
  }
  return out;
}

function parse(toks: string[], at: { i: number }, close?: string): Node[][] {
  const items: Node[][] = [[]];
  while (at.i < toks.length) {
    const t = toks[at.i++]!;
    if (t === close) return items;
    const current = items[items.length - 1]!;
    if (t in CLOSE) {
      const expanded = close !== undefined || t === "{" ? toks[at.i] === "\n" : false;
      const inner = parse(toks, at, CLOSE[t]);
      const last = inner[inner.length - 1]!;
      const trailingComma = inner.length > 1 && last.every((n) => n.kind === "newline");
      if (trailingComma) inner.pop();
      current.push({
        kind: "group",
        open: t,
        close: CLOSE[t]!,
        items: inner,
        expanded,
        trailingComma,
      });
    } else if (t === "," && close !== undefined) {
      items.push([]);
    } else if (t === "\n") {
      current.push({ kind: "newline" });
    } else if (t.startsWith("//")) {
      current.push({ kind: "comment", text: t });
    } else if (t.trim() !== "") {
      current.push({ kind: "text", text: t });
    }
  }
  return items;
}

/** The flat text of nodes, or undefined when they can't be flat (comments, forced breaks). */
function flat(nodes: Node[]): string | undefined {
  let out = "";
  for (const node of nodes) {
    if (node.kind === "newline") {
      out += " ";
      continue;
    }
    if (node.kind === "comment") return undefined;
    if (node.kind === "text") {
      out += node.text;
      continue;
    }
    if (node.expanded) return undefined;
    const parts = node.items.map(flat);
    if (parts.some((p) => p === undefined)) return undefined;
    const inner = parts.map((p) => p!.trim()).filter((p, i) => p !== "" || i < parts.length - 1);
    const body = inner.join(", ");
    const pad = node.open === "{" && body ? " " : "";
    out += `${node.open}${pad}${body}${pad}${node.close}`;
  }
  return out.replace(/ +/g, " ");
}

function print(nodes: Node[], indent: number, column: number, rest = 0): string {
  const asFlat = flat(nodes);
  if (asFlat !== undefined && column + asFlat.trim().length + rest <= PRINT_WIDTH)
    return asFlat.trim();
  // `key: (row) => body`: break after the arrow, as prettier does, unless the body hugs an object.
  const arrow = nodes.findIndex((n) => n.kind === "text" && n.text.trim() === "=>");
  if (arrow >= 0) {
    const head = flat(nodes.slice(0, arrow + 1));
    const body = trimNewlines(nodes.slice(arrow + 1));
    const first = body[0];
    const hugs = body.length === 1 && first?.kind === "group" && first.open === "{";
    if (head !== undefined && !hugs) {
      const pad = " ".repeat(indent + 2);
      return `${head.trim()}\n${pad}${print(body, indent + 2, indent + 2, rest)}`;
    }
  }
  const chain = memberChain(nodes, indent, rest);
  if (chain !== undefined) return chain;
  let out = "";
  nodes.forEach((node, index) => {
    const col = column + (out.length - (out.lastIndexOf("\n") + 1));
    if (node.kind === "newline") {
      if (out && !out.endsWith(" ")) out += " ";
      return;
    }
    if (node.kind === "comment") {
      out += `${node.text}\n${" ".repeat(indent)}`;
      return;
    }
    if (node.kind === "text") {
      out += node.text
        .replace(/^\s+/, out === "" || out.endsWith("\n" + " ".repeat(indent)) ? "" : " ")
        .replace(/\s+/g, " ");
      return;
    }
    const after = flat(nodes.slice(index + 1));
    const tail = (after?.trimEnd().length ?? 0) + rest;
    const f = flat([node]);
    if (f !== undefined && col + f.length + tail <= PRINT_WIDTH) {
      out += f;
      return;
    }
    const items = node.items
      .filter((item) => item.some((n) => n.kind !== "newline"))
      .map(trimNewlines);
    const isHuggable = (item: Node[] | undefined) =>
      item?.length === 1 && item[0]!.kind === "group" && item[0]!.open !== "(";
    // A sole object (or array) argument hugs its parentheses: `f({ … })`.
    if (node.open === "(" && items.length === 1 && isHuggable(items[0])) {
      out += `(${print(items[0]!, indent, col + 1, tail + 1)})`;
      return;
    }
    // So does a last object argument after short ones: `anyOf(a, {`.
    if (node.open === "(" && items.length > 1 && isHuggable(items[items.length - 1])) {
      const firsts = items.slice(0, -1);
      const flats = firsts.map(flat);
      const objects = firsts.some(
        (i) => i.length === 1 && i[0]!.kind === "group" && i[0]!.open === "{",
      );
      const prefix = flats.every((f) => f !== undefined)
        ? `(${flats.map((f) => f!.trim()).join(", ")}, `
        : undefined;
      const last = items[items.length - 1]![0] as Extract<Node, { kind: "group" }>;
      if (
        prefix !== undefined &&
        !objects &&
        col + prefix.length + 1 <= PRINT_WIDTH &&
        !last.expanded
      ) {
        const hugged = print([{ ...last, expanded: true }], indent, col + prefix.length, tail + 1);
        out += `${prefix}${hugged})`;
        return;
      }
    }
    const pad = " ".repeat(indent + 2);
    out += node.open;
    for (const item of items) out += `\n${pad}${print(item, indent + 2, indent + 2, 1)},`;
    out += `\n${" ".repeat(indent)}${node.close}`;
  });
  return out.trimEnd();
}

/**
 * `s.table(…).indexOnly(…)` that doesn't fit: prettier's member chain, one
 * call per line after the head.
 */
function memberChain(nodes: Node[], indent: number, rest: number): string | undefined {
  const content = nodes.filter((n) => n.kind !== "newline");
  // text group (text group)+, where each later text starts with "."
  if (content.length < 4 || content.length % 2 !== 0) return undefined;
  for (let i = 0; i < content.length; i += 2) {
    const text = content[i]!;
    const group = content[i + 1]!;
    if (text.kind !== "text" || group.kind !== "group" || group.open !== "(") return undefined;
    if (i > 0 && !text.text.trim().startsWith(".")) return undefined;
  }
  const first = (content[0] as { text: string }).text.trim();
  const dot = first.lastIndexOf(".");
  if (dot <= 0) return undefined;
  const pad = " ".repeat(indent + 2);
  const calls: Node[][] = [[{ kind: "text", text: first.slice(dot) }, content[1]!]];
  for (let i = 2; i < content.length; i += 2) calls.push([content[i]!, content[i + 1]!]);
  let out = first.slice(0, dot);
  calls.forEach((call, i) => {
    out += `\n${pad}${print(call, indent + 2, indent + 2, i === calls.length - 1 ? rest : 0)}`;
  });
  return out;
}

function trimNewlines(nodes: Node[]): Node[] {
  let start = 0;
  let end = nodes.length;
  while (start < end && nodes[start]!.kind === "newline") start++;
  while (end > start && nodes[end - 1]!.kind === "newline") end--;
  return nodes.slice(start, end);
}

/** Lay out generated TS statement by statement. */
export function layout(source: string): string {
  const toks = tokens(source);
  const out: string[] = [];
  let statement: string[] = [];
  let depth = 0;
  let blank = false;
  const flush = (end: string) => {
    const nodes = trimNewlines(parse(statement, { i: 0 })[0]!);
    if (nodes.length) out.push(print(nodes, 0, 0) + end);
    statement = [];
  };
  for (let i = 0; i < toks.length; i++) {
    const t = toks[i]!;
    if (t in CLOSE) depth++;
    if (t === ")" || t === "}" || t === "]") depth--;
    if (depth === 0 && t === ";") {
      flush(";");
      blank = false;
      continue;
    }
    if (depth === 0 && t === "\n") {
      if (statement.every((s) => s === "\n" || s.trim() === "")) {
        if (statement.length || blank) {
          if (out.length && out[out.length - 1] !== "") out.push("");
        }
        blank = true;
        statement = [];
        continue;
      }
      if (statement.length === 1 && statement[0]!.startsWith("//")) {
        out.push(statement[0]!);
        statement = [];
        continue;
      }
    }
    statement.push(t);
  }
  flush("");
  while (out[out.length - 1] === "") out.pop();
  return out.join("\n") + "\n";
}
