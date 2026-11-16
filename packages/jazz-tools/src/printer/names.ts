/** Spelling names in printed TS. */

const RESERVED = new Set([
  "break",
  "case",
  "catch",
  "class",
  "const",
  "continue",
  "debugger",
  "default",
  "delete",
  "do",
  "else",
  "enum",
  "export",
  "extends",
  "false",
  "finally",
  "for",
  "function",
  "if",
  "import",
  "in",
  "instanceof",
  "new",
  "null",
  "return",
  "super",
  "switch",
  "this",
  "throw",
  "true",
  "try",
  "typeof",
  "var",
  "void",
  "while",
  "with",
  "yield",
  "let",
  "static",
  "implements",
  "interface",
  "package",
  "private",
  "protected",
  "public",
  "await",
  "async",
  "of",
  "undefined",
  "arguments",
  "eval",
]);

export function isIdentifier(name: string): boolean {
  return /^[A-Za-z_$][A-Za-z0-9_$]*$/.test(name);
}

/** A usable variable name. */
export function isBindable(name: string): boolean {
  return isIdentifier(name) && !RESERVED.has(name);
}

/** `name` as an object key. */
export function propKey(name: string): string {
  return isIdentifier(name) ? name : JSON.stringify(name);
}

/** `base.name` or `base["name"]`. */
export function member(base: string, name: string): string {
  return isIdentifier(name) ? `${base}.${name}` : `${base}[${JSON.stringify(name)}]`;
}

export function pascal(name: string): string {
  return name
    .split(/[^A-Za-z0-9]+/)
    .filter(Boolean)
    .map((part) => part[0]!.toUpperCase() + part.slice(1))
    .join("");
}

export function camel(name: string): string {
  const p = pascal(name);
  return p ? p[0]!.toLowerCase() + p.slice(1) : p;
}

/** A rough singular of a table name: `chats` → `chat`, `canvases` → `canvas`. */
export function singular(name: string): string {
  const base = camel(name);
  const irregular: Record<string, string> = {
    people: "person",
    children: "child",
    men: "man",
    women: "woman",
  };
  for (const [plural, one] of Object.entries(irregular)) {
    if (base === plural) return one;
    if (base.endsWith(pascal(plural))) return base.slice(0, -plural.length) + pascal(one);
  }
  if (/[^aeiou]ies$/.test(base)) return base.slice(0, -3) + "y";
  if (/(ss|x|ch|sh|zz)es$/.test(base) || /[^bc](as|us|is)es$/.test(base)) return base.slice(0, -2);
  if (/[^s]s$/.test(base)) return base.slice(0, -1);
  return base;
}

/** A fresh name based on `base` that `taken` doesn't hold; adds it to `taken`. */
export function fresh(base: string, taken: Set<string>): string {
  let name = isBindable(base) ? base : `_${base}`;
  for (let i = 2; taken.has(name) || !isBindable(name); i++) name = `${base}${i}`;
  taken.add(name);
  return name;
}

/** A JSON value as a JS literal: `{ type: "object" }`, keys unquoted where they can be. */
export function jsLiteral(value: unknown): string {
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(jsLiteral).join(", ")}]`;
  const entries = Object.entries(value as Record<string, unknown>);
  if (entries.length === 0) return "{}";
  return `{ ${entries.map(([k, v]) => `${propKey(k)}: ${jsLiteral(v)}`).join(", ")} }`;
}
