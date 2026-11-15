/**
 * `package.json` `imports` aliases (`"#app": "./schema.ts"`). Code that
 * imports the schema or permissions through one keeps the alias, and the
 * alias is pointed at the new file, so every importer, including ones
 * upgrade can't see (scripts, tools), follows.
 */
import { existsSync, readFileSync } from "node:fs";
import { dirname, join, relative, resolve, sep } from "node:path";

export interface AliasEdit {
  /** The package.json. */
  file: string;
  alias: string;
  from: string;
  to: string;
}

export class PackageAliases {
  private readonly edits = new Map<string, AliasEdit>();

  constructor(private readonly root: string) {}

  /**
   * Point `alias` (as written in `importer`) from `from` at `to`, when the
   * importer's package maps it straight to `from`. Returns whether the alias
   * now names `to` and can stay as written.
   */
  retarget(alias: string, importer: string, from: string, to: string): boolean {
    if (!alias.startsWith("#")) return false;
    const manifest = this.manifestFor(importer);
    if (!manifest) return false;
    const imports = (
      JSON.parse(readFileSync(manifest, "utf8")) as { imports?: Record<string, unknown> }
    ).imports;
    const target = imports?.[alias];
    // Only a plain string mapping; conditions and patterns stay as they are.
    if (typeof target !== "string") return false;
    const dir = dirname(manifest);
    const key = `${manifest}\0${alias}`;
    const existing = this.edits.get(key);
    if (existing) return sameFile(resolve(dir, existing.to), to);
    if (!sameFile(resolve(dir, target), from)) return false;
    let next = relative(dir, to).split(sep).join("/");
    if (!next.startsWith(".")) next = `./${next}`;
    this.edits.set(key, { file: manifest, alias, from: target, to: next });
    return true;
  }

  /** The package.json files to rewrite, as their new text (from `base` when given). */
  apply(base: Map<string, string> = new Map()): Map<string, { text: string; changes: string[] }> {
    const out = new Map<string, { text: string; changes: string[] }>();
    for (const edit of this.edits.values()) {
      const current = out.get(edit.file) ?? {
        text: base.get(edit.file) ?? readFileSync(edit.file, "utf8"),
        changes: [],
      };
      const pattern = new RegExp(
        `(${escape(JSON.stringify(edit.alias))}\\s*:\\s*)${escape(JSON.stringify(edit.from))}`,
      );
      current.text = current.text.replace(pattern, `$1${JSON.stringify(edit.to)}`);
      current.changes.push(
        `${relative(this.root, edit.file) || "package.json"}: imports ${edit.alias}: ${edit.from} → ${edit.to}`,
      );
      out.set(edit.file, current);
    }
    return out;
  }

  private manifestFor(file: string): string | undefined {
    for (let dir = dirname(file); ; dir = dirname(dir)) {
      const manifest = join(dir, "package.json");
      if (existsSync(manifest)) {
        const parsed = JSON.parse(readFileSync(manifest, "utf8")) as { imports?: unknown };
        // The nearest package.json scopes `#` imports, whether or not it has a map.
        return parsed.imports ? manifest : undefined;
      }
      if (dir === this.root || dirname(dir) === dir) return undefined;
    }
  }
}

/** Whether two paths name the same module (`./schema.ts`, `./schema.js` and `./schema` do). */
function sameFile(a: string, b: string): boolean {
  const stem = (p: string) => p.replace(/\.(ts|tsx|mts|js|jsx|mjs|json)$/, "");
  return a === b || stem(a) === stem(b);
}

function escape(text: string): string {
  return text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}
