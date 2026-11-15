/**
 * Excerpts of generated bindings by symbol. Generated files can't carry
 * `#region` markers (regenerating must not change them), so the docs name the
 * declarations to show instead: `<BindingsExcerpt>` in a page becomes a code
 * block of those declarations, each from its first line to its matching close,
 * with the doc comments and attributes right above it.
 */

const DECLARATION = (name: string) =>
  new RegExp(
    // TS `export type X =`, Rust `pub struct X`/`pub enum X`, Swift `struct X`/`enum X`,
    // Kotlin `data class X(`/`enum class X`/`object X`, Go `type X struct`/`type X string`.
    String.raw`^[ \t]*(?:export\s+|pub(?:\([^)]*\))?\s+|public\s+|internal\s+)?` +
      String.raw`(?:(?:data|enum|sealed|value|final)\s+)*` +
      String.raw`(?:type|struct|enum|class|object|interface|protocol)\s+${name}\b`,
    "m",
  );

const LEADING = /^\s*(?:#\[|@|\/\/\/|\/\*\*|\*|\/\/)/;

/** The declaration of `name` in `source`, or undefined if there's none. */
export function extractSymbol(source: string, name: string): string | undefined {
  const match = DECLARATION(name).exec(source);
  if (!match) return undefined;
  const lines = source.split("\n");
  let first = source.slice(0, match.index).split("\n").length - 1;
  let last = first;

  // To the close that matches the first opener, then to the end of that line,
  // unless the line opens again (`data class X(…) {`).
  let depth = 0;
  let opened = false;
  scan: for (let i = first; i < lines.length; i++) {
    for (const ch of lines[i]) {
      if ("{([".includes(ch)) {
        depth++;
        opened = true;
      } else if ("})]".includes(ch)) depth--;
    }
    last = i;
    if (depth <= 0 && (opened || i === first)) {
      // A one-line declaration (`type X string`, `export type X = "a" | "b";`) ends here.
      break scan;
    }
  }

  while (first > 0 && LEADING.test(lines[first - 1]) && lines[first - 1].trim() !== "") first--;
  return lines.slice(first, last + 1).join("\n");
}

/** The declarations of `names`, in order, separated by blank lines. Throws on a missing one. */
export function extractSymbols(source: string, names: string[], file = "bindings"): string {
  return names
    .map((name) => {
      const found = extractSymbol(source, name);
      if (!found) throw new Error(`${file}: no declaration of ${name} to excerpt`);
      return found;
    })
    .join("\n\n");
}

const TAG = /^([ \t]*)<BindingsExcerpt\s+([^>]*?)\/>[ \t]*$/gm;

export type BindingsSource = { file: string; lang: string; symbols: string[] };

/**
 * Replaces each `<BindingsExcerpt language="Rust" />` line with a fenced code
 * block of that language's `bindings` declarations (see `LANGUAGES`).
 * `readFile` reads a file relative to the repository root.
 */
export function expandBindingsExcerpts(
  page: string,
  bindingsOf: (language: string) => BindingsSource | undefined,
  readFile: (file: string) => string,
  pageFile = "page",
): string {
  return page.replace(TAG, (_, indent: string, attrs: string) => {
    const language = /\blanguage="([^"]*)"/.exec(attrs)?.[1];
    const bindings = language ? bindingsOf(language) : undefined;
    if (!bindings)
      throw new Error(`${pageFile}: <BindingsExcerpt language="${language}"> has no bindings in docs/lib/languages`);
    const code = extractSymbols(readFile(bindings.file), bindings.symbols, bindings.file);
    const title = bindings.file.split("/").pop();
    const fence = "```";
    return [`${fence}${bindings.lang} title="${title}"`, ...code.split("\n"), fence]
      .map((l) => (l ? indent + l : l))
      .join("\n");
  });
}
