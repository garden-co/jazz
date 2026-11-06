import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

// What Next.js's bundlers (Turbopack, webpack) need of jazz-tools' backend,
// checked on the source since no bundler runs in these tests (both were
// checked by hand against Next 15 and 16):
// - An optional peer dependency is imported as a string literal inside
//   `try`. Then a missing package fails only where it's used, at runtime,
//   instead of every route that imports jazz-tools (`.catch()` on the
//   import is not enough for that), and an installed one is bundled.
// - Nothing finds a file through `import.meta.url` at runtime: bundlers
//   bake the build machine's path into it, which a deployed function lacks.
const sources = ["vercel-store.ts", "create-jazz.ts"].map((file) => ({
  file,
  text: readFileSync(new URL(file, import.meta.url), "utf8"),
}));
const OPTIONAL_PEERS = ["@vercel/sandbox", "@vercel/functions", "@vercel/functions/oidc"];

describe("backend imports under bundlers", () => {
  it("imports optional peers as literals inside try", () => {
    for (const peer of OPTIONAL_PEERS) {
      const imports = sources.flatMap(({ file, text }) =>
        [...text.matchAll(/await import\(\s*(?:\/\*[^*]*\*\/\s*)*"([^"]+)"([^)]*)\)/g)]
          .filter((match) => match[1] === peer)
          .map((match) => ({ file, text, at: match.index, rest: match[2] })),
      );
      expect(imports.length, peer).toBeGreaterThan(0);
      for (const { file, text, at, rest } of imports) {
        expect(rest.trim(), `${peer} in ${file}`).toBe("");
        const before = text.slice(0, at);
        const opened = before.lastIndexOf("try {");
        expect(opened, `${peer} in ${file} is not inside try`).toBeGreaterThan(-1);
        // No block closes between the `try {` and the import at the try's level.
        // A rough count: braces inside strings and comments count too, so keep
        // that stretch of code free of them.
        let depth = 0;
        for (const char of before.slice(opened + 4)) {
          if (char === "{") depth++;
          if (char === "}") depth--;
          expect(depth, `${peer} in ${file} is not inside try`).toBeGreaterThanOrEqual(1);
        }
        const after = text.slice(at, text.indexOf(";", at));
        expect(after, `${peer} in ${file}`).not.toContain(".catch(");
      }
    }
  });

  it("finds no file through import.meta.url at runtime", () => {
    for (const { file, text } of sources) {
      expect(text, file).not.toMatch(/createRequire\(import\.meta\.url\)/);
      expect(text, file).not.toMatch(/new URL\([^)]*import\.meta\.url\)/);
    }
  });
});
