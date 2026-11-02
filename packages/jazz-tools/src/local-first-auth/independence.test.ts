import { readdirSync, readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

// This module is functionally a separate library: it may import its own files
// and its npm dependencies, never the rest of jazz-tools.
const ALLOWED_PACKAGES = ["@noble/curves", "@noble/hashes", "@scure/bip39", "vitest", "node:fs"];

describe("local-first-auth module boundary", () => {
  const dir = new URL("./", import.meta.url);
  const files = readdirSync(dir).filter((name) => name.endsWith(".ts"));

  it.each(files)("%s imports only its own files and its dependencies", (name) => {
    const source = readFileSync(new URL(name, dir), "utf8");
    const specifiers = [
      ...source.matchAll(/\bfrom\s+["']([^"']+)["']|\bimport\(\s*["']([^"']+)["']/g),
    ].map((match) => match[1] ?? match[2]!);
    for (const specifier of specifiers) {
      if (specifier.startsWith(".")) {
        expect(specifier, `${name} imports ${specifier}`).toMatch(/^\.\/[^/]+$/);
      } else {
        expect(
          ALLOWED_PACKAGES.some((pkg) => specifier === pkg || specifier.startsWith(`${pkg}/`)),
          `${name} imports ${specifier}`,
        ).toBe(true);
      }
    }
  });
});
