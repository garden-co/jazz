import { readFileSync } from "node:fs";
import { join } from "node:path";

// The jazz-tools version the docs describe, from the package in this repo, so version strings in
// pages can't go stale: `%jazz.version%` becomes e.g. `2.0.0-alpha.62` in text and in link URLs.
// Docs deploy after a release's back-merge, so this is the version on npm.
let version: string | undefined;

export function jazzVersion(): string {
  version ??= (
    JSON.parse(
      readFileSync(join(process.cwd(), "../packages/jazz-tools/package.json"), "utf8"),
    ) as { version: string }
  ).version;
  return version;
}

/** Fills every `%jazz.…%` token; an unknown one fails the build rather than reaching a page. */
export function replaceVersionTokens(text: string, current = jazzVersion()): string {
  return text.replace(/%jazz\.[A-Za-z]*%?/g, (token) => {
    if (token !== "%jazz.version%")
      throw new Error(`Unknown token ${token}: the only one is %jazz.version%.`);
    return current;
  });
}
