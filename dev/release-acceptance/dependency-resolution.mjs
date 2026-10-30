import assert from "node:assert/strict";
import { realpathSync, existsSync } from "node:fs";
import { createRequire } from "node:module";
import { join, sep } from "node:path";

/** Probe every verified module directory before any Jazz code is loaded. */
export function verifyJazzResolutions(packageDirs, moduleDirectories) {
  for (const directory of moduleDirectories) {
    const require = createRequire(join(directory, "__jazz_acceptance_probe__.cjs"));
    for (const [name, verifiedDirectory] of packageDirs) {
      const verified = realpathSync(verifiedDirectory);
      // Check the package directory as well as its resolved entry. An unchecked
      // nested package must not disguise itself with a main/require entry that
      // redirects to verified bytes while its import entry points elsewhere.
      const located = require.resolve
        .paths(name)
        ?.map((base) => join(base, name))
        .find((candidate) => existsSync(join(candidate, "package.json")));
      assert(
        located && realpathSync(located) === verified,
        `Unverified Jazz dependency resolution: ${name} package from ${directory}`,
      );
      const selected = realpathSync(require.resolve(name));
      assert(
        selected.startsWith(`${verified}${sep}`) &&
          !selected
            .slice(verified.length + 1)
            .split(sep)
            .includes("node_modules"),
        `Unverified Jazz dependency resolution: ${name} from ${directory} selects ${selected}`,
      );
    }
  }
}
