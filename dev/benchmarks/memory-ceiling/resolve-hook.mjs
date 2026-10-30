// Module resolution for the memory-ceiling harness, registered with
// `module.register()` by every harness process.
//
// - `JAZZ_MEMCEIL_PACKAGES=<dir>` resolves the bare `jazz-tools`, `jazz-napi`
//   and `jazz-wasm` specifiers (and their subpaths) from that node_modules
//   directory, so an app's schema runs against a prebuilt release (an npm
//   alpha or a pkg.pr.new build) instead of the workspace packages.
// - Extensionless relative imports (`./src/roles`) try `.ts` and
//   `/index.ts`, as the app's bundler would, so Node's type stripping can
//   load the app's own schema and permissions unchanged.
import { existsSync, statSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const packagesDir = process.env.JAZZ_MEMCEIL_PACKAGES;
const redirected = /^(jazz-tools|jazz-napi|jazz-wasm)(\/.*)?$/;

export async function resolve(specifier, context, nextResolve) {
  if (packagesDir && redirected.test(specifier)) {
    // Resolve as if imported from a module inside the release directory, so
    // the package's own `exports` map still applies.
    return nextResolve(specifier, {
      ...context,
      parentURL: pathToFileURL(join(packagesDir, "__memceil__.mjs")).href,
    });
  }
  if ((specifier.startsWith("./") || specifier.startsWith("../")) && context.parentURL) {
    const base = new URL(specifier, context.parentURL);
    if (base.protocol === "file:") {
      const path = fileURLToPath(base);
      if (!existsSync(path) || statSync(path).isDirectory()) {
        for (const candidate of [`${path}.ts`, `${path}.tsx`, join(path, "index.ts")]) {
          if (existsSync(candidate)) return nextResolve(pathToFileURL(candidate).href, context);
        }
      }
      // `./schema.js` written for a TypeScript source next to it.
      if (!existsSync(path) && path.endsWith(".js") && existsSync(path.slice(0, -3) + ".ts")) {
        return nextResolve(pathToFileURL(path.slice(0, -3) + ".ts").href, context);
      }
    }
  }
  return nextResolve(specifier, context);
}
