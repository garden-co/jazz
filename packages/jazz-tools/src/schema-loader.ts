/**
 * Load an app's TypeScript (`schema.ts`, `permissions.ts`, `migrations/*.ts`)
 * for `jazz-tools generate`: bundle it with esbuild, run the bundle, read its
 * exports. `jazz-tools` resolves to the CLI's own copy, so `npx jazz-tools`
 * works in a project without node_modules. Node only.
 */
import { existsSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { build, type Plugin } from "esbuild";

// The authoring DSL alone (`schema`, `migration`): app TS loaded for
// `generate` never needs the runtime.
function localJazzToolsEntry(): string {
  const source = fileURLToPath(new URL("./authoring.ts", import.meta.url));
  return existsSync(source) ? source : fileURLToPath(new URL("./authoring.js", import.meta.url));
}

async function removeTempDirectory(tempDir: string): Promise<void> {
  await rm(tempDir, { force: true, recursive: true });
}

export const schemaLoaderTestHooks: {
  removeTempDirectory: (tempDir: string) => Promise<void>;
} = {
  removeTempDirectory,
};

/**
 * `jazz-tools` resolves to this copy of the package's authoring DSL, so
 * `npx jazz-tools generate` works in a project without node_modules, and
 * the app's TS and the CLI share one DSL.
 */
export function localJazzToolsPlugin(): Plugin {
  return {
    name: "local-jazz-tools",
    setup(build) {
      build.onResolve({ filter: /^jazz-tools$/ }, () => ({ path: localJazzToolsEntry() }));
      build.onResolve({ filter: /^file:\/\// }, (args) => ({ path: fileURLToPath(args.path) }));
    },
  };
}

async function bundleToTempFile(filePath: string, tempDir: string): Promise<string> {
  const outFile = join(tempDir, `${basename(filePath).replace(/\.[cm]?[jt]sx?$/, "")}.mjs`);
  await build({
    entryPoints: [resolve(filePath)],
    bundle: true,
    format: "esm",
    platform: "node",
    outfile: outFile,
    // Stack traces point at the app's own lines: the permissions DSL names
    // the line a refused rule comes from.
    sourcemap: "inline",
    sourcesContent: false,
    logLevel: "silent",
    // jazz-napi must stay external: inlining its loader breaks the relative
    // require of the platform .node binding once the bundle runs from outside
    // the package directory.
    external: ["jazz-napi", "*.node"],
    plugins: [localJazzToolsPlugin()],
  });
  return outFile;
}

let sourceMapsEnabled = false;

/**
 * Run the TS module at `filePath` (bundled with what it imports) and return
 * its exports. Each load bundles afresh, so edits are always seen.
 */
export async function loadModule(filePath: string): Promise<Record<string, unknown>> {
  if (!sourceMapsEnabled) {
    sourceMapsEnabled = true;
    process.setSourceMapsEnabled?.(true);
  }
  const tempDir = await mkdtemp(join(tmpdir(), "jazz-schema-loader-"));
  let loadFailed = false;
  try {
    let outFile: string;
    try {
      outFile = await bundleToTempFile(filePath, tempDir);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      throw new Error(`Bundling ${basename(filePath)} failed: ${message}`, { cause: error });
    }
    try {
      // The output path is unique per load, so the import cache can't hand
      // back an earlier load.
      return { ...((await import(pathToFileURL(outFile).href)) as Record<string, unknown>) };
    } catch (error) {
      // The DSL's own refusals name their JSON path and line already.
      if ((error as { code?: string })?.code === "JAZZ_PERMISSIONS_DSL") throw error;
      const message = error instanceof Error ? error.message : String(error);
      throw new Error(`Loading ${basename(filePath)} failed: ${message}`, { cause: error });
    }
  } catch (error) {
    loadFailed = true;
    throw error;
  } finally {
    try {
      await schemaLoaderTestHooks.removeTempDirectory(tempDir);
    } catch (cleanupError) {
      if (!loadFailed) throw cleanupError;
    }
  }
}
