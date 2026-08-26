import { existsSync, readdirSync, readFileSync } from "node:fs";
import { dirname, resolve, relative, sep } from "node:path";
import { fileURLToPath } from "node:url";

const packageRoot = fileURLToPath(new URL("..", import.meta.url));
const distRoot = resolve(packageRoot, "dist");
const sourceRoot = resolve(packageRoot, "src");

function fail(message) {
  throw new Error(`Runtime sourcemap verification failed: ${message}`);
}

function findMaps(directory) {
  return readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
    const entryPath = resolve(directory, entry.name);
    if (entry.isDirectory()) return findMaps(entryPath);
    return entry.name.endsWith(".js.map") ? [entryPath] : [];
  });
}

if (!existsSync(distRoot)) fail("missing dist directory");

const maps = findMaps(distRoot);
if (maps.length === 0) fail("no emitted JavaScript maps found");

for (const mapPath of maps) {
  const map = JSON.parse(readFileSync(mapPath, "utf8"));
  const sourcePath = mapPath.slice(0, -4);

  if (!existsSync(sourcePath)) {
    fail(`${relative(packageRoot, mapPath)} has no emitted JavaScript file`);
  }
  if (!Array.isArray(map.sources) || map.sources.length === 0) {
    fail(`${relative(packageRoot, mapPath)} has no sources`);
  }
  if (!Array.isArray(map.sourcesContent) || map.sourcesContent.length !== map.sources.length) {
    fail(`${relative(packageRoot, mapPath)} has incomplete sourcesContent`);
  }

  map.sources.forEach((source, index) => {
    if (typeof source !== "string" || typeof map.sourcesContent[index] !== "string") {
      fail(`${relative(packageRoot, mapPath)} has a non-text source entry`);
    }

    const resolvedSource = resolve(dirname(mapPath), map.sourceRoot || "", source);
    const sourceRelativeToRoot = relative(sourceRoot, resolvedSource);
    if (
      sourceRelativeToRoot.startsWith(`..${sep}`) ||
      sourceRelativeToRoot === ".." ||
      !existsSync(resolvedSource)
    ) {
      fail(`${relative(packageRoot, mapPath)} points outside emitted package sources: ${source}`);
    }
    if (readFileSync(resolvedSource, "utf8") !== map.sourcesContent[index]) {
      fail(`${relative(packageRoot, mapPath)} embeds stale source content for ${source}`);
    }
  });
}
