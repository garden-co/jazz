import { access, readdir, rm } from "node:fs/promises";
import { constants as fsConstants } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const packageRoot = fileURLToPath(new URL("..", import.meta.url));
const srcDir = join(packageRoot, "src");

async function fileExists(path) {
  try {
    await access(path, fsConstants.F_OK);
    return true;
  } catch {
    return false;
  }
}

async function collectFiles(dir, out = []) {
  const entries = await readdir(dir, { withFileTypes: true });
  for (const entry of entries) {
    const fullPath = join(dir, entry.name);
    if (entry.isDirectory()) {
      await collectFiles(fullPath, out);
    } else {
      out.push(fullPath);
    }
  }
  return out;
}

async function cleanupGeneratedSourceTypes() {
  const files = await collectFiles(srcDir);
  const declarationFiles = files.filter((file) => file.endsWith(".d.ts"));

  let removed = 0;
  for (const declarationPath of declarationFiles) {
    const basePath = declarationPath.slice(0, -".d.ts".length);
    const sourceExists =
      (await fileExists(`${basePath}.ts`)) || (await fileExists(`${basePath}.tsx`));
    if (!sourceExists) {
      continue;
    }

    await rm(declarationPath, { force: true });
    await rm(`${declarationPath}.map`, { force: true });
    removed += 1;
  }

  if (removed > 0) {
    console.log(`[build:svelte] removed ${removed} generated src declaration files`);
  }
}

await cleanupGeneratedSourceTypes();
