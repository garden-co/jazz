import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { createRequire } from "node:module";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const guide = new URL("../content/docs/e2ee/bandchat-walkthrough.mdx", import.meta.url);
const app = fileURLToPath(
  new URL("../../examples/band-chat/apps/react-hono-betterauth-e2ee/", import.meta.url),
);
const source = readFileSync(guide, "utf8");
const blocks = [...source.matchAll(/^```(tsx?)([^\n]*)\n([\s\S]*?)^```/gm)];
if (blocks.length === 0) throw new Error("BandChat's walkthrough has no TypeScript snippets");

const snippets = mkdtempSync(join(app, ".walkthrough-"));
try {
  for (const block of blocks) {
    const [, extension, metadata, code] = block;
    const line = source.slice(0, block.index).split("\n").length + 1;
    const title = /title="([\w-]+\.tsx?)"/.exec(metadata)?.[1];
    // Schema and permissions are paired excerpts. App imports resolve to the
    // actual generated bindings and permissions beside this scratch directory.
    writeFileSync(
      join(snippets, title ?? `walkthrough-line-${line}.${extension}`),
      code.replace(/(from\s+["'])\.\/(jazz\.generated|jazz\.permissions\.json)/g, "$1../$2"),
    );
  }
  const config = join(snippets, "tsconfig.json");
  writeFileSync(
    config,
    JSON.stringify({ extends: "../tsconfig.json", include: ["*.ts", "*.tsx"] }),
  );
  const tsc = createRequire(import.meta.url).resolve("typescript/bin/tsc");
  const { status, error } = spawnSync(process.execPath, [tsc, "-p", config], { stdio: "inherit" });
  if (error) throw error;
  process.exitCode = status ?? 1;
  if (status === 0) console.log(`Checked ${blocks.length} BandChat walkthrough snippets.`);
} finally {
  rmSync(snippets, { force: true, recursive: true });
}
