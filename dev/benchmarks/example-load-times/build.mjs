// Production-builds the example apps with the env their benchmark servers use,
// then precompresses their hashed assets for the CDN front:
//
//   node build.mjs [<app> ...]        (default: every app in apps.mjs)
//   node build.mjs --outputs          (what to ship to the measurement host)
//
// Only the framework build runs: `next build` or `vite build`. The apps'
// `build` scripts also run `jazz-tools validate`, a check with no output, and
// big-label's hardcodes another env.
import { spawnSync } from "node:child_process";
import { rmSync } from "node:fs";
import { join, relative } from "node:path";
import { appConfig, appIds, generated, repo } from "./apps.mjs";
import { loadAssets } from "./cdn.mjs";

const ids = process.argv.slice(2);
if (ids[0] === "--outputs") {
  // Repository-relative build directories and the CDN's compressed copies.
  const dirs = appIds.map((id) => join(appConfig(id).dir, appConfig(id).buildDir));
  console.log([...dirs, join(generated, "cdn-cache")].map((d) => relative(repo, d)).join("\n"));
  process.exit(0);
}
let failed = 0;
for (const id of ids.length ? ids : appIds) {
  const a = appConfig(id);
  rmSync(join(a.dir, a.buildDir), { recursive: true, force: true });
  const t0 = Date.now();
  const r = spawnSync("pnpm", ["exec", a.kind, "build"], {
    cwd: a.dir,
    env: { ...process.env, ...a.env },
    stdio: "inherit",
  });
  console.log(
    `=== ${id}: ${a.kind} build exit ${r.status} in ${Math.round((Date.now() - t0) / 1000)} s`,
  );
  if (r.status !== 0) {
    failed++;
    continue;
  }
  const t1 = Date.now();
  const assets = loadAssets(a.staticDir);
  console.log(
    `=== ${id}: precompressed ${assets.size} assets in ${Math.round((Date.now() - t1) / 1000)} s`,
  );
}
if (failed) {
  console.error(`${failed} app build(s) failed`);
  process.exit(1);
}
