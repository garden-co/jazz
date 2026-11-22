// Downloads a candidate's release artifacts, fail closed.
// usage: node fetch.mjs <sha> <release-artifacts run id> <artifact>[,<artifact>...]
// Env: GH_TOKEN, LA_WORK. Writes $LA_WORK/candidate.json and $LA_WORK/artifacts/<name>/.
import { mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { REPO, WORK, die, fetchStep, filesUnder, sha256, timed } from "./lib/common.mjs";
import { checkCandidate, checkFiles, checkManifest, checkRun } from "./lib/provenance.mjs";

const [sha, run, names] = process.argv.slice(2);
try {
  checkCandidate({ sha, run });
  const want = { sha, run, repo: REPO };
  checkRun(JSON.parse(fetchStep("gh", ["api", `repos/${REPO}/actions/runs/${run}`])), want);
  console.log(`PROVENANCE run ${run} built ${sha}`);

  const root = join(WORK, "artifacts");
  rmSync(root, { recursive: true, force: true });
  const download = (name) => {
    const dir = join(root, name);
    for (let attempt = 1; ; attempt++) {
      rmSync(dir, { recursive: true, force: true });
      mkdirSync(dir, { recursive: true });
      try {
        fetchStep("gh", ["run", "download", String(run), "-R", REPO, "-n", name, "-D", dir]);
        return dir;
      } catch (error) {
        // A partly written directory can't be resumed: start the download again.
        if (attempt === 3 || /no (valid )?artifact/i.test(error.message)) throw error;
      }
    }
  };

  const manifestFile = join(download("release-manifest"), "manifest.json");
  const manifest = JSON.parse(readFileSync(manifestFile, "utf8"));
  checkManifest(manifest, want);
  console.log(`MANIFEST ${await sha256(manifestFile)} ${manifest.version} tree ${manifest.tree}`);

  for (const name of names.split(",")) {
    await timed(`fetch ${name}`, async () => {
      const dir = download(name);
      const found = new Map();
      for (const path of filesUnder(dir)) found.set(path, await sha256(join(dir, path)));
      console.log(`VERIFIED ${name}: ${checkFiles(manifest, name, found)} files match the manifest`);
    });
  }
  writeFileSync(
    join(WORK, "candidate.json"),
    JSON.stringify({ sha, run: String(run), version: manifest.version, tree: manifest.tree }, null, 2),
  );
} catch (error) {
  die(`fetch: ${error.message}`);
}
