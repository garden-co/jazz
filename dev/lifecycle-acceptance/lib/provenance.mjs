// The pure half of fetch.mjs: what a candidate's run and manifest must say.
const SHA = /^[0-9a-f]{40}$/;
export const WORKFLOW = "release-artifacts.yml";

export function checkCandidate({ sha, run }) {
  if (!SHA.test(sha ?? "")) throw new Error(`sha must be a full commit sha, got '${sha}'`);
  if (!/^\d+$/.test(String(run ?? ""))) throw new Error(`run must be a run id, got '${run}'`);
}

export function checkRun(got, { sha, run, repo }) {
  const want = [
    ["id", String(got.id), String(run)],
    ["head_sha", got.head_sha, sha],
    ["repository", got.repository?.full_name, repo],
    ["head_repository", got.head_repository?.full_name, repo],
    ["path", got.path, `.github/workflows/${WORKFLOW}`],
    ["conclusion", got.conclusion, "success"],
  ];
  for (const [name, is, expected] of want)
    if (is !== expected) throw new Error(`run ${run}: ${name} is '${is}', not '${expected}'`);
  if (got.event !== "push" && got.event !== "workflow_dispatch")
    throw new Error(`run ${run} is a '${got.event}' run, not push or workflow_dispatch`);
}

export function checkManifest(manifest, { sha, run, repo }) {
  const want = [
    ["schema", manifest.schema, 1],
    ["sha", manifest.sha, sha],
    ["run_id", String(manifest.run_id), String(run)],
    ["repository", manifest.repository, repo],
    ["workflow", manifest.workflow, WORKFLOW],
  ];
  for (const [name, is, expected] of want)
    if (is !== expected) throw new Error(`manifest: ${name} is '${is}', not '${expected}'`);
  if (!/^2\.\d+\.\d+(-[0-9a-z.]+)?$/.test(manifest.version ?? ""))
    throw new Error(`manifest: odd version '${manifest.version}'`);
  if (!SHA.test(manifest.tree ?? "")) throw new Error("manifest: no tree");
}

/** An artifact holds exactly the manifest's files, byte for byte. */
export function checkFiles(manifest, artifact, found) {
  const listed = manifest.files.filter((file) => file.artifact === artifact);
  if (!listed.length) throw new Error(`manifest lists nothing for ${artifact}`);
  for (const { path, sha256 } of listed) {
    if (!found.has(path)) throw new Error(`${artifact}: ${path} is missing`);
    if (found.get(path) !== sha256)
      throw new Error(`${artifact}: ${path} has sha256 ${found.get(path)}, manifest ${sha256}`);
  }
  for (const path of found.keys())
    if (!listed.some((file) => file.path === path))
      throw new Error(`${artifact}: ${path} is not in the manifest`);
  return listed.length;
}
