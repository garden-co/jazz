import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { readFileSync, statSync } from "node:fs";
import { join } from "node:path";

const repository = "garden-co/jazz";
// These callers build CLI artifacts through build-jazz-packages.yml. The reusable
// workflow itself has no independent run. Keep this aligned with those callers.
const workflows = new Map([
  [".github/workflows/preview-build.yml", new Set(["pull_request"])],
  [
    ".github/workflows/preview-jazz-tools-alpha-release.yml",
    new Set(["push", "workflow_dispatch"]),
  ],
  [".github/workflows/publish-jazz-tools-alpha.yml", new Set(["push", "workflow_dispatch"])],
]);
const sha256 = (bytes) => createHash("sha256").update(bytes).digest("hex");
const id = (value, label) => {
  assert(
    /^[1-9][0-9]*$/.test(String(value)) && Number.isSafeInteger(Number(value)),
    `Invalid ${label}`,
  );
  return Number(value);
};

/** No token or arbitrary API host can be supplied by the acceptance JSON. */
async function githubJSON(path) {
  let token;
  try {
    token = execFileSync("gh", ["auth", "token", "--hostname", "github.com"], {
      encoding: "utf8",
      timeout: 10000,
      stdio: ["ignore", "pipe", "pipe"],
    }).trim();
  } catch {
    throw new Error("Authenticated github.com access requires gh auth or GH_TOKEN");
  }
  assert(token, "Missing GitHub authentication token");
  const response = await fetch(`https://api.github.com${path}`, {
    headers: {
      Authorization: `Bearer ${token}`,
      Accept: "application/vnd.github+json",
      "X-GitHub-Api-Version": "2022-11-28",
    },
    signal: AbortSignal.timeout(30000),
    redirect: "error",
  });
  assert(response.ok, `GitHub provenance API returned ${response.status}`);
  return response.json();
}

/** Verify the original upload-artifact ZIP; never trust caller-authored API JSON. */
export async function verifyCliArtifact(
  input,
  packedToolsDirectory,
  { getJSON = githubJSON } = {},
) {
  const pin = input.cliArtifact;
  assert(
    pin?.archivePath,
    "Final receipts require cliArtifact runId, artifactId and original archivePath",
  );
  const runId = id(pin.runId, "run ID"),
    artifactId = id(pin.artifactId, "artifact ID");
  const name = `jazz-tools-${process.platform}-${process.arch}`;
  assert(
    [
      "jazz-tools-linux-x64",
      "jazz-tools-linux-arm64",
      "jazz-tools-darwin-x64",
      "jazz-tools-darwin-arm64",
    ].includes(name),
    "Unsupported CLI artifact platform",
  );
  const run = await getJSON(`/repos/${repository}/actions/runs/${runId}`);
  assert.equal(run.id, runId, "GitHub run ID mismatch");
  assert.equal(run.repository?.full_name, repository, "Wrong producer repository");
  assert.equal(run.head_repository?.full_name, repository, "Fork producer is not accepted");
  assert.equal(run.status, "completed", "Producer run is not complete");
  assert.equal(run.conclusion, "success", "Producer run did not succeed");
  assert(workflows.get(run.path)?.has(run.event), "Unapproved CLI producer workflow/event");
  assert.equal(run.head_sha, input.sourceSha, "Producer run source SHA mismatch");
  const runUrl = `https://github.com/${repository}/actions/runs/${runId}`;
  assert.equal(run.html_url, runUrl, "Unexpected producer run URL");
  assert(
    [String(runId), runUrl].includes(String(input.previewRun)),
    "previewRun must identify the CLI producer run",
  );
  const repositoryId = id(run.repository.id, "producer repository ID");
  assert.equal(run.head_repository.id, repositoryId, "Fork producer ID mismatch");
  if (run.event === "pull_request") {
    assert(
      run.pull_requests?.some(
        (pr) => pr.head?.sha === input.sourceSha && pr.head?.repo?.id === repositoryId,
      ),
      "PR producer must name the same-repository candidate head, not a merge revision",
    );
  }
  const artifact = await getJSON(`/repos/${repository}/actions/artifacts/${artifactId}`);
  assert.equal(artifact.id, artifactId, "GitHub artifact ID mismatch");
  assert.equal(artifact.name, name, "Wrong platform CLI artifact");
  assert.equal(artifact.expired, false, "CLI artifact has expired");
  assert.equal(artifact.workflow_run?.id, runId, "Artifact belongs to another run");
  assert.equal(artifact.workflow_run.head_sha, input.sourceSha, "Artifact source SHA mismatch");
  assert.equal(artifact.workflow_run.repository_id, repositoryId, "Artifact repository mismatch");
  assert.equal(
    artifact.workflow_run.head_repository_id,
    repositoryId,
    "Artifact fork source mismatch",
  );
  assert.match(
    artifact.digest ?? "",
    /^sha256:[a-f0-9]{64}$/,
    "Missing authoritative artifact ZIP digest",
  );
  assert(
    statSync(pin.archivePath).size <= 256 * 1024 * 1024,
    "CLI ZIP exceeds bounded acceptance budget",
  );
  const archiveDigest = `sha256:${sha256(readFileSync(pin.archivePath))}`;
  assert.equal(archiveDigest, artifact.digest, "Original artifact ZIP digest mismatch");
  // Do not extract paths to disk. Require the upload's one expected root entry,
  // reject duplicates/extra entries, and bound decompression before hashing bytes.
  const entries = execFileSync("unzip", ["-Z1", pin.archivePath], {
    encoding: "utf8",
    timeout: 10000,
    maxBuffer: 32768,
  })
    .trimEnd()
    .split("\n");
  assert.deepEqual(entries, [name], "CLI ZIP must contain exactly the expected binary entry");
  const binary = execFileSync("unzip", ["-p", pin.archivePath, name], {
    timeout: 30000,
    maxBuffer: 128 * 1024 * 1024,
  });
  const binarySha256 = sha256(binary);
  assert.equal(binarySha256, input.cliSha256, "Artifact binary differs from configured CLI digest");
  assert.equal(
    sha256(readFileSync(input.cli)),
    binarySha256,
    "Configured CLI differs from producer artifact",
  );
  assert.equal(
    sha256(readFileSync(join(packedToolsDirectory, "bin", "native", name))),
    binarySha256,
    "Packed CLI differs from producer artifact",
  );
  return {
    summary: {
      repository,
      sourceSha: input.sourceSha,
      runId,
      runAttempt: run.run_attempt,
      workflow: run.path,
      runUrl,
      artifactId,
      artifactName: name,
      archiveDigest,
      binarySha256,
      verifiedAt: new Date().toISOString(),
    },
    run,
    artifact,
  };
}
