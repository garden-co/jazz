import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtempSync, mkdirSync, writeFileSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { verifyCliArtifact } from "./cli-artifact-provenance.mjs";
const hash = (bytes) => createHash("sha256").update(bytes).digest("hex");
const sha = "a".repeat(40),
  other = "b".repeat(40);
// Tiny stored ZIP fixtures avoid a network request or a zip-writer dependency.
function archive(entries) {
  let offset = 0;
  const locals = [],
    directory = [];
  for (const [name, bytes] of entries) {
    const filename = Buffer.from(name),
      data = Buffer.from(bytes);
    let crc = 0xffffffff;
    for (const byte of data) {
      crc ^= byte;
      for (let bit = 0; bit < 8; bit++) crc = (crc >>> 1) ^ (crc & 1 ? 0xedb88320 : 0);
    }
    crc = (crc ^ 0xffffffff) >>> 0;
    const local = Buffer.alloc(30);
    local.writeUInt32LE(0x04034b50);
    local.writeUInt16LE(20, 4);
    local.writeUInt32LE(crc, 14);
    local.writeUInt32LE(data.length, 18);
    local.writeUInt32LE(data.length, 22);
    local.writeUInt16LE(filename.length, 26);
    locals.push(local, filename, data);
    const central = Buffer.alloc(46);
    central.writeUInt32LE(0x02014b50);
    central.writeUInt16LE(20, 4);
    central.writeUInt16LE(20, 6);
    central.writeUInt32LE(crc, 16);
    central.writeUInt32LE(data.length, 20);
    central.writeUInt32LE(data.length, 24);
    central.writeUInt16LE(filename.length, 28);
    central.writeUInt32LE(offset, 42);
    directory.push(central, filename);
    offset += local.length + filename.length + data.length;
  }
  const central = Buffer.concat(directory),
    end = Buffer.alloc(22);
  end.writeUInt32LE(0x06054b50);
  end.writeUInt16LE(entries.length, 8);
  end.writeUInt16LE(entries.length, 10);
  end.writeUInt32LE(central.length, 12);
  end.writeUInt32LE(offset, 16);
  return Buffer.concat([...locals, central, end]);
}
function fixture(t) {
  const root = mkdtempSync(join(tmpdir(), "jazz-cli-provenance-test-"));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  const name = `jazz-tools-${process.platform}-${process.arch}`,
    bytes = Buffer.from("synthetic CLI bytes\n"),
    tools = join(root, "jazz-tools");
  mkdirSync(join(tools, "bin/native"), { recursive: true });
  const cli = join(root, "cli"),
    packed = join(tools, "bin/native", name),
    archivePath = join(root, "artifact.zip");
  writeFileSync(cli, bytes);
  writeFileSync(packed, bytes);
  writeFileSync(archivePath, archive([[name, bytes]]));
  const input = {
    sourceSha: sha,
    previewRun: "101",
    cli,
    cliSha256: hash(bytes),
    cliArtifact: { runId: 101, artifactId: 202, archivePath },
  };
  const run = {
    id: 101,
    run_attempt: 1,
    repository: { id: 303, full_name: "garden-co/jazz" },
    head_repository: { id: 303, full_name: "garden-co/jazz" },
    status: "completed",
    conclusion: "success",
    path: ".github/workflows/preview-jazz-tools-alpha-release.yml",
    event: "workflow_dispatch",
    head_sha: sha,
    html_url: "https://github.com/garden-co/jazz/actions/runs/101",
    pull_requests: [{ head: { sha, repo: { id: 303 } } }],
  };
  const artifact = {
    id: 202,
    name,
    expired: false,
    digest: `sha256:${hash(readFileSync(archivePath))}`,
    workflow_run: { id: 101, head_sha: sha, repository_id: 303, head_repository_id: 303 },
  };
  const calls = [];
  const getJSON = async (path) => {
    calls.push(path);
    if (path === "/repos/garden-co/jazz/actions/runs/101") return structuredClone(run);
    if (path === "/repos/garden-co/jazz/actions/artifacts/202") return structuredClone(artifact);
    throw new Error(`Unexpected API path ${path}`);
  };
  return { input, run, artifact, calls, tools, name, bytes, packed, getJSON };
}
for (const [workflow, event] of [
  ["preview-jazz-tools-alpha-release.yml", "workflow_dispatch"],
  ["publish-jazz-tools-alpha.yml", "push"],
  ["preview-build.yml", "pull_request"],
])
  test(`binds ${workflow} API evidence to original ZIP, executable and packed bytes`, async (t) => {
    const f = fixture(t);
    f.run.path = `.github/workflows/${workflow}`;
    f.run.event = event;
    const receipt = await verifyCliArtifact(f.input, f.tools, { getJSON: f.getJSON });
    assert.equal(receipt.summary.sourceSha, sha);
    assert.equal(receipt.summary.binarySha256, f.input.cliSha256);
    assert.equal(receipt.summary.archiveDigest, f.artifact.digest);
    assert.deepEqual(f.calls, [
      "/repos/garden-co/jazz/actions/runs/101",
      "/repos/garden-co/jazz/actions/artifacts/202",
    ]);
  });
for (const [label, mutate, message] of [
  [
    "failed run",
    (f) => {
      f.run.conclusion = "failure";
    },
    /did not succeed/,
  ],
  [
    "incomplete run",
    (f) => {
      f.run.status = "in_progress";
    },
    /not complete/,
  ],
  [
    "other source",
    (f) => {
      f.run.head_sha = other;
    },
    /source SHA mismatch/,
  ],
  [
    "unapproved workflow",
    (f) => {
      f.run.path = ".github/workflows/unrelated.yml";
    },
    /Unapproved/,
  ],
  [
    "unapproved event",
    (f) => {
      f.run.event = "pull_request_target";
    },
    /Unapproved/,
  ],
  [
    "fork run",
    (f) => {
      f.run.head_repository.full_name = "elsewhere/jazz";
    },
    /Fork producer/,
  ],
  [
    "other repository",
    (f) => {
      f.run.repository.full_name = "elsewhere/jazz";
    },
    /Wrong producer repository/,
  ],
  [
    "wrong PR head",
    (f) => {
      f.run.path = ".github/workflows/preview-build.yml";
      f.run.event = "pull_request";
      f.run.pull_requests[0].head.sha = other;
    },
    /candidate head/,
  ],
  [
    "other artifact run",
    (f) => {
      f.artifact.workflow_run.id = 999;
    },
    /another run/,
  ],
  [
    "other artifact head",
    (f) => {
      f.artifact.workflow_run.head_sha = other;
    },
    /Artifact source SHA/,
  ],
  [
    "other platform",
    (f) => {
      f.artifact.name = "jazz-tools-other-platform";
    },
    /Wrong platform/,
  ],
  [
    "expired artifact",
    (f) => {
      f.artifact.expired = true;
    },
    /expired/,
  ],
  [
    "missing API digest",
    (f) => {
      delete f.artifact.digest;
    },
    /authoritative artifact ZIP digest/,
  ],
  [
    "wrong ZIP digest",
    (f) => {
      f.artifact.digest = `sha256:${"0".repeat(64)}`;
    },
    /ZIP digest mismatch/,
  ],
  [
    "modified executable",
    (f) => {
      writeFileSync(f.input.cli, "wrong bytes");
    },
    /Configured CLI differs/,
  ],
  [
    "modified packed binary",
    (f) => {
      writeFileSync(f.packed, "wrong bytes");
    },
    /Packed CLI differs/,
  ],
  [
    "arbitrary digest claim",
    (f) => {
      f.input.cliSha256 = "0".repeat(64);
    },
    /configured CLI digest/,
  ],
  [
    "mislabelled producer receipt",
    (f) => {
      f.input.previewRun = "999";
    },
    /previewRun/,
  ],
])
  test(`rejects ${label}`, async (t) => {
    const f = fixture(t);
    mutate(f);
    await assert.rejects(verifyCliArtifact(f.input, f.tools, { getJSON: f.getJSON }), message);
  });
for (const extra of ["extra-file", `jazz-tools-${process.platform}-${process.arch}`, "../outside"])
  test(`rejects extra/duplicate/traversal ZIP entry ${extra}`, async (t) => {
    const f = fixture(t);
    writeFileSync(
      f.input.cliArtifact.archivePath,
      archive([
        [f.name, f.bytes],
        [extra, "extra"],
      ]),
    );
    f.artifact.digest = `sha256:${hash(readFileSync(f.input.cliArtifact.archivePath))}`;
    await assert.rejects(
      verifyCliArtifact(f.input, f.tools, { getJSON: f.getJSON }),
      /exactly the expected binary entry/,
    );
  });
test("caller-authored metadata cannot substitute for authenticated API evidence", async (t) => {
  const f = fixture(t);
  f.input.cliArtifact.run = f.run;
  f.input.cliArtifact.artifact = f.artifact;
  await assert.rejects(
    verifyCliArtifact(f.input, f.tools, {
      getJSON: async () => {
        throw new Error("API unavailable");
      },
    }),
    /API unavailable/,
  );
});

// Real successful preview-build API responses (including run 35153864977)
// contain pull_requests: []; fixture values remain synthetic and secret-free.
for (const associations of ["empty", "omitted"])
  test(`accepts trusted PR run with ${associations} PR associations`, async (t) => {
    const f = fixture(t);
    f.run.path = ".github/workflows/preview-build.yml";
    f.run.event = "pull_request";
    if (associations === "empty") f.run.pull_requests = [];
    else delete f.run.pull_requests;
    const receipt = await verifyCliArtifact(f.input, f.tools, { getJSON: f.getJSON });
    assert.equal(receipt.summary.sourceSha, sha);
    assert.equal(receipt.summary.archiveDigest, f.artifact.digest);
  });
