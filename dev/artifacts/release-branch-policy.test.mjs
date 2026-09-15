import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtempSync, mkdirSync, readFileSync, writeFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import test from "node:test";
import { parse } from "yaml";
const root = resolve(import.meta.dirname, "../..");
const workflow = (name) => parse(readFileSync(join(root, `.github/workflows/${name}.yml`), "utf8"));
const publisher = workflow("publish-jazz-tools-alpha");
const gate = publisher.jobs["release-push-gate"].steps[0].with.script;
async function runGate({
  event = "push",
  ref = "refs/heads/release",
  mode = "publish",
  prs = [],
  expectedSha = "",
  expectedVersion = "",
  script = gate,
} = {}) {
  const outputs = {},
    failures = [];
  const github = {
    rest: {
      pulls: { list: {} },
      repos: {
        getContent: async () => ({
          data: {
            type: "file",
            encoding: "base64",
            content: Buffer.from('{"version":"1.0.0-alpha.55"}').toString("base64"),
          },
        }),
      },
    },
    request: async () => ({ data: prs }),
    paginate: async () => prs,
  };
  await new (Object.getPrototypeOf(async function () {}).constructor)(
    "process",
    "context",
    "github",
    "core",
    "Buffer",
    script,
  )(
    {
      env: {
        EVENT_NAME: event,
        REF: ref,
        SHA: "candidate",
        RELEASE_MODE: mode,
        EXPECTED_SHA: expectedSha,
        EXPECTED_VERSION: expectedVersion,
      },
    },
    { repo: { owner: "fixture", repo: "fixture" } },
    github,
    {
      setOutput: (key, value) => (outputs[key] = value),
      setFailed: (value) => failures.push(value),
      info() {},
    },
    Buffer,
  );
  return { outputs, failures };
}
const pr = {
  number: 1,
  merged_at: "today",
  merge_commit_sha: "candidate",
  base: { ref: "release" },
  head: { ref: "changeset-release/release", sha: "preview" },
};
test("release triggers and cache trust retain development and release separation", () => {
  assert.deepEqual(publisher.on.push.branches, ["release"]);
  const changesets = workflow("changesets-release-pr");
  assert.deepEqual(changesets.on.push.branches, ["release"]);
  assert.match(changesets.jobs["release-pr"].if, /refs\/heads\/release/);
  assert.match(changesets.concurrency.group, /github.ref/);
  assert.equal(
    changesets.jobs["release-pr"].steps.at(-1).env.BASE_BRANCH,
    "${{ github.ref_name }}",
  );
  assert.deepEqual(workflow("ci").on.push.branches, ["main", "release"]);
  assert.match(JSON.stringify(workflow("ci")), /OWNER.*MEMBER.*COLLABORATOR/);
  assert.equal(JSON.parse(readFileSync(join(root, ".changeset/config.json"))).baseBranch, "main");
});
test("publisher requires the exact merged release version PR and fails closed for manual publication", async () => {
  assert.equal((await runGate({ prs: [pr] })).outputs.should_run, "true");
  for (const options of [
    {},
    { prs: [{ ...pr, merge_commit_sha: "old" }] },
    { prs: [{ ...pr, head: { ...pr.head, ref: "changeset-release/main" } }] },
    { ref: "refs/heads/main", prs: [pr] },
    { event: "workflow_dispatch", prs: [pr] },
  ])
    assert.equal((await runGate(options)).outputs.should_run, "false");
  const manual = {
    event: "workflow_dispatch",
    prs: [pr],
    expectedSha: "candidate",
    expectedVersion: "1.0.0-alpha.55",
  };
  assert.equal((await runGate(manual)).outputs.should_run, "true");
  assert.equal(
    (await runGate({ ...manual, expectedVersion: "wrong" })).outputs.should_run,
    "false",
  );
  assert.equal(
    (await runGate({ event: "workflow_dispatch", ref: "refs/heads/feature", mode: "dry-run" }))
      .outputs.should_run,
    "true",
  );
  // Prove the wrong-branch assertion detects removal of the actual workflow guard.
  const mutated = gate.replace('ref !== "refs/heads/release"', "false");
  assert.equal(
    (await runGate({ ref: "refs/heads/main", prs: [pr], script: mutated })).outputs.should_run,
    "true",
  );
});
test("Changesets prerelease consumption survives release cut, fix, version and merge back", () => {
  const dir = mkdtempSync(join(tmpdir(), "jazz-release-cycle-"));
  const git = (...args) =>
    execFileSync("git", args, { cwd: dir, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] });
  const write = (file, data) =>
    writeFileSync(
      join(dir, file),
      typeof data === "string" ? data : JSON.stringify(data, null, 2) + "\n",
    );
  const commit = (message) => {
    git("add", ".");
    git("commit", "-m", message);
  };
  const version = () =>
    execFileSync(process.execPath, [join(root, "node_modules/@changesets/cli/bin.js"), "version"], {
      cwd: dir,
      stdio: "pipe",
    });
  try {
    git("init", "-b", "main");
    git("config", "user.name", "Fixture");
    git("config", "user.email", "fixture@example.invalid");
    mkdirSync(join(dir, ".changeset"));
    write("package.json", { name: "release-fixture", version: "1.0.0-alpha.54" });
    write(".changeset/config.json", {
      changelog: false,
      commit: false,
      fixed: [],
      linked: [],
      access: "public",
      baseBranch: "main",
      updateInternalDependencies: "patch",
      ignore: [],
    });
    write(".changeset/pre.json", {
      mode: "pre",
      tag: "alpha",
      initialVersions: { "release-fixture": "0.9.0" },
      changesets: ["previous"],
    });
    write(".changeset/previous.md", '---\n"release-fixture": minor\n---\nPrevious release\n');
    commit("released alpha54");
    git("branch", "release");
    write(".changeset/future.md", '---\n"release-fixture": patch\n---\nNext development feature\n');
    commit("main continues");
    git("checkout", "release");
    write(".changeset/fix.md", '---\n"release-fixture": patch\n---\nCandidate fix\n');
    commit("release fix");
    git("checkout", "-b", "changeset-release/release");
    version();
    commit("Version packages");
    assert.equal(JSON.parse(readFileSync(join(dir, "package.json"))).version, "1.0.0-alpha.55");
    git("checkout", "release");
    git("merge", "--no-ff", "changeset-release/release", "-m", "Merge version PR");
    git("checkout", "main");
    git("merge", "--no-ff", "release", "-m", "Merge release bookkeeping back");
    const pre = JSON.parse(readFileSync(join(dir, ".changeset/pre.json")));
    assert.deepEqual(new Set(pre.changesets), new Set(["previous", "fix"]));
    assert.match(readFileSync(join(dir, ".changeset/future.md"), "utf8"), /Next development/);
    version();
    assert.equal(JSON.parse(readFileSync(join(dir, "package.json"))).version, "1.0.0-alpha.56");
    assert.deepEqual(
      new Set(JSON.parse(readFileSync(join(dir, ".changeset/pre.json"))).changesets),
      new Set(["previous", "fix", "future"]),
    );
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
