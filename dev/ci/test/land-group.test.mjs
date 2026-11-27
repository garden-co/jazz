import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const groupMembers = path.join(root, "dev/ci/group-members.sh");
const landGroup = path.join(root, "dev/ci/land-group.sh");

const makeRepo = () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "land-group-"));
  const git = (...args) => {
    const result = spawnSync("git", args, { cwd: dir, encoding: "utf8" });
    assert.equal(result.status, 0, result.stderr);
    return result.stdout.trim();
  };
  git("init", "-q", "-b", "main");
  git("config", "user.email", "ci@example.com");
  git("config", "user.name", "CI");
  git("config", "core.hooksPath", "/dev/null");
  const commitFile = (name) => {
    fs.writeFileSync(path.join(dir, name), `${name}\n`);
    git("add", name);
    git("commit", "-qm", name);
    return git("rev-parse", "HEAD");
  };
  // The Review Loop's group.py: one merge commit per PR onto the previous one.
  const cut = (base, label, prs) => {
    let cur = base;
    const trees = [];
    for (const [pr, sha] of prs) {
      const tree = git("merge-tree", "--write-tree", cur, sha).split("\n")[0];
      cur = git(
        "commit-tree",
        tree,
        "-p",
        cur,
        "-p",
        sha,
        "-m",
        `Merge #${pr} (${sha.slice(0, 8)}) into ${label}`,
      );
      trees.push(tree);
    }
    return { final: cur, trees };
  };
  return { dir, git, commitFile, cut };
};

const run = (script, args, options) =>
  spawnSync("bash", [script, ...args], { encoding: "utf8", ...options });

test("group-members lists a group's PRs, heads and trees down to its base", () => {
  const { dir, git, commitFile, cut } = makeRepo();
  const main = commitFile("base");
  git("checkout", "-qb", "a", main);
  const a = commitFile("a");
  git("checkout", "-qb", "b", main);
  const b = commitFile("b");
  const previous = cut(main, "g1", [[10, a]]);
  const group = cut(previous.final, "g2", [[11, b]]);
  const result = run(groupMembers, [group.final, "g2"], { cwd: dir });
  assert.equal(result.status, 0, result.stderr);
  assert.deepEqual(result.stdout.trim().split("\n"), [
    `base ${previous.final} ${previous.trees[0]}`,
    `pr 11 ${b} ${group.trees[0]}`,
  ]);

  // A merge whose tree is not git's own merge of its parents carries untested
  // content: refuse the whole group.
  const dirty = git(
    "commit-tree",
    `${main}^{tree}`,
    "-p",
    previous.final,
    "-p",
    b,
    "-m",
    `Merge #11 (${b.slice(0, 8)}) into g3`,
  );
  assert.notEqual(run(groupMembers, [dirty, "g3"], { cwd: dir }).status, 0);
  // A message naming another sha than the merged parent is refused too.
  const lying = git(
    "commit-tree",
    group.trees[0],
    "-p",
    previous.final,
    "-p",
    b,
    "-m",
    `Merge #11 (${a.slice(0, 8)}) into g4`,
  );
  assert.notEqual(run(groupMembers, [lying, "g4"], { cwd: dir }).status, 0);
  // A head that is no group at all.
  assert.notEqual(run(groupMembers, [main, "g9"], { cwd: dir }).status, 0);
});

// A fake `gh` over a JSON state file: main's tree, PRs, comments and statuses.
// A merge moves main's tree to the tree the state file maps the PR to.
const fakeGh = (dir, state) => {
  const bin = path.join(dir, "bin");
  fs.mkdirSync(bin);
  const statePath = path.join(dir, "state.json");
  fs.writeFileSync(statePath, JSON.stringify(state));
  fs.writeFileSync(
    path.join(bin, "gh"),
    `#!/usr/bin/env node
const fs = require("node:fs");
const { execFileSync } = require("node:child_process");
const statePath = ${JSON.stringify(statePath)};
const state = JSON.parse(fs.readFileSync(statePath, "utf8"));
const args = process.argv.slice(2);
const jqIndex = args.indexOf("--jq");
const jq = jqIndex === -1 ? "." : args[jqIndex + 1];
const method = args.includes("-X") ? args[args.indexOf("-X") + 1] : "GET";
const route = args.find((a) => a.startsWith("repos/"));
const field = (name) => { const f = args.find((a) => a.startsWith(name + "=")); return f && f.slice(name.length + 1); };
const out = (value) => {
  process.stdout.write(execFileSync("jq", ["-r", jq], { input: JSON.stringify(value) }));
};
let m;
if (route.endsWith("/branches/main")) {
  if (state.mainTreeSequence && state.mainTreeSequence.length) state.mainTree = state.mainTreeSequence.shift();
  fs.writeFileSync(statePath, JSON.stringify(state));
  out({ commit: { commit: { tree: { sha: state.mainTree } } } });
} else if ((m = route.match(/pulls\\/(\\d+)\\/merge$/)) && method === "PUT") {
  const pr = state.pulls[m[1]];
  if (field("sha") !== pr.head.sha) { console.error("sha mismatch"); process.exit(1); }
  pr.merged = true;
  state.mainTree = pr.landsTree;
  state.merged.push(Number(m[1]));
  fs.writeFileSync(statePath, JSON.stringify(state));
  out({ merged: true, sha: "0".repeat(40) });
} else if ((m = route.match(/pulls\\/(\\d+)$/))) {
  out(state.pulls[m[1]]);
} else if ((m = route.match(/issues\\/(\\d+)\\/comments$/))) {
  out(state.comments[m[1]] || []);
} else if ((m = route.match(/commits\\/([0-9a-f]+)\\/statuses/))) {
  out(state.statuses[m[1]] || []);
} else {
  console.error("unexpected gh call: " + args.join(" "));
  process.exit(2);
}
`,
  );
  fs.chmodSync(path.join(bin, "gh"), 0o755);
  return { bin, read: () => JSON.parse(fs.readFileSync(statePath, "utf8")) };
};

const scenario = (mutate = () => {}) => {
  const { dir, git, commitFile, cut } = makeRepo();
  const main = commitFile("base");
  git("checkout", "-qb", "a", main);
  const a = commitFile("a");
  git("checkout", "-qb", "b", main);
  const b = commitFile("b");
  const group = cut(main, "g7", [
    [21, a],
    [22, b],
  ]);
  const approved = (sha) => [
    {
      author_association: "OWNER",
      body: `Approved by Anselm (via project chat) at ${sha}\n\nFull suite passed.`,
    },
  ];
  const green = [
    { context: "full-suite", state: "success", creator: { login: "github-actions[bot]" } },
  ];
  const state = {
    mainTree: git("rev-parse", `${main}^{tree}`),
    merged: [],
    pulls: {
      21: { merged: false, head: { sha: a }, landsTree: group.trees[0] },
      22: { merged: false, head: { sha: b }, landsTree: group.trees[1] },
    },
    comments: { 21: approved(a), 22: approved(b) },
    statuses: { [a]: green, [b]: green },
  };
  mutate(state, { a, b, main, group, git });
  const gh = fakeGh(dir, state);
  const result = run(landGroup, [group.final, "g7"], {
    cwd: dir,
    env: {
      ...process.env,
      PATH: `${gh.bin}:${process.env.PATH}`,
      REPO: "o/r",
      GH_TOKEN: "x",
      LAND_WAIT_SECONDS: "0",
      LAND_TREE_POLL_SECONDS: "0",
    },
  });
  return { result, state: gh.read(), group };
};

test("land-group merges every PR of a green group in order", () => {
  const { result, state, group } = scenario();
  assert.equal(result.status, 0, result.stdout + result.stderr);
  assert.deepEqual(state.merged, [21, 22]);
  assert.equal(state.mainTree, group.trees[1]);
});

test("land-group resumes after the PRs a previous attempt landed", () => {
  const { result, state } = scenario((s, { group }) => {
    s.mainTree = group.trees[0];
    s.pulls[21].merged = true;
  });
  assert.equal(result.status, 0, result.stdout + result.stderr);
  assert.deepEqual(state.merged, [22]);
});

test("land-group stops at a PR whose head moved, merging nothing after it", () => {
  const { result, state } = scenario((s) => {
    s.pulls[22].head.sha = "f".repeat(40);
  });
  assert.notEqual(result.status, 0);
  assert.match(result.stdout, /head moved/);
  assert.deepEqual(state.merged, [21]);
});

test("land-group requires the approval comment at the exact head and from a member", () => {
  for (const comments of [
    [{ author_association: "OWNER", body: "Approved by Anselm (via project chat) at 0123" }],
    [{ author_association: "NONE", body: "Approved by Anselm (via project chat) at SHA" }],
  ]) {
    const { result, state } = scenario((s, { a }) => {
      s.comments[21] = comments.map((c) => ({ ...c, body: c.body.replace("SHA", a) }));
    });
    assert.notEqual(result.status, 0);
    assert.match(result.stdout, /no Review Loop approval comment/);
    assert.deepEqual(state.merged, []);
  }
});

test("land-group requires full-suite from github-actions on each head", () => {
  const { result, state } = scenario((s, { b }) => {
    s.statuses[b] = [{ context: "full-suite", state: "success", creator: { login: "someone" } }];
  });
  assert.notEqual(result.status, 0);
  assert.match(result.stdout, /no full-suite success/);
  assert.deepEqual(state.merged, [21]);
});

test("land-group refuses when main is on another tree than the group's base", () => {
  const { result, state } = scenario((s) => {
    s.mainTree = "e".repeat(40);
  });
  assert.notEqual(result.status, 0);
  assert.match(result.stdout, /never reached g7's base/);
  assert.deepEqual(state.merged, []);
});

test("land-group stops when a merge leaves main on an unexpected tree", () => {
  const { result, state } = scenario((s) => {
    s.pulls[21].landsTree = "d".repeat(40);
  });
  assert.notEqual(result.status, 0);
  assert.deepEqual(state.merged, [21]);
});

test("land-group stops merging once a containing PR has landed the rest of the group", () => {
  const { dir, git, commitFile, cut } = makeRepo();
  const main = commitFile("base");
  git("checkout", "-qb", "b", main);
  const b = commitFile("b");
  git("checkout", "-qb", "a", b);
  const a = commitFile("a");
  // #21's head contains #22's, as a stack's top does: landing #21 lands both.
  const group = cut(main, "g8", [
    [21, a],
    [22, b],
  ]);
  assert.equal(group.trees[0], group.trees[1]);
  const green = [
    { context: "full-suite", state: "success", creator: { login: "github-actions[bot]" } },
  ];
  const approved = (sha) => [
    { author_association: "OWNER", body: `Approved by Anselm (via project chat) at ${sha}` },
  ];
  const gh = fakeGh(dir, {
    mainTree: git("rev-parse", `${main}^{tree}`),
    merged: [],
    pulls: {
      21: { merged: false, head: { sha: a }, landsTree: group.trees[0] },
      // GitHub marks #22 merged on its own once #21 lands; merging it again 404s.
      22: { merged: false, head: { sha: b }, landsTree: "f".repeat(40) },
    },
    comments: { 21: approved(a), 22: approved(b) },
    statuses: { [a]: green, [b]: green },
  });
  const result = run(landGroup, [group.final, "g8"], {
    cwd: dir,
    env: {
      ...process.env,
      PATH: `${gh.bin}:${process.env.PATH}`,
      REPO: "o/r",
      GH_TOKEN: "x",
      LAND_WAIT_SECONDS: "0",
      LAND_TREE_POLL_SECONDS: "0",
    },
  });
  assert.equal(result.status, 0, result.stdout + result.stderr);
  assert.match(result.stdout, /already includes the group/);
  assert.deepEqual(gh.read().merged, [21]);
});
