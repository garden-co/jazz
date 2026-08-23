#!/usr/bin/env node
import { execFileSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";

const root = path.resolve(import.meta.dirname, "../..");
const fail = (message) => {
  throw new Error(`ignored-tests: ${message}`);
};
const issueReason = /^#(\d+):\s*(\S.*)$/;

function walk(directory, predicate) {
  const paths = [];
  for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
    const candidate = path.join(directory, entry.name);
    if (entry.isDirectory()) paths.push(...walk(candidate, predicate));
    else if (entry.isFile() && predicate(candidate)) paths.push(candidate);
  }
  return paths;
}

function rustSourceIgnores(source, file) {
  const found = [];
  const marker =
    /#\[ignore = "((?:\\.|[^"\\])*)"\]([\s\S]{0,240}?)\bfn\s+([A-Za-z_][A-Za-z0-9_]*)\b/g;
  for (const match of source.matchAll(marker)) {
    const reason = match[1].match(issueReason);
    if (!reason) fail(`${file}:${match.index} ignore needs "#NNNN: reason"`);
    found.push({ file, name: match[3], issue: reason[1], reason: reason[2] });
  }
  const bare = /#\[ignore(?:\s*=\s*"(?:\\.|[^"\\])*")?\]/g;
  if ([...source.matchAll(bare)].length !== found.length)
    fail(`${file} has an ignore marker that does not bind a nearby function`);
  return found;
}

function sourceRustIgnores() {
  return walk(path.join(root, "crates"), (candidate) => candidate.endsWith(".rs")).flatMap((file) =>
    rustSourceIgnores(fs.readFileSync(file, "utf8"), path.relative(root, file)),
  );
}

function compiledRustIgnores() {
  const raw = execFileSync(
    "cargo",
    [
      "nextest",
      "list",
      "--workspace",
      "--lib",
      "--bins",
      "--tests",
      "--features",
      "test",
      "--message-format",
      "json",
    ],
    { cwd: root, encoding: "utf8" },
  );
  const inventory = JSON.parse(raw);
  const ignored = [];
  for (const [binary, suite] of Object.entries(inventory["rust-suites"]))
    for (const [name, test] of Object.entries(suite.testcases))
      if (test.ignored) ignored.push(`${binary}::${name}`);
  return ignored;
}

function verifyRust(source, compiled) {
  if (source.length !== compiled.length)
    fail(
      `source/compiled ignored count differs: ${source.length} source, ${compiled.length} compiled`,
    );
  const used = new Set();
  for (const test of compiled) {
    const name = test.split("::").at(-1);
    const matches = source.filter((marker) => marker.name === name);
    if (matches.length !== 1)
      fail(`${test} must bind exactly one source issue annotation; found ${matches.length}`);
    const key = `${matches[0].file}:${matches[0].name}`;
    if (used.has(key)) fail(`${test} reuses source ignore annotation ${key}`);
    used.add(key);
  }
  if (used.size !== source.length)
    fail("a source ignore annotation is not compiled into the inventory");
}

function typeScriptIgnoresInSource(source, file) {
  const markers = [
    ...source.matchAll(
      /^\s*\/\/ @jazz-ignore #([0-9]+):\s*(\S.*?)\s*\n\s*(?:it|test|describe)\.skip\(/gm,
    ),
  ];
  const directSkips = [...source.matchAll(/^\s*(?:it|test|describe)\.skip\(/gm)];
  if (directSkips.length !== markers.length)
    fail(
      `${file} has ${directSkips.length} direct skip calls but ${markers.length} issue annotations`,
    );
  if ([...source.matchAll(/@jazz-ignore/g)].length !== markers.length)
    fail(`${file} has a malformed @jazz-ignore marker`);
  return markers.map((match) => ({ file, issue: match[1], reason: match[2] }));
}

function sourceTypeScriptIgnores() {
  const roots = [path.join(root, "packages"), path.join(root, "examples")].filter(fs.existsSync);
  const found = [];
  for (const directory of roots)
    for (const file of walk(directory, (candidate) =>
      /\.(?:test|spec)\.[cm]?[jt]sx?$/.test(candidate),
    )) {
      const source = fs.readFileSync(file, "utf8");
      found.push(...typeScriptIgnoresInSource(source, path.relative(root, file)));
    }
  return found;
}

function selfTest() {
  const valid = '#[ignore = "#12: waits for authority"]\n#[test]\nfn sample() {}';
  if (rustSourceIgnores(valid, "valid.rs").length !== 1) fail("self-test valid Rust marker");
  for (const [label, source] of [
    ["missing issue", '#[ignore = "known red"]\nfn sample() {}'],
    ["missing reason", '#[ignore = "#12: "]\nfn sample() {}'],
    ["bare", "#[ignore]\nfn sample() {}"],
  ]) {
    let rejected = false;
    try {
      rustSourceIgnores(source, `${label}.rs`);
    } catch {
      rejected = true;
    }
    if (!rejected) fail(`self-test did not reject ${label}`);
  }
  const ts = '// @jazz-ignore #34: manual browser soak\nit.skip("soak", () => {});';
  if (typeScriptIgnoresInSource(ts, "good.test.ts").length !== 1)
    fail("self-test valid TypeScript marker");
  let bareSkipRejected = false;
  try {
    typeScriptIgnoresInSource('test.skip("bare", () => {});', "bare.test.ts");
  } catch {
    bareSkipRejected = true;
  }
  if (!bareSkipRejected) fail("self-test did not reject bare TypeScript skip");
  console.log("ignored-tests: self-test passed");
}

if (process.argv.includes("--self-test")) selfTest();
else {
  const rust = sourceRustIgnores();
  verifyRust(rust, compiledRustIgnores());
  const ts = sourceTypeScriptIgnores();
  console.log(
    `ignored-tests: exact ${rust.length} Rust and ${ts.length} TypeScript issue-annotated ignores.`,
  );
}
