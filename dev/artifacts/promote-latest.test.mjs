import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { promote, shouldPromote, v2Packages, waitUntilVisible } from "./promote-latest.mjs";

const version = "2.0.0-alpha.60";

function repo() {
  const base = mkdtempSync(join(tmpdir(), "jazz-promote-latest-"));
  for (const platform of ["linux-x64-gnu", "darwin-arm64"]) {
    mkdirSync(join(base, "crates/jazz-napi/npm", platform), { recursive: true });
    writeFileSync(
      join(base, "crates/jazz-napi/npm", platform, "package.json"),
      JSON.stringify({ name: `@garden-co/jazz-napi-${platform}`, version }),
    );
  }
  return base;
}

test("v2 packages are promoted dependencies first, jazz-tools and create-jazz last", () => {
  const base = repo();
  try {
    assert.deepEqual(v2Packages(base), [
      "jazz-wasm",
      "@garden-co/jazz-napi-darwin-arm64",
      "@garden-co/jazz-napi-linux-x64-gnu",
      "jazz-napi",
      "jazz-rn-android",
      "jazz-rn-ios",
      "jazz-rn",
      "jazz-tools",
      "create-jazz",
    ]);
  } finally {
    rmSync(base, { recursive: true, force: true });
  }
});

test("latest moves off Classic and older alphas but never back to an older alpha", () => {
  assert.equal(shouldPromote("0.20.19", version), true);
  assert.equal(shouldPromote("2.0.0-alpha.6", version), true);
  assert.equal(shouldPromote(undefined, version), true);
  assert.equal(shouldPromote(version, version), false);
  assert.equal(shouldPromote("2.0.0-alpha.61", version), false);
  assert.throws(() => shouldPromote("0.20.19", "2.0.0-alpha.60-rc.2"), /2\.0\.0-alpha\.N/);
});

test("promote only tags versions that exist and skips packages already there", () => {
  const base = repo();
  const latest = {
    "jazz-tools": "0.20.19",
    "jazz-rn": "2.0.0-alpha.6",
    "create-jazz": version,
  };
  const calls = [];
  const run = (args) => {
    calls.push(args.join(" "));
    if (args[0] === "view" && args[2] === "dist-tags") {
      const current = latest[args[1]];
      return JSON.stringify(current ? { latest: current, alpha: version } : { alpha: version });
    }
    return "";
  };
  try {
    promote(version, base, run);
    const added = calls.filter((call) => call.startsWith("dist-tag add"));
    assert.equal(added.length, 8);
    assert.ok(added.includes(`dist-tag add jazz-tools@${version} latest`));
    assert.ok(!added.some((call) => call.includes("create-jazz")));
    assert.equal(
      calls.indexOf(`view jazz-tools@${version} version`) <
        calls.indexOf(`dist-tag add jazz-tools@${version} latest`),
      true,
    );
    assert.throws(
      () =>
        promote(
          version,
          base,
          (args) => {
            if (args[0] === "view" && args[1] === `jazz-wasm@${version}`) throw new Error("E404");
            return "{}";
          },
          { wait: { timeoutMs: 0 } },
        ),
      /E404/,
    );
  } finally {
    rmSync(base, { recursive: true, force: true });
  }
});

test("core and rn groups split the packages, React Native in its own group", () => {
  const base = repo();
  try {
    assert.deepEqual(v2Packages(base, "rn"), ["jazz-rn-android", "jazz-rn-ios", "jazz-rn"]);
    assert.deepEqual(v2Packages(base, "core"), [
      "jazz-wasm",
      "@garden-co/jazz-napi-darwin-arm64",
      "@garden-co/jazz-napi-linux-x64-gnu",
      "jazz-napi",
      "jazz-tools",
      "create-jazz",
    ]);
    assert.deepEqual(
      [...v2Packages(base, "core"), ...v2Packages(base, "rn")].sort(),
      [...v2Packages(base)].sort(),
    );
    assert.throws(() => v2Packages(base, "web"), /unknown package group/);
  } finally {
    rmSync(base, { recursive: true, force: true });
  }
});

test("a version npm accepted but does not serve yet is retried until it appears", () => {
  let clock = 0;
  let views = 0;
  const slept = [];
  waitUntilVisible("jazz-rn-android", version, {
    run: () => {
      views += 1;
      if (views < 3) throw new Error("E404");
      return version;
    },
    sleep: (ms) => {
      slept.push(ms);
      clock += ms;
    },
    now: () => clock,
    timeoutMs: 10 * 60_000,
    intervalMs: 60_000,
  });
  assert.equal(views, 3);
  assert.deepEqual(slept, [60_000, 60_000]);
});

test("waiting gives up with npm's error once the timeout passes", () => {
  let clock = 0;
  assert.throws(
    () =>
      waitUntilVisible("jazz-rn-android", version, {
        run: () => {
          throw new Error("E404 never published");
        },
        sleep: (ms) => {
          clock += ms;
        },
        now: () => clock,
        timeoutMs: 3 * 60_000,
        intervalMs: 60_000,
      }),
    /never published/,
  );
  assert.equal(clock, 3 * 60_000);
});
