import assert from "node:assert/strict";
import { test } from "node:test";
import { checkManifest, publishLevels } from "./npm-publish.mjs";
import { isPayload, requiredFiles } from "./maven-central.mjs";

const pkg = (name, deps = []) => ({ name, deps });

test("npm packages publish after the packages they depend on", () => {
  const levels = publishLevels([
    pkg("create-jazz"),
    pkg("jazz-tools", ["jazz-wasm", "jazz-napi", "jazz-rn", "react"]),
    pkg("jazz-napi", ["@garden-co/jazz-napi-linux-x64-gnu"]),
    pkg("@garden-co/jazz-napi-linux-x64-gnu"),
    pkg("jazz-wasm"),
    pkg("jazz-rn", ["jazz-rn-android", "jazz-rn-ios"]),
    pkg("jazz-rn-android"),
    pkg("jazz-rn-ios"),
  ]).map((level) => level.map((p) => p.name));
  assert.deepEqual(levels, [
    ["@garden-co/jazz-napi-linux-x64-gnu", "jazz-rn-android", "jazz-rn-ios", "jazz-wasm"],
    ["jazz-napi", "jazz-rn"],
    ["jazz-tools"],
    ["create-jazz"],
  ]);
});

test("a dependency cycle is refused", () => {
  assert.throws(() => publishLevels([pkg("a", ["b"]), pkg("b", ["a"])]), /cycle/);
});

test("manifests must be at the version, without workspace ranges", () => {
  checkManifest({ name: "x", version: "1.0.0", dependencies: { y: "1.0.0" } }, "1.0.0");
  assert.throws(() => checkManifest({ name: "x", version: "1.0.1" }, "1.0.0"), /not 1.0.0/);
  assert.throws(
    () => checkManifest({ name: "x", version: "1.0.0", optionalDependencies: { y: "workspace:*" } }, "1.0.0"),
    /workspace/,
  );
});

test("the Maven bundle signs payloads only", () => {
  assert.equal(isPayload("jazz-kotlin-1.jar"), true);
  assert.equal(isPayload("jazz-kotlin-1.jar.asc"), false);
  assert.equal(isPayload("jazz-kotlin-1.pom.sha1"), false);
  assert.equal(isPayload("maven-metadata-local.xml"), false);
  assert.deepEqual(requiredFiles("jazz-kotlin-android", "1")[0], "jazz-kotlin-android-1.aar");
});
