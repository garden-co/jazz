import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { test } from "node:test";
import {
  CHECKSUMS,
  compareVersions,
  publishVersion,
  updateMetadata,
  withChecksums,
} from "./maven-repository.mjs";

const ROOT = "tools/jazz";
const METADATA = `${ROOT}/jazz-kotlin/maven-metadata.xml`;

/** A bucket with the repository's policy: no put without a condition, no delete. */
function bucket({ beforePut } = {}) {
  const objects = new Map();
  const puts = [];
  let etag = 0;
  return {
    objects,
    puts,
    async list(prefix) {
      return [...objects.keys()].filter((key) => key.startsWith(prefix));
    },
    async get(key) {
      return objects.get(key) ?? null;
    },
    async put(key, bytes, condition, headers) {
      await beforePut?.(key);
      const current = objects.get(key);
      const metadata = key.includes("/maven-metadata.xml");
      if (condition.ifNoneMatch === "*") {
        if (current) return false;
      } else if (metadata && condition.ifMatch) {
        if (current?.etag !== condition.ifMatch) return false;
      } else {
        throw new Error(`the bucket policy refuses this put of ${key}`);
      }
      objects.set(key, { bytes: Buffer.from(bytes), etag: String(++etag), headers });
      puts.push(key);
      return true;
    },
  };
}

function release(version, { jar = `jar ${version}`, run = "1" } = {}) {
  return ["jazz-kotlin", "jazz-kotlin-android"].map((artifact) => ({
    artifact,
    files: withChecksums(
      new Map([
        [`${artifact}-${version}.pom`, Buffer.from(`pom ${artifact} ${version}`)],
        [`${artifact}-${version}.jar`, Buffer.from(jar)],
      ]),
      (name, bytes) => Buffer.from(`signed:${bytes}:${run}`),
    ),
  }));
}

const verify = (payload, signature) => signature.toString().startsWith(`signed:${payload}:`);
const publish = (store, version, options) =>
  publishVersion({ store, version, artifacts: release(version, options), verify });
const versionsIn = (store) =>
  [
    ...store.objects
      .get(METADATA)
      .bytes.toString()
      .matchAll(/<version>([^<]+)</g),
  ].map((m) => m[1]);
const latestIn = (store) =>
  store.objects
    .get(METADATA)
    .bytes.toString()
    .match(/<latest>([^<]+)</)[1];

function assertChecksumsMatch(store) {
  const { bytes } = store.objects.get(METADATA);
  for (const algorithm of CHECKSUMS)
    assert.equal(
      store.objects.get(`${METADATA}.${algorithm}`).bytes.toString(),
      createHash(algorithm).update(bytes).digest("hex"),
      algorithm,
    );
}

test("versions order by semver, Jazz's rc numbering included", () => {
  const ordered = [
    "2.0.0-alpha.65.66-rc.0",
    "2.0.0-alpha.66",
    "2.0.0-alpha.70",
    "2.0.0-beta.0",
    "2.0.0-beta.0.1-rc.1",
    "2.0.0-beta.1",
    "2.0.0-beta.2",
    "2.0.0-beta.10",
    "2.0.0",
  ];
  assert.deepEqual([...ordered].reverse().sort(compareVersions), ordered);
});

test("POMs go up after every other file, metadata after them, with their cache headers", async () => {
  const store = bucket();
  await publish(store, "2.0.0-beta.1");
  const kind = (key) => (key.includes("maven-metadata") ? 2 : key.endsWith(".pom") ? 1 : 0);
  assert.deepEqual(store.puts.map(kind), store.puts.map(kind).sort());
  assert.match(
    store.objects.get(`${ROOT}/jazz-kotlin/2.0.0-beta.1/jazz-kotlin-2.0.0-beta.1.jar`).headers
      .cacheControl,
    /immutable/,
  );
  assert.equal(store.objects.get(METADATA).headers.cacheControl, "no-cache");
  assertChecksumsMatch(store);
});

test("a re-run writes nothing, and keeps the first run's signatures", async () => {
  const store = bucket();
  await publish(store, "2.0.0-beta.1", { run: "1" });
  const puts = store.puts.length;
  await publish(store, "2.0.0-beta.1", { run: "2" });
  assert.equal(store.puts.length, puts);
});

test("other bytes are refused before anything is written", async () => {
  const store = bucket();
  await publish(store, "2.0.0-beta.1");
  const puts = store.puts.length;
  await assert.rejects(
    publish(store, "2.0.0-beta.1", { jar: "other" }),
    /already published with other bytes/,
  );
  assert.equal(store.puts.length, puts);
});

test("an existing signature that doesn't verify is refused", async () => {
  const store = bucket();
  await publish(store, "2.0.0-beta.1");
  await assert.rejects(
    publishVersion({
      store,
      version: "2.0.0-beta.1",
      artifacts: release("2.0.0-beta.1", { run: "2" }),
      verify: () => false,
    }),
    /\.asc is already published with other bytes/,
  );
});

test("a run that died before the POM isn't listed, and a re-run finishes it", async () => {
  let die = false;
  const store = bucket({
    beforePut(key) {
      if (die && key.endsWith(".pom")) throw new Error("runner lost");
    },
  });
  await publish(store, "2.0.0-beta.1");
  die = true;
  await assert.rejects(publish(store, "2.0.0-beta.2"), /runner lost/);
  await updateMetadata({ store, artifact: "jazz-kotlin" });
  assert.deepEqual(versionsIn(store), ["2.0.0-beta.1"]);
  die = false;
  await publish(store, "2.0.0-beta.2", { run: "2" });
  assert.deepEqual(versionsIn(store), ["2.0.0-beta.1", "2.0.0-beta.2"]);
});

test("publishing an older version doesn't move latest back", async () => {
  const store = bucket();
  await publish(store, "2.0.0-beta.2");
  await publish(store, "2.0.0-alpha.70");
  assert.deepEqual(versionsIn(store), ["2.0.0-alpha.70", "2.0.0-beta.2"]);
  assert.equal(latestIn(store), "2.0.0-beta.2");
});

test("concurrent publishes lose no version and leave matching checksums", async () => {
  const store = bucket({ beforePut: () => new Promise((resolve) => setImmediate(resolve)) });
  const versions = ["2.0.0-beta.1", "2.0.0-beta.2", "2.0.0-beta.3", "2.0.0-beta.4"];
  await Promise.all(versions.map((version) => publish(store, version)));
  assert.deepEqual(versionsIn(store), versions);
  assertChecksumsMatch(store);
});

test("two runs of one version with different bytes can't mix", async () => {
  const store = bucket({ beforePut: () => new Promise((resolve) => setImmediate(resolve)) });
  const results = await Promise.allSettled([
    publish(store, "2.0.0-beta.1", { jar: "one" }),
    publish(store, "2.0.0-beta.1", { jar: "two" }),
  ]);
  assert.deepEqual(results.map((result) => result.status).sort(), ["fulfilled", "rejected"]);
  const dir = `${ROOT}/jazz-kotlin/2.0.0-beta.1/jazz-kotlin-2.0.0-beta.1.jar`;
  const { bytes } = store.objects.get(dir);
  assert.equal(
    store.objects.get(`${dir}.sha256`).bytes.toString(),
    createHash("sha256").update(bytes).digest("hex"),
  );
});

test("only the version's own files, with its POM, are accepted", async () => {
  for (const name of ["../../evil.jar", "other-2.0.0.jar", "jazz-kotlin-2.0.0/../../x"])
    await assert.rejects(
      publishVersion({
        store: bucket(),
        version: "2.0.0",
        artifacts: [
          {
            artifact: "jazz-kotlin",
            files: new Map([
              ["jazz-kotlin-2.0.0.pom", Buffer.from("pom")],
              [name, Buffer.from("x")],
            ]),
          },
        ],
      }),
      /isn't a file of/,
    );
});
