// Jazz's Maven repository: write-once files, then maven-metadata.xml.
//
// A `store` is { list(prefix) -> [key], get(key) -> {bytes, etag} | null,
// put(key, bytes, {ifNoneMatch} | {ifMatch}, headers) -> boolean (false: the
// condition failed) }. get is only called for listed keys or after a refused
// put: without a listing S3 may answer 403 for a key that doesn't exist.
//
// A version's files never change. A re-run with the same bytes finishes what
// an earlier run left; other bytes are refused, before anything is written
// when they are already there. POMs go up after every other file and the
// metadata last, so nobody is pointed at half a version.
import { createHash } from "node:crypto";

export const GROUP_PATH = "tools/jazz";
export const CHECKSUMS = ["md5", "sha1", "sha256", "sha512"];
const IMMUTABLE = "public, max-age=31536000, immutable";
const ATTEMPTS = 20;

const digest = (algorithm, bytes) => createHash(algorithm).update(bytes).digest("hex");

/** Semver precedence, for the versions Jazz publishes. */
export function compareVersions(a, b) {
  const split = (version) => {
    const dash = version.indexOf("-");
    const core = dash < 0 ? version : version.slice(0, dash);
    return [core.split(".").map(Number), dash < 0 ? null : version.slice(dash + 1).split(".")];
  };
  const [coreA, preA] = split(a);
  const [coreB, preB] = split(b);
  for (let i = 0; i < 3; i++) if (coreA[i] !== coreB[i]) return Math.sign(coreA[i] - coreB[i]);
  if (!preA || !preB) return preA ? -1 : preB ? 1 : 0;
  for (let i = 0; i < Math.max(preA.length, preB.length); i++) {
    const [x, y] = [preA[i], preB[i]];
    if (x === y) continue;
    if (x === undefined) return -1;
    if (y === undefined) return 1;
    const [numberX, numberY] = [/^\d+$/.test(x), /^\d+$/.test(y)];
    if (numberX && numberY) return Math.sign(Number(x) - Number(y));
    if (numberX !== numberY) return numberX ? -1 : 1;
    return x < y ? -1 : 1;
  }
  return 0;
}

export function contentType(name) {
  if (/\.(pom|xml)$/.test(name)) return "application/xml";
  if (name.endsWith(".module")) return "application/json";
  if (/\.(asc|md5|sha1|sha256|sha512)$/.test(name)) return "text/plain";
  return "application/octet-stream";
}

/** Every file of a version: what was signed, its signature and its checksums. */
export function withChecksums(payloads, sign) {
  const files = new Map();
  for (const [name, bytes] of payloads) {
    files.set(name, bytes);
    files.set(`${name}.asc`, sign(name, bytes));
    for (const algorithm of CHECKSUMS)
      files.set(`${name}.${algorithm}`, Buffer.from(digest(algorithm, bytes)));
  }
  return files;
}

/**
 * Publishes `artifacts` ([{ artifact, files: Map<name, bytes> }]) at `version`
 * under `root`. An existing signature that differs is kept only if
 * `verify(payload, signature)` accepts it: signatures differ from run to run.
 */
export async function publishVersion({
  store,
  root = GROUP_PATH,
  version,
  artifacts,
  verify = () => false,
  now = () => new Date(),
}) {
  const uploads = [];
  for (const { artifact, files } of artifacts) {
    const pom = `${artifact}-${version}.pom`;
    if (!files.has(pom)) throw new Error(`${artifact} ${version} lacks ${pom}`);
    for (const [name, bytes] of files) {
      if (!name.startsWith(`${artifact}-${version}`) || name.includes("/"))
        throw new Error(`${name} isn't a file of ${artifact} ${version}`);
      const key = `${root}/${artifact}/${version}/${name}`;
      const payload = name.endsWith(".asc") ? files.get(name.slice(0, -4)) : undefined;
      uploads.push({ key, name, bytes, payload, last: name === pom });
    }
  }

  const accepts = async ({ key, bytes, payload }) => {
    const existing = await store.get(key);
    if (!existing) throw new Error(`${key} is there but can't be read`);
    if (Buffer.compare(existing.bytes, bytes) === 0) return;
    if (payload && (await verify(payload, existing.bytes))) return;
    throw new Error(`${key} is already published with other bytes`);
  };

  const there = new Set();
  for (const { artifact } of artifacts)
    for (const key of await store.list(`${root}/${artifact}/${version}/`)) there.add(key);
  for (const upload of uploads) if (there.has(upload.key)) await accepts(upload);

  const missing = uploads.filter((upload) => !there.has(upload.key));
  for (const upload of missing.sort((a, b) => Number(a.last) - Number(b.last))) {
    const written = await store.put(
      upload.key,
      upload.bytes,
      { ifNoneMatch: "*" },
      { cacheControl: IMMUTABLE, contentType: contentType(upload.name) },
    );
    if (!written) await accepts(upload); // a concurrent run got there first
  }

  for (const { artifact } of artifacts) await updateMetadata({ store, root, artifact, now });
  return { written: missing.length, skipped: there.size };
}

/** The versions whose POM is up: it is the last file of a version. */
export function publishedVersions(keys, root, artifact) {
  const prefix = `${root}/${artifact}/`;
  const versions = new Set();
  for (const key of keys) {
    if (!key.startsWith(prefix)) continue;
    const [version, name, rest] = key.slice(prefix.length).split("/");
    if (rest === undefined && name === `${artifact}-${version}.pom`) versions.add(version);
  }
  return [...versions].sort(compareVersions);
}

export function metadataXml(artifact, versions, updated) {
  const newest = versions.at(-1);
  return [
    '<?xml version="1.0" encoding="UTF-8"?>',
    "<metadata>",
    "  <groupId>tools.jazz</groupId>",
    `  <artifactId>${artifact}</artifactId>`,
    "  <versioning>",
    `    <latest>${newest}</latest>`,
    `    <release>${newest}</release>`,
    "    <versions>",
    ...versions.map((version) => `      <version>${version}</version>`),
    "    </versions>",
    `    <lastUpdated>${updated.toISOString().replace(/\D/g, "").slice(0, 14)}</lastUpdated>`,
    "  </versioning>",
    "</metadata>",
    "",
  ].join("\n");
}

const withoutTime = (xml) => xml.replace(/<lastUpdated>\d*<\/lastUpdated>/, "");

/**
 * Makes maven-metadata.xml and its checksums say what the listing says.
 * Every write is conditional on what was read; after any write or lost race
 * it starts again, and returns only on a pass that had nothing to change.
 */
export async function updateMetadata({
  store,
  root = GROUP_PATH,
  artifact,
  now = () => new Date(),
}) {
  const key = `${root}/${artifact}/maven-metadata.xml`;
  const write = (name, bytes, current) =>
    store.put(name, bytes, current ? { ifMatch: current.etag } : { ifNoneMatch: "*" }, {
      cacheControl: "no-cache",
      contentType: contentType(name),
    });

  for (let attempt = 0; attempt < ATTEMPTS; attempt++) {
    const keys = new Set(await store.list(`${root}/${artifact}/`));
    const versions = publishedVersions(keys, root, artifact);
    if (versions.length === 0) throw new Error(`${artifact} has no published version`);
    const read = (name) => (keys.has(name) ? store.get(name) : null);

    const current = await read(key);
    const wanted = metadataXml(artifact, versions, now());
    if (!current || withoutTime(current.bytes.toString()) !== withoutTime(wanted)) {
      await write(key, Buffer.from(wanted), current);
      continue;
    }
    let changed = false;
    for (const algorithm of CHECKSUMS) {
      const name = `${key}.${algorithm}`;
      const checksum = await read(name);
      const expected = digest(algorithm, current.bytes);
      if (checksum?.bytes.toString() === expected) continue;
      await write(name, Buffer.from(expected), checksum);
      changed = true;
    }
    if (!changed) return versions;
  }
  throw new Error(`${key} kept changing: ${ATTEMPTS} attempts`);
}
