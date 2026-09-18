// Run after build:runtime. Native qualification uses the workspace's built NAPI package.
import { strict as assert } from "node:assert";
import { execFile as rawExecFile } from "node:child_process";
import { mkdir, mkdtemp, readdir, readFile, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

const execFile = promisify(rawExecFile);
const root = fileURLToPath(new URL("..", import.meta.url));
const directory = await mkdtemp(resolve(tmpdir(), "jazz-e2ee-package-"));
try {
  await execFile("pnpm", ["pack", "--pack-destination", directory], { cwd: root });
  const archive = (await readdir(directory)).find((name) => name.endsWith(".tgz"));
  assert.ok(archive, "pnpm pack must produce an archive");
  const modules = resolve(directory, "node_modules");
  const consumer = resolve(modules, "jazz-tools");
  await mkdir(consumer, { recursive: true });
  await execFile("tar", [
    "-xzf",
    resolve(directory, archive),
    "--strip-components=1",
    "-C",
    consumer,
  ]);
  await symlink(resolve(root, "../../crates/jazz-napi"), resolve(modules, "jazz-napi"), "dir");
  const manifest = JSON.parse(await readFile(resolve(consumer, "package.json"), "utf8"));
  for (const dependency of ["@standard-schema/spec", "jazz-wasm", "json-schema-to-ts"]) {
    assert.ok(manifest.dependencies?.[dependency], `${dependency} must be declared`);
    const destination = resolve(modules, dependency);
    await mkdir(dirname(destination), { recursive: true });
    await symlink(resolve(root, "node_modules", dependency), destination, "dir");
  }
  await writeFile(
    resolve(directory, "consumer.mts"),
    `
    import type { CryptoAdapters, LargeValueCipher } from "jazz-tools/e2ee";
    import type { DeviceInfo } from "jazz-tools";
    export function deviceState(device: DeviceInfo): "pending" | "active" | "revoked" {
      return device.state;
    }
    export function keyReadiness(device: DeviceInfo): "verified" | "not-verified" {
      return device.keyReadiness;
    }
    import { E2eeRecoveryError } from "jazz-tools/e2ee";
    import type { E2eeRecoveryErrorCode } from "jazz-tools/e2ee";
    export const reason: E2eeRecoveryErrorCode = new E2eeRecoveryError("recovery-material-unusable").code;
    import { createBrowserCrypto } from "jazz-tools/e2ee/browser";
    import { createNativeCrypto } from "jazz-tools/e2ee/native";
    export const browser: Promise<CryptoAdapters> = createBrowserCrypto();
    export const native: Promise<CryptoAdapters> = createNativeCrypto();
    export type StreamAdapter = LargeValueCipher;
  `,
  );
  await execFile(
    process.execPath,
    [
      resolve(root, "node_modules/typescript/bin/tsc"),
      "--noEmit",
      "--strict",
      "--module",
      "nodenext",
      "--target",
      "es2022",
      "--lib",
      "es2022,dom,esnext.disposable",
      "consumer.mts",
    ],
    { cwd: directory },
  );
  const { stdout } = await execFile(
    process.execPath,
    [
      "--input-type=module",
      "-e",
      `
    import { strict as assert } from "node:assert";
    import { encodeCryptoContext, E2eeRecoveryError } from "jazz-tools/e2ee";
    const error = new E2eeRecoveryError("recovery-material-unusable");
    assert.ok(error instanceof Error);
    assert.equal(error.code, "recovery-material-unusable");
    assert.equal(error.cause, undefined);
    import { createBrowserCrypto } from "jazz-tools/e2ee/browser";
    import { createNativeCrypto } from "jazz-tools/e2ee/native";
    const browser = await createBrowserCrypto();
    const native = await createNativeCrypto();
    const context = encodeCryptoContext({
      application: "a", policy: "p", scope: "s", identifier: "i", epoch: "e"
    });
    const key = new Uint8Array(32).fill(7);
    const plaintext = new Uint8Array([1, 2, 3]);
    for (const [writer, reader] of [[browser, native], [native, browser]]) {
      const cell = await writer.cellCipher.encrypt(key, context, plaintext);
      assert.deepEqual(await reader.cellCipher.decrypt(key, context, cell), plaintext);
      const pair = await reader.keyEnvelope.createKeyPair();
      const sealed = await writer.keyEnvelope.seal(pair.publicKey, context, key);
      assert.deepEqual(await reader.keyEnvelope.open(pair, context, sealed), key);
    }
    console.log("Packed public browser/native imports and interoperability passed");
  `,
    ],
    { cwd: directory },
  );
  process.stdout.write("Packed public declarations resolved\n" + stdout);
} finally {
  await rm(directory, { recursive: true, force: true });
}
