import { describe, expect, it } from "vitest";
import { existsSync, readFileSync, readdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import * as packageRoot from "./index.js";
import * as runtime from "./runtime/index.js";

// @ts-expect-error CoreRuntime is intentionally not part of the public runtime surface.
import type { CoreRuntime as InternalCoreRuntimeExport } from "./runtime/index.js";
// @ts-expect-error CoreRuntime is intentionally not part of the package-root surface.
import type { CoreRuntime as PackageRootCoreRuntime } from "./index.js";
// @ts-expect-error DirectWebSocketCarrier is intentionally not part of the public runtime surface.
import type { DirectWebSocketCarrier as RuntimeDirectWebSocketCarrier } from "./runtime/index.js";
// @ts-expect-error DirectWebSocketCarrier is intentionally not part of the package-root surface.
import type { DirectWebSocketCarrier as PackageRootDirectWebSocketCarrier } from "./index.js";
// @ts-expect-error encodeDirectSchema is intentionally not part of the public runtime surface.
import type { encodeDirectSchema as RuntimeEncodeDirectSchema } from "./runtime/index.js";
// @ts-expect-error encodeDirectSchema is intentionally not part of the package-root surface.
import type { encodeDirectSchema as PackageRootEncodeDirectSchema } from "./index.js";
// @ts-expect-error encodeDirectWebSocketFrameBatch is intentionally not part of the public runtime surface.
import type { encodeDirectWebSocketFrameBatch as RuntimeEncodeDirectWebSocketFrameBatch } from "./runtime/index.js";
// @ts-expect-error encodeDirectWebSocketFrameBatch is intentionally not part of the package-root surface.
import type { encodeDirectWebSocketFrameBatch as PackageRootEncodeDirectWebSocketFrameBatch } from "./index.js";
// @ts-expect-error directWebSocketUrl is intentionally not part of the public runtime surface.
import type { directWebSocketUrl as RuntimeDirectWebSocketUrl } from "./runtime/index.js";
// @ts-expect-error directWebSocketUrl is intentionally not part of the package-root surface.
import type { directWebSocketUrl as PackageRootDirectWebSocketUrl } from "./index.js";
// @ts-expect-error DirectOpenPayload is intentionally not part of the public runtime surface.
import type { DirectOpenPayload as RuntimeDirectOpenPayload } from "./runtime/index.js";
// @ts-expect-error DirectOpenPayload is intentionally not part of the package-root surface.
import type { DirectOpenPayload as PackageRootDirectOpenPayload } from "./index.js";
// @ts-expect-error DbDirectBatch was removed from the public runtime surface.
import type { DbDirectBatch as RuntimeDbDirectBatch } from "./runtime/index.js";
// @ts-expect-error DbDirectBatch was removed from the package-root surface.
import type { DbDirectBatch as PackageRootDbDirectBatch } from "./index.js";
// @ts-expect-error BatchScope was removed from the public runtime surface.
import type { BatchScope as RuntimeBatchScope } from "./runtime/index.js";
// @ts-expect-error BatchScope was removed from the package-root surface.
import type { BatchScope as PackageRootBatchScope } from "./index.js";
// @ts-expect-error SubscriptionManager is an internal runtime helper.
import type { SubscriptionManager as RuntimeSubscriptionManager } from "./runtime/index.js";
// @ts-expect-error SubscriptionManager is not part of the package-root surface.
import type { SubscriptionManager as PackageRootSubscriptionManager } from "./index.js";
// @ts-expect-error allRowsInTableQuery is an internal runtime helper.
import type { allRowsInTableQuery as RuntimeAllRowsInTableQuery } from "./runtime/index.js";
// @ts-expect-error allRowsInTableQuery is not part of the package-root surface.
import type { allRowsInTableQuery as PackageRootAllRowsInTableQuery } from "./index.js";
// @ts-expect-error resolveClientSessionStateSync is an internal runtime helper.
import type { resolveClientSessionStateSync as RuntimeResolveClientSessionStateSync } from "./runtime/index.js";
// @ts-expect-error resolveClientSessionStateSync is not part of the package-root surface.
import type { resolveClientSessionStateSync as PackageRootResolveClientSessionStateSync } from "./index.js";
// @ts-expect-error resolveClientSessionSync is an internal runtime helper.
import type { resolveClientSessionSync as RuntimeResolveClientSessionSync } from "./runtime/index.js";
// @ts-expect-error resolveClientSessionSync is not part of the package-root surface.
import type { resolveClientSessionSync as PackageRootResolveClientSessionSync } from "./index.js";
// @ts-expect-error toValue is an internal runtime helper.
import type { toValue as RuntimeToValue } from "./runtime/index.js";
// @ts-expect-error toValue is not part of the package-root surface.
import type { toValue as PackageRootToValue } from "./index.js";
// @ts-expect-error toWriteRecord is an internal runtime helper.
import type { toWriteRecord as RuntimeToWriteRecord } from "./runtime/index.js";
// @ts-expect-error toWriteRecord is not part of the package-root surface.
import type { toWriteRecord as PackageRootToWriteRecord } from "./index.js";
// @ts-expect-error transformRows is an internal runtime helper.
import type { transformRows as RuntimeTransformRows } from "./runtime/index.js";
// @ts-expect-error transformRows is not part of the package-root surface.
import type { transformRows as PackageRootTransformRows } from "./index.js";
// @ts-expect-error translateQuery is an internal runtime helper.
import type { translateQuery as RuntimeTranslateQuery } from "./runtime/index.js";
// @ts-expect-error translateQuery is not part of the package-root surface.
import type { translateQuery as PackageRootTranslateQuery } from "./index.js";
// @ts-expect-error unwrapValue is an internal runtime helper.
import type { unwrapValue as RuntimeUnwrapValue } from "./runtime/index.js";
// @ts-expect-error unwrapValue is not part of the package-root surface.
import type { unwrapValue as PackageRootUnwrapValue } from "./index.js";
// @ts-expect-error WasmValue is an internal row-transformer detail.
import type { WasmValue as RuntimeWasmValue } from "./runtime/index.js";
// @ts-expect-error WasmValue is not part of the package-root surface.
import type { WasmValue as PackageRootWasmValue } from "./index.js";
// @ts-expect-error DynamicTableRow belongs to the internal dynamic-query helper.
import type { DynamicTableRow as RuntimeDynamicTableRow } from "./runtime/index.js";
// @ts-expect-error DynamicTableRow is not part of the package-root surface.
import type { DynamicTableRow as PackageRootDynamicTableRow } from "./index.js";

void (null as unknown as InternalCoreRuntimeExport);
void (null as unknown as PackageRootCoreRuntime);
void (null as unknown as RuntimeDirectWebSocketCarrier);
void (null as unknown as PackageRootDirectWebSocketCarrier);
void (null as unknown as RuntimeEncodeDirectSchema);
void (null as unknown as PackageRootEncodeDirectSchema);
void (null as unknown as RuntimeEncodeDirectWebSocketFrameBatch);
void (null as unknown as PackageRootEncodeDirectWebSocketFrameBatch);
void (null as unknown as RuntimeDirectWebSocketUrl);
void (null as unknown as PackageRootDirectWebSocketUrl);
void (null as unknown as RuntimeDirectOpenPayload);
void (null as unknown as PackageRootDirectOpenPayload);
void (null as unknown as RuntimeDbDirectBatch);
void (null as unknown as PackageRootDbDirectBatch);
void (null as unknown as RuntimeBatchScope);
void (null as unknown as PackageRootBatchScope);
void (null as unknown as RuntimeSubscriptionManager);
void (null as unknown as PackageRootSubscriptionManager);
void (null as unknown as RuntimeAllRowsInTableQuery);
void (null as unknown as PackageRootAllRowsInTableQuery);
void (null as unknown as RuntimeResolveClientSessionStateSync);
void (null as unknown as PackageRootResolveClientSessionStateSync);
void (null as unknown as RuntimeResolveClientSessionSync);
void (null as unknown as PackageRootResolveClientSessionSync);
void (null as unknown as RuntimeToValue);
void (null as unknown as PackageRootToValue);
void (null as unknown as RuntimeToWriteRecord);
void (null as unknown as PackageRootToWriteRecord);
void (null as unknown as RuntimeTransformRows);
void (null as unknown as PackageRootTransformRows);
void (null as unknown as RuntimeTranslateQuery);
void (null as unknown as PackageRootTranslateQuery);
void (null as unknown as RuntimeUnwrapValue);
void (null as unknown as PackageRootUnwrapValue);
void (null as unknown as RuntimeWasmValue);
void (null as unknown as PackageRootWasmValue);
void (null as unknown as RuntimeDynamicTableRow);
void (null as unknown as PackageRootDynamicTableRow);

// @ts-expect-error Db.beginBatch was removed from the public runtime surface.
type RuntimeBeginBatch = InstanceType<typeof runtime.Db>["beginBatch"];
// @ts-expect-error Db.batch was removed from the public runtime surface.
type RuntimeBatch = InstanceType<typeof runtime.Db>["batch"];
// @ts-expect-error Db.beginBatch was removed from the package-root surface.
type PackageRootBeginBatch = InstanceType<typeof packageRoot.Db>["beginBatch"];
// @ts-expect-error Db.batch was removed from the package-root surface.
type PackageRootBatch = InstanceType<typeof packageRoot.Db>["batch"];

void (null as unknown as RuntimeBeginBatch);
void (null as unknown as RuntimeBatch);
void (null as unknown as PackageRootBeginBatch);
void (null as unknown as PackageRootBatch);

const internalRuntimeExports = [
  "CoreRuntime",
  "DirectWebSocketCarrier",
  "encodeDirectSchema",
  "encodeDirectWebSocketFrameBatch",
  "directWebSocketUrl",
  "DirectOpenPayload",
] as const;

const removedBatchRuntimeExports = ["DbDirectBatch"] as const;

const internalHelperRuntimeExports = [
  "DynamicTableRow",
  "SubscriptionManager",
  "WasmValue",
  "allRowsInTableQuery",
  "resolveClientSessionStateSync",
  "resolveClientSessionSync",
  "toValue",
  "toWriteRecord",
  "transformRows",
  "translateQuery",
  "unwrapValue",
] as const;

const packageRootDir = dirname(fileURLToPath(import.meta.url));
const removedBrowserRuntimePrefix = ["browser", "broker"].join("-");
const removedPostMessagePathName = ["worker", "bridge"].join("-");
const removedLeaderLockName = ["leader", "lock"].join("-");
const removedBrowserRuntimeBuildArtifacts = [
  `runtime/${removedBrowserRuntimePrefix}-client.js`,
  `runtime/${removedBrowserRuntimePrefix}-client.d.ts`,
  `runtime/${removedBrowserRuntimePrefix}-errors.js`,
  `runtime/${removedBrowserRuntimePrefix}-errors.d.ts`,
  `runtime/${removedBrowserRuntimePrefix}-protocol.js`,
  `runtime/${removedBrowserRuntimePrefix}-protocol.d.ts`,
  `runtime/${removedLeaderLockName}.js`,
  `runtime/${removedLeaderLockName}.d.ts`,
  "runtime/sync-transport.js",
  "runtime/sync-transport.d.ts",
  `runtime/${removedPostMessagePathName}.js`,
  `runtime/${removedPostMessagePathName}.d.ts`,
  `worker/jazz-${removedBrowserRuntimePrefix.split("-")[1]}-worker.js`,
  `worker/jazz-${removedBrowserRuntimePrefix.split("-")[1]}-worker.d.ts`,
  "worker/jazz-worker.js",
  "worker/jazz-worker.d.ts",
] as const;

const removedBrowserRuntimeExportNames = [
  "BrowserBrokerClient",
  "BrowserBrokerError",
  "BrowserBrokerProtocol",
  "BrowserBrokerWorker",
  "BrowserRuntimeClient",
  "BrowserRuntimeProtocol",
  "BrokerWorker",
  "LeaderLock",
  "SyncTransport",
  "WorkerBridge",
  "createBrowserBrokerClient",
  "createSyncTransport",
  "createWithWorker",
] as const;

const removedBrowserRuntimeExportPathFragments = [
  "browser-broker",
  "broker-worker",
  "leader-lock",
  "sync-transport",
  "worker-bridge",
  "jazz-broker-worker",
  "jazz-worker",
] as const;

const intendedDirectCoreRuntimeBuildArtifacts = [
  "runtime/core-runtime/direct-codec.js",
  "runtime/core-runtime/direct-codec.d.ts",
  "runtime/core-runtime/direct-row-codec.js",
  "runtime/core-runtime/direct-row-codec.d.ts",
  "runtime/core-runtime/direct-schema-codec.js",
  "runtime/core-runtime/direct-schema-codec.d.ts",
  "runtime/core-runtime/direct-websocket.js",
  "runtime/core-runtime/direct-websocket.d.ts",
  "runtime/core-runtime/persistent-browser-runtime.js",
  "runtime/core-runtime/persistent-browser-runtime.d.ts",
  "runtime/core-runtime/persistent-browser-worker.js",
  "runtime/core-runtime/persistent-browser-worker.d.ts",
] as const;

function listDistFiles(dir: string, prefix = ""): string[] {
  const files: string[] = [];

  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const relativePath = prefix ? `${prefix}/${entry.name}` : entry.name;
    const absolutePath = join(dir, entry.name);

    if (entry.isDirectory()) {
      files.push(...listDistFiles(absolutePath, relativePath));
    } else {
      files.push(relativePath);
    }
  }

  return files;
}

describe("package root public API", () => {
  it("exposes intended runtime APIs without direct-core internals", () => {
    for (const publicRuntimeExport of [
      "BrowserAuthSecretStore",
      "Db",
      "FileNotFoundError",
      "IncompleteFileDataError",
      "JazzClient",
      "PersistedWriteRejectedError",
      "RowChangeKind",
      "Transaction",
      "createDb",
      "fetchSchemaHashes",
      "fetchStoredPermissions",
      "fetchStoredWasmSchema",
      "generateAuthSecret",
      "loadWasmModule",
      "publishStoredPermissions",
    ]) {
      expect(runtime, `runtime export ${publicRuntimeExport}`).toHaveProperty(publicRuntimeExport);
      expect(packageRoot, `package root export ${publicRuntimeExport}`).toHaveProperty(
        publicRuntimeExport,
      );
    }

    for (const internalRuntimeExport of internalRuntimeExports) {
      expect(runtime, `runtime internal export ${internalRuntimeExport}`).not.toHaveProperty(
        internalRuntimeExport,
      );
      expect(
        packageRoot,
        `package root internal export ${internalRuntimeExport}`,
      ).not.toHaveProperty(internalRuntimeExport);
    }

    for (const removedRuntimeExport of removedBatchRuntimeExports) {
      expect(runtime, `runtime removed export ${removedRuntimeExport}`).not.toHaveProperty(
        removedRuntimeExport,
      );
      expect(packageRoot, `package root removed export ${removedRuntimeExport}`).not.toHaveProperty(
        removedRuntimeExport,
      );
    }

    for (const internalHelperRuntimeExport of internalHelperRuntimeExports) {
      expect(runtime, `runtime helper export ${internalHelperRuntimeExport}`).not.toHaveProperty(
        internalHelperRuntimeExport,
      );
      expect(
        packageRoot,
        `package root helper export ${internalHelperRuntimeExport}`,
      ).not.toHaveProperty(internalHelperRuntimeExport);
    }

    for (const removedBrowserRuntimeExport of removedBrowserRuntimeExportNames) {
      expect(runtime, `runtime removed export ${removedBrowserRuntimeExport}`).not.toHaveProperty(
        removedBrowserRuntimeExport,
      );
      expect(
        packageRoot,
        `package root removed export ${removedBrowserRuntimeExport}`,
      ).not.toHaveProperty(removedBrowserRuntimeExport);
    }
  });

  it("does not leave deleted browser worker build artifacts in the package surface", () => {
    for (const artifact of removedBrowserRuntimeBuildArtifacts) {
      expect(existsSync(join(packageRootDir, "..", "dist", artifact)), artifact).toBe(false);
    }
  });

  it("does not publish subpath exports for deleted browser runtime paths", () => {
    const packageJson = JSON.parse(
      readFileSync(join(packageRootDir, "..", "package.json"), "utf8"),
    ) as { exports: Record<string, unknown> };
    const exportedPaths = JSON.stringify(packageJson.exports);

    for (const removedPathFragment of removedBrowserRuntimeExportPathFragments) {
      expect(exportedPaths, removedPathFragment).not.toContain(removedPathFragment);
    }
  });

  it("builds only the intended direct-core browser runtime boundary glue", () => {
    const distDir = join(packageRootDir, "..", "dist");
    const distFiles = listDistFiles(distDir);
    const unexpectedBrowserRuntimeFiles = distFiles.filter(
      (file) =>
        removedBrowserRuntimeExportPathFragments.some((fragment) => file.includes(fragment)) &&
        !file.includes(".test."),
    );

    expect(unexpectedBrowserRuntimeFiles).toEqual([]);

    for (const artifact of intendedDirectCoreRuntimeBuildArtifacts) {
      expect(existsSync(join(distDir, artifact)), artifact).toBe(true);
    }
  });
});
