import { describe, expect, it } from "vitest";
import { existsSync, readFileSync, readdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import * as packageRoot from "./index.js";
import * as runtime from "./runtime/index.js";

// @ts-expect-error NativeRuntimeAdapter is intentionally not part of the public runtime surface.
import type { NativeRuntimeAdapter as InternalNativeRuntimeAdapterExport } from "./runtime/index.js";
// @ts-expect-error NativeRuntimeAdapter is intentionally not part of the package-root surface.
import type { NativeRuntimeAdapter as PackageRootNativeRuntimeAdapter } from "./index.js";
// @ts-expect-error WebSocketCarrier is intentionally not part of the public runtime surface.
import type { WebSocketCarrier as RuntimeWebSocketCarrier } from "./runtime/index.js";
// @ts-expect-error WebSocketCarrier is intentionally not part of the package-root surface.
import type { WebSocketCarrier as PackageRootWebSocketCarrier } from "./index.js";
// @ts-expect-error encodeSchema is intentionally not part of the public runtime surface.
import type { encodeSchema as RuntimeEncodeSchema } from "./runtime/index.js";
// @ts-expect-error encodeSchema is intentionally not part of the package-root surface.
import type { encodeSchema as PackageRootEncodeSchema } from "./index.js";
// @ts-expect-error encodeWebSocketFrameBatch is intentionally not part of the public runtime surface.
import type { encodeWebSocketFrameBatch as RuntimeEncodeWebSocketFrameBatch } from "./runtime/index.js";
// @ts-expect-error encodeWebSocketFrameBatch is intentionally not part of the package-root surface.
import type { encodeWebSocketFrameBatch as PackageRootEncodeWebSocketFrameBatch } from "./index.js";
// @ts-expect-error webSocketUrl is intentionally not part of the public runtime surface.
import type { webSocketUrl as RuntimeWebSocketUrl } from "./runtime/index.js";
// @ts-expect-error webSocketUrl is intentionally not part of the package-root surface.
import type { webSocketUrl as PackageRootWebSocketUrl } from "./index.js";
// @ts-expect-error OpenPayload is intentionally not part of the public runtime surface.
import type { OpenPayload as RuntimeOpenPayload } from "./runtime/index.js";
// @ts-expect-error OpenPayload is intentionally not part of the package-root surface.
import type { OpenPayload as PackageRootOpenPayload } from "./index.js";
// @ts-expect-error DbBatch was removed from the public runtime surface.
import type { DbBatch as RuntimeDbBatch } from "./runtime/index.js";
// @ts-expect-error DbBatch was removed from the package-root surface.
import type { DbBatch as PackageRootDbBatch } from "./index.js";
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
// @ts-expect-error QueryInput was removed from the runtime JazzClient surface.
import type { QueryInput as RuntimeQueryInput } from "./runtime/index.js";
// @ts-expect-error QueryInput was removed from the package-root surface.
import type { QueryInput as PackageRootQueryInput } from "./index.js";
// @ts-expect-error JazzClient is a concrete runtime internal, not a public runtime export.
import type { JazzClient as RuntimeJazzClient } from "./runtime/index.js";
// @ts-expect-error JazzClient is not exported from the package root via the runtime barrel.
import type { JazzClient as PackageRootJazzClient } from "./index.js";
// @ts-expect-error Runtime is a concrete runtime internal, not a public runtime export.
import type { Runtime as RuntimeRuntime } from "./runtime/index.js";
// @ts-expect-error Runtime is not exported from the package root via the runtime barrel.
import type { Runtime as PackageRootRuntime } from "./index.js";
// @ts-expect-error RequestLike is a concrete runtime internal, not a public runtime export.
import type { RequestLike as RuntimeRequestLike } from "./runtime/index.js";
// @ts-expect-error RequestLike is not exported from the package root via the runtime barrel.
import type { RequestLike as PackageRootRequestLike } from "./index.js";

void (null as unknown as InternalNativeRuntimeAdapterExport);
void (null as unknown as PackageRootNativeRuntimeAdapter);
void (null as unknown as RuntimeWebSocketCarrier);
void (null as unknown as PackageRootWebSocketCarrier);
void (null as unknown as RuntimeEncodeSchema);
void (null as unknown as PackageRootEncodeSchema);
void (null as unknown as RuntimeEncodeWebSocketFrameBatch);
void (null as unknown as PackageRootEncodeWebSocketFrameBatch);
void (null as unknown as RuntimeWebSocketUrl);
void (null as unknown as PackageRootWebSocketUrl);
void (null as unknown as RuntimeOpenPayload);
void (null as unknown as PackageRootOpenPayload);
void (null as unknown as RuntimeDbBatch);
void (null as unknown as PackageRootDbBatch);
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
void (null as unknown as RuntimeQueryInput);
void (null as unknown as PackageRootQueryInput);
void (null as unknown as RuntimeJazzClient);
void (null as unknown as PackageRootJazzClient);
void (null as unknown as RuntimeRuntime);
void (null as unknown as PackageRootRuntime);
void (null as unknown as RuntimeRequestLike);
void (null as unknown as PackageRootRequestLike);

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
  "NativeRuntimeAdapter",
  "WebSocketCarrier",
  "encodeSchema",
  "encodeWebSocketFrameBatch",
  "webSocketUrl",
  "OpenPayload",
] as const;

const removedBatchRuntimeExports = ["DbBatch"] as const;

const removedConcreteRuntimeExports = ["JazzClient"] as const;

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
  "dev/expo.js",
  "dev/expo.d.ts",
  "expo/index.js",
  "expo/index.d.ts",
  "expo/polyfills.js",
  "expo/polyfills.d.ts",
  "react-native/index.js",
  "react-native/index.d.ts",
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
  ["direct", "wasm"].join("-"),
  "/dev/expo",
  "/expo",
  "leader-lock",
  "/react-native",
  "sync-transport",
  "worker-bridge",
  "jazz-broker-worker",
  "jazz-worker",
] as const;

const removedRuntimeBuildPathFragments = [
  "runtime/core-runtime/",
  "core-codec",
  "runtime/native-row-format",
] as const;

const internalRuntimeExportPathFragments = [
  "/runtime/core-runtime",
  "/runtime/native-runtime",
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
  it("exposes intended runtime APIs without core internals", () => {
    for (const publicRuntimeExport of [
      "BrowserAuthSecretStore",
      "Db",
      "FileNotFoundError",
      "IncompleteFileDataError",
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

    for (const removedRuntimeExport of removedConcreteRuntimeExports) {
      expect(runtime, `runtime concrete export ${removedRuntimeExport}`).not.toHaveProperty(
        removedRuntimeExport,
      );
      expect(
        packageRoot,
        `package root concrete export ${removedRuntimeExport}`,
      ).not.toHaveProperty(removedRuntimeExport);
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

  it("does not publish subpath exports for deleted or internal runtime paths", () => {
    const packageJson = JSON.parse(
      readFileSync(join(packageRootDir, "..", "package.json"), "utf8"),
    ) as { exports: Record<string, unknown> };
    const exportedPaths = JSON.stringify(packageJson.exports);

    for (const removedPathFragment of [
      ...removedBrowserRuntimeExportPathFragments,
      ...internalRuntimeExportPathFragments,
    ]) {
      expect(exportedPaths, removedPathFragment).not.toContain(removedPathFragment);
    }
  });

  it("does not leave stale runtime build artifacts in the package surface", () => {
    const distDir = join(packageRootDir, "..", "dist");
    const distFiles = listDistFiles(distDir);
    const unexpectedRuntimeFiles = distFiles.filter(
      (file) =>
        [...removedBrowserRuntimeExportPathFragments, ...removedRuntimeBuildPathFragments].some(
          (fragment) => file.includes(fragment),
        ) && !file.includes(".test."),
    );

    expect(unexpectedRuntimeFiles).toEqual([]);
  });
});
