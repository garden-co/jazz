import { describe, expect, it } from "vitest";

import * as packageRoot from "./index.js";
import * as runtime from "./runtime/index.js";

// @ts-expect-error WorkerBridgeOptions is intentionally not part of the public runtime surface.
import type { WorkerBridgeOptions as RuntimeWorkerBridgeOptions } from "./runtime/index.js";
// @ts-expect-error WorkerBridgeOptions is intentionally not part of the package-root surface.
import type { WorkerBridgeOptions as PackageRootWorkerBridgeOptions } from "./index.js";

void (null as unknown as RuntimeWorkerBridgeOptions);
void (null as unknown as PackageRootWorkerBridgeOptions);

describe("package root public API", () => {
  it("exposes intended runtime APIs without WorkerBridge internals", () => {
    for (const publicRuntimeExport of [
      "BrowserAuthSecretStore",
      "Db",
      "DbDirectBatch",
      "FileNotFoundError",
      "IncompleteFileDataError",
      "JazzClient",
      "PersistedWriteRejectedError",
      "RowChangeKind",
      "SubscriptionManager",
      "Transaction",
      "allRowsInTableQuery",
      "createDb",
      "fetchSchemaHashes",
      "fetchStoredPermissions",
      "fetchStoredWasmSchema",
      "generateAuthSecret",
      "loadWasmModule",
      "publishStoredPermissions",
      "resolveClientSessionStateSync",
      "resolveClientSessionSync",
      "toValue",
      "toWriteRecord",
      "transformRows",
      "translateQuery",
      "unwrapValue",
    ]) {
      expect(runtime, `runtime export ${publicRuntimeExport}`).toHaveProperty(publicRuntimeExport);
      expect(packageRoot, `package root export ${publicRuntimeExport}`).toHaveProperty(
        publicRuntimeExport,
      );
    }

    expect(runtime).not.toHaveProperty("WorkerBridge");
    expect(runtime).not.toHaveProperty("WorkerBridgeOptions");
    expect(packageRoot).not.toHaveProperty("WorkerBridge");
    expect(packageRoot).not.toHaveProperty("WorkerBridgeOptions");
  });
});
