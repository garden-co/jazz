import { describe, expect, it } from "vitest";

import * as packageRoot from "./index.js";
import * as runtime from "./runtime/index.js";

// @ts-expect-error CoreRuntime is intentionally not part of the public runtime surface.
import type { CoreRuntime as RuntimeCoreRuntime } from "./runtime/index.js";
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

void (null as unknown as RuntimeCoreRuntime);
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

const internalRuntimeExports = [
  "CoreRuntime",
  "DirectWebSocketCarrier",
  "encodeDirectSchema",
  "encodeDirectWebSocketFrameBatch",
  "directWebSocketUrl",
  "DirectOpenPayload",
] as const;

describe("package root public API", () => {
  it("exposes intended runtime APIs without direct-core internals", () => {
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

    for (const internalRuntimeExport of internalRuntimeExports) {
      expect(runtime, `runtime internal export ${internalRuntimeExport}`).not.toHaveProperty(
        internalRuntimeExport,
      );
      expect(
        packageRoot,
        `package root internal export ${internalRuntimeExport}`,
      ).not.toHaveProperty(internalRuntimeExport);
    }
  });
});
