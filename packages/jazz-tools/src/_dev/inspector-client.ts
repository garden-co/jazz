import type { WasmSchema } from "../drivers/types.js";
import { attachInspectorCacheRuntime } from "./inspector-cache-runtime.js";
import type { NativeRuntimeAdapter } from "../runtime/native-runtime/native-runtime-adapter.js";
import { SharedBrowserForegroundNodeLease } from "../runtime/native-runtime/browser-shared-worker-connection.js";
/** Private diagnostic clients. Public application contexts require AccountHandle. */
import { createDbWithRuntimeSource, type DbConfig } from "../runtime/db.js";
import {
  DefaultRuntimeSource,
  trustAttachedBrowserWorkerSession,
} from "../runtime/default-runtime-source.js";
import {
  createBrowserAuthSessionKey,
  createBrowserStorageOwner,
  createBrowserPhysicalDatabaseName,
} from "../runtime/browser-worker-config.js";
import { admitInspectorAccountConfig } from "../accounts/config-capability.js";
import { createJazzClientFromDb } from "../web/create-jazz-client.js";
import {
  deserializeBrowserRelayError,
  type BrowserFollowerPortEvent,
  type InspectorAttachmentBinding,
} from "../runtime/native-runtime/browser-worker-protocol.js";
import type { InspectorHostConfig } from "../dev/inspector-overlay/inspector-host-types.js";
export type { InspectorHostConfig } from "../dev/inspector-overlay/inspector-host-types.js";

async function verifyAttachment(
  port: MessagePort,
  binding: InspectorAttachmentBinding,
  leasePort: MessagePort,
): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    const id = -1; // The later follower owns positive RPC IDs.
    const finish = (error?: Error) => {
      clearTimeout(timer);
      port.removeEventListener("message", receive);
      port.removeEventListener("messageerror", failed);
      if (error) reject(error);
      else resolve();
    };
    const failed = () => finish(new Error("Inspector attachment channel failed"));
    const receive = ({ data }: MessageEvent<BrowserFollowerPortEvent>) => {
      if (!("id" in data) || data.id !== id) return;
      if (data.type === "result" && data.error)
        return finish(deserializeBrowserRelayError(data.error));
      if (
        data.type !== "inspector-binding" ||
        data.binding.appId !== binding.appId ||
        data.binding.physicalDbName !== binding.physicalDbName ||
        data.binding.authSessionKey !== binding.authSessionKey ||
        data.binding.storageOwner !== binding.storageOwner
      ) {
        return finish(new Error("Inspector attachment receipt does not match its requested scope"));
      }
      finish();
    };
    const timer = setTimeout(() => finish(new Error("Inspector attachment timed out")), 10_000);
    port.addEventListener("message", receive);
    port.addEventListener("messageerror", failed);
    port.start();
    port.postMessage({ type: "inspect-binding", id, binding, leasePort }, [leasePort]);
  });
}

class InspectorRuntimeSource extends DefaultRuntimeSource {
  leaseRequested = false;
  constructor(
    private readonly leasePort: MessagePort,
    private readonly inspectorPort: MessagePort,
  ) {
    super();
  }
  protected override wrapClientRuntime(
    runtime: NativeRuntimeAdapter,
    config: DbConfig,
    schema: WasmSchema,
  ) {
    return attachInspectorCacheRuntime(
      runtime,
      this.inspectorPort,
      config.runtimeSources!.inspectorBinding!,
      schema,
    );
  }
  override async acquireBrowserForegroundNodeLease(config: DbConfig) {
    this.leaseRequested = true;
    return SharedBrowserForegroundNodeLease.acquireFromPort(this.leasePort, {
      dbName: config.runtimeSources!.inspectorHostPhysicalDbName!,
      storageOwner: createBrowserStorageOwner(config),
    });
  }
}

/** A worker-minted, session-scoped port is the diagnostic admission authority.
 * Keep the existing native JWT proof verification; never turn a copied UUID
 * into a public account handle or an ordinary persistent context.
 */
export async function createInspectorAttachmentClient(
  host: InspectorHostConfig,
  appId: string,
  physicalDbName: string,
  port: MessagePort,
) {
  const leaseChannel = new MessageChannel();
  const source = new InspectorRuntimeSource(leaseChannel.port1, port);
  try {
    if (!host.jwtToken)
      throw new Error("Inspector attachment requires the host's resolved client token");
    const config: DbConfig = {
      appId,
      jwtToken: host.jwtToken,
      accountId: host.accountId,
      accountRegistryAuthority: host.accountRegistryAuthority,
      serverUrl: host.serverUrl,
      env: host.env,
      runtimeSources: {
        ...host.runtimeSources,
        browserWorkerPort: port,
        inspectorHostPhysicalDbName: physicalDbName,
      },
    };
    trustAttachedBrowserWorkerSession(config);
    // Selected context names are physical coordinates, whereas driver.dbName
    // is a logical base. Strip only the exact account scope suffix.
    const suffix = createBrowserPhysicalDatabaseName(config, "");
    if (!physicalDbName.endsWith(suffix)) throw new Error("Inspector storage scope mismatch");
    config.driver = { type: "persistent", dbName: physicalDbName.slice(0, -suffix.length) };
    const binding: InspectorAttachmentBinding = {
      appId,
      physicalDbName,
      authSessionKey: createBrowserAuthSessionKey(config),
      storageOwner: createBrowserStorageOwner(config),
    };
    await verifyAttachment(port, binding, leaseChannel.port2);
    config.runtimeSources!.inspectorBinding = binding;
    admitInspectorAccountConfig(config);
    return await createJazzClientFromDb(await createDbWithRuntimeSource(config, source));
  } catch (error) {
    try {
      port.postMessage({ type: "close" });
    } catch {
      /* already detached */
    }
    port.close();
    if (!source.leaseRequested) {
      leaseChannel.port1.postMessage({ type: "cancel-foreground-node-lease" });
      leaseChannel.port1.close();
      leaseChannel.port2.close();
    }
    throw error;
  }
}

/** Explicit privileged diagnostic path, separate from account and overlay admission. */
export async function createInspectorAdminClient(config: {
  appId: string;
  serverUrl: string;
  env?: string;
  adminSecret: string;
}) {
  if (!config.adminSecret) throw new Error("Inspector admin credential is required");
  const db = await createDbWithRuntimeSource(
    {
      appId: config.appId,
      serverUrl: config.serverUrl,
      env: config.env,
      adminSecret: config.adminSecret,
      driver: { type: "memory" },
    },
    new DefaultRuntimeSource(),
  );
  return createJazzClientFromDb(db);
}
