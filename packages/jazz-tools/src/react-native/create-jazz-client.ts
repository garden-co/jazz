import { trackPromise } from "../subscriptions-orchestrator.js";
import { createObservedJazzClient, type JazzClient } from "../observed-jazz-client.js";
import { createDb, type DbConfig } from "./create-db.js";
import type { AccountDbConfig } from "../accounts/context.js";

export type { JazzClient };

/**
 * The only React-Native client configuration surface. In persistent mode its
 * optional `nativeRelay` field carries the opaque capability issued by the
 * application's trusted native admission code; no JSI factory, byte codec,
 * storage path, or native owner helper is part of this public API.
 */
export type JazzClientConfig = AccountDbConfig & Pick<DbConfig, "nativeRelay">;

async function createJazzClientInternal(config: JazzClientConfig): Promise<JazzClient> {
  return createObservedJazzClient(await createDb(config), config.appId);
}

export function createJazzClient(config: JazzClientConfig): Promise<JazzClient> {
  return trackPromise(createJazzClientInternal(config));
}
