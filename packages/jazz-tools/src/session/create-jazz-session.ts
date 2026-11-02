import type { BackendAuth } from "../accounts/enrollment.js";
import {
  createAccountManager,
  type AccountManagerConfig,
} from "../accounts/create-account-manager.js";
import {
  createJazzClient,
  type JazzClientConfig,
  type JazzClient,
} from "../web/create-jazz-client.js";
import { createJazzSessionOwner, type JazzSession } from "./state.js";
import { DEFAULT_SERVER_URL } from "../runtime/client-scope.js";

export type JazzSessionConfig = Omit<JazzClientConfig, "account"> &
  AccountManagerConfig & { initial?: "local-first" | BackendAuth };
/** Own one configured client and serialize its account lifecycle. */
export async function createJazzSession(
  config: JazzSessionConfig,
): Promise<JazzSession<JazzClient>> {
  const serverUrl = config.serverUrl ?? DEFAULT_SERVER_URL;
  const { initial, store: _store, ...clientConfig } = { ...config, serverUrl };
  const accounts = await createAccountManager({ ...config, serverUrl });
  return createJazzSessionOwner({
    accounts,
    initial,
    openClient: (account) => createJazzClient({ ...clientConfig, account }),
  });
}
