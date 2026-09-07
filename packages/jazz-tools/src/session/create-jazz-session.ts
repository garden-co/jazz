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

export type JazzSessionConfig = Omit<JazzClientConfig, "account"> &
  AccountManagerConfig & { initial?: "local-first" };
/** Own one configured client and serialize its account lifecycle. */
export async function createJazzSession(
  config: JazzSessionConfig,
): Promise<JazzSession<JazzClient>> {
  const { initial, store: _store, ...clientConfig } = config;
  const accounts = await createAccountManager(config);
  return createJazzSessionOwner({
    accounts,
    initial,
    openClient: (account) => createJazzClient({ ...clientConfig, account }),
  });
}
