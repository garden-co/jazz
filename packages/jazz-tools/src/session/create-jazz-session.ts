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
import { connectAuthProvider, type AuthProviderConnection } from "./auth-provider.js";
import { DEFAULT_SERVER_URL } from "../runtime/client-scope.js";

export type JazzSessionConfig = Omit<JazzClientConfig, "account"> &
  AccountManagerConfig & {
    initial?: "local-first" | BackendAuth;
    /**
     * On sign-out, wipe the account's local storage, unsynced writes
     * included, instead of keeping it for that user's next sign-in. For
     * shared devices.
     */
    clearOnSignOut?: boolean;
  };

/** A browser's Jazz session, which follows your auth's signed-in user. */
export interface BrowserJazzSession extends JazzSession<JazzClient> {
  /**
   * Tell Jazz who your auth says is signed in: `undefined` while it loads,
   * `null` when signed out, and the user's stable id when signed in. Each
   * change signs in again through your backend, whose `authenticate` decides
   * who the user is; `null` closes the account's client.
   */
  setUser(user: string | null | undefined): void;
}

/** Own one configured client and serialize its account lifecycle. */
export async function createJazzSession(config: JazzSessionConfig): Promise<BrowserJazzSession> {
  const serverUrl = config.serverUrl ?? DEFAULT_SERVER_URL;
  const { initial, clearOnSignOut, store: _store, ...clientConfig } = { ...config, serverUrl };
  const accounts = await createAccountManager({ ...config, serverUrl });
  const session = await createJazzSessionOwner({
    accounts,
    initial,
    clearOnSignOut,
    openClient: (account) => createJazzClient({ ...clientConfig, account }),
  });
  let connection: AuthProviderConnection | undefined;
  const close = session.close;
  return Object.assign(session, {
    setUser(user: string | null | undefined) {
      connection ??= connectAuthProvider(session);
      connection.update({ key: user ?? null, isPending: user === undefined });
    },
    close() {
      connection?.dispose();
      connection = undefined;
      return close();
    },
  });
}
