/**
 * Names alpha.60 exported and alpha.61 removed (the session replaced them),
 * kept for one release as stubs that throw when used, naming what replaced
 * them. Without them a bundler's "not exported" error guesses a similarly
 * named export (Turbopack suggests `useJazzClient` for `useJazzAuth`), which
 * is the wrong one. They are not working aliases.
 *
 * Each is typed so TypeScript shows it too: `@deprecated` with the
 * replacement where it is declared, and parameters of type `never`, so a call
 * with arguments doesn't type-check.
 */

/** Where the alpha.60 → alpha.61 changes are written up. */
export const UPGRADE_FROM_ALPHA_60_URL =
  "https://jazz.tools/docs/operating/backups-and-upgrades#upgrading-from-alpha60";

declare const removedApi: unique symbol;

/** A removed export: calling or constructing it throws, naming its replacement. */
export type RemovedApi = ((...args: never[]) => never) &
  (new (...args: never[]) => never) & { readonly [removedApi]: never };

/** The error a removed export throws. `code` is `JAZZ_API_REMOVED`. */
export function removedApiError(name: string, replacement: string): Error {
  return Object.assign(
    new Error(
      `[JAZZ_API_REMOVED] ${name} was removed in jazz-tools alpha.61: use ${replacement}. See ${UPGRADE_FROM_ALPHA_60_URL}`,
    ),
    { code: "JAZZ_API_REMOVED" },
  );
}

/** @internal A stub for a removed export: throws when called or constructed. */
export function removed(name: string, replacement: string): RemovedApi {
  // A plain function (not a Proxy): React and Fast Refresh inspect exports,
  // which must stay inert. Calling or constructing it throws.
  const stub = function removedJazzApi(): never {
    throw removedApiError(name, replacement);
  };
  Object.defineProperty(stub, "name", { value: name });
  return stub as unknown as RemovedApi;
}

const SESSION_HOOK =
  "useJazzSession() (Svelte: getJazzSession()): the session, who is signed in, and its operations";
const SESSION =
  'createJazzClient(config) (the session signs in itself: initial: "device", createDeviceAccount(), or your backend\'s user)';
const DEVICE_ACCOUNTS =
  "device accounts on the session: createDeviceAccount(), exportDeviceKey() and restoreDeviceAccount(key)";

/** What replaced each removed name. */
export const REPLACED = {
  useJazzAuth: SESSION_HOOK,
  useJazzSessionOwner: SESSION_HOOK,
  useAccountState: SESSION_HOOK,
  createAccountState: "useJazzSession() (the session's snapshot is reactive)",
  createJazzSessionState: "useJazzSession()",
  accountState: "getJazzSession()",
  createAccountManager: SESSION,
  AccountManager: SESSION,
  createDb: "createJazzClient(config), then session.db",
  createJazzApp: "createJazzClient(config), or <JazzProvider config={…}>",
  exportLocalFirstSecret: "session.exportDeviceKey()",
  useLocalFirstAuth: DEVICE_ACCOUNTS,
  createUseLocalFirstAuth: DEVICE_ACCOUNTS,
  LocalFirstAuth: DEVICE_ACCOUNTS,
  BrowserAuthSecretStore: DEVICE_ACCOUNTS,
  ExpoAuthSecretStore: DEVICE_ACCOUNTS,
  expoAuthSecretStore: DEVICE_ACCOUNTS,
  JazzSessionProvider: "<JazzProvider session={session}>",
  JazzSvelteProvider:
    "<JazzProvider config={…}>, or <JazzSvelteClientProvider client> for a client you opened yourself",
  acceptLocalFirstAuth: "acceptDeviceAccounts",
} as const;

// The stubs. Each entry point re-exports the ones it exported in alpha.60.
/** @deprecated Removed in alpha.61, and throws when used: use useJazzSession() (Svelte: getJazzSession()): the session, who is signed in, and its operations. */
export const useJazzAuth: RemovedApi = removed("useJazzAuth", REPLACED.useJazzAuth);
/** @deprecated Removed in alpha.61, and throws when used: use useJazzSession() (Svelte: getJazzSession()): the session, who is signed in, and its operations. */
export const useJazzSessionOwner: RemovedApi = removed(
  "useJazzSessionOwner",
  REPLACED.useJazzSessionOwner,
);
/** @deprecated Removed in alpha.61, and throws when used: use useJazzSession() (Svelte: getJazzSession()): the session, who is signed in, and its operations. */
export const useAccountState: RemovedApi = removed("useAccountState", REPLACED.useAccountState);
/** @deprecated Removed in alpha.61, and throws when used: use useJazzSession() (the session's snapshot is reactive). */
export const createAccountState: RemovedApi = removed(
  "createAccountState",
  REPLACED.createAccountState,
);
/** @deprecated Removed in alpha.61, and throws when used: use useJazzSession(). */
export const createJazzSessionState: RemovedApi = removed(
  "createJazzSessionState",
  REPLACED.createJazzSessionState,
);
/** @deprecated Removed in alpha.61, and throws when used: use getJazzSession(). */
export const accountState: RemovedApi = removed("accountState", REPLACED.accountState);
/** @deprecated Removed in alpha.61, and throws when used: use createJazzClient(config) (the session signs in itself: initial: "device", createDeviceAccount(), or your backend's user). */
export const createAccountManager: RemovedApi = removed(
  "createAccountManager",
  REPLACED.createAccountManager,
);
/** @deprecated Removed in alpha.61, and throws when used: use createJazzClient(config) (the session signs in itself: initial: "device", createDeviceAccount(), or your backend's user). */
export const AccountManager: RemovedApi = removed("AccountManager", REPLACED.AccountManager);
/** @deprecated Removed in alpha.61, and throws when used: use createJazzClient(config), then session.db. */
export const createDb: RemovedApi = removed("createDb", REPLACED.createDb);
/** @deprecated Removed in alpha.61, and throws when used: use createJazzClient(config), or <JazzProvider config={…}>. */
export const createJazzApp: RemovedApi = removed("createJazzApp", REPLACED.createJazzApp);
/** @deprecated Removed in alpha.61, and throws when used: use session.exportDeviceKey(). */
export const exportLocalFirstSecret: RemovedApi = removed(
  "exportLocalFirstSecret",
  REPLACED.exportLocalFirstSecret,
);
/** @deprecated Removed in alpha.61, and throws when used: use device accounts on the session: createDeviceAccount(), exportDeviceKey() and restoreDeviceAccount(key). */
export const useLocalFirstAuth: RemovedApi = removed(
  "useLocalFirstAuth",
  REPLACED.useLocalFirstAuth,
);
/** @deprecated Removed in alpha.61, and throws when used: use device accounts on the session: createDeviceAccount(), exportDeviceKey() and restoreDeviceAccount(key). */
export const createUseLocalFirstAuth: RemovedApi = removed(
  "createUseLocalFirstAuth",
  REPLACED.createUseLocalFirstAuth,
);
/** @deprecated Removed in alpha.61, and throws when used: use device accounts on the session: createDeviceAccount(), exportDeviceKey() and restoreDeviceAccount(key). */
export const LocalFirstAuth: RemovedApi = removed("LocalFirstAuth", REPLACED.LocalFirstAuth);
/** @deprecated Removed in alpha.61, and throws when used: use device accounts on the session: createDeviceAccount(), exportDeviceKey() and restoreDeviceAccount(key). */
export const BrowserAuthSecretStore: RemovedApi = removed(
  "BrowserAuthSecretStore",
  REPLACED.BrowserAuthSecretStore,
);
/** @deprecated Removed in alpha.61, and throws when used: use device accounts on the session: createDeviceAccount(), exportDeviceKey() and restoreDeviceAccount(key). */
export const ExpoAuthSecretStore: RemovedApi = removed(
  "ExpoAuthSecretStore",
  REPLACED.ExpoAuthSecretStore,
);
/** @deprecated Removed in alpha.61, and throws when used: use device accounts on the session: createDeviceAccount(), exportDeviceKey() and restoreDeviceAccount(key). */
export const expoAuthSecretStore: RemovedApi = removed(
  "expoAuthSecretStore",
  REPLACED.expoAuthSecretStore,
);
/** @deprecated Removed in alpha.61, and throws when used: use <JazzProvider session={session}>. */
export const JazzSessionProvider: RemovedApi = removed(
  "JazzSessionProvider",
  REPLACED.JazzSessionProvider,
);
/** @deprecated Removed in alpha.61, and throws when used: use <JazzProvider config={…}>, or <JazzSvelteClientProvider client> for a client you opened yourself. */
export const JazzSvelteProvider: RemovedApi = removed(
  "JazzSvelteProvider",
  REPLACED.JazzSvelteProvider,
);
/** @deprecated Removed in alpha.61, and throws when used: use acceptDeviceAccounts. */
export const acceptLocalFirstAuth: RemovedApi = removed(
  "acceptLocalFirstAuth",
  REPLACED.acceptLocalFirstAuth,
);
