import { exportLocalFirstSecret, type AccountHandle } from "jazz-tools";
import { RecoveryPhrase } from "jazz-tools/passphrase";
import { BrowserPasskeyBackup } from "jazz-tools/passkey-backup";

import { createJazzSession, type JazzSessionActions } from "jazz-tools/client";

// #region auth-localfirst-ts
export async function createLocalFirstSession() {
  return createJazzSession({
    appId: "my-app",
    serverUrl: "https://core.example",
    initial: "local-first",
  });
}
// #endregion auth-localfirst-ts

// #region auth-jwt-ts
export async function createJwtSession(getToken: () => Promise<string>) {
  const session = await createJazzSession({ appId: "my-app", serverUrl: "https://core.example" });
  try {
    // Existing identity: login. A signup flow explicitly calls registerJWT instead.
    await session.loginJWT({ getToken });
    return session;
  } catch (error) {
    await session.close().catch(() => {});
    throw error;
  }
}
// #endregion auth-jwt-ts

// #region auth-localfirst-ts-backup
export function getRecoveryPhrase(account: AccountHandle): string {
  return RecoveryPhrase.fromSecret(exportLocalFirstSecret(account));
}
// #endregion auth-localfirst-ts-backup

// #region auth-localfirst-ts-restore
export function restoreFromRecoveryPhrase(
  session: JazzSessionActions,
  userInput: string,
): Promise<void> {
  return session.restoreLocalFirst(RecoveryPhrase.toSecret(userInput));
}
// #endregion auth-localfirst-ts-restore

// #region auth-localfirst-ts-passkey-backup
const passkeyBackup = new BrowserPasskeyBackup({
  appName: "My App",
  // Pin to your canonical production hostname rather than a preview hostname.
  appHostname: "myapp.com",
});

export async function backupToPasskey(account: AccountHandle, displayName: string): Promise<void> {
  await passkeyBackup.backup(exportLocalFirstSecret(account), displayName);
}
// #endregion auth-localfirst-ts-passkey-backup

// #region auth-localfirst-ts-passkey-restore
export async function restoreFromPasskey(session: JazzSessionActions): Promise<void> {
  return session.restoreLocalFirst(await passkeyBackup.restore());
}
// #endregion auth-localfirst-ts-passkey-restore
