import {
  createAccountManager,
  createDb,
  exportLocalFirstSecret,
  type AccountHandle,
} from "jazz-tools";
import { RecoveryPhrase } from "jazz-tools/passphrase";
import { BrowserPasskeyBackup } from "jazz-tools/passkey-backup";

type Accounts = Awaited<ReturnType<typeof createAccountManager>>;

// #region auth-localfirst-ts
export async function createLocalFirstDb() {
  const config = { appId: "my-app", serverUrl: "https://core.example" };
  const accounts = await createAccountManager(config);
  const account = accounts.getLoggedIn() ?? accounts.createLocalFirst();
  return createDb({ ...config, account });
}
// #endregion auth-localfirst-ts

// #region auth-jwt-ts
export async function createJwtDb(getToken: () => Promise<string>) {
  const config = { appId: "my-app", serverUrl: "https://core.example" };
  const accounts = await createAccountManager(config);
  // Existing identity: login. A signup flow explicitly calls registerJWT instead.
  const account = await accounts.loginJWT({ getToken });
  return createDb({ ...config, account });
}
// #endregion auth-jwt-ts

// #region auth-localfirst-ts-backup
export function getRecoveryPhrase(account: AccountHandle): string {
  return RecoveryPhrase.fromSecret(exportLocalFirstSecret(account));
}
// #endregion auth-localfirst-ts-backup

// #region auth-localfirst-ts-restore
// Call after the old context's normal shutdown({ waitForSync: true }).
// Use the returned handle to create the next context.
export function restoreFromRecoveryPhrase(accounts: Accounts, userInput: string): AccountHandle {
  return accounts.restoreLocalFirst(RecoveryPhrase.toSecret(userInput));
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
// Restore outside any context, after its ordinary graceful shutdown.
export async function restoreFromPasskey(accounts: Accounts): Promise<AccountHandle> {
  return accounts.restoreLocalFirst(await passkeyBackup.restore());
}
// #endregion auth-localfirst-ts-passkey-restore
