import { createAccountManager, exportLocalFirstSecret, type AccountHandle } from "jazz-tools";
import { RecoveryPhrase } from "jazz-tools/passphrase";
import { BrowserPasskeyBackup } from "jazz-tools/passkey-backup";

type Accounts = Awaited<ReturnType<typeof createAccountManager>>;

// #region auth-localfirst-vue-backup
export function getRecoveryPhrase(account: AccountHandle): string {
  return RecoveryPhrase.fromSecret(exportLocalFirstSecret(account));
}
// #endregion auth-localfirst-vue-backup

// #region auth-localfirst-vue-restore
// Call after the old context's normal shutdown({ waitForSync: true }).
// Use the returned handle to create the next context.
export function restoreFromRecoveryPhrase(accounts: Accounts, userInput: string): AccountHandle {
  return accounts.restoreLocalFirst(RecoveryPhrase.toSecret(userInput));
}
// #endregion auth-localfirst-vue-restore

// #region auth-localfirst-vue-passkey-backup
const passkeyBackup = new BrowserPasskeyBackup({
  appName: "My App",
  // Pin to your canonical production hostname rather than a preview hostname.
  appHostname: "myapp.com",
});

export async function backupToPasskey(account: AccountHandle, displayName: string): Promise<void> {
  await passkeyBackup.backup(exportLocalFirstSecret(account), displayName);
}
// #endregion auth-localfirst-vue-passkey-backup

// #region auth-localfirst-vue-passkey-restore
// Restore outside any context, after its ordinary graceful shutdown.
export async function restoreFromPasskey(accounts: Accounts): Promise<AccountHandle> {
  return accounts.restoreLocalFirst(await passkeyBackup.restore());
}
// #endregion auth-localfirst-vue-passkey-restore
