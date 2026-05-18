import { LocalFirstAuth } from "jazz-tools/svelte";
import { RecoveryPhrase } from "jazz-tools/passphrase";
import { BrowserPasskeyBackup } from "jazz-tools/passkey-backup";

// #region auth-localfirst-svelte-backup
export function createRecoveryPhraseBackup(auth: LocalFirstAuth) {
  return {
    get isLoading() {
      return auth.isLoading;
    },
    get recoveryPhrase() {
      return auth.secret ? RecoveryPhrase.fromSecret(auth.secret) : null;
    },
  };
}
// #endregion auth-localfirst-svelte-backup

// #region auth-localfirst-svelte-restore
export function createRecoveryPhraseRestore(auth: LocalFirstAuth) {
  return async (userInput: string) => {
    const restoredSecret = RecoveryPhrase.toSecret(userInput);
    await auth.login(restoredSecret);
  };
}
// #endregion auth-localfirst-svelte-restore

// #region auth-localfirst-svelte-passkey-backup
const passkeyBackup = new BrowserPasskeyBackup({
  appName: "My App",
  // Pin to your canonical production hostname. If omitted, defaults to `location.hostname`,
  // which scopes passkeys per preview-deploy URL.
  appHostname: "myapp.com",
});

export function createPasskeyBackup(auth: LocalFirstAuth) {
  return async (displayName: string) => {
    if (!auth.secret) throw new Error("No local secret to back up yet");
    await passkeyBackup.backup(auth.secret, displayName);
  };
}
// #endregion auth-localfirst-svelte-passkey-backup

// #region auth-localfirst-svelte-passkey-restore
export function createPasskeyRestore(auth: LocalFirstAuth) {
  return async () => {
    const restoredSecret = await passkeyBackup.restore();
    await auth.login(restoredSecret);
  };
}
// #endregion auth-localfirst-svelte-passkey-restore
