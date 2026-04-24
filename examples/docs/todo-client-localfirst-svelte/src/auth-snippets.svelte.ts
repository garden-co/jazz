import { onMount } from "svelte";
import { BrowserAuthSecretStore } from "jazz-tools/svelte";
import { RecoveryPhrase } from "jazz-tools/passphrase";
import { BrowserPasskeyBackup } from "jazz-tools/passkey-backup";

// #region auth-localfirst-svelte-backup
export function createRecoveryPhraseBackup() {
  let isLoading = $state(true);
  let recoveryPhrase = $state<string | null>(null);

  onMount(async () => {
    const secret = await BrowserAuthSecretStore.loadSecret();
    recoveryPhrase = secret ? RecoveryPhrase.fromSecret(secret) : null;
    isLoading = false;
  });

  return {
    get isLoading() {
      return isLoading;
    },
    get recoveryPhrase() {
      return recoveryPhrase;
    },
  };
}
// #endregion auth-localfirst-svelte-backup

// #region auth-localfirst-svelte-restore
export async function restoreFromRecoveryPhrase(userInput: string) {
  const restoredSecret = RecoveryPhrase.toSecret(userInput);
  await BrowserAuthSecretStore.saveSecret(restoredSecret);
  // Reload so the mounted JazzSvelteProvider picks up the restored secret.
  location.reload();
}
// #endregion auth-localfirst-svelte-restore

// #region auth-localfirst-svelte-passkey-backup
const passkeyBackup = new BrowserPasskeyBackup({
  appName: "My App",
  // Pin to your canonical production hostname. If omitted, defaults to `location.hostname`,
  // which scopes passkeys per preview-deploy URL.
  appHostname: "myapp.com",
});

export async function backupToPasskey(displayName: string) {
  const secret = await BrowserAuthSecretStore.loadSecret();
  if (!secret) throw new Error("No local secret to back up yet");
  await passkeyBackup.backup(secret, displayName);
}
// #endregion auth-localfirst-svelte-passkey-backup

// #region auth-localfirst-svelte-passkey-restore
export async function restoreFromPasskey() {
  const restoredSecret = await passkeyBackup.restore();
  await BrowserAuthSecretStore.saveSecret(restoredSecret);
  location.reload();
}
// #endregion auth-localfirst-svelte-passkey-restore
