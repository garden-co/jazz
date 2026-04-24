import { ref, onMounted } from "vue";
import { BrowserAuthSecretStore } from "jazz-tools";
import { RecoveryPhrase } from "jazz-tools/passphrase";
import { BrowserPasskeyBackup } from "jazz-tools/passkey-backup";

// #region auth-localfirst-vue-backup
export function useRecoveryPhraseBackup() {
  const isLoading = ref(true);
  const recoveryPhrase = ref<string | null>(null);

  onMounted(async () => {
    const secret = await BrowserAuthSecretStore.loadSecret();
    recoveryPhrase.value = secret ? RecoveryPhrase.fromSecret(secret) : null;
    isLoading.value = false;
  });

  return { isLoading, recoveryPhrase };
}
// #endregion auth-localfirst-vue-backup

// #region auth-localfirst-vue-restore
export function useRecoveryPhraseRestore() {
  return async (userInput: string) => {
    const restoredSecret = RecoveryPhrase.toSecret(userInput);
    await BrowserAuthSecretStore.saveSecret(restoredSecret);
    // Reload so the mounted JazzProvider picks up the restored secret.
    location.reload();
  };
}
// #endregion auth-localfirst-vue-restore

// #region auth-localfirst-vue-passkey-backup
const passkeyBackup = new BrowserPasskeyBackup({
  appName: "My App",
  // Pin to your canonical production hostname. If omitted, defaults to `location.hostname`,
  // which scopes passkeys per preview-deploy URL.
  appHostname: "myapp.com",
});

export function usePasskeyBackup() {
  return async (displayName: string) => {
    const secret = await BrowserAuthSecretStore.loadSecret();
    if (!secret) throw new Error("No local secret to back up yet");
    await passkeyBackup.backup(secret, displayName);
  };
}
// #endregion auth-localfirst-vue-passkey-backup

// #region auth-localfirst-vue-passkey-restore
export function usePasskeyRestore() {
  return async () => {
    const restoredSecret = await passkeyBackup.restore();
    await BrowserAuthSecretStore.saveSecret(restoredSecret);
    location.reload();
  };
}
// #endregion auth-localfirst-vue-passkey-restore
