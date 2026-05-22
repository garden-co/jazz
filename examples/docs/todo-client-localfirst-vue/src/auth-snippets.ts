import { computed } from "vue";
import { useLocalFirstAuth } from "jazz-tools/vue";
import { RecoveryPhrase } from "jazz-tools/passphrase";
import { BrowserPasskeyBackup } from "jazz-tools/passkey-backup";

// #region auth-localfirst-vue-backup
export function useRecoveryPhraseBackup() {
  const { secret, isLoading } = useLocalFirstAuth();
  const recoveryPhrase = computed(() =>
    secret.value ? RecoveryPhrase.fromSecret(secret.value) : null,
  );
  return { isLoading, recoveryPhrase };
}
// #endregion auth-localfirst-vue-backup

// #region auth-localfirst-vue-restore
export function useRecoveryPhraseRestore() {
  const { login } = useLocalFirstAuth();
  return async (userInput: string) => {
    const restoredSecret = RecoveryPhrase.toSecret(userInput);
    await login(restoredSecret);
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
  const { secret } = useLocalFirstAuth();
  return async (displayName: string) => {
    if (!secret.value) throw new Error("No local secret to back up yet");
    await passkeyBackup.backup(secret.value, displayName);
  };
}
// #endregion auth-localfirst-vue-passkey-backup

// #region auth-localfirst-vue-passkey-restore
export function usePasskeyRestore() {
  const { login } = useLocalFirstAuth();
  return async () => {
    const restoredSecret = await passkeyBackup.restore();
    await login(restoredSecret);
  };
}
// #endregion auth-localfirst-vue-passkey-restore
