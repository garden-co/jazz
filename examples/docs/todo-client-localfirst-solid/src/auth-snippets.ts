import { useLocalFirstAuth } from "jazz-tools/solid";
import { RecoveryPhrase } from "jazz-tools/passphrase";
import { BrowserPasskeyBackup } from "jazz-tools/passkey-backup";

// #region auth-localfirst-solid-backup
export function useRecoveryPhraseBackup() {
  const auth = useLocalFirstAuth();
  const recoveryPhrase = () => (auth.secret ? RecoveryPhrase.fromSecret(auth.secret) : null);
  return { isLoading: () => auth.isLoading, recoveryPhrase };
}
// #endregion auth-localfirst-solid-backup

// #region auth-localfirst-solid-restore
export function useRecoveryPhraseRestore() {
  const auth = useLocalFirstAuth();
  return async (userInput: string) => {
    const restoredSecret = RecoveryPhrase.toSecret(userInput);
    await auth.login(restoredSecret);
  };
}
// #endregion auth-localfirst-solid-restore

const passkeyBackup = new BrowserPasskeyBackup({
  appName: "My App",
  appHostname: "myapp.com",
});

// #region auth-localfirst-solid-passkey-backup
export function usePasskeyBackup() {
  const auth = useLocalFirstAuth();
  return async (displayName: string) => {
    if (!auth.secret) throw new Error("No local secret to back up yet");
    await passkeyBackup.backup(auth.secret, displayName);
  };
}
// #endregion auth-localfirst-solid-passkey-backup

// #region auth-localfirst-solid-passkey-restore
export function usePasskeyRestore() {
  const auth = useLocalFirstAuth();
  return async () => {
    const restoredSecret = await passkeyBackup.restore();
    await auth.login(restoredSecret);
  };
}
// #endregion auth-localfirst-solid-passkey-restore
