import { useLocalFirstAuth } from "jazz-tools/solid";
import { RecoveryPhrase } from "jazz-tools/passphrase";
import { BrowserPasskeyBackup } from "jazz-tools/passkey-backup";

export function useRecoveryPhraseBackup() {
  const auth = useLocalFirstAuth();
  const recoveryPhrase = () => (auth.secret ? RecoveryPhrase.fromSecret(auth.secret) : null);
  return { isLoading: () => auth.isLoading, recoveryPhrase };
}

export function useRecoveryPhraseRestore() {
  const auth = useLocalFirstAuth();
  return async (userInput: string) => {
    const restoredSecret = RecoveryPhrase.toSecret(userInput);
    await auth.login(restoredSecret);
  };
}

const passkeyBackup = new BrowserPasskeyBackup({
  appName: "My App",
  appHostname: "myapp.com",
});

export function usePasskeyBackup() {
  const auth = useLocalFirstAuth();
  return async (displayName: string) => {
    if (!auth.secret) throw new Error("No local secret to back up yet");
    await passkeyBackup.backup(auth.secret, displayName);
  };
}

export function usePasskeyRestore() {
  const auth = useLocalFirstAuth();
  return async () => {
    const restoredSecret = await passkeyBackup.restore();
    await auth.login(restoredSecret);
  };
}
