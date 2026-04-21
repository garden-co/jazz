import { BrowserPasskeyBackup } from "jazz-tools/passkey-backup";
import { RecoveryPhrase } from "jazz-tools/passphrase";
import { JazzProvider, useLocalFirstAuth } from "jazz-tools/react";

function TodoApp() {
  return null;
}

const passkeyBackup = new BrowserPasskeyBackup({
  appName: "My App",
  appHostname: "myapp.com",
});

// #region auth-localfirst-react
export function LocalFirstAuthApp() {
  const { secret, isLoading } = useLocalFirstAuth();

  if (isLoading || !secret) return null;

  return (
    <JazzProvider
      config={{
        appId: "my-app",
        secret,
      }}
    >
      <TodoApp />
    </JazzProvider>
  );
}
// #endregion auth-localfirst-react

// #region auth-jwt-react
export function JwtAuthApp() {
  return (
    <JazzProvider
      config={{
        appId: "my-app",
        serverUrl: "http://127.0.0.1:4200",
        jwtToken: "<provider-jwt>",
      }}
    >
      <TodoApp />
    </JazzProvider>
  );
}
// #endregion auth-jwt-react

// #region auth-localfirst-react-backup
export function useRecoveryPhraseBackup(): {
  isLoading: boolean;
  recoveryPhrase: string | null;
} {
  const { secret, isLoading } = useLocalFirstAuth();

  return {
    isLoading,
    recoveryPhrase: secret ? RecoveryPhrase.fromSecret(secret) : null,
  };
}
// #endregion auth-localfirst-react-backup

// #region auth-localfirst-react-restore
export function useRecoveryPhraseRestore(): (userInput: string) => Promise<void> {
  const { login } = useLocalFirstAuth();

  return async (userInput: string) => {
    const restoredSecret = RecoveryPhrase.toSecret(userInput);
    await login(restoredSecret);
  };
}
// #endregion auth-localfirst-react-restore

// #region auth-localfirst-react-passkey-backup
export function usePasskeyBackup(): {
  isLoading: boolean;
  backupWithPasskey: (displayName: string) => Promise<void>;
} {
  const { secret, isLoading } = useLocalFirstAuth();

  return {
    isLoading,
    backupWithPasskey: async (displayName: string) => {
      if (!secret) {
        throw new Error("Local-first secret is not ready yet");
      }

      await passkeyBackup.backup(secret, displayName);
    },
  };
}
// #endregion auth-localfirst-react-passkey-backup

// #region auth-localfirst-react-passkey-restore
export function usePasskeyRestore(): () => Promise<void> {
  const { login } = useLocalFirstAuth();

  return async () => {
    const restoredSecret = await passkeyBackup.restore();
    await login(restoredSecret);
  };
}
// #endregion auth-localfirst-react-passkey-restore
