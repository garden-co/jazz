import { use } from "react";
import { BrowserAuthSecretStore } from "jazz-tools";
import { JazzProvider } from "jazz-tools/react";

function TodoApp() {
  return null;
}

// #region auth-localfirst-react
export function LocalFirstAuthApp() {
  const secret = use(BrowserAuthSecretStore.getOrCreateSecret());

  return (
    <JazzProvider
      config={{
        appId: "my-app",
        auth: { localFirstSecret: secret },
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
export async function getRecoveryPhraseForBackup(): Promise<string | null> {
  const secret = await BrowserAuthSecretStore.loadSecret();
  if (!secret) return null;
  const { RecoveryPhrase } = await import("jazz-tools/passphrase");
  return RecoveryPhrase.fromSecret(secret);
}
// #endregion auth-localfirst-react-backup

// #region auth-localfirst-react-restore
export async function restoreFromRecoveryPhrase(userInput: string): Promise<void> {
  const { RecoveryPhrase } = await import("jazz-tools/passphrase");
  const secret = RecoveryPhrase.toSecret(userInput);
  await BrowserAuthSecretStore.saveSecret(secret);
}
// #endregion auth-localfirst-react-restore
