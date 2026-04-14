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

// #region auth-localfirst-react-passkey-backup
export async function backupWithPasskey(secret: string): Promise<void> {
  const { BrowserPasskeyBackup } = await import("jazz-tools/passkey-backup");
  const pb = new BrowserPasskeyBackup({ appName: "My App", appHostname: "myapp.com" });
  await pb.backup(secret);
}
// #endregion auth-localfirst-react-passkey-backup

// #region auth-localfirst-react-passkey-restore
export async function restoreWithPasskey(): Promise<void> {
  const { BrowserPasskeyBackup } = await import("jazz-tools/passkey-backup");
  const pb = new BrowserPasskeyBackup({ appName: "My App", appHostname: "myapp.com" });
  const secret = await pb.restore();
  await BrowserAuthSecretStore.saveSecret(secret);
}
// #endregion auth-localfirst-react-passkey-restore
