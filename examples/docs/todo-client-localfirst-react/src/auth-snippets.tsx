import { exportLocalFirstSecret, type AccountHandle } from "jazz-tools";
import { BrowserPasskeyBackup } from "jazz-tools/passkey-backup";
import { RecoveryPhrase } from "jazz-tools/passphrase";
import {
  JazzSessionProvider,
  useJazzSession,
  type JazzSessionConfig,
  type JazzSessionActions,
} from "jazz-tools/react";

function TodoApp() {
  return null;
}

// #region auth-localfirst-react
export function LocalFirstAuthApp({ config }: { config: JazzSessionConfig }) {
  return (
    <JazzSessionProvider config={{ ...config, initial: "local-first" }}>
      <TodoApp />
    </JazzSessionProvider>
  );
}
// #endregion auth-localfirst-react

// #region auth-jwt-react
export function JwtAuthApp({
  getToken,
  config,
}: {
  getToken: () => Promise<string>;
  config: JazzSessionConfig;
}) {
  return (
    <JazzSessionProvider config={config} fallback={<SignIn getToken={getToken} />}>
      <TodoApp />
    </JazzSessionProvider>
  );
}

function SignIn({ getToken }: { getToken: () => Promise<string> }) {
  const { loginJWT, pending, error } = useJazzSession();
  return (
    <>
      <button disabled={!!pending} onClick={() => void loginJWT({ getToken }).catch(() => {})}>
        Sign in
      </button>
      {error && <p role="alert">{error.message}</p>}
    </>
  );
}
// #endregion auth-jwt-react

// #region auth-localfirst-react-backup
export function getRecoveryPhrase(account: AccountHandle): string {
  return RecoveryPhrase.fromSecret(exportLocalFirstSecret(account));
}
// #endregion auth-localfirst-react-backup

// #region auth-localfirst-react-restore
export function restoreRecoveryPhrase(
  session: JazzSessionActions,
  userInput: string,
): Promise<void> {
  return session.restoreLocalFirst(RecoveryPhrase.toSecret(userInput));
}
// #endregion auth-localfirst-react-restore

// #region auth-localfirst-react-passkey-backup
const passkeyBackup = new BrowserPasskeyBackup({ appName: "My App", appHostname: "myapp.com" });
export async function backupWithPasskey(
  account: AccountHandle,
  displayName: string,
): Promise<void> {
  await passkeyBackup.backup(exportLocalFirstSecret(account), displayName);
}
// #endregion auth-localfirst-react-passkey-backup

// #region auth-localfirst-react-passkey-restore
export async function restoreWithPasskey(session: JazzSessionActions): Promise<void> {
  return session.restoreLocalFirst(await passkeyBackup.restore());
}
// #endregion auth-localfirst-react-passkey-restore
