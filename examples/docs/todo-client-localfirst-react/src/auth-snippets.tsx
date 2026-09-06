import {
  createAccountManager,
  exportLocalFirstSecret,
  type AccountHandle,
  type DbConfig,
} from "jazz-tools";
import { BrowserPasskeyBackup } from "jazz-tools/passkey-backup";
import { RecoveryPhrase } from "jazz-tools/passphrase";
import { JazzProvider, useAccountState } from "jazz-tools/react";

type Accounts = Awaited<ReturnType<typeof createAccountManager>>;
function TodoApp() {
  return null;
}

// #region auth-localfirst-react
// Bootstrap outside React: prepare the manager, then getLoggedIn() ?? createLocalFirst().
export function LocalFirstAuthApp({ config }: { config: DbConfig }) {
  return (
    <JazzProvider config={config}>
      <TodoApp />
    </JazzProvider>
  );
}
// #endregion auth-localfirst-react

// #region auth-jwt-react
// This screen starts without a context. Login selects an existing account;
// signup explicitly uses registerJWT. Linking belongs in a separate handoff flow
// after await oldClient.shutdown({ waitForSync: true }).
export function JwtAuthApp({
  accounts,
  getToken,
  config,
}: {
  accounts: Accounts;
  getToken: () => Promise<string>;
  config: Omit<DbConfig, "account">;
}) {
  const { account, pending, error } = useAccountState(accounts);
  if (account)
    return (
      <JazzProvider config={{ ...config, account }}>
        <TodoApp />
      </JazzProvider>
    );
  return (
    <>
      <button
        disabled={!!pending}
        onClick={() => {
          void accounts.loginJWT({ getToken }).catch(() => {});
        }}
      >
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
// Call outside any context after shutdown({ waitForSync: true }); mount with the returned handle.
export function restoreRecoveryPhrase(accounts: Accounts, userInput: string): AccountHandle {
  return accounts.restoreLocalFirst(RecoveryPhrase.toSecret(userInput));
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
// As with phrase recovery, await oldClient.shutdown({ waitForSync: true }) before restoring.
export async function restoreWithPasskey(accounts: Accounts): Promise<AccountHandle> {
  return accounts.restoreLocalFirst(await passkeyBackup.restore());
}
// #endregion auth-localfirst-react-passkey-restore
