import { BrowserPasskeyBackup } from "jazz-tools/passkey-backup";
import { RecoveryPhrase } from "jazz-tools/passphrase";
import {
  JazzProvider,
  useJazzSession,
  type JazzClient,
  type JazzClientConfig,
  type JazzSession,
} from "jazz-tools/react";

function TodoApp() {
  return null;
}

// #region auth-localfirst-react
// Everyone starts as a guest: a device account, created on this device.
export function DeviceAccountApp({ config }: { config: JazzClientConfig }) {
  return (
    <JazzProvider config={{ ...config, initial: "device" }}>
      <TodoApp />
    </JazzProvider>
  );
}
// #endregion auth-localfirst-react

// #region auth-backend-react
export function BackendAuthApp({ config }: { config: JazzClientConfig }) {
  return (
    <JazzProvider config={config} signedOut={<SignIn />}>
      <TodoApp />
    </JazzProvider>
  );
}

// After your own sign-in has set its session cookie, ask your backend who is signed in.
function SignIn() {
  const { refresh, pending, error } = useJazzSession();
  return (
    <>
      <button disabled={!!pending} onClick={() => void refresh().catch(() => {})}>
        Continue
      </button>
      {error && <p role="alert">{error.message}</p>}
    </>
  );
}
// #endregion auth-backend-react

// #region auth-localfirst-react-backup
// `session`: useJazzSession(), or the session from createJazzClient.
export function getRecoveryPhrase(session: Pick<JazzSession<JazzClient>, "exportDeviceKey">) {
  return RecoveryPhrase.fromSecret(session.exportDeviceKey());
}
// #endregion auth-localfirst-react-backup

// #region auth-localfirst-react-restore
export function restoreRecoveryPhrase(
  session: Pick<JazzSession<JazzClient>, "restoreDeviceAccount">,
  userInput: string,
): Promise<void> {
  return session.restoreDeviceAccount(RecoveryPhrase.toSecret(userInput));
}
// #endregion auth-localfirst-react-restore

// #region auth-localfirst-react-passkey-backup
const passkeyBackup = new BrowserPasskeyBackup({ appName: "My App", appHostname: "myapp.com" });

export async function backupWithPasskey(
  session: Pick<JazzSession<JazzClient>, "exportDeviceKey">,
  displayName: string,
): Promise<void> {
  await passkeyBackup.backup(session.exportDeviceKey(), displayName);
}
// #endregion auth-localfirst-react-passkey-backup

// #region auth-localfirst-react-passkey-restore
export async function restoreWithPasskey(
  session: Pick<JazzSession<JazzClient>, "restoreDeviceAccount">,
): Promise<void> {
  return session.restoreDeviceAccount(await passkeyBackup.restore());
}
// #endregion auth-localfirst-react-passkey-restore

// #region auth-link-react
// The guest signs up with your auth; keep their device account and its data.
export function KeepGuestData({ signUp }: { signUp: () => Promise<void> }) {
  const { user, linkDeviceAccount } = useJazzSession();
  if (user?.kind !== "device") return null;
  return (
    <button
      onClick={async () => {
        await signUp(); // your own sign-up, which sets your session cookie
        await linkDeviceAccount();
      }}
    >
      Sign up and keep my todos
    </button>
  );
}
// #endregion auth-link-react

// #region auth-signout-react
export function SignOutButton({ signOut }: { signOut: () => Promise<void> }) {
  const session = useJazzSession();
  return (
    <button
      onClick={async () => {
        await signOut(); // your own sign-out, which clears your session cookie
        await session.refresh(); // Jazz follows your backend: no one is signed in now
      }}
    >
      Sign out
    </button>
  );
}
// #endregion auth-signout-react
