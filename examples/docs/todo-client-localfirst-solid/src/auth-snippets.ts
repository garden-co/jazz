import type { JazzClient, JazzSession } from "jazz-tools/solid";
import { RecoveryPhrase } from "jazz-tools/passphrase";
import { BrowserPasskeyBackup } from "jazz-tools/passkey-backup";

// `session`: useJazzSession(), or the session from createJazzClient.
type DeviceKeys = Pick<JazzSession<JazzClient>, "exportDeviceKey" | "restoreDeviceAccount">;

// #region auth-localfirst-solid-backup
export function getRecoveryPhrase(session: DeviceKeys): string {
  return RecoveryPhrase.fromSecret(session.exportDeviceKey());
}
// #endregion auth-localfirst-solid-backup

// #region auth-localfirst-solid-restore
// The session switches to the restored device account.
export function restoreFromRecoveryPhrase(session: DeviceKeys, userInput: string): Promise<void> {
  return session.restoreDeviceAccount(RecoveryPhrase.toSecret(userInput));
}
// #endregion auth-localfirst-solid-restore

// #region auth-localfirst-solid-passkey-backup
const passkeyBackup = new BrowserPasskeyBackup({
  appName: "My App",
  // Pin to your canonical production hostname rather than a preview hostname.
  appHostname: "myapp.com",
});

export async function backupToPasskey(session: DeviceKeys, displayName: string): Promise<void> {
  await passkeyBackup.backup(session.exportDeviceKey(), displayName);
}
// #endregion auth-localfirst-solid-passkey-backup

// #region auth-localfirst-solid-passkey-restore
export async function restoreFromPasskey(session: DeviceKeys): Promise<void> {
  return session.restoreDeviceAccount(await passkeyBackup.restore());
}
// #endregion auth-localfirst-solid-passkey-restore
