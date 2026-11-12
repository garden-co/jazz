import type { JazzClient, JazzSession } from "jazz-tools/svelte";
import { RecoveryPhrase } from "jazz-tools/passphrase";
import { BrowserPasskeyBackup } from "jazz-tools/passkey-backup";

// `session`: getJazzSession(), or the session from createJazzClient.
type DeviceKeys = Pick<JazzSession<JazzClient>, "exportDeviceKey" | "restoreDeviceAccount">;

// #region auth-localfirst-svelte-backup
export function getRecoveryPhrase(session: DeviceKeys): string {
  return RecoveryPhrase.fromSecret(session.exportDeviceKey());
}
// #endregion auth-localfirst-svelte-backup

// #region auth-localfirst-svelte-restore
// The session switches to the restored device account.
export function restoreFromRecoveryPhrase(session: DeviceKeys, userInput: string): Promise<void> {
  return session.restoreDeviceAccount(RecoveryPhrase.toSecret(userInput));
}
// #endregion auth-localfirst-svelte-restore

// #region auth-localfirst-svelte-passkey-backup
const passkeyBackup = new BrowserPasskeyBackup({
  appName: "My App",
  // Pin to your canonical production hostname rather than a preview hostname.
  appHostname: "myapp.com",
});

export async function backupToPasskey(session: DeviceKeys, displayName: string): Promise<void> {
  await passkeyBackup.backup(session.exportDeviceKey(), displayName);
}
// #endregion auth-localfirst-svelte-passkey-backup

// #region auth-localfirst-svelte-passkey-restore
export async function restoreFromPasskey(session: DeviceKeys): Promise<void> {
  return session.restoreDeviceAccount(await passkeyBackup.restore());
}
// #endregion auth-localfirst-svelte-passkey-restore
