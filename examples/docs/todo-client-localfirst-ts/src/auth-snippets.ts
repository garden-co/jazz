import { RecoveryPhrase } from "jazz-tools/passphrase";
import { BrowserPasskeyBackup } from "jazz-tools/passkey-backup";

import { createJazzClient, type JazzClient, type JazzSession } from "jazz-tools/client";

type Session = JazzSession<JazzClient>;

// #region auth-localfirst-ts
export async function createDeviceAccountSession() {
  return createJazzClient({
    serverUrl: "/jazz",
    // Start as a device account: a guest, with no sign-in.
    initial: "device",
    // Local data, so the guest works offline from its first visit.
    client: () => import("jazz-tools/local-first"),
  });
}
// #endregion auth-localfirst-ts

// #region auth-backend-ts
export async function createBackendSession() {
  // Jazz runs in your backend under /jazz; your session cookie is the only credential.
  // The session asks your backend who is signed in, and follows its answer.
  return createJazzClient({ serverUrl: "/jazz" });
}

export async function afterYourSignIn(session: Session) {
  // Your own sign-in (or sign-out) changed the cookie: ask the backend again.
  await session.refresh();
}
// #endregion auth-backend-ts

// #region auth-localfirst-ts-backup
export function getRecoveryPhrase(session: Session): string {
  return RecoveryPhrase.fromSecret(session.exportDeviceKey());
}
// #endregion auth-localfirst-ts-backup

// #region auth-localfirst-ts-restore
export function restoreFromRecoveryPhrase(session: Session, userInput: string): Promise<void> {
  return session.restoreDeviceAccount(RecoveryPhrase.toSecret(userInput));
}
// #endregion auth-localfirst-ts-restore

// #region auth-localfirst-ts-passkey-backup
const passkeyBackup = new BrowserPasskeyBackup({
  appName: "My App",
  // Pin to your canonical production hostname rather than a preview hostname.
  appHostname: "myapp.com",
});

export async function backupToPasskey(session: Session, displayName: string): Promise<void> {
  await passkeyBackup.backup(session.exportDeviceKey(), displayName);
}
// #endregion auth-localfirst-ts-passkey-backup

// #region auth-localfirst-ts-passkey-restore
export async function restoreFromPasskey(session: Session): Promise<void> {
  return session.restoreDeviceAccount(await passkeyBackup.restore());
}
// #endregion auth-localfirst-ts-passkey-restore

// #region auth-link-device-account-ts
export async function afterYourSignUp(session: Session) {
  // The guest just signed up with your own auth: keep its account and data.
  await session.linkDeviceAccount();
}
// #endregion auth-link-device-account-ts
