import { createDb, BrowserAuthSecretStore } from "jazz-tools";
import { RecoveryPhrase } from "jazz-tools/passphrase";
import { BrowserPasskeyBackup } from "jazz-tools/passkey-backup";

// #region auth-localfirst-ts
export async function createLocalFirstDb() {
  const secret = await BrowserAuthSecretStore.getOrCreateSecret({ appId: "my-app" });

  return createDb({
    appId: "my-app",
    secret,
  });
}
// #endregion auth-localfirst-ts

// #region auth-jwt-ts
export async function createJwtDb() {
  return createDb({
    appId: "my-app",
    serverUrl: "http://127.0.0.1:4200",
    jwtToken: "<provider-jwt>",
  });
}
// #endregion auth-jwt-ts

// #region auth-localfirst-ts-backup
export async function getRecoveryPhrase(): Promise<string | null> {
  const secret = await BrowserAuthSecretStore.loadSecret();
  return secret ? RecoveryPhrase.fromSecret(secret) : null;
}
// #endregion auth-localfirst-ts-backup

// #region auth-localfirst-ts-restore
export async function restoreFromRecoveryPhrase(userInput: string): Promise<void> {
  const restoredSecret = RecoveryPhrase.toSecret(userInput);
  await BrowserAuthSecretStore.saveSecret(restoredSecret);
  // Reload so the live Jazz client picks up the restored secret.
  location.reload();
}
// #endregion auth-localfirst-ts-restore

// #region auth-localfirst-ts-passkey-backup
const passkeyBackup = new BrowserPasskeyBackup({
  appName: "My App",
  // Pin to your canonical production hostname. If omitted, defaults to `location.hostname`,
  // which scopes passkeys per preview-deploy URL.
  appHostname: "myapp.com",
});

export async function backupToPasskey(displayName: string): Promise<void> {
  const secret = await BrowserAuthSecretStore.loadSecret();
  if (!secret) throw new Error("No local secret to back up yet");
  await passkeyBackup.backup(secret, displayName);
}
// #endregion auth-localfirst-ts-passkey-backup

// #region auth-localfirst-ts-passkey-restore
export async function restoreFromPasskey(): Promise<void> {
  const restoredSecret = await passkeyBackup.restore();
  await BrowserAuthSecretStore.saveSecret(restoredSecret);
  location.reload();
}
// #endregion auth-localfirst-ts-passkey-restore
