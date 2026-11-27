import type { Db } from "jazz-tools";

// #region e2ee-approve-device-ts
// On the new device: told when it is challenged, then answers and shows the code.
export function answerChallenges(db: Db, show: (code: string) => void) {
  return db.encryption.onChallenge(async () => {
    const code = await db.encryption.devices.answer();
    show(code);
  });
}

// On a trusted device. Call it again after the new device answers if it returns "waiting".
export async function approveWaitingDevice(
  db: Db,
  confirmMatchingCodes: (code: string) => Promise<boolean>,
) {
  const waiting = (await db.encryption.devices.list()).filter((d) => d.status === "waiting");
  const session = await db.encryption.devices.challenge(waiting[0].id);
  const code = await session.code();
  if (code === null) return "waiting";
  if (await confirmMatchingCodes(code)) await session.approve(code);
}
// #endregion e2ee-approve-device-ts

// #region e2ee-recovery-enable-ts
export async function enableRecovery(
  db: Db,
  accountName: string,
  showToKeep: (phrase: string) => void,
) {
  const result = await db.encryption.recovery.enable({ appName: "My app", accountName });
  if (result.method === "phrase") showToKeep(result.phrase);
}
// #endregion e2ee-recovery-enable-ts

// #region e2ee-recovery-restore-ts
export async function restoreAccount(db: Db, phrase?: string) {
  if (phrase) await db.encryption.recovery.restore(phrase);
  else await db.encryption.recovery.restoreFromPasskey({ appName: "My app" });
}
// #endregion e2ee-recovery-restore-ts

// #region e2ee-recovery-phrase-ts
export async function registerRecoveryPhrase(db: Db) {
  const phrase = db.encryption.recovery.create(); // keep it; registers nothing
  await db.encryption.recovery.register(phrase); // on an approved device
  return phrase;
}

// After losing every device, on a new one:
export async function restoreFromPhrase(db: Db, phrase: string) {
  await db.encryption.recovery.restore(phrase);
}
// #endregion e2ee-recovery-phrase-ts
