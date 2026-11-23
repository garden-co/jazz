/**
 * Asks BandChat's server to end every other sign-in of this user (see
 * `server/index.ts`): it deletes the user's other sessions and closes their
 * open Jazz connections. Run before removing a lost device, so no open
 * connection of that device stays signed in. Resolves with how many sign-ins
 * it ended; rejects if the server did not do it.
 */
// #region bandchat-end-other-sign-ins
export async function endOtherSignIns(): Promise<number> {
  const response = await fetch("/api/end-other-sign-ins", {
    method: "POST",
    credentials: "same-origin",
  });
  if (!response.ok) {
    throw new Error(`The server could not sign your other browsers out (${response.status}).`);
  }
  const { ended } = (await response.json()) as { ended: number };
  return ended;
}
// #endregion bandchat-end-other-sign-ins
