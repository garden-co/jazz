import { CojsonInternalTypes, SessionID } from "cojson";
import { co, z } from "jazz-tools";

export const SuccessMap = co.record(
  z.string(), // sessionID
  z.object({
    nContinouslySuccessful: z.number(),
    laterSuccessfulTransactions: z.array(z.number()),
  }),
);

export function markSuccessful(
  successMap: co.loaded<typeof SuccessMap, { $each: true }>,
  txID: CojsonInternalTypes.TransactionID,
) {
  let entry = successMap[txID.sessionID];
  if (!entry) {
    entry = {
      nContinouslySuccessful: 0,
      laterSuccessfulTransactions: [],
    };
    successMap.$jazz.set(txID.sessionID, entry);
  }

  let nContinouslySuccessful = entry.nContinouslySuccessful;
  let newLaterSuccessfulTransactions = [
    ...entry.laterSuccessfulTransactions,
    txID.txIndex,
  ].sort();

  while (
    newLaterSuccessfulTransactions.length > 0 &&
    newLaterSuccessfulTransactions[0] === nContinouslySuccessful
  ) {
    nContinouslySuccessful++;
    newLaterSuccessfulTransactions.shift();
  }

  successMap.$jazz.set(txID.sessionID, {
    nContinouslySuccessful: nContinouslySuccessful,
    laterSuccessfulTransactions: newLaterSuccessfulTransactions,
  });
}

export function* getTransactionsToRetry(
  successMap: co.loaded<typeof SuccessMap, { $each: true }>,
  knownState: CojsonInternalTypes.CoValueKnownState,
) {
  for (const [sessionID, knownTxCount] of Object.entries(knownState.sessions)) {
    const entry = successMap[sessionID];

    const start = entry ? entry.nContinouslySuccessful : 0;
    const end = knownTxCount;

    for (let txIndex = start; txIndex < end; txIndex++) {
      if (!entry?.laterSuccessfulTransactions.includes(txIndex)) {
        yield {
          sessionID: sessionID as SessionID,
          txIndex: txIndex,
        } satisfies CojsonInternalTypes.TransactionID;
      }
    }
  }
}

export function isTxSuccessful(
  successMap: co.loaded<typeof SuccessMap, { $each: true }>,
  txID: CojsonInternalTypes.TransactionID,
) {
  const entry = successMap[txID.sessionID];
  if (!entry) {
    return false;
  }
  return (
    txID.txIndex < entry.nContinouslySuccessful ||
    entry.laterSuccessfulTransactions.includes(txID.txIndex)
  );
}
