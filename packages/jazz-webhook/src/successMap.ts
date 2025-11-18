import { CojsonInternalTypes, SessionID } from "cojson";
import { co, z } from "jazz-tools";

export const SuccessMap = co.record(
  z.string(), // stringified transaction ID
  z.boolean(),
);
export type SuccessMap = co.loaded<typeof SuccessMap>;

export type TxIdKey = `${SessionID}:${number}`;

export function getTxIdKey(txID: {
  sessionID: SessionID;
  txIndex: number;
}): TxIdKey {
  return `${txID.sessionID}:${txID.txIndex}`;
}

export function markSuccessful(
  successMap: SuccessMap,
  txID: CojsonInternalTypes.TransactionID,
) {
  let success = successMap[getTxIdKey(txID)];
  if (!success) {
    successMap.$jazz.set(getTxIdKey(txID), true);
  }
}

export function getTransactionsToTry(
  successMap: SuccessMap,
  knownState: CojsonInternalTypes.CoValueKnownState,
) {
  const result: CojsonInternalTypes.TransactionID[] = [];
  // TODO: optimisation: we can likely avoid even constructing a CoMap view
  // and just get/set raw transactions from the CoValueCore of SuccessMap
  for (const [sessionID, knownTxCount] of Object.entries(knownState.sessions)) {
    for (let txIndex = 0; txIndex < knownTxCount; txIndex++) {
      if (!successMap[`${sessionID}:${txIndex}`]) {
        result.push({
          sessionID: sessionID as SessionID,
          txIndex: txIndex,
        });
      }
    }
  }

  return result;
}

export function isTxSuccessful(
  successMap: SuccessMap,
  txID: CojsonInternalTypes.TransactionID,
) {
  return successMap[getTxIdKey(txID)] ?? false;
}
