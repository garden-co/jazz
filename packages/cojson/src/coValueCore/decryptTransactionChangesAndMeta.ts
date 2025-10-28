import { AvailableCoValueCore, VerifiedTransaction } from "./coValueCore.js";

export function decryptTransactionChangesAndMeta(
  coValue: AvailableCoValueCore,
  transaction: VerifiedTransaction,
) {
  if (
    !transaction.isValid ||
    transaction.tx.privacy === "trusting" // Trusting transactions are already decrypted
  ) {
    return;
  }

  const needsChagesParsing = !transaction.changes;
  const needsMetaParsing = !transaction.meta && transaction.tx.meta;

  if (!needsChagesParsing && !needsMetaParsing) {
    return;
  }

  const readKey = coValue.getReadKey(transaction.tx.keyUsed);

  if (!readKey) {
    return;
  }

  if (needsChagesParsing) {
    const changes = coValue.verified.decryptTransaction(
      transaction.txID.sessionID,
      transaction.txID.txIndex,
      readKey,
    );

    if (changes) {
      transaction.changes = changes;
    }
  }

  if (needsMetaParsing) {
    const meta = coValue.verified.decryptTransactionMeta(
      transaction.txID.sessionID,
      transaction.txID.txIndex,
      readKey,
    );

    if (meta) {
      transaction.meta = meta;
    }
  }
}
