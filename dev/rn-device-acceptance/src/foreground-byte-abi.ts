import type {
  NativeForegroundCommand,
  NativeForegroundResponse,
  NativeForegroundRuntime,
  NativeForegroundRuntimeFactory,
} from "jazz-rn";

export type ForegroundByteCodec = {
  encode(command: NativeForegroundCommand): Uint8Array;
  decode(bytes: Uint8Array): NativeForegroundResponse;
};

/**
 * Exercise the installed JSI HostObject through the first v1 byte vocabulary.
 * This is intentionally not a React-Native-shaped database API: it proves the
 * compiled C++ bridge copies postcard bytes to the actual foreground owner.
 */
export function proveForegroundByteAbi(
  factory: NativeForegroundRuntimeFactory,
  capability: Uint8Array,
  codec: ForegroundByteCodec,
): NativeForegroundRuntime {
  if (factory.abiVersion !== 4)
    throw new Error(`installed foreground factory has unexpected ABI ${factory.abiVersion}`);
  const foreground = factory.openAttached(capability);
  const probe = codec.decode(foreground.execute(codec.encode("probe")));
  if (probe.type !== "probe" || probe.abiVersion !== 4)
    throw new Error("installed foreground returned an unexpected Probe response");
  const tick = codec.decode(foreground.execute(codec.encode("tick")));
  if (tick.type !== "ticked") throw new Error("installed foreground did not acknowledge Tick");
  const close = codec.decode(foreground.execute(codec.encode("close")));
  if (close.type !== "closed" || !close.closed)
    throw new Error("installed foreground did not acknowledge its first Close");
  assertRejected(
    () => foreground.execute(codec.encode("probe")),
    "foreground accepted Probe after Close",
  );
  return foreground;
}

/** A foreground alias left open before native revoke must no longer execute. */
export function proveForegroundRevoked(
  foreground: NativeForegroundRuntime,
  encode: ForegroundByteCodec["encode"],
): void {
  assertRejected(() => foreground.execute(encode("probe")), "revoked foreground accepted Probe");
}

/**
 * Drive the mutable half of the installed foreground ABI through the JSI
 * HostObject.  The cell payloads are fixed canonical Rust fixture bytes, not
 * a JavaScript row codec: the device host's schema is the matching `todos`
 * text schema and Rust remains the only decoder of the record envelope.
 */
export function proveForegroundWriteAbi(
  factory: NativeForegroundRuntimeFactory,
  capability: Uint8Array,
  codec: ForegroundByteCodec,
): void {
  const foreground = factory.openAttached(capability);
  const execute = (command: NativeForegroundCommand): NativeForegroundResponse =>
    codec.decode(foreground.execute(codec.encode(command)));
  const requireTransaction = (kind: "mergeable" | "exclusive") => {
    const response = execute({ type: "beginTransaction", kind });
    if (response.type !== "transactionOpened")
      throw new Error(`${kind} foreground transaction did not open`);
    return response.transaction;
  };
  const rowId = Uint8Array.from({ length: 16 }, () => 0x71);
  const mergeable = requireTransaction("mergeable");
  const inserted = execute({
    type: "insert",
    transaction: mergeable,
    table: "todos",
    rowId,
    cells: fixtureCells("mergeable"),
  });
  if (inserted.type !== "inserted" || !sameBytes(inserted.rowId, rowId))
    throw new Error("foreground Insert did not return its supplied row id");
  for (const command of [
    { type: "update" as const, patch: fixtureCells("updated") },
    { type: "upsert" as const, cells: fixtureCells("upserted") },
    { type: "delete" as const },
  ]) {
    const response = execute({ ...command, transaction: mergeable, table: "todos", rowId });
    if (response.type !== "mutationStaged")
      throw new Error(`foreground ${command.type} was not staged`);
  }
  const committed = execute({ type: "commitTransaction", transaction: mergeable });
  if (
    committed.type !== "transactionCommitted" ||
    committed.txId.byteLength !== 16 ||
    committed.txId.every((byte) => byte === 0)
  )
    throw new Error("foreground Commit did not return a non-zero public txId");
  const retired = execute({ type: "rollbackTransaction", transaction: mergeable });
  if (retired.type !== "operationError")
    throw new Error("foreground accepted a terminal transaction handle");

  const exclusive = requireTransaction("exclusive");
  const rollbackRowId = Uint8Array.from({ length: 16 }, () => 0x72);
  const rollbackInsert = execute({
    type: "insert",
    transaction: exclusive,
    table: "todos",
    rowId: rollbackRowId,
    cells: fixtureCells("rolled back"),
  });
  if (rollbackInsert.type !== "inserted" || !sameBytes(rollbackInsert.rowId, rollbackRowId))
    throw new Error("exclusive foreground Insert did not return its supplied row id");
  const rolledBack = execute({ type: "rollbackTransaction", transaction: exclusive });
  if (rolledBack.type !== "transactionRolledBack" || !rolledBack.rolledBack)
    throw new Error("exclusive foreground transaction did not roll back");

  // Handles are local to their foreground alias. A sibling cannot commit an
  // otherwise well-formed handle from this terminal transaction.
  const sibling = factory.openAttached(capability);
  const siblingResponse = codec.decode(
    sibling.execute(
      codec.encode({
        type: "commitTransaction",
        transaction: exclusive,
      }),
    ),
  );
  if (siblingResponse.type !== "operationError")
    throw new Error("foreground accepted a transaction handle from another alias");
  sibling.close();
  foreground.close();
}

function fixtureCells(title: "mergeable" | "updated" | "upserted" | "rolled back"): Uint8Array {
  const bytes = {
    mergeable: [
      1, 1, 5, 116, 105, 116, 108, 101, 8, 10, 2, 109, 101, 114, 103, 101, 97, 98, 108, 101,
    ],
    updated: [1, 1, 5, 116, 105, 116, 108, 101, 8, 8, 2, 117, 112, 100, 97, 116, 101, 100],
    upserted: [1, 1, 5, 116, 105, 116, 108, 101, 8, 9, 2, 117, 112, 115, 101, 114, 116, 101, 100],
    "rolled back": [
      1, 1, 5, 116, 105, 116, 108, 101, 8, 12, 2, 114, 111, 108, 108, 101, 100, 32, 98, 97, 99, 107,
    ],
  } as const;
  return Uint8Array.from(bytes[title]);
}

function sameBytes(left: Uint8Array, right: Uint8Array): boolean {
  return left.byteLength === right.byteLength && left.every((byte, index) => byte === right[index]);
}

function assertRejected(action: () => unknown, message: string): void {
  try {
    action();
  } catch {
    return;
  }
  throw new Error(message);
}
