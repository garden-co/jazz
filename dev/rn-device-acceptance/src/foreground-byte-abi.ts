import type {
  NativeForegroundCommand,
  NativeForegroundResponse,
  NativeForegroundRuntime,
  NativeForegroundRuntimeFactory,
} from "jazz-rn";
import { NATIVE_RELAY_ABI_VERSION } from "jazz-rn/native-relay-abi";
import type { DeviceDiagnosticCode } from "./device-diagnostics";

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
  markFailure?: (
    stage:
      | "foreground-open-failed"
      | "foreground-probe-failed"
      | "foreground-tick-failed"
      | "foreground-close-failed",
  ) => void,
): NativeForegroundRuntime {
  if (factory.abiVersion !== NATIVE_RELAY_ABI_VERSION)
    throw new Error(`installed foreground factory has unexpected ABI ${factory.abiVersion}`);
  markFailure?.("foreground-open-failed");
  const foreground = factory.openAttached(capability);
  markFailure?.("foreground-probe-failed");
  const probe = codec.decode(foreground.execute(codec.encode("probe")));
  if (probe.type !== "probe" || probe.abiVersion !== NATIVE_RELAY_ABI_VERSION)
    throw new Error("installed foreground returned an unexpected Probe response");
  markFailure?.("foreground-tick-failed");
  const tick = codec.decode(foreground.execute(codec.encode("tick")));
  if (tick.type !== "ticked") throw new Error("installed foreground did not acknowledge Tick");
  markFailure?.("foreground-close-failed");
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
    const response = execute({
      ...command,
      transaction: mergeable,
      table: "todos",
      rowId,
    });
    if (response.type !== "mutationStaged")
      throw new Error(`foreground ${command.type} was not staged`);
  }
  const committed = execute({
    type: "commitTransaction",
    transaction: mergeable,
  });
  if (
    committed.type !== "transactionCommitted" ||
    committed.txId.byteLength !== 16 ||
    committed.txId.every((byte) => byte === 0)
  )
    throw new Error("foreground Commit did not return a non-zero public txId");
  const retired = execute({
    type: "rollbackTransaction",
    transaction: mergeable,
  });
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
  const rolledBack = execute({
    type: "rollbackTransaction",
    transaction: exclusive,
  });
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

/**
 * Actual two-foreground receipt for one installed JSI runtime. A and B are
 * separate foreground aliases in that same JSI runtime, both attached to the
 * same native relay admitted by the capability. B starts a local subscription before A writes;
 * bounded ordinary ticks must then deliver A's committed row as B's binding
 * delta.  This deliberately stays at the byte ABI boundary: JS only checks a
 * fixed fixture title in Rust-produced binding bytes.
 */
export function proveSameJsiRuntimeWriteSubscription(
  factory: NativeForegroundRuntimeFactory,
  capability: Uint8Array,
  codec: ForegroundByteCodec,
): void {
  const a = factory.openAttached(capability);
  const b = factory.openAttached(capability);
  const execute = (foreground: NativeForegroundRuntime, command: NativeForegroundCommand) =>
    codec.decode(foreground.execute(codec.encode(command)));
  try {
    const prepared = execute(b, { type: "prepareQuery", query: TODOS_QUERY });
    if (prepared.type !== "preparedQuery")
      throw new Error("foreground B could not prepare the todos subscription");
    const subscribed = execute(b, { type: "subscribe", query: prepared.query });
    if (subscribed.type !== "subscribed")
      throw new Error("foreground B could not subscribe to the todos query");

    // Consume the subscription's initial reset before performing A's write.
    // The resulting evidence must be the cross-foreground update, not B's
    // initial materialization.
    drainSubscription(b, subscribed.subscription, codec);

    const transaction = execute(a, {
      type: "beginTransaction",
      kind: "mergeable",
    });
    if (transaction.type !== "transactionOpened")
      throw new Error("foreground A write transaction did not open");
    const rowId = Uint8Array.from({ length: 16 }, () => 0x74);
    const staged = execute(a, {
      type: "upsert",
      transaction: transaction.transaction,
      table: "todos",
      rowId,
      cells: fixtureCells("subscription from foreground A"),
    });
    if (staged.type !== "mutationStaged") throw new Error("foreground A write was not staged");
    const committed = execute(a, {
      type: "commitTransaction",
      transaction: transaction.transaction,
    });
    if (committed.type !== "transactionCommitted")
      throw new Error("foreground A write did not commit");

    for (let attempt = 0; attempt < 96; attempt += 1) {
      // Both aliases get fair ordinary relay turns.  This is the same polling
      // progression used by the first native subscription slice, not a test
      // side channel into the persistent SQLite store.
      a.tick();
      b.tick();
      const events = drainSubscription(b, subscribed.subscription, codec);
      if (
        events.some(
          (event) =>
            event.type === "delta" && containsUtf8(event.delta, "foreground-a-subscription-row"),
        )
      ) {
        const closed = execute(b, {
          type: "unsubscribe",
          subscription: subscribed.subscription,
        });
        if (closed.type !== "unsubscribed" || !closed.closed)
          throw new Error("foreground B subscription did not close");
        return;
      }
    }
    throw new Error(
      "foreground B did not observe foreground A's committed row after bounded ticks",
    );
  } finally {
    a.close();
    b.close();
  }
}

/**
 * Prove the device fixture's trusted A -> B path selection is data-plane
 * isolation, not just control-plane capability revocation. The foreground
 * command surface stays byte-only: the fixed query bytes below are the
 * canonical postcard encoding of Rust's `Query::from("todos")`, and the
 * receipt only searches returned binding bytes for the fixed fixture title.
 * It deliberately does not grow a React-Native-shaped row/query API.
 *
 * The caller performs the native A -> B replacement between the two phases,
 * then later revokes B and re-admits A. That final A read proves that closing
 * a scope and its SQLite owner does not discard its data, while B never sees
 * A's row even though both scopes use the same application fixture.
 */
export type ScopeIsolationReceipt = {
  /** A fixed fixture row written through this admitted foreground, if any. */
  write?: "a" | "b";
  /** Fixed fixture rows that this scope must materialize. */
  contains: readonly ("a" | "b")[];
  /** Fixed fixture rows that this scope must never materialize. */
  excludes: readonly ("a" | "b")[];
};

/**
 * This deliberately accepts only the two compile-time fixture row names, not
 * a caller-selected query, path, or payload.  Native platform code remains
 * the sole selector of the app/storage/auth scope behind `capability`.
 */
export async function proveForegroundScopeIsolation(
  factory: NativeForegroundRuntimeFactory,
  capability: Uint8Array,
  codec: ForegroundByteCodec,
  receipt: ScopeIsolationReceipt,
  markFailure: (code: DeviceDiagnosticCode) => void = () => {},
): Promise<void> {
  let writer: ScopeForeground | undefined;
  try {
    if (receipt.write) {
      // The writer and reader are deliberately separate foreground handles. A
      // row must travel through the admitted relay/store rather than appearing
      // only in the memory of the handle that staged it. Keep the writer alive
      // and progressing until the reader observes the committed row: closing a
      // foreground is cancellation, not a flush primitive.
      markFailure("scope-isolation-open-failed");
      const openedWriter = openScopeForeground(factory, capability);
      writer = openedWriter;
      const execute = (command: NativeForegroundCommand): NativeForegroundResponse =>
        codec.decode(openedWriter.runtime.execute(codec.encode(command)));
      markFailure("scope-isolation-write-failed");
      const transaction = execute({
        type: "beginTransaction",
        kind: "mergeable",
      });
      if (transaction.type !== "transactionOpened")
        throw new Error("scope fixture foreground transaction did not open");
      const rowId = Uint8Array.from({ length: 16 }, () => (receipt.write === "a" ? 0x73 : 0x75));
      const staged = execute({
        type: "upsert",
        transaction: transaction.transaction,
        table: "todos",
        rowId,
        cells: fixtureCells(receipt.write === "a" ? "scope A private row" : "scope B private row"),
      });
      if (staged.type !== "mutationStaged")
        throw new Error("scope fixture foreground upsert was not staged");
      const committed = execute({
        type: "commitTransaction",
        transaction: transaction.transaction,
      });
      if (committed.type !== "transactionCommitted")
        throw new Error("scope fixture foreground transaction did not commit");
    }

    markFailure("scope-isolation-open-failed");
    const foreground = openScopeForeground(factory, capability);
    try {
      markFailure("scope-isolation-read-failed");
      const rows = await readTodos(
        foreground.runtime,
        codec,
        (candidate) =>
          receipt.contains.every((scope) => containsUtf8(candidate, scopeFixtureTitle(scope))),
        () => writer?.runtime.tick(),
        foreground.consumeWake,
      );
      markFailure("scope-isolation-assert-failed");
      for (const scope of receipt.contains) {
        if (!containsUtf8(rows, scopeFixtureTitle(scope)))
          throw new Error(
            `scope ${scope.toUpperCase()} did not materialize its persisted fixture row`,
          );
      }
      for (const scope of receipt.excludes) {
        if (containsUtf8(rows, scopeFixtureTitle(scope)))
          throw new Error(
            `scope ${receipt.write?.toUpperCase() ?? "read"} observed scope ${scope.toUpperCase()}'s persisted fixture row`,
          );
      }
    } finally {
      foreground.runtime.close();
    }
  } finally {
    writer?.runtime.close();
  }
}

type ScopeForeground = {
  runtime: NativeForegroundRuntime;
  consumeWake: () => boolean;
};

function openScopeForeground(
  factory: NativeForegroundRuntimeFactory,
  capability: Uint8Array,
): ScopeForeground {
  const foreground = factory.openAttached(capability);
  if (typeof foreground.setTickScheduler !== "function") {
    foreground.close();
    throw new Error("scope isolation foreground cannot install its native wake scheduler");
  }
  // Register the actual ForegroundWakeRegistration/CallInvoker path. The
  // callback only records delivery; the bounded read loop consumes that wake
  // before polling and performs the tick on a later event-loop turn.
  let pendingWakes = 0;
  foreground.setTickScheduler(() => {
    pendingWakes += 1;
  });
  return {
    runtime: foreground,
    consumeWake() {
      if (pendingWakes === 0) return false;
      pendingWakes -= 1;
      return true;
    },
  };
}

function scopeFixtureTitle(scope: "a" | "b") {
  return scope === "a" ? "scope-a-private-row" : "scope-b-private-row";
}

// `postcard::to_allocvec(&Query::from("todos"))` in the shared Rust binding.
// This is intentionally a fixed fixture byte sequence rather than a second
// TypeScript query encoder.
const TODOS_QUERY = Uint8Array.of(
  5,
  116,
  111,
  100,
  111,
  115,
  0,
  0,
  0,
  0,
  0,
  0,
  0,
  0,
  0,
  0,
  0,
  0,
  0,
);

async function readTodos(
  foreground: NativeForegroundRuntime,
  codec: ForegroundByteCodec,
  ready: (rows: Uint8Array) => boolean = () => true,
  progressWriter: () => void = () => {},
  consumeWake: () => boolean = () => true,
): Promise<Uint8Array> {
  const execute = (command: NativeForegroundCommand): NativeForegroundResponse =>
    codec.decode(foreground.execute(codec.encode(command)));
  const prepared = execute({ type: "prepareQuery", query: TODOS_QUERY });
  if (prepared.type !== "preparedQuery")
    throw new Error("scope isolation fixture could not prepare the todos query");
  let pendingOperation: number | undefined;
  for (let attempts = 0; attempts < 96; attempts += 1) {
    progressWriter();
    foreground.tick();
    // Async storage completion wakes the installed foreground through React
    // Native's CallInvoker. A synchronous tick loop starves that callback, so
    // each bounded attempt must hand control back to the app event loop.
    await new Promise<void>((resolve) => setTimeout(resolve, 0));
    if (pendingOperation !== undefined && !consumeWake()) continue;
    const response =
      pendingOperation === undefined
        ? execute({ type: "all", query: prepared.query })
        : execute({ type: "poll", operation: pendingOperation });
    if (response.type === "rows") {
      pendingOperation = undefined;
      if (ready(response.rows)) return response.rows;
      continue;
    }
    if (response.type === "pending") {
      pendingOperation = response.operation;
      continue;
    }
    throw new Error("scope isolation fixture read returned an unexpected response");
  }
  throw new Error("scope isolation fixture read did not settle after bounded ticks");
}

function drainSubscription(
  foreground: NativeForegroundRuntime,
  subscription: number,
  codec: ForegroundByteCodec,
): Extract<NativeForegroundResponse, { type: "subscriptionEvents" }>["events"] {
  const response = codec.decode(
    foreground.execute(codec.encode({ type: "drainSubscription", subscription })),
  );
  if (response.type !== "subscriptionEvents")
    throw new Error("foreground subscription drain returned an unexpected response");
  return response.events;
}

function containsUtf8(bytes: Uint8Array, value: string): boolean {
  const needle = utf8(value);
  return bytes.some(
    (_, offset) =>
      offset + needle.byteLength <= bytes.byteLength &&
      needle.every((byte, index) => bytes[offset + index] === byte),
  );
}

function utf8(value: string): Uint8Array {
  const encoded = encodeURIComponent(value);
  const bytes: number[] = [];
  for (let index = 0; index < encoded.length; index += 1) {
    if (encoded[index] === "%") {
      bytes.push(Number.parseInt(encoded.slice(index + 1, index + 3), 16));
      index += 2;
    } else {
      bytes.push(encoded.charCodeAt(index));
    }
  }
  return Uint8Array.from(bytes);
}

function fixtureCells(
  title:
    | "mergeable"
    | "updated"
    | "upserted"
    | "rolled back"
    | "scope A private row"
    | "scope B private row"
    | "subscription from foreground A",
): Uint8Array {
  const bytes = {
    mergeable: [
      1, 1, 5, 116, 105, 116, 108, 101, 8, 10, 2, 109, 101, 114, 103, 101, 97, 98, 108, 101,
    ],
    updated: [1, 1, 5, 116, 105, 116, 108, 101, 8, 8, 2, 117, 112, 100, 97, 116, 101, 100],
    upserted: [1, 1, 5, 116, 105, 116, 108, 101, 8, 9, 2, 117, 112, 115, 101, 114, 116, 101, 100],
    "rolled back": [
      1, 1, 5, 116, 105, 116, 108, 101, 8, 12, 2, 114, 111, 108, 108, 101, 100, 32, 98, 97, 99, 107,
    ],
    "scope A private row": [
      1, 1, 5, 116, 105, 116, 108, 101, 8, 20, 2, 115, 99, 111, 112, 101, 45, 97, 45, 112, 114, 105,
      118, 97, 116, 101, 45, 114, 111, 119,
    ],
    "scope B private row": [
      1, 1, 5, 116, 105, 116, 108, 101, 8, 20, 2, 115, 99, 111, 112, 101, 45, 98, 45, 112, 114, 105,
      118, 97, 116, 101, 45, 114, 111, 119,
    ],
    "subscription from foreground A": [
      1, 1, 5, 116, 105, 116, 108, 101, 8, 29, 2, 102, 111, 114, 101, 103, 114, 111, 117, 110, 100,
      45, 97, 45, 115, 117, 98, 115, 99, 114, 105, 112, 116, 105, 111, 110, 45, 114, 111, 119,
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
