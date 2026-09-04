import nativeRelay from './NativeJazzRelay';
import { NATIVE_RELAY_ABI, NATIVE_RELAY_ABI_V1 } from './native-relay-abi';

/**
 * Versioned private global installed by the native JSI bridge.
 *
 * This is an implementation detail of `jazz-rn`: applications keep using
 * `jazz-tools/react-native`; they neither construct nor retain a foreground
 * engine directly. A string key lets native C++ install a JSI HostObject in the
 * current JavaScript runtime without a second JavaScript/WASM loader.
 */
const NATIVE_FOREGROUND_RUNTIME_GLOBAL = '__jazzNativeForegroundRuntimeV1';

export interface NativeRelayAbiRange {
  minimum: number;
  maximum: number;
}

export { NATIVE_RELAY_ABI, NATIVE_RELAY_ABI_V1 };

function requireNativeRelay() {
  if (nativeRelay == null) {
    throw new Error(
      'Jazz native relay is unavailable: install a matching native development or release build containing the Jazz relay artifact. Expo Go never includes it.'
    );
  }
  return nativeRelay;
}

export type NativeForegroundRuntimeFactory = {
  /** Must match the enclosing native relay ABI before a runtime is opened. */
  readonly abiVersion: number;
  /**
   * Create one memory-only foreground runtime for an already admitted scope.
   * The returned JSI HostObject is consumed only by Jazz's internal native
   * adapter; its database method contract is intentionally not duplicated in
   * this package.
   */
  openAttached(capability: Uint8Array): NativeForegroundRuntime;
};

/**
 * Private, byte-oriented handle for one native in-memory foreground `Db`.
 *
 * The command and response bytes are postcard values owned by the shared Rust
 * relay ABI. This is deliberately not a React-Native-shaped row/query API:
 * `jazz-tools` is the sole adapter that will map its existing `NativeDb`
 * contract onto these commands.
 */
export type NativeForegroundRuntime = {
  execute(command: Uint8Array): Uint8Array;
  tick(): void;
  /**
   * Private wake registration used by jazz-tools' normal NativeRuntimeAdapter.
   * The native HostObject coalesces owner-thread wakes onto the current JSI
   * runtime; this is not an application callback API.
   */
  setTickScheduler?(callback: (urgency: string) => void): void;
  close(): boolean;
};

/**
 * Shared foreground NativeDb command vocabulary. Query bytes are the
 * canonical postcard query bytes from `jazz-tools`' existing codec, never a
 * React-Native-shaped query object. The first read slice intentionally fixes
 * `ReadOpts` to the regular local-first defaults; callers must not silently
 * reinterpret remote tiers, views, or relation terminal operations.
 */
export type NativeForegroundCommand =
  | 'probe'
  | 'tick'
  | { type: 'prepareQuery'; query: Uint8Array }
  | { type: 'all'; query: number }
  | { type: 'allWithOptions' | 'allRelationSnapshotWithOptions'; query: number; optionsJson: string; transaction?: number }
  | { type: 'subscribe'; query: number }
  | { type: 'drainSubscription'; subscription: number }
  | { type: 'unsubscribe'; subscription: number }
  | 'close'
  | { type: 'poll'; operation: number }
  | { type: 'cancel'; operation: number }
  | { type: 'beginTransaction'; kind: NativeForegroundTransactionKind }
  | {
      type: 'insert';
      transaction: number;
      table: string;
      cells: Uint8Array;
      rowId?: Uint8Array;
    }
  | {
      type: 'update';
      transaction: number;
      table: string;
      rowId: Uint8Array;
      patch: Uint8Array;
    }
  | {
      type: 'upsert';
      transaction: number;
      table: string;
      rowId: Uint8Array;
      cells: Uint8Array;
    }
  | { type: 'delete'; transaction: number; table: string; rowId: Uint8Array }
  | { type: 'commitTransaction'; transaction: number }
  | { type: 'rollbackTransaction'; transaction: number }
  | { type: 'subscribeWithOptions'; query: number; optionsJson: string }
  | { type: 'waitForTransaction'; txId: Uint8Array; tier: string }
  | { type: 'disconnectNativeUpstream' }
  | { type: 'reconnectNativeUpstream' }
  | { type: 'nativeConnectionStatus' };

/** The existing core transaction semantics selected by the foreground codec. */
export type NativeForegroundTransactionKind = 'mergeable' | 'exclusive';

export type NativeForegroundResponse =
  | { type: 'nativeConnectionStatus'; configured: boolean; explicitlyOffline: boolean; connected: boolean }
  | { type: 'probe'; abiVersion: number }
  | { type: 'ticked' }
  | { type: 'preparedQuery'; query: number }
  | { type: 'rows'; rows: Uint8Array }
  | { type: 'subscribed'; subscription: number }
  | { type: 'subscriptionEvents'; events: NativeForegroundSubscriptionEvent[] }
  | { type: 'unsubscribed'; closed: boolean }
  | { type: 'closed'; closed: boolean }
  | { type: 'pending'; operation: number }
  | { type: 'operationError'; reason: string }
  | { type: 'cancelled'; cancelled: boolean }
  | { type: 'transactionOpened'; transaction: number }
  | { type: 'inserted'; rowId: Uint8Array }
  | { type: 'mutationStaged' }
  | { type: 'transactionCommitted'; txId: Uint8Array }
  | { type: 'transactionRolledBack'; rolledBack: boolean }
  | { type: 'transactionSettled'; txId: Uint8Array };

export type NativeForegroundSubscriptionEvent =
  | {
      type: 'delta';
      reset: boolean;
      settled: boolean;
      tier: string;
      delta: Uint8Array;
      terminalOperations?: unknown[];
    }
  | { type: 'rejected'; reason: string }
  | { type: 'closed' };

function foregroundRuntimeInstallationError(): Error {
  return new Error(
    'Jazz native foreground runtime installation failed: the native build did not install a compatible JSI foreground engine. Install a matching native development or release build.'
  );
}

function requireCompatibleRelay() {
  const relay = requireNativeRelay();
  const nativeAbi = relay.getAbiVersion();
  if (
    nativeAbi < NATIVE_RELAY_ABI.minimum ||
    nativeAbi > NATIVE_RELAY_ABI.maximum
  ) {
    throw new Error(
      `Jazz native relay ABI ${nativeAbi} is incompatible with JavaScript ABI ${NATIVE_RELAY_ABI.minimum}..=${NATIVE_RELAY_ABI.maximum}; install a matching native development or release build.`
    );
  }
  return relay;
}

/**
 * Install and retrieve the private JSI foreground-runtime factory.
 *
 * It verifies the embedded relay before looking at the global: an OTA bundle
 * must not attach an old factory merely because a stale global survives a
 * bridge reload. The factory receives only the platform-issued opaque
 * capability, never path/schema/claims/identity/token configuration.
 *
 * @internal `jazz-tools/react-native` will call this once it selects the
 * native JSI engine instead of the browser/WASM runtime.
 */
export function installNativeForegroundRuntime(): NativeForegroundRuntimeFactory {
  const relay = requireCompatibleRelay();
  // React Native's TurboModuleWithJSIBindings lifecycle installs this factory
  // while resolving NativeJazzRelay in exactly the current JSI runtime. A new
  // runtime has a new global object; deleting the factory and trying to
  // reconstruct it through an ordinary TurboModule call discards the runtime
  // binding React Native deliberately provided.
  const descriptor = Object.getOwnPropertyDescriptor(
    globalThis,
    NATIVE_FOREGROUND_RUNTIME_GLOBAL
  );
  const factory = descriptor?.value;
  if (
    !factory ||
    typeof factory !== 'object' ||
    (factory as { abiVersion?: unknown }).abiVersion !==
      relay.getAbiVersion() ||
    typeof (factory as { openAttached?: unknown }).openAttached !== 'function'
  ) {
    throw foregroundRuntimeInstallationError();
  }
  const installed = factory as NativeForegroundRuntimeFactory;
  return {
    abiVersion: installed.abiVersion,
    openAttached(capability: Uint8Array): NativeForegroundRuntime {
      // This is not the authorization check--the native host still validates
      // capability admission and copies its bytes before queuing work. It does
      // keep malformed JavaScript input from reaching the JSI command bridge.
      if (!(capability instanceof Uint8Array) || capability.byteLength !== 32) {
        throw new Error(
          'Jazz native foreground runtime requires a 32-byte admitted capability'
        );
      }
      const foreground = installed.openAttached(
        capability
      ) as Partial<NativeForegroundRuntime>;
      if (
        !foreground ||
        typeof foreground.execute !== 'function' ||
        typeof foreground.tick !== 'function' ||
        typeof foreground.close !== 'function'
      ) {
        throw foregroundRuntimeInstallationError();
      }
      return {
        execute(command: Uint8Array): Uint8Array {
          if (!(command instanceof Uint8Array)) {
            throw new Error(
              'Jazz native foreground command requires a Uint8Array'
            );
          }
          const response = foreground.execute!(command);
          if (!(response instanceof Uint8Array)) {
            throw foregroundRuntimeInstallationError();
          }
          return response;
        },
        tick(): void {
          foreground.tick!();
        },
        setTickScheduler(callback: (urgency: string) => void): void {
          if (typeof foreground.setTickScheduler !== 'function') {
            throw foregroundRuntimeInstallationError();
          }
          foreground.setTickScheduler(callback);
        },
        close(): boolean {
          return foreground.close!();
        },
      };
    },
  };
}

/** Encode one foreground NativeDb command without exposing a row/object ABI. */
export function encodeNativeForegroundCommand(
  command: NativeForegroundCommand
): Uint8Array {
  if (command === 'probe') return Uint8Array.of(0);
  if (command === 'tick') return Uint8Array.of(1);
  if (command === 'close') return Uint8Array.of(7);
  if (command.type === 'disconnectNativeUpstream') return Uint8Array.of(23);
  if (command.type === 'reconnectNativeUpstream') return Uint8Array.of(24);
  if (command.type === 'nativeConnectionStatus') return Uint8Array.of(25);
  if (command.type === 'prepareQuery') {
    return concatForegroundBytes(
      Uint8Array.of(2),
      encodeForegroundBytes(command.query)
    );
  }
  if (command.type === 'subscribeWithOptions')
    return concatForegroundBytes(Uint8Array.of(20), encodeForegroundU64(command.query), encodeForegroundString(command.optionsJson));
  if (command.type === 'waitForTransaction')
    return concatForegroundBytes(Uint8Array.of(21), encodeForegroundId(command.txId, 'transaction id'), encodeForegroundString(command.tier));
  if (command.type === 'all')
    return concatForegroundBytes(
      Uint8Array.of(3),
      encodeForegroundU64(command.query)
    );
  if (command.type === 'allWithOptions' || command.type === 'allRelationSnapshotWithOptions')
    return concatForegroundBytes(
      Uint8Array.of(command.type === 'allWithOptions' ? 18 : 19),
      encodeForegroundU64(command.query),
      encodeForegroundString(command.optionsJson),
      command.transaction === undefined ? Uint8Array.of(0) : concatForegroundBytes(Uint8Array.of(1), encodeForegroundU64(command.transaction))
    );
  if (command.type === 'subscribe')
    return concatForegroundBytes(
      Uint8Array.of(4),
      encodeForegroundU64(command.query)
    );
  if (command.type === 'drainSubscription')
    return concatForegroundBytes(
      Uint8Array.of(5),
      encodeForegroundU64(command.subscription)
    );
  if (command.type === 'unsubscribe')
    return concatForegroundBytes(
      Uint8Array.of(6),
      encodeForegroundU64(command.subscription)
    );
  if (command.type === 'poll')
    return concatForegroundBytes(
      Uint8Array.of(8),
      encodeForegroundU64(command.operation)
    );
  if (command.type === 'cancel')
    return concatForegroundBytes(
      Uint8Array.of(9),
      encodeForegroundU64(command.operation)
    );
  if (command.type === 'beginTransaction') {
    if (command.kind !== 'mergeable' && command.kind !== 'exclusive') {
      throw new Error(
        'Jazz native foreground transaction kind must be mergeable or exclusive'
      );
    }
    return Uint8Array.of(10, command.kind === 'mergeable' ? 0 : 1);
  }
  if (command.type === 'insert') {
    return concatForegroundBytes(
      Uint8Array.of(11),
      encodeForegroundU64(command.transaction),
      encodeForegroundString(command.table),
      encodeForegroundBytes(command.cells),
      command.rowId === undefined
        ? Uint8Array.of(0)
        : concatForegroundBytes(
            Uint8Array.of(1),
            encodeForegroundId(command.rowId, 'row id')
          )
    );
  }
  if (command.type === 'update') {
    return concatForegroundBytes(
      Uint8Array.of(12),
      encodeForegroundU64(command.transaction),
      encodeForegroundString(command.table),
      encodeForegroundId(command.rowId, 'row id'),
      encodeForegroundBytes(command.patch)
    );
  }
  if (command.type === 'upsert') {
    return concatForegroundBytes(
      Uint8Array.of(13),
      encodeForegroundU64(command.transaction),
      encodeForegroundString(command.table),
      encodeForegroundId(command.rowId, 'row id'),
      encodeForegroundBytes(command.cells)
    );
  }
  if (command.type === 'delete') {
    return concatForegroundBytes(
      Uint8Array.of(14),
      encodeForegroundU64(command.transaction),
      encodeForegroundString(command.table),
      encodeForegroundId(command.rowId, 'row id')
    );
  }
  if (command.type === 'commitTransaction')
    return concatForegroundBytes(
      Uint8Array.of(15),
      encodeForegroundU64(command.transaction)
    );
  return concatForegroundBytes(
    Uint8Array.of(16),
    encodeForegroundU64(command.transaction)
  );
}

/** Decode the first vertical-slice foreground NativeDb response vocabulary. */
export function decodeNativeForegroundResponse(
  bytes: Uint8Array
): NativeForegroundResponse {
  if (!(bytes instanceof Uint8Array) || bytes.length === 0) {
    throw new Error(
      'Jazz native foreground returned an empty or malformed command response'
    );
  }
  const tag = bytes[0]!;
  if (tag === 0) {
    const abiVersion = decodePostcardU16(bytes.subarray(1));
    if (abiVersion === null) {
      throw new Error(
        'Jazz native foreground returned a malformed probe response'
      );
    }
    return { type: 'probe', abiVersion };
  }
  if (tag === 1 && bytes.length === 1) return { type: 'ticked' };
  if (tag === 2)
    return {
      type: 'preparedQuery',
      query: decodeForegroundU64(bytes.subarray(1), 'prepared query'),
    };
  if (tag === 3)
    return {
      type: 'rows',
      rows: decodeForegroundBytes(bytes.subarray(1), 'rows'),
    };
  if (tag === 4)
    return {
      type: 'subscribed',
      subscription: decodeForegroundU64(bytes.subarray(1), 'subscription'),
    };
  if (tag === 5)
    return {
      type: 'subscriptionEvents',
      events: decodeForegroundSubscriptionEvents(bytes.subarray(1)),
    };
  if (tag === 6 && bytes.length === 2 && (bytes[1] === 0 || bytes[1] === 1)) {
    return { type: 'unsubscribed', closed: bytes[1] === 1 };
  }
  if (tag === 7 && bytes.length === 2 && (bytes[1] === 0 || bytes[1] === 1)) {
    return { type: 'closed', closed: bytes[1] === 1 };
  }
  if (tag === 8)
    return {
      type: 'pending',
      operation: decodeForegroundU64(bytes.subarray(1), 'pending operation'),
    };
  if (tag === 9)
    return {
      type: 'operationError',
      reason: decodeForegroundString(bytes.subarray(1), 'operation error'),
    };
  if (tag === 10 && bytes.length === 2 && (bytes[1] === 0 || bytes[1] === 1)) {
    return { type: 'cancelled', cancelled: bytes[1] === 1 };
  }
  if (tag === 11)
    return {
      type: 'transactionOpened',
      transaction: decodeForegroundU64(bytes.subarray(1), 'transaction'),
    };
  if (tag === 12)
    return {
      type: 'inserted',
      rowId: decodeForegroundId(bytes.subarray(1), 'inserted row id'),
    };
  if (tag === 13 && bytes.length === 1) return { type: 'mutationStaged' };
  if (tag === 14)
    return {
      type: 'transactionCommitted',
      txId: decodeForegroundId(bytes.subarray(1), 'committed txId'),
    };
  if (tag === 16) return { type: 'transactionSettled', txId: decodeForegroundId(bytes.subarray(1), 'settled txId') };

  if (tag === 17 && bytes.length === 4 && bytes.subarray(1).every(value => value === 0 || value === 1)) {
    return { type: 'nativeConnectionStatus', configured: bytes[1] === 1, explicitlyOffline: bytes[2] === 1, connected: bytes[3] === 1 };
  }
  if (tag === 15 && bytes.length === 2 && (bytes[1] === 0 || bytes[1] === 1)) {
    return { type: 'transactionRolledBack', rolledBack: bytes[1] === 1 };
  }
  throw new Error(
    'Jazz native foreground returned an unknown or malformed command response'
  );
}

function encodeForegroundU64(value: number): Uint8Array {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new Error(
      'Jazz native foreground handle must be a non-negative safe integer'
    );
  }
  const bytes: number[] = [];
  let remaining = value;
  do {
    let byte = remaining % 128;
    remaining = Math.floor(remaining / 128);
    if (remaining > 0) byte |= 0x80;
    bytes.push(byte);
  } while (remaining > 0);
  return Uint8Array.from(bytes);
}

function encodeForegroundBytes(value: Uint8Array): Uint8Array {
  if (!(value instanceof Uint8Array))
    throw new Error('Jazz native foreground command requires Uint8Array bytes');
  return concatForegroundBytes(encodeForegroundU64(value.byteLength), value);
}

function encodeForegroundId(value: Uint8Array, label: string): Uint8Array {
  if (!(value instanceof Uint8Array) || value.byteLength !== 16) {
    throw new Error(
      `Jazz native foreground ${label} must be a 16-byte Uint8Array`
    );
  }
  return value;
}

function decodeForegroundId(bytes: Uint8Array, label: string): Uint8Array {
  if (bytes.byteLength !== 16)
    throw new Error(`Jazz native foreground returned malformed ${label}`);
  return bytes.slice();
}

function encodeForegroundString(value: string): Uint8Array {
  if (typeof value !== 'string')
    throw new Error('Jazz native foreground table must be a string');
  // React Native's configured TS lib does not promise TextEncoder. This is
  // the inverse of the strict URI-based UTF-8 decoder below and keeps the
  // command codec dependency-free in Hermes.
  const encoded = encodeURIComponent(value);
  const bytes: number[] = [];
  for (let index = 0; index < encoded.length; index += 1) {
    if (encoded[index] === '%') {
      const hex = encoded.slice(index + 1, index + 3);
      if (hex.length !== 2)
        throw new Error('Jazz native foreground table is malformed UTF-8');
      bytes.push(Number.parseInt(hex, 16));
      index += 2;
    } else {
      bytes.push(encoded.charCodeAt(index));
    }
  }
  return encodeForegroundBytes(Uint8Array.from(bytes));
}

function concatForegroundBytes(...parts: Uint8Array[]): Uint8Array {
  const result = new Uint8Array(
    parts.reduce((length, part) => length + part.byteLength, 0)
  );
  let offset = 0;
  for (const part of parts) {
    result.set(part, offset);
    offset += part.byteLength;
  }
  return result;
}

function decodeForegroundU64(bytes: Uint8Array, label: string): number {
  let value = 0;
  let multiplier = 1;
  for (let index = 0; index < bytes.length && index < 10; index += 1) {
    const byte = bytes[index]!;
    value += (byte & 0x7f) * multiplier;
    if (
      (byte & 0x80) === 0 &&
      index + 1 === bytes.length &&
      Number.isSafeInteger(value)
    )
      return value;
    multiplier *= 128;
  }
  throw new Error(`Jazz native foreground returned malformed ${label}`);
}

function decodeForegroundBytes(bytes: Uint8Array, label: string): Uint8Array {
  let length = 0;
  let multiplier = 1;
  for (let index = 0; index < bytes.length && index < 10; index += 1) {
    const byte = bytes[index]!;
    length += (byte & 0x7f) * multiplier;
    if ((byte & 0x80) === 0) {
      const body = bytes.subarray(index + 1);
      if (body.byteLength === length) return body;
      break;
    }
    multiplier *= 128;
  }
  throw new Error(`Jazz native foreground returned malformed ${label}`);
}

function decodeForegroundString(bytes: Uint8Array, label: string): string {
  const encoded = decodeForegroundBytes(bytes, label);
  return decodeForegroundUtf8(encoded, 0, encoded.byteLength, label);
}

function decodeForegroundSubscriptionEvents(
  bytes: Uint8Array
): NativeForegroundSubscriptionEvent[] {
  // This slice intentionally exposes the encoded event envelope only through
  // this module. `jazz-tools` will consume the normal binding delta bytes;
  // malformed/unknown events fail closed instead of becoming an empty update.
  let offset = 0;
  const readVarint = (): number => {
    let value = 0;
    let multiplier = 1;
    for (let index = 0; index < 10; index += 1) {
      const byte = bytes[offset++];
      if (byte === undefined)
        throw new Error(
          'Jazz native foreground returned truncated subscription events'
        );
      value += (byte & 0x7f) * multiplier;
      if ((byte & 0x80) === 0 && Number.isSafeInteger(value)) return value;
      multiplier *= 128;
    }
    throw new Error(
      'Jazz native foreground returned malformed subscription events'
    );
  };
  const count = readVarint();
  const events: NativeForegroundSubscriptionEvent[] = [];
  for (let index = 0; index < count; index += 1) {
    const tag = readVarint();
    if (tag === 0 || tag === 3) {
      const reset = bytes[offset++];
      const settled = bytes[offset++];
      if ((reset !== 0 && reset !== 1) || (settled !== 0 && settled !== 1))
        throw new Error(
          'Jazz native foreground returned malformed delta flags'
        );
      const tierLength = readVarint();
      const tier = decodeForegroundUtf8(bytes, offset, tierLength, 'tier');
      offset += tierLength;
      const deltaLength = readVarint();
      const delta = bytes.slice(offset, offset + deltaLength);
      offset += deltaLength;
      if (delta.byteLength !== deltaLength)
        throw new Error(
          'Jazz native foreground returned truncated subscription delta'
        );
      let terminalOperations: unknown[] | undefined;
      if (tag === 3) {
        const length = readVarint();
        const json = decodeForegroundUtf8(bytes, offset, length, 'terminal operations');
        offset += length;
        const parsed: unknown = JSON.parse(json);
        if (!Array.isArray(parsed)) throw new Error('Jazz native foreground returned malformed terminal operations');
        terminalOperations = parsed;
      }
      events.push({
        type: 'delta',
        reset: reset === 1,
        settled: settled === 1,
        tier,
        delta,
        ...(terminalOperations === undefined ? {} : { terminalOperations }),
      });
    } else if (tag === 1) {
      const length = readVarint();
      const reason = decodeForegroundUtf8(bytes, offset, length, 'rejection');
      offset += length;
      events.push({ type: 'rejected', reason });
    } else if (tag === 2) events.push({ type: 'closed' });
    else
      throw new Error(
        'Jazz native foreground returned unknown subscription event'
      );
  }
  if (offset !== bytes.length)
    throw new Error(
      'Jazz native foreground returned trailing subscription bytes'
    );
  return events;
}

function decodeForegroundUtf8(
  bytes: Uint8Array,
  start: number,
  length: number,
  label: string
): string {
  const slice = bytes.subarray(start, start + length);
  if (slice.byteLength !== length)
    throw new Error(`Jazz native foreground returned truncated ${label}`);
  // React Native's configured TS lib deliberately does not promise
  // `TextDecoder`; `decodeURIComponent` is available in Hermes and gives us a
  // strict UTF-8 decode without adding a platform polyfill to this tiny ABI.
  let escaped = '';
  for (const byte of slice) escaped += `%${byte.toString(16).padStart(2, '0')}`;
  try {
    return decodeURIComponent(escaped);
  } catch {
    throw new Error(`Jazz native foreground returned malformed UTF-8 ${label}`);
  }
}

function decodePostcardU16(bytes: Uint8Array): number | null {
  let value = 0;
  for (let index = 0; index < bytes.length && index < 3; index += 1) {
    const byte = bytes[index]!;
    value |= (byte & 0x7f) << (index * 7);
    if ((byte & 0x80) === 0) {
      return value <= 0xffff && index + 1 === bytes.length ? value : null;
    }
  }
  return null;
}

/**
 * Execute one opaque base64-encoded native-relay command after checking the
 * embedded ABI.
 *
 * The command codec is intentionally not defined by this package yet: it will
 * be generated from the shared relay command contract once the native module
 * is implemented. This adapter establishes the only permitted JS/native shape
 * in advance—one version probe plus encoded-binary commands—not a row-object
 * API.
 */
export async function executeNativeRelayCommand(
  commandBase64: string
): Promise<string> {
  const relay = requireCompatibleRelay();
  return relay.execute(commandBase64);
}
