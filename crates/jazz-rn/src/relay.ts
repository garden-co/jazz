import nativeRelay from './NativeJazzRelay';

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

export const NATIVE_RELAY_ABI: NativeRelayAbiRange = {
  minimum: 6,
  maximum: 6,
};

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
  /** Register the runtime-local wake receiver used by the native adapter.
   * Native work is coalesced before this callback runs; it must schedule an
   * ordinary JS-side tick rather than calling back into native synchronously. */
  setTickScheduler(
    callback: (urgency: "immediate" | "deferred" | `after:${number}`) => void,
  ): void;
  tick(): void;
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
  | "probe"
  | "tick"
  | { type: "prepareQuery"; query: Uint8Array }
  | { type: "all"; query: number }
  | { type: "subscribe"; query: number }
  | { type: "drainSubscription"; subscription: number }
  | { type: "unsubscribe"; subscription: number }
  | "close";

export type NativeForegroundResponse =
  | { type: "probe"; abiVersion: number }
  | { type: "ticked" }
  | { type: "preparedQuery"; query: number }
  | { type: "rows"; rows: Uint8Array }
  | { type: "subscribed"; subscription: number }
  | { type: "subscriptionEvents"; events: NativeForegroundSubscriptionEvent[] }
  | { type: "unsubscribed"; closed: boolean }
  | { type: "closed"; closed: boolean };

export type NativeForegroundSubscriptionEvent =
  | { type: "delta"; reset: boolean; settled: boolean; tier: string; delta: Uint8Array }
  | { type: "rejected"; reason: string }
  | { type: "closed" };

type NativeRelayWithForegroundInstaller = {
  installForegroundRuntime?: () => void;
};

function foregroundRuntimeInstallationError(): Error {
  return new Error(
    'Jazz native foreground runtime installation failed: the native build did not install a compatible JSI foreground engine. Install a matching native development or release build.'
  );
}

function requireCompatibleRelay() {
  const relay = requireNativeRelay();
  const nativeAbi = relay.getAbiVersion();
  if (nativeAbi === 0) {
    throw new Error(
      'Jazz native relay is unavailable: this native build contains only the source fallback (ABI 0), not the Jazz relay artifact. Install a matching native development or release build.'
    );
  }
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
  const relay = requireCompatibleRelay() as typeof nativeRelay &
    NativeRelayWithForegroundInstaller;
  if (typeof relay.installForegroundRuntime !== 'function') {
    throw new Error(
      'Jazz native foreground runtime is unavailable: install a matching native development or release build containing the JSI foreground engine. Expo Go never includes it.'
    );
  }

  // A bridge reload can leave a same-ABI global behind. Remove it before every
  // install so a native no-op cannot accidentally hand the new adapter a JSI
  // HostObject owned by a previous runtime. Native installation must replace
  // this configurable own property synchronously in the current JS runtime.
  if (!Reflect.deleteProperty(globalThis, NATIVE_FOREGROUND_RUNTIME_GLOBAL)) {
    throw foregroundRuntimeInstallationError();
  }
  relay.installForegroundRuntime();
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
      const foreground = installed.openAttached(capability) as Partial<NativeForegroundRuntime>;
      if (
        !foreground ||
        typeof foreground.execute !== "function" ||
        typeof foreground.setTickScheduler !== "function" ||
        typeof foreground.tick !== "function" ||
        typeof foreground.close !== "function"
      ) {
        throw foregroundRuntimeInstallationError();
      }
      return {
        execute(command: Uint8Array): Uint8Array {
          if (!(command instanceof Uint8Array)) {
            throw new Error("Jazz native foreground command requires a Uint8Array");
          }
          const response = foreground.execute!(command);
          if (!(response instanceof Uint8Array)) {
            throw foregroundRuntimeInstallationError();
          }
          return response;
        },
        setTickScheduler(
          callback: (urgency: "immediate" | "deferred" | `after:${number}`) => void,
        ): void {
          if (typeof callback !== "function") {
            throw new Error("Jazz native foreground tick scheduler requires a function");
          }
          foreground.setTickScheduler!(callback);
        },
        tick(): void {
          foreground.tick!();
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
  if (command.type === 'prepareQuery') {
    return concatForegroundBytes(
      Uint8Array.of(2),
      encodeForegroundBytes(command.query)
    );
  }
  if (command.type === 'all')
    return concatForegroundBytes(
      Uint8Array.of(3),
      encodeForegroundU64(command.query)
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
  return concatForegroundBytes(
    Uint8Array.of(6),
    encodeForegroundU64(command.subscription)
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
    throw new Error('Jazz native foreground query requires Uint8Array bytes');
  return concatForegroundBytes(encodeForegroundU64(value.byteLength), value);
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
    if (tag === 0) {
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
      events.push({
        type: 'delta',
        reset: reset === 1,
        settled: settled === 1,
        tier,
        delta,
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
