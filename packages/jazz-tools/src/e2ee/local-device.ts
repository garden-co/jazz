import type { AccountStore } from "../accounts/persistence.js";
import { runtimeRandomBytes } from "../runtime/runtime-entropy.js";
import { encodeEnvelope } from "./envelope.js";
import type { DeviceKeyPair, DeviceSigner, KeyEnvelope } from "./types.js";

type StoredDevice = {
  scope: string;
  id: string;
  mechanism: string;
  version: number;
  publicKey: number[];
  privateKey: number[];
  challenge: number[];
  signingMechanism: string;
  signingVersion: number;
  signingPublicKey: number[];
  signingPrivateKey: number[];
};
export type StoredDevices = { format: "jazz-e2ee-local-devices-v2"; devices: StoredDevice[] };
export type LocalDevice = DeviceKeyPair & {
  id: string;
  challenge: Uint8Array;
  signing: DeviceKeyPair;
};

type SecretBuffer = Uint8Array | number[];

/** Session ownership includes in-flight operation copies, not opaque adapter internals. */
export class DeviceKeyLifetime {
  private readonly buffers = new Set<SecretBuffer>();
  private closed = false;

  own<T extends SecretBuffer>(buffer: T): T {
    if (this.closed) {
      buffer.fill(0);
      throw new Error("Cannot operate on a closed E2EE context");
    }
    this.buffers.add(buffer);
    return buffer;
  }

  release(buffer: SecretBuffer): void {
    buffer.fill(0);
    this.buffers.delete(buffer);
  }

  close(): void {
    this.closed = true;
    for (const buffer of this.buffers) buffer.fill(0);
    this.buffers.clear();
  }
}

export interface LocalDeviceProvider {
  readonly id: string;
  load(): Promise<LocalDevice>;
  release(device: LocalDevice): void;
}

function bytes(value: unknown): value is number[] {
  return (
    Array.isArray(value) &&
    value.length > 0 &&
    value.length <= 65536 &&
    value.every((byte) => Number.isInteger(byte) && byte >= 0 && byte <= 255)
  );
}

function validate(device: StoredDevice): void {
  if (
    !device ||
    typeof device.scope !== "string" ||
    typeof device.id !== "string" ||
    !/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(device.id) ||
    !bytes(device.publicKey) ||
    !bytes(device.privateKey) ||
    !bytes(device.challenge) ||
    !bytes(device.signingPublicKey) ||
    !bytes(device.signingPrivateKey) ||
    device.challenge.length !== 32
  )
    throw new Error("Invalid persisted E2EE device");
  encodeEnvelope({ id: device.mechanism, version: device.version }, new Uint8Array());
  encodeEnvelope({ id: device.signingMechanism, version: device.signingVersion }, new Uint8Array());
}

/** Decoded arrays never escape synchronous use, including malformed/other-scope records. */
export function withLocalDeviceStore<T>(
  value: string | null,
  lifetime: DeviceKeyLifetime,
  use: (stored: StoredDevices) => T,
): T {
  const arrays: number[][] = [];
  try {
    const parsed: StoredDevices =
      value === null
        ? { format: "jazz-e2ee-local-devices-v2", devices: [] }
        : (JSON.parse(value, (key, entry: unknown) => {
            if ((key === "privateKey" || key === "signingPrivateKey") && Array.isArray(entry)) {
              arrays.push(entry);
              lifetime.own(entry);
            }
            return entry;
          }) as StoredDevices);
    const scopes = new Set<string>();
    if (parsed?.format !== "jazz-e2ee-local-devices-v2" || !Array.isArray(parsed.devices))
      throw new Error("Invalid persisted E2EE devices");
    for (const device of parsed.devices) {
      validate(device);
      if (scopes.has(device.scope)) throw new Error("Invalid persisted E2EE device");
      scopes.add(device.scope);
    }
    return use(parsed);
  } finally {
    for (const array of arrays) lifetime.release(array);
  }
}

function load(
  device: StoredDevice,
  envelope: KeyEnvelope,
  signer: DeviceSigner,
  own: (buffer: Uint8Array) => Uint8Array,
): LocalDevice {
  if (device.mechanism !== envelope.mechanism.id || device.version !== envelope.mechanism.version)
    throw new Error("Persisted E2EE device requires its original key-envelope mechanism");
  if (
    device.signingMechanism !== signer.mechanism.id ||
    device.signingVersion !== signer.mechanism.version
  )
    throw new Error("Persisted E2EE device requires its original signing mechanism");
  return {
    id: device.id,
    publicKey: Uint8Array.from(device.publicKey),
    privateKey: own(Uint8Array.from(device.privateKey)),
    challenge: Uint8Array.from(device.challenge),
    signing: {
      publicKey: Uint8Array.from(device.signingPublicKey),
      privateKey: own(Uint8Array.from(device.signingPrivateKey)),
    },
  };
}

async function checked(
  device: LocalDevice,
  scope: string,
  envelope: KeyEnvelope,
  signer: DeviceSigner,
  assertOpen: () => void,
): Promise<LocalDevice> {
  assertOpen();
  const context = new TextEncoder().encode(`jazz.e2ee.local-device-check.v1\0${scope}`);
  const sealed = await envelope.seal(device.publicKey, context, device.challenge);
  assertOpen();
  const opened = await envelope.open(device, context, sealed);
  try {
    assertOpen();
    if (
      opened.length !== device.challenge.length ||
      !opened.every((byte, index) => byte === device.challenge[index])
    )
      throw new Error("Invalid E2EE device keypair");
  } finally {
    opened.fill(0);
  }
  const proof = await signer.sign(device.signing.privateKey, context);
  assertOpen();
  const valid = await signer.verify(device.signing.publicKey, context, proof);
  assertOpen();
  if (!valid) throw new Error("Invalid E2EE signing keypair");
  return device;
}

/** Validate before persisting, persist before publishing. Host updates choose one winner. */
export async function localDevice(
  store: AccountStore,
  scope: string,
  envelope: KeyEnvelope,
  signer: DeviceSigner,
  lifetime: DeviceKeyLifetime,
  assertOpen: () => void,
): Promise<LocalDevice> {
  // Failed preparation releases only this attempt; the session remains retryable.
  const attempt = new Set<SecretBuffer>();
  const own = <T extends SecretBuffer>(buffer: T): T => {
    lifetime.own(buffer);
    attempt.add(buffer);
    return buffer;
  };
  const release = (buffer: SecretBuffer) => {
    lifetime.release(buffer);
    attempt.delete(buffer);
  };
  try {
    assertOpen();
    const stored = await store.read();
    assertOpen();
    const existing = withLocalDeviceStore(stored, lifetime, ({ devices }) => {
      const record = devices.find((device) => device.scope === scope);
      return record ? load(record, envelope, signer, own) : undefined;
    });
    let retained: LocalDevice;
    if (existing) {
      retained = await checked(existing, scope, envelope, signer, assertOpen);
    } else {
      const pair = await envelope.createKeyPair();
      own(pair.privateKey);
      assertOpen();
      const signing = await signer.createKeyPair();
      own(signing.privateKey);
      assertOpen();
      const device = await checked(
        { ...pair, signing, id: crypto.randomUUID(), challenge: runtimeRandomBytes(32) },
        scope,
        envelope,
        signer,
        assertOpen,
      );
      const candidate: StoredDevice = {
        scope,
        id: device.id,
        mechanism: envelope.mechanism.id,
        version: envelope.mechanism.version,
        publicKey: Array.from(device.publicKey),
        privateKey: own(Array.from(device.privateKey)),
        challenge: Array.from(device.challenge),
        signingMechanism: signer.mechanism.id,
        signingVersion: signer.mechanism.version,
        signingPublicKey: Array.from(device.signing.publicKey),
        signingPrivateKey: own(Array.from(device.signing.privateKey)),
      };
      validate(candidate);
      let selected: LocalDevice | undefined;
      await store.update((current) => {
        assertOpen();
        if (selected && selected !== device) {
          release(selected.privateKey);
          release(selected.signing.privateKey);
        }
        selected = undefined;
        return withLocalDeviceStore(current, lifetime, (latest) => {
          const winner = latest.devices.find((record) => record.scope === scope);
          if (winner) selected = load(winner, envelope, signer, own);
          else {
            selected = device;
            latest.devices.push(candidate);
          }
          return JSON.stringify(latest);
        });
      });
      assertOpen();
      if (!selected) throw new Error("E2EE device store did not perform its atomic update");
      retained =
        selected === device ? device : await checked(selected, scope, envelope, signer, assertOpen);
    }
    assertOpen();
    // Transfer exact buffers to preparation; no extra private-key copies are needed.
    attempt.delete(retained.privateKey);
    attempt.delete(retained.signing.privateKey);
    return retained;
  } finally {
    for (const buffer of attempt) lifetime.release(buffer);
    attempt.clear();
  }
}
