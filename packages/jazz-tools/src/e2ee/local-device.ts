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
type StoredDevices = { format: "jazz-e2ee-local-devices-v2"; devices: StoredDevice[] };
export type LocalDevice = DeviceKeyPair & {
  id: string;
  challenge: Uint8Array;
  signing: DeviceKeyPair;
};

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

export function decodeLocalDeviceStore(value: string | null): StoredDevices {
  if (value === null) return { format: "jazz-e2ee-local-devices-v2", devices: [] };
  const parsed = JSON.parse(value) as StoredDevices;
  const scopes = new Set<string>();
  if (parsed?.format !== "jazz-e2ee-local-devices-v2" || !Array.isArray(parsed.devices))
    throw new Error("Invalid persisted E2EE devices");
  for (const device of parsed.devices) {
    validate(device);
    if (scopes.has(device.scope)) throw new Error("Invalid persisted E2EE device");
    scopes.add(device.scope);
  }
  return parsed;
}

function load(device: StoredDevice, envelope: KeyEnvelope, signer: DeviceSigner): LocalDevice {
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
    privateKey: Uint8Array.from(device.privateKey),
    challenge: Uint8Array.from(device.challenge),
    signing: {
      publicKey: Uint8Array.from(device.signingPublicKey),
      privateKey: Uint8Array.from(device.signingPrivateKey),
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
  try {
    const context = new TextEncoder().encode(`jazz.e2ee.local-device-check.v1\0${scope}`);
    const sealed = await envelope.seal(device.publicKey, context, device.challenge);
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
    if (!(await signer.verify(device.signing.publicKey, context, proof)))
      throw new Error("Invalid E2EE signing keypair");
    assertOpen();
    return device;
  } catch (error) {
    device.privateKey.fill(0);
    device.signing.privateKey.fill(0);
    throw error;
  }
}

/** Retained-only probing never generates or publishes a new device. */
export async function retainedLocalDevice(
  store: AccountStore,
  scope: string,
  envelope: KeyEnvelope,
  signer: DeviceSigner,
  assertOpen: () => void,
): Promise<LocalDevice | undefined> {
  assertOpen();
  const existing = decodeLocalDeviceStore(await store.read()).devices.find(
    (device) => device.scope === scope,
  );
  assertOpen();
  return existing
    ? checked(load(existing, envelope, signer), scope, envelope, signer, assertOpen)
    : undefined;
}

/** Validate before persisting, persist before publishing. Host updates choose one winner. */
export async function localDevice(
  store: AccountStore,
  scope: string,
  envelope: KeyEnvelope,
  signer: DeviceSigner,
  assertOpen: () => void,
): Promise<LocalDevice> {
  const existing = await retainedLocalDevice(store, scope, envelope, signer, assertOpen);
  if (existing) return existing;
  const pair = await envelope.createKeyPair();
  let signing: DeviceKeyPair | undefined;
  let candidate: StoredDevice | undefined;
  try {
    signing = await signer.createKeyPair();
    const device = await checked(
      { ...pair, signing, id: crypto.randomUUID(), challenge: runtimeRandomBytes(32) },
      scope,
      envelope,
      signer,
      assertOpen,
    );
    candidate = {
      scope,
      id: device.id,
      mechanism: envelope.mechanism.id,
      version: envelope.mechanism.version,
      publicKey: Array.from(device.publicKey),
      privateKey: Array.from(device.privateKey),
      challenge: Array.from(device.challenge),
      signingMechanism: signer.mechanism.id,
      signingVersion: signer.mechanism.version,
      signingPublicKey: Array.from(device.signing.publicKey),
      signingPrivateKey: Array.from(device.signing.privateKey),
    };
    const validated = candidate;
    validate(validated);
    let selected: StoredDevice | undefined;
    await store.update((current) => {
      assertOpen();
      const latest = decodeLocalDeviceStore(current);
      selected = latest.devices.find((device) => device.scope === scope);
      if (!selected) {
        selected = validated;
        latest.devices.push(validated);
      }
      return JSON.stringify(latest);
    });
    assertOpen();
    if (!selected) throw new Error("E2EE device store did not perform its atomic update");
    const retained = load(selected, envelope, signer);
    return selected === validated
      ? retained
      : checked(retained, scope, envelope, signer, assertOpen);
  } finally {
    pair.privateKey.fill(0);
    candidate?.privateKey.fill(0);
    signing?.privateKey.fill(0);
    candidate?.signingPrivateKey.fill(0);
  }
}
