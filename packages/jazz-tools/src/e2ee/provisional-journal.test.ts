import { expect, it } from "vitest";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { accountRegistry } from "../accounts/enrollment.js";
import type { AccountStore } from "../accounts/persistence.js";
import { createBrowserCrypto } from "./browser.js";
import { localDevice } from "./local-device.js";
import { accountEpochContext, confirmAccountEpoch } from "./first-epoch.js";
import { InitializationJournal, type FounderProposal } from "./provisional-initialization.js";

async function unexpectedSpace(): Promise<void> {
  throw new Error("Unexpected space initialization in founder fixture");
}

it("atomically reuses one device-bound sealed founder epoch across competing local claims and reopen", async () => {
  const config = await localAccountConfig("e14-atomic-founder-journal");
  const db = await createDb(config);
  let value: string | null = null;
  const store: AccountStore = {
    async read() {
      return value;
    },
    async update(transform) {
      value = transform(value);
    },
  };
  const scope = JSON.stringify([accountRegistry(config.account), "main", config.account.id]);
  const crypto = await createBrowserCrypto();
  const [firstDevice, secondDevice] = await Promise.all([
    localDevice(store, scope, crypto.keyEnvelope, crypto.deviceSigner, () => {}),
    localDevice(store, scope, crypto.keyEnvelope, crypto.deviceSigner, () => {}),
  ]);
  try {
    expect(secondDevice.id).toBe(firstDevice.id);
    const proposal = async (epochId: string, fill: number): Promise<FounderProposal> => {
      const secret = new Uint8Array(32).fill(fill);
      try {
        return {
          kind: "founder",
          id: config.account.id,
          deviceId: firstDevice.id,
          epochId,
          publicKeyId: globalThis.crypto.randomUUID(),
          rootId: globalThis.crypto.randomUUID(),
          envelope: await crypto.keyEnvelope.seal(
            firstDevice.publicKey,
            accountEpochContext(scope, config.account.id, epochId, firstDevice.id),
            secret,
          ),
          verification: await crypto.keyEnvelope.wrap(
            secret,
            accountEpochContext(scope, config.account.id, epochId, "", "verification"),
            new Uint8Array(32),
          ),
        };
      } finally {
        secret.fill(0);
      }
    };
    const first = await proposal("00000000-0000-4000-8000-000000000001", 41);
    const second = await proposal("00000000-0000-4000-8000-000000000002", 73);
    const left = new InitializationJournal(
      db,
      store,
      scope,
      () => {},
      async () => {},
      unexpectedSpace,
    );
    const right = new InitializationJournal(
      db,
      store,
      scope,
      () => {},
      async () => {},
      unexpectedSpace,
    );
    const claims = await Promise.all([left.claimFounder(first), right.claimFounder(second)]);
    expect(claims).toEqual([first, first]);
    const reopened = new InitializationJournal(
      db,
      store,
      scope,
      () => {},
      async () => {},
      unexpectedSpace,
    );
    expect(await reopened.claimFounder(second)).toEqual(first);
    const secret = await crypto.keyEnvelope.open(
      secondDevice,
      accountEpochContext(scope, config.account.id, first.epochId, secondDevice.id),
      claims[1]!.envelope,
    );
    try {
      expect(secret).toEqual(new Uint8Array(32).fill(41));
      await confirmAccountEpoch(crypto.keyEnvelope, scope, config.account.id, first, secret);
    } finally {
      secret.fill(0);
    }
    const otherDevice = { ...second, deviceId: globalThis.crypto.randomUUID() };
    await expect(reopened.claimFounder(otherDevice)).rejects.toThrow();
    expect(await reopened.claimFounder(first)).toEqual(first);
    const original = value!;
    const corrupt = JSON.parse(original);
    const entry = corrupt.initializationJournalV1[0];
    entry.proposal = JSON.stringify({
      ...JSON.parse(entry.proposal),
      id: globalThis.crypto.randomUUID(),
    });
    value = JSON.stringify(corrupt);
    const unchanged = value;
    await expect(reopened.claimFounder(first)).rejects.toThrow();
    expect(value).toBe(unchanged);
    value = original;
    expect(await reopened.claimFounder(first)).toEqual(first);
  } finally {
    for (const device of [firstDevice, secondDevice]) {
      device.privateKey.fill(0);
      device.signing.privateKey.fill(0);
    }
    await db.shutdown();
  }
});

it("reopens the v1 sealed journal corpus and refuses corruption rather than replacing its founder", async () => {
  const db = await createDb(await localAccountConfig("e14-journal-corpus"));
  const encoded =
    '{"kind":"founder","id":"account","deviceId":"device","epochId":"epoch","publicKeyId":"keys","rootId":"root","envelope":{"e2eeBytesV1":[0,128,255]},"verification":{"e2eeBytesV1":[255,1,0]}}';
  const entry = { scope: "scope", proposal: encoded, local: false, outcome: "pending" };
  const initial = {
    format: "jazz-e2ee-local-devices-v2",
    devices: [],
    initializationJournalV1: [entry],
  };
  let value = JSON.stringify(initial);
  const store: AccountStore = {
    async read() {
      return value;
    },
    async update(transform) {
      value = transform(value)!;
    },
  };
  const journal = new InitializationJournal(
    db,
    store,
    "scope",
    () => {},
    async () => {},
    unexpectedSpace,
  );
  try {
    const [retained] = await journal.entries();
    expect(retained?.proposal).toEqual({
      kind: "founder",
      id: "account",
      deviceId: "device",
      epochId: "epoch",
      publicKeyId: "keys",
      rootId: "root",
      envelope: new Uint8Array([0, 128, 255]),
      verification: new Uint8Array([255, 1, 0]),
    });
    const replacement = retained!.proposal as FounderProposal;
    for (const invalid of [
      { ...initial, format: "jazz-e2ee-local-devices-v3" },
      {
        ...initial,
        initializationJournalV1: [
          { ...entry, proposal: encoded.replace("[0,128,255]", "[0,256]") },
        ],
      },
      { ...initial, initializationJournalV1: [{ ...entry, local: true }] },
      { ...initial, initializationJournalV1: [{ ...entry, promoted: true }] },
      { ...initial, initializationJournalV1: [entry, entry] },
    ]) {
      value = JSON.stringify(invalid);
      const unchanged = value;
      await expect(journal.claimFounder(replacement)).rejects.toThrow();
      expect(value).toBe(unchanged);
    }
  } finally {
    await db.shutdown();
  }
});
