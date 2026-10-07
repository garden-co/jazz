import { expect, it } from "vitest";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { createNativeCrypto } from "./native.js";
import { deviceRequestApp as app, deviceRequestPermissions } from "./device-requests.js";
import { encodeCryptoContext } from "./context.js";
import { accountEpochContext } from "./first-epoch.js";
import { publicDeviceApprovalBytes } from "./public-device-approval.js";
import {
  encodeEpochIds,
  encodeEpochDeliveries,
  encodePublicApprovalRevision,
  publicSuccessorSigningBytes,
  successorContext,
  successorSigningBytes,
} from "./account-successor.js";

function append(context: Uint8Array, value: Uint8Array): Uint8Array {
  const length = Buffer.alloc(4);
  length.writeUInt32BE(value.length);
  return Buffer.concat([context, length, value]);
}

it.each(["before", "same transaction"])(
  "rejects a valid grant published %s as its epoch creation",
  async (when) => {
    const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
    const clients: Awaited<ReturnType<typeof createDb>>[] = [];
    const secrets: Uint8Array[] = [];
    try {
      await deploy({
        serverUrl: server.url,
        appId: server.appId,
        adminSecret: server.adminSecret,
        schema: app,
        permissions: deviceRequestPermissions,
      });
      const account = await localAccountConfig(server.appId, server.url);
      const crypto = await createNativeCrypto();
      const open = async () => {
        let saved: string | null = null;
        const db = await createDb({
          ...account,
          e2ee: {
            crypto,
            store: {
              async read() {
                return saved;
              },
              async update(transform) {
                saved = transform(saved);
              },
            },
          },
        });
        clients.push(db);
        return {
          db,
          device() {
            const stored = JSON.parse(saved!).devices[0] as {
              scope: string;
              id: string;
              publicKey: number[];
              privateKey: number[];
              signingPrivateKey: number[];
            };
            const privateKey = Uint8Array.from(stored.privateKey);
            const signingKey = Uint8Array.from(stored.signingPrivateKey);
            secrets.push(privateKey, signingKey);
            return {
              ...stored,
              publicKey: Uint8Array.from(stored.publicKey),
              privateKey,
              signingKey,
            };
          },
        };
      };
      const first = await open();
      const [creator] = await first.db.e2ee.devices.list();
      const second = await open();
      const secondId = (await second.db.e2ee.devices.list()).find((d) => d.id !== creator!.id)!.id;
      await first.db.e2ee.devices.approve(secondId).wait();
      const third = await open();
      await third.db.e2ee.devices.list();
      const signer = first.device();
      const recipient = third.device();
      const identity = (await first.db.one(
        app.__e2ee_account_identities.where({ id: account.account.id }),
        { tier: "global" },
      ))!;
      const oldKey = await crypto.keyEnvelope.open(
        signer,
        accountEpochContext(signer.scope, account.account.id, identity.epochId, signer.id),
        identity.envelope,
      );
      const key = globalThis.crypto.getRandomValues(new Uint8Array(32));
      const challengeKey = globalThis.crypto.getRandomValues(new Uint8Array(32));
      secrets.push(oldKey, key, challengeKey);
      const epochId = globalThis.crypto.randomUUID();
      const challengeId = globalThis.crypto.randomUUID();
      const context = (column: string) =>
        encodeCryptoContext({
          application: signer.scope,
          policy: "jazz.e2ee.device-approval.v1",
          scope: "account",
          identifier: account.account.id,
          table: "__e2ee_device_challenges",
          row: challengeId,
          epoch: epochId,
          recipient: recipient.id,
          column,
        });
      const envelope = await crypto.keyEnvelope.seal(
        recipient.publicKey,
        context("challenge"),
        challengeKey,
      );
      await first.db
        .insert(
          app.__e2ee_device_challenges,
          { deviceId: recipient.id, epochId, envelope },
          { id: challengeId },
        )
        .wait({ tier: "global" });
      const proof = await crypto.keyEnvelope.wrap(
        challengeKey,
        context("proof"),
        new Uint8Array(32),
      );
      await first.db
        .insert(
          app.__e2ee_device_proofs,
          {
            challengeId,
            proof,
            signature: await crypto.deviceSigner.sign(
              recipient.signingKey,
              append(context("proof-signature"), proof),
            ),
          },
          { id: challengeId },
        )
        .wait({ tier: "global" });
      const verification = await crypto.keyEnvelope.wrap(
        key,
        context("approval"),
        new Uint8Array(32),
      );
      const grant = {
        challengeId,
        signerId: signer.id,
        verification,
        signature: await crypto.deviceSigner.sign(
          signer.signingKey,
          append(context(`approval-signature:${signer.id}`), verification),
        ),
      };
      const delivery = await crypto.keyEnvelope.seal(recipient.publicKey, context("delivery"), key);
      await first.db
        .insert(
          app.__e2ee_device_deliveries,
          {
            challengeId,
            envelope: delivery,
            verification: await crypto.keyEnvelope.wrap(
              key,
              append(context("delivery-verification"), delivery),
              new Uint8Array(32),
            ),
          },
          { id: challengeId },
        )
        .wait({ tier: "global" });
      const revision = (await first.db.all(app.__e2ee_device_approvals, { tier: "global" })).map(
        (row) => row.id,
      );
      const coordinates = {
        id: globalThis.crypto.randomUUID(),
        accountId: account.account.id,
        predecessor: identity.epochId,
        epochId,
      };
      const proposal = {
        ...coordinates,
        signerId: signer.id,
        removedDeviceId: secondId,
        membership: encodeEpochIds([signer.id]),
        revision: encodeEpochIds(revision),
        verification: await crypto.keyEnvelope.wrap(
          key,
          successorContext(signer.scope, coordinates, "verification"),
          new Uint8Array(32),
        ),
        history: await crypto.keyEnvelope.wrap(
          key,
          successorContext(signer.scope, coordinates, "history"),
          oldKey,
        ),
        deliveries: encodeEpochDeliveries(
          new Map([
            [
              signer.id,
              await crypto.keyEnvelope.seal(
                signer.publicKey,
                successorContext(signer.scope, coordinates, "delivery", signer.id),
                key,
              ),
            ],
          ]),
        ),
      };
      const signature = await crypto.deviceSigner.sign(
        signer.signingKey,
        successorSigningBytes(signer.scope, proposal),
      );
      const publicGrant = {
        id: challengeId,
        accountId: account.account.id,
        epochId,
        deviceId: recipient.id,
        signerId: signer.id,
      };
      const publicGrantSignature = await crypto.deviceSigner.sign(
        signer.signingKey,
        publicDeviceApprovalBytes(signer.scope, publicGrant),
      );
      const { id: publicGrantId, ...publicGrantColumns } = publicGrant;
      const publicSuccessor = {
        ...coordinates,
        signerId: signer.id,
        removedDeviceId: secondId,
        membership: proposal.membership,
        revision: encodePublicApprovalRevision(
          (await first.db.all(app.__e2ee_public_device_approvals, { tier: "global" }))
            .filter((row) => row.epochId === identity.epochId)
            .map((row) => row.id),
        ),
      };
      const publicSignature = await crypto.deviceSigner.sign(
        signer.signingKey,
        publicSuccessorSigningBytes(signer.scope, publicSuccessor),
      );
      if (when === "before") {
        const early = first.db.beginTransaction();
        early.insert(app.__e2ee_device_approvals, grant, { id: challengeId });
        early.insert(
          app.__e2ee_public_device_approvals,
          { ...publicGrantColumns, signature: publicGrantSignature },
          { id: publicGrantId },
        );
        await early.commit().wait({ tier: "global" });
      }
      const tx = first.db.beginTransaction();
      if (when === "same transaction") {
        tx.insert(app.__e2ee_device_approvals, grant, { id: challengeId });
        tx.insert(
          app.__e2ee_public_device_approvals,
          { ...publicGrantColumns, signature: publicGrantSignature },
          { id: publicGrantId },
        );
      }
      const { id, ...columns } = proposal;
      tx.insert(app.__e2ee_account_successors, { ...columns, signature }, { id });
      const { id: publicId, ...publicColumns } = publicSuccessor;
      tx.insert(
        app.__e2ee_public_account_successors,
        { ...publicColumns, signature: publicSignature },
        { id: publicId },
      );
      await tx.commit().wait({ tier: "global" });
      const devices = await first.db.e2ee.devices.list();
      expect(devices).toContainEqual(expect.objectContaining({ id: secondId, state: "revoked" }));
      expect(devices).toContainEqual(
        expect.objectContaining({ id: recipient.id, state: "pending" }),
      );
    } finally {
      for (const secret of secrets) secret.fill(0);
      await Promise.all(clients.map((db) => db.shutdown()));
      await server.stop();
    }
  },
  30_000,
);
