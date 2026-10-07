import { createHash } from "node:crypto";
import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createJazzSession, type JazzClient } from "../backend/create-jazz-session.js";
import type { JazzSession } from "../session/state.js";
import type { AccountStore } from "../accounts/persistence.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";
import { spaceSchema } from "./spaces.js";
import { createNativeCrypto } from "./native.js";
import { spaceContext, spaceGrantBytes, spaceRootBytes } from "./space-format.js";

function store(): AccountStore {
  let value: string | null = null;
  return {
    async read() {
      return value;
    },
    async update(transform: (current: string | null) => string) {
      value = transform(value);
    },
  };
}

type StoredDevice = {
  id: string;
  scope: string;
  publicKey: number[];
  signingPublicKey: number[];
  signingPrivateKey: number[];
};

// Author a protocol fixture without calling the private root-selection implementation.
function canonicalId(scopeId: string, identifier: string): string {
  const bytes = createHash("sha256")
    .update(JSON.stringify(["jazz.e2ee.space-id.v1", scopeId, identifier]))
    .digest()
    .subarray(0, 16);
  bytes[6] = (bytes[6]! & 0x0f) | 0x80;
  bytes[8] = (bytes[8]! & 0x3f) | 0x80;
  const hex = bytes.toString("hex");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

it.each([
  "wrong-ID same-address",
  "wrong-ID before canonical",
  "covered-empty strict-before creator",
  "inactive creator",
  "wrong-epoch creator",
  "operational verifier fault",
] as const)(
  "isolates native recovery root eligibility (%s)",
  async (scenario) => {
    const app = s.defineApp({
      ...deviceRequestSchema,
      ...spaceSchema,
      projects: s.table({ title: s.string() }, {}),
    });
    const policies = definePermissions(app, ({ policy, session }) => {
      const authenticated = session.where({ authMode: { in: ["local-first", "external"] } });
      policy.projects.allowRead.where(authenticated);
      policy.projects.allowInsert.where(authenticated);
      policy.__e2ee_spaces.allowRead.where(authenticated);
      policy.__e2ee_spaces.allowInsert.where({ accountId: session.user.account });
      policy.__e2ee_space_grants.allowRead.where(authenticated);
      policy.__e2ee_space_grants.allowInsert.where({ authorAccountId: session.user.account });
      policy.__e2ee_space_deliveries.allowRead.where(authenticated);
      policy.__e2ee_space_deliveries.allowInsert.where({ senderAccountId: session.user.account });
      policy.__e2ee_space_successors.allowRead.where(authenticated);
      policy.__e2ee_space_successors.allowInsert.where({ authorAccountId: session.user.account });
      policy.__e2ee_space_recovery_deliveries.allowRead.where(authenticated);
      policy.__e2ee_space_recovery_deliveries.allowInsert.where({
        senderAccountId: session.user.account,
      });
    });
    const permissions = { ...deviceRequestPermissions, ...policies };
    const native = await createNativeCrypto();
    const adapters = { ...native, deviceSigner: { ...native.deviceSigner } };
    const sessions: JazzSession<JazzClient>[] = [];
    const secrets: Uint8Array[] = [];
    const accountStore = store();
    const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
    try {
      await deploy({
        serverUrl: server.url,
        appId: server.appId,
        adminSecret: server.adminSecret,
        schema: app,
        permissions,
      });
      const open = async (deviceStore: AccountStore, account = accountStore) => {
        const session = await createJazzSession({
          appId: server.appId,
          serverUrl: server.url,
          app,
          permissions,
          driver: { type: "memory" },
          initial: "local-first",
          store: account,
          e2ee: { app, crypto: adapters, store: deviceStore },
        });
        sessions.push(session);
        return session;
      };
      const firstStore = store();
      const first = await open(firstStore);
      const db = first.getSnapshot().client!.db;
      const accountId = first.getSnapshot().account!.id;
      await db.e2ee.devices.list();
      const device: StoredDevice = JSON.parse((await firstStore.read())!).devices[0];
      const project = await db
        .insert(app.projects, { title: "Eligible recovery root" })
        .wait({ tier: "global" });
      await db.e2ee.spaces.grant(app.projects, project.id, accountId).wait();
      const target = { scope: app.projects, identifier: project.id };
      const accepted = (await db.one(app.__e2ee_spaces.where({ identifier: project.id }), {
        tier: "global",
      }))!;
      expect(accepted.id).toBe(canonicalId(accepted.scopeId, project.id));
      expect(await db.e2ee.explain(target)).toEqual({ state: "ready" });
      const { material } = await db.e2ee.recovery.create().wait();
      let otherRecipient: string | undefined;

      if (scenario !== "operational verifier fault") {
        let writer = db;
        let authorId = accountId;
        let author = device;
        let accountEpochId = accepted.accountEpochId;
        if (scenario === "covered-empty strict-before creator") {
          // This account has no E2EE history yet. Bootstrap only AFTER the root's
          // accepted position: the full history is present but its strict-before
          // prefix is covered-empty, not a missing-history/coverage fixture.
          const lateCreator = await open(store(), store());
          writer = lateCreator.getSnapshot().client!.db;
          authorId = lateCreator.getSnapshot().account!.id;
          expect(
            await writer.all(app.__e2ee_account_roots.where({ accountId: authorId }), {
              tier: "global",
            }),
          ).toEqual([]);
          const signing = await native.deviceSigner.createKeyPair();
          const envelope = await native.keyEnvelope.createKeyPair();
          secrets.push(signing.privateKey, envelope.privateKey);
          const context = JSON.parse(device.scope) as string[];
          context[2] = authorId;
          author = {
            id: crypto.randomUUID(),
            scope: JSON.stringify(context),
            publicKey: Array.from(envelope.publicKey),
            signingPublicKey: Array.from(signing.publicKey),
            signingPrivateKey: Array.from(signing.privateKey),
          };
          accountEpochId = crypto.randomUUID();
        } else if (scenario === "inactive creator") {
          const pendingStore = store();
          const pendingSession = await open(pendingStore);
          const pendingDb = pendingSession.getSnapshot().client!.db;
          const devices = await pendingDb.e2ee.devices.list();
          author = JSON.parse((await pendingStore.read())!).devices[0] as StoredDevice;
          expect(devices).toContainEqual(
            expect.objectContaining({ id: author.id, state: "pending" }),
          );
          // Genuine published device keys, but never approved in this account.
          expect(
            await db.one(app.__e2ee_device_keys.where({ deviceId: author.id }), {
              tier: "global",
            }),
          ).not.toBeNull();
        } else if (scenario === "wrong-epoch creator") {
          accountEpochId = crypto.randomUUID();
        }
        const identifier =
          scenario === "wrong-ID same-address"
            ? project.id
            : (
                await writer
                  .insert(app.projects, { title: "Ineligible creator root" })
                  .wait({ tier: "global" })
              ).id;
        const coordinates = {
          id:
            scenario === "wrong-ID same-address" || scenario === "wrong-ID before canonical"
              ? crypto.randomUUID()
              : canonicalId(accepted.scopeId, identifier),
          scopeId: accepted.scopeId,
          identifier,
          accountId: authorId,
          deviceId: author.id,
          accountEpochId,
          epochId: crypto.randomUUID(),
          initialGrantId: crypto.randomUUID(),
          mechanism: native.keyEnvelope.mechanism.id,
          version: native.keyEnvelope.mechanism.version,
        };
        const key = crypto.getRandomValues(new Uint8Array(32));
        const signingKey = Uint8Array.from(author.signingPrivateKey);
        secrets.push(key, signingKey);
        const root = {
          ...coordinates,
          verification: await native.keyEnvelope.wrap(
            key,
            spaceContext(author.scope, coordinates, "verification"),
            new Uint8Array(32),
          ),
          authorEnvelope: await native.keyEnvelope.seal(
            Uint8Array.from(author.publicKey),
            spaceContext(author.scope, coordinates, "author", author.id),
            key,
          ),
        };
        const grant = {
          id: root.initialGrantId,
          spaceId: root.id,
          epochId: root.epochId,
          authorAccountId: authorId,
          authorDeviceId: author.id,
          authorEpochId: accountEpochId,
          operation: "add",
          recipientKind: "account",
          // Every bad root is reachable even with recipient-scoped discovery.
          recipientId: accountId,
          recipientEpochId: accepted.accountEpochId,
        };
        const rootBytes = spaceRootBytes(author.scope, root);
        const grantBytes = spaceGrantBytes(author.scope, root, grant);
        const rootSignature = await native.deviceSigner.sign(signingKey, rootBytes);
        const grantSignature = await native.deviceSigner.sign(signingKey, grantBytes);
        for (const [bytes, signature] of [
          [rootBytes, rootSignature],
          [grantBytes, grantSignature],
        ] as const) {
          expect(
            await native.deviceSigner.verify(
              Uint8Array.from(author.signingPublicKey),
              bytes,
              signature,
            ),
          ).toBe(true);
        }
        const tx = writer.beginTransaction();
        const { id, ...values } = root;
        tx.insert(app.__e2ee_spaces, { ...values, signature: rootSignature }, { id });
        const { id: grantId, ...grantValues } = grant;
        tx.insert(
          app.__e2ee_space_grants,
          { ...grantValues, signature: grantSignature },
          {
            id: grantId,
          },
        );
        await tx.commit().wait({ tier: "global" });
        expect(
          await db.all(
            app.__e2ee_space_grants.where({
              spaceId: root.id,
              recipientKind: "account",
              recipientId: accountId,
            }),
            { tier: "global" },
          ),
        ).toHaveLength(1);
        if (scenario === "covered-empty strict-before creator") {
          await writer.e2ee.devices.list();
          expect(
            await db.all(app.__e2ee_account_roots.where({ accountId: authorId }), {
              tier: "global",
            }),
          ).toHaveLength(1);
        }
        if (scenario === "wrong-ID before canonical") {
          const canonical = app.__e2ee_spaces.where({
            id: canonicalId(accepted.scopeId, identifier),
          });
          expect(await db.one(canonical, { tier: "global" })).toBeNull();
          await db.e2ee.spaces.grant(app.projects, identifier, accountId).wait();
          expect(await db.one(canonical, { tier: "global" })).toMatchObject({
            scopeId: accepted.scopeId,
            identifier,
          });
          const created = { scope: app.projects, identifier };
          expect(await db.e2ee.explain(created)).toEqual({ state: "ready" });
          await db.e2ee.spaces.revoke(app.projects, identifier, accountId).wait();
          expect(await db.e2ee.explain(created)).toMatchObject({
            state: "refused",
            reason: "space-sealed",
          });
        }
        if (scenario === "wrong-ID same-address") {
          expect(
            await db.all(app.__e2ee_spaces.where({ identifier: project.id }), { tier: "global" }),
          ).toHaveLength(2);
          const recipient = await open(store(), store());
          await recipient.getSnapshot().client!.db.e2ee.devices.list();
          otherRecipient = recipient.getSnapshot().account!.id;
          // Soft promise assertions keep the shared fixture exercising every
          // point-lookup seam when the pre-fix multi-root read rejects.
          await expect.soft(db.e2ee.explain(target)).resolves.toEqual({ state: "ready" });
          await expect
            .soft(db.e2ee.spaces.grant(app.projects, project.id, otherRecipient).wait())
            .resolves.toBeUndefined();
          await expect
            .soft(db.e2ee.spaces.revoke(app.projects, project.id, otherRecipient).wait())
            .resolves.toBeUndefined();
        }
      }

      const checkedSpaces = {
        validation: "checked",
        paths: [
          expect.objectContaining({
            scopeId: accepted.scopeId,
            identifier: project.id,
            spaceId: accepted.id,
            validation: "validated",
          }),
        ],
      };
      const verifierFailure = new Error("Space root verifier unavailable");
      const armVerifierFault = () => {
        const bytes = spaceRootBytes(device.scope, accepted);
        // Change the adapter identity too: completed validation must not hide an
        // operational failure after the configured verifier changes.
        adapters.deviceSigner.verify = async (publicKey, record, signature) => {
          if (record.length === bytes.length && record.every((byte, i) => byte === bytes[i]))
            throw verifierFailure;
          return native.deviceSigner.verify(publicKey, record, signature);
        };
      };
      if (scenario === "operational verifier fault") {
        armVerifierFault();
        await expect(db.e2ee.recovery.status(material)).rejects.toBe(verifierFailure);
        await expect(db.e2ee.recovery.create().wait()).rejects.toBe(verifierFailure);
        adapters.deviceSigner.verify = native.deviceSigner.verify;
      }
      await expect.soft(db.e2ee.explain(target)).resolves.toEqual({ state: "ready" });
      await expect.soft(db.e2ee.recovery.status(material)).resolves.toMatchObject({
        spaces: checkedSpaces,
      });
      await expect.soft(db.e2ee.recovery.create().wait()).resolves.toBeDefined();
      await first.close();

      // No old device store survives, and no online holder of the valid key remains.
      const replacement = await open(store());
      const recovered = replacement.getSnapshot().client!.db;
      expect(replacement.getSnapshot().account!.id).toBe(accountId);
      const pending = (await recovered.e2ee.devices.list()).find((row) => row.state === "pending");
      expect(pending).toBeDefined();
      if (scenario === "operational verifier fault") {
        armVerifierFault();
        await expect(recovered.e2ee.recovery.use(material).wait()).rejects.toBe(verifierFailure);
        adapters.deviceSigner.verify = native.deviceSigner.verify;
      }
      await expect.soft(recovered.e2ee.recovery.use(material).wait()).resolves.toBeUndefined();
      await expect.soft(recovered.e2ee.explain(target)).resolves.toEqual({ state: "ready" });
      await expect.soft(recovered.e2ee.recovery.status(material)).resolves.toMatchObject({
        spaces: checkedSpaces,
      });
      if (otherRecipient) {
        expect(
          await recovered.all(app.__e2ee_space_grants.where({ recipientId: otherRecipient }), {
            tier: "global",
          }),
        ).toEqual(
          expect.arrayContaining([
            expect.objectContaining({ spaceId: accepted.id, operation: "add" }),
            expect.objectContaining({ spaceId: accepted.id, operation: "remove" }),
          ]),
        );
      }
    } finally {
      for (const secret of secrets) secret.fill(0);
      await Promise.all(sessions.map((session) => session.close()));
      await server.stop();
    }
  },
  120_000,
);
