import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, expect, it, vi } from "vitest";
import type { AccountStore } from "../../src/accounts/persistence.js";
import { EncryptionError, memoryPrivateStore } from "../../src/encryption/index.js";
import { createJazzClient } from "../../src/react-native/create-jazz-session.js";
import { startTestJazz } from "../../src/testing/index.js";
import { app } from "../fixtures/encrypted/jazz.generated.js";
import permissions from "../fixtures/encrypted/jazz.permissions.json";
import { createPlatformHost, installPlatformHost } from "./native-platform.js";

const cleanups: Array<() => unknown | Promise<unknown>> = [];

afterEach(async () => {
  const errors: unknown[] = [];
  for (const cleanup of cleanups.splice(0).reverse()) {
    try {
      await cleanup();
    } catch (error) {
      errors.push(error);
    }
  }
  if (errors.length)
    throw new AggregateError(errors, "React Native encryption fixture cleanup failed");
});

async function installPlatform(): Promise<AccountStore> {
  const directory = await mkdtemp(join(tmpdir(), "jazz-rn-encryption-"));
  cleanups.push(() => rm(directory, { recursive: true, force: true }));
  const platform = createPlatformHost(directory);
  cleanups.push(() => {
    platform.close();
  });
  installPlatformHost(platform);
  let stored: string | null = null;
  return {
    async read() {
      return stored;
    },
    async update(transform) {
      stored = transform(stored);
    },
  };
}

it("opens an encrypted device from the public React Native session over the app's private store", async () => {
  const backend = await startTestJazz({ app, permissions, coreOptions: { appId: randomUUID() } });
  cleanups.push(() => backend.stop());
  const accountStore = await installPlatform();
  const privateStore = memoryPrivateStore();
  const protect = vi.spyOn(privateStore, "protect");
  const config = {
    scope: backend.appId,
    serverUrl: backend.url,
    initial: "device" as const,
    store: accountStore,
    encryption: { app, store: privateStore },
  };

  const first = await createJazzClient(config);
  cleanups.push(() => first.close());
  expect(protect).toHaveBeenCalled();
  const encryption = first.db.encryption;
  const device = await encryption.deviceId();
  await expect(encryption.ready({ timeoutMs: 30_000 })).resolves.toBe("active");
  expect(() => encryption.recovery.create()).toThrow(/device account's recovery/);
  await encryption.ready();
  const [root] = await encryption.recovery.list();
  const retirement = encryption.recovery.retire(root!.id);
  await expect(retirement).rejects.toBeInstanceOf(EncryptionError);
  await expect(retirement).rejects.toMatchObject({ code: "device-account-root-required" });

  const projectId = randomUUID();
  const accountId = first.getSnapshot().user!.accountId;
  const self = await encryption.members.row({ table: "projects", id: projectId }, accountId);
  const project = await first.db.transaction((tx) => {
    tx.insert(app.projects, { name: "plans", ownerId: accountId }, { id: projectId });
    tx.insert(app.__e2ee_members, self);
  });
  await project.wait({ tier: "global" });
  const write = first.db.insert(app.tasks, { projectId, title: "private plans" });
  expect(write.value).toEqual({
    id: expect.any(String),
    projectId,
    title: "private plans",
    label: null,
    note: null,
  });
  await expect(write.wait({ tier: "global" })).resolves.toEqual(write.value);
  await expect(first.db.one(app.tasks.where({ id: write.value.id }))).resolves.toEqual(write.value);

  await first.db.disconnect();
  const local = first.db.insert(app.tasks, { projectId, title: "local draft" });
  await local.wait({ tier: "local" });
  await first.db.deleteClientStorage();
  await first.close();
  const reopened = await createJazzClient(config);
  cleanups.push(() => reopened.close());
  await expect(reopened.db.encryption.deviceId()).resolves.toBe(device);
  await expect
    .poll(() => reopened.db.one(app.tasks.where({ id: write.value.id })), {
      timeout: 30_000,
    })
    .toEqual(write.value);
  await expect(reopened.db.one(app.tasks.where({ id: local.value.id }))).resolves.toBeNull();

  await expect(async () => {
    const write = backend.jazz.db.insert(app.tasks, {
      projectId: randomUUID(),
      title: "only a device can write this",
    });
    await write.wait({ tier: "local" });
  }).rejects.toThrow(/the backend holds no encryption keys/);
}, 60_000);

it("can reopen an account after its private store fails during startup", async () => {
  const accountStore = await installPlatform();
  const config = {
    scope: "rn-encrypted-device",
    serverUrl: null,
    store: accountStore,
  };
  const broken = memoryPrivateStore();
  broken.openScope = async () => {
    throw new Error("private store is unavailable");
  };
  await expect(createJazzClient({ ...config, encryption: { app, store: broken } })).rejects.toThrow(
    /private store is unavailable/,
  );

  const opened = await createJazzClient({
    ...config,
    encryption: { app, store: memoryPrivateStore() },
  });
  cleanups.push(() => opened.close());
  await expect(opened.db.encryption.deviceId()).resolves.toEqual(expect.any(String));
});
