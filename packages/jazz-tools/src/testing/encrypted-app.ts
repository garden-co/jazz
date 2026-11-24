import { mkdtemp } from "node:fs/promises";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { AccountStore } from "../accounts/persistence.js";
import {
  acceptDeviceAccounts,
  createJazz,
  createJazzClient,
  memory,
  memoryPrivateStore,
  sqlite,
  type CreateJazzOptions,
  type Jazz,
  type PrivateStore,
} from "../backend/index.js";

type Cleanups = Array<() => Promise<unknown>>;
type AppConfig = Pick<CreateJazzOptions, "app" | "permissions">;

/** Starts a keyless backend for an encrypted test app. */
export async function serveEncryptedTestApp(
  config: AppConfig & {
    authenticate?: CreateJazzOptions["authenticate"];
    dir?: string;
    port?: number;
    storage?: "sqlite" | "memory";
  },
  cleanups: Cleanups,
) {
  const dir = config.dir ?? (await mkdtemp(join(tmpdir(), "jazz-test-encrypted-")));
  const storePath = join(dir, "core.sqlite");
  const jazz = createJazz({
    app: config.app,
    permissions: config.permissions,
    authenticate: config.authenticate ?? acceptDeviceAccounts,
    store: config.storage === "memory" ? memory() : sqlite({ path: storePath }),
  });
  await jazz.ready().catch(async (error) => {
    await jazz.close().catch(() => undefined);
    throw error;
  });
  const server = createServer((req, res) =>
    jazz.nodeHandler(req, res, () => {
      res.statusCode = 404;
      res.end();
    }),
  );
  let stopping: Promise<void> | undefined;
  const stop = () =>
    (stopping ??= (async () => {
      try {
        server.closeAllConnections();
        await new Promise((resolve) => server.close(resolve));
      } finally {
        await jazz.close();
      }
    })());
  cleanups.push(stop);
  await jazz.attach(server);
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(config.port ?? 0, "127.0.0.1", () => {
      server.off("error", reject);
      resolve();
    });
  });
  const port = (server.address() as AddressInfo).port;
  return { jazz, server, dir, port, storePath, serverUrl: `http://127.0.0.1:${port}/jazz`, stop };
}

export function memoryTestAccountStore(): AccountStore {
  let stored: string | null = null;
  return {
    read: async () => stored,
    update: async (transform) => void (stored = transform(stored)),
  };
}

/** Opens a device with its own keys. By default it waits for encryption to be active. */
export async function openEncryptedTestDevice(
  host: { serverUrl: string; jazz: Pick<Jazz, "appId"> },
  config: AppConfig & {
    key?: string;
    privateStore?: PrivateStore;
    accounts?: AccountStore;
    dataPath?: string;
    wait?: boolean;
  },
  cleanups: Cleanups,
) {
  const session = await createJazzClient({
    serverUrl: host.serverUrl,
    appId: host.jazz.appId,
    app: config.app,
    permissions: config.permissions,
    store: config.accounts ?? memoryTestAccountStore(),
    initial: "device",
    driver: config.dataPath
      ? { type: "persistent", dataPath: config.dataPath }
      : { type: "memory" },
    encryption: { store: config.privateStore ?? memoryPrivateStore() },
  });
  cleanups.push(() => session.close());
  if (config.key) await session.restoreDeviceAccount(config.key);
  const db = session.db;
  if (config.wait !== false) {
    const status = await db.encryption.ready({ timeoutMs: 20_000 });
    if (status !== "active") throw new Error(`Expected active encryption, got ${status}`);
  }
  return { session, db, account: session.getSnapshot().user!.accountId };
}

/** Runs the registered test cleanups, newest first. */
export async function cleanUpTestApp(cleanups: Cleanups): Promise<void> {
  while (cleanups.length) await cleanups.pop()!().catch(() => undefined);
}
