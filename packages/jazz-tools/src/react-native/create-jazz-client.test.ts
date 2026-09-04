import { afterEach, describe, expect, it, vi } from "vitest";
import { schema as s } from "../index.js";
import { NATIVE_RELAY_ABI_V1 } from "jazz-rn";
import {
  PostcardWriter,
  PostcardReader,
  createRecord,
  writeDescriptor,
} from "../runtime/native-runtime/native-codec.js";

const nativeForegroundTest = vi.hoisted(() => ({
  execute: undefined as undefined | ((command: Uint8Array) => Uint8Array),
  tick: vi.fn(),
  setTickScheduler: vi.fn(),
  close: vi.fn(() => true),
  openAttached: vi.fn(),
  turboModule: {
    getAbiVersion: () => NATIVE_RELAY_ABI_V1,
    execute: async () => {
      throw new Error("the read-only foreground path must not use TurboModule execute");
    },
  },
}));

// This intentionally mocks only the platform-installed TurboModule. The
// foreground smoke below imports the built installed-package `jazz-rn/relay` entry point and
// therefore exercises its real ABI guard plus byte encoder/decoder rather
// than a hand-maintained copy in jazz-tools. The native development build is
// responsible for installing this same HostObject factory through JSI.
vi.mock("react-native", () => ({
  TurboModuleRegistry: {
    get: () => nativeForegroundTest.turboModule,
  },
}));
import {
  createDb,
  createJazzClient,
  REACT_NATIVE_AUTH_SECRET_STORE_REQUIRED_ERROR,
  REACT_NATIVE_MEMORY_RUNTIME_UNSUPPORTED_ERROR,
  REACT_NATIVE_NATIVE_RELAY_MEMORY_ONLY_ERROR,
  REACT_NATIVE_NATIVE_RELAY_REQUIRED_ERROR,
  REACT_NATIVE_SQLITE_STORAGE_REJECTED_ERROR,
  type JazzClient,
  type ReactNativeSqliteStorageDriver,
  type JazzClientConfig,
  useLocalFirstAuth,
} from "./index.js";

const app = s.defineApp({
  notes: s.table({ title: s.string() }),
});

const nativeRelayCapability = Uint8Array.from({ length: 32 }, (_, index) => index);

function installJsiForegroundFactory() {
  (globalThis as Record<string, unknown>).__jazzNativeForegroundRuntimeV1 = {
    abiVersion: NATIVE_RELAY_ABI_V1,
    openAttached: (capability: Uint8Array) => {
      nativeForegroundTest.openAttached(capability);
      return {
        execute: (command: Uint8Array) => nativeForegroundTest.execute!(command),
        tick: nativeForegroundTest.tick,
        setTickScheduler: nativeForegroundTest.setTickScheduler,
        close: nativeForegroundTest.close,
      };
    },
  };
}

function nativeRelayReceipt() {
  const commands: number[] = [];
  const base64 = (bytes: number[]) => btoa(String.fromCharCode(...bytes));
  return {
    commands,
    config: {
      executor: {
        execute: async (request: string) => {
          const tag = atob(request).charCodeAt(0);
          commands.push(tag);
          // Open, Attach, Receive, CloseClient/CloseRelay, and frame work.
          return tag === 1
            ? base64([1, 9])
            : tag === 2
              ? base64([2, 7])
              : tag === 7
                ? base64([5, 0])
                : tag === 3 || tag === 4
                  ? base64([3, 1])
                  : base64([4]);
        },
      },
      capability: nativeRelayCapability,
    },
  };
}

describe("React Native binding scaffolding in the Node test runtime", () => {
  let client: JazzClient | undefined;

  afterEach(async () => {
    await client?.shutdown();
    client = undefined;
    delete (globalThis as Record<string, unknown>).__jazzNativeForegroundRuntimeV1;
  });

  it("exports the exact installed-package persistence boundary messages", () => {
    expect(REACT_NATIVE_SQLITE_STORAGE_REJECTED_ERROR).toBe(
      "ReactNativeDbConfig.sqliteStorage is proposal-only and cannot be used by the v2 runtime; remove sqliteStorage (memory mode remains unverified scaffolding)",
    );
    expect(REACT_NATIVE_NATIVE_RELAY_REQUIRED_ERROR).toMatch(/JazzRelay native artifact/);
  });

  it("never falls back to browser localStorage for a React Native auth root", () => {
    expect(() => useLocalFirstAuth({} as never)).toThrow(
      REACT_NATIVE_AUTH_SECRET_STORE_REQUIRED_ERROR,
    );
  });

  it("rejects a server-only credential copied through a React Native client config", async () => {
    const serverConfig = {
      appId: "react-native-backend-secret-boundary",
      driver: { type: "memory" as const },
      backendSecret: "server-only",
    };
    const error = await createDb({ ...serverConfig } as never).catch((error: unknown) => error);

    expect(error).toBeInstanceOf(Error);
    expect((error as Error).message).toMatch(/createJazzContext/);
  });

  it("rejects explicit memory configuration instead of importing the browser WASM runtime", async () => {
    const error = await createJazzClient({
      appId: "react-native-memory-launch-test",
      driver: { type: "memory" },
    }).catch((error: unknown) => error);

    expect(error).toBeInstanceOf(Error);
    expect((error as Error).message).toBe(REACT_NATIVE_MEMORY_RUNTIME_UNSUPPORTED_ERROR);
  });

  it("accepts native admission only through the public RN client config", () => {
    const config: JazzClientConfig = {
      appId: "react-native-native-relay-public-config",
      nativeRelay: { capability: nativeRelayCapability },
      cookieSession: {
        issuer: "https://issuer.example",
        user_id: "public-config-reader",
        claims: {},
        authMode: "external",
      },
    };
    expect(config.nativeRelay?.capability).toBe(nativeRelayCapability);
  });

  it("rejects the default persistent configuration", async () => {
    const error = await createDb({
      appId: "react-native-default-persistent-boundary-test",
    }).catch((error: unknown) => error);
    expect(error).toBeInstanceOf(Error);
    expect((error as Error).message).toBe(REACT_NATIVE_NATIVE_RELAY_REQUIRED_ERROR);
  });

  it("runs a schema-backed foreground insert, query, subscription, and shutdown without loading WASM", async () => {
    nativeForegroundTest.tick.mockClear();
    nativeForegroundTest.setTickScheduler.mockClear();
    nativeForegroundTest.close.mockClear();
    nativeForegroundTest.openAttached.mockClear();
    installJsiForegroundFactory();
    const rowId = new Uint8Array(16).fill(7);
    const rows = encodeRows([{ table: "notes", rowId, title: "Native note" }]);
    const delta = encodeSubscriptionDelta({
      added: [{ table: "notes", rowId, title: "Native note" }],
    });
    let subscriptionDrainCount = 0;
    const commandTags: number[] = [];
    const readOptions: unknown[] = [];
    nativeForegroundTest.execute = (command) => {
      commandTags.push(command[0]!);
      switch (command[0]) {
        case 2:
          return Uint8Array.of(2, 11);
        case 18: {
          const reader = new PostcardReader(command.subarray(1));
          expect(reader.u64()).toBe(11);
          readOptions.push(JSON.parse(reader.string()));
          return encodeBytesResponse(3, rows);
        }
        case 20:
          return Uint8Array.of(4, 12);
        case 5: {
          subscriptionDrainCount += 1;
          if (subscriptionDrainCount === 1 || subscriptionDrainCount > 2) {
            return Uint8Array.of(5, 0);
          }
          return encodeSubscriptionEvents(delta);
        }
        case 6:
          return Uint8Array.of(6, 1);
        case 7:
          return Uint8Array.of(7, 1);
        case 10:
          return Uint8Array.of(11, 13);
        case 22:
          return Uint8Array.from([12, ...rowId]);
        case 21:
          return Uint8Array.from([16, ...command.subarray(1, 17)]);
        case 25:
          return Uint8Array.of(17, 0, 0, 0);
        case 26:
          return encodeNativeSession("reader");
        case 15:
          return Uint8Array.from([14, ...new Uint8Array(16).fill(4)]);
        case 16:
          return Uint8Array.of(15, 1);
        default:
          throw new Error(`unexpected foreground command ${command[0]}`);
      }
    };

    const relay = nativeRelayReceipt();
    client = await createJazzClient({
      appId: "react-native-native-foreground-read-receipt",
      nativeRelay: relay.config,
      cookieSession: {
        issuer: "https://issuer.example",
        user_id: "reader",
        claims: {},
        authMode: "external",
      },
      runtimeSources: {
        get wasmModule(): never {
          throw new Error("native foreground must not inspect WASM sources");
        },
      },
    });

    await expect(client.db.all(app.notes)).resolves.toMatchObject([{ title: "Native note" }]);
    let unsubscribe: () => void = () => {};
    const published = new Promise<unknown[]>((resolve) => {
      unsubscribe = client!.db.subscribe(app.notes, (notes) => {
        if (notes.length > 0) resolve(notes);
      });
    });
    await expect(published).resolves.toMatchObject([{ title: "Native note" }]);
    const ticksBeforeUnsubscribe = nativeForegroundTest.tick.mock.calls.length;
    unsubscribe();
    expect(nativeForegroundTest.tick.mock.calls.length).toBeGreaterThan(ticksBeforeUnsubscribe);
    const inserted = client.db.insert(app.notes, { title: "Native note" });
    await expect(inserted.wait({ tier: "local" })).resolves.toMatchObject({
      title: "Native note",
    });
    await expect(inserted.txId).resolves.toMatch(/[0-9a-f-]{36}/);
    await expect(client.db.all(app.notes, { tier: "edge" })).resolves.toMatchObject([
      { title: "Native note" },
    ]);
    expect(readOptions).toContainEqual({ tier: "edge" });
    expect(commandTags).toEqual(expect.arrayContaining([2, 18, 20, 5, 6, 10, 22, 15, 21, 26]));
    expect(nativeForegroundTest.tick.mock.calls.length).toBeGreaterThanOrEqual(3);
    expect(nativeForegroundTest.turboModule).not.toHaveProperty("installForegroundRuntime");
    expect(nativeForegroundTest.setTickScheduler).toHaveBeenCalledTimes(1);
    expect(nativeForegroundTest.openAttached).toHaveBeenCalledWith(nativeRelayCapability);
    // The foreground engine enters only through the capability-gated JSI host;
    // the high-level insert/query/subscription path must not fall back to the
    // generic TurboModule frame executor or WASM.
    expect(relay.commands).toEqual([]);

    await client.shutdown();
    client = undefined;
    expect(commandTags).toContain(7);
    expect(nativeForegroundTest.close).toHaveBeenCalledTimes(1);
  });

  it("fails closed on an in-place auth refresh instead of reading through the prior native capability", async () => {
    nativeForegroundTest.close.mockClear();
    installJsiForegroundFactory();
    const rows = encodeRows([
      {
        table: "notes",
        rowId: new Uint8Array(16).fill(9),
        title: "old admitted scope",
      },
    ]);
    nativeForegroundTest.execute = (command) => {
      if (command[0] === 2) return Uint8Array.of(2, 21);
      if (command[0] === 18) return encodeBytesResponse(3, rows);
      if (command[0] === 25) return Uint8Array.of(17, 0, 0, 0);
      if (command[0] === 26) return encodeNativeSession("old-reader");
      if (command[0] === 7) return Uint8Array.of(7, 1);
      throw new Error(`unexpected foreground command ${command[0]}`);
    };

    client = await createJazzClient({
      appId: "react-native-native-foreground-auth-rotation",
      nativeRelay: nativeRelayReceipt().config,
      cookieSession: {
        issuer: "https://issuer.example",
        user_id: "old-reader",
        claims: {},
        authMode: "external",
      },
    });

    await expect(client.db.all(app.notes)).resolves.toMatchObject([
      { title: "old admitted scope" },
    ]);
    expect(() =>
      client!.db.updateCookieSession({
        issuer: "https://issuer.example",
        user_id: "old-reader",
        claims: { role: "changed-by-auth-refresh" },
        authMode: "external",
      }),
    ).toThrow(/cannot rotate authentication in place/);
    expect(client.db.getAuthState().session?.claims.role).toBeUndefined();
    await expect(client.db.all(app.notes)).resolves.toMatchObject([
      { title: "old admitted scope" },
    ]);
    expect(nativeForegroundTest.close).not.toHaveBeenCalled();
  });

  it("rejects an opaque relay capability when memory mode would ignore it", async () => {
    const relay = nativeRelayReceipt();
    const error = await createDb({
      appId: "react-native-native-relay-memory-boundary",
      driver: { type: "memory" },
      nativeRelay: relay.config,
    }).catch((error: unknown) => error);

    expect(error).toBeInstanceOf(Error);
    expect((error as Error).message).toBe(REACT_NATIVE_NATIVE_RELAY_MEMORY_ONLY_ERROR);
  });

  it("rejects an injected SQLite driver before opening it", async () => {
    const open = vi.fn();
    const sqliteStorage: ReactNativeSqliteStorageDriver = {
      type: "react-native-sqlite",
      open,
      deleteDatabase: vi.fn(),
    };

    const error = await createDb({
      appId: "react-native-persistent-boundary-test",
      sqliteStorage,
    }).catch((error: unknown) => error);
    expect(error).toBeInstanceOf(Error);
    expect((error as Error).message).toBe(REACT_NATIVE_SQLITE_STORAGE_REJECTED_ERROR);
    expect(open).not.toHaveBeenCalled();
  });

  it("rejects rather than ignores sqliteStorage combined with memory mode", async () => {
    const open = vi.fn();
    const sqliteStorage: ReactNativeSqliteStorageDriver = {
      type: "react-native-sqlite",
      open,
      deleteDatabase: vi.fn(),
    };

    const error = await createDb({
      appId: "react-native-memory-sqlite-ambiguity-test",
      driver: { type: "memory" },
      sqliteStorage,
    }).catch((error: unknown) => error);
    expect(error).toBeInstanceOf(Error);
    expect((error as Error).message).toBe(REACT_NATIVE_SQLITE_STORAGE_REJECTED_ERROR);
    expect(open).not.toHaveBeenCalled();
  });

  it("keeps the Node-only memory scaffold out of the React Native entrypoint", async () => {
    const config = {
      appId: "react-native-memory-reopen-test",
      driver: { type: "memory" as const },
    };
    const error = await createJazzClient(config).catch((error: unknown) => error);
    expect(error).toBeInstanceOf(Error);
    expect((error as Error).message).toBe(REACT_NATIVE_MEMORY_RUNTIME_UNSUPPORTED_ERROR);
  });
});

type EncodedRow = { table: string; rowId: Uint8Array; title: string };

function encodeRows(rows: EncodedRow[]): Uint8Array {
  const writer = new PostcardWriter();
  writeRowBatches(writer, rows);
  return writer.finish();
}

function writeRowBatches(writer: PostcardWriter, rows: EncodedRow[]): void {
  const byTable = new Map<string, EncodedRow[]>();
  for (const row of rows) byTable.set(row.table, [...(byTable.get(row.table) ?? []), row]);
  writer.vec((batch, batchIndex) => {
    const [table, tableRows] = Array.from(byTable.entries())[batchIndex]!;
    const descriptor = [{ name: "title", valueType: { tag: 8 } }];
    batch.string(table);
    writeDescriptor(batch, descriptor);
    batch.vec((encoded, index) => {
      const row = tableRows[index]!;
      encoded.bytes(row.rowId);
      encoded.bool(false);
      encoded.bytes(
        createRecord(descriptor, [Uint8Array.from([2, ...new TextEncoder().encode(row.title)])]),
      );
    }, tableRows.length);
  }, byTable.size);
}

function encodeSubscriptionDelta(delta: { added: EncodedRow[] }): Uint8Array {
  const writer = new PostcardWriter();
  writeRowBatches(writer, delta.added);
  writeRowBatches(writer, []);
  writer.vec(() => undefined, 0);
  const occurrenceKeys = delta.added.map((row) => Uint8Array.from([1, ...row.rowId]));
  writer.vec((key, index) => key.bytes(occurrenceKeys[index]!), occurrenceKeys.length);
  writer.vec(() => undefined, 0);
  writer.vec(() => undefined, 0);
  writer.vec((indexWriter, index) => indexWriter.u64(index), delta.added.length);
  writer.vec(() => undefined, 0);
  writer.vec(() => undefined, 0);
  writer.vec(() => undefined, 0);
  return writer.finish();
}

function varint(value: number): number[] {
  const bytes: number[] = [];
  do {
    let byte = value % 128;
    value = Math.floor(value / 128);
    if (value > 0) byte |= 0x80;
    bytes.push(byte);
  } while (value > 0);
  return bytes;
}

function encodeBytesResponse(tag: number, bytes: Uint8Array): Uint8Array {
  return Uint8Array.from([tag, ...varint(bytes.length), ...bytes]);
}

function encodeSubscriptionEvents(delta: Uint8Array): Uint8Array {
  const tier = new TextEncoder().encode("local");
  return Uint8Array.from([5, 1, 0, 1, 1, tier.length, ...tier, ...varint(delta.length), ...delta]);
}

function encodeNativeSession(userId: string): Uint8Array {
  const writer = new PostcardWriter();
  writer.u64(18);
  writer.string("https://issuer.example");
  writer.string(userId);
  return writer.finish();
}
