import type { TablePolicies } from "../../src/schema.js";
import { expect, it } from "vitest";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createAccountManager } from "../../src/react-native/index.js";
import { createNativeAccountTestSession } from "../../src/_dev/native-account-session.js";
import { createPlatformHost, installPlatformHost } from "./native-platform.js";
import { decodeNativeForegroundResponse, encodeNativeForegroundCommand } from "jazz-rn/relay";
import { proveForegroundScopeIsolation } from "../../../../dev/rn-device-acceptance/src/foreground-byte-abi.ts";
import { schemaSource } from "../../../../dev/rn-device-acceptance/src/scope-fixture.ts";

it.each([false, true])(
  "runs the device owner-scoped write/read receipt through the native account foreground (online=%s)",
  async (online) => {
    const { startLocalJazzServer } = await import("../../src/testing/index.js");
    const fixture = JSON.parse(schemaSource);
    const server = online
      ? await startLocalJazzServer({
          appId: "jazz-device-acceptance",
          inMemory: true,
          allowLocalFirstAuth: true,
          schema: fixture.tables,
          permissions: Object.fromEntries(
            Object.entries(fixture.tables).map(([name, table]) => [
              name,
              (table as { policies: TablePolicies }).policies,
            ]),
          ),
        })
      : undefined;
    const directory = await mkdtemp(join(tmpdir(), "rn-device-scope-receipt-"));
    const host = createPlatformHost(directory);
    installPlatformHost(host);
    let lease: Awaited<ReturnType<typeof createNativeAccountTestSession>> | undefined;
    try {
      let stored: string | null = null;
      const accounts = await createAccountManager({
        appId: "jazz-device-acceptance",
        serverUrl: server?.url ?? "https://core.example",
        store: {
          async read() {
            return stored;
          },
          async update(transform) {
            stored = transform(stored);
          },
        },
      });
      lease = await createNativeAccountTestSession(
        {
          appId: "jazz-device-acceptance",
          account: accounts.createLocalFirst(),
          ...(server ? { serverUrl: server.url } : {}),
        },
        schemaSource,
      );
      const diagnostics: string[] = [];
      await expect(
        proveForegroundScopeIsolation(
          host,
          lease.capability,
          {
            encode: encodeNativeForegroundCommand,
            decode: decodeNativeForegroundResponse,
          },
          { write: "a", contains: ["a"], excludes: ["b"] },
          (stage) => diagnostics.push(stage),
          undefined,
          (detail) => {
            diagnostics.push(detail);
          },
        ).catch((error) => {
          throw new Error(`Device scope receipt failed (${diagnostics.join(", ")})`, {
            cause: error,
          });
        }),
      ).resolves.toBeUndefined();
    } finally {
      lease?.close();
      host.close();
      await server?.stop();
      await rm(directory, { recursive: true, force: true });
    }
  },
);
