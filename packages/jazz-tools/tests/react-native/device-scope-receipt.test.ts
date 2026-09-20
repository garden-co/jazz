import type { TablePolicies } from "../../src/schema.js";
import { expect, it, vi } from "vitest";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createAccountManager } from "../../src/react-native/index.js";
import { createNativeAccountTestSession } from "../../src/_dev/native-account-session.js";
import { createPlatformHost, installPlatformHost } from "./native-platform.js";
import { decodeNativeForegroundResponse, encodeNativeForegroundCommand } from "jazz-rn/relay";
import { proveForegroundScopeIsolation } from "../../../../dev/rn-device-acceptance/src/foreground-byte-abi.ts";
import {
  schemaSource,
  scopeQuery,
} from "../../../../dev/rn-device-acceptance/src/scope-fixture.ts";

// The shared device helper must use this test module's source account registry.
vi.mock("jazz-tools/react-native", () => import("../../src/react-native/index.js"));

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

it.each([0, 16, 32])(
  "runs the scope receipt after device recovery with %ims event-loop turns",
  async (yieldDelayMs) => {
    const { startLocalEdgeSessionHarness } =
      await import("../../../../dev/rn-device-acceptance/scripts/edge-session-harness.mjs");
    const { seedHighLevelForegroundRuntime, proveHighLevelForegroundRelayReadback } =
      await import("../../../../dev/rn-device-acceptance/src/high-level-foreground.ts");
    const { proveForegroundByteAbi, proveForegroundJsReentry, proveForegroundRevoked } =
      await import("../../../../dev/rn-device-acceptance/src/foreground-byte-abi.ts");
    const runNonce = "7e2e2e20-849e-4ac7-8bda-3278a94f7531";
    const server = await startLocalEdgeSessionHarness({
      device: "native-replay",
      runNonce,
      host: "127.0.0.1",
    });
    const directory = await mkdtemp(join(tmpdir(), "rn-device-scope-sequence-"));
    const host = createPlatformHost(directory);
    installPlatformHost(host);
    let lease: Awaited<ReturnType<typeof createNativeAccountTestSession>> | undefined;
    try {
      let stored: string | null = null;
      const accounts = await createAccountManager({
        appId: "jazz-device-acceptance",
        serverUrl: server.endpoint,
        profile: "device-scope-a",
        store: {
          async read() {
            return stored;
          },
          async update(transform) {
            stored = transform(stored);
          },
        },
      });
      const account = accounts.createLocalFirst();
      const admit = () =>
        createNativeAccountTestSession(
          { appId: "jazz-device-acceptance", account, serverUrl: server.endpoint },
          schemaSource,
        );
      const codec = {
        encode: encodeNativeForegroundCommand,
        decode: decodeNativeForegroundResponse,
      };
      lease = await admit();
      proveForegroundJsReentry(host, lease.capability, codec);
      proveForegroundByteAbi(host, lease.capability, codec);
      const revoked = host.openAttached(lease.capability);
      lease.close();
      proveForegroundRevoked(revoked, codec.encode);
      lease = await admit();
      const rawSibling = host.openAttached(lease.capability);
      let acknowledgements = 0;
      try {
        await seedHighLevelForegroundRuntime(
          lease,
          runNonce,
          () => {},
          async () => {
            await server.waitForCoreObservation();
            if (acknowledgements++ === 0) await server.interruptAndRecover();
          },
        );
        expect(codec.decode(rawSibling.execute(codec.encode("tick"))).type).toBe("ticked");
      } finally {
        rawSibling.close();
      }
      await proveHighLevelForegroundRelayReadback(lease, runNonce);
      const diagnostics: string[] = [];
      await proveForegroundScopeIsolation(
        host,
        lease.capability,
        codec,
        { write: "a", contains: ["a"], excludes: ["b"] },
        (stage) => diagnostics.push(stage),
        {
          timeoutMs: 5_000,
          now: () => performance.now(),
          yieldTurn: () => new Promise<void>((resolve) => setTimeout(resolve, yieldDelayMs)),
        },
        (detail) => {
          diagnostics.push(detail);
        },
      ).catch((cause) => {
        throw new Error(`Device lifecycle scope receipt failed (${diagnostics.join(", ")})`, {
          cause,
        });
      });
      // Local success must not hide a later rejected authority closure. Keep a
      // strict Global subscription alive until its authoritative row is read.
      const authority = host.openAttached(lease.capability);
      const execute = (command: Parameters<typeof codec.encode>[0]) =>
        codec.decode(authority.execute(codec.encode(command)));
      authority.setTickScheduler?.(() => {});
      const subscribed = execute({
        type: "subscribe",
        query: scopeQuery,
        optionsJson: '{"tier":"global","local_updates":"deferred"}',
      });
      expect(subscribed.type).toBe("subscribed");
      const deadline = performance.now() + 5_000;
      const settle = async (response: ReturnType<typeof codec.decode>) => {
        while (response.type === "pending" && performance.now() < deadline) {
          authority.tick();
          await new Promise<void>((resolve) => setTimeout(resolve, yieldDelayMs));
          response = execute({ type: "poll", operation: response.operation });
        }
        expect(
          response.type,
          "authority operation must settle within the existing five-second budget",
        ).not.toBe("pending");
        return response;
      };
      try {
        let globalPublished = false;
        while (!globalPublished && performance.now() < deadline) {
          authority.tick();
          await new Promise<void>((resolve) => setTimeout(resolve, yieldDelayMs));
          if (subscribed.type !== "subscribed")
            throw new Error("authority subscription did not open");
          const events = await settle(
            execute({ type: "drainSubscription", subscription: subscribed.subscription }),
          );
          expect(events.type).toBe("subscriptionEvents");
          if (events.type !== "subscriptionEvents")
            throw new Error("authority subscription did not drain");
          expect(events.events.filter((event) => event.type === "rejected")).toEqual([]);
          globalPublished = events.events.some(
            (event) => event.type === "delta" && event.settled && event.tier === "global",
          );
        }
        expect(globalPublished, "Global subscription must publish an authority receipt").toBe(true);
        const rows = await settle(
          execute({
            type: "all",
            query: scopeQuery,
            optionsJson: '{"tier":"global","local_updates":"deferred"}',
          }),
        );
        expect(rows.type).toBe("rows");
        if (rows.type !== "rows") throw new Error("authority read did not return rows");
        expect(new TextDecoder().decode(rows.rows)).toContain("scope-a-private-row");
      } finally {
        authority.close();
      }
    } finally {
      lease?.close();
      host.close();
      await server.terminate();
      await rm(directory, { recursive: true, force: true });
    }
  },
  90_000,
);
