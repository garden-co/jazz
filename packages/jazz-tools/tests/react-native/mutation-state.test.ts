import { expect, it, vi } from "vitest";
import { schema } from "../../src/index.js";
import {
  NativeForegroundDb,
  type NativeForegroundModule,
} from "../../src/react-native/native-foreground-db.js";
import {
  encodeCellsForRow,
  encodeCellsForPatch,
} from "../../src/runtime/native-runtime/native-runtime-adapter.js";

import { withNativeRelayFixture } from "./fixture.js";

const app = schema.defineApp({ documents: schema.table({ title: schema.string() }) });

// writeState is the existing low-level NativeDb contract; public WriteHandle
// exposes waits, so this receipt intentionally uses the real binding adapter.
it("reads live native fate/durability and retires closed write state", async () => {
  await withNativeRelayFixture(app, async (fixture) => {
    const commands = (await import("jazz-rn/relay")) as unknown as NativeForegroundModule;
    const native = new NativeForegroundDb(
      fixture.nativeHost.openAttached(fixture.capability),
      commands,
    );
    try {
      const write = native.insertEncoded(
        "documents",
        encodeCellsForRow(app.wasmSchema.documents!, {
          title: { type: "Text", value: "local state" },
        }),
      );
      expect(write.writeState()).toMatchObject({ fate: "Pending", global_time: null });
      await write.wait("local");
      expect(write.writeState()).toMatchObject({
        fate: "Pending",
        durability: "Local",
        global_time: null,
      });
      expect(write.close()).toBe(true);
      expect(write.close()).toBe(false);
      expect(() => write.writeState()).toThrow("write state is unavailable");
      await expect(write.wait("local")).rejects.toThrow("write state is unavailable");
    } finally {
      native.close();
    }
  });
});

it("delivers unwaited authority rejection once and leaves waited rejection with its waiter", async () => {
  const { startLocalJazzServer, startTestJwtIssuer, deploy, mergePermissionsIntoWasmSchema } =
    await import("../../src/testing/index.js");
  const issuer = await startTestJwtIssuer();
  const server = await startLocalJazzServer({
    inMemory: true,
    jwksUrl: issuer.jwksUrl,
    jwtIssuer: issuer.issuer,
    jwtAudience: issuer.audience,
  });
  try {
    const permissions = schema.definePermissions(app, ({ policy }) => [
      policy.documents.allowRead.always(),
    ]);
    await deploy({
      appId: server.appId,
      serverUrl: server.url,
      adminSecret: server.adminSecret,
      schema: app,
      permissions,
    });
    await withNativeRelayFixture(
      { wasmSchema: mergePermissionsIntoWasmSchema(app.wasmSchema, permissions) },
      async (fixture) => {
        const db = await fixture.createDb();
        const errors = vi.fn();
        db.onMutationError(errors);
        const rejected = db.insert(app.documents, { title: "unwaited rejection" });
        const id = await rejected.txId;
        await expect.poll(() => errors.mock.calls.length, { timeout: 10_000 }).toBe(1);
        expect(errors).toHaveBeenCalledWith(
          expect.objectContaining({
            code: "permission_denied",
            transaction: expect.objectContaining({
              transactionId: id,
              latestSettlement: expect.objectContaining({ kind: "rejected" }),
            }),
          }),
        );
        expect(await db.all(app.documents)).toEqual([]);
        const waited = db.insert(app.documents, { title: "waited rejection" });
        await expect(waited.wait({ tier: "edge" })).rejects.toMatchObject({
          name: "PersistedWriteRejectedError",
          code: "permission_denied",
        });
        expect(await db.all(app.documents)).toEqual([]);
        expect(errors).toHaveBeenCalledTimes(1);
      },
      {
        appId: server.appId,
        session: {
          issuer: issuer.issuer,
          user_id: "rn-mutation-user",
          claims: { role: "user" },
          authMode: "external",
        },
        upstream: { serverUrl: server.url, jwt: issuer.jwtForUser("rn-mutation-user") },
      },
    );
  } finally {
    await server.stop();
    await issuer.stop();
  }
}, 30_000);

it("closes an unfinished large upload without publication or late mutation callbacks", async () => {
  await withNativeRelayFixture(app, async (fixture) => {
    const observer = await fixture.createDb();
    const commands = (await import("jazz-rn/relay")) as unknown as NativeForegroundModule;
    const native = new NativeForegroundDb(
      fixture.nativeHost.openAttached(fixture.capability),
      commands,
    );
    const errors = vi.fn();
    native.onMutationError(errors);
    const upload = native.beginStreamingMutationEncoded(
      "documents",
      crypto.getRandomValues(new Uint8Array(16)),
      encodeCellsForPatch(app.wasmSchema.documents!, {}),
      "title",
    );
    // The source has delivered a bounded chunk but has not finalized a scalar.
    // Close before JS resumes its in-flight push; either native completion or
    // cancellation is valid, but no partial row or late callback is valid.
    const pushed = upload.push(new TextEncoder().encode("x".repeat(64 * 1024)));
    native.close();
    await Promise.allSettled([pushed]);
    await expect(upload.finish()).rejects.toThrow("closed");
    expect(await upload.abort()).toBe(false);
    expect(await observer.all(app.documents)).toEqual([]);
    const row = await observer
      .insert(app.documents, { title: "after close" })
      .wait({ tier: "local" });
    expect(await observer.all(app.documents)).toEqual([row]);
    expect(errors).not.toHaveBeenCalled();
  });
});
