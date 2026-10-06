import { afterEach, expect, it } from "vitest";
import { generateAuthSecret, schema, ReadTier } from "../../src/index.js";
import { deploy } from "../../src/dev/catalogue.js";
import { TestCleanup, createBrowserTestDb, uniqueDbName, waitForCondition } from "./support.js";
import { getJazzServerInfo, createJazzServerTransportControl } from "./testing-server.js";

const app = schema.defineApp({ entries: schema.table({ title: schema.string() }, {}) });
const permissions = schema.definePermissions(app, ({ policy }) => [
  policy.entries.allowRead.always(),
  policy.entries.allowInsert.always(),
]);

const ctx = new TestCleanup();
afterEach(async () => {
  await ctx.cleanup();
});

it("shows local rows at the deadline when a live server has not answered", async () => {
  const { appId, serverUrl, adminSecret } = await getJazzServerInfo(
    uniqueDbName("local-first-server-wait-deadline"),
  );
  await deploy({ appId, serverUrl, adminSecret, schema: app.wasmSchema, permissions });
  const transport = await createJazzServerTransportControl(serverUrl);
  try {
    const writer = ctx.track(
      await createBrowserTestDb({
        appId,
        serverUrl,
        secret: generateAuthSecret(),
        driver: { type: "memory" },
      }),
    );
    await writer.insert(app.entries, { title: "Only on the server" }).wait({ tier: "global" });

    const reader = ctx.track(
      await createBrowserTestDb({
        appId,
        serverUrl: transport.url,
        secret: generateAuthSecret(),
        driver: { type: "memory" },
      }),
    );
    // The reader's own write reaching the server proves its link is live.
    await reader.insert(app.entries, { title: "Written by the reader" }).wait({ tier: "global" });

    await transport.blockInbound();
    const waitMs = 1_500;
    const started = Date.now();
    const deliveries: string[][] = [];
    ctx.trackSubscription(
      reader.subscribe(
        app.entries,
        (rows) => deliveries.push(rows.map((row) => row.title).sort()),
        { tier: ReadTier.LocalFirst, firstLoadRemoteWaitMs: waitMs },
      ),
    );
    await waitForCondition(
      async () => deliveries.length > 0,
      10_000,
      "local-first opening at the deadline",
    );
    // The opening waited for the whole timeout, then showed the local rows.
    expect(Date.now() - started).toBeGreaterThanOrEqual(waitMs - 50);
    expect(deliveries[0]).toEqual(["Written by the reader"]);

    const oneShotStarted = Date.now();
    expect(
      (
        await reader.all(app.entries, { tier: ReadTier.LocalFirst, firstLoadRemoteWaitMs: waitMs })
      ).map((row) => row.title),
    ).toEqual(["Written by the reader"]);
    expect(Date.now() - oneShotStarted).toBeGreaterThanOrEqual(waitMs - 50);

    // The late answer arrives as an ordinary change.
    await transport.unblock();
    await waitForCondition(
      async () =>
        JSON.stringify(deliveries.at(-1)) ===
        JSON.stringify(["Only on the server", "Written by the reader"]),
      10_000,
      "the late server answer as a change",
    );
  } finally {
    await transport.stop();
  }
}, 60_000);
