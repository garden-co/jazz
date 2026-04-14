import { afterEach, describe, expect, it, vi } from "vitest";
import { readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { createTempRootTracker, getAvailablePort, todoSchema } from "./test-helpers.js";
import * as devServer from "./dev-server.js";
import { __resetJazzPluginForTests, withJazz } from "./expo.js";

const dev = await import("./index.js");

const tempRoots = createTempRootTracker();
const originalJazzServerUrl = process.env.EXPO_PUBLIC_JAZZ_SERVER_URL;
const originalJazzAppId = process.env.EXPO_PUBLIC_JAZZ_APP_ID;
const originalNodeEnv = process.env.NODE_ENV;

afterEach(async () => {
  await __resetJazzPluginForTests();
  await tempRoots.cleanup();
  vi.restoreAllMocks();

  if (originalJazzServerUrl === undefined) {
    delete process.env.EXPO_PUBLIC_JAZZ_SERVER_URL;
  } else {
    process.env.EXPO_PUBLIC_JAZZ_SERVER_URL = originalJazzServerUrl;
  }

  if (originalJazzAppId === undefined) {
    delete process.env.EXPO_PUBLIC_JAZZ_APP_ID;
  } else {
    process.env.EXPO_PUBLIC_JAZZ_APP_ID = originalJazzAppId;
  }

  process.env.NODE_ENV = originalNodeEnv;
});

describe("withJazz", () => {
  it("preserves existing config fields", async () => {
    process.env.NODE_ENV = "production";

    const resolved = await withJazz({
      name: "my-app",
      slug: "my-app",
      extra: { existingKey: "existingValue" },
    });

    expect(resolved.name).toBe("my-app");
    expect(resolved.slug).toBe("my-app");
    expect(resolved.extra?.existingKey).toBe("existingValue");
  });

  it("does not inject Jazz env vars outside development", async () => {
    process.env.NODE_ENV = "production";

    await withJazz({});

    expect(process.env.EXPO_PUBLIC_JAZZ_APP_ID).toBeUndefined();
    expect(process.env.EXPO_PUBLIC_JAZZ_SERVER_URL).toBeUndefined();
  });

  it("starts a local server in development and injects EXPO_PUBLIC_JAZZ_* env vars", async () => {
    const port = await getAvailablePort();
    const schemaDir = await tempRoots.create("jazz-expo-test-");
    await writeFile(join(schemaDir, "schema.ts"), todoSchema());

    const resolved = await withJazz(
      { name: "my-app" },
      {
        server: { port, adminSecret: "expo-test-admin" },
        schemaDir,
      },
    );

    const healthResponse = await fetch(`http://127.0.0.1:${port}/health`);
    expect(healthResponse.ok).toBe(true);

    const schemasResponse = await fetch(`http://127.0.0.1:${port}/schemas`, {
      headers: { "X-Jazz-Admin-Secret": "expo-test-admin" },
    });
    expect(schemasResponse.ok).toBe(true);

    const body = (await schemasResponse.json()) as { hashes?: string[] };
    expect(body.hashes?.length).toBeGreaterThan(0);

    expect(process.env.EXPO_PUBLIC_JAZZ_APP_ID).toBeTruthy();
    expect(process.env.EXPO_PUBLIC_JAZZ_SERVER_URL).toBe(`http://127.0.0.1:${port}`);
    expect(resolved.extra?.jazzAppId).toBe(process.env.EXPO_PUBLIC_JAZZ_APP_ID);
    expect(resolved.extra?.jazzServerUrl).toBe(`http://127.0.0.1:${port}`);
  }, 30_000);

  it("releases a failed startup before retrying the same port after the schema is fixed", async () => {
    const port = await getAvailablePort();
    const schemaDir = await tempRoots.create("jazz-expo-retry-");
    await writeFile(join(schemaDir, "schema.ts"), todoSchema());

    const pushSchemaCatalogue = vi
      .spyOn(devServer, "pushSchemaCatalogue")
      .mockRejectedValueOnce(new Error("schema push failed"));

    await expect(
      withJazz(
        {},
        {
          server: { port, adminSecret: "expo-retry-admin" },
          schemaDir,
        },
      ),
    ).rejects.toThrow("schema push failed");

    const resolved = await withJazz(
      {},
      {
        server: { port, adminSecret: "expo-retry-admin" },
        schemaDir,
      },
    );

    expect(process.env.EXPO_PUBLIC_JAZZ_SERVER_URL).toBe(`http://127.0.0.1:${port}`);
    expect(pushSchemaCatalogue).toHaveBeenCalledTimes(2);
  }, 30_000);

  it("throws when connecting to an existing server without adminSecret", async () => {
    process.env.EXPO_PUBLIC_JAZZ_SERVER_URL = "http://127.0.0.1:4000";
    process.env.EXPO_PUBLIC_JAZZ_APP_ID = "00000000-0000-0000-0000-000000000111";

    await expect(withJazz({})).rejects.toThrow(
      "adminSecret is required when connecting to an existing server",
    );
  });

  it("throws when connecting to an existing server without appId", async () => {
    process.env.EXPO_PUBLIC_JAZZ_SERVER_URL = "http://127.0.0.1:4000";
    delete process.env.EXPO_PUBLIC_JAZZ_APP_ID;

    await expect(withJazz({}, { adminSecret: "expo-test-admin" })).rejects.toThrow(
      "appId is required when connecting to an existing server",
    );
  });

  it("reuses the same managed server across repeated config calls in one process", async () => {
    const port = await getAvailablePort();
    const schemaDir = await tempRoots.create("jazz-expo-repeat-");
    await writeFile(join(schemaDir, "schema.ts"), todoSchema());

    const options = {
      server: { port, adminSecret: "expo-repeat-admin" },
      schemaDir,
    };

    const first = await withJazz({}, options);
    const second = await withJazz({}, options);

    expect(process.env.EXPO_PUBLIC_JAZZ_SERVER_URL).toBe(`http://127.0.0.1:${port}`);
    expect(second.extra?.jazzServerUrl).toBe(first.extra?.jazzServerUrl);
    expect(second.extra?.jazzAppId).toBe(first.extra?.jazzAppId);
  }, 30_000);

  it("persists the generated app id to .env in the schema dir", async () => {
    const port = await getAvailablePort();
    const schemaDir = await tempRoots.create("jazz-expo-env-");
    await writeFile(join(schemaDir, "schema.ts"), todoSchema());

    await withJazz(
      {},
      {
        server: { port, adminSecret: "expo-env-admin" },
        schemaDir,
      },
    );

    const envContent = await readFile(join(schemaDir, ".env"), "utf8");
    expect(envContent).toContain(`EXPO_PUBLIC_JAZZ_APP_ID=${process.env.EXPO_PUBLIC_JAZZ_APP_ID}`);
  }, 30_000);

  it("reuses the app id from .env across restarts", async () => {
    const port = await getAvailablePort();
    const schemaDir = await tempRoots.create("jazz-expo-reuse-env-");
    await writeFile(join(schemaDir, "schema.ts"), todoSchema());

    const existingAppId = "11111111-1111-1111-1111-111111111111";
    await writeFile(join(schemaDir, ".env"), `EXPO_PUBLIC_JAZZ_APP_ID=${existingAppId}\n`);

    await withJazz(
      {},
      {
        server: { port, adminSecret: "expo-reuse-admin" },
        schemaDir,
      },
    );

    expect(process.env.EXPO_PUBLIC_JAZZ_APP_ID).toBe(existingAppId);
  }, 30_000);

  it("throws on conflicting dev configurations in one process", async () => {
    const firstPort = await getAvailablePort();
    const firstSchemaDir = await tempRoots.create("jazz-expo-conflict-a-");
    await writeFile(join(firstSchemaDir, "schema.ts"), todoSchema());

    await withJazz(
      {},
      {
        server: { port: firstPort, adminSecret: "expo-conflict-a" },
        schemaDir: firstSchemaDir,
      },
    );

    const secondPort = await getAvailablePort();
    const secondSchemaDir = await tempRoots.create("jazz-expo-conflict-b-");
    await writeFile(join(secondSchemaDir, "schema.ts"), todoSchema());

    await expect(
      withJazz(
        {},
        {
          server: { port: secondPort, adminSecret: "expo-conflict-b" },
          schemaDir: secondSchemaDir,
        },
      ),
    ).rejects.toThrow("conflicting Jazz dev runtime configuration");
  }, 30_000);
});

describe("dev barrel", () => {
  it("exposes withJazzExpo as a barrel alias for withJazz", () => {
    expect(dev.withJazzExpo).toBe(withJazz);
  });
});
