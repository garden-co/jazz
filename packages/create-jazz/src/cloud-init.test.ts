import { mkdtempSync, readFileSync, writeFileSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { runHostedInit } from "./cloud-init.js";
import * as cloudProvision from "./cloud-provision.js";
import * as cloudEnv from "./cloud-env.js";

const NEXT_KEYS = {
  appId: "NEXT_PUBLIC_JAZZ_APP_ID",
  serverUrl: "NEXT_PUBLIC_JAZZ_SERVER_URL",
  adminSecret: "JAZZ_ADMIN_SECRET",
  backendSecret: "BACKEND_SECRET",
};

const SVELTE_KEYS = {
  appId: "PUBLIC_JAZZ_APP_ID",
  serverUrl: "PUBLIC_JAZZ_SERVER_URL",
  adminSecret: "JAZZ_ADMIN_SECRET",
  backendSecret: "BACKEND_SECRET",
};

const CLOUD_SYNC_URL = "https://v2.sync.jazz.tools/";
const API_URL = "https://example.com/api/apps/generate";

let dir: string;
let warnSpy: ReturnType<typeof vi.spyOn>;
let logSpy: ReturnType<typeof vi.spyOn>;

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), "cloud-init-test-"));
  warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
  logSpy = vi.spyOn(console, "log").mockImplementation(() => {});
});

afterEach(() => {
  warnSpy.mockRestore();
  logSpy.mockRestore();
  vi.restoreAllMocks();
});

function readEnv(d: string): string {
  return readFileSync(join(d, ".env"), "utf8");
}

function parseEnv(content: string): Record<string, string> {
  const map: Record<string, string> = {};
  for (let line of content.split("\n")) {
    if (line.endsWith("\r")) line = line.slice(0, -1);
    if (!line || line.startsWith("#")) continue;
    const eq = line.indexOf("=");
    if (eq === -1) continue;
    map[line.slice(0, eq)] = line.slice(eq + 1);
  }
  return map;
}

describe("runHostedInit", () => {
  describe("success path", () => {
    it("writes all four keys and prints credentials + banner", async () => {
      vi.spyOn(cloudProvision, "provisionHostedApp").mockResolvedValue({
        appId: "app-alice",
        adminSecret: "admin-secret-alice",
        backendSecret: "backend-secret-alice",
      });

      await runHostedInit({
        dir,
        cloudSyncUrl: CLOUD_SYNC_URL,
        envKeys: NEXT_KEYS,
        apiUrl: API_URL,
      });

      const content = readEnv(dir);
      const values = parseEnv(content);
      expect(values["NEXT_PUBLIC_JAZZ_APP_ID"]).toBe("app-alice");
      expect(values["NEXT_PUBLIC_JAZZ_SERVER_URL"]).toBe(CLOUD_SYNC_URL);
      expect(values["JAZZ_ADMIN_SECRET"]).toBe("admin-secret-alice");
      expect(values["BACKEND_SECRET"]).toBe("backend-secret-alice");
      expect(content).not.toContain("TODO");

      const logCalls = logSpy.mock.calls.map((c: unknown[]) => c.join(" "));
      expect(logCalls.some((l: string) => l.includes("app-alice"))).toBe(true);
      expect(logCalls.some((l: string) => l.includes("https://v2.dashboard.jazz.tools"))).toBe(
        true,
      );
      expect(logCalls.some((l: string) => l.includes("NEXT_PUBLIC_JAZZ_APP_ID=app-alice"))).toBe(
        true,
      );
      expect(logCalls.some((l: string) => l.includes("JAZZ_ADMIN_SECRET=admin-secret-alice"))).toBe(
        true,
      );
      expect(logCalls.some((l: string) => l.includes("BACKEND_SECRET=backend-secret-alice"))).toBe(
        true,
      );
    });

    it("works with SvelteKit PUBLIC_* keys", async () => {
      vi.spyOn(cloudProvision, "provisionHostedApp").mockResolvedValue({
        appId: "app-bob",
        adminSecret: "admin-bob",
        backendSecret: "backend-bob",
      });

      await runHostedInit({
        dir,
        cloudSyncUrl: CLOUD_SYNC_URL,
        envKeys: SVELTE_KEYS,
        apiUrl: API_URL,
      });

      const values = parseEnv(readEnv(dir));
      expect(values["PUBLIC_JAZZ_APP_ID"]).toBe("app-bob");
      expect(values["PUBLIC_JAZZ_SERVER_URL"]).toBe(CLOUD_SYNC_URL);
      expect(values["JAZZ_ADMIN_SECRET"]).toBe("admin-bob");
      expect(values["BACKEND_SECRET"]).toBe("backend-bob");
    });
  });

  describe("non-2xx HTTP failure path", () => {
    it("writes empty TODO placeholder and returns successfully", async () => {
      vi.spyOn(cloudProvision, "provisionHostedApp").mockRejectedValue(
        new cloudProvision.ProvisionHttpError(API_URL, 500),
      );

      await expect(
        runHostedInit({ dir, cloudSyncUrl: CLOUD_SYNC_URL, envKeys: NEXT_KEYS, apiUrl: API_URL }),
      ).resolves.toBeUndefined();

      const content = readEnv(dir);
      expect(content).toContain("TODO");
      const values = parseEnv(content);
      expect(values["NEXT_PUBLIC_JAZZ_APP_ID"]).toBe("");
      expect(values["NEXT_PUBLIC_JAZZ_SERVER_URL"]).toBe("");
      expect(values["JAZZ_ADMIN_SECRET"]).toBe("");
      expect(values["BACKEND_SECRET"]).toBe("");
    });

    it("names the error class on stderr", async () => {
      vi.spyOn(cloudProvision, "provisionHostedApp").mockRejectedValue(
        new cloudProvision.ProvisionHttpError(API_URL, 503),
      );

      await runHostedInit({
        dir,
        cloudSyncUrl: CLOUD_SYNC_URL,
        envKeys: NEXT_KEYS,
        apiUrl: API_URL,
      });

      const warnArgs = warnSpy.mock.calls.map((c: unknown[]) => c.join(" "));
      expect(warnArgs.some((w: string) => w.includes("ProvisionHttpError"))).toBe(true);
    });
  });

  describe("network failure path", () => {
    it("writes empty TODO placeholder and returns successfully", async () => {
      vi.spyOn(cloudProvision, "provisionHostedApp").mockRejectedValue(
        new cloudProvision.ProvisionNetworkError(API_URL, new TypeError("Failed to fetch")),
      );

      await expect(
        runHostedInit({ dir, cloudSyncUrl: CLOUD_SYNC_URL, envKeys: NEXT_KEYS, apiUrl: API_URL }),
      ).resolves.toBeUndefined();

      const content = readEnv(dir);
      expect(content).toContain("TODO");
      const values = parseEnv(content);
      expect(values["NEXT_PUBLIC_JAZZ_APP_ID"]).toBe("");
      expect(values["JAZZ_ADMIN_SECRET"]).toBe("");
    });

    it("names the error class on stderr", async () => {
      vi.spyOn(cloudProvision, "provisionHostedApp").mockRejectedValue(
        new cloudProvision.ProvisionNetworkError(API_URL, new TypeError("Failed to fetch")),
      );

      await runHostedInit({
        dir,
        cloudSyncUrl: CLOUD_SYNC_URL,
        envKeys: NEXT_KEYS,
        apiUrl: API_URL,
      });

      const warnArgs = warnSpy.mock.calls.map((c: unknown[]) => c.join(" "));
      expect(warnArgs.some((w: string) => w.includes("ProvisionNetworkError"))).toBe(true);
    });
  });

  describe("idempotency", () => {
    it("short-circuits when appId key already has a value", async () => {
      const provisionSpy = vi
        .spyOn(cloudProvision, "provisionHostedApp")
        .mockResolvedValue({ appId: "should-not-be-used", adminSecret: "x", backendSecret: "y" });

      writeFileSync(join(dir, ".env"), "NEXT_PUBLIC_JAZZ_APP_ID=existing-app-id\n", "utf8");

      await runHostedInit({
        dir,
        cloudSyncUrl: CLOUD_SYNC_URL,
        envKeys: NEXT_KEYS,
        apiUrl: API_URL,
      });

      expect(provisionSpy).not.toHaveBeenCalled();
      const content = readEnv(dir);
      expect(content).toBe("NEXT_PUBLIC_JAZZ_APP_ID=existing-app-id\n");
    });

    it("short-circuits when any hosted key has a non-empty value", async () => {
      const provisionSpy = vi
        .spyOn(cloudProvision, "provisionHostedApp")
        .mockResolvedValue({ appId: "should-not-be-used", adminSecret: "x", backendSecret: "y" });

      writeFileSync(join(dir, ".env"), "JAZZ_ADMIN_SECRET=some-secret\n", "utf8");

      await runHostedInit({
        dir,
        cloudSyncUrl: CLOUD_SYNC_URL,
        envKeys: NEXT_KEYS,
        apiUrl: API_URL,
      });

      expect(provisionSpy).not.toHaveBeenCalled();
    });

    it("does not short-circuit when all hosted keys are empty placeholders", async () => {
      vi.spyOn(cloudProvision, "provisionHostedApp").mockResolvedValue({
        appId: "new-app",
        adminSecret: "new-admin",
        backendSecret: "new-backend",
      });

      writeFileSync(
        join(dir, ".env"),
        "NEXT_PUBLIC_JAZZ_APP_ID=\nNEXT_PUBLIC_JAZZ_SERVER_URL=\nJAZZ_ADMIN_SECRET=\nBACKEND_SECRET=\n",
        "utf8",
      );

      await runHostedInit({
        dir,
        cloudSyncUrl: CLOUD_SYNC_URL,
        envKeys: NEXT_KEYS,
        apiUrl: API_URL,
      });

      expect(cloudProvision.provisionHostedApp).toHaveBeenCalledOnce();
    });
  });

  describe("writeHostedEnv throws (outer catch)", () => {
    it("best-effort writes empty placeholder and returns successfully without throwing", async () => {
      vi.spyOn(cloudProvision, "provisionHostedApp").mockResolvedValue({
        appId: "app-carol",
        adminSecret: "secret-with\nnewline",
        backendSecret: "backend-carol",
      });

      await expect(
        runHostedInit({ dir, cloudSyncUrl: CLOUD_SYNC_URL, envKeys: NEXT_KEYS, apiUrl: API_URL }),
      ).resolves.toBeUndefined();

      const warnArgs = warnSpy.mock.calls.map((c: unknown[]) => c.join(" "));
      expect(warnArgs.some((w: string) => w.includes("init-env failed unexpectedly"))).toBe(true);

      const envPath = join(dir, ".env");
      expect(existsSync(envPath)).toBe(true);
      const content = readEnv(dir);
      expect(content).toContain("TODO");
    });

    it("swallows writeHostedEnv errors in the outer catch fallback and does not rethrow", async () => {
      vi.spyOn(cloudProvision, "provisionHostedApp").mockResolvedValue({
        appId: "app-dave",
        adminSecret: "secret-with\nnewline",
        backendSecret: "backend-dave",
      });

      const writeHostedEnvSpy = vi
        .spyOn(cloudEnv, "writeHostedEnv")
        .mockImplementationOnce(() => {
          throw new Error("newline in value");
        })
        .mockImplementationOnce(() => {
          throw new Error("also broken");
        });

      await expect(
        runHostedInit({ dir, cloudSyncUrl: CLOUD_SYNC_URL, envKeys: NEXT_KEYS, apiUrl: API_URL }),
      ).resolves.toBeUndefined();

      writeHostedEnvSpy.mockRestore();
    });
  });
});
