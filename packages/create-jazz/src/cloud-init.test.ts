import { chmodSync, existsSync, mkdtempSync, readFileSync, statSync, writeFileSync } from "node:fs";
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
    it("writes all four keys and redacts credentials while preserving guidance", async () => {
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

      const output = logSpy.mock.calls.map((c: unknown[]) => c.join(" ")).join("\n");
      expect(output).toContain("app-alice");
      expect(output).toContain(CLOUD_SYNC_URL);
      expect(output).toContain("https://v2.dashboard.jazz.tools");
      expect(output).not.toContain("admin-secret-alice");
      expect(output).not.toContain("backend-secret-alice");
    });

    it("redacts credentials from the deferred onLog adapter", async () => {
      vi.spyOn(cloudProvision, "provisionHostedApp").mockResolvedValue({
        appId: "app-deferred",
        adminSecret: "admin-secret-deferred",
        backendSecret: "backend-secret-deferred",
      });

      const logs: { kind: string; message: string }[] = [];
      await runHostedInit({
        dir,
        cloudSyncUrl: CLOUD_SYNC_URL,
        envKeys: NEXT_KEYS,
        apiUrl: API_URL,
        onLog: (kind, message) => logs.push({ kind, message }),
      });

      const output = logs.map(({ message }) => message).join("\n");
      expect(output).toContain("app-deferred");
      expect(output).toContain(CLOUD_SYNC_URL);
      expect(output).toContain("https://v2.dashboard.jazz.tools");
      expect(output).not.toContain("admin-secret-deferred");
      expect(output).not.toContain("backend-secret-deferred");
      expect(logSpy).not.toHaveBeenCalled();
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

    it("keeps the HTTP failure diagnostic actionable without serializing the error", async () => {
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
      expect(warnArgs.some((w: string) => w.includes("HTTP 503 provisioning error"))).toBe(true);
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

    it("does not leak credentials embedded in a network error", async () => {
      const adminSecret = "admin-secret-from-network-error";
      const backendSecret = "backend-secret-from-network-error";
      vi.spyOn(cloudProvision, "provisionHostedApp").mockRejectedValue(
        new cloudProvision.ProvisionNetworkError(
          API_URL,
          new TypeError(
            `Failed to fetch: adminSecret=${adminSecret}, backendSecret=${backendSecret}`,
          ),
        ),
      );

      await runHostedInit({
        dir,
        cloudSyncUrl: CLOUD_SYNC_URL,
        envKeys: NEXT_KEYS,
        apiUrl: API_URL,
      });

      const warnArgs = warnSpy.mock.calls.map((c: unknown[]) => c.join(" "));
      const output = warnArgs.join("\n");
      expect(output).toContain("network provisioning error");
      expect(output).not.toContain(adminSecret);
      expect(output).not.toContain(backendSecret);
    });
  });

  describe("idempotency", () => {
    it("does not short-circuit when only appId is present", async () => {
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

      expect(provisionSpy).toHaveBeenCalledOnce();
      expect(parseEnv(readEnv(dir))).toMatchObject({
        NEXT_PUBLIC_JAZZ_APP_ID: "existing-app-id",
        NEXT_PUBLIC_JAZZ_SERVER_URL: CLOUD_SYNC_URL,
        JAZZ_ADMIN_SECRET: "x",
        BACKEND_SECRET: "y",
      });
    });

    it("short-circuits only when every hosted key has a non-empty value", async () => {
      const provisionSpy = vi
        .spyOn(cloudProvision, "provisionHostedApp")
        .mockResolvedValue({ appId: "should-not-be-used", adminSecret: "x", backendSecret: "y" });

      const envPath = join(dir, ".env");
      const existing =
        "NEXT_PUBLIC_JAZZ_APP_ID=existing-app\n" +
        "NEXT_PUBLIC_JAZZ_SERVER_URL=https://existing.example.com\n" +
        "JAZZ_ADMIN_SECRET=some-secret\n" +
        "BACKEND_SECRET=some-backend-secret\n";
      writeFileSync(envPath, existing, "utf8");
      if (process.platform !== "win32") chmodSync(envPath, 0o644);

      await runHostedInit({
        dir,
        cloudSyncUrl: CLOUD_SYNC_URL,
        envKeys: NEXT_KEYS,
        apiUrl: API_URL,
      });

      expect(provisionSpy).not.toHaveBeenCalled();
      expect(readEnv(dir)).toBe(existing);
      // Windows uses ACLs rather than POSIX mode bits, so this assertion is
      // intentionally limited to platforms where chmod is the security model.
      if (process.platform !== "win32") {
        expect(statSync(envPath).mode & 0o777).toBe(0o600);
      }
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
    it("replaces an empty placeholder after a failed attempt succeeds on retry", async () => {
      const provisionSpy = vi
        .spyOn(cloudProvision, "provisionHostedApp")
        .mockRejectedValueOnce(new cloudProvision.ProvisionHttpError(API_URL, 503))
        .mockResolvedValueOnce({
          appId: "retry-app",
          adminSecret: "retry-admin",
          backendSecret: "retry-backend",
        });

      const options = {
        dir,
        cloudSyncUrl: CLOUD_SYNC_URL,
        envKeys: NEXT_KEYS,
        apiUrl: API_URL,
      };
      await runHostedInit(options);
      await runHostedInit(options);

      expect(provisionSpy).toHaveBeenCalledTimes(2);
      const values = parseEnv(readEnv(dir));
      expect(values["NEXT_PUBLIC_JAZZ_APP_ID"]).toBe("retry-app");
      expect(values["NEXT_PUBLIC_JAZZ_SERVER_URL"]).toBe(CLOUD_SYNC_URL);
      expect(values["JAZZ_ADMIN_SECRET"]).toBe("retry-admin");
      expect(values["BACKEND_SECRET"]).toBe("retry-backend");
    });

    it("retries a partial write, preserves its explicit values, and fills missing credentials", async () => {
      const abandonedAdminSecret = "admin-secret-in-interrupted-write";
      const abandonedBackendSecret = "backend-secret-in-interrupted-write";
      const retryAdminSecret = "admin-secret-after-retry";
      const retryBackendSecret = "backend-secret-after-retry";
      const provisionSpy = vi
        .spyOn(cloudProvision, "provisionHostedApp")
        .mockResolvedValueOnce({
          appId: "app-written-before-interruption",
          adminSecret: abandonedAdminSecret,
          backendSecret: abandonedBackendSecret,
        })
        .mockResolvedValueOnce({
          appId: "app-from-retry",
          adminSecret: retryAdminSecret,
          backendSecret: retryBackendSecret,
        });
      vi.spyOn(cloudEnv, "writeHostedEnv").mockImplementationOnce(() => {
        // Plant the pre-atomic failure mode: content reaches .env, then the
        // write fails before the credential lines are present.
        writeFileSync(
          join(dir, ".env"),
          "NEXT_PUBLIC_JAZZ_APP_ID=user-kept-app\n" +
            "NEXT_PUBLIC_JAZZ_SERVER_URL=https://user-kept.example.com\n",
        );
        throw new Error(`interrupted while writing BACKEND_SECRET=${abandonedBackendSecret}`);
      });
      const logs: string[] = [];
      const options = {
        dir,
        cloudSyncUrl: CLOUD_SYNC_URL,
        envKeys: NEXT_KEYS,
        apiUrl: API_URL,
        onLog: (_kind: "info" | "warn", message: string) => logs.push(message),
      };

      await runHostedInit(options);
      expect(parseEnv(readEnv(dir))).toMatchObject({
        NEXT_PUBLIC_JAZZ_APP_ID: "user-kept-app",
        NEXT_PUBLIC_JAZZ_SERVER_URL: "https://user-kept.example.com",
        JAZZ_ADMIN_SECRET: "",
        BACKEND_SECRET: "",
      });

      await runHostedInit(options);

      expect(provisionSpy).toHaveBeenCalledTimes(2);
      expect(parseEnv(readEnv(dir))).toMatchObject({
        NEXT_PUBLIC_JAZZ_APP_ID: "user-kept-app",
        NEXT_PUBLIC_JAZZ_SERVER_URL: "https://user-kept.example.com",
        JAZZ_ADMIN_SECRET: retryAdminSecret,
        BACKEND_SECRET: retryBackendSecret,
      });
      const output = logs.join("\n");
      expect(output).not.toContain(abandonedAdminSecret);
      expect(output).not.toContain(abandonedBackendSecret);
      expect(output).not.toContain(retryAdminSecret);
      expect(output).not.toContain(retryBackendSecret);
    });
  });

  describe("spinner-safe progress and logging", () => {
    it("calls onStep with a provisioning label before the HTTP request fires", async () => {
      const events: string[] = [];

      vi.spyOn(cloudProvision, "provisionHostedApp").mockImplementation(async () => {
        events.push("fetch");
        return { appId: "app-eve", adminSecret: "admin-eve", backendSecret: "backend-eve" };
      });

      await runHostedInit({
        dir,
        cloudSyncUrl: CLOUD_SYNC_URL,
        envKeys: NEXT_KEYS,
        apiUrl: API_URL,
        onStep: (label) => events.push(`step:${label}`),
      });

      const firstStepIdx = events.findIndex((e) => e.startsWith("step:"));
      const fetchIdx = events.indexOf("fetch");
      expect(firstStepIdx).toBeGreaterThanOrEqual(0);
      expect(fetchIdx).toBeGreaterThan(firstStepIdx);
      expect(events[firstStepIdx]).toMatch(/provision/i);
    });

    it("routes redacted success output through onLog and does not call console.log", async () => {
      vi.spyOn(cloudProvision, "provisionHostedApp").mockResolvedValue({
        appId: "app-frank",
        adminSecret: "admin-frank",
        backendSecret: "backend-frank",
      });

      const logs: { kind: string; message: string }[] = [];

      await runHostedInit({
        dir,
        cloudSyncUrl: CLOUD_SYNC_URL,
        envKeys: NEXT_KEYS,
        apiUrl: API_URL,
        onLog: (kind, message) => logs.push({ kind, message }),
      });

      expect(logSpy).not.toHaveBeenCalled();

      const allInfo = logs
        .filter((l) => l.kind === "info")
        .map((l) => l.message)
        .join("\n");
      expect(allInfo).toContain("app-frank");
      expect(allInfo).toContain("NEXT_PUBLIC_JAZZ_APP_ID=app-frank");
      expect(allInfo).toContain(CLOUD_SYNC_URL);
      expect(allInfo).toContain("https://v2.dashboard.jazz.tools");
      expect(allInfo).not.toContain("admin-frank");
      expect(allInfo).not.toContain("backend-frank");
    });

    it("routes failure warnings through onLog and does not call console.warn", async () => {
      vi.spyOn(cloudProvision, "provisionHostedApp").mockRejectedValue(
        new cloudProvision.ProvisionHttpError(API_URL, 503),
      );

      const logs: { kind: string; message: string }[] = [];

      await runHostedInit({
        dir,
        cloudSyncUrl: CLOUD_SYNC_URL,
        envKeys: NEXT_KEYS,
        apiUrl: API_URL,
        onLog: (kind, message) => logs.push({ kind, message }),
      });

      expect(warnSpy).not.toHaveBeenCalled();

      const warnMessages = logs.filter((l) => l.kind === "warn").map((l) => l.message);
      expect(warnMessages.some((m) => m.includes("HTTP 503 provisioning error"))).toBe(true);
    });
  });

  describe("writeHostedEnv throws (outer catch)", () => {
    it("does not serialize a write error containing a credential", async () => {
      const backendSecret = "backend-secret-from-write-error";
      vi.spyOn(cloudProvision, "provisionHostedApp").mockResolvedValue({
        appId: "app-safe-error",
        adminSecret: "admin-safe-error",
        backendSecret,
      });
      vi.spyOn(cloudEnv, "writeHostedEnv")
        .mockImplementationOnce(() => {
          throw new Error(`could not save BACKEND_SECRET=${backendSecret}`);
        })
        .mockImplementationOnce(() => {});

      await runHostedInit({
        dir,
        cloudSyncUrl: CLOUD_SYNC_URL,
        envKeys: NEXT_KEYS,
        apiUrl: API_URL,
      });

      const output = warnSpy.mock.calls.map((c: unknown[]) => c.join(" ")).join("\n");
      expect(output).toContain("init-env failed unexpectedly");
      expect(output).not.toContain(backendSecret);
    });

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
