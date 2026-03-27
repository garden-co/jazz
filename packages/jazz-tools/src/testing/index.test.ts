import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { TestingServer, pushSchemaCatalogue } from "./index.js";

describe("TestingServer", () => {
  it("starts and is reachable at /health", async () => {
    const server = await TestingServer.start();
    try {
      const response = await fetch(`${server.url}/health`);
      expect(response.status).toBe(200);
    } finally {
      await server.stop();
    }
  }, 15_000);

  it("exposes appId, url, port, adminSecret, backendSecret", async () => {
    const server = await TestingServer.start();
    try {
      expect(server.appId).toEqual(expect.any(String));
      expect(server.url).toMatch(/^http:\/\/127\.0\.0\.1:\d+$/);
      expect(server.port).toEqual(expect.any(Number));
      expect(server.adminSecret).toEqual(expect.any(String));
      expect(server.backendSecret).toEqual(expect.any(String));
    } finally {
      await server.stop();
    }
  }, 15_000);

  it("respects custom adminSecret and backendSecret", async () => {
    const adminSecret = "custom-admin-secret-test";
    const backendSecret = "custom-backend-secret-test";
    const server = await TestingServer.start({ adminSecret, backendSecret });
    try {
      expect(server.adminSecret).toBe(adminSecret);
      expect(server.backendSecret).toBe(backendSecret);

      const syncBody = {
        client_id: "01234567-89ab-cdef-0123-456789abcdef",
        payloads: [
          {
            ObjectUpdated: {
              object_id: "01234567-89ab-cdef-0123-456789abcdef",
              metadata: {
                id: "01234567-89ab-cdef-0123-456789abcdef",
                metadata: { type: "catalogue_schema" },
              },
              branch_name: "main",
              commits: [],
            },
          },
        ],
      };

      const allowed = await fetch(`${server.url}/sync`, {
        method: "POST",
        headers: { "content-type": "application/json", "X-Jazz-Admin-Secret": adminSecret },
        body: JSON.stringify(syncBody),
      });
      expect(allowed.status).toBe(200);

      const denied = await fetch(`${server.url}/sync`, {
        method: "POST",
        headers: { "content-type": "application/json", "X-Jazz-Admin-Secret": "wrong-secret" },
        body: JSON.stringify(syncBody),
      });
      expect(denied.status).toBe(401);
    } finally {
      await server.stop();
    }
  }, 15_000);

  it("generates valid JWTs via jwtForUser", async () => {
    const server = await TestingServer.start();
    try {
      const token = server.jwtForUser("test-user");
      expect(typeof token).toBe("string");
      expect(token.split(".")).toHaveLength(3);
    } finally {
      await server.stop();
    }
  }, 15_000);
});

describe("pushSchemaCatalogue", () => {
  it("pushes schema catalogue via schema directory", async () => {
    const server = await TestingServer.start();
    try {
      await pushSchemaCatalogue({
        serverUrl: server.url,
        appId: server.appId,
        adminSecret: server.adminSecret,
        schemaDir: join(import.meta.dirname ?? __dirname, "fixtures/basic"),
      });
    } finally {
      await server.stop();
    }
  }, 30_000);

  it("rejects when server is unreachable", async () => {
    await expect(
      pushSchemaCatalogue({
        serverUrl: "http://127.0.0.1:9",
        appId: "00000000-0000-0000-0000-000000000001",
        adminSecret: "admin-secret",
        schemaDir: join(import.meta.dirname ?? __dirname, "fixtures/basic"),
      }),
    ).rejects.toThrow();
  }, 10_000);
});
