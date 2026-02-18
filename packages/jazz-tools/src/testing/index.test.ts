import { chmod, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  createTestJwt,
  expectAllowed,
  expectDenied,
  requestForClaims,
  scopedClientForClaims,
  seedRows,
  startLocalJazzServer,
} from "./index.js";

function decodeJwtPayload(token: string): Record<string, unknown> {
  const payload = token.split(".")[1];
  const base64 = payload.replace(/-/g, "+").replace(/_/g, "/");
  const padded = base64 + "=".repeat((4 - (base64.length % 4)) % 4);
  return JSON.parse(Buffer.from(padded, "base64").toString("utf8")) as Record<string, unknown>;
}

const tempRoots: string[] = [];
const managedServers: Array<{ stop(): Promise<void> }> = [];

afterEach(async () => {
  await Promise.all(
    managedServers.splice(0).map(async (server) => {
      await server.stop();
    }),
  );
  await Promise.all(
    tempRoots.splice(0).map(async (root) => {
      await rm(root, { recursive: true, force: true });
    }),
  );
});

async function createFakeJazzBin(): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), "jazz-tools-fake-bin-"));
  tempRoots.push(root);
  const scriptPath = join(root, "fake-jazz");

  const script = `#!/bin/sh
PORT="$4"
node -e '
const http = require("http");
const port = Number(process.argv[1]);
const server = http.createServer((req, res) => {
  if (req.url === "/health") {
    res.statusCode = 200;
    res.end("ok");
    return;
  }
  res.statusCode = 404;
  res.end("missing");
});
server.listen(port, "127.0.0.1");
process.on("SIGTERM", () => server.close(() => process.exit(0)));
setInterval(() => {}, 1000);
' "$PORT"
`;

  await writeFile(scriptPath, script);
  await chmod(scriptPath, 0o755);
  return scriptPath;
}

describe("testing helpers", () => {
  it("creates a request with bearer token claims", () => {
    const request = requestForClaims({
      sub: "user-123",
      claims: { role: "admin" },
    });

    const auth = (request.headers as Record<string, string>).authorization;
    expect(auth).toMatch(/^Bearer /);
    const token = auth.slice("Bearer ".length);
    expect(decodeJwtPayload(token)).toEqual({
      sub: "user-123",
      claims: { role: "admin" },
    });
  });

  it("creates a scoped client via forRequest", () => {
    const scoped = { query: vi.fn() };
    const forRequest = vi.fn((request: unknown) => {
      void request;
      return scoped;
    });
    const client = { forRequest } as unknown as Parameters<typeof scopedClientForClaims>[0];

    const result = scopedClientForClaims(client, { sub: "user-9" });

    expect(result).toBe(scoped);
    expect(forRequest).toHaveBeenCalledTimes(1);
    const firstCall = forRequest.mock.calls.at(0);
    expect(firstCall).toBeDefined();
    const request = firstCall![0] as { headers: Record<string, string> };
    expect(request.headers.authorization).toMatch(/^Bearer /);
  });

  it("supports allow/deny assertions", async () => {
    await expect(expectAllowed(async () => "ok")).resolves.toBeUndefined();
    await expect(
      expectDenied(async () => {
        throw new Error("permission denied");
      }),
    ).resolves.toBeInstanceOf(Error);

    await expect(
      expectDenied(
        async () => {
          throw new Error("denied by policy");
        },
        { match: /policy/ },
      ),
    ).resolves.toBeInstanceOf(Error);

    await expect(expectDenied(async () => "ok")).rejects.toThrow(
      "Expected operation to be denied, but it succeeded.",
    );
  });

  it("seeds rows through db.insert", async () => {
    const insert = vi.fn((_table: unknown, row: unknown) => JSON.stringify(row));
    const db = { insert } as unknown as Parameters<typeof seedRows>[0];
    const table = { _table: "todos" } as unknown as Parameters<typeof seedRows>[1];

    const ids = await seedRows(db, table, [
      { title: "a", owner_id: "u1" },
      { title: "b", owner_id: "u2" },
    ]);

    expect(insert).toHaveBeenCalledTimes(2);
    expect(ids).toEqual([
      JSON.stringify({ title: "a", owner_id: "u1" }),
      JSON.stringify({ title: "b", owner_id: "u2" }),
    ]);
  });

  it("creates unsigned JWT fixtures", () => {
    const token = createTestJwt({ sub: "user-1", claims: { org: "acme" } });
    expect(token.split(".")).toHaveLength(3);
    expect(decodeJwtPayload(token)).toEqual({
      sub: "user-1",
      claims: { org: "acme" },
    });
  });

  it("combines scoped clients with deny assertions in a realistic flow", async () => {
    const scoped = {
      query: vi.fn(async () => {
        throw new Error("denied by policy");
      }),
    };
    const forRequest = vi.fn(() => scoped);
    const client = { forRequest } as unknown as Parameters<typeof scopedClientForClaims>[0];

    const userClient = scopedClientForClaims(client, {
      sub: "user-42",
      claims: { role: "member", org: "acme" },
    });

    await expect(
      expectDenied(() => userClient.query('{"table":"todos"}'), { match: /policy/ }),
    ).resolves.toBeInstanceOf(Error);
    expect(scoped.query).toHaveBeenCalledTimes(1);
  });

  it("combines seedRows with expectAllowed", async () => {
    const inserted: Array<{ title: string; owner_id: string }> = [];
    const insert = vi.fn((_table: unknown, row: unknown) => {
      inserted.push(row as { title: string; owner_id: string });
      return `id-${inserted.length}`;
    });
    const db = { insert } as unknown as Parameters<typeof seedRows>[0];
    const table = { _table: "todos" } as unknown as Parameters<typeof seedRows>[1];

    await expect(
      expectAllowed(async () => {
        const ids = await seedRows(db, table, [
          { title: "a", owner_id: "u1" },
          { title: "b", owner_id: "u1" },
        ]);
        expect(ids).toEqual(["id-1", "id-2"]);
      }),
    ).resolves.toBeUndefined();
    expect(inserted).toEqual([
      { title: "a", owner_id: "u1" },
      { title: "b", owner_id: "u1" },
    ]);
  });

  it("starts and stops a local server process", async () => {
    const fakeJazzBin = await createFakeJazzBin();
    const server = await startLocalJazzServer({
      appId: "test-app",
      jazzBin: fakeJazzBin,
      startupTimeoutMs: 3_000,
    });

    managedServers.push(server);

    const health = await fetch(`${server.url}/health`);
    expect(health.status).toBe(200);

    await server.stop();
    managedServers.pop();
  });
});
