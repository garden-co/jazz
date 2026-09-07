import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { randomUUID } from "node:crypto";
import { createAccountManager } from "jazz-tools";
import {
  deploy,
  startLocalJazzServer,
  startTestJwtIssuer,
  type LocalJazzServerHandle,
  type TestJwtIssuerHandle,
} from "jazz-tools/testing";
import { app } from "../schema.js";
import permissions from "../permissions.js";
import {
  createServer,
  startServer,
  stopServer,
  type RunningServer,
  type Todo,
} from "../src/main.ts";

const EXTERNAL_ISSUER = "https://todo-server.example.test";

type Identity = {
  token: string;
  userId: string;
  user: string;
};

type OwnedTodo = Todo & {
  owner_id: string;
};

async function createIdentity(
  jwtIssuer: TestJwtIssuerHandle,
  upstream: LocalJazzServerHandle,
  userId: string,
): Promise<Identity> {
  const token = jwtIssuer.jwtForUser(userId, {}, { issuer: EXTERNAL_ISSUER });
  const payload = JSON.parse(Buffer.from(token.split(".")[1]!, "base64url").toString("utf8"));
  expect(payload).toMatchObject({ iss: EXTERNAL_ISSUER, sub: userId });
  let stored: string | null = null;
  const accounts = await createAccountManager({
    appId: upstream.appId,
    serverUrl: upstream.url,
    env: `todo-server-auth-${crypto.randomUUID()}`,
    store: {
      async read() {
        return stored;
      },
      async update(transform) {
        stored = transform(stored);
      },
    },
  });
  const account = await accounts.registerJWT(token);
  return { token, userId, user: account.id };
}

function authorization(identity: Identity): Record<string, string> {
  return { Authorization: `Bearer ${identity.token}` };
}

describe("Todo Server request authentication", () => {
  let server: RunningServer;
  let baseUrl: string;
  let jwtIssuer: TestJwtIssuerHandle;
  let upstream: LocalJazzServerHandle;
  let alice: Identity;
  let bob: Identity;

  beforeAll(async () => {
    jwtIssuer = await startTestJwtIssuer();
    upstream = await startLocalJazzServer({
      jwksUrl: jwtIssuer.jwksUrl,
      jwtIssuer: EXTERNAL_ISSUER,
      jwtAudience: jwtIssuer.audience,
    });
    await deploy({
      serverUrl: upstream.url,
      appId: upstream.appId,
      adminSecret: upstream.adminSecret,
      schema: app,
      permissions,
    });
    alice = await createIdentity(jwtIssuer, upstream, "todo-rest-auth-alice");
    bob = await createIdentity(jwtIssuer, upstream, "todo-rest-auth-bob");
    server = await startServer(
      await createServer(undefined, {
        jwksUrl: jwtIssuer.jwksUrl,
        appId: upstream.appId,
        serverUrl: upstream.url,
        backendSecret: upstream.backendSecret,
        adminSecret: upstream.adminSecret,
      }),
      0,
    );
    baseUrl = server.baseUrl;
  });

  afterAll(async () => {
    if (server) {
      await stopServer(server);
    }
    await jwtIssuer?.stop();
    await upstream?.stop();
  });

  const protectedRequests: Array<[string, string, RequestInit]> = [
    ["list", "/todos", {}],
    [
      "create",
      "/todos",
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ title: "must authenticate" }),
      },
    ],
    ["read", `/todos/${randomUUID()}`, {}],
    [
      "update",
      `/todos/${randomUUID()}`,
      {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ done: true }),
      },
    ],
    ["delete", `/todos/${randomUUID()}`, { method: "DELETE" }],
    ["live stream", "/todos/live", {}],
  ];

  it.each(protectedRequests)("rejects missing credentials for %s", async (_name, path, init) => {
    const response = await fetch(`${baseUrl}${path}`, init);
    expect(response.status).toBe(401);
    await response.body?.cancel();
  });

  it.each(protectedRequests)("rejects invalid credentials for %s", async (_name, path, init) => {
    const response = await fetch(`${baseUrl}${path}`, {
      ...init,
      headers: {
        ...init.headers,
        Authorization: "Bearer not-a-valid-token",
      },
    });
    expect(response.status).toBe(401);
    await response.body?.cancel();
  });

  it("rejects a valid external JWT until its identity is registered with this app", async () => {
    const token = jwtIssuer.jwtForUser("todo-rest-unregistered", {}, { issuer: EXTERNAL_ISSUER });
    const response = await fetch(`${baseUrl}/todos`, {
      headers: { Authorization: `Bearer ${token}` },
    });
    expect(response.status).toBe(401);
    await response.body?.cancel();
  });

  it("derives ownership from the credential and enforces it across CRUD", async () => {
    const createResponse = await fetch(`${baseUrl}/todos`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        ...authorization(alice),
      },
      body: JSON.stringify({
        title: "Alice private todo",
        owner_id: bob.userId,
      }),
    });
    expect(createResponse.status).toBe(201);
    const created = (await createResponse.json()) as OwnedTodo;
    expect(created.owner_id).toBe(alice.user);

    const aliceListResponse = await fetch(`${baseUrl}/todos`, {
      headers: authorization(alice),
    });
    expect(aliceListResponse.status).toBe(200);
    const aliceTodos = (await aliceListResponse.json()) as OwnedTodo[];
    expect(aliceTodos.map((todo) => todo.id)).toContain(created.id);

    const bobListResponse = await fetch(`${baseUrl}/todos`, {
      headers: authorization(bob),
    });
    expect(bobListResponse.status).toBe(200);
    const bobTodos = (await bobListResponse.json()) as OwnedTodo[];
    expect(bobTodos.map((todo) => todo.id)).not.toContain(created.id);

    const bobReadResponse = await fetch(`${baseUrl}/todos/${created.id}`, {
      headers: authorization(bob),
    });
    expect(bobReadResponse.status).toBe(404);

    const bobUpdateResponse = await fetch(`${baseUrl}/todos/${created.id}`, {
      method: "PUT",
      headers: {
        "Content-Type": "application/json",
        ...authorization(bob),
      },
      body: JSON.stringify({ done: true }),
    });
    expect(bobUpdateResponse.status).toBe(404);

    const bobDeleteResponse = await fetch(`${baseUrl}/todos/${created.id}`, {
      method: "DELETE",
      headers: authorization(bob),
    });
    expect(bobDeleteResponse.status).toBe(404);

    const aliceUpdateResponse = await fetch(`${baseUrl}/todos/${created.id}`, {
      method: "PUT",
      headers: {
        "Content-Type": "application/json",
        ...authorization(alice),
      },
      body: JSON.stringify({ done: true }),
    });
    expect(aliceUpdateResponse.status).toBe(200);
    expect(((await aliceUpdateResponse.json()) as Todo).done).toBe(true);

    const aliceDeleteResponse = await fetch(`${baseUrl}/todos/${created.id}`, {
      method: "DELETE",
      headers: authorization(alice),
    });
    expect(aliceDeleteResponse.status).toBe(204);
  });

  it("does not expose a route that impersonates a caller-selected user", async () => {
    const response = await fetch(`${baseUrl}/todos/as/${bob.userId}`, {
      headers: authorization(alice),
    });
    expect(response.status).toBe(404);
  });
});
