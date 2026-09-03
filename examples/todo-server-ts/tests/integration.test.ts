/**
 * Integration tests for the todo server.
 *
 * These tests start the server programmatically with Fjall-backed storage,
 * exercise the full HTTP API, and clean up afterwards.
 */

import { describe, it, expect, beforeAll, afterAll, vi } from "vitest";
import { userIdentity } from "jazz-tools";
import { startTestJwtIssuer, type TestJwtIssuerHandle } from "jazz-tools/testing";
import { tmpdir } from "node:os";
import { mkdtempSync } from "node:fs";
import { createServer as createHttpServer } from "node:http";
import { join } from "node:path";
import {
  createServer,
  startServer,
  stopServer,
  type RunningServer,
  type Todo,
} from "../src/main.ts";
import { app } from "../schema.js";

const EXTERNAL_ISSUER = "https://todo-server.example.test";

type Identity = {
  token: string;
  userId: string;
  user: string;
};

function createIdentity(jwtIssuer: TestJwtIssuerHandle, userId: string): Identity {
  const token = jwtIssuer.jwtForUser(userId, {}, { issuer: EXTERNAL_ISSUER });
  const payload = JSON.parse(Buffer.from(token.split(".")[1]!, "base64url").toString("utf8"));
  expect(payload).toMatchObject({ iss: EXTERNAL_ISSUER, sub: userId });
  return { token, userId, user: userIdentity(EXTERNAL_ISSUER, userId) };
}
let primaryIdentity: Identity;
let jwtIssuer: TestJwtIssuerHandle;

function authenticatedFetch(
  input: string | URL,
  init: RequestInit = {},
  identity = primaryIdentity,
): Promise<Response> {
  const headers = new Headers(init.headers);
  headers.set("Authorization", `Bearer ${identity.token}`);
  return fetch(input, { ...init, headers });
}
function withTimeout<T>(promise: Promise<T>, timeoutMs: number): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    // This integration test deliberately bounds platform shutdown with a real timer.
    const timer = setTimeout(() => reject(new Error(`Timed out after ${timeoutMs}ms`)), timeoutMs);
    promise.then(
      (value) => {
        clearTimeout(timer);
        resolve(value);
      },
      (error: unknown) => {
        clearTimeout(timer);
        reject(error);
      },
    );
  });
}

describe("Todo Server Integration", () => {
  let server: RunningServer;
  let baseUrl: string;

  beforeAll(async () => {
    jwtIssuer = await startTestJwtIssuer();
    primaryIdentity = createIdentity(jwtIssuer, "todo-rest-integration");
    // Create server with Fjall-backed storage (temp directory)
    const todoServer = await createServer(undefined, { jwksUrl: jwtIssuer.jwksUrl });

    // Start on random available port
    server = await startServer(todoServer, 0);
    baseUrl = server.baseUrl;
  });

  afterAll(async () => {
    if (server) {
      await stopServer(server);
    }
    await jwtIssuer?.stop();
  });

  describe("Health Check", () => {
    it("returns healthy status", async () => {
      const res = await fetch(`${baseUrl}/health`);
      expect(res.status).toBe(200);
      const data = await res.json();
      expect(data.status).toBe("healthy");
    });
  });

  describe("CRUD Operations", () => {
    let createdTodoId: string;

    it("creates a todo", async () => {
      const res = await authenticatedFetch(`${baseUrl}/todos`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          title: "Test Todo",
          description: "A test todo item",
        }),
      });

      expect(res.status).toBe(201);
      const todo: Todo = await res.json();
      expect(todo.title).toBe("Test Todo");
      expect(todo.done).toBe(false);
      expect(todo.description).toBe("A test todo item");
      expect(todo.id).toBeDefined();

      createdTodoId = todo.id;
    });

    it("lists todos", async () => {
      const res = await authenticatedFetch(`${baseUrl}/todos`);
      expect(res.status).toBe(200);
      const todos: Todo[] = await res.json();
      expect(Array.isArray(todos)).toBe(true);

      // Should include our created todo
      const found = todos.find((t) => t.id === createdTodoId);
      expect(found).toBeDefined();
      expect(found?.title).toBe("Test Todo");
    });

    it("gets a single todo", async () => {
      const res = await authenticatedFetch(`${baseUrl}/todos/${createdTodoId}`);
      expect(res.status).toBe(200);
      const todo: Todo = await res.json();
      expect(todo.id).toBe(createdTodoId);
      expect(todo.title).toBe("Test Todo");
    });

    it("updates a todo", async () => {
      const res = await authenticatedFetch(`${baseUrl}/todos/${createdTodoId}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          done: true,
          title: "Updated Todo",
        }),
      });

      expect(res.status).toBe(200);
      const todo: Todo = await res.json();
      expect(todo.done).toBe(true);
      expect(todo.title).toBe("Updated Todo");
    });

    it("deletes a todo", async () => {
      const res = await authenticatedFetch(`${baseUrl}/todos/${createdTodoId}`, {
        method: "DELETE",
      });
      expect(res.status).toBe(204);

      // Verify it's gone
      const getRes = await authenticatedFetch(`${baseUrl}/todos/${createdTodoId}`);
      expect(getRes.status).toBe(404);
    });
  });

  describe("Error Handling", () => {
    it("rejects startup with the underlying listen error when the port is occupied", async () => {
      const occupied = createHttpServer();
      await new Promise<void>((resolve) => occupied.listen(0, resolve));
      const address = occupied.address();
      if (!address || typeof address === "string") throw new Error("expected TCP listener");

      const candidate = await createServer(undefined, { jwksUrl: jwtIssuer.jwksUrl });
      try {
        await expect(startServer(candidate, address.port)).rejects.toMatchObject({
          code: "EADDRINUSE",
        });
      } finally {
        await candidate.shutdown();
        await new Promise<void>((resolve, reject) =>
          occupied.close((error) => (error ? reject(error) : resolve())),
        );
      }
    });

    it("returns 404 for non-existent todo", async () => {
      const res = await authenticatedFetch(`${baseUrl}/todos/00000000-0000-0000-0000-000000000000`);
      expect(res.status).toBe(404);
    });

    it("returns 400 for missing title", async () => {
      const res = await authenticatedFetch(`${baseUrl}/todos`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({}),
      });
      expect(res.status).toBe(400);
    });
  });

  describe("Policy-Aware Requests", () => {
    it("filters rows by the authenticated session owner", async () => {
      const alice = createIdentity(jwtIssuer, "todo-rest-policy-alice");
      const bob = createIdentity(jwtIssuer, "todo-rest-policy-bob");
      const aliceTitle = `Alice private ${Date.now()}`;
      const bobTitle = `Bob private ${Date.now()}`;

      const createAlice = await authenticatedFetch(
        `${baseUrl}/todos`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ title: aliceTitle }),
        },
        alice,
      );
      expect(createAlice.status).toBe(201);
      const aliceTodo: Todo = await createAlice.json();
      expect(aliceTodo.owner_id).toBe(alice.user);

      const createBob = await authenticatedFetch(
        `${baseUrl}/todos`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ title: bobTitle }),
        },
        bob,
      );
      expect(createBob.status).toBe(201);
      const bobTodo: Todo = await createBob.json();
      expect(bobTodo.owner_id).toBe(bob.user);

      const aliceViewRes = await authenticatedFetch(`${baseUrl}/todos`, {}, alice);
      expect(aliceViewRes.status).toBe(200);
      const aliceView: Todo[] = await aliceViewRes.json();
      const aliceTitles = new Set(aliceView.map((todo) => todo.title));
      expect(aliceTitles.has(aliceTitle)).toBe(true);
      expect(aliceTitles.has(bobTitle)).toBe(false);

      const bobViewRes = await authenticatedFetch(`${baseUrl}/todos`, {}, bob);
      expect(bobViewRes.status).toBe(200);
      const bobView: Todo[] = await bobViewRes.json();
      const bobTitles = new Set(bobView.map((todo) => todo.title));
      expect(bobTitles.has(bobTitle)).toBe(true);
      expect(bobTitles.has(aliceTitle)).toBe(false);

      const deleteAlice = await authenticatedFetch(
        `${baseUrl}/todos/${aliceTodo.id}`,
        { method: "DELETE" },
        alice,
      );
      expect(deleteAlice.status).toBe(204);
      const deleteBob = await authenticatedFetch(
        `${baseUrl}/todos/${bobTodo.id}`,
        { method: "DELETE" },
        bob,
      );
      expect(deleteBob.status).toBe(204);
    });
  });

  describe("Persistence / Cold Start", () => {
    it("survives a server restart", async () => {
      // Use a shared data path so both server instances see the same Fjall file
      const dataDir = mkdtempSync(join(tmpdir(), "jazz-cold-start-"));
      const dbPath = join(dataDir, "jazz.db");

      // --- First boot: create some todos ---
      const server1 = await startServer(
        await createServer(dbPath, { jwksUrl: jwtIssuer.jwksUrl }),
        0,
      );

      const createRes1 = await authenticatedFetch(`${server1.baseUrl}/todos`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ title: "Survive restart", description: "persistent" }),
      });
      expect(createRes1.status).toBe(201);
      const todo1: Todo = await createRes1.json();

      const createRes2 = await authenticatedFetch(`${server1.baseUrl}/todos`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ title: "Also persist" }),
      });
      expect(createRes2.status).toBe(201);
      const todo2: Todo = await createRes2.json();

      // Flush to disk and shut down
      server1.flush();
      await stopServer(server1);

      // --- Second boot: same data path, fresh server ---
      const server2 = await startServer(
        await createServer(dbPath, { jwksUrl: jwtIssuer.jwksUrl }),
        0,
      );

      // The server's public authenticated route must be able to serve the
      // persisted current state immediately after reopening, rather than only
      // exposing it through the administrative local handle.
      const response = await authenticatedFetch(`${server2.baseUrl}/todos`);
      expect(response.status).toBe(200);
      const todos = (await response.json()) as Todo[];

      // Both todos should be present
      expect(todos.length).toBe(2);

      const found1 = todos.find((t) => t.id === todo1.id);
      expect(found1).toBeDefined();
      expect(found1!.title).toBe("Survive restart");
      expect(found1!.description).toBe("persistent");
      expect(found1!.done).toBe(false);

      const found2 = todos.find((t) => t.id === todo2.id);
      expect(found2).toBeDefined();
      expect(found2!.title).toBe("Also persist");

      await stopServer(server2);
    });

    it("returns the current value after dense update history and a restart", async () => {
      const dataDir = mkdtempSync(join(tmpdir(), "jazz-dense-history-"));
      const dbPath = join(dataDir, "jazz.db");
      const server1 = await startServer(
        await createServer(dbPath, { jwksUrl: jwtIssuer.jwksUrl }),
        0,
      );

      let todoId: string | undefined;
      try {
        const create = await authenticatedFetch(`${server1.baseUrl}/todos`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ title: "revision 0" }),
        });
        expect(create.status).toBe(201);
        todoId = ((await create.json()) as Todo).id;

        for (let revision = 1; revision <= 256; revision++) {
          const update = await authenticatedFetch(`${server1.baseUrl}/todos/${todoId}`, {
            method: "PUT",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ title: `revision ${revision}` }),
          });
          expect(update.status).toBe(200);
        }
        server1.flush();
      } finally {
        await stopServer(server1);
      }

      const server2 = await startServer(
        await createServer(dbPath, { jwksUrl: jwtIssuer.jwksUrl }),
        0,
      );
      try {
        const response = await authenticatedFetch(`${server2.baseUrl}/todos/${todoId}`);
        expect(response.status).toBe(200);
        expect((await response.json()) as Todo).toMatchObject({
          id: todoId,
          title: "revision 256",
        });
      } finally {
        await stopServer(server2);
      }
    });
  });

  describe("SSE Live Endpoint", () => {
    it("streams only the authenticated caller's todos and updates on changes", async () => {
      // Use an isolated server instance so this test has an independent persistence context.
      const sseServer = await startServer(
        await createServer(undefined, { jwksUrl: jwtIssuer.jwksUrl }),
        0,
      );
      const sseBaseUrl = sseServer.baseUrl;
      let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;
      try {
        const otherIdentity = createIdentity(jwtIssuer, "todo-rest-sse-other");
        const foreignCreate = await authenticatedFetch(
          `${sseBaseUrl}/todos`,
          {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ title: "Another user's todo" }),
          },
          otherIdentity,
        );
        expect(foreignCreate.status).toBe(201);
        const foreignTodo: Todo = await foreignCreate.json();

        // Connect to SSE endpoint
        const res = await authenticatedFetch(`${sseBaseUrl}/todos/live`);
        expect(res.status).toBe(200);
        expect(res.headers.get("content-type")).toBe("text/event-stream");

        reader = res.body!.getReader();
        const decoder = new TextDecoder();

        // Helper to read next SSE event
        async function readEvent(): Promise<Todo[]> {
          let buffer = "";
          while (true) {
            const { value, done } = await reader.read();
            if (done) throw new Error("Stream ended unexpectedly");
            buffer += decoder.decode(value, { stream: true });

            // Parse SSE format: "data: {...}\n\n"
            const eventEnd = buffer.indexOf("\n\n");
            if (eventEnd !== -1) {
              const eventData = buffer.slice(0, eventEnd);
              buffer = buffer.slice(eventEnd + 2);

              const dataLine = eventData.split("\n").find((line) => line.startsWith("data: "));
              if (dataLine) {
                return JSON.parse(dataLine.slice(6));
              }
            }
          }
        }

        // 1. Initial event should be empty list
        const initial = await readEvent();
        expect(initial).toEqual([]);

        // 2. Create a todo - should see it in next event
        const createRes = await authenticatedFetch(`${sseBaseUrl}/todos`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ title: "SSE Test Todo" }),
        });
        expect(createRes.status).toBe(201);
        const createdTodo: Todo = await createRes.json();

        const afterCreate = await readEvent();
        expect(afterCreate.length).toBe(1);
        expect(afterCreate[0].id).toBe(createdTodo.id);
        expect(afterCreate[0].title).toBe("SSE Test Todo");

        // 3. Update the todo - should see updated state
        await authenticatedFetch(`${sseBaseUrl}/todos/${createdTodo.id}`, {
          method: "PUT",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ done: true }),
        });

        const afterUpdate = await readEvent();
        expect(afterUpdate.length).toBe(1);
        expect(afterUpdate[0].done).toBe(true);

        // 4. Delete the todo - should see empty list again
        await authenticatedFetch(`${sseBaseUrl}/todos/${createdTodo.id}`, {
          method: "DELETE",
        });

        const afterDelete = await readEvent();
        expect(afterDelete).toEqual([]);

        const deleteForeign = await authenticatedFetch(
          `${sseBaseUrl}/todos/${foreignTodo.id}`,
          { method: "DELETE" },
          otherIdentity,
        );
        expect(deleteForeign.status).toBe(204);
      } finally {
        if (reader) {
          await reader.cancel().catch(() => undefined);
        }
        await stopServer(sseServer);
      }
    });
    it("rejects todo requests admitted after draining begins", async () => {
      const drainingServer = await startServer(
        await createServer(undefined, { jwksUrl: jwtIssuer.jwksUrl }),
        0,
      );
      const originalClose = drainingServer.server.close.bind(drainingServer.server);
      let closeCallback: ((error?: Error) => void) | undefined;
      drainingServer.server.close = vi.fn((callback?: (error?: Error) => void) => {
        closeCallback = callback;
        return drainingServer.server;
      }) as typeof drainingServer.server.close;
      const shutdown = stopServer(drainingServer);
      try {
        const createResponse = await fetch(`${drainingServer.baseUrl}/todos`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ title: "Too late" }),
        });
        expect(createResponse.status).toBe(503);

        const streamResponse = await fetch(`${drainingServer.baseUrl}/todos/live`);
        expect(streamResponse.status).toBe(503);
        await expect(
          drainingServer.db.all(app.todos.where({ title: "Too late" })),
        ).resolves.toEqual([]);
      } finally {
        drainingServer.server.close = originalClose as typeof drainingServer.server.close;
        await new Promise<void>((resolve, reject) => {
          originalClose((error) => {
            if (error) reject(error);
            else resolve();
          });
        });
        closeCallback?.();
        await shutdown;
      }
    });

    it("gracefully closes active SSE connections during shutdown", async () => {
      const sseServer = await startServer(
        await createServer(undefined, { jwksUrl: jwtIssuer.jwksUrl }),
        0,
      );
      const sseBaseUrl = sseServer.baseUrl;
      let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;
      let shutdown: Promise<void> | undefined;
      let succeeded = false;
      try {
        const res = await authenticatedFetch(`${sseBaseUrl}/todos/live`);
        expect(res.status).toBe(200);
        expect(res.headers.get("content-type")).toBe("text/event-stream");

        const sseReader = res.body!.getReader();
        reader = sseReader;
        let initialEvent = "";
        const decoder = new TextDecoder();
        while (!initialEvent.includes("\n\n")) {
          const { value, done } = await sseReader.read();
          expect(done).toBe(false);
          initialEvent += decoder.decode(value, { stream: true });
        }
        const dataLine = initialEvent
          .slice(0, initialEvent.indexOf("\n\n"))
          .split("\n")
          .find((line) => line.startsWith("data: "));
        expect(dataLine).toBeDefined();
        expect(JSON.parse(dataLine!.slice(6))).toEqual([]);

        const stop = stopServer(sseServer);
        shutdown = stop;
        const eof = await withTimeout(sseReader.read(), 10_000);
        expect(eof.done).toBe(true);
        await stop;
        succeeded = true;
      } finally {
        if (!succeeded) {
          await reader?.cancel().catch(() => undefined);
          await (shutdown ?? stopServer(sseServer)).catch(() => undefined);
        }
      }
    });
  });
});
