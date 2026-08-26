/**
 * Integration tests for the todo server.
 *
 * These tests start the server programmatically with Fjall-backed storage,
 * exercise the full HTTP API, and clean up afterwards.
 */

import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { startTestJwtIssuer, type TestJwtIssuerHandle } from "jazz-tools/testing";
import { tmpdir } from "node:os";
import { mkdtempSync } from "node:fs";
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
};

function createIdentity(jwtIssuer: TestJwtIssuerHandle, userId: string): Identity {
  const token = jwtIssuer.jwtForUser(userId, {}, { issuer: EXTERNAL_ISSUER });
  const payload = JSON.parse(Buffer.from(token.split(".")[1]!, "base64url").toString("utf8"));
  expect(payload).toMatchObject({ iss: EXTERNAL_ISSUER, sub: userId });
  return { token, userId };
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
      expect(aliceTodo.owner_id).toBe(alice.userId);

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
      expect(bobTodo.owner_id).toBe(bob.userId);

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

      // This receipt is specifically about the local Fjall store surviving a
      // cold restart. The HTTP route uses an Edge read, whose immediate
      // read-your-writes behavior after reopening is intentionally tracked in
      // https://github.com/garden-co/jazz/issues/1995.
      const todos = await server2.db.all(app.todos, { tier: "local" });

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
  });
});
