/**
 * Integration tests for the todo server.
 *
 * These tests start the server programmatically with SQLite-backed storage,
 * exercise the full HTTP API, and clean up afterwards.
 */

import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { randomUUID } from "node:crypto";
import { tmpdir } from "node:os";
import { mkdtempSync } from "node:fs";
import { join } from "node:path";
import { WebSocket as UndiciWebSocket } from "undici";
import { startTestJwtIssuer, type TestJwtIssuerHandle } from "jazz-tools/testing";
import { app } from "../schema.js";
import {
  createServer,
  startServer,
  stopServer,
  type RunningServer,
  type Todo,
} from "../src/main.ts";

globalThis.WebSocket ??= UndiciWebSocket as unknown as typeof globalThis.WebSocket;

describe("Todo Server Integration", () => {
  let server: RunningServer;
  let baseUrl: string;
  let issuer: TestJwtIssuerHandle;

  // Keep individual request tests on the suite default, while allowing this
  // explicit lifecycle setup to contend safely with CI workers.
  beforeAll(async () => {
    issuer = await startTestJwtIssuer();

    // Create server with temp persistent storage.
    const todoServer = await createServer({ jwksUrl: issuer.jwksUrl });

    // Start on random available port
    server = await startServer(todoServer, 0);
    baseUrl = server.baseUrl;
  }, 20_000);

  afterAll(async () => {
    if (server) {
      await stopServer(server);
    }
    await issuer?.stop();
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
      const res = await fetch(`${baseUrl}/todos`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          title: "Test Todo",
        }),
      });

      expect(res.status).toBe(201);
      const todo: Todo = await res.json();
      expect(todo.title).toBe("Test Todo");
      expect(todo.done).toBe(false);
      expect(todo.id).toBeDefined();

      createdTodoId = todo.id;
    });

    it("lists todos", async () => {
      const res = await fetch(`${baseUrl}/todos`);
      expect(res.status).toBe(200);
      const todos: Todo[] = await res.json();
      expect(Array.isArray(todos)).toBe(true);

      // Should include our created todo
      const found = todos.find((t) => t.id === createdTodoId);
      expect(found).toBeDefined();
      expect(found?.title).toBe("Test Todo");
    });

    it("gets a single todo", async () => {
      const res = await fetch(`${baseUrl}/todos/${createdTodoId}`);
      expect(res.status).toBe(200);
      const todo: Todo = await res.json();
      expect(todo.id).toBe(createdTodoId);
      expect(todo.title).toBe("Test Todo");
    });

    it("updates a todo", async () => {
      const res = await fetch(`${baseUrl}/todos/${createdTodoId}`, {
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
      const res = await fetch(`${baseUrl}/todos/${createdTodoId}`, {
        method: "DELETE",
      });
      expect(res.status).toBe(204);

      // Verify it's gone
      const getRes = await fetch(`${baseUrl}/todos/${createdTodoId}`);
      expect(getRes.status).toBe(404);
    });
  });

  describe("Error Handling", () => {
    it("returns 404 for non-existent todo", async () => {
      const res = await fetch(`${baseUrl}/todos/00000000-0000-0000-0000-000000000000`);
      expect(res.status).toBe(404);
    });

    it("returns 400 for missing title", async () => {
      const res = await fetch(`${baseUrl}/todos`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({}),
      });
      expect(res.status).toBe(400);
    });
  });

  describe("Policy-Aware Session Reads", () => {
    it("filters rows by owner_id when querying with session context", async () => {
      const aliceTitle = `Alice private ${Date.now()}`;
      const bobTitle = `Bob private ${Date.now()}`;
      const admit = async () => {
        const token = await issuer.jwtForUser(
          randomUUID(),
          {},
          { issuer: "https://todo-docs.example.test" },
        );
        // Signing in returns the account Jazz made for the identity.
        const signIn = await fetch(`${baseUrl}/sign-in`, {
          method: "POST",
          headers: { Authorization: `Bearer ${token}` },
        });
        expect(signIn.status).toBe(200);
        const { account } = (await signIn.json()) as { account: string };
        return { id: account, token };
      };
      const alice = await admit();
      const bob = await admit();
      const aliceId = alice.id;
      const bobId = bob.id;
      const unauthenticated = await fetch(`${baseUrl}/todos/as/${aliceId}`);
      expect(unauthenticated.status).toBe(401);
      const impersonation = await fetch(`${baseUrl}/todos/as/${bobId}`, {
        headers: { Authorization: `Bearer ${alice.token}` },
      });
      expect(impersonation.status).toBe(403);
      const aliceOwner = aliceId;
      const bobOwner = bobId;

      const createAlice = await fetch(`${baseUrl}/todos`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ title: aliceTitle, owner_id: aliceOwner }),
      });
      expect(createAlice.status).toBe(201);
      const aliceTodo: Todo = await createAlice.json();
      expect(aliceTodo.owner_id).toBe(aliceOwner);

      const createBob = await fetch(`${baseUrl}/todos`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ title: bobTitle, owner_id: bobOwner }),
      });
      expect(createBob.status).toBe(201);
      const bobTodo: Todo = await createBob.json();
      expect(bobTodo.owner_id).toBe(bobOwner);

      const aliceViewRes = await fetch(`${baseUrl}/todos/as/${aliceId}`, {
        headers: { Authorization: `Bearer ${alice.token}` },
      });
      expect(aliceViewRes.status).toBe(200);
      const aliceView: Todo[] = await aliceViewRes.json();
      const aliceTitles = new Set(aliceView.map((todo) => todo.title));
      expect(aliceTitles.has(aliceTitle)).toBe(true);
      expect(aliceTitles.has(bobTitle)).toBe(false);

      const bobViewRes = await fetch(`${baseUrl}/todos/as/${bobId}`, {
        headers: { Authorization: `Bearer ${bob.token}` },
      });
      expect(bobViewRes.status).toBe(200);
      const bobView: Todo[] = await bobViewRes.json();
      const bobTitles = new Set(bobView.map((todo) => todo.title));
      expect(bobTitles.has(bobTitle)).toBe(true);
      expect(bobTitles.has(aliceTitle)).toBe(false);

      const deleteAlice = await fetch(`${baseUrl}/todos/${aliceTodo.id}`, { method: "DELETE" });
      expect(deleteAlice.status).toBe(204);
      const deleteBob = await fetch(`${baseUrl}/todos/${bobTodo.id}`, { method: "DELETE" });
      expect(deleteBob.status).toBe(204);
    });
  });

  describe("Persistence / Cold Start", () => {
    it("survives a server restart", async () => {
      // Use a shared data path so both server instances see the same database
      const dbPath = mkdtempSync(join(tmpdir(), "jazz-cold-start-"));

      // --- First boot: create some todos ---
      const server1 = await startServer(await createServer({ dataPath: dbPath }), 0);

      const createRes1 = await fetch(`${server1.baseUrl}/todos`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ title: "Survive restart" }),
      });
      expect(createRes1.status).toBe(201);
      const todo1: Todo = await createRes1.json();

      const createRes2 = await fetch(`${server1.baseUrl}/todos`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ title: "Also persist" }),
      });
      expect(createRes2.status).toBe(201);
      const todo2: Todo = await createRes2.json();

      await stopServer(server1);

      // --- Second boot: same data path, fresh server ---
      const server2 = await startServer(await createServer({ dataPath: dbPath }), 0);

      const todos = await server2.db.all(app.todos);

      // Both todos should be present
      expect(todos.length).toBe(2);

      const found1 = todos.find((t) => t.id === todo1.id);
      expect(found1).toBeDefined();
      expect(found1!.title).toBe("Survive restart");
      expect(found1!.done).toBe(false);

      const found2 = todos.find((t) => t.id === todo2.id);
      expect(found2).toBeDefined();
      expect(found2!.title).toBe("Also persist");

      await stopServer(server2);
    });
  });

  describe("SSE Live Endpoint", () => {
    it("streams all todos and updates on changes", async () => {
      // Use an isolated server so this test starts from a clean state.
      const sseServer = await startServer(await createServer(), 0);
      const sseBaseUrl = sseServer.baseUrl;
      let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;
      try {
        // Connect to SSE endpoint
        const res = await fetch(`${sseBaseUrl}/todos/live`);
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
        const createRes = await fetch(`${sseBaseUrl}/todos`, {
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
        await fetch(`${sseBaseUrl}/todos/${createdTodo.id}`, {
          method: "PUT",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ done: true }),
        });

        const afterUpdate = await readEvent();
        expect(afterUpdate.length).toBe(1);
        expect(afterUpdate[0].done).toBe(true);

        // 4. Delete the todo - should see empty list again
        await fetch(`${sseBaseUrl}/todos/${createdTodo.id}`, {
          method: "DELETE",
        });

        const afterDelete = await readEvent();
        expect(afterDelete).toEqual([]);
      } finally {
        if (reader) {
          await reader.cancel().catch(() => undefined);
        }
        await stopServer(sseServer);
      }
    });
  });
});
