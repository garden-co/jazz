/**
 * Integration tests for the todo server.
 *
 * These tests start the server programmatically with an in-memory database,
 * exercise the full HTTP API, and clean up afterwards.
 */

import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { SqliteNodeDriver } from "jazz-ts";
import {
  createServer,
  startServer,
  stopServer,
  type RunningServer,
  type Todo,
} from "../src/main.ts";

describe("Todo Server Integration", () => {
  let server: RunningServer;
  let baseUrl: string;

  beforeAll(async () => {
    // Create server with in-memory database
    const driver = await SqliteNodeDriver.open(":memory:");
    const todoServer = await createServer(driver);

    // Start on random available port
    server = await startServer(todoServer, 0);
    baseUrl = server.baseUrl;
  });

  afterAll(async () => {
    if (server) {
      await stopServer(server);
    }
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

  describe("SSE Live Endpoint", () => {
    it("streams all todos and updates on changes", async () => {
      // Connect to SSE endpoint
      const res = await fetch(`${baseUrl}/todos/live`);
      expect(res.status).toBe(200);
      expect(res.headers.get("content-type")).toBe("text/event-stream");

      const reader = res.body!.getReader();
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
      const createRes = await fetch(`${baseUrl}/todos`, {
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
      await fetch(`${baseUrl}/todos/${createdTodo.id}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ done: true }),
      });

      const afterUpdate = await readEvent();
      expect(afterUpdate.length).toBe(1);
      expect(afterUpdate[0].done).toBe(true);

      // 4. Delete the todo - should see empty list again
      await fetch(`${baseUrl}/todos/${createdTodo.id}`, {
        method: "DELETE",
      });

      const afterDelete = await readEvent();
      expect(afterDelete).toEqual([]);

      // Clean up
      reader.cancel();
    });
  });
});
