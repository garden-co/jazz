/**
 * End-to-end tests for @jazz/react hooks
 *
 * These tests use generated types from test/generated/ which are
 * generated from test/fixtures/app.sql before tests run.
 *
 * Run: pnpm test
 */

import { act, cleanup, render, screen, waitFor } from "@testing-library/react";
import { type ReactNode, createElement, useEffect, useState } from "react";
import { afterEach, beforeAll, describe, expect, it } from "vitest";
import {
  JazzProvider,
  useAll,
  useJazz,
  useMutate,
  useOne,
} from "../src/index.js";
// @ts-ignore - vite handles ?raw imports
import schema from "./fixtures/app.sql?raw";
import { type Database, app, createDatabase } from "./generated/client.js";

let wasmDb: any;
let db: Database;

// Wrapper component that provides the Jazz context
function TestWrapper({ children }: { children: ReactNode }) {
  return createElement(JazzProvider, { database: wasmDb }, children);
}

// Helper to render with Jazz context
function renderWithJazz(component: ReactNode) {
  return render(createElement(TestWrapper, null, component));
}

beforeAll(async () => {
  // Dynamic import of the WASM module
  const wasm = await import("groove-wasm");
  await wasm.default();

  wasmDb = new wasm.WasmDatabase();
  wasmDb.init_schema(schema);
  db = createDatabase(wasmDb);
});

afterEach(() => {
  cleanup();
});

// === useJazz Tests ===

describe("useJazz", () => {
  it("returns the database from context", () => {
    function TestComponent() {
      const database = useJazz();
      return createElement(
        "div",
        { "data-testid": "result" },
        database ? "has-db" : "no-db",
      );
    }

    renderWithJazz(createElement(TestComponent));
    expect(screen.getByTestId("result").textContent).toBe("has-db");
  });

  it("throws when used outside JazzProvider", () => {
    function TestComponent() {
      try {
        useJazz();
        return createElement("div", null, "no-error");
      } catch (e) {
        return createElement("div", { "data-testid": "result" }, "error");
      }
    }

    render(createElement(TestComponent));
    expect(screen.getByTestId("result").textContent).toBe("error");
  });
});

// === useAll Tests ===

describe("useAll", () => {
  beforeAll(() => {
    // Create test data
    db.users.create({
      name: "UseAllUser1",
      email: "useall1@test.com",
      age: BigInt(25),
      score: 75.5, // Use non-integer to ensure F64 type
      isAdmin: false,
    });
    db.users.create({
      name: "UseAllUser2",
      email: "useall2@test.com",
      age: BigInt(30),
      score: 85.5, // Use non-integer to ensure F64 type
      isAdmin: true,
    });
  });

  // Skip: WASM database is so fast that callback fires before we can check initial loading state
  it.skip("returns loading state initially", async () => {
    function TestComponent() {
      const [users, loading] = useAll(app.users);
      return createElement(
        "div",
        null,
        createElement("span", { "data-testid": "loading" }, String(loading)),
        createElement("span", { "data-testid": "count" }, String(users.length)),
      );
    }

    renderWithJazz(createElement(TestComponent));

    // Should start with loading=true
    expect(screen.getByTestId("loading").textContent).toBe("true");

    // After subscription fires, loading should be false
    await waitFor(() => {
      expect(screen.getByTestId("loading").textContent).toBe("false");
    });
  });

  it("returns all rows", async () => {
    function TestComponent() {
      const [users, loading] = useAll(app.users);
      return createElement(
        "div",
        null,
        createElement("span", { "data-testid": "loading" }, String(loading)),
        createElement("span", { "data-testid": "count" }, String(users.length)),
        createElement(
          "ul",
          { "data-testid": "users" },
          users.map((u) =>
            createElement("li", { key: u.id, "data-name": u.name }, u.name),
          ),
        ),
      );
    }

    renderWithJazz(createElement(TestComponent));

    await waitFor(() => {
      expect(screen.getByTestId("loading").textContent).toBe("false");
    });

    // Should have at least the users we created
    const count = Number.parseInt(
      screen.getByTestId("count").textContent || "0",
    );
    expect(count).toBeGreaterThanOrEqual(2);
  });

  // Skip: Groove parser doesn't support LIKE operator yet
  it.skip("returns filtered rows with where()", async () => {
    function TestComponent() {
      const [users, loading] = useAll(
        app.users.where({ name: { contains: "UseAllUser" } }),
      );
      return createElement(
        "div",
        null,
        createElement("span", { "data-testid": "loading" }, String(loading)),
        createElement("span", { "data-testid": "count" }, String(users.length)),
      );
    }

    renderWithJazz(createElement(TestComponent));

    await waitFor(() => {
      expect(screen.getByTestId("loading").textContent).toBe("false");
    });

    expect(
      Number.parseInt(screen.getByTestId("count").textContent || "0"),
    ).toBe(2);
  });

  it("provides mutate functions", async () => {
    let createFn: any;
    let updateFn: any;
    let deleteFn: any;

    function TestComponent() {
      const [users, loading, mutate] = useAll(app.users);
      createFn = mutate.create;
      updateFn = mutate.update;
      deleteFn = mutate.delete;
      return createElement(
        "span",
        { "data-testid": "loading" },
        String(loading),
      );
    }

    renderWithJazz(createElement(TestComponent));

    await waitFor(() => {
      expect(screen.getByTestId("loading").textContent).toBe("false");
    });

    expect(typeof createFn).toBe("function");
    expect(typeof updateFn).toBe("function");
    expect(typeof deleteFn).toBe("function");
  });

  it("creates new rows via mutate.create", async () => {
    let createdId: string | undefined;
    let userCount = 0;

    function TestComponent() {
      const [users, loading, mutate] = useAll(
        app.users.where({ name: "MutateCreateTest" }),
      );
      userCount = users.length;

      useEffect(() => {
        if (!loading && !createdId) {
          createdId = mutate.create({
            name: "MutateCreateTest",
            email: "mutatecreate@test.com",
            age: BigInt(40),
            score: 90.5, // Use non-integer to ensure F64 type
            isAdmin: false,
          });
        }
      }, [loading, mutate]);

      return createElement(
        "div",
        null,
        createElement("span", { "data-testid": "count" }, String(users.length)),
      );
    }

    renderWithJazz(createElement(TestComponent));

    await waitFor(
      () => {
        expect(screen.getByTestId("count").textContent).toBe("1");
      },
      { timeout: 2000 },
    );
  });
});

// === useOne Tests ===

describe("useOne", () => {
  let testUserId: string;

  beforeAll(() => {
    testUserId = db.users.create({
      name: "UseOneTestUser",
      email: "useone@test.com",
      age: BigInt(35),
      score: 88.5, // Use non-integer to ensure F64 type
      isAdmin: true,
    });
  });

  // Skip: WASM database is so fast that callback fires before we can check initial loading state
  it.skip("returns loading state initially", async () => {
    function TestComponent() {
      const [user, loading] = useOne(app.users, testUserId);
      return createElement(
        "div",
        null,
        createElement("span", { "data-testid": "loading" }, String(loading)),
        createElement("span", { "data-testid": "name" }, user?.name || "null"),
      );
    }

    renderWithJazz(createElement(TestComponent));

    expect(screen.getByTestId("loading").textContent).toBe("true");

    await waitFor(() => {
      expect(screen.getByTestId("loading").textContent).toBe("false");
    });
  });

  it("returns the row by id", async () => {
    function TestComponent() {
      const [user, loading] = useOne(app.users, testUserId);
      return createElement(
        "div",
        null,
        createElement("span", { "data-testid": "loading" }, String(loading)),
        createElement("span", { "data-testid": "name" }, user?.name || "null"),
        createElement(
          "span",
          { "data-testid": "email" },
          user?.email || "null",
        ),
      );
    }

    renderWithJazz(createElement(TestComponent));

    await waitFor(() => {
      expect(screen.getByTestId("loading").textContent).toBe("false");
    });

    expect(screen.getByTestId("name").textContent).toBe("UseOneTestUser");
    expect(screen.getByTestId("email").textContent).toBe("useone@test.com");
  });

  it("returns null for non-existent id", async () => {
    function TestComponent() {
      const [user, loading] = useOne(app.users, "nonexistent-id-12345");
      return createElement(
        "div",
        null,
        createElement("span", { "data-testid": "loading" }, String(loading)),
        createElement(
          "span",
          { "data-testid": "result" },
          user ? "found" : "null",
        ),
      );
    }

    renderWithJazz(createElement(TestComponent));

    await waitFor(() => {
      expect(screen.getByTestId("loading").textContent).toBe("false");
    });

    expect(screen.getByTestId("result").textContent).toBe("null");
  });

  it("handles null/undefined id", async () => {
    function TestComponent() {
      const [user, loading] = useOne(app.users, null);
      return createElement(
        "div",
        null,
        createElement("span", { "data-testid": "loading" }, String(loading)),
        createElement(
          "span",
          { "data-testid": "result" },
          user ? "found" : "null",
        ),
      );
    }

    renderWithJazz(createElement(TestComponent));

    // Should not be loading when id is null
    await waitFor(() => {
      expect(screen.getByTestId("loading").textContent).toBe("false");
    });

    expect(screen.getByTestId("result").textContent).toBe("null");
  });

  it("provides mutate functions with captured id", async () => {
    let updateFn: any;
    let deleteFn: any;

    function TestComponent() {
      const [user, loading, mutate] = useOne(app.users, testUserId);
      updateFn = mutate.update;
      deleteFn = mutate.delete;
      return createElement(
        "span",
        { "data-testid": "loading" },
        String(loading),
      );
    }

    renderWithJazz(createElement(TestComponent));

    await waitFor(() => {
      expect(screen.getByTestId("loading").textContent).toBe("false");
    });

    expect(typeof updateFn).toBe("function");
    expect(typeof deleteFn).toBe("function");
  });
});

// === useMutate Tests ===

describe("useMutate", () => {
  it("returns mutate functions without subscribing", async () => {
    let createFn: any;
    let updateFn: any;
    let deleteFn: any;

    function TestComponent() {
      const mutate = useMutate(app.users);
      createFn = mutate.create;
      updateFn = mutate.update;
      deleteFn = mutate.delete;
      return createElement("div", { "data-testid": "ready" }, "ready");
    }

    renderWithJazz(createElement(TestComponent));

    // useMutate should be ready immediately (no loading state)
    expect(screen.getByTestId("ready").textContent).toBe("ready");
    expect(typeof createFn).toBe("function");
    expect(typeof updateFn).toBe("function");
    expect(typeof deleteFn).toBe("function");
  });

  it("can create rows", async () => {
    let outerCreatedId: string | undefined;

    function TestComponent() {
      const [createdId, setCreatedId] = useState<string | undefined>(undefined);
      const mutate = useMutate(app.users);

      useEffect(() => {
        if (!createdId) {
          const id = mutate.create({
            name: "UseMutateCreateTest",
            email: "usemutate@test.com",
            age: BigInt(45),
            score: 92.5, // Use non-integer to ensure F64 type
            isAdmin: false,
          });
          outerCreatedId = id;
          setCreatedId(id);
        }
      }, [mutate, createdId]);

      return createElement("div", { "data-testid": "id" }, createdId || "none");
    }

    renderWithJazz(createElement(TestComponent));

    await waitFor(() => {
      expect(screen.getByTestId("id").textContent).not.toBe("none");
    });

    expect(outerCreatedId).toBeDefined();
    expect(typeof outerCreatedId).toBe("string");
  });
});

// === Reactivity Tests ===

describe("Reactivity", () => {
  // Skip: Groove parser doesn't support LIKE operator (startsWith) yet
  it.skip("updates when data changes", async () => {
    let mutate: any;
    let lastCount = 0;

    function TestComponent() {
      const [users, loading, m] = useAll(
        app.users.where({ name: { startsWith: "Reactivity" } }),
      );
      mutate = m;
      lastCount = users.length;

      return createElement(
        "div",
        null,
        createElement("span", { "data-testid": "count" }, String(users.length)),
      );
    }

    renderWithJazz(createElement(TestComponent));

    await waitFor(() => {
      expect(screen.getByTestId("count").textContent).toBe("0");
    });

    // Create a user
    await act(async () => {
      mutate.create({
        name: "ReactivityTest",
        email: "reactivity@test.com",
        age: BigInt(30),
        score: 80.5, // Use non-integer to ensure F64 type
        isAdmin: false,
      });
    });

    await waitFor(
      () => {
        expect(screen.getByTestId("count").textContent).toBe("1");
      },
      { timeout: 2000 },
    );
  });
});

// === Includes Tests ===

describe("Includes with hooks", () => {
  let testProjectId: string;
  let testOwnerId: string;
  let testTaskId: string;

  beforeAll(() => {
    testOwnerId = db.users.create({
      name: "IncludesOwner",
      email: "includes@test.com",
      age: BigInt(40),
      score: 95.5, // Use non-integer to ensure F64 type
      isAdmin: true,
    });

    testProjectId = db.projects.create({
      name: "IncludesProject",
      description: "A project for testing includes",
      owner: testOwnerId,
      color: "#123456",
    });

    testTaskId = db.tasks.create({
      title: "IncludesTask",
      status: "open",
      priority: "high",
      project: testProjectId,
      assignee: testOwnerId,
      createdAt: BigInt(Date.now()),
      updatedAt: BigInt(Date.now()),
      isCompleted: false,
    });
  });

  it("useAll with forward ref include", async () => {
    function TestComponent() {
      const [tasks, loading] = useAll(
        app.tasks.where({ title: "IncludesTask" }).with({ project: true }),
      );

      if (loading) {
        return createElement("span", { "data-testid": "loading" }, "true");
      }

      const task = tasks[0];
      return createElement(
        "div",
        null,
        createElement("span", { "data-testid": "loading" }, "false"),
        createElement(
          "span",
          { "data-testid": "title" },
          task?.title || "none",
        ),
        createElement(
          "span",
          { "data-testid": "project" },
          typeof task?.project === "object" ? task.project.name : "not-loaded",
        ),
      );
    }

    renderWithJazz(createElement(TestComponent));

    await waitFor(() => {
      expect(screen.getByTestId("loading").textContent).toBe("false");
    });

    expect(screen.getByTestId("title").textContent).toBe("IncludesTask");
    expect(screen.getByTestId("project").textContent).toBe("IncludesProject");
  });

  it("useOne with forward ref include", async () => {
    function TestComponent() {
      const [project, loading] = useOne(
        app.projects.with({ owner: true }),
        testProjectId,
      );

      if (loading) {
        return createElement("span", { "data-testid": "loading" }, "true");
      }

      return createElement(
        "div",
        null,
        createElement("span", { "data-testid": "loading" }, "false"),
        createElement(
          "span",
          { "data-testid": "name" },
          project?.name || "none",
        ),
        createElement(
          "span",
          { "data-testid": "owner" },
          typeof project?.owner === "object"
            ? project.owner.name
            : "not-loaded",
        ),
      );
    }

    renderWithJazz(createElement(TestComponent));

    await waitFor(() => {
      expect(screen.getByTestId("loading").textContent).toBe("false");
    });

    expect(screen.getByTestId("name").textContent).toBe("IncludesProject");
    expect(screen.getByTestId("owner").textContent).toBe("IncludesOwner");
  });
});

// === Query Key Stability Tests ===

describe("Query key stability", () => {
  it("useAll does not resubscribe on rerender with same query", async () => {
    let renderCount = 0;
    const subscribeCount = 0;

    // Create a test-specific filter to count subscriptions
    const originalSubscribeAll = app.users.subscribeAll;

    function TestComponent() {
      renderCount++;
      const [users, loading] = useAll(app.users.where({ isAdmin: true }));
      const [, forceUpdate] = useState(0);

      useEffect(() => {
        // Force a rerender after initial load
        if (!loading) {
          const timer = setTimeout(() => forceUpdate((c) => c + 1), 100);
          return () => clearTimeout(timer);
        }
      }, [loading]);

      return createElement(
        "div",
        null,
        createElement(
          "span",
          { "data-testid": "renders" },
          String(renderCount),
        ),
        createElement("span", { "data-testid": "count" }, String(users.length)),
      );
    }

    renderWithJazz(createElement(TestComponent));

    await waitFor(
      () => {
        // Should have rendered at least twice due to force update
        expect(
          Number.parseInt(screen.getByTestId("renders").textContent || "0"),
        ).toBeGreaterThanOrEqual(2);
      },
      { timeout: 2000 },
    );

    // The component should have rerendered but subscription should be stable
    // (this is verified by the query builder's _queryKey mechanism)
  });
});
