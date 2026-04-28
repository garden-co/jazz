import { createDb, type Db } from "../../src/runtime/db.js";
import { afterEach, beforeEach, describe, expect, expectTypeOf, it } from "vitest";
import { app } from "./fixtures/basic/schema";
import { insertProject, insertUser, uniqueDbName } from "./factories";
import { schema as s } from "../../src/index.js";

type TodoCard = {
  id: string;
  label: string;
  project: string;
  owner: string | null;
  completed: boolean;
};

type TodoCardInput = {
  label: string;
  project: string;
  owner?: string | null;
  completed?: boolean;
};

const todoCards = app.todos.transformed<TodoCard, TodoCardInput>({
  row: (todo) => ({
    id: todo.id,
    label: todo.title,
    project: todo.projectId,
    owner: todo.ownerId,
    completed: todo.done,
  }),
  insert: (card) => ({
    title: card.label,
    projectId: card.project,
    ownerId: card.owner,
    done: card.completed,
  }),
  update: (card) => ({
    title: card.label,
    projectId: card.project,
    ownerId: card.owner,
    done: card.completed,
  }),
});

describe("TS transformed row API", () => {
  let db: Db;

  beforeEach(async () => {
    db = await createDb({
      appId: "test-app",
      driver: { type: "persistent", dbName: uniqueDbName("transformed-api") },
    });
  });

  afterEach(async () => {
    await db.shutdown();
  });

  it("returns transformed rows from raw-column queries", async () => {
    const project = insertProject(db, "Launch");
    const owner = insertUser(db, "Alice");
    const { value: inserted } = db.insert(app.todos, {
      title: "Write announcement",
      projectId: project.id,
      ownerId: owner.id,
    });

    const cards = await db.all(todoCards.where({ done: false }));

    expectTypeOf(cards).toEqualTypeOf<TodoCard[]>();
    expect(cards).toEqual([
      {
        id: inserted.id,
        label: "Write announcement",
        project: project.id,
        owner: owner.id,
        completed: false,
      },
    ]);
  });

  it("accepts transformed insert and update payloads", async () => {
    const project = insertProject(db, "Roadmap");
    const owner = insertUser(db, "Bob");

    const { value: card } = db.insert(todoCards, {
      label: "Draft milestone",
      project: project.id,
      owner: owner.id,
    });

    expectTypeOf(card).toEqualTypeOf<TodoCard>();
    expect(card).toMatchObject({
      label: "Draft milestone",
      project: project.id,
      owner: owner.id,
      completed: false,
    });

    db.update(todoCards, card.id, { completed: true, label: "Publish milestone" });

    const raw = await db.one(app.todos.where({ id: card.id }));
    expect(raw).toMatchObject({
      title: "Publish milestone",
      done: true,
      projectId: project.id,
      ownerId: owner.id,
    });

    const transformed = await db.one(todoCards.where({ id: card.id }));
    expect(transformed).toMatchObject({
      id: card.id,
      label: "Publish milestone",
      completed: true,
    });
  });

  it("emits transformed rows from subscriptions", async () => {
    const project = insertProject(db, "Realtime");
    let resolveUpdate: (all: TodoCard[]) => void = () => {};
    const nextUpdate = new Promise<TodoCard[]>((resolve) => {
      resolveUpdate = resolve;
    });

    const unsubscribe = db.subscribeAll(todoCards.where({ projectId: project.id }), ({ all }) => {
      if (all.length > 0) {
        resolveUpdate(all);
      }
    });

    db.insert(todoCards, {
      label: "Notify listeners",
      project: project.id,
      completed: false,
    });

    await expect(nextUpdate).resolves.toEqual([
      expect.objectContaining({
        label: "Notify listeners",
        project: project.id,
        completed: false,
      }),
    ]);

    unsubscribe();
  });

  it("exposes transformed helper types while keeping where input raw", () => {
    expectTypeOf<s.RowOf<typeof todoCards>>().toEqualTypeOf<TodoCard>();
    expectTypeOf<s.InsertOf<typeof todoCards>>().toEqualTypeOf<TodoCardInput>();
    expectTypeOf<s.WhereOf<typeof todoCards>>().toEqualTypeOf<s.WhereOf<typeof app.todos>>();
  });
});
