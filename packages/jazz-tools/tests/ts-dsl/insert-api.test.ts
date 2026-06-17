import { createDb, type Db } from "../../src/runtime/db.js";
import { afterEach, beforeEach, describe, expect, expectTypeOf, it } from "vitest";
import { app } from "./fixtures/basic/schema";
import { insertProject, insertUser, uniqueDbName } from "./factories";

describe("TS Insert API", () => {
  let db: Db;

  beforeEach(async () => {
    db = await createDb({
      appId: "test-app",
      driver: { type: "persistent", dbName: uniqueDbName("insert-row-shape") },
    });
  });

  afterEach(async () => {
    await db.shutdown();
  });

  it("returns the inserted row", async () => {
    const { value: project } = db.insert(app.projects, { name: "Test Project" });
    const owner = insertUser(db);

    expect(project).toEqual({
      id: expect.any(String),
      name: "Test Project",
    });

    const { value: todo } = db.insert(app.todos, {
      title: "Test Todo",
      done: true,
      tags: ["tag1", "tag2"],
      projectId: project.id,
      ownerId: owner.id,
      assigneesIds: [],
    });

    expect(todo).toEqual({
      id: expect.any(String),
      title: "Test Todo",
      done: true,
      tags: ["tag1", "tag2"],
      projectId: project.id,
      ownerId: owner.id,
      assigneesIds: [],
    });
  });

  it("can wait for row to be persisted up to a specific durability tier", async () => {
    const project = await db.insert(app.projects, { name: "Test Project" }).wait({ tier: "local" });
    expect(project).toEqual({
      id: expect.any(String),
      name: "Test Project",
    });
    const owner = insertUser(db);

    const todo = await db
      .insert(app.todos, {
        title: "Test Todo",
        done: true,
        tags: ["tag1", "tag2"],
        projectId: project.id,
        ownerId: owner.id,
        assigneesIds: [],
      })
      .wait({ tier: "local" });
    expect(todo).toEqual({
      id: expect.any(String),
      title: "Test Todo",
      done: true,
      tags: ["tag1", "tag2"],
      projectId: project.id,
      ownerId: owner.id,
      assigneesIds: [],
    });
  });

  it("can provide an id on insert", async () => {
    const id = "00000000-0000-0000-0000-000000000000";
    const { value: project } = db.insert(app.projects, { name: "Test Project" }, { id });
    expect(project.id).toEqual(id);
  });

  it("can use caller-supplied updatedAt on insert", async () => {
    const updatedAt = 1_704_067_200_123_000;
    const { value: project } = db.insert(
      app.projects,
      { name: "Backfilled Project" },
      { updatedAt },
    );

    const projected = await db.one(
      app.projects.select("name", "$updatedAt").where({ id: { eq: project.id } }),
    );

    expect(projected).toEqual({
      id: project.id,
      name: "Backfilled Project",
      $updatedAt: new Date(Math.trunc(updatedAt / 1_000)),
    });
  });

  it("cannot insert two rows with the same id", async () => {
    const id = "00000000-0000-0000-0000-000000000000";
    const { value: project } = db.insert(app.projects, { name: "Test Project 1" }, { id });
    expect(() => db.insert(app.projects, { name: "Test Project 2" }, { id: project.id })).toThrow(
      `Insert failed: WriteError("encoding error: object already exists: ${project.id}")`,
    );
  });

  it("keeps caller-supplied ids reserved after the row is deleted", async () => {
    const id = "00000000-0000-0000-0000-000000000000";
    const { value: project } = db.insert(app.projects, { name: "Test Project 1" }, { id });
    db.delete(app.projects, project.id);

    expect(() => db.insert(app.projects, { name: "Test Project 2" }, { id: project.id })).toThrow(
      `Insert failed: WriteError("row already deleted: ${project.id}")`,
    );
  });

  it("uses default values missing from the insert data", async () => {
    const project = insertProject(db);
    const owner = insertUser(db);
    const { value: todo } = db.insert(app.todos, {
      title: "Test Todo",
      projectId: project.id,
      ownerId: owner.id,
    });

    expect(todo).toEqual({
      id: todo.id,
      title: "Test Todo",
      projectId: project.id,
      ownerId: owner.id,
      done: false,
      tags: [],
      assigneesIds: [],
    });
  });

  it("support schema defaults for all data types", async () => {
    const { value: rowWithDefaults } = db.insert(app.table_with_defaults, {});

    expectTypeOf(rowWithDefaults.timestampDate).toEqualTypeOf<Date>();
    expectTypeOf(rowWithDefaults.timestampNumber).toEqualTypeOf<Date>();
    expect(rowWithDefaults).toEqual({
      id: expect.any(String),
      integer: 1,
      float: 1,
      bytes: new Uint8Array([0, 1, 255]),
      enum: "a",
      json: { name: "default name" },
      timestampDate: new Date("2026-01-01"),
      timestampNumber: new Date(0),
      string: "default value",
      array: ["a", "b", "c"],
      boolean: true,
      nullable: null,
      nullableInteger: null,
      refId: "00000000-0000-0000-0000-000000000000",
    });
  });

  it("enforces constraints on JSON schemas", async () => {
    expect(() => db.insert(app.table_with_constraints, { data: { string: "" } })).toThrow(
      'Insert failed: WriteError("encoding error: JSON schema validation failed for column `data`: \\"\\" is shorter than 1 character")',
    );

    expect(() =>
      db.insert(app.table_with_constraints, { data: { string: "01234567890" } }),
    ).toThrow(
      'Insert failed: WriteError("encoding error: JSON schema validation failed for column `data`: \\"01234567890\\" is longer than 10 characters")',
    );

    expect(() => db.insert(app.table_with_constraints, { data: { integer: -1 } })).toThrow(
      'Insert failed: WriteError("encoding error: JSON schema validation failed for column `data`: -1 is less than the minimum of 0")',
    );

    expect(() => db.insert(app.table_with_constraints, { data: { integer: 11 } })).toThrow(
      'Insert failed: WriteError("encoding error: JSON schema validation failed for column `data`: 11 is greater than the maximum of 10")',
    );

    expect(() =>
      db.insert(app.table_with_constraints, { data: { datetime: "2020-01-01T06:15:00" } }),
    ).toThrow(
      'Insert failed: WriteError("encoding error: JSON schema validation failed for column `data`: \\"2020-01-01T06:15:00\\" does not match \\"^(?:(?:[0-9][0-9][2468][048]|[0-9][0-9][13579][26]|[0-9][0-9]0[48]|[02468][048]00|[13579][26]00)-02-29|[0-9]{4}-(?:(?:0[13578]|1[02])-(?:0[1-9]|[12][0-9]|3[01])|(?:0[469]|11)-(?:0[1-9]|[12][0-9]|30)|(?:02)-(?:0[1-9]|1[0-9]|2[0-8])))T(?:(?:[01][0-9]|2[0-3]):[0-5][0-9](?::[0-5][0-9](?:\\\\.[0-9]+)?)?(?:Z))$\\"")',
    );
  });

  it("allows explicit null for nullable fields without triggering defaults", async () => {
    const { value: rowWithDefaults } = db.insert(app.table_with_defaults, {
      nullable: null,
      refId: null,
    });

    expect(rowWithDefaults).toEqual({
      id: expect.any(String),
      integer: 1,
      float: 1,
      bytes: new Uint8Array([0, 1, 255]),
      enum: "a",
      json: { name: "default name" },
      timestampDate: new Date("2026-01-01"),
      timestampNumber: new Date(0),
      string: "default value",
      array: ["a", "b", "c"],
      boolean: true,
      nullable: null,
      nullableInteger: null,
      refId: null,
    });
  });
});
