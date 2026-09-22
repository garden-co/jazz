import { describe, expect, it } from "vitest";
import { app } from "./fixtures/basic/schema";

describe("schema.ts TS DSL fixture", () => {
  it("serializes select metadata on generated query builders", () => {
    expect(JSON.parse(app.todos.select("title").include({ project: true })._build())).toEqual({
      table: "todos",
      conditions: [],
      includes: { project: true },
      select: ["title"],
      orderBy: [],
      hops: [],
    });
  });

  it("serializes provenance magic select metadata on generated query builders", () => {
    expect(JSON.parse(app.todos.select("title", "$createdAt", "$updatedBy")._build())).toEqual({
      table: "todos",
      conditions: [],
      includes: {},
      select: ["title", "$createdAt", "$updatedBy"],
      orderBy: [],
      hops: [],
    });
  });

  it('serializes select("*") metadata on generated query builders', () => {
    expect(JSON.parse(app.todos.select("*")._build())).toEqual({
      table: "todos",
      conditions: [],
      includes: {},
      select: ["*"],
      orderBy: [],
      hops: [],
    });
  });

  it('serializes mixed select("*", "$updatedAt") metadata on generated query builders', () => {
    expect(JSON.parse(app.todos.select("*", "$updatedAt")._build())).toEqual({
      table: "todos",
      conditions: [],
      includes: {},
      select: ["*", "$updatedAt"],
      orderBy: [],
      hops: [],
    });
  });

  it("serializes nested include builders as query objects", () => {
    expect(
      JSON.parse(app.projects.include({ todosViaProject: app.todos.select("title") })._build()),
    ).toEqual({
      table: "projects",
      conditions: [],
      includes: {
        todosViaProject: {
          table: "todos",
          conditions: [],
          includes: {},
          select: ["title"],
          orderBy: [],
          hops: [],
        },
      },
      orderBy: [],
      hops: [],
    });
  });
});
