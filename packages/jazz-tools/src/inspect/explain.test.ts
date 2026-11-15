import { describe, expect, it } from "vitest";
import { testApp, testPermissions } from "../testing/index.js";
import { policyClauses, renderPolicy } from "./protocol.js";

// `explain` numbers a read policy's clauses (the arms of its top-level `or`)
// and shows each as text a person reads.

const app = testApp({
  tables: {
    projects: { columns: { owner: "uuid", archived: "boolean" } },
    tasks: {
      columns: { title: "string", projectId: "uuid", tags: { array: "string" } },
      relations: { project: { rel: "projects", column: "projectId" } },
    },
  },
});

describe("explain's clauses", () => {
  it("splits a read policy into its top-level clauses and renders each", () => {
    const permissions = testPermissions(app, {
      tables: {
        projects: {
          read: { anyOf: ["owner = session.user", "session.claims.role = 'manager'"] },
        },
        tasks: {
          read: {
            anyOf: [
              "allowedTo.read(project)",
              "tags contains 'public' and session.claims.team in ['a', 'b']",
            ],
          },
        },
      },
    });
    const projects = policyClauses(permissions.projects?.select?.using);
    expect(projects.map(renderPolicy)).toEqual([
      "owner = session.user",
      'session.claims.role = "manager"',
    ]);
    const tasks = policyClauses(permissions.tasks?.select?.using);
    expect(tasks.map(renderPolicy)).toEqual([
      "the row projectId points to allows read",
      'tags contains "public" and session.claims.team in ["a", "b"]',
    ]);
  });

  it("has one clause for a policy that isn't an or, and none without a policy", () => {
    const permissions = testPermissions(app, {
      tables: { projects: { read: "archived = false" } },
    });
    expect(policyClauses(permissions.projects?.select?.using).map(renderPolicy)).toEqual([
      "archived = false",
    ]);
    expect(policyClauses(undefined)).toEqual([]);
    expect(renderPolicy({ type: "Not", expr: { type: "True" } })).toBe("not (always)");
  });
});
