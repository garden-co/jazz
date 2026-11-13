import { describe, expect, it } from "vitest";
import { definePermissions } from "../permissions/index.js";
import { schema as s } from "../schema-namespace.js";
import { policyClauses, renderPolicy } from "./protocol.js";

// `explain` numbers a read policy's clauses (the arms of its top-level `or`)
// and shows each as text a person reads.

const app = s.defineApp(
  {
    projects: s.table({ owner: s.uuid(), archived: s.boolean() }, {}),
    tasks: s.table(
      { title: s.string(), projectId: s.uuid(), tags: s.array(s.string()) },
      { project: s.rel("projects", "projectId") },
    ),
  },
  { migrations: "none" },
);

describe("explain's clauses", () => {
  it("splits a read policy into its top-level clauses and renders each", () => {
    const permissions = definePermissions(app, ({ policy, anyOf, allOf, session, allowedTo }) => {
      policy.projects.allowRead.where({ owner: session.user });
      policy.projects.allowRead.where(session.where({ "claims.role": "manager" }));
      policy.tasks.allowRead.where(
        anyOf([
          allowedTo.read("project"),
          allOf([
            { tags: { contains: "public" } },
            session.where({ "claims.team": { in: ["a", "b"] } }),
          ]),
        ]),
      );
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
    const permissions = definePermissions(app, ({ policy }) => {
      policy.projects.allowRead.where({ archived: false });
    });
    expect(policyClauses(permissions.projects?.select?.using).map(renderPolicy)).toEqual([
      "archived = false",
    ]);
    expect(policyClauses(undefined)).toEqual([]);
    expect(renderPolicy({ type: "Not", expr: { type: "True" } })).toBe("not (always)");
  });
});
