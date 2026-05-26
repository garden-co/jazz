import { describe, expect, it } from "vitest";
import { col } from "./dsl.js";
import { definePermissions } from "./permissions/index.js";
import {
  collectMissingExplicitPolicyDiagnostics,
  normalizePermissionsForWasm,
} from "./schema-permissions.js";
import { defineApp, defineTable } from "./typed-app.js";

function collectRelationLiteralTextValues(value: unknown): string[] {
  const found: string[] = [];

  function visit(node: unknown): void {
    if (!node || typeof node !== "object") {
      return;
    }
    if (
      "Literal" in node &&
      node.Literal &&
      typeof node.Literal === "object" &&
      "type" in node.Literal &&
      node.Literal.type === "Text" &&
      "value" in node.Literal &&
      typeof node.Literal.value === "string"
    ) {
      found.push(node.Literal.value);
    }
    for (const child of Object.values(node)) {
      visit(child);
    }
  }

  visit(value);
  return found;
}

describe("normalizePermissionsForWasm", () => {
  it("encodes public permission literals into tagged wire values", () => {
    const app = defineApp({
      chats: defineTable({
        isPublic: col.boolean(),
      }),
    });

    const permissions = definePermissions(app, ({ policy }) => {
      policy.chats.allowRead.where({ isPublic: true });
    });

    const normalized = normalizePermissionsForWasm(permissions);
    const readPolicy = normalized.chats?.select?.using;

    expect(readPolicy?.type).toBe("Cmp");
    if (readPolicy?.type !== "Cmp") {
      throw new Error("expected chats.allowRead to normalize to a comparison");
    }
    expect(readPolicy.column).toBe("isPublic");
    expect(readPolicy.value.type).toBe("Literal");
    if (readPolicy.value.type !== "Literal") {
      throw new Error("expected public read policy to compare against a literal");
    }
    expect(readPolicy.value.value.type).toBe("Boolean");
    if (readPolicy.value.value.type !== "Boolean") {
      throw new Error("expected public read policy literal to normalize to a boolean");
    }
    expect(readPolicy.value.value.value).toBe(true);
    expect(normalized.chats?.insert).toBeUndefined();
    expect(normalized.chats?.update).toBeUndefined();
    expect(normalized.chats?.delete).toBeUndefined();
  });

  it("encodes nested relation literals produced by public exists queries", () => {
    const app = defineApp({
      resources: defineTable({
        title: col.string(),
      }),
      resource_access_edges: defineTable({
        resource: col.ref("resources"),
        kind: col.string(),
        grant_role: col.string(),
      }),
    });

    const permissions = definePermissions(app, ({ policy }) => {
      policy.resources.allowRead.where((resource) =>
        policy.exists(
          policy.resource_access_edges.where({
            resource: resource.id,
            kind: "individual",
            grant_role: "viewer",
          }),
        ),
      );
    });

    const normalized = normalizePermissionsForWasm(permissions);
    const relationLiteralTexts = collectRelationLiteralTextValues(
      normalized.resources?.select?.using,
    );

    expect(normalized.resources?.select?.using?.type).toBe("ExistsRel");
    expect(relationLiteralTexts).toEqual(expect.arrayContaining(["individual", "viewer"]));
  });

  it("normalizes branch-scoped table policies recursively", () => {
    const app = defineApp({
      projects: defineTable({
        title: col.string(),
      }),
      branches: defineTable({
        projectId: col.ref("projects"),
        ownerId: col.string(),
      }),
      todos: defineTable({
        projectId: col.ref("projects"),
        title: col.string(),
        done: col.boolean(),
      }),
    });

    const permissions = definePermissions(app, ({ policy }) => {
      policy.forBranch(policy.branches, ({ $branch, branchPolicy }) => {
        branchPolicy.todos.allowRead.where({ projectId: $branch.projectId });
        branchPolicy.todos.allowInsert.where({
          projectId: $branch.projectId,
          done: false,
        });
      });
    });

    const normalized = normalizePermissionsForWasm(permissions);
    const branchPolicies = normalized.todos?.for_branch?.branches;

    expect(branchPolicies?.select?.using).toEqual(
      permissions.todos?.for_branch?.branches?.select?.using,
    );
    const insertCheck = branchPolicies?.insert?.with_check;
    expect(insertCheck?.type).toBe("And");
    if (insertCheck?.type !== "And") {
      throw new Error("expected branch insert policy to normalize to a conjunction");
    }
    const doneCheck = insertCheck.exprs.find(
      (expr) => expr.type === "Cmp" && expr.column === "done",
    );
    expect(doneCheck?.type).toBe("Cmp");
    if (doneCheck?.type !== "Cmp") {
      throw new Error("expected done literal check in branch insert policy");
    }
    expect(doneCheck.value.type).toBe("Literal");
    if (doneCheck.value.type !== "Literal") {
      throw new Error("expected done check to compare against a literal");
    }
    expect(doneCheck.value.value.type).toBe("Boolean");
    if (doneCheck.value.value.type !== "Boolean") {
      throw new Error("expected done check literal to normalize to a boolean");
    }
    expect(doneCheck.value.value.value).toBe(false);
    expect(branchPolicies?.update).toBeUndefined();
    expect(branchPolicies?.delete).toBeUndefined();
  });

  it("treats branch-scoped table policies as explicit policy coverage", () => {
    const app = defineApp({
      projects: defineTable({
        title: col.string(),
      }),
      branches: defineTable({
        projectId: col.ref("projects"),
      }),
      todos: defineTable({
        projectId: col.ref("projects"),
        title: col.string(),
      }),
    });

    const permissions = definePermissions(app, ({ policy }) => {
      policy.forBranch(policy.branches, ({ $branch, branchPolicy }) => {
        branchPolicy.todos.allowRead.where({ projectId: $branch.projectId });
        branchPolicy.todos.allowInsert.where({ projectId: $branch.projectId });
        branchPolicy.todos.allowUpdate.where({ projectId: $branch.projectId });
        branchPolicy.todos.allowDelete.where({ projectId: $branch.projectId });
      });
    });

    expect(collectMissingExplicitPolicyDiagnostics(["todos"], permissions)).toEqual([]);
  });
});
