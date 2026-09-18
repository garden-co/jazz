import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { createDb } from "../runtime/default-create-db.js";
import type { QueryBuilder } from "../runtime/db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";

const cases = ["hop", "gather", "union", "partial"].flatMap((plan) =>
  ["read", "transaction", "subscription"].map((boundary) => ({ plan, boundary })),
);

it.each(cases)(
  "rejects encrypted $plan plans through the public $boundary boundary",
  async ({ plan, boundary }) => {
    const app = s.defineApp({
      projects: s.table({ title: s.string() }, {}),
      notes: s
        .table(
          {
            projectId: s.uuid(),
            parent: s.uuid().optional(),
            title: s.string(),
          },
          { project: s.rel("projects", "projectId"), parentRelation: s.rel("notes", "parent") },
        )
        .encrypted({ space: "projectId", columns: ["title"], indexes: { title: "equality" } }),
    });
    const base = app.notes.where({ projectId: "project", title: "Wanted" });
    let query: QueryBuilder<{ id: string }>;
    if (plan === "hop") {
      query = base.hopTo("project");
    } else if (plan === "gather") {
      query = base.gather({
        step: ({ current }) => app.notes.where({ parent: current }).hopTo("parentRelation"),
        maxDepth: 1,
      });
    } else if (plan === "union") {
      query = app.union([base, app.notes.where({ projectId: "project", title: "Other" })]);
    } else {
      query = base.select({ title: { from: 0, to: 4 } });
    }
    const db = await createDb(await localAccountConfig(`encrypted-plan-${crypto.randomUUID()}`));
    try {
      if (boundary === "read") {
        await expect(db.all(query, { tier: "local" })).rejects.toThrow(
          "Unsupported encrypted query",
        );
      } else if (boundary === "transaction") {
        const tx = db.beginTransaction();
        try {
          await expect(tx.all(query, { tier: "local" })).rejects.toThrow(
            "Unsupported encrypted query",
          );
        } finally {
          await tx.rollback();
        }
      } else {
        let failure: unknown;
        let delivered = false;
        let stop: (() => void) | undefined;
        try {
          try {
            stop = db.subscribe(
              query,
              {
                onUpdate: () => {
                  delivered = true;
                },
                onError: (error) => {
                  failure = error;
                },
              },
              { tier: "local" },
            );
          } catch (error) {
            failure = error;
          }
          await expect
            .poll(() => failure, { timeout: 2_000 })
            .toMatchObject({
              message: expect.stringContaining("Unsupported encrypted query"),
            });
          expect(delivered).toBe(false);
        } finally {
          stop?.();
        }
      }
    } finally {
      await db.shutdown();
    }
  },
);
