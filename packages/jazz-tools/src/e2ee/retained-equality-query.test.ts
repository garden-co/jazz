import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import type { Db } from "../runtime/db.js";
import { prepareEqualityQuery } from "./equality-query.js";
import { equalityValue } from "./equality-data.js";
import { decryptedIndexBytes } from "./cell-data.js";
import { retainedEqualityLimits } from "./retained-equality-limits.js";

const app = s.defineApp(
  {
    projects: s.table({ title: s.string() }, {}),
    notes: s
      .table(
        { projectId: s.uuid(), title: s.string(), rank: s.int() },
        {
          project: s.rel("projects", "projectId"),
        },
      )
      .encrypted({ space: "projectId", columns: ["title"], indexes: { title: "equality" } }),
  },
  { migrations: "none" },
);
const projectId = "00000000-0000-4000-8000-000000000001";

it("retains plaintext constraints but removes every encrypted/token predicate and caller page", async () => {
  const query = app.notes
    .where({ projectId, title: "match", rank: 7 })
    .orderBy("rank")
    .offset(3)
    .limit(1);
  const plan = await prepareEqualityQuery({} as Db, query, query._build(), undefined, true);
  expect(plan?.retainedContent).toBe(true);
  const physical = JSON.parse(plan!.json);
  const text = JSON.stringify(physical);
  expect(text).not.toContain("__e2ee_eq_");
  expect(text).not.toContain('"match"');
  expect(text).toContain("projectId");
  expect(text).toContain("rank");
  expect(physical.limit).toBe(retainedEqualityLimits.rows + 1);
  expect(physical.offset).toBeUndefined();
  plan!.dispose();
});

it("canonically filters the complete decrypted frontier before applying the caller page", async () => {
  const query = app.notes.where({ projectId, title: "match" }).offset(1).limit(1);
  const plan = await prepareEqualityQuery({} as Db, query, query._build(), undefined, true);
  const rows = ["other", "match", "match"].map((title, rank) => ({
    id: String(rank),
    projectId,
    title,
    rank,
  }));
  for (const row of rows)
    decryptedIndexBytes.set(
      row,
      new Map([["title", equalityValue(app.notes, "title", { type: "Text", value: row.title })]]),
    );
  expect(plan!.verify(rows)).toEqual([rows[2]]);
  // An unverified later candidate cannot be ignored because it is beyond page 1.
  expect(() => plan!.verify([...rows, { id: "unverified", projectId, title: "other" }])).toThrow(
    "authenticated",
  );
  plan!.dispose();
});

it("refuses projected candidate plans instead of silently dropping their semantics", async () => {
  const query = app.notes.where({ projectId, title: "match" }).select("id");
  await expect(
    prepareEqualityQuery({} as Db, query, query._build(), undefined, true),
  ).rejects.toThrow("without includes, projections");
});
