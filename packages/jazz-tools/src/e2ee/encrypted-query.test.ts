import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";

it.each(["read", "transaction", "subscription"])(
  "rejects unindexed encrypted null predicates through the public %s boundary",
  async (boundary) => {
    const app = s.defineApp({
      projects: s.table({ title: s.string() }, {}),
      notes: s
        .table(
          { projectId: s.uuid(), title: s.string().optional(), done: s.boolean() },
          { project: s.rel("projects", "projectId") },
        )
        .encrypted({ space: "projectId", columns: ["title"] }),
    });
    const db = await createDb(await localAccountConfig(`encrypted-filter-${crypto.randomUUID()}`));
    try {
      await expect(
        db.all(app.notes.where({ done: false }).select("id", "done"), { tier: "local" }),
      ).resolves.toEqual([]);
      const query = app.notes.where({ title: null });
      const message = "Unsupported encrypted query";
      if (boundary === "read") {
        await expect(db.all(query, { tier: "local" })).rejects.toThrow(message);
      } else if (boundary === "transaction") {
        const tx = db.beginExclusiveTransaction();
        try {
          await expect(tx.all(query, { tier: "local" })).rejects.toThrow(message);
        } finally {
          await tx.rollback();
        }
      } else {
        expect(() => {
          const stop = db.subscribe(query, () => {}, { tier: "local" });
          stop();
        }).toThrow(message);
      }
    } finally {
      await db.shutdown();
    }
  },
);

it.each(["read", "transaction", "subscription"])(
  "rejects ordering by encrypted values through the public %s boundary",
  async (boundary) => {
    const app = s.defineApp({
      projects: s.table({ title: s.string() }, {}),
      notes: s
        .table(
          { projectId: s.uuid(), title: s.string(), done: s.boolean() },
          { project: s.rel("projects", "projectId") },
        )
        .encrypted({ space: "projectId", columns: ["title"] }),
    });
    const db = await createDb(await localAccountConfig(`encrypted-order-${crypto.randomUUID()}`));
    try {
      await expect(
        db.all(app.notes.orderBy("done").select("id", "done"), { tier: "local" }),
      ).resolves.toEqual([]);
      const query = app.notes.orderBy("title");
      const message = /cannot be used in orderBy|Unsupported encrypted query/;
      if (boundary === "read") {
        await expect(db.all(query, { tier: "local" })).rejects.toThrow(message);
      } else if (boundary === "transaction") {
        const tx = db.beginExclusiveTransaction();
        try {
          await expect(tx.all(query, { tier: "local" })).rejects.toThrow(message);
        } finally {
          await tx.rollback();
        }
      } else {
        expect(() => {
          const stop = db.subscribe(query, () => {}, { tier: "local" });
          stop();
        }).toThrow(message);
      }
    } finally {
      await db.shutdown();
    }
  },
);

it.each(["read", "transaction", "subscription"])(
  "rejects unsupported encrypted predicates in includes through the public %s boundary",
  async (boundary) => {
    const app = s.defineApp({
      projects: s.table(
        { title: s.string() },
        {
          notesViaProject: s.reverse("notes", "project"),
          commentsViaProject: s.reverse("comments", "project"),
        },
      ),
      notes: s
        .table(
          { projectId: s.uuid(), title: s.string() },
          { project: s.rel("projects", "projectId") },
        )
        .encrypted({ space: "projectId", columns: ["title"] }),
      comments: s.table(
        { projectId: s.uuid(), text: s.string() },
        { project: s.rel("projects", "projectId") },
      ),
    });
    const db = await createDb(await localAccountConfig(`encrypted-query-${crypto.randomUUID()}`));
    try {
      // An encrypted declaration must not disable unrelated plaintext queries,
      // including ordinary relations, or require enrolment to run them.
      await expect(
        db.all(app.projects.include({ commentsViaProject: true }), { tier: "local" }),
      ).resolves.toEqual([]);
      const query = app.projects.include({ notesViaProject: app.notes.where({ title: "Secret" }) });
      if (boundary === "read") {
        await expect(db.all(query, { tier: "local" })).rejects.toThrow(
          "Unsupported encrypted query",
        );
      } else if (boundary === "transaction") {
        const tx = db.beginExclusiveTransaction();
        try {
          await expect(tx.all(query, { tier: "local" })).rejects.toThrow(
            "Unsupported encrypted query",
          );
        } finally {
          await tx.rollback();
        }
      } else {
        expect(() => {
          const stop = db.subscribe(query, () => {}, { tier: "local" });
          stop();
        }).toThrow("Unsupported encrypted query");
      }
    } finally {
      await db.shutdown();
    }
  },
);
