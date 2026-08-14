import { describe, expect, it } from "vitest";
import { createJazzContext } from "../backend/create-jazz-context.js";
import { schema as s } from "../index.js";
import { createTextStore, textTableDefinitions, textTablesFromApp } from "./index.js";

const app = s.defineApp({ ...textTableDefinitions });

function setup(options: Parameters<typeof createTextStore>[2] = {}) {
  const context = createJazzContext({
    appId: `ordinary-text-${crypto.randomUUID()}`,
    app,
    permissions: {},
    driver: { type: "in-memory" },
    adminSecret: "ordinary-text-test",
    tier: "local",
  });
  const db = context.asBackend();
  const text = createTextStore(db, textTablesFromApp(app), options);
  return { context, db, text };
}

describe("ordinary-row text", () => {
  it("inserts at Unicode code-point positions without splitting scalars", async () => {
    const { context, text } = setup();
    try {
      let snapshot = await text.create("A😀C");
      snapshot = await text.insert(snapshot, 2, "é");
      snapshot = await text.insert(snapshot, 0, "🙂");
      snapshot = await text.insert(snapshot, snapshot.length, "!");

      expect(snapshot.text).toBe("🙂A😀éC!");
      expect(snapshot.length).toBe(6);
      await expect(text.read(snapshot.documentId)).resolves.toEqual(snapshot);
      await expect(text.insert(snapshot, 7, "x")).rejects.toThrow("outside 0..6");
    } finally {
      await context.shutdown();
    }
  });

  it("consolidates synchronously while old versions remain independently readable", async () => {
    const { context, db, text } = setup({ maxPatches: 2, maxPatchBytes: 1024, leafBytes: 4 });
    try {
      const initial = await text.create("abcdefgh");
      const first = await text.insert(initial, 4, "X");
      const second = await text.insert(first, 5, "Y");
      const consolidated = await text.insert(second, 6, "Z");

      expect(first.patchCount).toBe(1);
      expect(second.patchCount).toBe(2);
      expect(consolidated.patchCount).toBe(0);
      expect(consolidated.baseRoot).not.toBe(initial.baseRoot);
      await expect(text.readVersion(initial.versionId)).resolves.toMatchObject({
        text: "abcdefgh",
      });
      await expect(text.readVersion(first.versionId)).resolves.toMatchObject({ text: "abcdXefgh" });
      await expect(text.readVersion(second.versionId)).resolves.toMatchObject({
        text: "abcdXYefgh",
      });
      await expect(text.readVersion(consolidated.versionId)).resolves.toMatchObject({
        text: "abcdXYZefgh",
      });

      const document = await db.one(
        app.jazz_text_documents.where({ id: consolidated.documentId }),
        { tier: "local" },
      );
      const version = await db.one(app.jazz_text_versions.where({ id: consolidated.versionId }), {
        tier: "local",
      });
      expect(document?.current_version).toBe(consolidated.versionId);
      expect(version?.previous_version).toBe(second.versionId);
    } finally {
      await context.shutdown();
    }
  });

  it("path-copies consolidation and retains untouched ordinary rope rows", async () => {
    const { context, db, text } = setup({ maxPatches: 2, maxPatchBytes: 1024, leafBytes: 4 });
    try {
      const initialText = "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
      const initial = await text.create(initialText);
      const initialNodes = await db.all(app.jazz_text_nodes, { tier: "local" });
      const initialIds = new Set(initialNodes.map((node) => node.id));
      let snapshot = await text.insert(initial, 0, "X");
      snapshot = await text.insert(snapshot, 0, "Y");
      snapshot = await text.insert(snapshot, 0, "Z");
      expect(snapshot.patchCount).toBe(0);

      const reachable = new Set<string>();
      const visit = async (id: string): Promise<void> => {
        if (reachable.has(id)) return;
        reachable.add(id);
        const node = await db.one(app.jazz_text_nodes.where({ id }), { tier: "local" });
        if (node?.left) await visit(node.left);
        if (node?.right) await visit(node.right);
      };
      await visit(snapshot.baseRoot);

      expect([...reachable].some((id) => initialIds.has(id))).toBe(true);
      expect((await text.readVersion(snapshot.versionId)).text).toBe(`ZYX${initialText}`);
    } finally {
      await context.shutdown();
    }
  });

  it("fails closed on a missing base node instead of replaying version ancestry", async () => {
    const { context, db, text } = setup();
    try {
      const initial = await text.create("base");
      const edited = await text.insert(initial, 4, "!");
      const removed = db.delete(app.jazz_text_nodes, edited.baseRoot);
      await removed.wait({ tier: "local" });

      await expect(text.readVersion(edited.versionId)).rejects.toThrow(
        `Text rope node ${edited.baseRoot} was not found`,
      );
    } finally {
      await context.shutdown();
    }
  });

  it("rejects a root transplanted from another document", async () => {
    const { context, db, text } = setup();
    try {
      const first = await text.create("first");
      const second = await text.create("second");
      const tampered = db.update(app.jazz_text_versions, first.versionId, {
        base_root: second.baseRoot,
      });
      await tampered.wait({ tier: "local" });

      await expect(text.readVersion(first.versionId)).rejects.toThrow(
        "belongs to a different document",
      );
    } finally {
      await context.shutdown();
    }
  });

  it("enforces both frontier bounds", async () => {
    const { context, text } = setup({ maxPatches: 100, maxPatchBytes: 40 });
    try {
      const initial = await text.create("");
      const first = await text.insert(initial, 0, "12345678901234567890");
      const second = await text.insert(first, first.length, "12345678901234567890");
      expect(first.patchCount).toBe(1);
      expect(second.patchCount).toBe(0);
      expect(second.patchBytes).toBe(2);
      expect(second.text).toBe("1234567890123456789012345678901234567890");
    } finally {
      await context.shutdown();
    }
  });

  it("rejects fabricated snapshots before they can author inconsistent patches", async () => {
    const { context, text } = setup();
    try {
      const snapshot = await text.create("safe");
      expect(Object.isFrozen(snapshot)).toBe(true);
      await expect(text.insert({ ...snapshot, text: "forged" }, 1, "x")).rejects.toThrow(
        "snapshot returned by this module",
      );
    } finally {
      await context.shutdown();
    }
  });
});
