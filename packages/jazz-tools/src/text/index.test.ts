import { describe, expect, it } from "vitest";
import { createJazzContext } from "../backend/create-jazz-context.js";
import { deploy } from "../dev/catalogue.js";
import { schema as s } from "../index.js";
import { startLocalJazzServer } from "../testing/index.js";
import {
  createTextStore,
  TEXT_FRONTIER_MAX_BYTES,
  TEXT_FRONTIER_MAX_PATCHES,
  textTableDefinitions,
  textTablesFromApp,
} from "./index.js";

const app = s.defineApp({ ...textTableDefinitions });

const readInsertOnlyPermissions = s.definePermissions(app, ({ policy }) => [
  policy.jazz_text_documents.allowRead.always(),
  policy.jazz_text_documents.allowInsert.always(),
  policy.jazz_text_documents.allowUpdate.never(),
  policy.jazz_text_versions.allowRead.always(),
  policy.jazz_text_versions.allowInsert.always(),
  policy.jazz_text_nodes.allowRead.always(),
  policy.jazz_text_nodes.allowInsert.always(),
]);

const readWritePermissions = s.definePermissions(app, ({ policy }) => [
  policy.jazz_text_documents.allowRead.always(),
  policy.jazz_text_documents.allowInsert.always(),
  policy.jazz_text_documents.allowUpdate.always(),
  policy.jazz_text_versions.allowRead.always(),
  policy.jazz_text_versions.allowInsert.always(),
  policy.jazz_text_nodes.allowRead.always(),
  policy.jazz_text_nodes.allowInsert.always(),
]);

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

  it("rejects a non-string insert without poisoning the document transaction", async () => {
    const { context, db, text } = setup();
    try {
      const snapshot = await text.create("safe");
      const before = await Promise.all([
        db.all(app.jazz_text_documents, { tier: "local" }),
        db.all(app.jazz_text_versions, { tier: "local" }),
        db.all(app.jazz_text_nodes, { tier: "local" }),
      ]);

      await expect(text.insert(snapshot, 0, 0 as unknown as string)).rejects.toThrow(
        "Text insertion must be a string",
      );

      const after = await Promise.all([
        db.all(app.jazz_text_documents, { tier: "local" }),
        db.all(app.jazz_text_versions, { tier: "local" }),
        db.all(app.jazz_text_nodes, { tier: "local" }),
      ]);
      expect(after).toEqual(before);
      await expect(text.read(snapshot.documentId)).resolves.toEqual(snapshot);
    } finally {
      await context.shutdown();
    }
  });

  it("rolls back the complete edit when ordinary row permissions deny the head update", async () => {
    const appId = crypto.randomUUID();
    const adminSecret = `ordinary-text-permission-${appId}`;
    const backendSecret = `ordinary-text-backend-${appId}`;
    const server = await startLocalJazzServer({
      appId,
      adminSecret,
      backendSecret,
      inMemory: true,
    });
    await deploy({
      appId,
      serverUrl: server.url,
      adminSecret,
      schema: app,
      permissions: readInsertOnlyPermissions,
    });
    const context = createJazzContext({
      appId,
      app,
      permissions: readInsertOnlyPermissions,
      driver: { type: "memory" },
      serverUrl: server.url,
      backendSecret,
      tier: "global",
    });
    const db = context.forSession({
      user_id: "ordinary-text-writer",
      claims: {},
      authMode: "external",
    });
    const text = createTextStore(db, textTablesFromApp(app), { durability: "global" });
    try {
      await new Promise((resolve) => setTimeout(resolve, 50));
      const initial = await text.create("safe");
      const before = await Promise.all([
        db.all(app.jazz_text_documents, { tier: "global" }),
        db.all(app.jazz_text_versions, { tier: "global" }),
        db.all(app.jazz_text_nodes, { tier: "global" }),
      ]);

      await expect(text.insert(initial, 4, "!")).rejects.toMatchObject({
        code: "permission_denied",
      });

      const after = await Promise.all([
        db.all(app.jazz_text_documents, { tier: "global" }),
        db.all(app.jazz_text_versions, { tier: "global" }),
        db.all(app.jazz_text_nodes, { tier: "global" }),
      ]);
      expect(after).toEqual(before);
      await expect(text.read(initial.documentId)).resolves.toEqual(initial);
    } finally {
      await context.shutdown();
      await server.stop();
    }
  }, 20_000);

  it("syncs the ordinary current head across clients and branch writers", async () => {
    const appId = crypto.randomUUID();
    const adminSecret = `ordinary-text-branches-${appId}`;
    const backendSecret = `ordinary-text-branches-backend-${appId}`;
    const server = await startLocalJazzServer({
      appId,
      adminSecret,
      backendSecret,
      inMemory: true,
    });
    await deploy({
      appId,
      serverUrl: server.url,
      adminSecret,
      schema: app,
      permissions: readWritePermissions,
    });
    const makeClient = (userBranch: string) => {
      const context = createJazzContext({
        appId,
        app,
        permissions: readWritePermissions,
        driver: { type: "memory" },
        serverUrl: server.url,
        backendSecret,
        userBranch,
        tier: "global",
      });
      const db = context.forSession({
        user_id: "ordinary-text-writer",
        claims: {},
        authMode: "external",
      });
      return {
        context,
        text: createTextStore(db, textTablesFromApp(app), { durability: "global" }),
      };
    };
    const first = makeClient("main");
    const second = makeClient("main");
    const draft = makeClient("draft");
    try {
      const initial = await first.text.create("shared");
      const observed = await second.text.read(initial.documentId);
      expect(observed.text).toBe("shared");
      const mainEdit = await second.text.insert(observed, observed.length, "!");
      await expect(first.text.read(initial.documentId)).resolves.toMatchObject({
        versionId: mainEdit.versionId,
        text: "shared!",
      });

      const draftBase = await draft.text.read(initial.documentId);
      const draftEdit = await draft.text.insert(draftBase, draftBase.length, "?");
      await expect(draft.text.read(initial.documentId)).resolves.toMatchObject({
        versionId: draftEdit.versionId,
        text: "shared!?",
      });
      await expect(first.text.read(initial.documentId)).resolves.toMatchObject({
        versionId: draftEdit.versionId,
        text: "shared!?",
      });
    } finally {
      await Promise.all([
        first.context.shutdown(),
        second.context.shutdown(),
        draft.context.shutdown(),
      ]);
      await server.stop();
    }
  }, 20_000);

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

  it("rejects a current-version pointer transplanted from another document", async () => {
    const { context, db, text } = setup();
    try {
      const first = await text.create("first");
      const second = await text.create("second");
      const tampered = db.update(app.jazz_text_documents, first.documentId, {
        current_version: second.versionId,
      });
      await tampered.wait({ tier: "local" });

      await expect(text.read(first.documentId)).rejects.toThrow(
        "points to another document's version",
      );
    } finally {
      await context.shutdown();
    }
  });

  it("enforces both frontier bounds", async () => {
    const { context, text } = setup({
      maxPatches: TEXT_FRONTIER_MAX_PATCHES,
      maxPatchBytes: 40,
    });
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

  it("enforces format-level frontier limits on replicated rows", async () => {
    const { context, db, text } = setup({ maxPatches: 4, maxPatchBytes: 128 });
    try {
      expect(() =>
        createTextStore(db, textTablesFromApp(app), {
          maxPatches: TEXT_FRONTIER_MAX_PATCHES + 1,
        }),
      ).toThrow(`maxPatches cannot exceed ${TEXT_FRONTIER_MAX_PATCHES}`);
      expect(() =>
        createTextStore(db, textTablesFromApp(app), {
          maxPatchBytes: TEXT_FRONTIER_MAX_BYTES + 1,
        }),
      ).toThrow(`maxPatchBytes cannot exceed ${TEXT_FRONTIER_MAX_BYTES}`);

      const snapshot = await text.create("safe");
      const oversizedBytes = db.update(app.jazz_text_versions, snapshot.versionId, {
        patches: " ".repeat(TEXT_FRONTIER_MAX_BYTES + 1),
      });
      await oversizedBytes.wait({ tier: "local" });
      await expect(text.readVersion(snapshot.versionId)).rejects.toThrow(
        "exceeds the persisted byte limit",
      );

      const oversizedCount = db.update(app.jazz_text_versions, snapshot.versionId, {
        patches: JSON.stringify(
          Array.from({ length: TEXT_FRONTIER_MAX_PATCHES + 1 }, () => ({ at: 0, text: "" })),
        ),
      });
      await oversizedCount.wait({ tier: "local" });
      await expect(text.readVersion(snapshot.versionId)).rejects.toThrow(
        "exceeds the persisted count limit",
      );
    } finally {
      await context.shutdown();
    }
  });

  it("reads valid frontiers independently of local writer thresholds", async () => {
    const { context, db, text: writer } = setup({ maxPatches: 4, maxPatchBytes: 128 });
    try {
      const initial = await writer.create("base");
      const first = await writer.insert(initial, 4, "1");
      const second = await writer.insert(first, 5, "2");
      const stricterReader = createTextStore(db, textTablesFromApp(app), {
        maxPatches: 1,
        maxPatchBytes: 16,
      });

      await expect(stricterReader.readVersion(second.versionId)).resolves.toMatchObject({
        text: "base12",
        patchCount: 2,
      });
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

  it("matches a plain-string oracle across localized and scattered consolidations", async () => {
    const { context, text } = setup({ maxPatches: 7, maxPatchBytes: 120, leafBytes: 16 });
    try {
      let expected = "start🙂end";
      let snapshot = await text.create(expected);
      const retained = [{ versionId: snapshot.versionId, expected }];
      for (let edit = 0; edit < 80; edit += 1) {
        const at = (edit * 48271 + 17) % (Array.from(expected).length + 1);
        const inserted = edit % 9 === 0 ? "é" : edit % 13 === 0 ? "🚀" : "x";
        const points = Array.from(expected);
        points.splice(at, 0, inserted);
        expected = points.join("");
        snapshot = await text.insert(snapshot, at, inserted);
        expect(snapshot.text).toBe(expected);
        if (edit % 10 === 0) retained.push({ versionId: snapshot.versionId, expected });
      }
      for (const version of retained) {
        await expect(text.readVersion(version.versionId)).resolves.toMatchObject({
          text: version.expected,
        });
      }
    } finally {
      await context.shutdown();
    }
  });
});
