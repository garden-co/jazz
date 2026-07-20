import { randomUUID } from "node:crypto";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createJazzContext } from "jazz-tools/backend";
import permissions from "../permissions.js";
import { app } from "../schema.js";
import { stableObjectId } from "./projection-model.js";

describe("Bluesky/Jazz bridge", () => {
  afterEach(() => {
    vi.doUnmock("./bluesky.js");
    vi.doUnmock("./jazz.js");
    vi.doUnmock("./projector.js");
    vi.doUnmock("./reconciler.js");
    vi.resetModules();
  });

  it("projects an ATProto timeline into Jazz", async () => {
    const dataDirectory = mkdtempSync(join(tmpdir(), "jazz-timeline-projection-"));
    const context = createJazzContext({
      appId: randomUUID(),
      app,
      permissions,
      driver: { type: "persistent", dataPath: join(dataDirectory, "jazz.db") },
      env: "test",
      userBranch: "main",
    });
    const authorDid = "did:plc:author";
    const viewerDid = "did:plc:viewer";
    const database = context.db();
    const post = {
      uri: `at://${authorDid}/app.bsky.feed.post/3m12345678921`,
      cid: "bafypost",
      author: { did: authorDid, handle: "author.test" },
      record: { text: "Hello", createdAt: "2026-07-16T10:00:00.000Z" },
      indexedAt: "2026-07-16T10:00:01.000Z",
    };

    vi.doMock("./jazz.js", () => ({ db: database }));
    vi.doMock("./bluesky.js", () => ({
      deleteRecord: vi.fn(),
      fetchPostThread: vi.fn(),
      fetchProfile: vi.fn(async () => ({
        did: viewerDid,
        handle: "viewer.test",
        indexedAt: post.indexedAt,
      })),
      fetchTimelineFeed: vi.fn(async () => ({ feed: [{ post }], cursor: "next" })),
      fetchViewerPosts: vi.fn(),
      OperationError: class OperationError extends Error {},
      putRecord: vi.fn(),
      recordKey: vi.fn(),
    }));

    try {
      const { projectTimelinePage } = await import("./bridge.js");
      await projectTimelinePage(viewerDid, { fetchHandler: vi.fn() });

      await vi.waitFor(async () => {
        expect(await database.one(app.profiles.where({
          id: { eq: stableObjectId("bluesky-profile", authorDid) },
        }))).toMatchObject({ handle: "author.test" });
        expect(await database.one(app.posts.where({
          id: { eq: stableObjectId("bluesky-post", post.uri) },
        }))).toMatchObject({ text: "Hello" });
      });
    } finally {
      await context.shutdown();
      rmSync(dataDirectory, { recursive: true, force: true });
    }
  });

  it("exposes only the application operations", async () => {
    vi.doMock("./jazz.js", () => ({ db: {} }));
    vi.doMock("./projector.js", () => ({
      createProjector: () => ({
        projectThread: vi.fn(),
        projectTimelinePage: vi.fn(),
      }),
    }));
    vi.doMock("./reconciler.js", () => ({
      createReconciler: () => ({ reconcileOperations: vi.fn() }),
    }));

    expect(Object.keys(await import("./bridge.js")).sort()).toEqual([
      "projectThread",
      "projectTimelinePage",
      "reconcileOperations",
    ]);
  });
});
