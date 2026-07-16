import { randomUUID } from "node:crypto";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createJazzContext } from "jazz-tools/backend";
import permissions from "../permissions.js";
import { app } from "../schema.js";
import { stableObjectId } from "./timeline.js";

describe("timeline projection", () => {
  afterEach(() => {
    vi.doUnmock("./bluesky.js");
    vi.doUnmock("./jazz.js");
    vi.resetModules();
  });

  it("updates rows when the same deterministic objects are projected again", async () => {
    expect(stableObjectId("bluesky-profile", "did:plc:author", "app-v1"))
      .not.toBe(stableObjectId("bluesky-profile", "did:plc:author", "app-v2"));
    const dataDirectory = mkdtempSync(join(tmpdir(), "jazz-timeline-projection-"));
    const dataPath = join(dataDirectory, "jazz.db");
    const appId = randomUUID();
    const context = createJazzContext({
      appId,
      app,
      permissions,
      driver: { type: "persistent", dataPath },
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
    const viewerProfile = {
      did: viewerDid,
      handle: "viewer.test",
      indexedAt: "2026-07-16T10:00:01.000Z",
    };

    vi.doMock("./jazz.js", () => ({ db: database }));
    vi.doMock("./bluesky.js", () => ({
      deleteRecord: vi.fn(),
      fetchPostThread: vi.fn(),
      fetchProfile: vi.fn(async () => viewerProfile),
      fetchTimelineFeed: vi.fn(async () => ({ feed: [{ post }], cursor: "next" })),
      fetchViewerPosts: vi.fn(),
      OperationError: class OperationError extends Error {},
      putRecord: vi.fn(),
      recordKey: vi.fn(),
    }));

    try {
      const { projectTimelinePage } = await import("./bridge.js");
      await projectTimelinePage(viewerDid, {} as never);
      await vi.waitFor(async () => {
        expect(await database.one(app.profiles.where({
          id: { eq: stableObjectId("bluesky-profile", authorDid) },
        }))).toMatchObject({ handle: "author.test" });
      });

      vi.spyOn(database, "one").mockResolvedValueOnce(null as never);
      vi.spyOn(database, "upsert").mockImplementationOnce(() => {
        throw new Error("object already exists");
      });

      post.author.handle = "author-updated.test";
      viewerProfile.handle = "viewer-updated.test";
      await projectTimelinePage(viewerDid, {} as never);

      await vi.waitFor(async () => {
        expect(await database.one(app.profiles.where({
          id: { eq: stableObjectId("bluesky-profile", authorDid) },
        }))).toMatchObject({ handle: "author-updated.test" });
        expect(await database.one(app.profiles.where({
          id: { eq: stableObjectId("bluesky-profile", viewerDid) },
        }))).toMatchObject({ handle: "viewer-updated.test" });
      });
    } finally {
      await context.shutdown();
      rmSync(dataDirectory, { recursive: true, force: true });
    }
  });

});
