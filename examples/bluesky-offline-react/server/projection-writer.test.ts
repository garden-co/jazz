import { afterEach, describe, expect, it, vi } from "vitest";
import type { Operation } from "../operations.js";
import { stableObjectId } from "./timeline.js";

afterEach(() => {
  vi.doUnmock("./jazz.js");
  vi.resetModules();
});

describe("profile projection", () => {
  it("updates a deterministic Jazz object that already exists remotely", async () => {
    const database = {
      all: vi.fn(async () => []),
      one: vi.fn(async () => null),
      upsert: vi.fn(() => {
        throw new Error("Upsert failed: object already exists: profile-id");
      }),
      update: vi.fn(),
      delete: vi.fn(),
    };
    vi.doMock("./jazz.js", () => ({ db: database }));
    const { createProjectionWriter } = await import("./projection-writer.js");

    await createProjectionWriter().projectProfile({
      did: "did:plc:viewer",
      handle: "viewer.test",
      indexedAt: "2026-07-17T08:00:00.000Z",
    });

    expect(database.update).toHaveBeenCalledWith(
      expect.anything(),
      expect.any(String),
      expect.objectContaining({ handle: "viewer.test" }),
    );
  });

  it("reports when a remote deterministic object has not reached the local cache", async () => {
    const database = {
      all: vi.fn(async () => []),
      one: vi.fn(async () => null),
      upsert: vi.fn(() => {
        throw new Error("Upsert failed: object already exists: profile-id");
      }),
      update: vi.fn(() => {
        throw new Error("Update failed: object not found: profile-id");
      }),
      delete: vi.fn(),
    };
    vi.doMock("./jazz.js", () => ({ db: database }));
    const { createProjectionWriter } = await import("./projection-writer.js");

    await expect(createProjectionWriter().projectProfile({
      did: "did:plc:viewer",
      handle: "viewer.test",
      indexedAt: "2026-07-17T08:00:00.000Z",
    })).rejects.toThrow("Update failed: object not found: profile-id");
  });

  it("does not overwrite enrichment with missing fields from a sparse source", async () => {
    const { mergeProfileProjection } = await import("./projection-writer.js");
    expect(mergeProfileProjection({
      did: "did:plc:author",
      handle: "author.test",
      displayName: "Author",
      description: null,
      avatar: null,
      indexedAt: "2026-07-16T11:00:00.000Z",
    }, {
      id: "profile-id",
      did: "did:plc:author",
      indexedAt: "2026-07-16T10:00:00.000Z",
    })).toEqual({
      did: "did:plc:author",
      handle: undefined,
      displayName: undefined,
      description: undefined,
      avatar: undefined,
      indexedAt: "2026-07-16T11:00:00.000Z",
    });
  });
});

describe("durable reaction projection", () => {
  it.each(["queued", "sent"] as const)("preserves a %s intention across writer instances until AppView confirms it", async (state) => {
    const operation: Operation = {
      id: "00000000-0000-0000-0000-000000000001",
      ownerDid: "did:plc:viewer",
      kind: "like",
      rkey: "3mlike",
      state,
      createdAt: "2026-07-16T10:00:00.000Z",
      payload: {
        subjectUri: "at://author/app.bsky.feed.post/post",
        subjectCid: "bafypost",
        active: true,
        createdAt: "2026-07-16T10:00:00.000Z",
      },
    };
    const database = {
      all: vi.fn(async () => [{ ...operation, payload: JSON.stringify(operation.payload) }]),
      one: vi.fn(async () => null),
      upsert: vi.fn(),
      update: vi.fn(),
      delete: vi.fn(),
    };
    vi.doMock("./jazz.js", () => ({ db: database }));
    const { createProjectionWriter } = await import("./projection-writer.js");
    const writer = createProjectionWriter();
    const intents = await writer.loadReactionIntents(operation.ownerDid);
    const post = {
      id: stableObjectId("bluesky-post", operation.payload.subjectUri),
      uri: operation.payload.subjectUri,
      authorDid: "did:plc:author",
      authorProfileId: "profile-id",
      text: "post",
      createdAt: operation.createdAt,
      indexedAt: operation.createdAt,
      replyCount: 0,
      likeCount: 0,
      repostCount: 0,
      state: "synced" as const,
    };

    await writer.projectViewerState(operation.ownerDid, post, {}, intents);
    expect(database.delete).not.toHaveBeenCalled();
    expect(database.upsert).not.toHaveBeenCalled();

    const restartedWriter = createProjectionWriter();
    const restoredIntents = await restartedWriter.loadReactionIntents(operation.ownerDid);
    await restartedWriter.projectViewerState(
      operation.ownerDid,
      post,
      { like: "at://viewer/app.bsky.feed.like/3mlike" },
      restoredIntents,
    );
    expect(database.delete).toHaveBeenCalledWith(expect.anything(), operation.id);
    expect(database.upsert).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({ active: true }),
      expect.anything(),
    );
  });
});
