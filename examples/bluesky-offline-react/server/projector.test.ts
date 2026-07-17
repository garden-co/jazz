import { describe, expect, it, vi } from "vitest";
import { createProjector } from "./projector.js";

describe("timeline projector", () => {
  it("projects the signed-in profile without waiting for the timeline fetch", async () => {
    const profile = {
      did: "did:plc:viewer",
      handle: "viewer.test",
    };
    const projectProfile = vi.fn(async () => undefined);
    const projector = createProjector({
      fetchTimelineFeed: vi.fn(() => new Promise(() => undefined)),
      fetchPostThread: vi.fn(),
      fetchProfile: vi.fn(async () => profile),
      writer: {
        loadReactionIntents: vi.fn(async () => new Map()),
        projectProfile,
        projectTimelinePage: vi.fn(async () => undefined),
        projectThread: vi.fn(),
      },
    });

    projector.projectTimelinePage("did:plc:viewer", { fetchHandler: vi.fn() });
    await Promise.resolve();
    await Promise.resolve();

    expect(projectProfile).toHaveBeenCalledWith(profile);
  });

  it("does not let pagination swallow a fresh head projection", async () => {
    const releases = new Map<string, (value: { feed: never[]; cursor?: string }) => void>();
    const fetchTimelineFeed = vi.fn((_session: unknown, cursor?: string) =>
      new Promise<{ feed: never[]; cursor?: string }>((resolve) => {
        releases.set(cursor ?? "head", resolve);
      }));
    const projector = createProjector({
      fetchTimelineFeed,
      fetchPostThread: vi.fn(),
      fetchProfile: vi.fn(),
      writer: {
        loadReactionIntents: vi.fn(async () => new Map()),
        projectProfile: vi.fn(async () => undefined),
        projectTimelinePage: vi.fn(async () => undefined),
        projectThread: vi.fn(),
      },
    });
    const session = { fetchHandler: vi.fn() };

    const pagination = projector.projectTimelinePage("did:plc:viewer", session, "older");
    const refresh = projector.projectTimelinePage("did:plc:viewer", session);

    expect(fetchTimelineFeed).toHaveBeenCalledTimes(2);
    releases.get("older")?.({ feed: [], cursor: "oldest" });
    releases.get("head")?.({ feed: [], cursor: "newer" });
    await expect(pagination).resolves.toMatchObject({ cursor: "oldest" });
    await expect(refresh).resolves.toMatchObject({ cursor: "newer" });
  });

  it("shares one in-flight fetch and exposes projection completion", async () => {
    let releaseFetch!: (value: { feed: never[]; cursor: string }) => void;
    const fetchTimelineFeed = vi.fn(() => new Promise<{ feed: never[]; cursor: string }>((resolve) => {
      releaseFetch = resolve;
    }));
    let releaseProjection!: () => void;
    const projectTimelinePage = vi.fn(() => new Promise<void>((resolve) => {
      releaseProjection = resolve;
    }));
    const projector = createProjector({
      fetchTimelineFeed,
      fetchPostThread: vi.fn(),
      fetchProfile: vi.fn(),
      writer: {
        loadReactionIntents: vi.fn(async () => new Map()),
        projectProfile: vi.fn(async () => undefined),
        projectTimelinePage,
        projectThread: vi.fn(),
      },
    });
    const session = { fetchHandler: vi.fn() };

    const first = projector.projectTimelinePage("did:plc:viewer", session);
    const second = projector.projectTimelinePage("did:plc:viewer", session);
    expect(fetchTimelineFeed).toHaveBeenCalledTimes(1);

    releaseFetch({ feed: [], cursor: "next" });
    const [firstResult, secondResult] = await Promise.all([first, second]);
    expect(firstResult.projection.state).toBe("accepted");
    expect(secondResult.projection.state).toBe("running");
    expect(secondResult.projection.id).toBe(firstResult.projection.id);
    expect(projector.getTimelineProjectionStatus("did:plc:viewer")).toMatchObject({ state: "projecting" });

    releaseProjection();
    await projector.waitForTimelineProjection("did:plc:viewer");
    expect(projector.getTimelineProjectionStatus("did:plc:viewer")).toMatchObject({ state: "completed" });
  });

  it("retains projection failures for inspection", async () => {
    const projector = createProjector({
      fetchTimelineFeed: vi.fn(async () => ({ feed: [], cursor: undefined })),
      fetchPostThread: vi.fn(),
      fetchProfile: vi.fn(),
      writer: {
        loadReactionIntents: vi.fn(async () => new Map()),
        projectProfile: vi.fn(async () => undefined),
        projectTimelinePage: vi.fn(async () => { throw new Error("projection exploded"); }),
        projectThread: vi.fn(),
      },
    });

    await projector.projectTimelinePage("did:plc:viewer", { fetchHandler: vi.fn() });
    await projector.waitForTimelineProjection("did:plc:viewer");

    expect(projector.getTimelineProjectionStatus("did:plc:viewer")).toMatchObject({
      state: "failed",
      error: "projection exploded",
    });
  });

  it("does not overwrite an early profile failure when the timeline fetch finishes later", async () => {
    let releaseTimeline!: (value: { feed: never[] }) => void;
    const projector = createProjector({
      fetchTimelineFeed: vi.fn(() => new Promise<{ feed: never[] }>((resolve) => {
        releaseTimeline = resolve;
      })),
      fetchPostThread: vi.fn(),
      fetchProfile: vi.fn(async () => ({ did: "did:plc:viewer", handle: "viewer.test" })),
      writer: {
        loadReactionIntents: vi.fn(async () => new Map()),
        projectProfile: vi.fn(async () => { throw new Error("profile projection failed"); }),
        projectTimelinePage: vi.fn(async () => undefined),
        projectThread: vi.fn(),
      },
    });

    const request = projector.projectTimelinePage("did:plc:viewer", { fetchHandler: vi.fn() });
    await projector.waitForTimelineProjection("did:plc:viewer");
    releaseTimeline({ feed: [] });
    await request;

    expect(projector.getTimelineProjectionStatus("did:plc:viewer")).toMatchObject({
      state: "failed",
      error: "profile projection failed",
    });
  });
});
