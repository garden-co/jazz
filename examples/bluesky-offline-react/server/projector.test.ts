import { describe, expect, it, vi } from "vitest";

vi.mock("./jazz.js", () => ({ db: {} }));

import { createProjector } from "./projector.js";

function projectorDependencies(overrides: Partial<Parameters<typeof createProjector>[0]> = {}) {
  return {
    fetchTimelineFeed: vi.fn(async () => ({ feed: [], cursor: "next" })),
    fetchPostThread: vi.fn(),
    fetchProfile: vi.fn(async () => undefined),
    writer: {
      loadReactionIntents: vi.fn(async () => new Map()),
      projectProfile: vi.fn(async () => undefined),
      projectTimelinePage: vi.fn(async () => undefined),
      projectThread: vi.fn(),
    },
    ...overrides,
  };
}

describe("timeline projector", () => {
  it("projects the signed-in profile without waiting for the timeline fetch", async () => {
    const profile = { did: "did:plc:viewer", handle: "viewer.test" };
    const projectProfile = vi.fn(async () => undefined);
    const dependencies = projectorDependencies({
      fetchTimelineFeed: vi.fn(() => new Promise(() => undefined)),
      fetchProfile: vi.fn(async () => profile),
    });
    dependencies.writer.projectProfile = projectProfile;
    const projector = createProjector(dependencies);

    projector.projectTimelinePage("did:plc:viewer", { fetchHandler: vi.fn() });
    await vi.waitFor(() => expect(projectProfile).toHaveBeenCalledWith(profile));
  });

  it("shares one in-flight projection and returns only pagination metadata", async () => {
    let releaseFetch!: (value: { feed: never[]; cursor: string }) => void;
    const fetchTimelineFeed = vi.fn(() => new Promise<{ feed: never[]; cursor: string }>((resolve) => {
      releaseFetch = resolve;
    }));
    let releaseProjection!: () => void;
    const projectTimelinePage = vi.fn(() => new Promise<void>((resolve) => {
      releaseProjection = resolve;
    }));
    const dependencies = projectorDependencies({ fetchTimelineFeed });
    dependencies.writer.projectTimelinePage = projectTimelinePage;
    const projector = createProjector(dependencies);
    const session = { fetchHandler: vi.fn() };

    const first = projector.projectTimelinePage("did:plc:viewer", session);
    const second = projector.projectTimelinePage("did:plc:viewer", session);
    expect(fetchTimelineFeed).toHaveBeenCalledTimes(1);

    releaseFetch({ feed: [], cursor: "next" });
    await expect(Promise.all([first, second])).resolves.toEqual([
      { cursor: "next", hasMore: true, count: 0 },
      { cursor: "next", hasMore: true, count: 0 },
    ]);

    releaseProjection();
    await new Promise((resolve) => setImmediate(resolve));
    const third = projector.projectTimelinePage("did:plc:viewer", session);
    expect(fetchTimelineFeed).toHaveBeenCalledTimes(2);
    releaseFetch({ feed: [], cursor: "later" });
    await expect(third).resolves.toMatchObject({ cursor: "later" });
  });

  it("does not let pagination swallow a fresh head projection", async () => {
    const releases = new Map<string, (value: { feed: never[]; cursor?: string }) => void>();
    const fetchTimelineFeed = vi.fn((_session: unknown, cursor?: string) =>
      new Promise<{ feed: never[]; cursor?: string }>((resolve) => {
        releases.set(cursor ?? "head", resolve);
      }));
    const projector = createProjector(projectorDependencies({ fetchTimelineFeed }));
    const session = { fetchHandler: vi.fn() };

    const pagination = projector.projectTimelinePage("did:plc:viewer", session, "older");
    const refresh = projector.projectTimelinePage("did:plc:viewer", session);

    expect(fetchTimelineFeed).toHaveBeenCalledTimes(2);
    releases.get("older")?.({ feed: [], cursor: "oldest" });
    releases.get("head")?.({ feed: [], cursor: "newer" });
    await expect(pagination).resolves.toEqual({ cursor: "oldest", hasMore: true, count: 0 });
    await expect(refresh).resolves.toEqual({ cursor: "newer", hasMore: true, count: 0 });
  });

  it("reports a background projection failure", async () => {
    const error = new Error("projection exploded");
    const dependencies = projectorDependencies();
    dependencies.writer.projectTimelinePage = vi.fn(async () => { throw error; });
    const reportError = vi.fn();
    const projector = createProjector(dependencies, reportError);

    await projector.projectTimelinePage("did:plc:viewer", { fetchHandler: vi.fn() });

    await vi.waitFor(() => expect(reportError).toHaveBeenCalledWith("Timeline projection failed", error));
  });
});
