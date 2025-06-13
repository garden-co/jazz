import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";
import { PeerState } from "../PeerState";
import { CoValueCore } from "../coValueCore/coValueCore";
import { CoValueHeader, VerifiedState } from "../coValueCore/verifiedState";
import { RawCoID } from "../ids";
import { LocalNode } from "../localNode";
import { Peer } from "../sync";
import { createTestMetricReader, tearDownTestMetricReader } from "./testUtils";

let metricReader: ReturnType<typeof createTestMetricReader>;

beforeEach(() => {
  metricReader = createTestMetricReader();
});

afterEach(() => {
  tearDownTestMetricReader();
});

const mockNode = {} as LocalNode;

describe("CoValueCore loading state", () => {
  const mockCoValueId = "co_test123" as RawCoID;

  test("should create unknown state", async () => {
    const state = CoValueCore.fromID(mockCoValueId, mockNode);

    expect(state.id).toBe(mockCoValueId);
    expect(state.loadingState).toBe("unknown");
    expect(
      await metricReader.getMetricValue("jazz.covalues.loaded", {
        state: "unknown",
      }),
    ).toBe(1);
  });

  test("should create loading state", async () => {
    const state = CoValueCore.fromID(mockCoValueId, mockNode);
    state.loadFromPeers([
      createMockPeerState({ id: "peer1", role: "server" }),
      createMockPeerState({ id: "peer2", role: "server" }),
    ]);

    expect(state.id).toBe(mockCoValueId);
    expect(state.loadingState).toBe("loading");
    expect(
      await metricReader.getMetricValue("jazz.covalues.loaded", {
        state: "loading",
      }),
    ).toBe(1);
  });

  test("should create available state", async () => {
    const mockVerified = createMockCoValueVerified(mockCoValueId);
    const state = CoValueCore.fromID(mockCoValueId, mockNode);
    state.internalMarkMagicallyAvailable(mockVerified);

    expect(state.id).toBe(mockCoValueId);
    expect(state.loadingState).toBe("available");
    expect(state.verified).toBe(mockVerified);
    await expect(state.waitForAvailableOrUnavailable()).resolves.toMatchObject({
      verified: mockVerified,
    });
    expect(
      await metricReader.getMetricValue("jazz.covalues.loaded", {
        state: "available",
      }),
    ).toBe(1);
  });

  test("should handle found action", async () => {
    const mockVerified = createMockCoValueVerified(mockCoValueId);
    const state = CoValueCore.fromID(mockCoValueId, mockNode);
    state.loadFromPeers([
      createMockPeerState({ id: "peer1", role: "server" }),
      createMockPeerState({ id: "peer2", role: "server" }),
    ]);

    expect(
      await metricReader.getMetricValue("jazz.covalues.loaded", {
        state: "available",
      }),
    ).toBe(undefined);
    expect(
      await metricReader.getMetricValue("jazz.covalues.loaded", {
        state: "loading",
      }),
    ).toBe(1);

    const stateValuePromise = state.waitForAvailableOrUnavailable();

    state.internalMarkMagicallyAvailable(mockVerified);

    const result = await state.waitForAvailableOrUnavailable();
    expect(result).toMatchObject({ verified: mockVerified });
    await expect(stateValuePromise).resolves.toMatchObject({
      verified: mockVerified,
    });

    expect(
      await metricReader.getMetricValue("jazz.covalues.loaded", {
        state: "available",
      }),
    ).toBe(1);
    expect(
      await metricReader.getMetricValue("jazz.covalues.loaded", {
        state: "loading",
      }),
    ).toBe(0);
  });

  test("should skip errored coValues when loading from peers", async () => {
    vi.useFakeTimers();

    const peer1 = createMockPeerState(
      {
        id: "peer1",
        role: "server",
      },
      async () => {
        state.markErrored("peer1", {} as any);
      },
    );
    const peer2 = createMockPeerState(
      {
        id: "peer2",
        role: "server",
      },
      async () => {
        state.markNotFoundInPeer("peer2");
      },
    );

    const mockPeers = [peer1, peer2] as unknown as PeerState[];

    const state = CoValueCore.fromID(mockCoValueId, mockNode);
    const loadPromise = state.loadFromPeers(mockPeers);

    await vi.runAllTimersAsync();
    await loadPromise;

    expect(peer1.pushOutgoingMessage).toHaveBeenCalledTimes(1);
    expect(peer2.pushOutgoingMessage).toHaveBeenCalledTimes(1);
    expect(state.loadingState).toBe("unavailable");
    await expect(state.waitForAvailableOrUnavailable()).resolves.toMatchObject({
      verified: null,
    });

    vi.useRealTimers();
  });

  test("should have a coValue as value property when becomes available after that have been marked as unavailable", async () => {
    vi.useFakeTimers();

    const peer1 = createMockPeerState(
      {
        id: "peer1",
        role: "server",
      },
      async () => {
        state.markNotFoundInPeer("peer1");
      },
    );

    const mockPeers = [peer1] as unknown as PeerState[];

    const state = CoValueCore.fromID(mockCoValueId, mockNode);
    const loadPromise = state.loadFromPeers(mockPeers);

    await vi.runAllTimersAsync();

    state.internalMarkMagicallyAvailable(
      createMockCoValueVerified(mockCoValueId),
    );

    await loadPromise;

    expect(peer1.pushOutgoingMessage).toHaveBeenCalledTimes(1);
    expect(state.loadingState).toBe("available");
    await expect(state.waitForAvailableOrUnavailable()).resolves.toMatchObject({
      _verified: expect.any(Object),
    });

    vi.useRealTimers();
  });

  test("should start sending the known state to peers when available", async () => {
    vi.useFakeTimers();

    const mockVerified = createMockCoValueVerified(mockCoValueId);

    const peer1 = createMockPeerState(
      {
        id: "peer1",
        role: "storage",
      },
      async () => {
        state.provideHeader({} as CoValueHeader, "peer1");
      },
    );
    const peer2 = createMockPeerState(
      {
        id: "peer2",
        role: "server",
      },
      async () => {
        state.markNotFoundInPeer("peer2");
      },
    );

    const state = CoValueCore.fromID(mockCoValueId, mockNode);
    const loadPromise = state.loadFromPeers([peer1, peer2]);

    await vi.runAllTimersAsync();
    await loadPromise;

    expect(peer1.pushOutgoingMessage).toHaveBeenCalledTimes(1);
    expect(peer2.pushOutgoingMessage).toHaveBeenCalledTimes(1);
    expect(peer2.pushOutgoingMessage).toHaveBeenCalledWith({
      action: "load",
      ...mockVerified.knownState(),
    });
    expect(state.loadingState).toBe("available");
    await expect(state.waitForAvailableOrUnavailable()).resolves.toMatchObject({
      verified: mockVerified,
    });

    vi.useRealTimers();
  });

  test("should skip closed peers", async () => {
    vi.useFakeTimers();

    const mockVerified = createMockCoValueVerified(mockCoValueId);

    const peer1 = createMockPeerState(
      {
        id: "peer1",
        role: "storage",
      },
      async () => {
        return new Promise(() => {});
      },
    );
    const peer2 = createMockPeerState(
      {
        id: "peer2",
        role: "server",
      },
      async () => {
        state.provideHeader({} as CoValueHeader, "peer2");
      },
    );

    peer1.closed = true;

    const state = CoValueCore.fromID(mockCoValueId, mockNode);
    const loadPromise = state.loadFromPeers([peer1, peer2]);

    await vi.runAllTimersAsync();
    await loadPromise;

    expect(peer1.pushOutgoingMessage).toHaveBeenCalledTimes(0);
    expect(peer2.pushOutgoingMessage).toHaveBeenCalledTimes(1);

    expect(state.loadingState).toBe("available");
    await expect(state.waitForAvailableOrUnavailable()).resolves.toMatchObject({
      verified: mockVerified,
    });

    vi.useRealTimers();
  });

  test("should not be stuck in loading state when not getting a response", async () => {
    vi.useFakeTimers();

    const peer1 = createMockPeerState(
      {
        id: "peer1",
        role: "server",
      },
      async () => {},
    );

    const state = CoValueCore.fromID(mockCoValueId, mockNode);
    const loadPromise = state.loadFromPeers([peer1]);

    await vi.runAllTimersAsync();
    await loadPromise;

    expect(peer1.pushOutgoingMessage).toHaveBeenCalledTimes(1);

    expect(state.loadingState).toBe("unavailable");
    await expect(state.waitForAvailableOrUnavailable()).resolves.toMatchObject({
      verified: null,
    });

    vi.useRealTimers();
  });
});

function createMockPeerState(
  peer: Partial<Peer>,
  pushFn = () => Promise.resolve(),
) {
  const peerState = new PeerState(
    {
      id: "peer",
      role: "server",
      outgoing: {
        push: pushFn,
      },
      ...peer,
    } as Peer,
    undefined,
  );

  vi.spyOn(peerState, "pushOutgoingMessage").mockImplementation(pushFn);

  return peerState;
}

function createMockCoValueVerified(mockCoValueId: string) {
  // Setting the knownState as part of the prototype to simplify
  // the equality checks
  const mockCoValueVerified = Object.create({
    id: mockCoValueId,
    knownState: vi.fn().mockReturnValue({
      id: mockCoValueId,
      header: true,
      sessions: {},
    }),
    clone: vi.fn().mockReturnThis(),
  }) as unknown as VerifiedState;

  return mockCoValueVerified as unknown as VerifiedState;
}
