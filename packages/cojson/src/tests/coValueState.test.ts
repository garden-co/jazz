import {
  assert,
  afterEach,
  beforeEach,
  describe,
  expect,
  onTestFinished,
  test,
  vi,
} from "vitest";
import { PeerState } from "../PeerState";
import { CoValueCore } from "../coValueCore";
import { CoValueState } from "../coValueState";
import { RawCoID } from "../ids";
import { Peer } from "../sync";
import { createTestMetricReader, tearDownTestMetricReader } from "./testUtils";

let metricReader: ReturnType<typeof createTestMetricReader>;

beforeEach(() => {
  metricReader = createTestMetricReader();
});

afterEach(() => {
  tearDownTestMetricReader();
});

describe("CoValueState", () => {
  const mockCoValueId = "co_test123" as RawCoID;

  test("should create unknown state", async () => {
    const state = new CoValueState(mockCoValueId);

    expect(state.id).toBe(mockCoValueId);
    expect(state.highLevelState).toBe("unknown");
    expect(
      await metricReader.getMetricValue("jazz.covalues.loaded", {
        state: "unknown",
      }),
    ).toBe(1);
  });

  test("should create loading state", async () => {
    const state = new CoValueState(mockCoValueId);
    state.loadFromPeers([
      createMockPeerState({ id: "peer1", role: "server" }),
      createMockPeerState({ id: "peer2", role: "server" }),
    ]);

    expect(state.id).toBe(mockCoValueId);
    expect(state.highLevelState).toBe("loading");
    expect(
      await metricReader.getMetricValue("jazz.covalues.loaded", {
        state: "loading",
      }),
    ).toBe(1);
  });

  test("should create available state", async () => {
    const mockCoValue = createMockCoValueCore(mockCoValueId);
    const state = new CoValueState(mockCoValueId);
    state.internalMarkMagicallyAvailable(mockCoValue);

    expect(state.id).toBe(mockCoValueId);
    expect(state.highLevelState).toBe("available");
    expect(state.core).toBe(mockCoValue);
    await expect(state.getCoValue()).resolves.toEqual(mockCoValue);
    expect(
      await metricReader.getMetricValue("jazz.covalues.loaded", {
        state: "available",
      }),
    ).toBe(1);
  });

  test("should handle found action", async () => {
    const mockCoValue = createMockCoValueCore(mockCoValueId);
    const state = new CoValueState(mockCoValueId);
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

    const stateValuePromise = state.getCoValue();

    state.internalMarkMagicallyAvailable(mockCoValue);

    const result = await state.getCoValue();
    expect(result).toBe(mockCoValue);
    await expect(stateValuePromise).resolves.toBe(mockCoValue);

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

    const state = new CoValueState(mockCoValueId);
    const loadPromise = state.loadFromPeers(mockPeers);

    await vi.runAllTimersAsync();

    await loadPromise;

    expect(peer1.pushOutgoingMessage).toHaveBeenCalledTimes(1);
    expect(peer2.pushOutgoingMessage).toHaveBeenCalledTimes(1);
    expect(state.highLevelState).toBe("unavailable");
    await expect(state.getCoValue()).resolves.toBe("unavailable");

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

    const state = new CoValueState(mockCoValueId);
    const loadPromise = state.loadFromPeers(mockPeers);

    await vi.runAllTimersAsync();

    state.internalMarkMagicallyAvailable(createMockCoValueCore(mockCoValueId));

    await loadPromise;

    expect(peer1.pushOutgoingMessage).toHaveBeenCalledTimes(1);
    expect(state.highLevelState).toBe("available");
    await expect(state.getCoValue()).resolves.toEqual({ id: mockCoValueId });

    vi.useRealTimers();
  });

  test("should start sending the known state to peers when available", async () => {
    vi.useFakeTimers();

    const mockCoValue = createMockCoValueCore(mockCoValueId);

    const peer1 = createMockPeerState(
      {
        id: "peer1",
        role: "storage",
      },
      async () => {
        state.markAvailable(mockCoValue, "peer1");
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

    const state = new CoValueState(mockCoValueId);
    const loadPromise = state.loadFromPeers([peer1, peer2]);

    await vi.runAllTimersAsync();
    await loadPromise;

    expect(peer1.pushOutgoingMessage).toHaveBeenCalledTimes(1);
    expect(peer2.pushOutgoingMessage).toHaveBeenCalledTimes(1);
    expect(peer2.pushOutgoingMessage).toHaveBeenCalledWith({
      action: "load",
      ...mockCoValue.knownState(),
    });
    expect(state.highLevelState).toBe("available");
    await expect(state.getCoValue()).resolves.toEqual({ id: mockCoValueId });

    vi.useRealTimers();
  });

  test("should skip closed peers", async () => {
    vi.useFakeTimers();

    const mockCoValue = createMockCoValueCore(mockCoValueId);

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
        state.markAvailable(mockCoValue, "peer2");
      },
    );

    peer1.closed = true;

    const state = new CoValueState(mockCoValueId);
    const loadPromise = state.loadFromPeers([peer1, peer2]);

    await vi.runAllTimersAsync();
    await loadPromise;

    expect(peer1.pushOutgoingMessage).toHaveBeenCalledTimes(0);
    expect(peer2.pushOutgoingMessage).toHaveBeenCalledTimes(1);

    expect(state.highLevelState).toBe("available");
    await expect(state.getCoValue()).resolves.toEqual({ id: mockCoValueId });

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

    const state = new CoValueState(mockCoValueId);
    const loadPromise = state.loadFromPeers([peer1]);

    await vi.runAllTimersAsync();
    await loadPromise;

    expect(peer1.pushOutgoingMessage).toHaveBeenCalledTimes(1);

    expect(state.highLevelState).toBe("unavailable");
    await expect(state.getCoValue()).resolves.toEqual("unavailable");

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

function createMockCoValueCore(mockCoValueId: string) {
  // Setting the knownState as part of the prototype to simplify
  // the equality checks
  const mockCoValue = Object.create({
    knownState: vi.fn().mockReturnValue({
      id: mockCoValueId,
      header: true,
      sessions: {},
    }),
  });

  mockCoValue.id = mockCoValueId;
  return mockCoValue as unknown as CoValueCore;
}
