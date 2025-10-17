import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";
import { PeerState } from "../PeerState";
import { CoValueCore, idforHeader } from "../coValueCore/coValueCore";
import { CoValueHeader, VerifiedState } from "../coValueCore/verifiedState";
import { RawCoID } from "../ids";
import { LocalNode } from "../localNode";
import { Peer } from "../sync";
import {
  createTestMetricReader,
  createTestNode,
  tearDownTestMetricReader,
} from "./testUtils";
import { WasmCrypto } from "../crypto/WasmCrypto";

let metricReader: ReturnType<typeof createTestMetricReader>;

beforeEach(() => {
  metricReader = createTestMetricReader();
});

afterEach(() => {
  tearDownTestMetricReader();
});

function setup() {
  const node = createTestNode();

  const header = {
    type: "comap",
    ruleset: { type: "ownedByGroup", group: "co_ztest123" },
    meta: null,
    ...node.crypto.createdNowUnique(),
  } as CoValueHeader;

  const id = idforHeader(header, node.crypto);

  const state = CoValueCore.fromID(id, node);

  return { node, state, id, header };
}

describe("CoValueCore loading state", () => {
  test("should create unknown state", async () => {
    const { state, id } = setup();

    expect(state.id).toBe(id);
    expect(state.loadingState).toBe("unknown");
    expect(
      await metricReader.getMetricValue("jazz.covalues.loaded", {
        state: "unknown",
      }),
    ).toBe(1);
  });

  test("should create loading state", async () => {
    const { state, id } = setup();
    state.loadFromPeers([
      createMockPeerState({ id: "peer1", role: "server" }),
      createMockPeerState({ id: "peer2", role: "server" }),
    ]);

    expect(state.id).toBe(id);
    expect(state.loadingState).toBe("loading");
    expect(
      await metricReader.getMetricValue("jazz.covalues.loaded", {
        state: "loading",
      }),
    ).toBe(1);
  });

  test("should create available state", async () => {
    const { state, id, header } = setup();

    const previousState = state.loadingState;
    state.provideHeader(header);
    state.markFoundInPeer("peer1", previousState);

    expect(state.id).toBe(id);
    expect(state.loadingState).toBe("available");
    await expect(state.waitForAvailableOrUnavailable()).resolves.toMatchObject({
      verified: expect.any(Object),
    });
    expect(
      await metricReader.getMetricValue("jazz.covalues.loaded", {
        state: "available",
      }),
    ).toBe(1);
  });

  test("should handle found action", async () => {
    const { state, header } = setup();
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
    const previousState = state.loadingState;

    state.provideHeader(header);
    state.markFoundInPeer("peer1", previousState);

    await stateValuePromise;

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
    const { state } = setup();
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
    const { state, header } = setup();
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

    const loadPromise = state.loadFromPeers(mockPeers);

    await vi.runAllTimersAsync();

    const previousState = state.loadingState;
    state.provideHeader(header);
    state.markFoundInPeer("peer1", previousState);

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

    const { state, header } = setup();

    const peer1 = createMockPeerState(
      {
        id: "peer1",
        role: "server",
      },
      async () => {
        const previousState = state.loadingState;
        state.provideHeader(header);
        state.markFoundInPeer("peer1", previousState);
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

    const loadPromise = state.loadFromPeers([peer1, peer2]);

    await vi.runAllTimersAsync();
    await loadPromise;

    expect(peer1.pushOutgoingMessage).toHaveBeenCalledTimes(1);
    expect(peer2.pushOutgoingMessage).toHaveBeenCalledTimes(1);
    expect(peer2.pushOutgoingMessage).toHaveBeenCalledWith({
      action: "load",
      ...state.knownState(),
    });
    expect(state.loadingState).toBe("available");

    vi.useRealTimers();
  });

  test("should skip closed peers", async () => {
    vi.useFakeTimers();

    const { state, header } = setup();

    const peer1 = createMockPeerState(
      {
        id: "peer1",
        role: "server",
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
        const previousState = state.loadingState;
        state.provideHeader(header);
        state.markFoundInPeer("peer2", previousState);
      },
    );

    peer1.closed = true;

    const loadPromise = state.loadFromPeers([peer1, peer2]);

    await vi.runAllTimersAsync();
    await loadPromise;

    expect(peer1.pushOutgoingMessage).toHaveBeenCalledTimes(0);
    expect(peer2.pushOutgoingMessage).toHaveBeenCalledTimes(1);

    expect(state.loadingState).toBe("available");

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

    const { state } = setup();
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
