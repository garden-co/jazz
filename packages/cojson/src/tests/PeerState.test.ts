import { describe, expect, test, vi } from "vitest";
import { PeerState } from "../PeerState.js";
import { ConnectedPeerChannel } from "../streamUtils.js";
import { Peer, SyncMessage } from "../sync.js";
import { CoValueKnownState, KnownStateSessions } from "../knownState.js";

function setup() {
  const mockPeer: Peer = {
    id: "test-peer",
    role: "client",
    priority: 1,
    incoming: new ConnectedPeerChannel(),
    outgoing: new ConnectedPeerChannel(),
  };
  vi.spyOn(mockPeer.outgoing, "push");
  vi.spyOn(mockPeer.incoming, "close");
  vi.spyOn(mockPeer.outgoing, "close");
  const peerState = new PeerState(mockPeer, undefined);
  return { mockPeer, peerState };
}

describe("PeerState", () => {
  test("should push outgoing message to peer", async () => {
    const { mockPeer, peerState } = setup();
    const message: SyncMessage = {
      action: "load",
      id: "co_ztest-id",
      header: false,
      sessions: {},
    };
    await peerState.pushOutgoingMessage(message);
    expect(mockPeer.outgoing.push).toHaveBeenCalledWith(message);
  });

  test("should return peer's incoming when not closed", () => {
    const { mockPeer, peerState } = setup();
    expect(peerState.incoming).toBe(mockPeer.incoming);
  });

  test("should perform graceful shutdown", () => {
    const { mockPeer, peerState } = setup();
    peerState.gracefulShutdown();
    expect(mockPeer.outgoing.close).toHaveBeenCalled();
    expect(peerState.closed).toBe(true);
  });

  test("should clone the knownStates into optimisticKnownStates and knownStates when passed as argument", () => {
    const { peerState, mockPeer } = setup();
    peerState.setKnownState("co_z1", {
      id: "co_z1",
      header: false,
      sessions: {},
    });

    peerState.combineOptimisticWith("co_z1", {
      id: "co_z1",
      header: true,
      sessions: {
        "session-1": 1,
      } as KnownStateSessions,
    });

    const newPeerState = peerState.newStateFrom(mockPeer);

    expect(newPeerState.getKnownState("co_z1")).toEqual(
      peerState.getKnownState("co_z1"),
    );
    expect(newPeerState.getOptimisticKnownState("co_z1")).toEqual(
      peerState.getKnownState("co_z1"),
    );
  });

  test("should dispatch to both states", () => {
    const { peerState } = setup();

    const state: CoValueKnownState = {
      id: "co_z1",
      header: true,
      sessions: {},
    };

    peerState.setKnownState("co_z1", state);

    expect(peerState.getKnownState("co_z1")).toEqual(state);
    expect(peerState.getOptimisticKnownState("co_z1")).toEqual(state);
  });

  test("dispatching an optimistic update should not affect the known states", () => {
    const { peerState } = setup();

    const state: CoValueKnownState = {
      id: "co_z1",
      header: false,
      sessions: {},
    };

    peerState.setKnownState("co_z1", state);

    const optimisticState: CoValueKnownState = {
      id: "co_z1",
      header: false,
      sessions: {
        "session-1": 1,
      } as KnownStateSessions,
    };

    peerState.combineOptimisticWith("co_z1", optimisticState);

    expect(peerState.getKnownState("co_z1")).not.toEqual(optimisticState);
    expect(peerState.getOptimisticKnownState("co_z1")).toEqual(optimisticState);
  });
});
