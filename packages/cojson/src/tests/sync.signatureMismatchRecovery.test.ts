import { beforeEach, describe, expect, test, vi } from "vitest";
import type { RawCoMap } from "../exports.js";
import { logger } from "../logger.js";
import type { SignatureMismatchErrorMessage } from "../sync.js";
import {
  SyncMessagesLog,
  TEST_NODE_CONFIG,
  blockMessageTypeOnOutgoingPeer,
  loadCoValueOrFail,
  setupTestNode,
  waitFor,
} from "./testUtils.js";

// We want to simulate a real world communication that happens asynchronously
TEST_NODE_CONFIG.withAsyncPeers = true;

describe("SignatureMismatch recovery", () => {
  beforeEach(() => {
    SyncMessagesLog.clear();
  });

  test("non-owner recovery is a no-op for now", async () => {
    const server = setupTestNode({ isSyncServer: true });
    const client = setupTestNode();

    const { peerOnServer } = client.connectToSyncServer();
    client.addStorage();

    const group = server.node.createGroup();
    const map = group.createMap();
    map.set("k", "server-value", "trusting");
    await map.core.waitForSync();

    const mapOnClient = await loadCoValueOrFail(client.node, map.id);
    expect(mapOnClient.get("k")).toBe("server-value");

    const authoritativeContent =
      map.core.verified?.getFullSessionContent(server.node.currentSessionID) ??
      [];

    const mismatchMessage: SignatureMismatchErrorMessage = {
      action: "error",
      errorType: "SignatureMismatch",
      id: map.id,
      sessionID: server.node.currentSessionID,
      content: authoritativeContent,
      reason: "test non-owner",
    };

    SyncMessagesLog.clear();
    peerOnServer.outgoing.push(mismatchMessage);

    await waitFor(() => {
      const messages = SyncMessagesLog.getMessages({
        Map: map.core,
      });
      expect(
        messages.some((message) =>
          message.includes("ERROR Map errorType: SignatureMismatch"),
        ),
      ).toBe(true);
    });

    await new Promise((resolve) => setTimeout(resolve, 25));

    const messages = SyncMessagesLog.getMessages({
      Map: map.core,
    });

    expect(
      messages.some((message) =>
        message.includes("REPLACE_SESSION_HISTORY Map"),
      ),
    ).toBe(false);
  });

  test("owner recovery replaces local session and replays local tail", async () => {
    const server = setupTestNode({ isSyncServer: true });
    const client = setupTestNode();

    const { peer, peerOnServer } = client.connectToSyncServer();
    const { storage } = client.addStorage();

    const group = client.node.createGroup();
    const map = group.createMap();

    map.set("k", "v1", "trusting");
    await map.core.waitForSync();

    const blockedContent = blockMessageTypeOnOutgoingPeer(peer, "content", {
      id: map.id,
      once: true,
      matcher: (message) =>
        Boolean(
          message.action === "content" &&
            message.new[client.node.currentSessionID],
        ),
    });

    map.set("k", "v2", "trusting");

    await waitFor(() => {
      expect(blockedContent.blockedMessages.length).toBeGreaterThan(0);
      expect(
        storage.getKnownState(map.id).sessions[client.node.currentSessionID],
      ).toBe(2);
    });

    const mapOnServer = await loadCoValueOrFail(server.node, map.id);
    expect(mapOnServer.get("k")).toBe("v1");

    const authoritativeContent =
      mapOnServer.core.verified?.getFullSessionContent(
        client.node.currentSessionID,
      ) ?? [];

    const authoritativeTxCount = authoritativeContent.reduce(
      (total, piece) => total + piece.newTransactions.length,
      0,
    );

    expect(authoritativeTxCount).toBe(1);

    const mismatchMessage: SignatureMismatchErrorMessage = {
      action: "error",
      errorType: "SignatureMismatch",
      id: map.id,
      sessionID: client.node.currentSessionID,
      content: authoritativeContent,
      reason: "test owner",
    };

    SyncMessagesLog.clear();
    peerOnServer.outgoing.push(mismatchMessage);

    await waitFor(() => {
      const messages = SyncMessagesLog.getMessages({
        Map: map.core,
      });
      expect(
        messages.some((message) =>
          message.includes("REPLACE_SESSION_HISTORY Map"),
        ),
      ).toBe(true);
    });

    await waitFor(() => {
      expect(
        storage.getKnownState(map.id).sessions[client.node.currentSessionID],
      ).toBe(2);
    });

    const verificationNode = setupTestNode();
    verificationNode.addStorage({ storage });
    const reloadedMap = (await loadCoValueOrFail(
      verificationNode.node,
      map.id,
    )) as RawCoMap;

    expect(reloadedMap.get("k")).toBe("v2");
  });

  test("owner recovery resolves missing dependencies and replays referenced CoValue tail", async () => {
    const server = setupTestNode({ isSyncServer: true });
    const client = setupTestNode();

    const { peer, peerOnServer } = client.connectToSyncServer();
    const { storage } = client.addStorage();

    const group = client.node.createGroup();
    const map = group.createMap();
    const child = group.createMap();
    child.set("childKey", "child-value", "trusting");

    map.set("k", "v1", "trusting");
    await map.core.waitForSync();

    const blockedContent = blockMessageTypeOnOutgoingPeer(peer, "content", {
      id: map.id,
      once: true,
      matcher: (message) =>
        Boolean(
          message.action === "content" &&
            message.new[client.node.currentSessionID],
        ),
    });

    map.set("child", child.id, "trusting");

    await waitFor(() => {
      expect(blockedContent.blockedMessages.length).toBeGreaterThan(0);
      expect(
        storage.getKnownState(map.id).sessions[client.node.currentSessionID],
      ).toBe(2);
    });

    const mapOnServer = await loadCoValueOrFail(server.node, map.id);
    expect(mapOnServer.get("k")).toBe("v1");
    expect(mapOnServer.get("child")).toBeUndefined();

    const authoritativeContent =
      mapOnServer.core.verified?.getFullSessionContent(
        client.node.currentSessionID,
      ) ?? [];

    const mismatchMessage: SignatureMismatchErrorMessage = {
      action: "error",
      errorType: "SignatureMismatch",
      id: map.id,
      sessionID: client.node.currentSessionID,
      content: authoritativeContent,
      reason: "test owner missing dependencies",
    };

    SyncMessagesLog.clear();
    peerOnServer.outgoing.push(mismatchMessage);

    await waitFor(() => {
      const messages = SyncMessagesLog.getMessages({
        Map: map.core,
      });
      expect(
        messages.some((message) =>
          message.includes("REPLACE_SESSION_HISTORY Map"),
        ),
      ).toBe(true);
    });

    await waitFor(() => {
      expect(
        storage.getKnownState(map.id).sessions[client.node.currentSessionID],
      ).toBe(2);
    });

    const verificationNode = setupTestNode();
    verificationNode.addStorage({ storage });
    const reloadedMap = (await loadCoValueOrFail(
      verificationNode.node,
      map.id,
    )) as RawCoMap;

    expect(reloadedMap.get("child")).toBe(child.id);
  });

  test("owner recovery logs and aborts when private tail replay cannot decrypt", async () => {
    const server = setupTestNode({ isSyncServer: true });
    const client = setupTestNode();

    const { peer, peerOnServer } = client.connectToSyncServer();
    const { storage } = client.addStorage();

    const group = client.node.createGroup();
    const map = group.createMap();

    map.set("secret", "v1", "private");
    await map.core.waitForSync();

    const blockedContent = blockMessageTypeOnOutgoingPeer(peer, "content", {
      id: map.id,
      once: true,
      matcher: (message) =>
        Boolean(
          message.action === "content" &&
            message.new[client.node.currentSessionID],
        ),
    });

    map.set("secret", "v2", "private");

    await waitFor(() => {
      expect(blockedContent.blockedMessages.length).toBeGreaterThan(0);
      expect(
        storage.getKnownState(map.id).sessions[client.node.currentSessionID],
      ).toBe(2);
    });

    const mapOnServer = await loadCoValueOrFail(server.node, map.id);

    const authoritativeContent =
      mapOnServer.core.verified?.getFullSessionContent(
        client.node.currentSessionID,
      ) ?? [];

    const getReadKeySpy = vi
      .spyOn(map.core, "getReadKey")
      .mockReturnValue(undefined);
    const loggerErrorSpy = vi.spyOn(logger, "error");

    const mismatchMessage: SignatureMismatchErrorMessage = {
      action: "error",
      errorType: "SignatureMismatch",
      id: map.id,
      sessionID: client.node.currentSessionID,
      content: authoritativeContent,
      reason: "test owner private replay failure",
    };

    SyncMessagesLog.clear();
    peerOnServer.outgoing.push(mismatchMessage);

    await waitFor(() => {
      expect(loggerErrorSpy).toHaveBeenCalledWith(
        "Failed to run owner SignatureMismatch recovery",
        expect.objectContaining({
          id: map.id,
          sessionID: client.node.currentSessionID,
          error: expect.stringContaining(
            "Unable to replay transaction 1: missing parsed changes",
          ),
        }),
      );
    });

    const messages = SyncMessagesLog.getMessages({
      Map: map.core,
    });
    expect(
      messages.some((message) =>
        message.includes("REPLACE_SESSION_HISTORY Map"),
      ),
    ).toBe(false);

    getReadKeySpy.mockRestore();
    loggerErrorSpy.mockRestore();
  });
});
