import { RawGroup } from "cojson";
import { createWebSocketPeer } from "cojson-transport-ws";
import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import { LocalNode } from "cojson/dist/localNode.js";
import { describe, expect, it, onTestFinished } from "vitest";
import { createWorkerGroupWithAdmin } from "../createWorkerGroupWithAdmin.js";
import { startSyncServer } from "../startSyncServer.js";

describe("createWorkerGroupWithAdmin - integration tests", () => {
  it("should create a worker group with admin using the local sync server", async () => {
    // Pass port: undefined to let the server choose a random port
    const server = await startSyncServer({
      port: undefined,
      inMemory: true,
      db: "",
    });

    onTestFinished(() => {
      server.close();
    });

    const address = server.address();

    if (typeof address !== "object" || address === null) {
      throw new Error("Server address is not an object");
    }

    const {
      adminAccountID,
      adminAgentSecret,
      groupID,
      accountID,
      agentSecret,
    } = await createWorkerGroupWithAdmin({
      name: "test",
      peer: `ws://localhost:${address.port}`,
    });

    expect(adminAccountID).toBeDefined();
    expect(adminAgentSecret).toBeDefined();
    expect(groupID).toBeDefined();
    expect(accountID).toBeDefined();
    expect(agentSecret).toBeDefined();

    const peer = createWebSocketPeer({
      id: "upstream",
      websocket: new WebSocket(`ws://localhost:${address.port}`),
      role: "server",
    });

    const crypto = await WasmCrypto.create();
    const { node } = await LocalNode.withNewlyCreatedAccount({
      creationProps: { name: "test" },
      peersToLoadFrom: [peer],
      crypto,
    });

    const loadedGroup = await node.load(groupID as any);

    expect(loadedGroup).not.toBe("unavailable");
    expect((loadedGroup as RawGroup).get(accountID as any)).toBe("writer");
  });

  it("should create a worker group with admin using the Jazz cloud", async () => {
    const {
      adminAccountID,
      adminAgentSecret,
      groupID,
      accountID,
      agentSecret,
    } = await createWorkerGroupWithAdmin({
      name: "test",
      peer: `wss://cloud.jazz.tools`,
    });

    expect(adminAccountID).toBeDefined();
    expect(adminAgentSecret).toBeDefined();
    expect(groupID).toBeDefined();
    expect(accountID).toBeDefined();
    expect(agentSecret).toBeDefined();

    const peer = createWebSocketPeer({
      id: "upstream",
      websocket: new WebSocket(`wss://cloud.jazz.tools`),
      role: "server",
    });

    const crypto = await WasmCrypto.create();
    const { node } = await LocalNode.withNewlyCreatedAccount({
      creationProps: { name: "test" },
      peersToLoadFrom: [peer],
      crypto,
    });

    const loadedGroup = await node.load(groupID as any);

    expect(loadedGroup).not.toBe("unavailable");
    expect((loadedGroup as RawGroup).get(accountID as any)).toBe("writer");
  });
});
