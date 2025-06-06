import { AgentSecret } from "cojson";
import { createWebSocketPeer } from "cojson-transport-ws";
import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import {
  Account,
  Group,
  createJazzContextFromExistingCredentials,
  randomSessionProvider,
} from "jazz-tools";
import { WebSocket } from "ws";

export const createWorkerGroup = async ({
  owner,
  ownerSecret,
  peer: peerAddr,
}: { owner: string; ownerSecret: string; peer: string }) => {
  const crypto = await WasmCrypto.create();

  const peer = createWebSocketPeer({
    id: "upstream",
    websocket: new WebSocket(peerAddr),
    role: "server",
  });

  const context = await createJazzContextFromExistingCredentials({
    credentials: {
      accountID: owner,
      secret: ownerSecret as AgentSecret,
    },
    AccountSchema: Account,
    sessionProvider: randomSessionProvider,
    peersToLoadFrom: [peer],
    crypto,
  });

  const group = Group.create({ owner: context.account });
  await context.account.waitForAllCoValuesSync();

  return {
    groupID: group.id,
    groupAsOwner: group,
  };
};
