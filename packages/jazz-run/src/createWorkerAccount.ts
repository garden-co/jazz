import { createWebSocketPeer } from "cojson-transport-ws";
import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import { Account, isControlledAccount } from "jazz-tools";
import { WebSocket } from "ws";

export const createWorkerAccount = async ({
  name,
  peer: peerAddr,
}: {
  name: string;
  peer: string;
}) => {
  const crypto = await WasmCrypto.create();

  const peer = createWebSocketPeer({
    id: "upstream",
    websocket: new WebSocket(peerAddr),
    role: "server",
  });

  const account = await Account.create({
    creationProps: { name },
    peers: [peer],
    crypto,
  });

  if (!isControlledAccount(account)) {
    throw new Error("account is not a controlled account");
  }

  await account.$jazz.waitForAllCoValuesSync({ timeout: 4_000 });

  return {
    accountID: account.$jazz.id,
    agentSecret: account.$jazz.localNode.getCurrentAgent().agentSecret,
  };
};
