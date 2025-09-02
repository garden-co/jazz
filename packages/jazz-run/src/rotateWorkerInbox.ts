import { AgentSecret, LocalNode, RawAccountID, RawCoMap } from "cojson";
import { createWebSocketPeer } from "cojson-transport-ws";
import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import { Account, createInboxRoot } from "jazz-tools";
import { WebSocket } from "ws";
import { loadEnvFile } from "node:process";

export const rotateWorkerInbox = async ({
  peer: peerAddr,
}: {
  peer: string;
}) => {
  try {
    loadEnvFile();
  } catch (error) {
    // ignore
  }

  const crypto = await WasmCrypto.create();

  const accountID = process.env.JAZZ_WORKER_ACCOUNT;
  const accountSecret = process.env.JAZZ_WORKER_SECRET;

  if (!accountID || !accountSecret) {
    throw new Error(
      "JAZZ_WORKER_ACCOUNT and JAZZ_WORKER_SECRET environment variables must be set",
    );
  }

  const peer = createWebSocketPeer({
    id: "upstream",
    websocket: new WebSocket(peerAddr),
    role: "server",
  });

  const node = await LocalNode.withLoadedAccount({
    accountID: accountID as RawAccountID,
    accountSecret: accountSecret as AgentSecret,
    peersToLoadFrom: [peer],
    crypto,
    sessionID: crypto.newRandomSessionID(accountID as RawAccountID),
  });

  const account = Account.fromNode(node);

  const profile = node
    .expectCoValueLoaded(account.$jazz.raw.get("profile")!)
    .getCurrentContent() as RawCoMap;

  const inboxRoot = createInboxRoot(account);
  profile.set("inbox", inboxRoot.id);
  profile.set("inboxInvite", inboxRoot.inviteLink);

  await account.$jazz.waitForAllCoValuesSync({ timeout: 4_000 });
};
