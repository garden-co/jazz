import "jazz-tools/load-edge-wasm";
import { createWebSocketPeer } from "cojson-transport-ws";
import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import { Hono } from "hono";
import { Account, co, z } from "jazz-tools";
import { startWorker } from "jazz-tools/worker";

const app = new Hono();

const MyAccountRoot = co.map({ text: z.string() });

const MyAccount = co
  .account({
    root: MyAccountRoot,
    profile: co.profile(),
  })
  .withMigration((account) => {
    if (!account.$jazz.has("root")) {
      account.$jazz.set("root", {
        text: "Hello world!",
      });
    }
  });

const syncServer = "wss://cloud.jazz.tools/?key=jazz@jazz.tools";

app.get("/", async (c) => {
  const crypto = await WasmCrypto.create();
  const cryptoSync = WasmCrypto.createSync();

  const peer = createWebSocketPeer({
    id: "upstream",
    websocket: new WebSocket(syncServer),
    role: "server",
  });

  const account = await Account.create({
    creationProps: { name: "Cloudflare test account" },
    peers: [peer],
    crypto,
  });

  await account.$jazz.waitForAllCoValuesSync();

  const admin = await startWorker({
    accountID: account.$jazz.id,
    accountSecret: account.$jazz.localNode.getCurrentAgent().agentSecret,
    AccountSchema: MyAccount,
    syncServer,
    crypto,
  });

  const { root } = await admin.worker.$jazz.ensureLoaded({
    resolve: { root: true },
  });

  await admin.done();

  return c.json({
    text: root.text,
    isWasmCrypto: crypto instanceof WasmCrypto,
    isWasmCryptoSync: cryptoSync instanceof WasmCrypto,
  });
});

export default app;
