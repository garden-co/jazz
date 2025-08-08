import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import { createWebSocketPeer } from "cojson-transport-ws";
import { Hono } from "hono";
import { startSyncServer } from "jazz-run/startSyncServer";
import { Account, CoMap, coField } from "jazz-tools";
import { startWorker } from "jazz-tools/worker";

const app = new Hono();

class MyAccountRoot extends CoMap {
  text = coField.string;
}

class MyAccount extends Account {
  root = coField.ref(MyAccountRoot);

  migrate(): void {
    if (this.root === undefined) {
      this.root = MyAccountRoot.create({
        text: "Hello world!",
      });
    }
  }
}

const syncServer = "wss://cloud.jazz.tools/?key=jazz@jazz.tools";

app.get("/", async (c) => {
  const crypto = await WasmCrypto.create();

  const peer = createWebSocketPeer({
    id: "upstream",
    websocket: new WebSocket(syncServer),
    role: "server",
  });

  const account = await Account.create({
    creationProps: { name: "Cloudflare test account" },
    peersToLoadFrom: [peer],
    crypto,
  });

  await account.waitForAllCoValuesSync();

  const admin = await startWorker({
    accountID: account.id,
    accountSecret: account._raw.core.node.getCurrentAgent().agentSecret,
    AccountSchema: MyAccount,
    syncServer,
    crypto,
  });

  const { root } = await admin.worker.ensureLoaded({
    resolve: { root: true },
  });

  await admin.done();

  return c.json({
    text: root.text,
  });
});

export default app;
