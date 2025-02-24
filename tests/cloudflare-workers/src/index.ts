import { createWebSocketPeer } from "cojson-transport-ws";
import { PureJSCrypto } from "cojson/crypto/PureJSCrypto";
import { Hono } from "hono";
import { startWorker } from "jazz-nodejs";
import { CoMap, co } from "jazz-tools";
import { Account } from "jazz-tools";

const app = new Hono();

class MyAccountRoot extends CoMap {
  text = co.string;
}

class MyAccount extends Account {
  root = co.ref(MyAccountRoot);

  migrate(): void {
    if (this.root === undefined) {
      this.root = MyAccountRoot.create({
        text: "Hello world!",
      });
    }
  }
}

const syncServer = "wss://cloud.jazz.tools/?key=jazz@jazz.tools";

const crypto = await PureJSCrypto.create();

app.get("/", async (c) => {
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

  const admin = await startWorker({
    accountID: account.id,
    accountSecret: account._raw.core.node.account.agentSecret,
    AccountSchema: MyAccount,
    syncServer,
    crypto,
  });

  const { root } = await admin.worker.ensureLoaded({ root: {} });

  await admin.done();

  return c.json({
    text: root.text,
  });
});

export default app;
