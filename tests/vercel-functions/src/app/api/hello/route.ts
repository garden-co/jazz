import "jazz-tools/load-edge-wasm";
import { createWebSocketPeer } from "cojson-transport-ws";
import { CoMap, coField } from "jazz-tools";
import { Account } from "jazz-tools";
import { startWorker } from "jazz-tools/worker";
import { NextResponse } from "next/server";
import { WasmCrypto } from "cojson/crypto/WasmCrypto";

class MyAccountRoot extends CoMap {
  text = coField.string;
}
class MyAccount extends Account {
  root = coField.ref(MyAccountRoot);

  migrate(): void {
    if (this.root === undefined) {
      //@ts-ignore
      this.$jazz.set("root", {
        text: "Hello world!",
      });
    }
  }
}

export const runtime = "edge"; // 'nodejs' is the default

const syncServer = "wss://cloud.jazz.tools/?key=jazz@jazz.tools";

export async function GET(request: Request) {
  const crypto = await WasmCrypto.create();

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

  return NextResponse.json({
    text: root.text,
    isWasmCrypto: crypto instanceof WasmCrypto,
  });
}
