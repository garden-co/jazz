import { createWebSocketPeer } from "cojson-transport-ws";
import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import {
  AuthSecretStorage,
  InMemoryKVStore,
  KvStoreContext,
  co,
  createJazzContext,
  randomSessionProvider,
  z,
} from "jazz-tools";

export const initializeKvStore = () => {
  const kvStore = new InMemoryKVStore();
  KvStoreContext.getInstance().initialize(kvStore);
};

export async function createAccount() {
  const { account, authSecretStorage } = await createJazzContext({
    defaultProfileName: "Inspector test account",
    crypto: await WasmCrypto.create(),
    sessionProvider: randomSessionProvider,
    authSecretStorage: new AuthSecretStorage(),
    peersToLoadFrom: [
      createWebSocketPeer({
        id: "upstream",
        role: "server",
        websocket: new WebSocket(
          "wss://cloud.jazz.tools/?key=inspector-test@jazz.tools",
        ),
      }),
    ],
  });

  await account.waitForAllCoValuesSync();

  const credentials = await authSecretStorage.get();
  if (!credentials) {
    throw new Error("No credentials found");
  }

  return { account, ...credentials };
}
