import { Page } from "@playwright/test";
import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import { createWebSocketPeer } from "cojson-transport-ws";
import {
  AuthSecretStorage,
  createJazzContext,
  InMemoryKVStore,
  KvStoreContext,
  randomSessionProvider,
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

export async function addAccount(
  page: Page,
  accountID: string,
  accountSecret: string,
) {
  await page.goto("/");
  await page.getByLabel("Account ID").fill(accountID);
  await page.getByLabel("Account secret").fill(accountSecret);
  await page.getByRole("button", { name: "Add account" }).click();
}

export async function inspectCoValue(page: Page, coValueId: string) {
  await page.goto("/");
  await page.getByLabel("CoValue ID").fill(coValueId);
  await page.getByRole("button", { name: "Inspect CoValue" }).click();
}
