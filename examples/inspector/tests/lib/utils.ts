import type { Page } from "@playwright/test";
import { createWebSocketPeer } from "cojson-transport-ws";
import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import {
  AuthSecretStorage,
  InMemoryKVStore,
  KvStoreContext,
  createJazzContext,
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
    peers: [
      createWebSocketPeer({
        id: "upstream",
        role: "server",
        websocket: new WebSocket(
          "wss://cloud.jazz.tools/?key=inspector-test@jazz.tools",
        ),
      }),
    ],
  });

  await account.$jazz.waitForAllCoValuesSync();

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
