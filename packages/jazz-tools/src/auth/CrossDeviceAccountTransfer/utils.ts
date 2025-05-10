import {
  type CryptoProvider,
  type InviteSecret,
  LocalNode,
  cojsonInternals,
} from "cojson";
import { Account } from "../../coValues/account.js";
import type { ID } from "../../internal.js";
import type { CrossDeviceAccountTransferCoMap } from "./CrossDeviceAccountTransfer.js";
import type { CrossDeviceAccountTransferOptions } from "./types.js";

/**
 * Create a temporary agent to keep the transfer secret isolated from persistent accounts.
 * @param crypto - The crypto provider to use for agent creation.
 * @returns The created Account.
 */
export async function createTemporaryAgent(crypto: CryptoProvider) {
  const { node } = await LocalNode.withNewlyCreatedAccount({
    creationProps: { name: "Sandbox account" },
    peersToLoadFrom: [],
    crypto,
  });
  const account = Account.fromNode(node);

  const [localPeer, authTransferPeer] = cojsonInternals.connectedPeers(
    "local",
    // Use an unique identifier to avoid conflicts with other cross-device account transfer instances
    "crossDeviceAccountTransfer/" + account.id,
    { peer1role: "server", peer2role: "client" },
  );

  Account.getMe()._raw.core.node.syncManager.addPeer(authTransferPeer);
  account._raw.core.node.syncManager.addPeer(localPeer);

  await account.waitForAllCoValuesSync();

  return account;
}

/**
 * Parse a transfer URL.
 * @param handlerPath - The path of the handler.
 * @param url - The URL to parse.
 * @returns The transfer ID and invite secret.
 */
export function parseTransferUrl(handlerPath: string, url: string) {
  const re = new RegExp(`${handlerPath}/(co_z[^/]+)/(inviteSecret_z[^/]+)$`);

  const match = url.match(re);
  if (!match) throw new Error("Invalid URL");

  const transferId = match[1] as
    | ID<CrossDeviceAccountTransferCoMap>
    | undefined;
  const inviteSecret = match[2] as InviteSecret | undefined;

  if (!transferId || !inviteSecret) throw new Error("Invalid URL");

  return { transferId, inviteSecret };
}

/**
 * Default function to generate a 6-digit confirmation code.
 * @param crypto - The crypto provider to use for random number generation.
 * @returns The generated confirmation code.
 */
async function defaultConfirmationCodeFn(crypto: CryptoProvider) {
  let code = "";
  while (code.length < 6) {
    // value is 0-15
    const value = crypto.randomBytes(1)[0]! & 0x0f;
    // discard values >=10 for uniform distribution 0-9
    if (value >= 10) continue;
    code += value.toString();
  }
  return code;
}

export function shutdownTransferAccount(
  transfer: CrossDeviceAccountTransferCoMap | undefined,
) {
  if (!transfer || transfer._loadedAs._type !== "Account") return;
  transfer._loadedAs._raw.core.node.gracefulShutdown();
}

export const defaultOptions: CrossDeviceAccountTransferOptions = {
  confirmationCodeFn: defaultConfirmationCodeFn,
  handlerPath: "/accept-account-transfer",
};
