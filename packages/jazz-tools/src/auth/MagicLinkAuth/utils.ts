import {
  type CryptoProvider,
  type InviteSecret,
  LocalNode,
  cojsonInternals,
} from "cojson";
import { Account } from "../../coValues/account.js";
import type { MagicLinkAuthTransfer } from "./MagicLinkAuth.js";
import type { MagicLinkAuthOptions } from "./types.js";
import type { ID } from "../../internal.js";

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

  const [localPeer, magicLinkAuthPeer] = cojsonInternals.connectedPeers(
    "local",
    "magicLinkAuth/" + account.id, // Use an unique identifier to avoid conflicts with other magic link auth instances
    { peer1role: "server", peer2role: "client" },
  );

  Account.getMe()._raw.core.node.syncManager.addPeer(magicLinkAuthPeer);
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

  const transferId = match[1] as ID<MagicLinkAuthTransfer> | undefined;
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
  transfer: MagicLinkAuthTransfer | undefined,
) {
  if (!transfer || transfer._loadedAs._type !== "Account") return;
  transfer._loadedAs._raw.core.node.gracefulShutdown();
}

export const defaultOptions: MagicLinkAuthOptions = {
  confirmationCodeFn: defaultConfirmationCodeFn,
  consumerHandlerPath: "/magic-link-handler-consumer",
  providerHandlerPath: "/magic-link-handler-provider",
};
