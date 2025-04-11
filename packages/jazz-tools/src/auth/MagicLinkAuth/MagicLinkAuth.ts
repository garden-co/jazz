import {
  CryptoProvider,
  InviteSecret,
  LocalNode,
  base64URLtoBytes,
  bytesToBase64url,
  cojsonInternals,
} from "cojson";
import { Account } from "../../coValues/account.js";
import { CoMap, Group } from "../../exports.js";
import { ID, co } from "../../internal.js";
import { AuthenticateAccountFunction } from "../../types.js";
import { AuthSecretStorage } from "../AuthSecretStorage.js";

export class MagicLinkAuthTransfer extends CoMap {
  status = co.literal("pending", "incorrectCode", "authorized");
  secret = co.optional.string;
  acceptedBy = co.optional.ref(Account);
  confirmationCodeInput = co.optional.string;
}

export interface MagicLinkAuthOptions {
  confirmationCodeFn: (crypto: CryptoProvider) => string | Promise<string>;
  consumerHandlerPath: string;
  providerHandlerPath: string;
}

/**
 * `MagicLinkAuth` provides a `JazzAuth` object for secret URL authentication. Good for use in a QR code.
 *
 * ```ts
 * import { MagicLinkAuth } from "jazz-tools";
 *
 * const auth = new MagicLinkAuth(crypto, jazzContext.authenticate, new AuthSecretStorage(), window.location.origin, options);
 * ```
 *
 * @category Auth Providers
 */
export class MagicLinkAuth {
  constructor(
    private crypto: CryptoProvider,
    private authenticate: AuthenticateAccountFunction,
    private authSecretStorage: AuthSecretStorage,
    private origin: string,
    options?: Partial<MagicLinkAuthOptions>,
  ) {
    this.options = { ...defaultOptions, ...options };
  }

  private options: MagicLinkAuthOptions;

  /**
   * Creates a magic link URL for authentication.
   * @param targetHandler - Specifies whether the link should be handled by consumer or provider.
   * @param transfer - The MagicLinkAuthTransfer to create the link for.
   * @returns A URL that can be displayed as a QR code to be scanned by the handler.
   */
  public createLink(
    targetHandler: "consumer" | "provider",
    transfer: MagicLinkAuthTransfer,
  ) {
    let handlerUrl = this.origin + this.options[`${targetHandler}HandlerPath`];

    let transferCore = transfer._raw.core;
    while (transferCore.header.ruleset.type === "ownedByGroup") {
      transferCore = transferCore.getGroup().core;
    }

    const group = cojsonInternals.expectGroup(transferCore.getCurrentContent());
    const inviteSecret = group.createInvite("writer");

    return handlerUrl + `/${transfer.id}/${inviteSecret}`;
  }

  /**
   * Creates a confirmation code using the configured function.
   * @returns The generated confirmation code.
   */
  public async createConfirmationCode() {
    return await this.options.confirmationCodeFn(this.crypto);
  }

  /**
   * Creates a transfer as the auth provider.
   * @returns The created MagicLinkAuthTransfer.
   */
  public async createTransferAsProvider() {
    const group = Group.create();

    const transfer = MagicLinkAuthTransfer.create(
      { status: "pending" },
      { owner: group },
    );

    return transfer;
  }

  /**
   * Creates a transfer as the auth consumer.
   * @returns The created MagicLinkAuthTransfer.
   */
  public async createTransferAsConsumer() {
    const temporaryAgent = await createTemporaryAgent(this.crypto);
    const group = Group.create({ owner: temporaryAgent });

    const transfer = MagicLinkAuthTransfer.create(
      { status: "pending" },
      { owner: group },
    );

    return transfer;
  }

  /**
   * Load the secret seed from auth storage and reveal it to the transfer.
   * @param transfer - The MagicLinkAuthTransfer to reveal the secret to.
   */
  public async revealSecretToTransfer(transfer: MagicLinkAuthTransfer) {
    const credentials = await this.authSecretStorage.get();
    if (!credentials?.secretSeed) {
      throw new Error("No existing authentication found");
    }

    transfer.secret = bytesToBase64url(credentials.secretSeed);
  }

  /**
   * Log in via a transfer secret.
   * @param transfer - The MagicLinkAuthTransfer with the secret to log in with.
   */
  public async logInViaTransfer(transfer: MagicLinkAuthTransfer) {
    const secret = transfer.secret;
    if (!secret) throw new Error("Transfer secret not set");
    transfer.status = "authorized";
    await transfer.waitForSync();

    (transfer._loadedAs as Account)._raw.core.node.gracefulShutdown();

    const secretSeed = base64URLtoBytes(secret);
    const accountSecret = this.crypto.agentSecretFromSecretSeed(secretSeed);

    const accountID = cojsonInternals.idforHeader(
      cojsonInternals.accountHeaderForInitialAgentSecret(
        accountSecret,
        this.crypto,
      ),
      this.crypto,
    ) as ID<Account>;

    await this.authenticate({ accountID, accountSecret });

    await this.authSecretStorage.set({
      accountID,
      secretSeed,
      accountSecret,
      provider: "magicLink",
    });
  }

  /**
   * Accept a transfer from a URL.
   * @param url - The URL to accept the transfer from.
   * @param targetHandler - Specifies whether the URL is for consumer or provider.
   * @returns The accepted MagicLinkAuthTransfer.
   */
  public async acceptTransferUrl(
    url: string,
    targetHandler: "consumer" | "provider",
  ) {
    const { transferId, inviteSecret } = parseMagicLinkAuthUrl(
      this.options[`${targetHandler}HandlerPath`],
      url,
    );

    const account =
      targetHandler === "consumer"
        ? await createTemporaryAgent(this.crypto)
        : Account.getMe();

    const transfer = await account.acceptInvite(
      transferId,
      inviteSecret,
      MagicLinkAuthTransfer,
    );
    if (!transfer) throw new Error("Failed to accept invite");

    transfer.acceptedBy = account;

    return transfer;
  }
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

const defaultOptions: MagicLinkAuthOptions = {
  confirmationCodeFn: defaultConfirmationCodeFn,
  consumerHandlerPath: "/magic-link-handler-consumer",
  providerHandlerPath: "/magic-link-handler-provider",
};

/**
 * Create a temporary agent to keep the transfer secret isolated from persistent accounts.
 * @param crypto - The crypto provider to use for agent creation.
 * @returns The created Account.
 */
async function createTemporaryAgent(crypto: CryptoProvider) {
  const [localPeer, magicLinkAuthPeer] = cojsonInternals.connectedPeers(
    "local",
    "magicLinkAuth",
    { peer1role: "server", peer2role: "client" },
  );
  Account.getMe()._raw.core.node.syncManager.addPeer(magicLinkAuthPeer);

  const { node } = await LocalNode.withNewlyCreatedAccount({
    creationProps: { name: "Sandbox account" },
    peersToLoadFrom: [localPeer],
    crypto,
  });
  return Account.fromNode(node);
}

/**
 * Parse a magic link URL to extract the transfer ID and invite secret.
 * @param basePath - The base path of the magic link handler.
 * @param url - The URL to parse.
 * @returns The extracted transfer ID and invite secret.
 */
function parseMagicLinkAuthUrl(basePath: string, url: string) {
  const re = new RegExp(`${basePath}/(co_z[^/]+)/(inviteSecret_z[^/]+)$`);

  const match = url.match(re);
  if (!match) throw new Error("Invalid URL");

  const transferId = match[1] as ID<MagicLinkAuthTransfer> | undefined;
  const inviteSecret = match[2] as InviteSecret | undefined;

  if (!transferId || !inviteSecret) throw new Error("Invalid URL");

  return { transferId, inviteSecret };
}
