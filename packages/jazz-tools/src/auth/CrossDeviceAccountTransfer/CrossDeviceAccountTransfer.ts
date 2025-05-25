import {
  CryptoProvider,
  InviteSecret,
  base64URLtoBytes,
  bytesToBase64url,
  cojsonInternals,
} from "cojson";
import { Account } from "../../coValues/account.js";
import { CoMap, Group } from "../../exports.js";
import { ID, co } from "../../internal.js";
import { AuthenticateAccountFunction } from "../../types.js";
import { AuthSecretStorage } from "../AuthSecretStorage.js";
import { CrossDeviceAccountTransferOptions } from "./types.js";
import {
  createTemporaryAgent,
  defaultOptions,
  parseTransferUrl,
  shutdownTransferAccount,
} from "./utils.js";

export class CrossDeviceAccountTransferCoMap extends CoMap {
  status = co.literal("pending", "incorrectCode", "authorized");
  secret = co.optional.string;
  acceptedBy = co.optional.ref(Account);
  confirmationCodeInput = co.optional.string;
}

/**
 * `CrossDeviceAccountTransfer` provides a `JazzAuth` object for secret URL authentication. Good for use in a QR code.
 *
 * ```ts
 * import { CrossDeviceAccountTransfer } from "jazz-tools";
 *
 * const auth = new CrossDeviceAccountTransfer(crypto, jazzContext.authenticate, new AuthSecretStorage(), window.location.origin, options);
 * ```
 *
 * @category Auth Providers
 */
export class CrossDeviceAccountTransfer {
  constructor(
    private crypto: CryptoProvider,
    private authenticate: AuthenticateAccountFunction,
    private authSecretStorage: AuthSecretStorage,
    private origin: string,
    options?: Partial<CrossDeviceAccountTransferOptions>,
  ) {
    this.options = { ...defaultOptions, ...options };
  }

  private options: CrossDeviceAccountTransferOptions;

  /**
   * Creates a transfer URL for authentication.
   * @param transfer - The CrossDeviceAccountTransferCoMap to create the link for.
   * @returns A URL that can be displayed as a QR code to be scanned by the handler.
   */
  public createLink(transfer: CrossDeviceAccountTransferCoMap) {
    let handlerUrl = this.origin + this.options.handlerPath;

    let transferCore = transfer._raw.core;
    while (transferCore.verified.header.ruleset.type === "ownedByGroup") {
      transferCore = transferCore.getGroup().core;
    }

    const group = cojsonInternals.expectGroup(transferCore.getCurrentContent());
    const inviteSecret = group.createInvite("writer");

    const url = handlerUrl + `/${transfer.id}/${inviteSecret}`;

    if (!url.includes("#")) {
      console.warn(
        "CrossDeviceAccountTransfer: URL does not include # - consider using a hash fragment to avoid leaking the transfer secret",
      );
    }

    return url;
  }

  /**
   * Creates a confirmation code using the configured function.
   * @returns The generated confirmation code.
   */
  public async createConfirmationCode() {
    return await this.options.confirmationCodeFn(this.crypto);
  }

  /**
   * Creates a transfer as a temporary agent.
   * @returns The created CrossDeviceAccountTransferCoMap.
   */
  public async createTransfer() {
    const temporaryAgent = await createTemporaryAgent(this.crypto);
    const group = Group.create({ owner: temporaryAgent });

    const transfer = CrossDeviceAccountTransferCoMap.create(
      { status: "pending" },
      { owner: group },
    );

    return transfer;
  }

  /**
   * Load the secret seed from auth storage and reveal it to the transfer.
   * @param transfer - The CrossDeviceAccountTransferCoMap to reveal the secret to.
   */
  public async revealSecretToTransfer(
    transfer: CrossDeviceAccountTransferCoMap,
  ) {
    const credentials = await this.authSecretStorage.get();
    if (!credentials?.secretSeed) {
      throw new Error("No existing authentication found");
    }

    transfer.secret = bytesToBase64url(credentials.secretSeed);
  }

  /**
   * Log in via a transfer secret.
   * @param transfer - The CrossDeviceAccountTransferCoMap with the secret to log in with.
   */
  public async logInViaTransfer(transfer: CrossDeviceAccountTransferCoMap) {
    const secret = transfer.secret;
    if (!secret) throw new Error("Transfer secret not set");
    transfer.status = "authorized";
    await transfer.waitForSync();
    shutdownTransferAccount(transfer);

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
      provider: "crossDeviceAccountTransfer",
    });
  }

  /**
   * Accept a transfer from a URL.
   * @param url - The URL to accept the transfer from.
   * @param handler - Specifies whether the URL is for consumer or provider.
   * @returns The accepted CrossDeviceAccountTransferCoMap.
   */
  public async acceptTransferUrl(url: string) {
    const { transferId, inviteSecret } = parseTransferUrl(
      this.options.handlerPath,
      url,
    );

    const account = await createTemporaryAgent(this.crypto);

    const transfer = await account.acceptInvite(
      transferId,
      inviteSecret,
      CrossDeviceAccountTransferCoMap,
    );
    if (!transfer) throw new Error("Failed to accept invite");

    transfer.acceptedBy = account;

    return transfer;
  }

  /**
   * Check if a URL is a valid transfer URL.
   * @param url - The URL to check.
   * @param handler - Specifies whether the URL is for source or target.
   * @returns True if the URL is a valid transfer URL, false otherwise.
   */
  public checkValidUrl(url: string, handler: "source" | "target") {
    try {
      parseTransferUrl(this.options.handlerPath, url);
      return true;
    } catch (error) {
      return false;
    }
  }
}
