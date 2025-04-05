import {
  AccountRole,
  CryptoProvider,
  InviteSecret,
  LocalNode,
  base64URLtoBytes,
  bytesToBase64url,
  cojsonInternals,
} from "cojson";
import { Account } from "../coValues/account.js";
import { CoMap, Group } from "../exports.js";
import { ID, co } from "../internal.js";
import { AuthenticateAccountFunction } from "../types.js";
import { AuthSecretStorage } from "./AuthSecretStorage.js";

export class MagicLinkAuthTransfer extends CoMap {
  status = co.literal("pending", "authorized");
  secret = co.optional.string;
  expiresAt = co.optional.Date;
  acceptedBy = co.optional.ref(Account);
}

export interface MagicLinkAuthOptions {
  consumerHandlerPath: string;
  providerHandlerPath: string;
}

const defaultOptions: MagicLinkAuthOptions = {
  consumerHandlerPath: "/magic-link-handler-consumer",
  providerHandlerPath: "/magic-link-handler-provider",
};

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

  public createLink(
    targetHandler: "consumer" | "provider",
    transfer: MagicLinkAuthTransfer,
    role: AccountRole,
  ) {
    let handlerUrl = this.origin + this.options[`${targetHandler}HandlerPath`];

    let transferCore = transfer._raw.core;
    while (transferCore.header.ruleset.type === "ownedByGroup") {
      transferCore = transferCore.getGroup().core;
    }

    const group = cojsonInternals.expectGroup(transferCore.getCurrentContent());
    const inviteSecret = group.createInvite(role);

    return handlerUrl + `/${transfer.id}/${inviteSecret}`;
  }

  public async createTransferAsProvider(expiresAt?: Date) {
    const group = Group.create();

    const transfer = MagicLinkAuthTransfer.create(
      {
        status: "pending",
        expiresAt: expiresAt ?? new Date(Date.now() + 15 * 60 * 1000),
      },
      { owner: group },
    );

    return transfer;
  }

  public async createTransferAsConsumer() {
    const temporaryAgent = await createTemporaryAgent(this.crypto);
    const group = Group.create({ owner: temporaryAgent });

    const transfer = MagicLinkAuthTransfer.create(
      { status: "pending" },
      { owner: group },
    );

    return transfer;
  }

  public async revealSecretToTransfer(
    transfer: MagicLinkAuthTransfer,
    role: AccountRole = "writer",
  ) {
    const credentials = await this.authSecretStorage.get();
    if (!credentials?.secretSeed) {
      throw new Error("No existing authentication found");
    }

    transfer.secret = bytesToBase64url(credentials.secretSeed);

    if (!transfer.acceptedBy) return;
    transfer._owner.castAs(Group).addMember(transfer.acceptedBy, role);
  }

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

function parseMagicLinkAuthUrl(basePath: string, url: string) {
  const re = new RegExp(`${basePath}/(co_z[^/]+)/(inviteSecret_z[^/]+)$`);

  const match = url.match(re);
  if (!match) throw new Error("Invalid URL");

  const transferId = match[1] as ID<MagicLinkAuthTransfer> | undefined;
  const inviteSecret = match[2] as InviteSecret | undefined;

  if (!transferId || !inviteSecret) throw new Error("Invalid URL");

  return { transferId, inviteSecret };
}
