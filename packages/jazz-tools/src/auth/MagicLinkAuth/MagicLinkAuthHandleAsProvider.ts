import { waitForCoValueCondition } from "../../internal.js";
import { MagicLinkAuth, MagicLinkAuthTransfer } from "./MagicLinkAuth.js";
import { MagicLinkAuthProviderOptions } from "./types.js";
import { shutdownTransferAccount } from "./utils.js";

export type MagicLinkAuthHandleAsProviderStatus =
  | "idle"
  | "confirmationCodeGenerated"
  | "confirmationCodeIncorrect"
  | "authorized"
  | "error"
  | "cancelled";

export class MagicLinkAuthHandleAsProvider {
  constructor(
    private magicLinkAuth: MagicLinkAuth,
    options?: MagicLinkAuthProviderOptions,
  ) {
    this.options = { ...defaultOptions, ...options };
  }

  private options: MagicLinkAuthProviderOptions;
  private abortController: AbortController | null = null;

  public authState: {
    status: MagicLinkAuthHandleAsProviderStatus;
    confirmationCode: string | undefined;
  } = {
    status: "idle",
    confirmationCode: undefined,
  };

  private set status(status: MagicLinkAuthHandleAsProviderStatus) {
    this.authState = { ...this.authState, status };
  }
  private set confirmationCode(confirmationCode: string | undefined) {
    this.authState = { ...this.authState, confirmationCode };
  }

  public async handleFlow(url: string) {
    this.abortController = new AbortController();
    const { signal } = this.abortController;

    let transfer: MagicLinkAuthTransfer | undefined;

    try {
      transfer = await this.magicLinkAuth.acceptTransferUrl(url, "provider");

      // Generate and set confirmation code
      const code = await this.magicLinkAuth.createConfirmationCode();
      this.confirmationCode = code;
      this.status = "confirmationCodeGenerated";
      this.notify();

      // Wait for confirmation code input
      transfer = await waitForCoValueCondition(
        transfer,
        { abortSignal: signal },
        (t) => Boolean(t.confirmationCodeInput),
        this.options.expireInMs,
      );

      // Check if the confirmation code is correct
      if (transfer.confirmationCodeInput !== code) {
        transfer.status = "incorrectCode";
        await transfer.waitForSync();
        this.status = "confirmationCodeIncorrect";
        this.notify();
        return;
      }

      // Reveal the secret to the transfer
      await this.magicLinkAuth.revealSecretToTransfer(transfer);

      // Wait for the transfer to be authorized and update the status
      await waitForCoValueCondition(
        transfer,
        { abortSignal: signal },
        (t) => t.status === "authorized",
      );
      this.status = "authorized";
      this.notify();
    } catch (error) {
      if (error instanceof Error && error.message.startsWith("Aborted")) {
        this.status = "cancelled";
      } else {
        console.error("Magic Link Auth error", error);
        this.status = "error";
      }
      this.notify();
    } finally {
      this.abortController = null;
      shutdownTransferAccount(transfer);
    }
  }

  public cancelFlow() {
    this.abortController?.abort();
  }

  public checkValidUrl(url: string) {
    return this.magicLinkAuth.checkValidUrl(url, "provider");
  }

  listeners = new Set<() => void>();
  subscribe = (callback: () => void): (() => void) => {
    this.listeners.add(callback);

    return () => {
      this.listeners.delete(callback);
    };
  };
  notify() {
    for (const listener of this.listeners) {
      listener();
    }
  }
}

const defaultOptions: MagicLinkAuthProviderOptions = {
  expireInMs: 15 * 60 * 1000,
};
