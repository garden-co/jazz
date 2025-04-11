import { waitForCoValueCondition } from "../../internal.js";
import { MagicLinkAuth } from "./MagicLinkAuth.js";
import { MagicLinkAuthProviderOptions } from "./types.js";

export type MagicLinkAuthHandleAsProviderStatus =
  | "idle"
  | "confirmationCodeGenerated"
  | "confirmationCodeIncorrect"
  | "authorized"
  | "error";

export class MagicLinkAuthHandleAsProvider {
  constructor(
    private magicLinkAuth: MagicLinkAuth,
    private url: string,
    options?: MagicLinkAuthProviderOptions,
  ) {
    this.options = { ...defaultOptions, ...options };
  }

  private options: MagicLinkAuthProviderOptions;

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

  public async handleFlow() {
    try {
      let transfer = await this.magicLinkAuth.acceptTransferUrl(
        this.url,
        "provider",
      );

      // Generate and set confirmation code
      const code = await this.magicLinkAuth.createConfirmationCode();
      this.confirmationCode = code;
      this.status = "confirmationCodeGenerated";
      this.notify();

      // Wait for confirmation code input
      transfer = await waitForCoValueCondition(
        transfer,
        {},
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
        {},
        (t) => t.status === "authorized",
      );
      this.status = "authorized";
      this.notify();
    } catch (error) {
      console.error("Magic Link Auth error", error);
      this.status = "error";
      this.notify();
    }
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
