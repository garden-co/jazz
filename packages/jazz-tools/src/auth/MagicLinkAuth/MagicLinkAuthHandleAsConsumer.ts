import { waitForCoValueCondition } from "../../internal.js";
import { MagicLinkAuth, MagicLinkAuthTransfer } from "./MagicLinkAuth.js";
import { MagicLinkAuthConsumerOptions } from "./types.js";
import { shutdownTransferAccount } from "./utils.js";

export type MagicLinkAuthHandleAsConsumerStatus =
  | "idle"
  | "confirmationCodeRequired"
  | "confirmationCodePending"
  | "confirmationCodeIncorrect"
  | "authorized"
  | "error"
  | "cancelled";

export class MagicLinkAuthHandleAsConsumer {
  constructor(
    private magicLinkAuth: MagicLinkAuth,
    options?: MagicLinkAuthConsumerOptions,
  ) {
    this.options = { ...defaultOptions, ...options };
  }

  private options: MagicLinkAuthConsumerOptions;
  private abortController: AbortController | null = null;

  public authState: {
    status: MagicLinkAuthHandleAsConsumerStatus;
    sendConfirmationCode: null | ((code: string) => void);
  } = {
    status: "idle",
    sendConfirmationCode: null,
  };

  private set status(status: MagicLinkAuthHandleAsConsumerStatus) {
    this.authState = { ...this.authState, status };
  }
  private set sendConfirmationCode(sendConfirmationCode:
    | null
    | ((code: string) => void)) {
    this.authState = { ...this.authState, sendConfirmationCode };
  }

  public async handleFlow(url: string) {
    this.abortController = new AbortController();
    const { signal } = this.abortController;

    let transfer: MagicLinkAuthTransfer | undefined;

    try {
      transfer = await this.magicLinkAuth.acceptTransferUrl(url, "consumer");

      this.status = "confirmationCodeRequired";
      this.notify();

      const code = await new Promise<string>((resolve, reject) => {
        this.sendConfirmationCode = (code: string) => resolve(code);
        signal.addEventListener("abort", () => reject(new Error("Aborted")));
      });

      transfer.confirmationCodeInput = code;
      this.status = "confirmationCodePending";
      this.notify();

      // Wait for provider to reject or confirm and reveal the secret
      transfer = await waitForCoValueCondition(
        transfer,
        { resolve: {}, abortSignal: signal },
        (t) => t.status === "incorrectCode" || Boolean(t.secret),
        this.options.handlerTimeout,
      );

      if (transfer.status === "incorrectCode") {
        this.status = "confirmationCodeIncorrect";
        this.notify();
        return;
      }

      await this.magicLinkAuth.logInViaTransfer(transfer);
      this.status = "authorized";
      this.notify();
      this.options.onLoggedIn?.();
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
    return this.magicLinkAuth.checkValidUrl(url, "consumer");
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

const defaultOptions: MagicLinkAuthConsumerOptions = {
  handlerTimeout: 30 * 1000,
};
