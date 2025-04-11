import { waitForCoValueCondition } from "../../internal.js";
import { MagicLinkAuth } from "./MagicLinkAuth.js";
import { MagicLinkAuthConsumerOptions } from "./types.js";

export type MagicLinkAuthCreateAsConsumerStatus =
  | "idle"
  | "waitingForProvider"
  | "confirmationCodeRequired"
  | "confirmationCodePending"
  | "confirmationCodeIncorrect"
  | "authorized"
  | "error";

export class MagicLinkAuthCreateAsConsumer {
  constructor(
    private magicLinkAuth: MagicLinkAuth,
    options?: MagicLinkAuthConsumerOptions,
  ) {
    this.options = { ...defaultOptions, ...options };
  }

  private options: MagicLinkAuthConsumerOptions;

  public authState: {
    status: MagicLinkAuthCreateAsConsumerStatus;
    sendConfirmationCode: null | ((code: string) => void);
  } = {
    status: "idle",
    sendConfirmationCode: null,
  };

  private set status(status: MagicLinkAuthCreateAsConsumerStatus) {
    this.authState = { ...this.authState, status };
  }
  private set sendConfirmationCode(sendConfirmationCode:
    | null
    | ((code: string) => void)) {
    this.authState = { ...this.authState, sendConfirmationCode };
  }

  public async createLink() {
    let transfer = await this.magicLinkAuth.createTransferAsConsumer();

    const url = this.magicLinkAuth.createLink("provider", transfer);

    const handleFlow = async () => {
      try {
        // Wait for the provider to accept the transfer
        this.status = "waitingForProvider";
        this.notify();

        transfer = await waitForCoValueCondition(
          transfer,
          {},
          (t) => Boolean(t.acceptedBy),
          this.options.handlerTimeout,
        );

        this.status = "confirmationCodeRequired";
        this.notify();

        const code = await new Promise<string>((resolve) => {
          this.sendConfirmationCode = (code: string) => resolve(code);
          this.notify();
        });

        transfer.confirmationCodeInput = code;
        this.status = "confirmationCodePending";
        this.notify();

        // Wait for provider to reject or confirm and reveal the secret
        transfer = await waitForCoValueCondition(
          transfer,
          { resolve: {} },
          (t) => t.status === "incorrectCode" || Boolean(t.secret),
          this.options.handlerTimeout,
        );

        if (transfer.status === "incorrectCode") {
          this.status = "confirmationCodeIncorrect";
          this.notify();
          return;
        }

        // Log in using the transfer secret
        await this.magicLinkAuth.logInViaTransfer(transfer);
        this.status = "authorized";
        this.notify();
        this.options.onLoggedIn?.();
      } catch (error) {
        console.error("Magic Link Auth error", error);
        this.status = "error";
        this.notify();
      }
    };

    handleFlow();

    return url;
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
