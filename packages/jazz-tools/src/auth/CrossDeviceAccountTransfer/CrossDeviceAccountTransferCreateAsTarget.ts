import { waitForCoValueCondition } from "../../internal.js";
import { CrossDeviceAccountTransfer } from "./CrossDeviceAccountTransfer.js";
import { CrossDeviceAccountTransferAsTargetOptions } from "./types.js";
import { shutdownTransferAccount } from "./utils.js";

export type CrossDeviceAccountTransferCreateAsTargetStatus =
  | "idle"
  | "waitingForHandler"
  | "confirmationCodeRequired"
  | "confirmationCodePending"
  | "confirmationCodeIncorrect"
  | "authorized"
  | "error"
  | "cancelled";

export class CrossDeviceAccountTransferCreateAsTarget {
  constructor(
    private crossDeviceAccountTransfer: CrossDeviceAccountTransfer,
    options?: CrossDeviceAccountTransferAsTargetOptions,
  ) {
    this.options = { ...defaultOptions, ...options };
  }

  private options: CrossDeviceAccountTransferAsTargetOptions;
  private abortController: AbortController | null = null;

  public authState: {
    status: CrossDeviceAccountTransferCreateAsTargetStatus;
    sendConfirmationCode: undefined | ((code: string) => void);
  } = {
    status: "idle",
    sendConfirmationCode: undefined,
  };

  private set status(status: CrossDeviceAccountTransferCreateAsTargetStatus) {
    this.authState = { ...this.authState, status };
  }
  private set sendConfirmationCode(sendConfirmationCode:
    | undefined
    | ((code: string) => void)) {
    this.authState = { ...this.authState, sendConfirmationCode };
  }

  public async createLink() {
    this.abortController = new AbortController();
    const { signal } = this.abortController;

    let transfer = await this.crossDeviceAccountTransfer.createTransfer();

    const url = this.crossDeviceAccountTransfer.createLink(transfer);

    const handleFlow = async () => {
      try {
        // Wait for the source device to accept the transfer
        this.status = "waitingForHandler";
        this.notify();

        transfer = await waitForCoValueCondition(
          transfer,
          { abortSignal: signal },
          (t) => Boolean(t.acceptedBy),
          this.options.handlerTimeout,
        );

        this.status = "confirmationCodeRequired";
        this.notify();

        const code = await new Promise<string>((resolve, reject) => {
          this.sendConfirmationCode = (code: string) => resolve(code);
          signal.addEventListener("abort", () => reject(new Error("Aborted")));
        });

        transfer.confirmationCodeInput = code;
        this.status = "confirmationCodePending";
        this.notify();

        // Wait for source device to reject or confirm and reveal the secret
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

        // Log in using the transfer secret
        await this.crossDeviceAccountTransfer.logInViaTransfer(transfer);
        this.status = "authorized";
        this.notify();
        this.options.onLoggedIn?.();
      } catch (error) {
        if (error instanceof Error && error.message.startsWith("Aborted")) {
          this.status = "cancelled";
        } else {
          console.error("Cross-Device Account Transfer error", error);
          this.status = "error";
        }
        this.notify();
      } finally {
        this.abortController = null;
        shutdownTransferAccount(transfer);
      }
    };

    handleFlow();

    return url;
  }

  public cancelFlow() {
    this.abortController?.abort();
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

const defaultOptions: CrossDeviceAccountTransferAsTargetOptions = {
  handlerTimeout: 15 * 60 * 1000,
};
