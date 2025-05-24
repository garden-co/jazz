import { waitForCoValueCondition } from "../../internal.js";
import { CrossDeviceAccountTransfer } from "./CrossDeviceAccountTransfer.js";
import { CrossDeviceAccountTransferAsSourceOptions } from "./types.js";
import { shutdownTransferAccount } from "./utils.js";

export type CrossDeviceAccountTransferCreateAsSourceStatus =
  | "idle"
  | "waitingForHandler"
  | "confirmationCodeGenerated"
  | "confirmationCodeCorrect"
  | "confirmationCodeIncorrect"
  | "authorized"
  | "error"
  | "cancelled";

export class CrossDeviceAccountTransferCreateAsSource {
  constructor(
    private crossDeviceAccountTransfer: CrossDeviceAccountTransfer,
    options?: CrossDeviceAccountTransferAsSourceOptions,
  ) {
    this.options = { ...defaultOptions, ...options };
  }

  private options: CrossDeviceAccountTransferAsSourceOptions;
  private abortController: AbortController | null = null;

  public authState: {
    status: CrossDeviceAccountTransferCreateAsSourceStatus;
    confirmationCode: string | undefined;
  } = {
    status: "idle",
    confirmationCode: undefined,
  };

  private set status(status: CrossDeviceAccountTransferCreateAsSourceStatus) {
    this.authState = { ...this.authState, status };
  }
  private set confirmationCode(confirmationCode: string | undefined) {
    this.authState = { ...this.authState, confirmationCode };
  }

  public createLink = async () => {
    this.abortController = new AbortController();
    const { signal } = this.abortController;

    let transfer = await this.crossDeviceAccountTransfer.createTransfer();

    const url = this.crossDeviceAccountTransfer.createLink(transfer);

    const handleFlow = async () => {
      try {
        // Wait for target device to accept the transfer
        this.status = "waitingForHandler";
        this.notify();

        transfer = await waitForCoValueCondition(
          transfer,
          { abortSignal: signal },
          (t) => Boolean(t.acceptedBy),
          this.options.expireInMs,
        );

        if (!transfer.acceptedBy) throw new Error("Transfer not accepted");

        // Wait for confirmation code
        const code =
          await this.crossDeviceAccountTransfer.createConfirmationCode();
        this.confirmationCode = code;
        this.status = "confirmationCodeGenerated";
        this.notify();

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
        this.status = "confirmationCodeCorrect";

        // Reveal the secret to the transfer
        await this.crossDeviceAccountTransfer.revealSecretToTransfer(transfer);

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
  };

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

const defaultOptions: CrossDeviceAccountTransferAsSourceOptions = {
  expireInMs: 15 * 60 * 1000,
};
