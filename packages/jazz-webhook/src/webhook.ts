import { co, z, Account, Group } from "jazz-tools";
import { CojsonInternalTypes, CoID, RawCoValue } from "cojson";
import {
  getTransactionsToTry,
  getTxIdKey,
  markSuccessful,
  SuccessMap,
  TxIdKey,
} from "./successMap.js";

export const WebhookRegistration = co.map({
  webhookUrl: z.string(),
  coValueId: z.string(),
  active: z.boolean(),
  successMap: SuccessMap,
});
export type WebhookRegistration = co.loaded<typeof WebhookRegistration>;

export const RegistryState = co.record(z.string(), WebhookRegistration);
export type RegistryState = co.loaded<typeof RegistryState>;

export interface JazzWebhookOptions {
  /** Maximum number of retry attempts for failed webhooks (default: 5) */
  maxRetries?: number;
  /** Base delay in milliseconds for exponential backoff (default: 1000ms) */
  baseDelayMs?: number;
}

export const DEFAULT_JazzWebhookOptions: Required<JazzWebhookOptions> = {
  maxRetries: 3,
  baseDelayMs: 20_000,
};

export class WebhookRegistry {
  public state: RegistryState;
  private activeSubscriptions = new Map<string, () => void>();
  private rootUnsubscribe: (() => void) | null = null;

  constructor(
    registry: RegistryState,
    private options: JazzWebhookOptions = {},
  ) {
    this.state = registry;
  }

  static createRegistry(owner: Group | Account): RegistryState {
    return RegistryState.create({}, owner);
  }

  static async loadAndStart(
    registryId: string,
    options: JazzWebhookOptions = {},
  ) {
    const registry = await RegistryState.load(registryId);

    if (!registry) {
      throw new Error(`Webhook registry with ID ${registryId} not found`);
    }

    const webhook = new WebhookRegistry(registry, options);
    webhook.start();
    return webhook;
  }

  /**
   * Registers a new webhook for a CoValue.
   *
   * @param webhookUrl - The HTTP URL to call when the CoValue changes
   * @param coValueId - The ID of the CoValue to monitor (must start with "co_z")
   * @returns The ID of the registered webhook
   */
  async register(webhookUrl: string, coValueId: string): Promise<string> {
    const registrationId = await registerWebhook({
      registryId: this.state.$jazz.id,
      webhookUrl,
      coValueId,
    });

    // wait for registration to become visible in the registry
    return new Promise((resolve) => {
      this.state.$jazz.subscribe((state, unsubscribe) => {
        if (state.$jazz.refs[registrationId]) {
          resolve(registrationId);
          unsubscribe();
        }
      });
    });
  }

  /**
   * Unregisters a webhook and stops monitoring the CoValue.
   *
   * @param webhookId - The ID of the webhook to unregister
   */
  unregister(webhookId: string): void {
    const webhook = this.state[webhookId];
    if (!webhook) {
      throw new Error(`Webhook with ID ${webhookId} not found`);
    }

    this.state.$jazz.delete(webhookId);
    webhook.$jazz.set("active", false);
  }

  /**
   * Starts monitoring a CoValue and emitting webhooks when it changes.
   *
   * @param webhookId - The ID of the webhook to start monitoring
   */
  private subscribe(webhook: WebhookRegistration) {
    const emitter = new WebhookEmitter(webhook, this.options);

    this.activeSubscriptions.set(webhook.$jazz.id, () => {
      emitter.stop();
      this.activeSubscriptions.delete(webhook.$jazz.id);
    });
  }

  /**
   * Starts monitoring all active webhooks in the registry.
   *
   * This method iterates through all webhooks in the registry and starts
   * subscriptions for any active webhooks that don't already have active
   * subscriptions. This is useful for resuming webhook monitoring after
   * a shutdown or when loading an existing registry.
   */
  async start(): Promise<void> {
    if (this.rootUnsubscribe) {
      return;
    }

    // TODO: this would be much more efficient with subscription diffs
    const createAndDeleteSubscriptions = (
      registry: co.loaded<typeof RegistryState, { $each: true }>,
    ) => {
      for (const webhook of Object.values(registry)) {
        const exists = this.activeSubscriptions.has(webhook.$jazz.id);
        if (webhook.active && !exists) {
          this.subscribe(webhook);
        }
      }
      for (const [id, unsubscribe] of this.activeSubscriptions.entries()) {
        if (!registry[id]) {
          unsubscribe();
          this.activeSubscriptions.delete(id);
        }
      }
    };

    this.rootUnsubscribe = this.state.$jazz.subscribe(
      {
        resolve: {
          $each: true,
        },
      },
      createAndDeleteSubscriptions,
    );
  }

  shutdown(): void {
    for (const unsubscribe of this.activeSubscriptions.values()) {
      unsubscribe();
    }
    this.rootUnsubscribe?.();
    this.rootUnsubscribe = null;
  }
}

class WebhookError extends Error {
  constructor(
    message: string,
    public readonly retryAfterMs?: number,
  ) {
    super(message);
  }
}

/**
 * Manages webhook emission with queuing, retry logic, and exponential backoff.
 *
 * The WebhookEmitter handles sending HTTP POST requests to webhook callbacks when
 * a CoValue changes. It implements several key behaviors:
 *
 */
class WebhookEmitter {
  successMap: Promise<SuccessMap> | undefined;
  loadedSuccessMap: SuccessMap | undefined;
  pending = new Map<
    TxIdKey,
    { nRetries: number; timeout?: ReturnType<typeof setTimeout> }
  >();
  unsubscribe: () => void;

  constructor(
    private webhook: WebhookRegistration,
    private options: JazzWebhookOptions = {},
  ) {
    const unsubscribeWebhook = this.webhook.$jazz.subscribe((webhook) => {
      this.webhook = webhook;
    });
    const unsubscribeValue = this.webhook.$jazz.localNode.subscribe(
      this.webhook.coValueId as CoID<RawCoValue>,
      async (value) => {
        if (value === "unavailable") {
          return;
        }
        if (!this.webhook.active) {
          return;
        }
        const todo = getTransactionsToTry(
          await this.loadSuccessMap(),
          value.core.knownState(),
        );
        for (const txID of todo) {
          if (!this.pending.has(getTxIdKey(txID))) {
            this.makeAttempt(txID);
          }
        }
      },
    );

    this.unsubscribe = () => {
      unsubscribeWebhook();
      unsubscribeValue();
    };
  }

  async loadSuccessMap() {
    if (this.loadedSuccessMap) {
      return this.loadedSuccessMap;
    }

    if (!this.successMap) {
      this.successMap = this.webhook.$jazz
        .ensureLoaded({ resolve: { successMap: true } })
        .then((loaded) => loaded.successMap);
    }

    this.loadedSuccessMap = await this.successMap;
    return this.loadedSuccessMap;
  }

  getRetryDelay(nRetries: number) {
    const baseDelayMs =
      this.options.baseDelayMs || DEFAULT_JazzWebhookOptions.baseDelayMs;

    return baseDelayMs * 2 ** nRetries;
  }

  getMaxRetries() {
    return this.options.maxRetries || DEFAULT_JazzWebhookOptions.maxRetries;
  }

  stop() {
    this.pending.forEach((entry) => {
      clearTimeout(entry.timeout);
    });
    this.pending.clear();
    this.unsubscribe();
  }

  makeAttempt(txID: CojsonInternalTypes.TransactionID) {
    const txIdKey = getTxIdKey(txID);
    let entry = this.pending.get(txIdKey);

    if (entry && entry.nRetries >= this.getMaxRetries()) {
      // TODO: should we track failed transactions?
      this.pending.delete(txIdKey);
      clearTimeout(entry.timeout);
      console.error(
        `Max retries reached for txID: ${txIdKey} on webhook: ${this.webhook.$jazz.id}`,
      );
      return;
    }

    if (entry) {
      entry.nRetries++;
      clearTimeout(entry.timeout);
    } else {
      entry = { nRetries: 0 };
      this.pending.set(txIdKey, entry);
    }

    const scheduleRetry = (delayMs?: number) => {
      const delay = delayMs ?? this.getRetryDelay(entry?.nRetries || 0);

      entry.timeout = setTimeout(() => {
        this.makeAttempt(txID);
      }, delay);
    };

    fetch(this.webhook.webhookUrl, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        coValueId: this.webhook.coValueId,
        txID: txID,
      }),
    })
      .then(async (response) => {
        if (response.ok) {
          const successMap = await this.loadSuccessMap();
          markSuccessful(successMap, txID);
          this.pending.delete(txIdKey);
          clearTimeout(entry.timeout);
          return;
        }

        let retryAfterMs: number | undefined;
        if (response.headers.has("Retry-After")) {
          try {
            retryAfterMs =
              parseFloat(response.headers.get("Retry-After")!) * 1000;
          } catch {
            console.warn(
              `Invalid Retry-After header: ${response.headers.get("Retry-After")}`,
            );
          }
        }
        throw new WebhookError(
          `HTTP ${response.status}: ${response.statusText}`,
          retryAfterMs,
        );
      })
      .catch((error) => {
        scheduleRetry(
          error instanceof WebhookError ? error.retryAfterMs : undefined,
        );
      });
  }
}

export const registerWebhook = async (options: {
  webhookUrl: string;
  coValueId: string;
  registryId?: string;
}): Promise<string> => {
  const { webhookUrl, coValueId, registryId } = {
    registryId: process.env.JAZZ_WEBHOOK_REGISTRY_ID,
    ...options,
  };

  if (!registryId) {
    throw new Error("Invalid webhook secret");
  }

  try {
    new URL(webhookUrl);
  } catch {
    throw new Error(`Invalid webhook URL: ${webhookUrl}`);
  }

  if (!registryId.startsWith("co_z")) {
    throw new Error(
      `Invalid Registry ID format: ${coValueId}. Expected format: co_z...`,
    );
  }

  if (!coValueId.startsWith("co_z")) {
    throw new Error(
      `Invalid CoValue ID format: ${coValueId}. Expected format: co_z...`,
    );
  }

  const registry = await RegistryState.load(registryId);

  if (!registry) {
    throw new Error(`Couldn't load registry with ID ${registryId}`);
  }

  const registration = WebhookRegistration.create(
    {
      webhookUrl,
      coValueId,
      active: true,
      successMap: SuccessMap.create({}, registry.$jazz.owner),
    },
    registry.$jazz.owner,
  );

  registry.$jazz.set(registration.$jazz.id, registration);

  await Account.getMe().$jazz.waitForAllCoValuesSync();
  return registration.$jazz.id;
};
