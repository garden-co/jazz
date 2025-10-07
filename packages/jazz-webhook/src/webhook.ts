import { co, z, Account, Group } from "jazz-tools";

export const LastSuccessfulEmit = co.map({
  v: z.number(),
});
export type LastSuccessfulEmit = co.loaded<typeof LastSuccessfulEmit>;

export const WebhookRegistration = co.map({
  webhookUrl: z.string(),
  coValueId: z.string(),
  active: z.boolean(),
  lastSuccessfulEmit: LastSuccessfulEmit,
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
  register(webhookUrl: string, coValueId: string): Promise<string> {
    return registerWebhook(this.state.$jazz.id, webhookUrl, coValueId);
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
    const coValueId = webhook.coValueId as `co_z${string}`;

    if (!coValueId.startsWith("co_z")) {
      return;
    }

    const localNode = webhook.$jazz.localNode;
    const coValue = localNode.getCoValue(coValueId);

    if (!coValue.isAvailable()) {
      localNode.loadCoValueCore(coValueId);
    }

    const emitter = new WebhookEmitter(webhook, this.options);

    const unsubscribe = coValue.subscribe(() => {
      if (!coValue.isAvailable()) return;

      const updates = Object.values(coValue.knownState().sessions).reduce(
        (acc, tx) => {
          return acc + tx;
        },
        0,
      );

      emitter.schedule(updates);
    });

    this.activeSubscriptions.set(webhook.$jazz.id, () => {
      unsubscribe();
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
        resolve: { $each: true },
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

/**
 * Manages webhook emission with queuing, retry logic, and exponential backoff.
 *
 * The WebhookEmitter handles sending HTTP POST requests to webhook callbacks when
 * a CoValue changes. It implements several key behaviors:
 *
 * - **Queuing**: If a webhook is already being sent, new changes are queued and
 *   only the latest hash is emitted (older hashes are discarded)
 * - **Retry Logic**: Failed webhooks are retried up to 5 times with exponential
 *   backoff (1s, 2s, 4s, 8s, 16s delays)
 * - **State Tracking**: Successfully emitted hashes are stored in lastSuccessfulEmit
 * - **Graceful Shutdown**: Can be stopped to clean up timers and prevent new emissions
 */
class WebhookEmitter {
  /** The next hash to be emitted, null if no queued emissions */
  private nextSchedule: number | null = null;
  /** Whether a webhook is currently being sent */
  private running: boolean = false;
  /** Timeout ID for retry scheduling, null if no retry scheduled */
  private retryTimeoutId: NodeJS.Timeout | null = null;

  constructor(
    private webhook: WebhookRegistration,
    private options: JazzWebhookOptions = {},
  ) {
    this.initialize();
  }

  /** Last successful emit hash */
  private lastSuccessfulEmit: LastSuccessfulEmit | null = null;

  async initialize() {
    const { lastSuccessfulEmit } = await this.webhook.$jazz.ensureLoaded({
      resolve: {
        lastSuccessfulEmit: true,
      },
    });

    this.lastSuccessfulEmit = lastSuccessfulEmit;

    this.runNextSchedule();
  }

  /** Number of consecutive failures for the current webhook attempt */
  failures = 0;
  /** Maximum number of retry attempts before giving up */
  private readonly maxRetries = this.options.maxRetries ?? 5;
  /** Base delay in milliseconds for exponential backoff (1000ms = 1 second) */
  private readonly baseDelayMs = this.options.baseDelayMs ?? 1000;

  /**
   * Schedules a webhook emission for the given hash.
   *
   * If a webhook is already being sent, the new hash is queued and will replace
   * any previously queued hash. Only the most recent hash is emitted to avoid
   * overwhelming the webhook endpoint with outdated notifications.
   *
   * @param hash - The CoValue state hash to emit in the webhook payload
   */
  schedule(updates: number) {
    this.nextSchedule = updates;

    if (this.running) {
      return;
    }

    this.runNextSchedule();
  }

  /**
   * Sends the webhook HTTP request for the given hash.
   *
   * Makes a POST request to the webhook callback URL with the CoValue ID, hash,
   * and timestamp. On success, updates the lastSuccessfulEmit and processes any
   * queued emissions. On failure, increments the failure count and schedules a retry.
   *
   * @param hash - The CoValue state hash to send in the webhook payload
   */
  private async emitWebhook(updates: number): Promise<void> {
    try {
      const response = await fetch(this.webhook.webhookUrl, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          coValueId: this.webhook.coValueId,
          updates: updates,
        }),
      });

      if (response.ok) {
        this.trackSuccess(updates);
        this.runNextSchedule();
      } else {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }
    } catch (error) {
      this.failures++;
      console.error(
        `Webhook emission failed (attempt ${this.failures}):`,
        error,
      );

      if (!this.nextSchedule) {
        this.nextSchedule = updates;
      }
      this.scheduleRetry();
    }
  }

  /**
   * Schedules a retry with exponential backoff delay.
   *
   * Uses exponential backoff with the formula: baseDelayMs * 2^(failures-1)
   * This results in delays of: 1s, 2s, 4s, 8s, 16s for attempts 1-5.
   * After maxRetries attempts, gives up and logs an error.
   */
  private scheduleRetry() {
    if (this.failures >= this.maxRetries) {
      console.error(
        `Webhook emission failed after ${this.maxRetries} attempts, giving up`,
      );
      return;
    }

    const delay = this.baseDelayMs * Math.pow(2, this.failures - 1);
    console.log(`Scheduling retry ${this.failures} in ${delay}ms`);

    this.retryTimeoutId = setTimeout(() => {
      this.retryTimeoutId = null;
      this.runNextSchedule();
    }, delay);
  }

  trackSuccess(updates: number) {
    if (!this.lastSuccessfulEmit) {
      throw new Error("Last successful emit not initialized");
    }

    this.failures = 0;
    this.lastSuccessfulEmit.$jazz.set("v", updates);
  }

  /**
   * Processes the next queued hash emission.
   *
   * If there's a queued hash, emits it and clears the queue. If no hash is queued,
   * sets the emitter to not running, allowing new emissions to proceed immediately.
   *
   * @private
   */
  private runNextSchedule() {
    if (!this.nextSchedule) {
      this.running = false;
      return;
    }

    if (!this.lastSuccessfulEmit) {
      return;
    }

    if (this.lastSuccessfulEmit.v === this.nextSchedule) {
      this.nextSchedule = null;
      this.running = false;
      return;
    }

    const next = this.nextSchedule;
    this.nextSchedule = null;
    this.running = true;
    this.emitWebhook(next);
  }

  /**
   * Stops the webhook emitter and cleans up resources.
   *
   * Cancels any pending retry timers, clears the emission queue, and sets the
   * emitter to not running. After calling stop(), no new webhooks will be emitted
   * and any queued emissions are discarded.
   */
  stop() {
    if (this.retryTimeoutId) {
      clearTimeout(this.retryTimeoutId);
      this.retryTimeoutId = null;
    }

    this.running = false;
    this.nextSchedule = null;
  }
}

export const registerWebhook = async (
  registryId: string,
  webhookUrl: string,
  coValueId: string,
): Promise<string> => {
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
      lastSuccessfulEmit: LastSuccessfulEmit.create(
        { v: -1 },
        registry.$jazz.owner,
      ),
    },
    registry.$jazz.owner,
  );

  registry.$jazz.set(registration.$jazz.id, registration);

  return registration.$jazz.id;
};
