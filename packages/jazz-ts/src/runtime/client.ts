/**
 * JazzClient - High-level TypeScript client for Jazz.
 *
 * Wraps the WASM runtime and provides a clean API for CRUD operations,
 * subscriptions, and sync.
 */

import type { AppContext } from "./context.js";
import type { Value, RowDelta, Schema } from "../drivers/types.js";
import type { WasmRuntime as GrooveWasmRuntime } from "groove-wasm";

// Re-type the WasmRuntime to work with our local types
type WasmRuntime = GrooveWasmRuntime;

/**
 * Query row result.
 */
export interface Row {
  id: string;
  values: Value[];
}

/**
 * Subscription callback type.
 */
export type SubscriptionCallback = (delta: RowDelta) => void;

/**
 * High-level Jazz client.
 */
export class JazzClient {
  private runtime: WasmRuntime;
  private tickInterval: ReturnType<typeof setInterval> | null = null;
  private sseConnection: EventSource | null = null;
  private subscriptions = new Map<number, SubscriptionCallback>();
  private context: AppContext;

  private constructor(runtime: WasmRuntime, context: AppContext) {
    this.runtime = runtime;
    this.context = context;
  }

  /**
   * Connect to Jazz with the given context.
   *
   * @param context Application context with driver and schema
   * @returns Connected JazzClient instance
   */
  static async connect(context: AppContext): Promise<JazzClient> {
    // Load WASM module dynamically
    // In a real implementation, this would import from groove-wasm
    const wasmModule = await loadWasmModule();

    // Create WASM runtime with the JS driver
    const schemaJson = JSON.stringify(context.schema);
    const runtime = new wasmModule.WasmRuntime(
      context.driver,
      schemaJson,
      context.appId,
      context.env ?? "dev",
      context.userBranch ?? "main",
    );

    const client = new JazzClient(runtime, context);

    // Start tick loop
    client.startTickLoop();

    // Set up sync if server URL provided
    if (context.serverUrl) {
      client.setupSync(context.serverUrl);
    }

    return client;
  }

  /**
   * Insert a new row into a table.
   *
   * @param table Table name
   * @param values Array of column values
   * @returns The new row's ID (UUID string)
   */
  async create(table: string, values: Value[]): Promise<string> {
    const id = await this.runtime.insert(table, values);
    return id;
  }

  /**
   * Execute a query and return all matching rows.
   *
   * @param queryJson JSON-encoded query specification
   * @returns Array of matching rows
   */
  async query(queryJson: string): Promise<Row[]> {
    const results = await this.runtime.query(queryJson);
    return results as Row[];
  }

  /**
   * Update a row by ID.
   *
   * @param objectId Row ID (UUID string)
   * @param updates Object mapping column names to new values
   */
  async update(objectId: string, updates: Record<string, Value>): Promise<void> {
    await this.runtime.update(objectId, updates);
  }

  /**
   * Delete a row by ID.
   *
   * @param objectId Row ID (UUID string)
   */
  async delete(objectId: string): Promise<void> {
    await this.runtime.delete(objectId);
  }

  /**
   * Subscribe to a query and receive updates when results change.
   *
   * @param queryJson JSON-encoded query specification
   * @param callback Called with delta whenever results change
   * @returns Subscription ID for unsubscribing
   */
  async subscribe(queryJson: string, callback: SubscriptionCallback): Promise<number> {
    const subId = await this.runtime.subscribe(queryJson, (delta: RowDelta) => {
      callback(delta);
    });
    this.subscriptions.set(subId, callback);
    return subId;
  }

  /**
   * Unsubscribe from a query.
   *
   * @param subscriptionId ID returned from subscribe()
   */
  unsubscribe(subscriptionId: number): void {
    this.runtime.unsubscribe(subscriptionId);
    this.subscriptions.delete(subscriptionId);
  }

  /**
   * Get the current schema.
   */
  getSchema(): Schema {
    return this.runtime.getSchema();
  }

  /**
   * Shutdown the client and release resources.
   */
  async shutdown(): Promise<void> {
    // Stop tick loop
    if (this.tickInterval) {
      clearInterval(this.tickInterval);
      this.tickInterval = null;
    }

    // Close SSE connection
    if (this.sseConnection) {
      this.sseConnection.close();
      this.sseConnection = null;
    }

    // Close driver if it supports it
    if (this.context.driver.close) {
      await this.context.driver.close();
    }
  }

  private startTickLoop(): void {
    // Tick every 100ms to process async operations
    this.tickInterval = setInterval(async () => {
      try {
        await this.runtime.tick();
      } catch (e) {
        console.error("Jazz tick error:", e);
      }
    }, 100);
  }

  private setupSync(serverUrl: string): void {
    // Set up outgoing message handler
    this.runtime.onSyncMessageToSend((message: string) => {
      this.sendSyncMessage(serverUrl, message);
    });

    // Connect to SSE endpoint for incoming messages
    this.connectSSE(serverUrl);

    // Register server connection
    this.runtime.addServer();
  }

  private async sendSyncMessage(serverUrl: string, message: string): Promise<void> {
    try {
      const response = await fetch(`${serverUrl}/sync/payload`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: message,
      });

      if (!response.ok) {
        console.error("Sync send error:", response.statusText);
      }
    } catch (e) {
      console.error("Sync send error:", e);
    }
  }

  private connectSSE(serverUrl: string): void {
    const eventsUrl = `${serverUrl}/events`;
    this.sseConnection = new EventSource(eventsUrl);

    this.sseConnection.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);

        // Handle different event types
        if (data.type === "SyncUpdate") {
          // Pass the payload to the runtime
          this.runtime.onSyncMessageReceived(JSON.stringify(data.payload));
        }
      } catch (e) {
        console.error("SSE parse error:", e);
      }
    };

    this.sseConnection.onerror = (e) => {
      console.error("SSE error:", e);
      // Attempt to reconnect after a delay
      setTimeout(() => {
        if (this.sseConnection) {
          this.sseConnection.close();
          this.connectSSE(serverUrl);
        }
      }, 5000);
    };
  }
}

/**
 * Load and initialize the WASM module.
 */
async function loadWasmModule() {
  const wasmModule = await import("groove-wasm");

  // In Node.js, we need to read the .wasm file and use initSync
  // In browsers, the default fetch-based init works
  if (typeof process !== "undefined" && process.versions?.node) {
    const { readFileSync } = await import("node:fs");
    const { fileURLToPath } = await import("node:url");
    const { dirname, join } = await import("node:path");

    // Find the .wasm file relative to the groove-wasm package
    const wasmPath = join(
      dirname(fileURLToPath(import.meta.url)),
      "../../node_modules/groove-wasm/groove_wasm_bg.wasm",
    );
    const wasmBytes = readFileSync(wasmPath);
    wasmModule.initSync(wasmBytes);
  } else {
    await wasmModule.default();
  }

  return wasmModule;
}
