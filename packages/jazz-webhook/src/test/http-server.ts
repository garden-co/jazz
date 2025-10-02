import { createServer, IncomingMessage, ServerResponse, Server } from "http";
import { AddressInfo } from "net";
import { EventEmitter } from "events";

export interface WebhookRequest {
  coValueId: string;
  updates: number;
  timestamp: number;
}

export interface WebhookResponse {
  statusCode: number;
  body?: string;
  delay?: number;
}

export class WebhookTestServer {
  private server: Server;
  private port: number = 0;
  private url: string = "";
  private responseIndex: number = 0;

  public readonly requests: WebhookRequest[] = [];
  public readonly responses: WebhookResponse[] = [];

  constructor() {
    this.server = createServer(this.handleRequest.bind(this));
  }

  listeners = new Set<() => void>();
  addRequestListener(listener: () => void): void {
    this.listeners.add(listener);
  }
  removeRequestListener(listener: () => void): void {
    this.listeners.delete(listener);
  }

  emitRequest(): void {
    for (const listener of this.listeners) {
      listener();
    }
  }

  private handleRequest(req: IncomingMessage, res: ServerResponse): void {
    if (req.method === "POST" && req.url === "/webhook") {
      let body = "";

      req.on("data", (chunk) => {
        body += chunk.toString();
      });

      req.on("end", async () => {
        try {
          const response = this.responses[this.responseIndex] || {
            statusCode: 200,
          };
          this.responseIndex++;

          // Add delay if specified
          if (response.delay) {
            await new Promise((resolve) => setTimeout(resolve, response.delay));
          }

          res.statusCode = response.statusCode;
          res.setHeader("Content-Type", "application/json");
          res.end(response.body || JSON.stringify({ received: true }));
        } catch (error) {
          res.statusCode = 400;
          res.setHeader("Content-Type", "application/json");
          res.end(JSON.stringify({ error: "Invalid JSON" }));
        }

        const webhookData: WebhookRequest = JSON.parse(body);
        this.requests.push(webhookData);

        this.emitRequest();
      });
    } else {
      res.statusCode = 404;
      res.end("Not Found");
    }
  }

  /**
   * Starts the server on an available port
   */
  async start(): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      this.server.listen(0, () => {
        try {
          const address = this.server.address() as AddressInfo;
          this.port = address.port;
          this.url = `http://localhost:${this.port}/webhook`;
          resolve();
        } catch (error) {
          reject(error);
        }
      });

      this.server.on("error", reject);
    });
  }

  /**
   * Stops the server
   */
  async close(): Promise<void> {
    return new Promise<void>((resolve) => {
      this.server.close(() => {
        resolve();
      });
    });
  }

  /**
   * Resets all stored requests and responses
   */
  reset(): void {
    this.requests.length = 0;
    this.responses.length = 0;
    this.responseIndex = 0;
  }

  /**
   * Sets a response for a specific request index
   */
  setResponse(
    index: number,
    statusCode: number,
    body?: string,
    delay?: number,
  ): void {
    this.responses[index] = { statusCode, body, delay };
  }

  /**
   * Waits for a specific number of requests to be received
   */
  async waitForRequests(
    count: number,
    timeout: number = 5000,
  ): Promise<WebhookRequest[]> {
    return new Promise<WebhookRequest[]>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.removeRequestListener(onRequest);
        reject(
          new Error(
            `Timeout waiting for ${count} requests. Got ${this.requests.length}`,
          ),
        );
      }, timeout);

      const onRequest = () => {
        if (this.requests.length >= count) {
          clearTimeout(timer);
          this.removeRequestListener(onRequest);
          resolve(this.requests);
        }
      };

      this.addRequestListener(onRequest);
      onRequest();
    });
  }

  /**
   * Gets the server port
   */
  getPort(): number {
    return this.port;
  }

  /**
   * Gets the webhook URL
   */
  getUrl(): string {
    return this.url;
  }

  /**
   * Gets the number of requests received
   */
  getRequestCount(): number {
    return this.requests.length;
  }

  expectSingleRequest(): WebhookRequest {
    if (this.requests.length !== 1) {
      throw new Error("Expected 1 request, got " + this.requests.length);
    }

    return this.getLastRequest();
  }

  /**
   * Gets the last request received
   */
  getLastRequest(): WebhookRequest {
    const lastRequest = this.requests.at(-1);
    if (!lastRequest) {
      throw new Error("No requests received");
    }

    return lastRequest;
  }

  /**
   * Checks if the server is running
   */
  isRunning(): boolean {
    return this.server.listening;
  }
}

/**
 * Factory function to create and start a webhook test server
 * @deprecated Use WebhookTestServer class directly
 */
export async function createWebhookTestServer(): Promise<WebhookTestServer> {
  const server = new WebhookTestServer();
  await server.start();
  return server;
}
