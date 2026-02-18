import { spawn, type ChildProcess } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { RequestLike, SessionClient, JazzClient } from "../runtime/client.js";
import type { Db, TableProxy } from "../runtime/db.js";

export interface TestClaims {
  sub: string;
  claims?: Record<string, unknown>;
  [key: string]: unknown;
}

export interface LocalJazzServerOptions {
  appId: string;
  jazzBin?: string;
  port?: number;
  dataDir?: string;
  jwtSecret?: string;
  adminSecret?: string;
  startupTimeoutMs?: number;
  env?: NodeJS.ProcessEnv;
}

export interface LocalJazzServer {
  appId: string;
  url: string;
  port: number;
  dataDir: string;
  jwtSecret: string;
  adminSecret: string;
  process: ChildProcess;
  stop(): Promise<void>;
}

async function terminateProcess(child: ChildProcess, timeoutMs: number): Promise<void> {
  const exited = await new Promise<boolean>((resolve) => {
    if (child.exitCode !== null || child.signalCode !== null) {
      resolve(true);
      return;
    }
    const onExit = (): void => {
      clearTimeout(timer);
      resolve(true);
    };
    const timer = setTimeout(() => {
      child.off("exit", onExit);
      resolve(false);
    }, timeoutMs);
    child.once("exit", onExit);
    child.kill("SIGTERM");
  });

  if (exited) {
    return;
  }

  await new Promise<void>((resolve) => {
    const onExit = (): void => {
      clearTimeout(timer);
      resolve();
    };
    const timer = setTimeout(() => {
      child.off("exit", onExit);
      resolve();
    }, timeoutMs);
    child.once("exit", onExit);
    child.kill("SIGKILL");
  });
}

function toBase64Url(value: unknown): string {
  const encoded = Buffer.from(JSON.stringify(value), "utf8").toString("base64");
  return encoded.replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}

export function createTestJwt(payload: TestClaims): string {
  const header = { alg: "HS256", typ: "JWT" };
  return `${toBase64Url(header)}.${toBase64Url(payload)}.test-signature`;
}

export function requestForClaims(payload: TestClaims): RequestLike {
  const token = createTestJwt(payload);
  return {
    headers: {
      authorization: `Bearer ${token}`,
    },
  };
}

export function scopedClientForClaims(client: JazzClient, payload: TestClaims): SessionClient {
  return client.forRequest(requestForClaims(payload));
}

export async function seedRows<T, Init>(
  db: Db,
  table: TableProxy<T, Init>,
  rows: Init[],
): Promise<string[]> {
  return rows.map((row) => db.insert(table, row));
}

export async function expectAllowed(op: () => Promise<unknown> | unknown): Promise<void> {
  await op();
}

export async function expectDenied(
  op: () => Promise<unknown> | unknown,
  options?: { match?: RegExp | string },
): Promise<Error> {
  try {
    await op();
  } catch (error) {
    if (!options?.match) {
      return toError(error);
    }
    const err = toError(error);
    const message = err.message;
    if (typeof options.match === "string") {
      if (!message.includes(options.match)) {
        throw new Error(
          `Operation failed, but error did not include expected text: "${options.match}". Actual: "${message}"`,
        );
      }
      return err;
    }
    if (!options.match.test(message)) {
      throw new Error(
        `Operation failed, but error did not match ${options.match}. Actual: "${message}"`,
      );
    }
    return err;
  }
  throw new Error("Expected operation to be denied, but it succeeded.");
}

function toError(error: unknown): Error {
  if (error instanceof Error) {
    return error;
  }
  return new Error(String(error));
}

async function pickOpenPort(): Promise<number> {
  return await new Promise<number>((resolve, reject) => {
    const server = createServer();
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (!address || typeof address === "string") {
        server.close(() => reject(new Error("Failed to resolve an open port.")));
        return;
      }
      const { port } = address;
      server.close((closeErr) => {
        if (closeErr) {
          reject(closeErr);
          return;
        }
        resolve(port);
      });
    });
  });
}

async function waitForHealth(child: ChildProcess, url: string, timeoutMs: number): Promise<void> {
  const startedAt = Date.now();
  let lastHealthError: unknown;

  await new Promise<void>((resolve, reject) => {
    let settled = false;
    let timer: NodeJS.Timeout | undefined;

    const cleanup = (): void => {
      child.off("exit", onExit);
      child.off("error", onError);
      if (timer) {
        clearTimeout(timer);
      }
    };

    const finish = (error?: Error): void => {
      if (settled) {
        return;
      }
      settled = true;
      cleanup();
      if (error) {
        reject(error);
        return;
      }
      resolve();
    };

    const onExit = (code: number | null, signal: NodeJS.Signals | null): void => {
      finish(
        new Error(
          `Jazz server exited before becoming healthy (code: ${code ?? "null"}, signal: ${signal ?? "none"}).`,
        ),
      );
    };

    const onError = (error: Error): void => {
      finish(new Error(`Failed to start Jazz server process: ${error.message}`));
    };

    const poll = async (): Promise<void> => {
      if (settled) {
        return;
      }
      if (Date.now() - startedAt >= timeoutMs) {
        const detail =
          lastHealthError instanceof Error ? ` Last health error: ${lastHealthError.message}` : "";
        finish(
          new Error(`Jazz server did not become healthy within ${timeoutMs}ms (${url}).${detail}`),
        );
        return;
      }

      try {
        const resp = await fetch(url);
        if (resp.ok) {
          finish();
          return;
        }
      } catch (error) {
        lastHealthError = error;
      }

      timer = setTimeout(() => {
        void poll();
      }, 100);
    };

    child.once("exit", onExit);
    child.once("error", onError);
    void poll();
  });
}

export async function startLocalJazzServer(
  options: LocalJazzServerOptions,
): Promise<LocalJazzServer> {
  const jazzBin = options.jazzBin ?? "jazz-tools";
  const port = options.port ?? (await pickOpenPort());
  const jwtSecret = options.jwtSecret ?? "test-jwt-secret";
  const adminSecret = options.adminSecret ?? "test-admin-secret";
  const ownsDataDir = !options.dataDir;
  const dataDir = options.dataDir ?? (await mkdtemp(join(tmpdir(), "jazz-policy-test-")));

  const child = spawn(
    jazzBin,
    ["server", options.appId, "--port", String(port), "--data-dir", dataDir],
    {
      env: {
        ...process.env,
        ...options.env,
        JAZZ_JWT_SECRET: jwtSecret,
        JAZZ_ADMIN_SECRET: adminSecret,
      },
      stdio: ["ignore", "pipe", "pipe"],
    },
  );
  // Prevent child stdio backpressure from stalling long-lived test servers.
  child.stdout?.on("data", () => {});
  child.stderr?.on("data", () => {});

  const healthUrl = `http://127.0.0.1:${port}/health`;

  try {
    await waitForHealth(child, healthUrl, options.startupTimeoutMs ?? 10_000);
  } catch (error) {
    await terminateProcess(child, 1_000);
    if (ownsDataDir) {
      await rm(dataDir, { recursive: true, force: true });
    }
    throw error;
  }

  return {
    appId: options.appId,
    url: `http://127.0.0.1:${port}`,
    port,
    dataDir,
    jwtSecret,
    adminSecret,
    process: child,
    async stop() {
      await terminateProcess(child, 2_000);
      if (ownsDataDir) {
        await rm(dataDir, { recursive: true, force: true });
      }
    },
  };
}
