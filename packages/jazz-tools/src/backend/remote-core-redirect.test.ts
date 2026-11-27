import { randomUUID } from "node:crypto";
import { createServer, type IncomingHttpHeaders, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { afterEach, describe, expect, expectTypeOf, it } from "vitest";
import { testApp } from "../testing/index.js";
import { networkTransport } from "./core-transport.js";
import { CoreRedirectError, createJazz, remote } from "./index.js";
// Types only: the Worker entry loads jazz-wasm, which Node tests don't build.
import type * as workerdEntry from "./workerd.js";

const cleanups: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  while (cleanups.length) await cleanups.pop()!().catch(() => undefined);
});

async function listening(server: Server): Promise<string> {
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  cleanups.push(
    () =>
      new Promise((resolve) => {
        server.closeAllConnections();
        server.close(resolve);
      }),
  );
  return `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
}

/**
 * Where a core's redirects point: records every request and socket upgrade.
 * It must see none, so none carries the backend credential there.
 */
async function elsewhere() {
  const reached: IncomingHttpHeaders[] = [];
  const server = createServer((request, response) => {
    reached.push(request.headers);
    response.end("{}");
  });
  server.on("upgrade", (request, socket) => {
    reached.push(request.headers);
    socket.destroy();
  });
  return { url: await listening(server), reached };
}

/** A core that answers every request, and every socket upgrade, with `status` to `target`. */
async function redirectingCore(status: number, target: string) {
  const server = createServer((request, response) => {
    response.writeHead(status, { location: `${target}${request.url}` });
    response.end();
  });
  server.on("upgrade", (request, socket) => {
    socket.end(
      `HTTP/1.1 ${status} Redirect\r\nLocation: ${target}${request.url}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n`,
    );
  });
  return listening(server);
}

const REDIRECTS = [301, 302, 303, 307, 308];

describe.each(REDIRECTS)("a core answering %i", (status) => {
  it("fails remote() with a named error and never reaches the target", async () => {
    const target = await elsewhere();
    const core = await redirectingCore(status, target.url);
    const jazz = createJazz({
      app: testApp({ tables: { todos: { columns: { title: "string" } } } }),
      permissions: { format: 1, tables: { todos: { read: "true", insert: "true" } } },
      store: remote({ url: core, appId: randomUUID(), secret: "s".repeat(40) }),
    });
    cleanups.push(() => jazz.close());
    const error = await jazz.ready().then(
      () => undefined,
      (error: unknown) => error,
    );
    expect(error).toBeInstanceOf(CoreRedirectError);
    expect(String((error as Error).message)).toContain(`the Jazz core at ${core} redirected to`);
    expect(String((error as Error).message)).toContain(target.url);
    expect(String((error as Error).message)).toContain(
      "configure remote() with the core's final URL",
    );
    expect(target.reached).toEqual([]);
  });

  it("networkTransport's fetch never follows it, whatever redirect the caller asks for", async () => {
    const target = await elsewhere();
    const core = await redirectingCore(status, target.url);
    const response = await networkTransport().fetch(`${core}/apps/a/schemas`, {
      headers: { "x-jazz-backend-credential": "secret" },
      redirect: "follow",
    });
    expect(response.status).toBe(status);
    expect(response.headers.get("location")).toBe(`${target.url}/apps/a/schemas`);
    expect(target.reached).toEqual([]);
  });

  it("networkTransport's socket fails on it and never reaches the target", async () => {
    const target = await elsewhere();
    const core = await redirectingCore(status, target.url);
    const ws = networkTransport().openWebSocket(`${core.replace(/^http/, "ws")}/apps/a/ws`, {
      "x-jazz-backend-credential": "secret",
      "x-jazz-backend-access": "backend",
    });
    const events: string[] = [];
    await new Promise<void>((resolve) => {
      ws.addEventListener("open", () => events.push("open"));
      ws.addEventListener("error", () => events.push("error"));
      ws.addEventListener("close", () => {
        events.push("close");
        resolve();
      });
    });
    expect(events).toEqual(["error", "close"]);
    expect(target.reached).toEqual([]);
  });
});

it("both backend entries export CoreRedirectError", () => {
  expectTypeOf<typeof workerdEntry.CoreRedirectError>().toEqualTypeOf<typeof CoreRedirectError>();
});

it("CoreRedirectError names the core and where it pointed", () => {
  const error = new CoreRedirectError("https://core.example", "https://other.example/x");
  expect(error.name).toBe("CoreRedirectError");
  expect(error.message).toBe(
    "the Jazz core at https://core.example redirected to https://other.example/x; Jazz never follows a core's redirects, since they would carry the backend credential along: configure remote() with the core's final URL",
  );
});
