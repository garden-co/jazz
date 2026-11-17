import { randomUUID } from "node:crypto";
import { createServer, type IncomingHttpHeaders, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { afterEach, expect, it } from "vitest";
import { testApp } from "../testing/index.js";
import { createJazz, remote } from "./index.js";

const cleanups: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  while (cleanups.length) await cleanups.pop()!().catch(() => undefined);
});

async function listening(server: Server): Promise<string> {
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  cleanups.push(() => new Promise((resolve) => server.close(resolve)));
  return `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
}

it("doesn't follow a redirect from the core's URL to another", async () => {
  const reached: IncomingHttpHeaders[] = [];
  const elsewhere = await listening(
    createServer((request, response) => {
      reached.push(request.headers);
      response.end("{}");
    }),
  );
  const redirecting = await listening(
    createServer((request, response) => {
      response.writeHead(307, { location: `${elsewhere}${request.url}` });
      response.end();
    }),
  );
  const jazz = createJazz({
    app: testApp({ tables: { todos: { columns: { title: "string" } } } }),
    permissions: { format: 1, tables: { todos: { read: "true", insert: "true" } } },
    store: remote({ url: redirecting, appId: randomUUID(), secret: "s".repeat(40) }),
  });
  cleanups.push(() => jazz.close());
  await expect(jazz.ready()).rejects.toThrow();
  expect(reached).toEqual([]);
});
