import { expect, test, vi } from "vitest";
import { shutdownOnDispose, type ClientWithShutdown } from "./client-lifecycle.js";

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

async function flush() {
  await Promise.resolve();
  await Promise.resolve();
}

test("unmount closes a client after its graceful sync barrier rejects", async () => {
  const graceful = deferred<void>();
  const client: ClientWithShutdown = { shutdown: vi.fn().mockResolvedValue(undefined) };
  const gracefulShutdowns = new WeakMap([[client, graceful.promise]]);
  const shutdownClients = new WeakSet([client]);

  shutdownOnDispose(client, gracefulShutdowns, shutdownClients);
  expect(client.shutdown).not.toHaveBeenCalled();

  graceful.reject(new Error("edge unavailable"));
  await flush();

  expect(client.shutdown).toHaveBeenCalledTimes(1);
  expect(client.shutdown).toHaveBeenLastCalledWith();
});

test("a completed graceful shutdown is not closed again", async () => {
  const graceful = deferred<void>();
  const client: ClientWithShutdown = { shutdown: vi.fn().mockResolvedValue(undefined) };
  const gracefulShutdowns = new WeakMap([[client, graceful.promise]]);

  shutdownOnDispose(client, gracefulShutdowns, new WeakSet([client]));
  graceful.resolve();
  await flush();

  expect(client.shutdown).not.toHaveBeenCalled();
});
