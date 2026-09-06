export type ClientWithShutdown = {
  shutdown(options?: { waitForSync?: boolean }): Promise<void>;
};

/** Close a client on teardown, including after an interrupted sync barrier rejects. */
export function shutdownOnDispose<T extends ClientWithShutdown>(
  client: T,
  gracefulShutdowns: WeakMap<T, Promise<void>>,
  shutdownClients: WeakSet<T>,
): void {
  const graceful = gracefulShutdowns.get(client);
  if (graceful) {
    void graceful.catch(() => client.shutdown()).catch(() => undefined);
    return;
  }
  if (shutdownClients.has(client)) return;
  shutdownClients.add(client);
  void client.shutdown().catch(() => undefined);
}
