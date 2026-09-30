import { backend } from "./backend";
import { startFulfilmentWorker } from "./fulfilment";
import { seedCatalogue } from "./seed";

declare global {
  var __jamazonStore: Promise<void> | undefined;
}

/** Seed the catalogue and start the fulfilment worker, once per server process. */
export async function ensureStore(): Promise<void> {
  const pending = (globalThis.__jamazonStore ??= (async () => {
    const { db } = await backend();
    await seedCatalogue(db);
    startFulfilmentWorker(db);
  })());
  try {
    await pending;
  } catch (error) {
    if (globalThis.__jamazonStore === pending) globalThis.__jamazonStore = undefined;
    throw error;
  }
}
