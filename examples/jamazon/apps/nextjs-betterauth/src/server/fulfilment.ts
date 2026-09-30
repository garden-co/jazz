import type { Db } from "jazz-tools";
import { app } from "@/schema";
import { shipOrder } from "./orders";

/** How long the pretend warehouse takes to pack a paid order. */
export const PACKING_MS = 6_000;

/**
 * A backend worker: it subscribes to paid orders and ships each one after a
 * packing delay. Shipping is idempotent, so a second worker (another server
 * instance, or a restart) racing this one cannot ship an order twice; a paid
 * order left behind by a restart is picked up by the next subscription.
 */
export function startFulfilmentWorker(db: Db, packingMs = PACKING_MS): () => void {
  const scheduled = new Map<string, ReturnType<typeof setTimeout>>();
  const unsubscribe = db.subscribe(
    app.orders.where({ status: "paid" }),
    {
      onUpdate(orders) {
        for (const order of orders) {
          if (scheduled.has(order.id)) continue;
          scheduled.set(
            order.id,
            setTimeout(() => {
              shipOrder(db, order.id)
                .catch((error) => console.error(`[jamazon] shipping ${order.code} failed`, error))
                .finally(() => scheduled.delete(order.id));
            }, packingMs),
          );
        }
      },
      onError(error) {
        console.error("[jamazon] fulfilment subscription failed", error);
      },
    },
    { tier: "global" },
  );
  return () => {
    unsubscribe();
    for (const timer of scheduled.values()) clearTimeout(timer);
  };
}
