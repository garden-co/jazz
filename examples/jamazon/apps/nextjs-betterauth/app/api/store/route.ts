import { ensureStore } from "@/src/server/store";
import { jsonRoute } from "@/src/server/shopper";

export const runtime = "nodejs";

/** Idempotent: seeds the catalogue and starts the fulfilment worker once. */
export async function POST() {
  return jsonRoute(async () => {
    await ensureStore();
    return { ok: true };
  });
}
