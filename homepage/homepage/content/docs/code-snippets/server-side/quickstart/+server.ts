import { startWorker } from "jazz-tools/worker";
import { announceBand } from "$lib/announceBandSchema";
import { JazzFestWorkerAccount } from "./schema";
import {
  PUBLIC_JAZZ_API_KEY,
  PUBLIC_JAZZ_WORKER_ACCOUNT,
  // @ts-expect-error Not available outside of a real SK app
} from "$env/static/public";
// @ts-expect-error Not available outside of a real SK app
import { JAZZ_WORKER_SECRET } from "$env/static/private";
// @ts-expect-error Not available outside of a real SK app
import type { RequestHandler } from "./$types";

const { worker } = await startWorker({
  syncServer: `wss://cloud.jazz.tools/?key=${PUBLIC_JAZZ_API_KEY}`,
  accountID: PUBLIC_JAZZ_WORKER_ACCOUNT,
  accountSecret: JAZZ_WORKER_SECRET,
  AccountSchema: JazzFestWorkerAccount,
});

// @ts-expect-error Request can't be typed outside a real SK app
export const POST: RequestHandler = async ({ request }) => {
  return announceBand.handle(request, worker, async ({ band }) => {
    if (!band) {
      throw new Error("Band is required");
    }
    const {
      root: { bandList },
    } = await worker.$jazz.ensureLoaded({
      resolve: {
        root: {
          bandList: true,
        },
      },
    });
    bandList.$jazz.push(band);
    return {
      bandList,
    };
  });
};
