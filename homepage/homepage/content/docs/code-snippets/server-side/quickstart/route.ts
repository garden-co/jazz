import { startWorker } from "jazz-tools/worker";
import { announceBand } from "@/app/announceBandSchema";
import { JazzFestWorkerAccount } from "./schema";

const { worker } = await startWorker({
  syncServer: `wss://cloud.jazz.tools/?key=${process.env.NEXT_PUBLIC_JAZZ_API_KEY}`,
  accountID: process.env.NEXT_PUBLIC_JAZZ_WORKER_ACCOUNT,
  accountSecret: process.env.JAZZ_WORKER_SECRET,
  AccountSchema: JazzFestWorkerAccount,
});

export async function POST(request: Request) {
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
    return { bandList };
  });
}
