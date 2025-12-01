import { experimental_defineRequest } from "jazz-tools";
import { Band, BandList } from "./schema";

const workerId = process.env.NEXT_PUBLIC_JAZZ_WORKER_ACCOUNT;

if (!workerId) throw new Error("NEXT_PUBLIC_JAZZ_WORKER_ACCOUNT is not set");

export const announceBand = experimental_defineRequest({
  url: "/api/announce-band",
  workerId: workerId,
  request: { schema: { band: Band }, resolve: { band: true } },
  response: {
    schema: { bandList: BandList },
    resolve: { bandList: { $each: true } },
  },
});
