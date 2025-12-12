import { createSSRJazzAgent } from "jazz-tools/ssr";

export const jazzSSR = await createSSRJazzAgent({
  peer: "wss://cloud.jazz.tools/",
});
