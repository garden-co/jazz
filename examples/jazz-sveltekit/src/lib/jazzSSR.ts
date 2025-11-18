import { createSSRJazzAgent } from "jazz-tools/ssr";

export const jazzSSR = createSSRJazzAgent({
  peer: "wss://cloud.jazz.tools/",
});
