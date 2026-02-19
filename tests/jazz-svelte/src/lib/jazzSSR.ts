import { createSSRJazzAgent } from "jazz-tools/ssr";

export const jazzSSR = createSSRJazzAgent({
  peer: "ws://localhost:4250/",
});
