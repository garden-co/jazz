import { createJazzBrowserContext } from "jazz-tools/browser";

// Get a free API Key at dashboard.jazz.tools, or use your email as a temporary key.
const apiKey = "you@example.com";
// @ts-expect-error only showing sync options
const ctx = await createJazzBrowserContext({
  sync: {
    peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
  },
  // Rest of config
});
