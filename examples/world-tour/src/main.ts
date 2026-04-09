import { createApp, h } from "vue";
import { createJazzClient, JazzProvider } from "jazz-tools/vue";
import { loadOrCreateIdentitySeed, mintSelfSignedToken } from "jazz-tools";
import App from "./App.vue";
import { appId, isPublicMode } from "./constants";

const clientConfig: Parameters<typeof createJazzClient>[0] = {
  appId,
  env: "dev",
  userBranch: "main",
  serverUrl: import.meta.env.VITE_JAZZ_SERVER_URL ?? "http://localhost:4200",
};

if (!isPublicMode) {
  const seed = loadOrCreateIdentitySeed(appId);
  clientConfig.jwtToken = mintSelfSignedToken(seed.seed, appId);
}

const client = createJazzClient(clientConfig);

const vueApp = createApp({
  render() {
    return h(
      JazzProvider,
      { client },
      {
        default: () => h(App),
        fallback: () => h("p", "Loading globe..."),
      },
    );
  },
});

vueApp.mount("#app");
