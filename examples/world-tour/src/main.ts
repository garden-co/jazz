import { createApp, h } from "vue";
import { createJazzClient, JazzProvider, getActiveSyntheticAuth } from "jazz-tools/vue";
import App from "./App.vue";

const appId = "world-tour-example";
const isPublicMode = new URLSearchParams(window.location.search).has("public");

(window as any).__worldtour_public = isPublicMode;

const clientConfig: Parameters<typeof createJazzClient>[0] = {
  appId,
  env: "dev",
  userBranch: "main",
  serverUrl: import.meta.env.VITE_JAZZ_SERVER_URL ?? "http://localhost:4200",
};

if (isPublicMode) {
  clientConfig.localAuthMode = "anonymous";
} else {
  const auth = getActiveSyntheticAuth(appId, { defaultMode: "demo" });
  clientConfig.localAuthMode = auth.localAuthMode;
  clientConfig.localAuthToken = auth.localAuthToken;
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
