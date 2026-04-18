import { createApp, h } from "vue";
import { createJazzClient, JazzProvider } from "jazz-tools/vue";
import { generateAuthSecret } from "jazz-tools";
import App from "./App.vue";
import { appId } from "./constants";

const secretStorageKey = `jazz-auth-secret:${encodeURIComponent(appId)}`;

function getOrCreateSecretSync(): string {
  const stored = localStorage.getItem(secretStorageKey);
  if (stored) return stored;
  const secret = generateAuthSecret();
  localStorage.setItem(secretStorageKey, secret);
  return secret;
}

const secret = getOrCreateSecretSync();

const clientConfig: Parameters<typeof createJazzClient>[0] = {
  appId,
  env: "dev",
  userBranch: "main",
  serverUrl: import.meta.env.VITE_JAZZ_SERVER_URL ?? "http://localhost:4200",
  auth: { localFirstSecret: secret },
};

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
