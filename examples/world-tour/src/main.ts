import { createApp, h } from "vue";
import { JazzProvider } from "jazz-tools/vue";
import { createAccountManager } from "jazz-tools";
import App from "./App.vue";

const config = {
  appId: import.meta.env.VITE_JAZZ_APP_ID,
  serverUrl: import.meta.env.VITE_JAZZ_SERVER_URL,
};

const accounts = await createAccountManager(config);
const account = accounts.getLoggedIn() ?? accounts.createLocalFirst();

const vueApp = createApp({
  render() {
    return h(
      JazzProvider,
      { config: { ...config, account } },
      {
        default: () => h(App),
        fallback: () => h("p", "Loading globe..."),
      },
    );
  },
});

vueApp.mount("#app");
