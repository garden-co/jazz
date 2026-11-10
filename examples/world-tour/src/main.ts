import "@garden-co/design/jazz/tokens.css";
import "@garden-co/design/jazz/fonts.css";
import "./styles/app.css";
import { createApp, h } from "vue";
import { JazzProvider } from "jazz-tools/vue";
import { createAccountManager } from "jazz-tools";
import App from "./App.vue";

// The app's backend serves Jazz on this origin, under `/jazz`.
const config = { serverUrl: "/jazz" };

// Every visitor gets a local-first account. Band membership, not a login, decides
// what they can see and edit.
const accounts = await createAccountManager(config);
const account = accounts.getLoggedIn() ?? accounts.createLocalFirst();

createApp({
  render: () =>
    h(
      JazzProvider,
      { config: { ...config, account } },
      { default: () => h(App), fallback: () => h("p", { class: "loading" }, "Loading the tour…") },
    ),
}).mount("#app");
