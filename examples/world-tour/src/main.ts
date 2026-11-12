import "@garden-co/design/jazz/tokens.css";
import "@garden-co/design/jazz/fonts.css";
import "./styles/app.css";
import { createApp, h } from "vue";
import { JazzProvider } from "jazz-tools/vue";
import App from "./App.vue";

createApp({
  render: () =>
    h(
      JazzProvider,
      {
        // The app's backend serves Jazz on this origin, under `/jazz` (the default).
        // Every visitor gets a device account. Band membership, not a login,
        // decides what they can see and edit.
        config: {
          initial: "device",
          client: () => import("jazz-tools/local-first"),
          localFirst: "now",
        },
      },
      { default: () => h(App), loading: () => h("p", { class: "loading" }, "Loading the tour…") },
    ),
}).mount("#app");
