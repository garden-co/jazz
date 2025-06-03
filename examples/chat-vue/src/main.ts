import { DemoAuthBasicUI, JazzProvider } from "jazz-vue";
import { createApp, defineComponent, h } from "vue";
import App from "./App.vue";
import "./index.css";
import { apiKey } from "@/apiKey";
import router from "./router";

const url = new URL(window.location.href);
const peer = url.searchParams.get("peer") as `wss://${string}` | `ws://${string}` | null;

const RootComponent = defineComponent({
  name: "RootComponent",
  setup() {
    return () =>
      h(
        JazzProvider,
        {
          sync: {
            peer: peer ?? `wss://cloud.jazz.tools/?key=${apiKey}`,
          },
        },
        h(
          DemoAuthBasicUI,
          {
            appName: "Jazz Vue Chat",
          },
          {
            default: () => h(App),
          },
        ),
      );
  },
});

const app = createApp(RootComponent);

app.use(router);

app.mount("#app");
