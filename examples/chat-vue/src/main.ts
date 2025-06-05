import { DemoAuthBasicUI, JazzProvider } from "jazz-vue";
import { createApp, defineComponent, h } from "vue";
import App from "./App.vue";
import "./index.css";
import { apiKey } from "@/apiKey";
import router from "./router";

const RootComponent = defineComponent({
  name: "RootComponent",
  setup() {
    return () =>
      h(
        JazzProvider,
        {
          sync: {
            peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
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
