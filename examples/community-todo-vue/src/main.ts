import { JazzVueProvider, PasskeyAuthBasicUI } from "community-jazz-vue";
import { createApp, defineComponent, h, markRaw } from "vue";
import App from "./App.vue";
import "./assets/main.css";
import { apiKey } from "./apiKey";
import router from "./router";
import { TodoAccount } from "./schema";

import "jazz-tools/inspector/register-custom-element";

const RootComponent = defineComponent({
  name: "RootComponent",
  setup() {
    return () =>
      h(
        JazzVueProvider,
        {
          AccountSchema: TodoAccount,
          sync: {
            peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
          },
        },
        () => [
          h(
            PasskeyAuthBasicUI,
            {
              appName: "Jazz Vue Todo",
            },
            () => h(App),
          ),
          h("jazz-inspector", {
            style: {
              position: "fixed",
              bottom: "20px",
              right: "20px",
              zIndex: 9999,
            },
          }),
        ],
      );
  },
});

const app = createApp(RootComponent);

app.use(router);

app.mount("#app");
