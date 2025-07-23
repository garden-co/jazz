import {
  DemoAuthBasicUI,
  JazzInspector,
  JazzVueProvider,
} from "community-jazz-vue";
import { createApp, defineComponent, h } from "vue";
import App from "./App.vue";
import "./assets/main.css";
import { apiKey } from "./apiKey";
import router from "./router";
import { TodoAccount } from "./schema";

import "community-jazz-vue/dist/community-jazz-vue.css";

declare module "community-jazz-vue" {
  interface Register {
    Account: typeof TodoAccount;
  }
}

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
            DemoAuthBasicUI,
            {
              appName: "Jazz Vue Todo",
            },
            () => h(App),
          ),
          h(JazzInspector),
        ],
      );
  },
});

const app = createApp(RootComponent);

app.use(router);

app.mount("#app");
