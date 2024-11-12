import "./index.css";
import { DemoAuthBasicUI, createJazzVueApp, useDemoAuth } from "jazz-vue";
import { createApp, defineComponent, h } from "vue";
import App from "./App.vue";
import router from "./router";

const Jazz = createJazzVueApp();
export const { useAccount, useCoState } = Jazz;
const { JazzProvider } = Jazz;

const RootComponent = defineComponent({
  name: "RootComponent",
  setup() {
    const { authMethod, state } = useDemoAuth();

    return () => [
      h(
        JazzProvider,
        {
          auth: authMethod.value,
          peer: "wss://mesh.jazz.tools/?key=chat-example-jazz@gcmp.io",
        },
        {
          default: () => h(App),
        },
      ),

      state.state !== "signedIn" &&
        h(DemoAuthBasicUI, {
          appName: "Jazz Chat",
          state,
        }),
    ];
  },
});

const app = createApp(RootComponent);

app.use(router);

app.mount("#app");
