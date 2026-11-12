import { createApp } from "vue";
import App from "./App.vue";

import { sessionConfig } from "./account.js";

createApp(App, { config: sessionConfig() }).mount("#app");
