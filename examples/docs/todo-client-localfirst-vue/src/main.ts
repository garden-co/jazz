import { createApp } from "vue";
import App from "./App.vue";

import { prepareAccountConfig } from "./account.js";

const config = await prepareAccountConfig();
createApp(App, { config }).mount("#app");
