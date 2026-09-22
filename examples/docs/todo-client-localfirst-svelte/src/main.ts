import { mount } from "svelte";
import App from "./App.svelte";

import { prepareAccountConfig } from "./account.js";

const config = await prepareAccountConfig();
mount(App, { props: { config }, target: document.getElementById("app")! });
