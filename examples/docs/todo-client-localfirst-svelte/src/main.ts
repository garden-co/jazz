import { mount } from "svelte";
import App from "./App.svelte";

import { sessionConfig } from "./account.js";

mount(App, { props: { config: sessionConfig() }, target: document.getElementById("app")! });
