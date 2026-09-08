import { render } from "solid-js/web";
import { App } from "./App.js";

import { prepareAccountConfig } from "./account.js";

const config = await prepareAccountConfig();
render(() => <App config={config} />, document.getElementById("app")!);
