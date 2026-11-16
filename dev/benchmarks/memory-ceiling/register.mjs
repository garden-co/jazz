// `node --import ./register.mjs`: install the harness's module resolution
// (see resolve-hook.mjs) before the app's schema loads.
import { register } from "node:module";

register(new URL("./resolve-hook.mjs", import.meta.url));
