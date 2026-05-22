// Bootstrap passed via `node --import` so the spawned MCP server runs as if
// `node:sqlite` did not exist. register() loads the hooks on a separate
// thread, so the hooks live in their own module.

import { register } from "node:module";

register("./block-node-sqlite.hooks.mjs", import.meta.url);
