"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const node_server_1 = require("@hono/node-server");
const app_js_1 = require("./app.js");
const PORT = Number(process.env.PORT ?? 3001);
(0, node_server_1.serve)({ fetch: app_js_1.app.fetch, port: PORT }, (info) => {
  console.log(`Hono listening on http://127.0.0.1:${info.port}`);
});
