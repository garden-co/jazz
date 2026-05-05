"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.app = void 0;
const hono_1 = require("hono");
const auth_js_1 = require("./auth.js");
exports.app = new hono_1.Hono();
exports.app.get("/health", (c) => c.text("ok"));
exports.app.on(["GET", "POST"], "/api/auth/*", (c) => auth_js_1.auth.handler(c.req.raw));
