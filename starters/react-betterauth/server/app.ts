import { Hono } from "hono";
import { auth } from "./auth.js";

export const app = new Hono();

app.get("/health", (c) => c.text("ok"));
app.on(["GET", "POST"], "/api/auth/*", (c) => auth.handler(c.req.raw));
