import { serveStatic } from "@hono/node-server/serve-static";
import { Hono } from "hono";
import { auth } from "./auth.js";

export const app = new Hono();

const staticFiles = serveStatic({ root: "./dist" });
const spaIndex = serveStatic({ root: "./dist", path: "index.html" });

app.get("/health", (c) => c.text("ok"));
app.on(["GET", "POST"], "/api/auth/*", (c) => auth.handler(c.req.raw));

app.use("/*", async (c, next) => {
  if (c.req.method !== "GET" && c.req.method !== "HEAD") {
    return next();
  }
  return staticFiles(c, next);
});

app.get("*", async (c, next) => {
  if (c.req.method !== "GET") {
    return next();
  }
  const pathname = c.req.path;
  if (
    pathname === "/api" ||
    pathname.startsWith("/api/") ||
    pathname === "/assets" ||
    pathname.startsWith("/assets/")
  ) {
    return next();
  }

  const acceptsHtml = c.req
    .header("Accept")
    ?.split(",")
    .some((value) => value.trim().split(";", 1)[0].toLowerCase() === "text/html");
  if (!acceptsHtml) {
    return next();
  }

  return spaIndex(c, next);
});
