import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { spawn } from "node:child_process";

// Proxies SQL queries to `everr telemetry query` because the local collector's
// SQL HTTP endpoint isn't directly browser-callable. Dev-only.
function everrProxy() {
  return {
    name: "everr-proxy",
    configureServer(server: any) {
      server.middlewares.use("/api/query", async (req: any, res: any) => {
        const url = new URL(req.url, "http://localhost");
        const sql = url.searchParams.get("sql");
        if (!sql) {
          res.statusCode = 400;
          res.end("missing sql param");
          return;
        }
        try {
          const stdout = await runEverr(sql);
          res.setHeader("content-type", "application/x-ndjson");
          res.end(stdout);
        } catch (err: any) {
          res.statusCode = 500;
          res.end(String(err?.message ?? err));
        }
      });
    },
  };
}

function runEverr(sql: string): Promise<string> {
  return new Promise((resolve, reject) => {
    const child = spawn("everr", ["telemetry", "query", sql], {
      stdio: ["ignore", "pipe", "pipe"],
    });
    let out = "";
    let err = "";
    child.stdout.on("data", (chunk) => (out += chunk.toString()));
    child.stderr.on("data", (chunk) => (err += chunk.toString()));
    child.on("error", reject);
    child.on("close", (code) => {
      if (code === 0) resolve(out);
      else reject(new Error(err || `everr exited with code ${code}`));
    });
  });
}

export default defineConfig({
  plugins: [react(), everrProxy()],
});
