import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

export const QUERY_ENDPOINT_ENV = "JAZZ_SYNC_VIEWER_QUERY_ENDPOINT";

function telemetryQueryProxy() {
  return {
    name: "local-telemetry-query-proxy",
    configureServer(server: any) {
      server.middlewares.use("/api/query", async (req: any, res: any) => {
        if (req.method !== "POST") {
          res.statusCode = 405;
          res.setHeader("allow", "POST");
          res.end("method not allowed");
          return;
        }

        const sql = await readTelemetryQuerySql(req);
        if (!sql) {
          res.statusCode = 400;
          res.end("missing sql");
          return;
        }

        try {
          const stdout = await runTelemetryQuery(sql);
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

export async function runTelemetryQuery(sql: string): Promise<string> {
  const endpoint = process.env[QUERY_ENDPOINT_ENV];
  if (!endpoint) {
    throw new Error(`${QUERY_ENDPOINT_ENV} is required`);
  }

  const response = await fetch(endpoint, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ sql }),
  });
  const body = await response.text();
  if (!response.ok) {
    throw new Error(body || `telemetry query returned ${response.status}`);
  }
  return body;
}

async function readTelemetryQuerySql(req: NodeJS.ReadableStream): Promise<string | null> {
  const body = await readBody(req);
  if (!body.trim()) return null;

  try {
    const parsed = JSON.parse(body) as { sql?: unknown };
    return typeof parsed.sql === "string" ? parsed.sql : null;
  } catch {
    return null;
  }
}

function readBody(req: NodeJS.ReadableStream): Promise<string> {
  return new Promise((resolve, reject) => {
    let body = "";
    req.setEncoding("utf8");
    req.on("data", (chunk) => {
      body += chunk;
    });
    req.on("end", () => resolve(body));
    req.on("error", reject);
  });
}

export default defineConfig({
  plugins: [react(), telemetryQueryProxy()],
});
