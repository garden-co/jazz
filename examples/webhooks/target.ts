import { createServer } from "http";

const PORT = 4444;

const server = createServer((req, res) => {
  let body = "";

  req.on("data", (chunk) => {
    body += chunk.toString();
  });

  req.on("end", () => {
    console.log("=== Incoming Request ===");
    console.log("Method:", req.method);
    console.log("URL:", req.url);
    console.log("Headers:", JSON.stringify(req.headers, null, 2));
    console.log("Body:", body);
    console.log("========================\n");

    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ status: "received" }));
  });
});

server.listen(PORT, () => {
  console.log(`Webhook target server listening on http://localhost:${PORT}`);
});
