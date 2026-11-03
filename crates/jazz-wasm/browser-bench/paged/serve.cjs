const http = require("http"),
  fs = require("fs"),
  path = require("path");
const root = process.argv[2];
const types = {
  ".html": "text/html",
  ".js": "text/javascript",
  ".wasm": "application/wasm",
  ".yjs": "application/octet-stream",
};
http
  .createServer((req, res) => {
    const file = path.join(root, decodeURIComponent(req.url.split("?")[0]));
    fs.readFile(file, (e, data) => {
      if (e) {
        res.writeHead(404);
        return res.end();
      }
      res.writeHead(200, {
        "Content-Type": types[path.extname(file)] || "application/octet-stream",
        "Cross-Origin-Opener-Policy": "same-origin",
        "Cross-Origin-Embedder-Policy": "require-corp",
        "Cache-Control": "no-store",
      });
      res.end(data);
    });
  })
  .listen(8767, "127.0.0.1");
