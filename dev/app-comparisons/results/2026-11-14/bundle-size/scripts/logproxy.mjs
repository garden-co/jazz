// Usage: node logproxy.mjs <listenPort> <targetPort>
// Logs every asset request (page, SharedWorker, WASM) to stdout; proxies HTTP and WebSocket upgrades.
import http from "node:http"; import net from "node:net";
const [port, target] = process.argv.slice(2).map(Number);
const s = http.createServer((req, res) => {
  console.log("GET", req.url);
  const p = http.request({ port: target, path: req.url, method: req.method, headers: req.headers }, (r) => { res.writeHead(r.statusCode, r.headers); r.pipe(res); });
  req.pipe(p);
});
s.on("upgrade", (req, sock, head) => {
  const t = net.connect(target, "127.0.0.1", () => {
    t.write(`${req.method} ${req.url} HTTP/1.1\r\n` + Object.entries(req.headers).map(([k, v]) => `${k}: ${v}`).join("\r\n") + "\r\n\r\n"); t.write(head); sock.pipe(t).pipe(sock);
  });
});
s.listen(port);
