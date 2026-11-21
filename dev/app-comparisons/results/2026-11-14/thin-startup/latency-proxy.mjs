// A TCP proxy that adds round-trip latency to everything (HTTP and WebSocket
// alike; Chrome's own network emulation leaves WebSockets alone): each chunk
// is delayed by rtt/2 in each direction, in order, and a new connection's
// first bytes wait one extra rtt for the TCP handshake a remote server would
// cost. Used by thin-reload.mjs --rtt-ms.
import net from "node:net";

export function latencyProxy({ listenPort, targetPort, rttMs }) {
  const half = rttMs / 2;
  const pipe = (from, to, extraFirst) => {
    let ready = Date.now() + extraFirst;
    from.on("data", (chunk) => {
      const at = Math.max(ready, Date.now() + half);
      ready = at;
      setTimeout(() => to.writable && to.write(chunk), at - Date.now());
    });
    from.on("end", () => setTimeout(() => to.end(), Math.max(0, ready - Date.now()) + half));
    from.on("error", () => to.destroy());
  };
  const server = net.createServer((client) => {
    const upstream = net.connect(targetPort, "127.0.0.1");
    // No Nagle: small back-to-back frames must not wait for an ACK (an RTT).
    client.setNoDelay(true);
    upstream.setNoDelay(true);
    pipe(client, upstream, rttMs);
    pipe(upstream, client, 0);
    upstream.on("close", () => client.destroy());
    client.on("close", () => upstream.destroy());
  });
  return new Promise((resolve) => server.listen(listenPort, "127.0.0.1", () => resolve(() => server.close())));
}
