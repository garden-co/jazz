import { readFileSync } from "node:fs";
import { createServer } from "node:net";

/**
 * A loopback port nothing listens on, for a server that is down (and may
 * come up there later), or for a client that must be refused.
 *
 * Not one from `listen(0)`: once released, an ephemeral port goes back to
 * the pool, where any other test's `listen(0)` or outgoing connection can
 * take it before the test uses it, and the test then reaches some other
 * server. This one is picked below the ephemeral range, which only a server
 * asking for that exact port can take. Where the server under test can bind
 * port 0 and say which port it got, let it do that instead.
 */
export async function closedLoopbackPort(): Promise<number> {
  let ephemeralStart = 32_768;
  try {
    const range = readFileSync("/proc/sys/net/ipv4/ip_local_port_range", "utf8");
    const first = Number(range.trim().split(/\s+/)[0]);
    if (first > 10_000) ephemeralStart = Math.min(ephemeralStart, first);
  } catch {
    // Not Linux: macOS and Windows hand out ephemeral ports from 49152.
  }
  for (;;) {
    const port = 10_000 + Math.floor(Math.random() * (ephemeralStart - 10_000));
    const free = await new Promise<boolean>((resolve) => {
      const probe = createServer();
      probe.once("error", () => resolve(false));
      probe.listen(port, "127.0.0.1", () => probe.close(() => resolve(true)));
    });
    if (free) return port;
  }
}
