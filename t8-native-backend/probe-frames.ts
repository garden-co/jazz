// Item 4: malformed frames and an oversized message must not crash the server (it should close/refuse, then still serve).
// Usage: WS=ws://localhost:PORT/jazz/apps/<id>/ws HEALTH=http://localhost:PORT/<health or any GET> MAX_BYTES=<documented limit> TOKEN=<valid jwt> npx tsx probe-frames.ts
import WebSocket from "ws";
const WS_URL = process.env.WS!, HEALTH = process.env.HEALTH!, MAX = Number(process.env.MAX_BYTES ?? 33554432), TOKEN = process.env.TOKEN;
const alive = async () => (await fetch(HEALTH).then((r) => r.status).catch(() => -1)) !== -1;
async function send(label: string, payload: Buffer | string) {
  const res = await new Promise<string>((done) => {
    const ws = new WebSocket(WS_URL, { headers: TOKEN ? { authorization: `Bearer ${TOKEN}` } : {} });
    const t = setTimeout(() => { ws.terminate(); done("timeout (no close)"); }, 8000);
    ws.on("open", () => ws.send(payload));
    ws.on("close", (c, r) => { clearTimeout(t); done(`closed ${c} ${r.toString().slice(0, 80)}`); });
    ws.on("error", (e) => { clearTimeout(t); done(`error ${e.message.slice(0, 80)}`); });
  });
  const up = await alive();
  console.log(`${up ? "PASS" : "FAIL"} ${label}: ${res}; server ${up ? "still up" : "DOWN"}`);
  return up;
}
let ok = await alive(); if (!ok) { console.log("FAIL server not up before probes"); process.exit(1); }
ok = (await send("garbage binary", Buffer.from([0xff, 0x00, 0x13, 0x37, 0xde, 0xad]))) && ok;
ok = (await send("truncated length prefix", Buffer.from([0x7f, 0xff, 0xff, 0xff]))) && ok;
ok = (await send("text frame junk", "{not json")) && ok;
ok = (await send("empty frame", Buffer.alloc(0))) && ok;
ok = (await send(`oversized ${MAX + 1024} bytes`, Buffer.alloc(MAX + 1024, 7))) && ok;
console.log(ok ? "FRAMES PASS" : "FRAMES FAIL"); process.exit(ok ? 0 : 1);
