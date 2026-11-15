// Raw RFC 6455 frame probes (things `ws` can't send): unmasked, reserved bits, unknown opcode, oversized control,
// text frame, bad close code, and a 64-bit length header just over the cap (only the header is sent).
// Usage: HOST=127.0.0.1 PORT=<p> WSPATH=/jazz/apps/<id>/ws TOKEN=<jwt> HEALTH=http://127.0.0.1:<p>/healthz CAP=33554432 npx tsx probe-raw.ts
import net from "node:net"; import crypto from "node:crypto";
const HOST = process.env.HOST ?? "127.0.0.1", PORT = Number(process.env.PORT), PATH = process.env.WSPATH!, TOKEN = process.env.TOKEN, HEALTH = process.env.HEALTH!, CAP = Number(process.env.CAP ?? 33554432);
const mask = (payload: Buffer, m: Buffer) => Buffer.from(payload.map((b, i) => b ^ m[i % 4]));
function frame(opcode: number, payload: Buffer, opts: { masked?: boolean; rsv?: number; fin?: boolean; lenOverride?: bigint } = {}) {
  const { masked = true, rsv = 0, fin = true } = opts; const len = opts.lenOverride ?? BigInt(payload.length);
  const head: number[] = [(fin ? 0x80 : 0) | (rsv << 4) | opcode];
  let ext = Buffer.alloc(0);
  if (len < 126n) head.push((masked ? 0x80 : 0) | Number(len));
  else if (len < 65536n) { head.push((masked ? 0x80 : 0) | 126); ext = Buffer.alloc(2); ext.writeUInt16BE(Number(len)); }
  else { head.push((masked ? 0x80 : 0) | 127); ext = Buffer.alloc(8); ext.writeBigUInt64BE(len); }
  const m = crypto.randomBytes(4);
  return Buffer.concat([Buffer.from(head), ext, masked ? m : Buffer.alloc(0), masked ? mask(payload, m) : payload]);
}
async function probe(label: string, bytes: Buffer, expect: number[]) {
  const res = await new Promise<string>((done) => {
    const s = net.connect(PORT, HOST); let buf = Buffer.alloc(0), upgraded = false;
    const t = setTimeout(() => { s.destroy(); done("timeout"); }, 8000);
    s.on("connect", () => s.write([`GET ${PATH} HTTP/1.1`, `Host: ${HOST}:${PORT}`, "Upgrade: websocket", "Connection: Upgrade",
      `Sec-WebSocket-Key: ${crypto.randomBytes(16).toString("base64")}`, "Sec-WebSocket-Version: 13", ...(TOKEN ? [`Authorization: Bearer ${TOKEN}`] : []), "", ""].join("\r\n")));
    s.on("data", (d) => { buf = Buffer.concat([buf, d]);
      if (!upgraded) { const i = buf.indexOf("\r\n\r\n"); if (i < 0) return; const head = buf.subarray(0, i).toString(); if (!/ 101 /.test(head)) { clearTimeout(t); s.destroy(); return done("no upgrade: " + head.split("\r\n")[0]); } upgraded = true; buf = buf.subarray(i + 4); s.write(bytes); }
      if (upgraded && buf.length >= 2 && (buf[0] & 0x0f) === 8) { const code = buf.length >= 4 ? buf.readUInt16BE(2) : 0; clearTimeout(t); s.destroy(); done(`close ${code}`); } });
    s.on("close", () => { clearTimeout(t); done("socket closed (no close frame)"); }); s.on("error", (e) => { clearTimeout(t); done("error " + e.message); });
  });
  const up = (await fetch(HEALTH).then((r) => r.status).catch(() => -1)) !== -1;
  const code = Number(res.match(/close (\d+)/)?.[1] ?? 0);
  const ok = up && (expect.length === 0 || expect.includes(code));
  console.log(`${ok ? "PASS" : "FAIL"} ${label}: ${res} (expected ${expect.join("/") || "any"}); server ${up ? "up" : "DOWN"}`); return ok;
}
let ok = true; const p = Buffer.from("x");
ok = (await probe("unmasked binary", frame(2, p, { masked: false }), [1002])) && ok;
ok = (await probe("reserved bits set", frame(2, p, { rsv: 4 }), [1002])) && ok;
ok = (await probe("unknown opcode 0x3", frame(3, p), [1002])) && ok;
ok = (await probe("ping > 125 bytes", frame(9, Buffer.alloc(126)), [1002])) && ok;
ok = (await probe("fragmented control", frame(9, p, { fin: false }), [1002])) && ok;
ok = (await probe("continuation without start", frame(0, p), [1002])) && ok;
ok = (await probe("text frame", frame(1, Buffer.from("{}")), [1003, 1008])) && ok;
ok = (await probe("close with bad code 999", frame(8, Buffer.from([0x03, 0xe7])), [1002])) && ok;
ok = (await probe(`64-bit length ${CAP + 1} (header only)`, frame(2, Buffer.alloc(0), { lenOverride: BigInt(CAP + 1) }), [1009])) && ok;
console.log(ok ? "RAW PASS" : "RAW FAIL"); process.exit(ok ? 0 : 1);
