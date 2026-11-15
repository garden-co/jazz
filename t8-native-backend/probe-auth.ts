// Item 1: missing / expired / wrong-issuer / bad-signature / no-sub tokens must be refused by the backend's /jazz.
// Usage: BASE=http://localhost:PORT/jazz JWKS=http://localhost:8690 PROBE_PATH=<an authenticated endpoint, e.g. /apps/<id>/accounts/backend-user> npx tsx probe-auth.ts
const BASE = process.env.BASE!, JWKS = process.env.JWKS ?? "http://localhost:8690", P = process.env.PROBE_PATH ?? "/";
const tok = async (kind: string) => (await fetch(`${JWKS}/token?sub=alice&kind=${kind}`)).text();
let fail = false;
for (const kind of ["missing", "expired", "wrong-iss", "bad-sig", "no-sub", "valid"]) {
  const h: Record<string, string> = { "content-type": "application/json" };
  if (kind !== "missing") h.authorization = `Bearer ${await tok(kind)}`;
  const r = await fetch(BASE + P, { method: process.env.METHOD ?? "POST", headers: h, body: process.env.BODY ?? "{}" }).catch((e) => ({ status: -1, text: async () => String(e) } as any));
  const body = (await r.text()).slice(0, 160);
  const refused = r.status === 401 || r.status === 403;
  const ok = kind === "valid" ? r.status >= 200 && r.status < 300 : refused;
  if (!ok) fail = true;
  console.log(`${ok ? "PASS" : "FAIL"} ${kind}: ${r.status} ${body}`);
}
// oversized account-route body (> 64 KiB) must be refused with 413, not crash
{ const r = await fetch(BASE + P, { method: "POST", headers: { "content-type": "application/json", authorization: `Bearer ${await tok("valid")}` }, body: JSON.stringify({ pad: "x".repeat(64 * 1024 + 16) }) }).catch((e) => ({ status: -1 } as any));
  const ok = r.status === 413; if (!ok) fail = true; console.log(`${ok ? "PASS" : "FAIL"} oversized account body: ${r.status} (expected 413)`); }
console.log(fail ? "AUTH FAIL" : "AUTH PASS"); process.exit(fail ? 1 : 0);
