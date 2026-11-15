// Node client helper for smoke tests.
import { createJazzClient } from "jazz-tools/client";
export const PORT = Number(process.env.PORT ?? 8600);
export async function account(u: string): Promise<string> { const r = await fetch(`http://localhost:${PORT}/account/${u}`, { method: "POST" }); return (await r.json()).id; }
export async function clientAs(u: string, claims?: object) {
  const tok = claims ? `${u}|${JSON.stringify(claims)}` : u;
  const s: any = await createJazzClient({ serverUrl: `http://localhost:${PORT}/jazz`, bearer: () => tok } as any);
  await s.refresh?.();
  return s;
}
export const results: string[] = [];
export function check(name: string, ok: boolean, detail = "") { results.push(`${ok ? "PASS" : "FAIL"} ${name}${detail ? " — " + detail : ""}`); console.log(results.at(-1)); }
export function done() { const fail = results.some((r) => r.startsWith("FAIL")); console.log(fail ? "SMOKE FAIL" : "SMOKE PASS"); process.exit(fail ? 1 : 0); }
