// Calls the rc's real openVercelCore against production's existing core with a WRONG region,
// using the real @vercel/sandbox only for read-only lookups. Every call that could create,
// resume, start, write or extend anything is replaced by a function that throws, and logged.
// usage: node wrong-region-probe.mjs <app dir with the release installed> <wrong region>
// env: VERCEL_TOKEN, VERCEL_ORG_ID (team id), VERCEL_PROJECT_ID; FAIL_GET_AT=n makes the n-th
// Sandbox.get throw once (a transient 503); PROBE_VERSION overrides the version it reports.
// Prints the calls it saw and the result. Nothing can be created: see the stubs below.
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { createRequire } from "node:module";
const [app, region] = process.argv.slice(2);
const req = createRequire(join(app, "package.json"));
const store = await import(
  pathToFileURL(join(req.resolve("jazz-tools/package.json"), "../dist/backend/vercel-store.js"))
    .href
);
const real = await import(pathToFileURL(req.resolve("@vercel/sandbox")).href);
const calls = [];
// FAIL_GET_AT=n: the n-th Sandbox.get (1-based) throws once, as a transient API error would.
const failAt = Number(process.env.FAIL_GET_AT || 0);
let getCount = 0;
const blocked = (what) => async () => {
  calls.push(`BLOCKED ${what}`);
  throw new Error(`BLOCKED ${what}`);
};
const READ_ONLY = new Set(["region", "status", "name", "id", "sandboxId"]);
const wrapSbx = (s) =>
  s &&
  new Proxy(s, {
    get(t, p) {
      if (p === "then") return undefined;
      if (READ_ONLY.has(p)) {
        calls.push(`read sandbox.${String(p)} = ${t[p]}`);
        return t[p];
      }
      if (p === "currentSession")
        return () => ({
          startedAt: new Date(0),
          timeout: 0,
          extendTimeout: blocked("session.extendTimeout"),
        });
      return typeof t[p] === "function" ? blocked(`sandbox.${String(p)}`) : t[p];
    },
  });
const sdk = new Proxy(
  {
    Sandbox: new Proxy(
      {
        get: async (p) => {
          getCount++;
          calls.push(`Sandbox.get #${getCount} name=${p.name} resume=${p.resume}`);
          if (getCount === failAt) {
            calls.push(`  -> injected transient failure (503)`);
            const e = new Error("injected transient 503 from Sandbox.get");
            e.status = 503;
            throw e;
          }
          if (p.resume !== false) return blocked("Sandbox.get(resume)")();
          const { onResume, ...rest } = p;
          return wrapSbx(await real.Sandbox.get({ ...rest, resume: false }));
        },
      },
      { get: (t, p) => t[p] ?? (typeof p === "string" ? blocked(`Sandbox.${p}`) : undefined) },
    ),
    Drive: new Proxy(
      {},
      {
        get: (t, p) =>
          typeof p === "string"
            ? async (args) => {
                calls.push(
                  `BLOCKED Drive.${p} ${JSON.stringify({ name: args?.name, region: args?.region })}`,
                );
                throw new Error(`BLOCKED Drive.${p}`);
              }
            : undefined,
      },
    ),
  },
  {
    get: (t, p) =>
      t[p] ?? (typeof p === "string" && p !== "then" ? blocked(`sdk.${p}`) : undefined),
  },
);
const proj = { orgId: process.env.VERCEL_ORG_ID, projectId: process.env.VERCEL_PROJECT_ID };
if (!process.env.VERCEL_TOKEN || !proj.orgId || !proj.projectId)
  throw new Error("set VERCEL_TOKEN, VERCEL_ORG_ID, VERCEL_PROJECT_ID");
const b64 = (o) => Buffer.from(JSON.stringify(o)).toString("base64url");
const fakeJwt = `${b64({ alg: "none" })}.${b64({ iss: "https://oidc.vercel.com/probe", aud: "probe", sub: "owner:probe:project:probe:environment:production" })}.x`;
let result;
try {
  await store.openVercelCore({
    options: {
      namespace: process.env.T4_MODE === "preview" ? "cli-previews" : "production",
      region,
      sdk,
      credentials: {
        token: process.env.VERCEL_TOKEN,
        teamId: proj.orgId,
        projectId: proj.projectId,
      },
      backendCredential: async () => fakeJwt,
    },
    openRemote: blocked("openRemote"),
    env: {},
    version: process.env.PROBE_VERSION,
    sdk,
  });
  result = "NO ERROR (unexpected)";
} catch (e) {
  result = String(e?.message ?? e);
}
console.log("calls:\n  " + calls.join("\n  "));
console.log("result:", result);
const reachedCreate = calls.some((c) => /BLOCKED (Drive\.getOrCreate|Sandbox\.create)/.test(c));
console.log(
  reachedCreate
    ? "FINDING: reached a create call with the wrong region (blocked)"
    : /isn't supported yet/.test(result)
      ? "PASS: refused before any create call"
      : "FAIL: unexpected result",
);
