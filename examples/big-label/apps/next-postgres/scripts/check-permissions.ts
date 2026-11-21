// Checks the README's "Roles" and "Admission boundary" against a running app
// (after `pnpm seed` and `pnpm start`), over HTTP as fixture users: a (admin
// of label 0, editor of label 1), b (editor of label 0), l0m3 (viewer of
// label 0) and l1m1 (label 1 only). It writes test rows, so seed again before
// a benchmark run.
import { readFileSync } from "node:fs";

const origin = process.env.ORIGIN ?? "http://127.0.0.1:4341";
const { labelId, secondLabelId, statusReleaseId } = JSON.parse(readFileSync(".bench-ctx.json", "utf8"));
let failures = 0;
const check = (label: string, ok: boolean) => {
  console.log(`${ok ? "ok  " : "FAIL"} ${label}`);
  if (!ok) failures++;
};

async function as(key: string) {
  let r: Response;
  // Better Auth rate-limits sign-ins: wait it out.
  for (;;) {
    r = await fetch(`${origin}/api/auth/sign-in/email`, {
      method: "POST",
      headers: { origin, "content-type": "application/json" },
      body: JSON.stringify({ email: `${key}@bench.invalid`, password: `bench-password-${key}` }),
    });
    if (r.status !== 429) break;
    await new Promise((resolve) => setTimeout(resolve, 1000));
  }
  const cookie = r.headers.getSetCookie().map((c) => c.split(";")[0]).join("; ");
  const { user } = await r.json();
  const post = (path: string, body: object) =>
    fetch(`${origin}${path}`, { method: "POST", headers: { cookie, origin, "content-type": "application/json" }, body: JSON.stringify(body) });
  const query = async (table: string, where: object = {}, limit = 100) => {
    const res = await post("/api/query", { table, where, orderBy: [], include: {}, limit });
    return res.ok ? ((await res.json()) as Record<string, unknown>[]) : res.status;
  };
  const call = async (name: string, args: object) => (await post(`/api/rpc/${name}`, args)).status;
  return { id: user.id as string, post, query, call };
}
const uuid = () => crypto.randomUUID();

const a = await as("a");
const b = await as("b");
const viewer = await as("l0m3");
const other = await as("l1m1");

check("a label's releases are for its members", ((await b.query("releases", { organizationId: secondLabelId })) as unknown[]).length === 0);
check("members read their label", ((await b.query("releases", { organizationId: labelId })) as unknown[]).length === 100);
check("sign-in emails can't be queried", (await b.query("personEmails")) === 400);
const people = (await b.query("people", {}, 500)) as { userId: string }[];
check("people are visible only to co-members", !people.some((p) => p.userId === other.id) && people.some((p) => p.userId === a.id));

const [release] = (await viewer.query("releases", { id: statusReleaseId })) as Record<string, unknown>[];
const save = (who: typeof a, fields: object, id = statusReleaseId, isNew = false, organizationId = labelId) =>
  who.call("saveRelease", { organizationId, id, isNew, fields: { ...release, ...fields, id: undefined, organizationId: undefined } });
check("viewers can't edit releases", (await save(viewer, { status: "released" })) === 403);
check("editors edit releases", (await save(b, { status: "released" })) === 200);
const [taken] = ((await b.query("releases", { organizationId: labelId }, 2)) as Record<string, string>[]).filter((r) => r.id !== statusReleaseId);
check("catalogue numbers are unique per label", (await save(b, { catalogNumber: taken!.catalogNumber })) === 409);
const [foreignArtist] = (await a.query("artists", { organizationId: secondLabelId }, 1)) as { id: string }[];
check("a release can't point at another label's artist", (await save(a, { artistId: foreignArtist!.id })) === 403);
check("editors can't delete releases", (await b.call("delete", { table: "releases", id: statusReleaseId })) === 403);
const [artist] = (await a.query("artists", { organizationId: labelId }, 1)) as { id: string }[];
check("an artist with releases can't be deleted", (await a.call("delete", { table: "artists", id: artist!.id })) === 409);
check("editors can't create teams", (await b.call("insert", { table: "teams", id: uuid(), row: { organizationId: labelId, name: "x" } })) === 403);
check("viewers can't add artists", (await viewer.call("insert", { table: "artists", id: uuid(), row: { organizationId: labelId, name: "x", genre: "x", status: "active", searchKey: "x" } })) === 403);
check("nobody creates labels", (await a.call("insert", { table: "organizations", id: uuid(), row: { name: "x", slug: uuid() } })) === 403);

const memberships = (await a.query("memberships", { organizationId: labelId })) as { id: string; userId: string }[];
const bMembership = memberships.find((m) => m.userId === b.id)!;
check("no self-promotion", (await b.call("update", { table: "memberships", id: bMembership.id, row: { role: "admin" } })) === 403);
check("members are added by admins only", (await b.post("/api/members", { organizationId: labelId, email: "l1m1@bench.invalid", role: "editor" })).status === 403);
check("never as admins", (await a.post("/api/members", { organizationId: labelId, email: "l1m1@bench.invalid", role: "admin" })).status === 403);
check("admins add members by email", (await a.post("/api/members", { organizationId: labelId, email: "l1m1@bench.invalid", role: "viewer" })).status === 201);
check("a double submit adds them once", (await a.post("/api/members", { organizationId: labelId, email: "l1m1@bench.invalid", role: "viewer" })).status === 409);
check("the new member reads the label", ((await other.query("releases", { organizationId: labelId }, 5)) as unknown[]).length === 5);
const added = ((await a.query("memberships", { organizationId: labelId })) as { id: string; userId: string }[]).find((m) => m.userId === other.id)!;
check("admins remove members", (await a.call("delete", { table: "memberships", id: added.id })) === 200);
check("removed, they read nothing", ((await other.query("releases", { organizationId: labelId }, 5)) as unknown[]).length === 0);
process.exit(failures ? 1 : 0);
