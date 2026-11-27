// Checks the README's "Who can do what" against a running app (after
// `pnpm seed` and `pnpm start`), over HTTP as the creator, an editor and a
// new outsider who is then added as a viewer. It writes test rows, so seed
// again before a benchmark run.
import { readFileSync } from "node:fs";

const origin = process.env.ORIGIN ?? "http://127.0.0.1:4331";
const { sessionId, users } = JSON.parse(readFileSync(".bench-ctx.json", "utf8"));
let failures = 0;
const check = (label: string, ok: boolean) => {
  console.log(`${ok ? "ok  " : "FAIL"} ${label}`);
  if (!ok) failures++;
};

async function as(path: string, body: object) {
  const r = await fetch(`${origin}/api/auth/${path}`, { method: "POST", headers: { origin, "content-type": "application/json" }, body: JSON.stringify(body) });
  const cookie = r.headers.getSetCookie().map((c) => c.split(";")[0]).join("; ");
  const { user } = await r.json();
  const get = (p: string) => fetch(`${origin}/api${p}`, { headers: { cookie } }).then((x) => x.json());
  const call = (name: string, args: object) =>
    fetch(`${origin}/api/rpc/${name}`, { method: "POST", headers: { cookie, "content-type": "application/json" }, body: JSON.stringify(args) }).then((x) => x.status);
  await fetch(`${origin}/api/bootstrap`, { method: "POST", headers: { cookie } });
  return { id: user.id as string, get, call };
}
const uuid = () => crypto.randomUUID();

const creator = await as("sign-in/email", { email: users.seed.email, password: users.seed.password });
const editor = await as("sign-in/email", { email: users.a.email, password: users.a.password });
const outsider = await as("sign-up/email", { email: `out-${Date.now()}@bench.invalid`, password: "bench-password-out", name: "Outsider" });

const [pattern] = await editor.get(`/sessions/${sessionId}/patterns`);
const [track] = await editor.get(`/sessions/${sessionId}/tracks`);
const step = { id: uuid(), session_id: sessionId, track_id: track.id, pattern_id: pattern.id, position: 3, enabled: true, velocity: 100, probability: 100 };

check("outsider can't read the session", (await outsider.get(`/sessions/${sessionId}`)) === null);
check("outsider can't read tracks", (await outsider.get(`/sessions/${sessionId}/tracks`)).length === 0);
check("outsider can't read pads", (await outsider.get(`/sessions/${sessionId}/patterns/${pattern.id}/steps`)).length === 0);
check("outsider can't toggle a pad", (await outsider.call("steps.set", step)) === 403);
check("editor toggles a pad", (await editor.call("steps.set", step)) === 200);
check("editor can't add members", (await editor.call("members.add", { id: uuid(), session_id: sessionId, member_author: outsider.id, role: "editor" })) === 403);
check("editor can't delete a track", (await editor.call("tracks.remove", { id: track.id })) === 403);
check("creator adds a viewer", (await creator.call("members.add", { id: uuid(), session_id: sessionId, member_author: outsider.id, role: "viewer" })) === 200);
check("the viewer reads pads", (await outsider.get(`/sessions/${sessionId}/patterns/${pattern.id}/steps`)).length > 0);
check("the viewer can't toggle", (await outsider.call("steps.set", { ...step, enabled: false })) === 403);
check("the viewer can't play", (await outsider.call("transport.observe", { id: uuid(), session_id: sessionId, playing: true, bar: 0, observed_at: Date.now(), tempo_bpm: 120 })) === 403);
const [otherSession] = (await creator.get("/sessions")).filter((s: { id: string }) => s.id !== sessionId);
check("a pad must use this session's track", (await editor.call("steps.set", { ...step, id: uuid(), session_id: otherSession.id })) === 403);
const [creatorProfile] = [await creator.get("/profile")];
check("presence only for your own profile", (await editor.call("presence.beat", { id: uuid(), session_id: sessionId, profile_id: creatorProfile.id, cursor_step: 0, heartbeat_at: Date.now() })) === 403);
const members = await creator.get(`/sessions/${sessionId}/members`);
const viewerRow = members.find((m: { member_author: string }) => m.member_author === outsider.id);
check("editor can't remove members", (await editor.call("members.remove", { id: viewerRow.id })) === 403);
check("creator removes the viewer", (await creator.call("members.remove", { id: viewerRow.id })) === 200);
check("removed, they read nothing", (await outsider.get(`/sessions/${sessionId}/tracks`)).length === 0);
process.exit(failures ? 1 : 0);
