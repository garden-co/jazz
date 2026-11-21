// Checks the README's "Who can do what" against a running server (after
// `pnpm seed` and `pnpm start`), over HTTP as the seed creator, a member and
// an outsider. It writes test rows, so seed again before a benchmark run.
import { readFileSync } from "node:fs";

const origin = process.env.ORIGIN ?? "http://127.0.0.1:4321";
const { busyRoomId: roomId, users } = JSON.parse(readFileSync(".bench-ctx.json", "utf8"));
let failures = 0;
const check = (label: string, ok: boolean) => {
  console.log(`${ok ? "ok  " : "FAIL"} ${label}`);
  if (!ok) failures++;
};

async function as(path: string, body: object) {
  const r = await fetch(`${origin}/api/auth/${path}`, { method: "POST", headers: { origin, "content-type": "application/json" }, body: JSON.stringify(body) });
  const cookie = r.headers.getSetCookie().map((c) => c.split(";")[0]).join("; ");
  const get = (p: string) => fetch(`${origin}/api${p}`, { headers: { cookie } }).then((x) => x.json());
  const call = (name: string, args: object) =>
    fetch(`${origin}/api/rpc/${name}`, { method: "POST", headers: { cookie, "content-type": "application/json" }, body: JSON.stringify(args) }).then((x) => x.status);
  return { get, call };
}
const uuid = () => crypto.randomUUID();

const creator = await as("sign-in/email", { email: users.seed.email, password: users.seed.password });
const member = await as("sign-in/email", { email: users.a.email, password: users.a.password });
const outsider = await as("sign-up/email", { email: `out-${Date.now()}@bench.invalid`, password: "bench-password-out", name: "Outsider" });
const outsiderProfile = uuid();
await outsider.call("profiles.create", { id: outsiderProfile, displayName: "Outsider" });
const [memberProfile] = await member.get("/profiles/mine");
const [creatorProfile] = await creator.get("/profiles/mine");

check("outsider can't read messages", (await outsider.get(`/rooms/${roomId}/messages`)).length === 0);
check("outsider can't read members", (await outsider.get(`/rooms/${roomId}/members`)).length === 0);
check("outsider can't post", (await outsider.call("messages.send", { id: uuid(), roomId, senderId: outsiderProfile, text: "x" })) === 403);
check("member reads the newest 200", (await member.get(`/rooms/${roomId}/messages`)).length === 200);
check("member posts as themselves", (await member.call("messages.send", { id: uuid(), roomId, senderId: memberProfile.id, text: "hi" })) === 200);
check("member can't post as someone else", (await member.call("messages.send", { id: uuid(), roomId, senderId: creatorProfile.id, text: "x" })) === 403);
check("member can't rename the room", (await member.call("rooms.rename", { id: roomId, name: "x" })) === 403);
check("creator renames the room", (await creator.call("rooms.rename", { id: roomId, name: "Busy room" })) === 200);
const [theirs] = await member.get(`/rooms/${roomId}/messages`);
check("can't delete someone else's message", theirs.senderId === memberProfile.id || (await member.call("messages.delete", { id: theirs.id })) === 403);
check("creator can't admit someone who didn't ask", (await creator.call("members.admit", { id: uuid(), roomId, memberAuthor: "nobody", memberProfileId: outsiderProfile })) === 403);
check("outsider can't see join requests", (await outsider.get(`/rooms/${roomId}/join-requests`)).length === 0);
const request = uuid();
check("outsider asks to join", (await outsider.call("joinRequests.create", { id: request, roomId, profileId: outsiderProfile })) === 200);
check("member can't see join requests", (await member.get(`/rooms/${roomId}/join-requests`)).length === 0);
const [asked] = await creator.get(`/rooms/${roomId}/join-requests`);
check("creator sees the request", asked?.id === request);
check("member can't admit", (await member.call("members.admit", { id: uuid(), roomId, memberAuthor: asked.requester, memberProfileId: outsiderProfile })) === 403);
check("creator admits", (await creator.call("members.admit", { id: uuid(), roomId, memberAuthor: asked.requester, memberProfileId: outsiderProfile })) === 200);
check("the admitted outsider reads messages", (await outsider.get(`/rooms/${roomId}/messages`)).length === 200);
const marker = uuid();
await member.call("readMarkers.mark", { id: marker, roomId, lastReadAt: Date.now() });
check("read markers are private", !(await outsider.get("/read-markers")).some((m: { id: string }) => m.id === marker));
check("others can't move your marker", (await outsider.call("readMarkers.mark", { id: marker, roomId, lastReadAt: 0 })) === 403);
const members = await creator.get(`/rooms/${roomId}/members`);
const memberRow = members.find((m: { memberProfileId: string }) => m.memberProfileId === memberProfile.id);
const outsiderRow = members.find((m: { memberProfileId: string }) => m.memberProfileId === outsiderProfile);
check("member can't remove others", (await member.call("members.remove", { id: outsiderRow.id })) === 403);
check("creator removes a member", (await creator.call("members.remove", { id: outsiderRow.id })) === 200);
check("removed, they read nothing", (await outsider.get(`/rooms/${roomId}/messages`)).length === 0);
void memberRow;
process.exit(failures ? 1 : 0);
