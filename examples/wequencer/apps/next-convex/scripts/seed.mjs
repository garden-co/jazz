// `pnpm seed`: resets the local deployment and writes fixtures/wequencer.mjs.
// Needs the backend running (`pnpm backend`). Like the Jazz seed, every user
// signs up through the app's auth (which bootstraps their profile), then the
// creator's sessions are written in bulk by an internal function. Prints
// the harness ctx as JSON.
import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { ConvexHttpClient } from "convex/browser";
import { api } from "../convex/_generated/api.js";
import { wequencerFixture } from "../../../../../dev/app-comparisons/fixtures/wequencer.mjs";

const run = (fn, args = {}) =>
  JSON.parse(execFileSync("npx", ["convex", "run", fn, JSON.stringify(args)], { encoding: "utf8" }) || "null");
const url = readFileSync(".env.local", "utf8").match(/NEXT_PUBLIC_CONVEX_URL=(.*)/)[1].trim();
const fixture = wequencerFixture();

run("seed:reset");
const users = {};
for (const [key, user] of Object.entries(fixture.users)) {
  const client = new ConvexHttpClient(url);
  const { tokens } = await client.action(api.auth.signIn, {
    provider: "password",
    params: { flow: "signUp", email: user.email, password: user.password, name: user.name },
  });
  client.setAuth(tokens.token);
  users[key] = (await client.query(api.users.viewer, {})).id;
}
const { sessionId, onPads } = run("seed:writeSessions", { users });
console.log(JSON.stringify({ sessionId, onPads, users: fixture.users }));
