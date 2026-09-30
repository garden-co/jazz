import { describe, expect, it } from "vitest";
import { POST as bootstrap } from "../app/api/bootstrap/route.js";
import { POST as demoData } from "../app/api/demo-data/route.js";
import { POST as members } from "../app/api/members/route.js";

const post = (headers: Record<string, string> = {}) =>
  new Request("http://127.0.0.1:3000/api", { method: "POST", headers });

describe("BigLabel API authentication", () => {
  it.each([
    ["bootstrap", bootstrap],
    ["demo-data", demoData],
    ["members", members],
  ])("answers an unauthenticated %s request with 401", async (_route, handler) => {
    expect((await handler(post())).status).toBe(401);
    expect((await handler(post({ authorization: "Bearer not-a-jwt" }))).status).toBe(401);
  });
});
