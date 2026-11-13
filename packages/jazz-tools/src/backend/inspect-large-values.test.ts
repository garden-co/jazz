import { describe, expect, it } from "vitest";
import { createInspector, type InspectorDeps } from "./inspect.js";
import { runInspectCommand } from "../inspect/cli.js";
import {
  LARGE_VALUE_BYTES,
  isLargeValueSummary,
  summarizeLargeValue,
  toJsonValue,
} from "../inspect/protocol.js";

// #5202: a row with a large value (a 5-10 MB photo) went missing from
// Inspector answers: the answer's byte limit dropped it, so query answered
// rows: [] truncated, and get answered row: null for a row that exists.

const ID = "11111111-1111-4111-8111-111111111111";
const photo = new Uint8Array(5 * 1024 * 1024).map((_, index) => index % 251);
const caption = "x".repeat(200_000);

function inspector() {
  const db = {
    all: async () => [{ id: ID, name: "beach", photo, caption, count: 3 }],
  };
  const handle = createInspector({
    appId: "app",
    basePath: "/jazz",
    publicUrls: [],
    options: {},
    schema: () => ({
      entries: {
        columns: [
          { name: "name", column_type: { type: "Text" }, nullable: false },
          { name: "photo", column_type: { type: "Bytea" }, nullable: false },
          { name: "caption", column_type: { type: "Text" }, nullable: false },
          { name: "count", column_type: { type: "Integer" }, nullable: false },
        ],
      },
    }),
    permissions: {},
    db: () => db,
    dbAs: async () => db,
    sessions: () => [],
    hook: () => ({ by: "ops@example.com", write: false }),
    tokenSecret: async () => null,
    remoteAddress: () => undefined,
    serveUi: async () => null,
    env: { NODE_ENV: "production" },
  } as unknown as InspectorDeps);
  return async (body: unknown) => {
    const response = await handle.handle(
      new Request("https://app.example.com/jazz/inspect/api", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(body),
      }),
      "/inspect/api",
    );
    return { status: response.status, body: (await response.json()) as any };
  };
}

describe("large values in Inspector answers", () => {
  it("summarizes a large value, so its row is listed and get finds it", async () => {
    const call = inspector();
    const listed = await call({ op: "query", query: { table: "entries" } });
    expect(listed.body.truncated).toBe(false);
    expect(listed.body.rows).toHaveLength(1);
    const [row] = listed.body.rows;
    expect(row).toMatchObject({ id: ID, name: "beach", count: 3 });
    expect(row.photo.$large).toMatchObject({ kind: "bytes", size: photo.length });
    expect(row.caption.$large).toMatchObject({ kind: "text", size: caption.length });
    expect(JSON.stringify(listed.body).length).toBeLessThan(10_000);

    const got = await call({ op: "get", table: "entries", id: ID });
    expect(got.body.row).toMatchObject({ id: ID, name: "beach" });
    expect(isLargeValueSummary(got.body.row.photo)).toBe(true);
  });

  it("answers one value with get and column, in parts when it's too large for one", async () => {
    const call = inspector();
    const text = await call({ op: "get", table: "entries", id: ID, column: "caption" });
    expect(text.body).toMatchObject({
      value: caption,
      size: caption.length,
      from: 0,
      to: caption.length,
    });

    const whole = await call({ op: "get", table: "entries", id: ID, column: "photo" });
    expect(whole).toMatchObject({
      status: 400,
      body: { message: expect.stringMatching(/in parts with from and to/) },
    });

    const part = await call({
      op: "get",
      table: "entries",
      id: ID,
      column: "photo",
      from: 10,
      to: 20,
    });
    expect(part.body).toMatchObject({ size: photo.length, from: 10, to: 20 });
    expect(part.body.value).toEqual(toJsonValue(photo.subarray(10, 20)));

    expect(
      (await call({ op: "get", table: "entries", id: ID, column: "count", from: 1 })).status,
    ).toBe(400);
    expect(
      (await call({ op: "get", table: "entries", id: ID, column: "photo", from: -1 })).status,
    ).toBe(400);
  });

  it("the CLI's get passes --column, --from and --to", async () => {
    const bodies: any[] = [];
    let out = "";
    await runInspectCommand(
      [
        "get",
        "entries",
        ID,
        "--column",
        "photo",
        "--from",
        "0",
        "--to",
        "1024",
        "--app",
        "http://localhost:3000/jazz",
      ],
      {
        stdout: (text) => (out += text),
        stderr: () => {},
        env: {},
        fetch: (async (_url: string, init?: RequestInit) => {
          bodies.push(JSON.parse(String(init?.body)));
          return Response.json({ value: null });
        }) as typeof fetch,
      },
    );
    expect(bodies).toEqual([
      { op: "get", table: "entries", id: ID, column: "photo", from: 0, to: 1024 },
    ]);
    expect(
      await runInspectCommand(["get", "entries", ID, "--from", "x"], {
        stdout: (t) => (out = t),
        stderr: () => {},
        env: {},
      }),
    ).toBe(1);
    expect(out).toMatch(/--from must be a whole number/);
  });
});

describe("summarizeLargeValue", () => {
  it("leaves small values alone and summarizes large ones by kind", () => {
    expect(summarizeLargeValue("short")).toBe("short");
    expect(summarizeLargeValue(toJsonValue(new Uint8Array(10)))).toEqual(
      toJsonValue(new Uint8Array(10)),
    );
    expect(summarizeLargeValue({ small: [1, 2] })).toEqual({ small: [1, 2] });
    expect(summarizeLargeValue(42)).toBe(42);
    expect(summarizeLargeValue(null)).toBeNull();

    const bytes = summarizeLargeValue(toJsonValue(new Uint8Array(100_001))) as any;
    expect(bytes.$large).toMatchObject({ kind: "bytes", size: 100_001 });
    expect(bytes.$large.preview.length).toBeLessThanOrEqual(200);
    const json = summarizeLargeValue({ list: Array.from({ length: 20_000 }, (_, i) => i) }) as any;
    expect(json.$large.kind).toBe("json");
    expect(json.$large.size).toBeGreaterThan(LARGE_VALUE_BYTES);
  });
});
