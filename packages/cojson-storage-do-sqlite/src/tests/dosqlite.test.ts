import { dirname, join } from "path";
import { randomUUID } from "node:crypto";
import { fileURLToPath } from "url";
import { execa } from "execa";
import { afterAll, beforeAll, describe, expect, test } from "vitest";

// @ts-ignore
const currentDir = dirname(fileURLToPath(import.meta.url));
const packageRoot = join(currentDir, "../..");
const projectRoot = join(packageRoot, "../..");

describe("Cloudflare DurableObject SQL storage", () => {
  let server: ReturnType<typeof execa>;
  let url: URL;

  beforeAll(async () => {
    server = execa(join(projectRoot, "node_modules/.bin/wrangler"), ["dev"], {
      cwd: currentDir,
    });
    // Wait for server to be ready
    url = await new Promise<URL>((resolve, reject) => {
      server.stdout?.on("data", (data) => {
        console.log("stdout server:", data.toString());
        if (data.toString().includes("Ready on http://localhost:")) {
          resolve(new URL(data.toString().split("Ready on ")[1].trim()));
        }
      });

      server.stderr?.on("data", (data) => {
        console.log("stderr server:", data.toString());
      });

      // Reject if server fails to start within 10 seconds
      setTimeout(() => {
        reject(new Error("Server failed to start within timeout"));
      }, 10000);
    });
  });

  afterAll(async () => {
    server.kill();
  });

  test("should sync and load data from storage", async () => {
    const doId = randomUUID();
    const res = await fetch(`${url}sync-and-load`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ doId }),
    });

    const json = (await res.json()) as unknown;
    if (
      !(
        typeof json === "object" &&
        json !== null &&
        "messageRes1" in json &&
        "messageRes2" in json &&
        "map2Hello" in json
      )
    ) {
      throw new Error("Invalid response");
    }
    expect(json.messageRes1).toMatchInlineSnapshot(`
      [
        "client -> CONTENT Group header: true new: After: 0 New: 4",
        "client -> CONTENT Map header: true new: After: 0 New: 1",
      ]
    `);
    expect(json.map2Hello).toEqual("world");
    expect(json.messageRes2).toMatchInlineSnapshot(`
      [
        "client -> LOAD Map sessions: empty",
        "storage -> CONTENT Group header: true new: After: 0 New: 4",
        "storage -> CONTENT Map header: true new: After: 0 New: 1",
      ]
    `);
  });

  test("should send an empty content message if there is no content", async () => {
    const doId = randomUUID();
    const res = await fetch(`${url}sync-and-load-empty`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ doId }),
    });

    const json = (await res.json()) as unknown;
    if (
      !(
        typeof json === "object" &&
        json !== null &&
        "messageRes1" in json &&
        "messageRes2" in json
      )
    ) {
      throw new Error("Invalid response");
    }
    expect(json.messageRes1).toMatchInlineSnapshot(`
      [
        "client -> CONTENT Group header: true new: After: 0 New: 4",
        "client -> CONTENT Map header: true new: ",
      ]
    `);
    expect(json.messageRes2).toMatchInlineSnapshot(`
      [
        "client -> LOAD Map sessions: empty",
        "storage -> CONTENT Group header: true new: After: 0 New: 4",
        "storage -> CONTENT Map header: true new: ",
      ]
    `);
  });

  test("should load dependencies correctly (group inheritance)", async () => {
    const doId = randomUUID();
    const res = await fetch(`${url}group-load`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ doId }),
    });

    const json = (await res.json()) as unknown;
    if (
      !(
        typeof json === "object" &&
        json !== null &&
        "messageRes1" in json &&
        "messageRes2" in json &&
        "mapLoaded" in json &&
        "groupLoaded" in json &&
        "parentGroupLoaded" in json
      )
    ) {
      throw new Error("Invalid response");
    }

    expect(json.messageRes1).toMatchInlineSnapshot(`
      [
        "client -> CONTENT Group header: true new: After: 0 New: 4",
        "client -> CONTENT ParentGroup header: true new: After: 0 New: 4",
        "client -> CONTENT Group header: false new: After: 4 New: 2",
        "client -> CONTENT Map header: true new: After: 0 New: 1",
      ]
    `);
    expect(json.mapLoaded).toBeTruthy();
    expect(json.groupLoaded).toBeTruthy();
    expect(json.parentGroupLoaded).toBeTruthy();

    expect(json.messageRes2).toMatchInlineSnapshot(`
      [
        "client -> LOAD Map sessions: empty",
        "storage -> CONTENT ParentGroup header: true new: After: 0 New: 4",
        "storage -> CONTENT Group header: true new: After: 0 New: 6",
        "storage -> CONTENT Map header: true new: After: 0 New: 1",
      ]
    `);
  });

  test("should not send the same dependency value twice", async () => {
    const doId = randomUUID();
    const res = await fetch(`${url}group-load-duplicate`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ doId }),
    });

    const json = (await res.json()) as unknown;
    if (
      !(
        typeof json === "object" &&
        json !== null &&
        "messageRes1" in json &&
        "mapLoaded" in json &&
        "mapFromParentLoaded" in json &&
        "groupLoaded" in json &&
        "parentGroupLoaded" in json
      )
    ) {
      console.log(json);
      throw new Error("Invalid response");
    }

    expect(json.mapLoaded).toBeTruthy();
    expect(json.mapFromParentLoaded).toBeTruthy();
    expect(json.groupLoaded).toBeTruthy();
    expect(json.parentGroupLoaded).toBeTruthy();
    expect(json.messageRes1).toMatchInlineSnapshot(`
      [
        "client -> LOAD Map sessions: empty",
        "storage -> CONTENT ParentGroup header: true new: After: 0 New: 4",
        "storage -> CONTENT Group header: true new: After: 0 New: 6",
        "storage -> CONTENT Map header: true new: After: 0 New: 1",
        "client -> LOAD MapFromParent sessions: empty",
        "storage -> CONTENT MapFromParent header: true new: After: 0 New: 1",
      ]
    `);
  });

  test("should recover from data loss", async () => {
    const doId = randomUUID();
    const res = await fetch(`${url}data-loss-recovery`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ doId }),
    });

    const json = (await res.json()) as unknown;
    if (
      !(
        typeof json === "object" &&
        json !== null &&
        "messageRes1" in json &&
        "messageRes2" in json &&
        "mapContent" in json
      )
    ) {
      console.log(json);
      throw new Error("Invalid response");
    }

    expect(json.messageRes1).toMatchInlineSnapshot(`
      [
        "client -> CONTENT Group header: true new: After: 0 New: 4",
        "client -> CONTENT Map header: true new: After: 0 New: 1",
        "client -> CONTENT Map header: false new: After: 3 New: 1",
        "storage -> KNOWN CORRECTION Map sessions: header/4",
        "client -> CONTENT Map header: false new: After: 1 New: 3",
      ]
    `);

    expect(json.mapContent).toEqual({
      "0": 0,
      "1": 1,
      "2": 2,
      "3": 3,
    });

    expect(json.messageRes2).toMatchInlineSnapshot(`
      [
        "client -> LOAD Map sessions: empty",
        "storage -> CONTENT Group header: true new: After: 0 New: 4",
        "storage -> CONTENT Map header: true new: After: 0 New: 4",
      ]
    `);
  });

  test("should recover missing dependencies from storage", async () => {
    const doId = randomUUID();
    const res = await fetch(`${url}missing-dependency-recovery`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ doId }),
    });

    const json = (await res.json()) as unknown;
    if (!(typeof json === "object" && json !== null && "mapContent" in json)) {
      console.log(json);
      throw new Error("Invalid response");
    }

    expect(json.mapContent).toEqual({
      "0": 0,
    });
  });

  test("should sync multiple sessions in a single content message", async () => {
    const doId = randomUUID();
    const res = await fetch(`${url}multiple-sessions`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ doId }),
    });

    const json = (await res.json()) as unknown;
    if (
      !(
        typeof json === "object" &&
        json !== null &&
        "messageRes1" in json &&
        "map2Hello" in json &&
        "map3Hello" in json
      )
    ) {
      console.log(json);
      throw new Error("Invalid response");
    }

    expect(json.map2Hello).toBe("world");
    expect(json.map3Hello).toBe("world2");
    expect(json.messageRes1).toMatchInlineSnapshot(`
      [
        "client -> LOAD Map sessions: empty",
        "storage -> CONTENT Group header: true new: After: 0 New: 4",
        "storage -> CONTENT Map header: true new: After: 0 New: 1 | After: 0 New: 1",
      ]
    `);
  });

  test("large coValue upload streaming", async () => {
    const doId = randomUUID();
    const res = await fetch(`${url}large-covalue-upload`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ doId }),
    });

    const json = (await res.json()) as unknown;
    if (!(typeof json === "object" && json !== null && "messageRes1" in json)) {
      console.log(json);
      throw new Error("Invalid response");
    }

    expect(json.messageRes1).toMatchInlineSnapshot(`
      [
        "client -> LOAD Map sessions: empty",
        "storage -> CONTENT Group header: true new: After: 0 New: 4",
        "storage -> CONTENT Map header: true new: After: 0 New: 97",
        "storage -> CONTENT Map header: true new: After: 97 New: 97",
        "storage -> CONTENT Map header: true new: After: 194 New: 6",
      ]
    `);
  });
});
