import { createHash } from "node:crypto";
import { describe, expect, it } from "vitest";
import { appId } from "./app-id.js";
import { formatObjectId, objectIdKey } from "./object-id.js";
import { stableObjectId as serverObjectId } from "./server/projection.js";
import { stableObjectId as browserObjectId } from "./src/object-id.js";

describe("deterministic Jazz object IDs", () => {
  it("keeps the client and server key contract stable", () => {
    const key = objectIdKey("app-v1", "bluesky-profile", "did:plc:alice");
    const digest = createHash("sha256").update(key).digest();
    expect(key).toBe("app-v1:projection-v3:bluesky-profile:did:plc:alice");
    expect(formatObjectId(digest)).toBe("1ac25034-8fae-578d-8de3-dbc5cfebb05b");
  });

  it("uses one application ID in the browser and BFF", async () => {
    const namespace = "bluesky-profile";
    const value = "did:plc:alice";

    expect(await browserObjectId(namespace, value)).toBe(serverObjectId(namespace, value));
    expect(objectIdKey(appId, namespace, value)).toContain(appId);
  });
});
