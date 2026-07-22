import { createHash } from "node:crypto";
import { describe, expect, it } from "vitest";
import { formatObjectId, objectIdKey, parseAtRecordUri } from "../../shared/identifiers.js";
import { stableObjectId as serverObjectId } from "../../server/projection.js";
import { stableObjectId as browserObjectId } from "../../src/model/object-id.js";

describe("shared identifiers", () => {
  it("adapts ATProto record URIs to PDS identifiers", () => {
    expect(parseAtRecordUri("at://did:plc:alice/app.bsky.feed.like/3mlike")).toEqual({
      repo: "did:plc:alice",
      collection: "app.bsky.feed.like",
      rkey: "3mlike",
    });
    expect(parseAtRecordUri("at://did:plc:alice/app.bsky.feed.like")).toBeUndefined();
    expect(parseAtRecordUri("https://example.test")).toBeUndefined();
  });

  it("keeps the projection namespace and object ID format stable", () => {
    const key = objectIdKey("app-v1", "bluesky-profile", "did:plc:alice");
    const digest = createHash("sha256").update(key).digest();

    expect(key).toBe("app-v1:projection-v3:bluesky-profile:did:plc:alice");
    expect(formatObjectId(digest)).toBe("1ac25034-8fae-578d-8de3-dbc5cfebb05b");
  });

  it("produces the same object ID in the browser and BFF", async () => {
    const namespace = "bluesky-profile";
    const value = "did:plc:alice";

    expect(await browserObjectId(namespace, value)).toBe(serverObjectId(namespace, value));
  });
});
