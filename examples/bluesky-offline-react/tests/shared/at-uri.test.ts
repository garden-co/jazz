import { describe, expect, it } from "vitest";
import { parseAtUri } from "../../shared/at-uri.js";

describe("AT URIs", () => {
  it("extracts repository, collection, and record key", () => {
    expect(parseAtUri("at://did:plc:alice/app.bsky.feed.like/3mlike")).toEqual({
      repository: "did:plc:alice",
      collection: "app.bsky.feed.like",
      recordKey: "3mlike",
    });
    expect(parseAtUri("https://example.test")).toBeUndefined();
  });
});
