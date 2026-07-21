import { describe, expect, it } from "vitest";
import { parseAtRecordUri } from "../../shared/at-uri.js";

describe("ATProto record URIs", () => {
  it("adapts a record URI to the identifiers used for PDS writes", () => {
    expect(parseAtRecordUri("at://did:plc:alice/app.bsky.feed.like/3mlike")).toEqual({
      repo: "did:plc:alice",
      collection: "app.bsky.feed.like",
      rkey: "3mlike",
    });
    expect(parseAtRecordUri("at://did:plc:alice/app.bsky.feed.like")).toBeUndefined();
    expect(parseAtRecordUri("https://example.test")).toBeUndefined();
  });
});
