import { describe, expect, it } from "vitest";
import { profileNameParts } from "../../src/ProfileName.js";

describe("profile names", () => {
  it("shows a display name and an explicit @handle when both are available", () => {
    expect(profileNameParts({
      displayName: "Joe Innes",
      handle: "joeinn.es",
    }, "did:plc:joe")).toEqual({
      name: "Joe Innes",
      handle: "@joeinn.es",
    });
  });

  it("falls back without inventing profile data", () => {
    expect(profileNameParts({ handle: "joeinn.es" }, "did:plc:joe"))
      .toEqual({ name: "@joeinn.es" });
    expect(profileNameParts({ displayName: "Joe Innes" }, "did:plc:joe"))
      .toEqual({ name: "Joe Innes" });
    expect(profileNameParts(undefined, "did:plc:joe"))
      .toEqual({ name: "did:plc:joe" });
  });
});
