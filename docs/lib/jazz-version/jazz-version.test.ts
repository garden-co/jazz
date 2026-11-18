import { describe, expect, it } from "vitest";
import { jazzVersion, replaceVersionTokens } from "./index";
import { remarkJazzVersion } from "./remark";

describe("jazz version tokens", () => {
  it("reads the jazz-tools version", () => {
    expect(jazzVersion()).toMatch(/^\d+\.\d+\.\d+/);
  });

  it("fills text and link URLs", () => {
    expect(replaceVersionTokens("v%jazz.version%", "2.0.0-alpha.62")).toBe("v2.0.0-alpha.62");
    const tree = {
      type: "root",
      children: [
        {
          type: "link",
          url: "https://x/tree/v%jazz.version%/a",
          children: [{ type: "text", value: "%jazz.version%" }],
        },
      ],
    };
    remarkJazzVersion()(tree);
    expect(JSON.stringify(tree)).not.toContain("%jazz.");
    expect(JSON.stringify(tree)).toContain(`tree/v${jazzVersion()}/a`);
  });

  it("fails on an unknown token", () => {
    expect(() => replaceVersionTokens("%jazz.verison%", "1")).toThrow(
      /Unknown token %jazz\.verison%/,
    );
  });
});
