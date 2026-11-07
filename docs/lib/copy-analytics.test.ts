import { describe, expect, it } from "vitest";
import { COPY_EVENT, copyEvent, copyWhat } from "./copy-analytics";

describe("copyWhat", () => {
  it("counts a near-complete quickstart selection as the whole quickstart", () => {
    expect(copyWhat({ inQuickstart: true, inCode: true }, 980, 1000)).toBe("quickstart");
    expect(copyWhat({ inQuickstart: true, inCode: true }, 200, 1000)).toBe("quickstart-partial");
  });

  it("tells code from other text outside the quickstart", () => {
    expect(copyWhat({ inQuickstart: false, inCode: true }, 40, 1000)).toBe("code");
    expect(copyWhat({ inQuickstart: false, inCode: false }, 40, 1000)).toBe("text");
  });
});

describe("copyEvent", () => {
  it("carries what was copied and how, within Vercel's two properties", () => {
    expect(copyEvent("quickstart", "button")).toEqual({
      name: COPY_EVENT,
      properties: { what: "quickstart", method: "button" },
    });
  });
});
