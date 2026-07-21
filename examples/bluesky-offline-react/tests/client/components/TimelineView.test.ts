import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import { Intro } from "../../../src/components/TimelineView.js";

describe("timeline introduction", () => {
  it("is an expanded disclosure that can be collapsed", () => {
    const html = renderToStaticMarkup(createElement(Intro));

    expect(html).toContain("<details");
    expect(html).toContain("<summary");
    expect(html).toContain("open=\"\"");
    expect(html).toContain("Why Jazz?");
    expect(html).toContain("Your Bluesky timeline, available offline.");
  });
});
