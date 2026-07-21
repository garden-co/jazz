import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import { Intro } from "../../../src/components/TimelineView.js";

describe("timeline introduction", () => {
  it("uses a default-open accordion with a concise summary", () => {
    const html = renderToStaticMarkup(createElement(Intro));
    const trigger = html.match(/<button[^>]*aria-expanded="true"[^>]*>(.*?)<\/button>/)?.[1];

    expect(trigger).toBeDefined();
    expect(trigger ?? "").toContain("Why Jazz?");
    expect(trigger ?? "").not.toContain("Your Bluesky timeline, available offline.");
    expect(html).toContain('data-state="open"');
    expect(html).toContain("Your Bluesky timeline, available offline.");
  });
});
