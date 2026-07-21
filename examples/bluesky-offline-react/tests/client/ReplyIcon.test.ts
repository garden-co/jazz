import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import { ReplyIcon } from "../../src/ReplyIcon.js";

describe("ReplyIcon", () => {
  it("renders a decorative vector icon instead of a text glyph", () => {
    const html = renderToStaticMarkup(createElement(ReplyIcon));

    expect(html).toContain("<svg");
    expect(html).toContain('aria-hidden="true"');
    expect(html).not.toContain("↩");
  });
});
