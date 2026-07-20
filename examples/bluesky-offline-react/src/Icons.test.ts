import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import {
  BackIcon,
  DisclosureIcon,
  LikeIcon,
  RepostIcon,
  SuccessIcon,
  ThreadLinkIcon,
} from "./Icons.js";

describe("interface icons", () => {
  it("renders decorative vector icons instead of Unicode symbols", () => {
    const icons = [
      createElement(BackIcon),
      createElement(DisclosureIcon),
      createElement(LikeIcon, { active: false }),
      createElement(LikeIcon, { active: true }),
      createElement(RepostIcon),
      createElement(SuccessIcon),
      createElement(ThreadLinkIcon),
    ];

    for (const icon of icons) {
      const html = renderToStaticMarkup(icon);
      expect(html).toContain("<svg");
      expect(html).toContain('aria-hidden="true"');
      expect(html).not.toMatch(/[↻♡♥✓›←↗]/);
    }
  });
});
