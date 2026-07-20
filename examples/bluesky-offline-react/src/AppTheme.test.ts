import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import { AppTheme } from "./AppTheme.js";

describe("AppTheme", () => {
  it("provides the shared visual theme without hiding application content", () => {
    const html = renderToStaticMarkup(
      createElement(AppTheme, null, createElement("button", { type: "button" }, "Post")),
    );

    expect(html).toContain("Post");
  });
});
