import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import { normalizePost } from "../server/timeline.js";
import { PostText } from "./TimelineView.js";

describe("post text", () => {
  it("uses ATProto link facets for shortened display URLs", () => {
    const text = "ah here we go again news.ycombinator.com/item?id=4892...";
    const label = "news.ycombinator.com/item?id=4892...";
    const byteStart = new TextEncoder().encode(text.slice(0, text.indexOf(label))).length;
    const byteEnd = byteStart + new TextEncoder().encode(label).length;
    const normalized = normalizePost({
      uri: "at://did:plc:author/app.bsky.feed.post/3m12345678921",
      cid: "bafypost",
      author: { did: "did:plc:author", handle: "author.test" },
      record: {
        text,
        createdAt: "2026-07-16T10:00:00.000Z",
        facets: [{
          index: { byteStart, byteEnd },
          features: [{
            $type: "app.bsky.richtext.facet#link",
            uri: "https://news.ycombinator.com/item?id=48921234",
          }],
        }],
      },
      indexedAt: "2026-07-16T10:00:01.000Z",
    });

    const html = renderToStaticMarkup(
      createElement(PostText, {
        text: normalized!.post.text,
        facetsJson: normalized!.post.facetsJson,
      }),
    );

    expect(html).toContain('href="https://news.ycombinator.com/item?id=48921234"');
    expect(html).toContain(label);
  });
});
