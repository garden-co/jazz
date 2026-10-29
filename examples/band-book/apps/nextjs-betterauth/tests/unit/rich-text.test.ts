import { describe, expect, it } from "vitest";
import {
  docToMarkdown,
  markdownToDoc,
  markdownToPlainText,
  rebaseText,
} from "../../src/lib/rich-text";
import { DEMO_PAGES, type SeedBlock, type SeedPage } from "../../src/lib/seed";

const roundTrip = (text: string) => docToMarkdown(markdownToDoc(text));

describe("block Markdown", () => {
  // Every open editor saves what it loaded, so a value must survive a load and
  // save without edits unchanged. Otherwise editors would keep rewriting text
  // nobody touched, and those rewrites would compete with real edits.
  const canonical = [
    "",
    "Key of D, 92 bpm. **Capo 2** on the acoustic.",
    "**Bold**, *italic*, ~~struck~~ and `code`",
    "***both*** and **bold *nested* bold**",
    "[the venue](https://example.com/venue) at 20:00",
    "Salt on the window, the ferry's late again\nWe count the harbour lights like they were friends",
    "  leading and trailing spaces ",
    "emoji 🎸 and accents: café",
    "50% \\* 3 and snake\\_case",
    "\\# not a heading",
    "\\- not a list item",
    "1\\. not a numbered item",
    "a line\n&gt; not a quote",
    "\\---",
  ];
  it.each(canonical)("keeps %j unchanged through load and save", (text) => {
    expect(roundTrip(text)).toBe(text);
  });

  it("keeps the seeded band's text unchanged through load and save", () => {
    const texts: string[] = [];
    const blocks = (list: SeedBlock[]): void =>
      list.forEach((block) => {
        if (block.text) texts.push(block.text);
        blocks(block.children ?? []);
      });
    const pages = (list: SeedPage[]): void =>
      list.forEach((page) => {
        blocks(page.blocks ?? []);
        pages(page.children ?? []);
      });
    pages(DEMO_PAGES);
    expect(texts.length).toBeGreaterThan(10);
    for (const text of texts) expect(roundTrip(text)).toBe(text);
  });

  // Text from before blocks had formatting is plain text. Loading keeps every
  // character, and the first save settles on a form that is stable from then on.
  it.each([
    ["1. Intro", "1\\. Intro"],
    ["# chords", "\\# chords"],
    ["- bass", "\\- bass"],
    ["C# and 50% * 3", "C# and 50% \\* 3"],
    ["> quote", "&gt; quote"],
    ["x <b>tag</b>", "x &lt;b&gt;tag&lt;/b&gt;"],
  ])("loads plain %j as text and saves it as %j", (plain, saved) => {
    expect(markdownToDoc(plain).content?.[0]?.content?.[0]?.text).toBe(plain);
    expect(roundTrip(plain)).toBe(saved);
    expect(roundTrip(saved)).toBe(saved);
  });

  // Markdown this schema has no node for (indented code, hard breaks) would
  // otherwise vanish with its text; loading keeps the text and the line break.
  it.each([
    ["    indented", "indented"],
    ["\ttabbed", "tabbed"],
    ["first\n\n    after a blank line", "first\n\nafter a blank line"],
    ["a line\n    continued", "a line\n    continued"],
    ["hard  \nbreak", "hard\nbreak"],
    ["hard\\\nbreak", "hard\nbreak"],
    ["an escaped backslash\\\\\nstays", "an escaped backslash\\\\\nstays"],
  ])("keeps the text of %j, saved as %j", (stored, saved) => {
    expect(roundTrip(stored)).toBe(saved);
    expect(roundTrip(saved)).toBe(saved);
  });

  // Some Markdown has more than one spelling. Loading keeps the text and the
  // formatting, and the first save picks one spelling, stable from then on.
  it.each([
    ["_x_", "*x*"],
    ["__x__", "**x**"],
    ["see https://example.com now", "see [https://example.com](https://example.com) now"],
    ["x &copy; y", "x &amp;copy; y"],
    ["a\n\n\nb", "a\n\nb"],
    ["\nleading and trailing line breaks\n", "leading and trailing line breaks"],
  ])("settles %j as %j", (stored, saved) => {
    expect(roundTrip(stored)).toBe(saved);
    expect(roundTrip(saved)).toBe(saved);
  });

  it("gives the text without its formatting for labels", () => {
    expect(markdownToPlainText("Bring **snare** and [kick](https://example.com)")).toBe(
      "Bring snare and kick",
    );
    expect(markdownToPlainText("")).toBe("");
  });
});

describe("rebaseText", () => {
  const base = "Hold on, the tide will turn";
  it("keeps both edits when they touch different words", () => {
    expect(
      rebaseText(
        base,
        "Hold on, the tide will turn around",
        "Hold on, hold on, the tide will turn",
      ),
    ).toBe("Hold on, hold on, the tide will turn around");
    expect(
      rebaseText(
        base,
        "Hold on, hold on, the tide will turn",
        "Hold on, the tide will turn around",
      ),
    ).toBe("Hold on, hold on, the tide will turn around");
  });
  it("puts the remote insertion first when both insert at the same spot", () => {
    expect(rebaseText("ab", "aLb", "aRb")).toBe("aRLb");
  });
  it("lets the local edit win the span both replaced", () => {
    expect(rebaseText(base, "Hold on, the moon will turn", "Hold on, the sea will turn")).toBe(
      "Hold on, the moon will turn",
    );
  });
  it("returns either side when the other has no edits", () => {
    expect(rebaseText(base, base, "changed")).toBe("changed");
    expect(rebaseText(base, "changed", base)).toBe("changed");
  });
});
