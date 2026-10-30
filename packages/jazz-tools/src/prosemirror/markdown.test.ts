import { describe, expect, it } from "vitest";
import { defaultMarkdownParser, schema } from "prosemirror-markdown";
import type { Node } from "prosemirror-model";
import { toMarkdown } from "./markdown.js";

const parse = (markdown: string) => defaultMarkdownParser.parse(markdown);

/** Canonical markdown: writing it parsed gives it back. */
const CANONICAL = [
  "Hello world",
  "# Tour notes\n\nDay one went well.",
  "Some *emphasis*, **strong** and `code` text.",
  'A [link](https://jazz.tools) and a [titled one](https://jazz.tools "Jazz").',
  "> Quoted\n>\n> Two paragraphs",
  "- one\n- two\n- three",
  "1. first\n2. second",
  "3. starts at three\n4. next",
  "- loose\n\n- items",
  "- item\n  - nested\n  - list\n- back",
  "- item with\n\n  two paragraphs\n\n- next",
  "```js\nconst x = 1;\n```",
  "```\n```",
  "Line one\\\nline two",
  "![a picture](https://example.com/a.png)",
  "---",
  "Escapes: \\* \\_ \\[x\\] \\` \\\\",
  "\\# not a heading",
  "1\\. not a list",
  "\\- not a list",
  "**bold *and em* inside**",
  "Code with a backtick: ``a`b``",
  "`` `tick ``",
  "Emoji 🎸 and accents: Café Olé",
  "> - quoted\n> - list",
];

/** Every text unit's document position and its unit. */
function units(doc: Node): { pos: number; unit: string }[] {
  const found: { pos: number; unit: string }[] = [];
  doc.descendants((node, pos) => {
    if (node.isText) {
      for (let i = 0; i < node.text!.length; i++)
        found.push({ pos: pos + i, unit: node.text![i]! });
    }
  });
  return found;
}

describe("toMarkdown", () => {
  it.each(CANONICAL)("writes canonical markdown back unchanged: %j", (markdown) => {
    expect(toMarkdown(parse(markdown)).text).toBe(markdown);
  });

  it.each(CANONICAL)("parses what it writes into the same document: %j", (markdown) => {
    const doc = parse(markdown);
    expect(parse(toMarkdown(doc).text).eq(doc)).toBe(true);
  });

  it.each(CANONICAL)("maps every text unit onto itself: %j", (markdown) => {
    const doc = parse(markdown);
    const mapped = toMarkdown(doc);
    for (const { pos, unit } of units(doc)) {
      const offset = mapped.toText(pos, "after");
      const at = mapped.text[offset] === unit ? offset : offset + 1;
      expect(mapped.text[at], `unit ${JSON.stringify(unit)} at ${pos}`).toBe(unit);
      expect(mapped.toDoc(offset, "after")).toBe(pos);
    }
  });

  it("moves whitespace at a mark's edge outside it", () => {
    const doc = schema.node("doc", null, [
      schema.node("paragraph", null, [
        schema.text("a"),
        schema.text(" bold ", [schema.marks.strong.create()]),
        schema.text("b"),
      ]),
    ]);
    const { text } = toMarkdown(doc);
    expect(text).toBe("a **bold** b");
  });

  it("maps a cursor at a paragraph's end and the next one's start apart", () => {
    const doc = parse("First\n\nSecond");
    const mapped = toMarkdown(doc);
    // "First" spans 1..6; "Second" starts at 8.
    expect(mapped.toText(6)).toBe(5);
    expect(mapped.toText(8)).toBe(7);
    // The blank line between them leans to either paragraph.
    expect(mapped.toDoc(6, "before")).toBe(6);
    expect(mapped.toDoc(6, "after")).toBe(8);
  });

  it("maps positions inside syntax to the content beside it", () => {
    const doc = parse("Some **bold** text");
    const mapped = toMarkdown(doc);
    // Offsets 5 and 6 are the opening "**".
    expect(mapped.toDoc(5, "before")).toBe(6);
    expect(mapped.toDoc(6, "after")).toBe(6);
    expect(mapped.toText(6, "before")).toBe(5);
    expect(mapped.toText(6, "after")).toBe(7);
  });

  it("maps an empty document", () => {
    const doc = schema.node("doc", null, [schema.node("paragraph")]);
    const mapped = toMarkdown(doc);
    expect(mapped.text).toBe("");
    expect(mapped.toText(1)).toBe(0);
    expect(mapped.toDoc(0)).toBe(1);
  });
});

/** A small seeded generator, so failures replay. */
function random(seed: number) {
  let state = seed >>> 0 || 1;
  const next = () => {
    state ^= state << 13;
    state ^= state >>> 17;
    state ^= state << 5;
    return (state >>> 0) / 2 ** 32;
  };
  return {
    int: (n: number) => Math.floor(next() * n),
    pick: <T>(items: readonly T[]) => items[Math.floor(next() * items.length)]!,
    chance: (p: number) => next() < p,
  };
}

const PIECES = [
  "word",
  "Jazz",
  "1.",
  "2)",
  "#",
  ">",
  "-",
  "+",
  "=",
  "*",
  "_",
  "`",
  "``",
  "[",
  "]",
  "\\",
  "<a",
  "&amp;",
  "!",
  "~~~",
  " ",
  "  ",
  "🎸",
  "é",
  "(x)",
  "http://x.y",
  "a_b",
  "**",
];

function randomInline(r: ReturnType<typeof random>, breaks = true) {
  const marks = [schema.marks.em, schema.marks.strong, schema.marks.code, schema.marks.link];
  const nodes: Node[] = [];
  for (let i = r.int(5) + 1; i > 0; i--) {
    if (breaks && r.chance(0.08)) {
      nodes.push(schema.nodes.hard_break.create());
      continue;
    }
    let text = "";
    for (let j = r.int(4) + 1; j > 0; j--) text += r.pick(PIECES) + (r.chance(0.5) ? " " : "");
    const chosen = marks
      .filter(() => r.chance(0.25))
      .map((type) =>
        type === schema.marks.link ? type.create({ href: "https://jazz.tools" }) : type.create(),
      );
    nodes.push(schema.text(text, chosen));
  }
  return nodes;
}

function randomBlocks(r: ReturnType<typeof random>, depth: number): Node[] {
  const blocks: Node[] = [];
  for (let i = r.int(3) + 1; i > 0; i--) {
    const kind = depth > 1 ? 0 : r.int(7);
    if (kind <= 1) blocks.push(schema.node("paragraph", null, randomInline(r)));
    else if (kind === 2)
      blocks.push(schema.node("heading", { level: r.int(3) + 1 }, randomInline(r, false)));
    else if (kind === 3) blocks.push(schema.node("blockquote", null, randomBlocks(r, depth + 1)));
    else if (kind === 4) {
      blocks.push(
        schema.node("code_block", { params: r.pick(["", "js"]) }, [
          schema.text("let x = `y`;\n```\nz"),
        ]),
      );
    } else {
      const type = r.chance(0.5) ? "bullet_list" : "ordered_list";
      const items = Array.from({ length: r.int(3) + 1 }, () =>
        schema.node("list_item", null, [
          schema.node("paragraph", null, randomInline(r)),
          ...(r.chance(0.3) ? randomBlocks(r, depth + 1) : []),
        ]),
      );
      blocks.push(schema.node(type, { tight: r.chance(0.5) }, items));
    }
  }
  return blocks;
}

describe("toMarkdown on random documents", () => {
  const seeds = Array.from({ length: 300 }, (_, i) => i + 1);

  it.each(seeds)("reaches a canonical form in one round trip and maps it, seed %i", (seed) => {
    // A random document may hold what markdown cannot say, such as
    // trailing spaces; once parsed, writing and parsing changes nothing.
    const doc = schema.node("doc", null, randomBlocks(random(seed), 0));
    const parsed = parse(toMarkdown(doc).text);
    const canonical = toMarkdown(parsed);
    expect(toMarkdown(parse(canonical.text)).text).toBe(canonical.text);
    expect(parse(canonical.text).eq(parsed)).toBe(true);
    for (const { pos, unit } of units(parsed)) {
      const offset = canonical.toText(pos, "after");
      const at = canonical.text[offset] === unit ? offset : offset + 1;
      expect(canonical.text[at], `unit ${JSON.stringify(unit)} at ${pos}`).toBe(unit);
      expect(canonical.toDoc(offset, "after")).toBe(pos);
    }
  });
});
