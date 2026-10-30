import type { Mark, Node } from "prosemirror-model";

/**
 * Which way a position leans when it sits where two places map to one:
 * `"before"` sticks to what precedes it, `"after"` to what follows (like
 * `Side` in jazz-text anchors).
 */
export type Side = "before" | "after";

/**
 * A document's canonical markdown, with a map between ProseMirror
 * positions and offsets into the markdown. Both count UTF-16 code units,
 * like JavaScript strings, ProseMirror and jazz-text anchors.
 */
export interface MappedMarkdown {
  readonly text: string;
  /** The markdown offset of a document position. */
  toText(pos: number, side?: Side): number;
  /** The document position of a markdown offset. */
  toDoc(offset: number, side?: Side): number;
}

/** How nodes and marks are written, for schemas beyond the CommonMark set. */
export interface MarkdownSpec {
  nodes?: Record<string, NodeWriter>;
  marks?: Record<string, MarkWriter>;
}

/** Writes `node`, which starts at `pos` in the document. */
export type NodeWriter = (out: MarkdownWriter, node: Node, pos: number) => void;

export interface MarkWriter {
  open(mark: Mark): string;
  close(mark: Mark): string;
}

/**
 * Writes markdown and records where document positions land in it. Node
 * writers call {@link blocks}, {@link inline}, {@link write} and
 * {@link point}; prefixes such as `"> "` apply to each line started while
 * they are pushed.
 */
export class MarkdownWriter {
  private out = "";
  // Points where a document position meets a markdown offset, in writing
  // order (both never decrease). `linear[i]` says positions between point
  // i and i + 1 map unit for unit.
  private positions: number[] = [];
  private offsets: number[] = [];
  private linear: boolean[] = [];
  private prefixes: string[] = [];
  private lineStart = true;
  // Content written on the current line, and whether inline syntax came
  // first; together they say whether `#` or `1.` would start a block.
  private lineText = "";
  private inlineSyntax = false;
  private separator: "" | "\n" | "\n\n" = "";
  // Newlines since the last line with something on it.
  private newlines = 0;
  // Right after a list marker, whose first block shares its line.
  private fresh = false;

  constructor(private readonly spec: Required<MarkdownSpec>) {}

  /** Writes block syntax, such as `"# "` or a code fence. */
  write(syntax: string): void {
    syntax.split("\n").forEach((line, i) => {
      if (i > 0) this.newline();
      if (line) {
        this.startLine();
        this.out += line;
      }
    });
  }

  newline(): void {
    if (this.lineStart) this.out += this.prefixes.join("").trimEnd();
    this.out += "\n";
    this.newlines++;
    this.lineStart = true;
    this.lineText = "";
    this.inlineSyntax = false;
  }

  /** Runs `body` with `prefix` before each line it starts. `first`, if
   * given, is written now in its place, as list markers are. */
  withPrefix(prefix: string, body: () => void, first?: string): void {
    if (first !== undefined) {
      this.flushSeparator();
      this.startLine();
      this.out += first;
      this.fresh = true;
    }
    this.prefixes.push(prefix);
    try {
      body();
    } finally {
      this.prefixes.pop();
    }
  }

  /** Records that `pos` maps to the current end of the markdown. */
  point(pos: number): void {
    this.flushSeparator();
    this.startLine();
    this.mark(pos, this.out.length, false);
  }

  /** Writes `text`, which starts at `pos` in the document. Unless `raw`,
   * characters markdown would read as syntax are escaped. */
  content(text: string, pos: number, raw = false): void {
    for (let i = 0; i < text.length; i++) {
      const unit = text[i]!;
      if (unit === "\n") {
        this.mark(pos + i, this.out.length, false);
        this.newline();
        this.startLine();
        this.mark(pos + i + 1, this.out.length, false);
        continue;
      }
      this.startLine();
      const escape = !raw && this.needsEscape(text, i);
      this.mark(pos + i, this.out.length, true);
      if (escape) this.out += "\\";
      this.out += unit;
      this.lineText += unit;
      this.mark(pos + i + 1, this.out.length, !escape);
    }
  }

  /** Asks for at least `separator` before the next block. */
  separate(separator: "\n" | "\n\n"): void {
    if (this.out.length === 0 || this.fresh) return;
    if (separator.length > this.separator.length) this.separator = separator;
  }

  /** Writes the child blocks of `parent`, whose content starts at `pos`. */
  blocks(parent: Node, pos: number, separator: "\n" | "\n\n" = "\n\n"): void {
    parent.forEach((child, offset) => {
      this.separate(separator);
      this.block(child, pos + offset);
    });
  }

  block(node: Node, pos: number): void {
    const writer = this.spec.nodes[node.type.name];
    if (!writer) throw new Error(`No markdown writer for node type "${node.type.name}"`);
    this.flushSeparator();
    this.fresh = false;
    writer(this, node, pos);
  }

  /** Writes the inline content of `parent`, whose content starts at `pos`.
   * Whitespace at the edge of a mark moves outside it, where CommonMark
   * still reads the mark. */
  inline(parent: Node, pos: number): void {
    this.point(pos);
    let open: readonly Mark[] = [];
    const children: { node: Node; pos: number }[] = [];
    parent.forEach((node, offset) => children.push({ node, pos: pos + offset }));
    children.forEach(({ node, pos: at }, index) => {
      // Marks already open stay open, so they come first.
      const own = outerMarks(node);
      const keep = openPrefix(open, own);
      const marks = [
        ...open.slice(0, keep),
        ...own.filter((mark) => !mark.isInSet(open.slice(0, keep))),
      ];
      const next = children[index + 1] ? outerMarks(children[index + 1]!.node) : [];
      const stays = openPrefix(marks, next);
      const text = node.isText ? node.text! : "";
      let lead = 0;
      let trail = text.length;
      if (marks.length > keep) while (lead < trail && isSpace(text[lead]!)) lead++;
      if (stays < marks.length) while (trail > lead && isSpace(text[trail - 1]!)) trail--;
      open = this.closeMarks(open, keep);
      if (node.isText && lead === trail) {
        // Only whitespace where marks open or close: written unmarked.
        this.content(text, at);
        return;
      }
      this.content(text.slice(0, lead), at);
      for (const mark of marks.slice(keep)) this.syntax(this.markWriter(mark).open(mark));
      open = marks;
      if (!node.isText) this.block(node, at);
      else if (node.marks.some((mark) => mark.type.name === "code")) {
        this.codeSpan(text.slice(lead, trail), at + lead);
      } else this.content(text.slice(lead, trail), at + lead);
      open = this.closeMarks(open, stays);
      this.content(text.slice(trail), at + trail);
    });
    this.closeMarks(open, 0);
    this.point(pos + parent.content.size);
  }

  /** The markdown written so far and its map. */
  finish(): MappedMarkdown {
    return new Mapped(this.out, this.positions, this.offsets, this.linear);
  }

  /** Writes `text` as a code span, fenced by enough backticks. */
  private codeSpan(text: string, pos: number): void {
    const longest = Math.max(0, ...(text.match(/`+/g) ?? []).map((run) => run.length));
    const fence = "`".repeat(longest + 1);
    const pad = /^`|`$|^ .* $/.test(text) ? " " : "";
    this.syntax(fence + pad);
    this.content(text, pos, true);
    this.syntax(pad + fence);
  }

  private syntax(text: string): void {
    this.startLine();
    this.out += text;
    this.inlineSyntax = true;
  }

  private closeMarks(open: readonly Mark[], keep: number): readonly Mark[] {
    for (let i = open.length - 1; i >= keep; i--) {
      this.syntax(this.markWriter(open[i]!).close(open[i]!));
    }
    return open.slice(0, keep);
  }

  private markWriter(mark: Mark): MarkWriter {
    const writer = this.spec.marks[mark.type.name];
    if (!writer) throw new Error(`No markdown writer for mark type "${mark.type.name}"`);
    return writer;
  }

  private needsEscape(text: string, i: number): boolean {
    const unit = text[i]!;
    if (ALWAYS_ESCAPED.has(unit)) return true;
    if (unit === "<") return /[A-Za-z/!?]/.test(text[i + 1] ?? "");
    if (unit === "&") return ENTITY.test(text.slice(i));
    if (this.inlineSyntax) return false;
    if (this.lineText === "") return BLOCK_STARTS.has(unit);
    return (unit === "." || unit === ")") && /^\d{1,9}$/.test(this.lineText);
  }

  private flushSeparator(): void {
    const separator = this.separator;
    this.separator = "";
    for (let i = this.newlines; i < separator.length; i++) this.newline();
  }

  private startLine(): void {
    if (!this.lineStart) return;
    this.lineStart = false;
    this.newlines = 0;
    this.out += this.prefixes.join("");
  }

  /** Adds the point (`pos`, `offset`), `linear` from the last one. */
  private mark(pos: number, offset: number, linear: boolean): void {
    const n = this.positions.length;
    if (n > 0 && this.positions[n - 1] === pos && this.offsets[n - 1] === offset) return;
    linear &&= n > 0 && pos - this.positions[n - 1]! === offset - this.offsets[n - 1]!;
    if (linear && n > 1 && this.linear[n - 2]) {
      // Extend the run instead of adding a point inside it.
      this.positions[n - 1] = pos;
      this.offsets[n - 1] = offset;
      return;
    }
    if (n > 0) this.linear[n - 1] = linear;
    this.positions.push(pos);
    this.offsets.push(offset);
    this.linear.push(false);
  }
}

const ALWAYS_ESCAPED = new Set(["\\", "`", "*", "_", "[", "]"]);
const BLOCK_STARTS = new Set(["#", ">", "-", "+", "=", "~"]);
const ENTITY = /^&(?:#\d+|#[xX][0-9a-fA-F]+|[A-Za-z][A-Za-z0-9]*);/;

function outerMarks(node: Node): readonly Mark[] {
  return node.marks.filter((mark) => mark.type.name !== "code");
}

/** How many of the `open` marks, outermost first, `marks` all have. */
function openPrefix(open: readonly Mark[], marks: readonly Mark[]): number {
  let n = 0;
  while (n < open.length && open[n]!.isInSet(marks)) n++;
  return n;
}

function isSpace(unit: string): boolean {
  return unit === " " || unit === "\t" || unit === "\u00a0";
}

class Mapped implements MappedMarkdown {
  constructor(
    readonly text: string,
    private readonly positions: readonly number[],
    private readonly offsets: readonly number[],
    private readonly linear: readonly boolean[],
  ) {}

  toText(pos: number, side: Side = "after"): number {
    return this.lookup(this.positions, this.offsets, pos, side);
  }

  toDoc(offset: number, side: Side = "after"): number {
    return this.lookup(this.offsets, this.positions, offset, side);
  }

  /** Maps `at` from `from` to `to`, both non-decreasing. Exact at points
   * and inside linear runs; elsewhere `at` snaps to the neighbour `side`
   * names. */
  private lookup(from: readonly number[], to: readonly number[], at: number, side: Side): number {
    const n = from.length;
    if (n === 0) return 0;
    // First index with from[i] >= at.
    let lo = 0;
    let hi = n;
    while (lo < hi) {
      const mid = (lo + hi) >> 1;
      if (from[mid]! < at) lo = mid + 1;
      else hi = mid;
    }
    if (lo < n && from[lo] === at) {
      if (side === "before") return to[lo]!;
      let last = lo;
      while (last + 1 < n && from[last + 1] === at) last++;
      return to[last]!;
    }
    const previous = lo - 1;
    if (previous < 0) return to[0]!;
    if (lo >= n) return to[n - 1]!;
    if (this.linear[previous]) return to[previous]! + (at - from[previous]!);
    return side === "before" ? to[previous]! : to[lo]!;
  }
}

function escapeTitle(title: string): string {
  return title.replace(/["\\]/g, "\\$&");
}

function destination(url: string): string {
  return /[\s()<>]/.test(url) || url === "" ? `<${url.replace(/[<>]/g, encodeURI)}>` : url;
}

function writeList(out: MarkdownWriter, list: Node, pos: number, marker: (i: number) => string) {
  // Two textblocks in one item would run together in a tight list.
  let loose = list.attrs.tight === false;
  list.forEach((item) => {
    let textblocks = 0;
    item.forEach((child) => (textblocks += child.isTextblock ? 1 : 0));
    loose ||= textblocks > 1;
  });
  const separator = loose ? "\n\n" : "\n";
  list.forEach((item, offset, i) => {
    out.separate(separator);
    const first = marker(i);
    out.withPrefix(
      " ".repeat(first.length),
      () => {
        if (item.childCount === 0) out.point(pos + offset + 1);
        else out.blocks(item, pos + offset + 1, separator);
      },
      first,
    );
  });
}

/** Writers for the CommonMark nodes and marks of `prosemirror-markdown`'s
 * schema, whose names `prosemirror-schema-basic` and `-list` share. */
export const commonMarkSpec: Required<MarkdownSpec> = {
  nodes: {
    doc: (out, node) => out.blocks(node, 0),
    paragraph: (out, node, pos) => out.inline(node, pos + 1),
    heading: (out, node, pos) => {
      out.write("#".repeat(node.attrs.level ?? 1) + " ");
      out.inline(node, pos + 1);
    },
    blockquote: (out, node, pos) => out.withPrefix("> ", () => out.blocks(node, pos + 1)),
    code_block: (out, node, pos) => {
      const text = node.textContent;
      const longest = Math.max(0, ...(text.match(/^`+/gm) ?? []).map((run) => run.length));
      const fence = "`".repeat(Math.max(3, longest + 1));
      out.write(fence + (node.attrs.params ?? ""));
      out.newline();
      out.point(pos + 1);
      out.content(text, pos + 1, true);
      if (text.length > 0) out.newline();
      out.write(fence);
    },
    horizontal_rule: (out, _node, pos) => {
      out.point(pos);
      out.write("---");
      out.point(pos + 1);
    },
    bullet_list: (out, node, pos) => writeList(out, node, pos + 1, () => "- "),
    ordered_list: (out, node, pos) => {
      const start = node.attrs.order ?? 1;
      writeList(out, node, pos + 1, (i) => `${start + i}. `);
    },
    list_item: (out, node, pos) => out.blocks(node, pos + 1),
    hard_break: (out, _node, pos) => {
      out.point(pos);
      out.write("\\\n");
      out.point(pos + 1);
    },
    image: (out, node, pos) => {
      out.point(pos);
      const alt = String(node.attrs.alt ?? "").replace(/[[\]\\]/g, "\\$&");
      const title = node.attrs.title ? ` "${escapeTitle(node.attrs.title)}"` : "";
      out.write(`![${alt}](${destination(node.attrs.src)}${title})`);
      out.point(pos + 1);
    },
  },
  marks: {
    em: { open: () => "*", close: () => "*" },
    strong: { open: () => "**", close: () => "**" },
    link: {
      open: () => "[",
      close: (mark) => {
        const title = mark.attrs.title ? ` "${escapeTitle(mark.attrs.title)}"` : "";
        return `](${destination(mark.attrs.href)}${title})`;
      },
    },
  },
};

/**
 * Writes `doc` as canonical markdown and maps its positions. Markdown this
 * wrote, parsed and written again, comes back the same, so a document and
 * the value it was parsed from map onto each other exactly.
 */
export function toMarkdown(doc: Node, spec: MarkdownSpec = {}): MappedMarkdown {
  const writer = new MarkdownWriter({
    nodes: { ...commonMarkSpec.nodes, ...spec.nodes },
    marks: { ...commonMarkSpec.marks, ...spec.marks },
  });
  writer.block(doc, 0);
  return writer.finish();
}
