import type { JSONContent } from "@tiptap/core";
import { MarkdownManager } from "@tiptap/markdown";
import StarterKit from "@tiptap/starter-kit";
import { applyTextSplice, textSplice } from "./text-splice";

// #region rich-text-markdown
/**
 * The editor schema of one block's text: inline formatting only (bold,
 * italic, strikethrough, code, links). Headings, lists and quotes are block
 * kinds, so a block's own text never holds block-level Markdown.
 */
export const richTextExtensions = [
  StarterKit.configure({
    heading: false,
    bulletList: false,
    orderedList: false,
    listItem: false,
    listKeymap: false,
    blockquote: false,
    codeBlock: false,
    horizontalRule: false,
    hardBreak: false,
    underline: false,
    trailingNode: false,
    dropcursor: false,
    gapcursor: false,
    link: { openOnClick: false, autolink: true, defaultProtocol: "https" },
  }),
];

const markdown = new MarkdownManager({ extensions: richTextExtensions });

// A line that Markdown would read as a heading, list item, quote, fence,
// rule or setext underline. The marker is the second group.
const BLOCK_MARKER =
  /^( {0,3})(#{1,6}(?=\s|$)|[-*+](?=\s|$)|\d{1,9}[.)](?=\s|$)|>|`{3,}|~{3,}|(?:[-*_]\s*){3,}$|=+\s*$)/;

/** Escape every line start that would otherwise turn into block-level Markdown. */
function escapeBlockMarkers(text: string): string {
  return text
    .split("\n")
    .map((line) =>
      line.replace(BLOCK_MARKER, (_, indent: string, marker: string) =>
        /^\d/.test(marker)
          ? `${indent}${marker.slice(0, -1)}\\${marker.slice(-1)}`
          : `${indent}\\${marker}`,
      ),
    )
    .join("\n");
}

/**
 * Drop the Markdown this schema has no node for, which would otherwise take
 * its text with it: indentation that opens a code block (4+ spaces or a tab
 * at the start of a paragraph) and hard-break markers (two trailing spaces or
 * a backslash before a line end). The text and the line break both stay.
 */
function dropUnsupportedMarkup(text: string): string {
  const lines = text.split("\n");
  return lines
    .map((line, i) => {
      if (i < lines.length - 1) {
        // An odd run of backslashes ends in a hard break; an even one is escaped text.
        line = line.replace(/ {2,}$/, "").replace(/\\+$/, (run) => run.slice(run.length % 2));
      }
      if (i === 0 || lines[i - 1]!.trim() === "") line = line.replace(/^(?: {4,}| *\t)[ \t]*/, "");
      return line;
    })
    .join("\n");
}

/**
 * A block's stored Markdown as an editor document. Text written before blocks
 * had formatting is plain text, so a line such as `1. Intro` stays text
 * rather than becoming a list the editor cannot show.
 */
export function markdownToDoc(text: string): JSONContent {
  return markdown.parse(escapeBlockMarkers(dropUnsupportedMarkup(text)));
}

/** A block's text without its formatting, for labels and search. */
export function markdownToPlainText(text: string): string {
  const collect = (node: JSONContent): string =>
    node.text ?? (node.content ?? []).map(collect).join(node.type === "doc" ? "\n\n" : "");
  return collect(markdownToDoc(text));
}

/** The editor document as the Markdown stored in `blocks.text`. */
export function docToMarkdown(doc: JSONContent): string {
  return escapeBlockMarkers(markdown.serialize(doc));
}

// #endregion rich-text-markdown

// #region rich-text-rebase
/**
 * Carry the local edits made since `base` over to `remote`, a newer value
 * that arrived from Jazz before those edits were written.
 *
 * Both sides are one splice away from `base`. Where they touch different
 * parts, both apply. Where they overlap, the local side wins the span both
 * touched, as the server does for the later of two overlapping writes.
 */
export function rebaseText(base: string, local: string, remote: string): string {
  const mine = textSplice(base, local);
  const theirs = textSplice(base, remote);
  if (!mine) return remote;
  if (!theirs) return local;
  const mineEnd = mine.at + mine.delete;
  const theirsEnd = theirs.at + theirs.delete;
  if (mineEnd <= theirs.at && !(mine.delete === 0 && theirs.delete === 0 && mine.at === theirs.at))
    return applyTextSplice(remote, mine);
  if (theirsEnd <= mine.at) {
    const shift = theirs.insert.length - theirs.delete;
    return applyTextSplice(remote, { ...mine, at: mine.at + shift });
  }
  // Overlap: the local text of the span either side touched, between the
  // remote text before and after it.
  const from = Math.min(mine.at, theirs.at);
  const after = base.length - Math.max(mineEnd, theirsEnd);
  return (
    remote.slice(0, from) +
    local.slice(from, local.length - after) +
    remote.slice(remote.length - after)
  );
}
// #endregion rich-text-rebase
