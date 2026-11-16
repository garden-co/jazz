import MarkdownIt from "markdown-it";

// Raw HTML stays text: descriptions and notes are written by band members and
// shown to others, so only Markdown formatting renders.
const markdown = new MarkdownIt({ html: false, linkify: true, breaks: true });

/** A stop's description or notes, stored as Markdown, as HTML to show. */
export function renderMarkdown(text: string): string {
  return markdown.render(text);
}
