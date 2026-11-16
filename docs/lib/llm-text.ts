// Pages keep notes for us as `{/* TODO … */}` lines; they don't render, and agents reading the
// Markdown export shouldn't see them either.
export function stripTodoComments(markdown: string) {
  return markdown.replace(/^\{\/\* TODO[^\n]*\*\/\}\n?/gm, "");
}
