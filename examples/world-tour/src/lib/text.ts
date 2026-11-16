// The stop texts (`stops.publicDescription`, `stopNotes.body`) are Markdown
// merge columns written from a form.

/**
 * A text as the stop form saves it: trailing whitespace dropped and one
 * closing line break, or "" when empty. Two bandmates adding a first line at
 * the same spot then keep two lines rather than one run-together line.
 */
export function asLines(text: string): string {
  return text.trim() ? `${text.trimEnd()}\n` : "";
}
