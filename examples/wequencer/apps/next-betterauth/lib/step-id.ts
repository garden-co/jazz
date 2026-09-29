/**
 * The row id of the step at (track, pattern, position). Every client derives
 * the same id, so toggling a pad upserts one shared row: two bandmates who
 * press the same empty pad at once converge on it rather than creating two.
 */
export async function stepId(trackId: string, patternId: string, position: number) {
  const digest = await crypto.subtle.digest(
    "SHA-256",
    new TextEncoder().encode(`wequencer-step:${trackId}:${patternId}:${position}`),
  );
  const hex = Array.from(new Uint8Array(digest).slice(0, 16), (byte) =>
    byte.toString(16).padStart(2, "0"),
  ).join("");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-5${hex.slice(13, 16)}-a${hex.slice(17, 20)}-${hex.slice(20, 32)}`;
}
