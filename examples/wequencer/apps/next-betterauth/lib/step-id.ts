import { sha256 } from "@noble/hashes/sha2.js";
import { bytesToHex, utf8ToBytes } from "@noble/hashes/utils.js";

/**
 * The row id of the step at (track, pattern, position). Every client derives
 * the same id, so toggling a pad upserts one shared row: two bandmates who
 * press the same empty pad at once converge on it rather than creating two.
 *
 * Hashed in JavaScript rather than with `crypto.subtle`, which browsers only
 * expose on secure origins; a plain-http LAN address must work too.
 *
 * The id is a convention, not a guarantee: the step policy checks that a row's
 * track and pattern belong to its session, but not that its id matches its
 * (track, pattern, position). So pad ids can be squatted: an editor can store
 * a row under another pad's derived id. Pressing that pad then rewrites the
 * squatting row, or is rejected if the row sits in a session the presser
 * cannot edit. Deriving an id takes the track and pattern ids, which only
 * session members can read.
 */
export function stepId(trackId: string, patternId: string, position: number) {
  const hex = bytesToHex(
    sha256(utf8ToBytes(`wequencer-step:${trackId}:${patternId}:${position}`)).slice(0, 16),
  );
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-5${hex.slice(13, 16)}-a${hex.slice(17, 20)}-${hex.slice(20, 32)}`;
}
