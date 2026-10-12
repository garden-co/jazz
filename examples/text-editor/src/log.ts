import { createDecoder, hasContent } from "lib0/decoding";
import * as Y from "yjs";

export function applyUpdates(doc: Y.Doc, bytes: Uint8Array, origin?: unknown): void {
  const decoder = createDecoder(bytes);
  while (hasContent(decoder)) Y.readUpdate(decoder, doc, origin);
}
