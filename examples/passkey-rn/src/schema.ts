import { co } from "jazz-tools";

/**
 * Simple note schema to demonstrate Jazz functionality with passkey auth.
 */
export const Note = co.map({
  title: co.plainText(),
  content: co.plainText(),
});

export const NoteList = co.list(Note);
