import type { Db } from "jazz-tools";
import { app, type StopStatus } from "../../schema.js";
import { asLines } from "./text.js";

/** The stop texts as the edit form opened them. */
export interface OpenedTexts {
  description: string;
  notes: string;
}

export interface StopEdit {
  date: Date;
  status: StopStatus;
  description: string;
  notes: string;
}

/**
 * Saves the stop form: the stop and its private note change together, in one
 * transaction. The texts are written as edits of what the form opened, so a
 * bandmate's edit that arrived while the form was open is merged, not undone.
 */
export function saveStopEdit(
  db: Db,
  stop: { id: string; bandId: string },
  note: { id: string } | null,
  opened: OpenedTexts,
  edit: StopEdit,
) {
  const body = asLines(edit.notes);
  return db.transaction((tx) => {
    tx.update(
      app.stops,
      stop.id,
      { date: edit.date, status: edit.status, publicDescription: asLines(edit.description) },
      { editedFrom: { publicDescription: opened.description } },
    );
    if (note && body) {
      tx.update(app.stopNotes, note.id, { body }, { editedFrom: { body: opened.notes } });
    } else if (note) tx.delete(app.stopNotes, note.id);
    else if (body) tx.insert(app.stopNotes, { stopId: stop.id, bandId: stop.bandId, body });
  });
}
