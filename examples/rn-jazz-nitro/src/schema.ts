// Schema + value helpers for the todo app.
// All Jazz FFI data crosses the boundary as JSON strings using NitroValue tagged unions.

export const TODO_SCHEMA_JSON = JSON.stringify({
  tables: {
    todos: {
      columns: [
        { name: "title", column_type: { type: "Text" }, nullable: false },
        { name: "done", column_type: { type: "Boolean" }, nullable: false },
      ],
    },
  },
});

// Build NitroValue array for insert (column order: title, done)
export function todoValues(title: string, done: boolean): string {
  return JSON.stringify([
    { type: "Text", value: title },
    { type: "Boolean", value: done },
  ]);
}

// Build partial update object (keyed by column name)
export function todoUpdate(fields: { title?: string; done?: boolean }): string {
  const obj: Record<string, { type: string; value: string | boolean }> = {};
  if (fields.title !== undefined) {
    obj.title = { type: "Text", value: fields.title };
  }
  if (fields.done !== undefined) {
    obj.done = { type: "Boolean", value: fields.done };
  }
  return JSON.stringify(obj);
}

// Parsed todo row
export interface Todo {
  id: string;
  title: string;
  done: boolean;
}

// Parse query result JSON into Todo[]
export function parseTodoRows(queryResultJson: string): Todo[] {
  const rows: { id: string; values: { type: string; value: unknown }[] }[] =
    JSON.parse(queryResultJson);
  return rows.map((row) => ({
    id: row.id,
    title: row.values[0].value as string,
    done: row.values[1].value as boolean,
  }));
}

// Subscription delta types
export interface DeltaRow {
  id: string;
  title: string;
  done: boolean;
}

interface RawDeltaRow {
  row: { id: string; values: { type: string; value: unknown }[] };
  index: number;
}

interface RawUpdatedRow {
  old_row: { id: string; values: { type: string; value: unknown }[] };
  new_row: { id: string; values: { type: string; value: unknown }[] };
  old_index: number;
  new_index: number;
}

export interface Delta {
  added: { row: DeltaRow; index: number }[];
  removed: { row: DeltaRow; index: number }[];
  updated: {
    oldRow: DeltaRow;
    newRow: DeltaRow;
    oldIndex: number;
    newIndex: number;
  }[];
}

function parseRow(raw: { id: string; values: { type: string; value: unknown }[] }): DeltaRow {
  return {
    id: raw.id,
    title: raw.values[0].value as string,
    done: raw.values[1].value as boolean,
  };
}

export function parseDelta(deltaJson: string): Delta {
  const raw = JSON.parse(deltaJson);
  return {
    added: (raw.added || []).map((a: RawDeltaRow) => ({
      row: parseRow(a.row),
      index: a.index,
    })),
    removed: (raw.removed || []).map((r: RawDeltaRow) => ({
      row: parseRow(r.row),
      index: r.index,
    })),
    updated: (raw.updated || []).map((u: RawUpdatedRow) => ({
      oldRow: parseRow(u.old_row),
      newRow: parseRow(u.new_row),
      oldIndex: u.old_index,
      newIndex: u.new_index,
    })),
  };
}
