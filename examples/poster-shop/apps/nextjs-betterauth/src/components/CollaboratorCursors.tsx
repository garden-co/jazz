"use client";
import { useAll, useDb } from "jazz-tools/react";
import { app } from "../../schema.js";
export function CollaboratorCursors({ canvasId, userId }: { canvasId: string; userId: string }) {
  const db = useDb();
  const { data: cursors = [] } = useAll(app.cursors.where({ canvasId }).orderBy("userId", "asc"));
  const publishCursor = () => {
    const mine = cursors.find((cursor) => cursor.userId === userId);
    const next = mine ? { x: mine.x + 12, y: mine.y + 8 } : { x: 120, y: 120 };
    if (mine) db.update(app.cursors, mine.id, next);
    else db.insert(app.cursors, { canvasId, userId, ...next, color: "#ff5a36" });
  };
  return (
    <section aria-label="Collaborators">
      <h2>Collaborators</h2>
      <button onClick={publishCursor}>Move cursor</button>
      <ul>
        {cursors.map((cursor) => (
          <li key={cursor.id}>
            {cursor.userId}: {cursor.x}, {cursor.y}
          </li>
        ))}
      </ul>
    </section>
  );
}
