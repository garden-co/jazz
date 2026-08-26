"use client";
import { useAll } from "jazz-tools/react";
import { app } from "../../schema.js";
export function CollaboratorCursors({ canvasId, userId }: { canvasId: string; userId: string }) {
  const { data: cursors = [] } = useAll(app.cursors.where({ canvasId }).orderBy("userId", "asc"));
  return (
    <section aria-label="Collaborators">
      <h2>Collaborators</h2>
      <p>
        Your cursor:{" "}
        {cursors.some((cursor) => cursor.userId === userId) ? "visible" : "not published"}
      </p>
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
