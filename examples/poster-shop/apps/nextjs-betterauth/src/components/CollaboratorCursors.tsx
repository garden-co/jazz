"use client";
import { useAll } from "jazz-tools/react";
import { app } from "@/schema";
export function CollaboratorCursors({
  canvasId,
  author,
}: {
  canvasId: string;
  author: string | null;
}) {
  const { data: cursors = [] } = useAll(app.cursors.where({ canvasId }).orderBy("author", "asc"));
  return (
    <section aria-label="Collaborators">
      <h2>Collaborators</h2>
      <p>
        Your cursor:{" "}
        {author && cursors.some((cursor) => cursor.author === author) ? "visible" : "not published"}
      </p>
      <ul>
        {cursors.map((cursor) => (
          <li key={cursor.id}>
            {cursor.author}: {cursor.x}, {cursor.y}
          </li>
        ))}
      </ul>
    </section>
  );
}
