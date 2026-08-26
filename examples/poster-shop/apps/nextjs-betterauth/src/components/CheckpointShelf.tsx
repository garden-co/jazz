"use client";
import { useAll, useDb } from "jazz-tools/react";
import { app } from "@/schema";
export function CheckpointShelf({ canvasId }: { canvasId: string }) {
  const db = useDb();
  const { data: checkpoints = [] } = useAll(
    app.checkpoints.where({ canvasId }).orderBy("label", "asc"),
  );
  return (
    <section aria-label="Checkpoints">
      <h2>Checkpoints</h2>
      <button
        onClick={() =>
          db.insert(app.checkpoints, {
            canvasId,
            label: `Checkpoint ${checkpoints.length + 1}`,
            branch: "main",
          })
        }
      >
        Save checkpoint
      </button>
      <ol>
        {checkpoints.map((checkpoint) => (
          <li key={checkpoint.id}>
            {checkpoint.label} · {checkpoint.branch}
          </li>
        ))}
      </ol>
    </section>
  );
}
