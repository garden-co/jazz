"use client";

import { PersistedWriteRejectedError } from "jazz-tools";
import { useAll, useDb } from "jazz-tools/react";
import { useState } from "react";
import { app } from "@/schema";

export function TrackLane({
  trackId,
  name,
  color,
}: {
  trackId: string;
  name: string;
  color: string;
}) {
  const db = useDb();
  const { data: steps = [] } = useAll(
    app.steps.where({ track_id: trackId }).orderBy("position", "asc"),
  );
  const [writeError, setWriteError] = useState<string | null>(null);

  async function toggle(step: (typeof steps)[number]) {
    setWriteError(null);
    try {
      await db.update(app.steps, step.id, { enabled: !step.enabled }).wait({ tier: "edge" });
    } catch (error) {
      // Local visibility remains optimistic. The receipt makes a server-side
      // permission rejection observable instead of silently looking like a
      // conflicting edit.
      setWriteError(
        error instanceof PersistedWriteRejectedError && error.code === "permission_denied"
          ? "Pad update was rejected by session permissions."
          : "Pad update could not be confirmed. Check your connection and try again.",
      );
    }
  }

  return (
    <div className="track-lane">
      <strong style={{ borderColor: color }}>{name}</strong>
      <div className="pads" role="group" aria-label={`${name} steps`}>
        {steps.map((step) => (
          <button
            aria-label={`${name}, step ${step.position + 1}`}
            aria-pressed={step.enabled}
            className={step.enabled ? "pad pad-on" : "pad"}
            key={step.id}
            onClick={() => void toggle(step)}
            style={step.enabled ? { background: color } : undefined}
            type="button"
          >
            {step.position + 1}
          </button>
        ))}
      </div>
      {writeError ? <p role="status">{writeError}</p> : null}
    </div>
  );
}
