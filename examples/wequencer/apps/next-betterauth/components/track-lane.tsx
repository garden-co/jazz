"use client";

import { useAll, useDb } from "jazz-tools/react";
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
            onClick={() => db.update(app.steps, step.id, { enabled: !step.enabled })}
            style={step.enabled ? { background: color } : undefined}
            type="button"
          >
            {step.position + 1}
          </button>
        ))}
      </div>
    </div>
  );
}
