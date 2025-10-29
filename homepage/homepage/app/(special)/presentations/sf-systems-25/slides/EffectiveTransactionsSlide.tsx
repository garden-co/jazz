"use client";

import { useState } from "react";
import { CoValueCoreDiagram } from "../diagrams";
import { scenario1Timestamps, scenario1, header } from "../page";

export function EffectiveTransactionsSlide() {
  const [currentTimestampIdx, setCurrentTimestampIdx] = useState(0);
  const currentTimestamp = scenario1Timestamps[currentTimestampIdx];

  const filteredSessions = Object.fromEntries(
    Object.entries(scenario1).flatMap(([key, session]) => {
      const filteredSession = session.filter(
        (entry) => entry.t <= currentTimestamp,
      );

      if (filteredSession.length === 0) {
        return [];
      }

      return [[key, session.filter((entry) => entry.t <= currentTimestamp)]];
    }),
  );

  return (
    <div className="self-start">
      <input
        type="range"
        min={0}
        max={scenario1Timestamps.length - 1}
        value={currentTimestampIdx}
        onChange={(e) => setCurrentTimestampIdx(parseInt(e.target.value))}
        className="w-[50vw]"
      />
      <p className="text-center">
        Current time:{" "}
        {currentTimestamp.toLocaleString("en-us", {
          hour: "numeric",
          minute: "2-digit",
        })}
      </p>
      <CoValueCoreDiagram
        header={header}
        sessions={filteredSessions}
        showView={true}
        showHashAndSignature={false}
        encryptedItems={false}
        showEditor={true}
      />
    </div>
  );
}
