"use client";

import { useState } from "react";
import { CoValueCoreDiagram } from "../diagrams";
import { scenario1Timestamps, scenario1, header } from "../page";

export function EffectiveTransactionsSlide({
  timestampIdx,
  showCore,
  codeStep,
  showEditor,
}: {
  timestampIdx: number;
  showCore: boolean;
  codeStep?: {
    fileName: string;
    code: React.ReactNode;
  }[];
  showEditor?: boolean;
}) {
  const [currentTimestampIdx, setCurrentTimestampIdx] = useState(timestampIdx);
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
    <div className="mt-[10vh] self-start">
      <input
        type="range"
        min={0}
        max={scenario1Timestamps.length - 1}
        value={currentTimestampIdx}
        onChange={(e) => setCurrentTimestampIdx(parseInt(e.target.value))}
        className="w-[50vw]"
      />
      <p className="text-center text-2xl">
        {currentTimestamp.toLocaleString("en-us", {
          hour: "numeric",
          minute: "2-digit",
        })}
      </p>
      <CoValueCoreDiagram
        header={header}
        sessions={filteredSessions}
        showView={true}
        showCore={showCore}
        showHashAndSignature={false}
        encryptedItems={false}
        showEditor={showEditor}
        currentTimestamp={currentTimestamp}
      />
      {codeStep && (
        <div className="flex gap-4 justify-center items-start -mx-[20vw]">
          {codeStep.map((deviceCodeStep, idx) => (
            <div key={idx} className="rounded-lg border bg-white ring-4 ring-stone-400/20 dark:bg-stone-925">
              <span className="block border-b px-2 py-2 text-xs font-light text-stone-700 dark:text-stone-300 md:px-3 md:text-sm">
                {deviceCodeStep.fileName}
              </span>
              <pre className="whitespace-pre-wrap break-words p-1 pb-2 text-xs md:text-sm [&_code]:whitespace-pre-wrap [&_code]:break-words">
                {deviceCodeStep.code}
              </pre>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
