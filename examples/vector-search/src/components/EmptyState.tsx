import { ModelStatus } from "./ModelStatus";
import { type ModelStatus as ModelStatusType } from "../embeddings";
import { useState } from "react";

export function EmptyState({
  isSeeding,
  seedJournal,
  modelName,
  modelStatus,
}: {
  isSeeding: boolean;
  seedJournal: () => void;
  modelName: string;
  modelStatus: ModelStatusType | null;
}) {
  return (
    <div className="relative flex flex-col gap-3 items-center justify-center">
      <div className="absolute top-12 z-50 bg-white flex flex-col gap-4 items-center justify-center p-6 rounded-xl shadow-md w-full max-w-120 min-h-64">
        <p className="text-zinc-500">Start the personal journal.</p>

        <button
          disabled={isSeeding}
          onClick={seedJournal}
          className="bg-zinc-800 text-white py-3 px-6 rounded-md hover:bg-zinc-900 cursor-pointer disabled:opacity-50 disabled:pointer-events-none"
        >
          {isSeeding ? "Loading dataset..." : "Load testing dataset"}
        </button>

        <ModelStatus modelName={modelName} modelStatus={modelStatus} />
      </div>

      <div className="w-full grid md:grid-cols-2 gap-8 items-center">
        {Array.from({ length: 6 }).map((_, index) => (
          <Card key={index} />
        ))}
      </div>
    </div>
  );
}

function Card() {
  const [rotation, _] = useState(() => Math.random() * 4 - 2);

  return (
    <div
      className="bg-zinc-200 w-full p-6 rounded-xl min-h-40 flex flex-col gap-2 opacity-50"
      style={{ transform: `rotate(${rotation}deg)` }}
    />
  );
}
