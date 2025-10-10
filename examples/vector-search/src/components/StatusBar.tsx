import { Embedding } from "../embeddings";

export function StatusBar({
  isLoading,
  queryEmbedding,
  journalEntries,
  searchInputRef,
  isSeeding,
  seedingProgress,
  isCreatingEntry,
  promptNewEntry,
}: {
  isLoading: boolean;
  queryEmbedding: Embedding | null;
  journalEntries: Array<any> | undefined;
  searchInputRef: React.RefObject<HTMLInputElement | null>;
  isSeeding: boolean;
  seedingProgress: { seededCount: number; targetCount: number };
  isCreatingEntry: boolean;
  promptNewEntry: () => void;
}) {
  return (
    <div className="px-4 flex flex-row gap-2 justify-between items-center text-sm text-zinc-500">
      {isLoading ? (
        <>&nbsp;</>
      ) : queryEmbedding && journalEntries !== undefined ? (
        <div>
          Found {journalEntries.length} journal entries relatable to{" "}
          <span className="inline-block rounded-full py-px px-2 bg-zinc-200">
            {searchInputRef.current?.value}
          </span>
        </div>
      ) : isSeeding ? (
        <div>
          Creating testing dataset:{" "}
          <span className="tabular-nums">
            {seedingProgress.seededCount} / {seedingProgress.targetCount}
          </span>
        </div>
      ) : journalEntries && journalEntries.length > 0 ? (
        <>
          <div>
            Showing{" "}
            <span className="tabular-nums">{journalEntries.length}</span>{" "}
            journal entries.
          </div>
          <button
            className="bg-zinc-200 px-2 rounded cursor-pointer hover:bg-zinc-300 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
            onClick={promptNewEntry}
            disabled={isCreatingEntry || isLoading}
          >
            {isCreatingEntry ? "Creating..." : "+ New entry"}
          </button>
        </>
      ) : null}
    </div>
  );
}
