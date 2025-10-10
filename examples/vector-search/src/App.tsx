import { useRef, useState } from "react";
import { useAccount, useCoStateWithSelector } from "jazz-tools/react";

import { Header } from "./components/Header";
import { EmbeddingPill } from "./components/EmbeddingPill";
import { ModelStatus } from "./components/ModelStatus";
import { EmptyState } from "./components/EmptyState";

import { useJournalSeed } from "./helpers/use-journal-seed";
import { useCreateEmbedding } from "./helpers/use-create-embedding";
import { useLocalEmbeddings, DEFAULT_MODEL } from "./embeddings";

import { JazzAccount, JournalEntry, JournalEntryList } from "./schema";
import { useCreateEntry } from "./helpers/use-create-entry";
import { useDeleteEntries } from "./helpers/use-delete-entries";
import { Footer } from "./components/Footer";

function App() {
  const { me } = useAccount(JazzAccount, {
    resolve: { root: { journalEntries: true } },
  });

  // 1) Prepare local embeddings model (for query text embedding)
  const { createEmbedding, modelStatus } = useLocalEmbeddings({
    modelName: DEFAULT_MODEL,
  });
  const { queryEmbedding, isCreatingEmbedding, createQueryEmbedding } =
    useCreateEmbedding({ createEmbedding });

  // 2) Load a list of elements containing embeddings
  const journalEntries = useCoStateWithSelector(
    JournalEntryList,
    me?.root.journalEntries?.$jazz.id,
    {
      resolve: { $each: { embedding: true } },
      select(journalEntries) {
        if (!journalEntries) return;

        // If no query embedding, return all entries
        if (!queryEmbedding) return journalEntries.map((value) => ({ value }));

        return journalEntries
          .map((value) => ({
            value,
            similarity: value.embedding.$jazz.cosineSimilarity(queryEmbedding),
          }))
          .sort((a, b) => b.similarity - a.similarity)
          .slice(0, 5);
      },
    },
  );

  // -- Helpers
  const searchInputRef = useRef<HTMLInputElement>(null);
  const isLoading = journalEntries === undefined;
  const isEmptyState =
    journalEntries !== undefined &&
    journalEntries !== null &&
    journalEntries.length === 0 &&
    queryEmbedding === null;
  const {
    isSeeding,
    seedJournal,
    progress: seedingProgress,
  } = useJournalSeed({
    createEmbedding,
    owner: me as JazzAccount,
  });
  const { isCreatingEntry, promptNewEntry } = useCreateEntry({
    createEmbedding,
    owner: me as JazzAccount,
  });
  const { deleteEntries } = useDeleteEntries({ owner: me as JazzAccount });

  return (
    <>
      <Header isSeeding={isSeeding} />

      <main className="max-w-2xl mx-auto px-3 pt-8 pb-10 flex flex-col gap-2">
        <h2 className="text-4xl font-bold text-zinc-800 mb-8">Journal</h2>

        {/* Search UI */}
        {!isEmptyState && (
          <>
            <div className="flex flex-row flex-wrap gap-2">
              <ModelStatus
                modelName={DEFAULT_MODEL}
                modelStatus={modelStatus}
              />
              {queryEmbedding && <EmbeddingPill embedding={queryEmbedding} />}
            </div>

            <form
              className="sticky top-3 flex flex-col gap-2 z-30 mb-10"
              onSubmit={(e) => (
                e.preventDefault(),
                createQueryEmbedding(searchInputRef.current?.value)
              )}
            >
              <div className="relative flex flex-row">
                <input
                  type="search"
                  placeholder="Search relatable moments in the journal"
                  className="w-full pl-5 pr-32 py-5 text-lg rounded-full bg-white shadow-lg border border-zinc-200 focus:outline-none focus:ring-2 focus:ring-blue-500"
                  ref={searchInputRef}
                  disabled={isLoading}
                />
                <button
                  type="submit"
                  className="absolute top-2.5 right-2.5 px-5 py-3  rounded-full shadow-lg bg-linear-to-t from-blue-700 to-blue-600 hover:to-blue-700 text-white cursor-pointer disabled:opacity-50 disabled:cursor-not-allowed"
                  disabled={isCreatingEmbedding || isLoading}
                >
                  {isCreatingEmbedding ? "Searching" : "Search"}
                </button>
              </div>
            </form>
          </>
        )}

        {/* Status line */}
        {!isEmptyState && (
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
        )}

        {/* Journal entries */}
        {isEmptyState ? (
          <EmptyState
            isSeeding={isSeeding}
            seedJournal={seedJournal}
            modelName={DEFAULT_MODEL}
            modelStatus={modelStatus}
          />
        ) : isLoading ? (
          <div className="grid md:grid-cols-2 gap-8 items-center">
            <LoadingCard />
            <LoadingCard />
            <LoadingCard />
            <LoadingCard />
            <LoadingCard />
          </div>
        ) : (
          <div className="grid md:grid-cols-2 gap-8 items-center">
            {journalEntries.map(
              (entry) =>
                entry && (
                  <JournalEntryCard entry={entry} key={entry.value.$jazz.id} />
                ),
            )}
          </div>
        )}

        {/* Footer */}
        <div className="mt-12">
          <Footer deleteEntries={deleteEntries} />
        </div>
      </main>
    </>
  );
}

function JournalEntryCard({
  entry,
}: {
  entry: { value: JournalEntry; similarity?: number };
}) {
  const [rotation, _] = useState(() => Math.random() * 4 - 2);

  return (
    <div
      className="flex flex-col gap-2"
      style={{ transform: `rotate(${rotation}deg)` }}
    >
      <div className="bg-white shadow-lg p-6 rounded-xl max-h-min flex flex-col gap-2">
        {entry?.value.text}
      </div>

      {typeof entry.similarity === "number" && (
        <span className="px-6 text-xs text-zinc-500 flex gap-2 items-baseline">
          <span>
            {Math.round((entry.similarity + Number.EPSILON) * 100) / 100}
          </span>
          <span className="uppercase opacity-50">Similarity</span>
        </span>
      )}
    </div>
  );
}

function LoadingCard() {
  const [rotation, _] = useState(() => Math.random() * 4 - 2);

  return (
    <div
      className="bg-zinc-200 w-full p-6 rounded-xl min-h-40 flex flex-col gap-2 animate-pulse"
      style={{ transform: `rotate(${rotation}deg)` }}
    />
  );
}

export default App;
