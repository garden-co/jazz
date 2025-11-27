import { useRef, useState } from "react";
import { useAccount, useCoState } from "jazz-tools/react";

import { Header } from "./components/Header";
import { EmptyState } from "./components/EmptyState";
import { SearchBar } from "./components/SearchBar";
import { StatusBar } from "./components/StatusBar";
import { Footer } from "./components/Footer";

import { useJournalSeed } from "./helpers/use-journal-seed";
import { useCreateEmbedding } from "./helpers/use-create-embedding";
import { useLocalEmbeddings, DEFAULT_MODEL } from "./embeddings";

import { JazzAccount, JournalEntry, JournalEntryList } from "./schema";
import { useCreateEntry } from "./helpers/use-create-entry";
import { useDeleteEntries } from "./helpers/use-delete-entries";

function App() {
  const me = useAccount(JazzAccount, {
    resolve: { root: { journalEntries: true } },
  });

  // 1) Prepare local embeddings model (for query text embedding)
  const { createEmbedding, modelStatus } = useLocalEmbeddings({
    modelName: DEFAULT_MODEL,
  });
  const { queryEmbedding, isCreatingEmbedding, createQueryEmbedding } =
    useCreateEmbedding({ createEmbedding });

  // 2) Load a CoList and sort the results by similarity to the query embedding
  const journalEntries = useCoState(
    JournalEntryList,
    me.$isLoaded ? me.root.journalEntries?.$jazz.id : undefined,
    {
      resolve: { $each: { embedding: true } },
      select(journalEntries) {
        if (!journalEntries.$isLoaded) return;

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
      equalityFn(a, b) {
        // Re-render only when the results change
        if (!a || !b || a.length !== b.length) return false;

        return (
          a.every(
            (entry, ix) => entry?.value.$jazz.id === b[ix].value?.$jazz.id,
          ) ?? false
        );
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
    journalEntries: me.$isLoaded ? me.root.journalEntries : undefined,
  });
  const { isCreatingEntry, promptNewEntry } = useCreateEntry({
    createEmbedding,
    journalEntries: me.$isLoaded ? me.root.journalEntries : undefined,
  });
  const { deleteEntries } = useDeleteEntries({
    journalEntries: me.$isLoaded ? me.root.journalEntries : undefined,
  });

  return (
    <>
      <Header isSeeding={isSeeding} />

      <main className="max-w-2xl mx-auto px-3 pt-8 pb-10 flex flex-col gap-2">
        <h2 className="text-4xl font-bold text-zinc-800 mb-8">Journal</h2>

        {!isEmptyState && (
          <>
            <SearchBar
              modelStatus={modelStatus}
              queryEmbedding={queryEmbedding}
              createQueryEmbedding={createQueryEmbedding}
              searchInputRef={searchInputRef}
              isLoading={isLoading}
              isCreatingEmbedding={isCreatingEmbedding}
            />

            <StatusBar
              isLoading={isLoading}
              queryEmbedding={queryEmbedding}
              journalEntries={journalEntries}
              searchInputRef={searchInputRef}
              isSeeding={isSeeding}
              seedingProgress={seedingProgress}
              isCreatingEntry={isCreatingEntry}
              promptNewEntry={promptNewEntry}
            />
          </>
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
          <Footer
            deleteEntries={deleteEntries}
            promptNewEntry={promptNewEntry}
            isCreatingEntry={isCreatingEntry}
            isLoading={isLoading}
            showCreateNew={isEmptyState}
          />
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
      className="journal-entry-card flex flex-col gap-2"
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
