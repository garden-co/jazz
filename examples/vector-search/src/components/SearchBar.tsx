import { DEFAULT_MODEL, Embedding } from "../embeddings";
import { EmbeddingPill } from "./EmbeddingPill";
import { ModelStatus as ModelStatusType } from "../embeddings";
import { ModelStatus } from "./ModelStatus";

export function SearchBar({
  modelStatus,
  queryEmbedding,
  createQueryEmbedding,
  searchInputRef,
  isLoading,
  isCreatingEmbedding,
}: {
  modelStatus: ModelStatusType | null;
  queryEmbedding: Embedding | null;
  createQueryEmbedding: (text: string) => void;
  searchInputRef: React.RefObject<HTMLInputElement | null>;
  isLoading: boolean;
  isCreatingEmbedding: boolean;
}) {
  return (
    <>
      <div className="flex flex-row flex-wrap gap-2">
        <ModelStatus modelName={DEFAULT_MODEL} modelStatus={modelStatus} />
        {queryEmbedding && <EmbeddingPill embedding={queryEmbedding} />}
      </div>

      <form
        className="sticky top-3 flex flex-col gap-2 z-30 mb-10"
        onSubmit={(e) => (
          e.preventDefault(),
          createQueryEmbedding(searchInputRef.current?.value ?? "")
        )}
      >
        <div className="relative flex flex-row">
          <input
            type="search"
            placeholder="Search relatable moments in the journal"
            className="w-full pl-5 pr-32 py-5 text-lg rounded-full bg-white shadow-lg border border-zinc-200 focus:outline-none focus:ring-2 focus:ring-blue-500"
            ref={searchInputRef}
            disabled={isLoading}
            role="searchbox"
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
  );
}
