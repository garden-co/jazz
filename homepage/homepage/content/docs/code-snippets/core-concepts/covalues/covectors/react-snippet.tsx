import { co, z } from "jazz-tools";
const Document = co.map({
  content: z.string(),
  embedding: co.vector(384),
});
export const DocumentsList = co.list(Document);
const documents = DocumentsList.create([]);
const documentsListId: string = "co_876TBN";

type YourCustomHook = {
  queryEmbedding: number[] | null;
  createQueryEmbedding: (text: string) => Promise<number[]>;
};
const useCreateEmbedding: () => YourCustomHook = () => ({
  queryEmbedding: null,
  createQueryEmbedding: async (s) => [],
});

// #region SemanticSearch
import { useCoState } from "jazz-tools/react";

const { queryEmbedding } = useCreateEmbedding();

const foundDocuments = useCoState(DocumentsList, documentsListId, {
  resolve: {
    $each: { embedding: true },
  },
  select(documents) {
    if (!documents.$isLoaded) return;

    // If no query embedding, return all entries
    if (!queryEmbedding) return documents.map((value) => ({ value }));

    return documents
      .map((value) => ({
        value,
        similarity: value.embedding.$jazz.cosineSimilarity(queryEmbedding), // [!code ++]
      }))
      .sort((a, b) => b.similarity - a.similarity)
      .filter((result) => result.similarity > 0.5);
  },
});
// #endregion
