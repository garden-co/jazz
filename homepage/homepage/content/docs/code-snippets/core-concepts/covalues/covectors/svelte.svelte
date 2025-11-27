<script lang="ts">
  import { CoState } from "jazz-tools/svelte";
  import { DocumentsList } from "./schema";
  const { queryEmbedding, documentsListId }: { queryEmbedding: number[], documentsListId: string } = $props();
  // 1) Load your documents
  const documents = new CoState(DocumentsList, documentsListId, {
    resolve: {
      $each: { embedding: true },
    }
  });

  // 2) Sort documents by vector similarity
  const foundDocuments = $derived((() => {
    const docs = documents.current;
    if (!docs.$isLoaded) return;

    if (!queryEmbedding) {
      return docs.map((value) => ({ value }));
    }

    return docs
      .map((value) => ({
        value,
        similarity: value.embedding.$jazz.cosineSimilarity(queryEmbedding), // [!code ++]
      }))
      .sort((a, b) => b.similarity - a.similarity)
      .filter((result) => result.similarity > 0.5);
  })());
</script>
