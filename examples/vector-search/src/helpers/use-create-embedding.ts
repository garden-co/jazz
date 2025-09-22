import { useCallback, useState } from "react";
import { Embedding } from "../embeddings";

/**
 * Simple `createEmbedding` wrapper that handles the state of the embedding creation.
 */
export const useCreateEmbedding = ({
  createEmbedding,
}: {
  createEmbedding: (text: string) => Promise<number[]>;
}) => {
  const [queryEmbedding, setQueryEmbedding] = useState<Embedding | null>(null);
  const [isCreating, setIsCreating] = useState(false);

  const createQueryEmbedding = useCallback(
    async (text: string | undefined) => {
      if (!text) {
        setQueryEmbedding(null);
        return;
      }

      try {
        setIsCreating(true);
        const embedding = await createEmbedding(text);
        console.info(`Embeddings for '${text}':`, embedding);
        setQueryEmbedding(embedding);
      } catch (error) {
        console.error(error);
      } finally {
        setIsCreating(false);
      }
    },
    [createEmbedding],
  );

  return {
    createQueryEmbedding,
    queryEmbedding,
    isCreatingEmbedding: isCreating,
  };
};
