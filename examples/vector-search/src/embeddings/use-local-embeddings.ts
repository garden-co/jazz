import { useCallback, useRef, useState } from "react";
import { DEFAULT_MODEL, loadModel, ModelName, ModelStatus } from "./model";
import { FeatureExtractionPipeline } from "@huggingface/transformers";
import { createEmbedding as createEmbeddingFn } from "./embeddings";

type UseLocalEmbeddingsOptions = {
  modelName?: ModelName;
};

export function useLocalEmbeddings({
  modelName = DEFAULT_MODEL,
}: UseLocalEmbeddingsOptions = {}) {
  const extractor = useRef<FeatureExtractionPipeline | null>(null);

  const [status, setStatus] = useState<ModelStatus | null>(null);

  const createEmbedding = useCallback(
    async (text: string) => {
      if (!extractor.current) {
        extractor.current = await loadModel({
          modelName,
          onStatusChange: setStatus,
        });
      }

      return await createEmbeddingFn(extractor.current)(text);
    },
    [modelName, status],
  );

  return { createEmbedding, modelStatus: status };
}
