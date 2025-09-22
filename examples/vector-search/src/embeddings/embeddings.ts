import { FeatureExtractionPipeline } from "@huggingface/transformers";

export type Embedding = number[];

export const createEmbedding =
  (extractor: FeatureExtractionPipeline) =>
  async (text: string): Promise<Embedding> => {
    const embedding = await extractor(text, {
      pooling: "mean",
      normalize: true,
      quantize: false,
    });

    return embedding.tolist()[0];
  };
