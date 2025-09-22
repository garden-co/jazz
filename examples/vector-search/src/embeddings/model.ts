import { pipeline, ProgressInfo } from "@huggingface/transformers";

export const MODELS = [
  "Xenova/all-MiniLM-L6-v2",
  "mixedbread-ai/mxbai-embed-large-v1",
  "jinaai/jina-clip-v2",
] as const;

export type ModelName = (typeof MODELS)[number];

export const DEFAULT_MODEL = MODELS[0];

type DownloadingModel = { status: "downloading"; progress: number };
type ReadyModel = { status: "ready" };

export type ModelStatus = DownloadingModel | ReadyModel;

export const loadModel = async ({
  modelName,
  onStatusChange = () => {},
}: {
  modelName: ModelName;
  onStatusChange?: (status: ModelStatus) => void;
}) => {
  try {
    return await pipeline("feature-extraction", modelName, {
      progress_callback: onProgressUpdate(modelName, onStatusChange),
    });
  } catch (e) {
    console.error("Error loading model", e);
    throw e;
  }
};

const onProgressUpdate =
  (modelName: ModelName, onStatusChange: (status: ModelStatus) => void) =>
  (progress: ProgressInfo) => {
    switch (progress.status) {
      case "progress": {
        if (isModelFile(progress.file) && progress.progress < 100) {
          onStatusChange({
            status: "downloading",
            progress: progress.progress,
          });
        }
        return;
      }
      case "ready": {
        if (progress.model === modelName) {
          onStatusChange({ status: "ready" });
        }
        return;
      }
    }
  };

const isModelFile = (file: string) => file.endsWith(".onnx");
