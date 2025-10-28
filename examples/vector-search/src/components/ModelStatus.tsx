import { DownloadIcon, ModelIcon } from "./Icons";
import { type ModelStatus } from "../embeddings";
import { Pill, PillColumn, PillLabel, PillValue } from "./Pill";

export function ModelStatus({
  modelName,
  modelStatus,
}: {
  modelName: string;
  modelStatus: ModelStatus | null;
}) {
  const isDownloading = modelStatus?.status === "downloading";

  return (
    <Pill
      icon={
        isDownloading ? (
          <div className="animate-bounce text-blue-600">
            <DownloadIcon />
          </div>
        ) : (
          <div
            className={modelStatus?.status === "ready" ? "text-blue-600" : ""}
          >
            <ModelIcon />
          </div>
        )
      }
    >
      <PillColumn>
        <PillLabel>
          {isDownloading
            ? `${modelStatus.progress.toFixed()}% Downloading...`
            : "Local Embeddings Model"}
        </PillLabel>

        <PillValue>
          <a
            href={`https://huggingface.co/${modelName}`}
            target="_blank"
            rel="noopener noreferrer"
            className="
           hover:underline"
          >
            {modelName}
          </a>
        </PillValue>
      </PillColumn>
    </Pill>
  );
}
