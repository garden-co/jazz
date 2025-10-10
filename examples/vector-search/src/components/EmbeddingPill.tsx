import { Embedding } from "../embeddings";
import { EmbeddingIcon } from "./Icons";
import { Pill, PillColumn, PillLabel, PillValue } from "./Pill";

export function EmbeddingPill({ embedding }: { embedding: Embedding }) {
  return (
    <Pill
      icon={
        <div className="text-blue-600">
          <EmbeddingIcon />
        </div>
      }
    >
      <PillColumn>
        <PillLabel>{embedding.length}-dim embedding</PillLabel>

        <PillValue>
          <a
            onClick={() => alert(`Full embedding vector in developer console`)}
            className="cursor-pointer hover:underline"
          >
            {embedding.slice(0, 1).join(", ")}...
          </a>
        </PillValue>
      </PillColumn>
    </Pill>
  );
}
