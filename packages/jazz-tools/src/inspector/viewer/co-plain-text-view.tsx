import { JsonObject } from "cojson";
import { RawDataCard } from "./raw-data-card.js";

export function CoPlainTextView({ data }: { data: JsonObject }) {
  if (!data) return;

  return (
    <>
      <p>{Object.values(data).join("")}</p>
      <RawDataCard data={data} />
    </>
  );
}
