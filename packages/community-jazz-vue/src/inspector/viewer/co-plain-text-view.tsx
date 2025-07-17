import { JsonObject } from "cojson";
import { RawDataCard } from "./raw-data-card.js";

export function CoPlainTextView({
  data,
}: {
  data: JsonObject;
}) {
  if (!data) return null;

  return (
    <>
      <p>{Object.values(data).join("")}</p>
      <RawDataCard data={data} />
    </>
  );
}
