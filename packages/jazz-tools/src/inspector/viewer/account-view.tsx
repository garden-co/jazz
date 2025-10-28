import { JsonObject, LocalNode } from "cojson";
import { GridView } from "./grid-view.js";
import { RawDataCard } from "./raw-data-card.js";
import { PageInfo } from "./types.js";

export function AccountView({
  data,
  onNavigate,
  node,
}: {
  data: JsonObject;
  onNavigate: (pages: PageInfo[]) => void;
  node: LocalNode;
}) {
  const readableData = { ...data };

  for (const key in readableData) {
    if (
      key === "readKey" ||
      key.startsWith("sealer_z") ||
      key.startsWith("key_z")
    ) {
      delete readableData[key];
    }
  }

  return (
    <>
      <GridView data={readableData} onNavigate={onNavigate} node={node} />

      <RawDataCard data={data} />
    </>
  );
}
