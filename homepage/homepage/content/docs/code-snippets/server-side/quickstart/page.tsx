"use client";
import type { co } from "jazz-tools";
import { useState } from "react";
import { announceBand } from "@/app/announceBandSchema";
import type { BandList } from "./schema";

export default function Home() {
  const [bandName, setBandName] = useState("");
  const [bandList, setBandList] =
    useState<co.loaded<typeof BandList, { $each: true }>>();
  const handleAnnounceBand = async () => {
    const bandListResponse = await announceBand.send({
      band: { name: bandName },
    });
    setBandName("");
    if (bandListResponse.bandList.$isLoaded) {
      setBandList(bandListResponse.bandList);
    }
  };

  return (
    <div>
      <input
        type="text"
        value={bandName}
        onChange={(e) => setBandName(e.target.value)}
      />
      <button type="button" onClick={handleAnnounceBand}>
        Announce Band
      </button>
      <div>
        {bandList?.$isLoaded &&
          bandList.map(
            (band) => band && <div key={band?.$jazz.id}>{band.name}</div>,
          )}
      </div>
    </div>
  );
}
