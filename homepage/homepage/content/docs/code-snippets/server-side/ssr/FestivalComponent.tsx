"use client";
import { Festival } from "@/app/schema";
// [!code ++:2]
import { ExportedCoValue, co } from "jazz-tools";
import { useCoState } from "jazz-tools/react";

// [!code ++:1]
type ExportedFestival = ExportedCoValue<co.loaded<typeof Festival, { $each: { $onError: "catch" } }>>;

export function FestivalComponent(props: { preloaded: ExportedFestival, festivalId: string }) {
  const festival = useCoState(Festival, props.festivalId, {
    // [!code ++:1]
    preloaded: props.preloaded,
    resolve: {
      $each: {
        $onError: "catch",
      },
    },
  });

  return (
    <main>
      <h1>🎪 Server-rendered Festival {props.festivalId}</h1>

      <ul>
        {festival.$isLoaded &&
          festival.map((band) => {
            if (!band.$isLoaded) return null;
            return <li key={band.$jazz.id}>🎶 {band.name}</li>;
          })}
      </ul>
    </main>
  );
}
