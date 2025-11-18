"use client";
import { use } from "react";
import { Festival } from "$lib/Festival";
import { NewBand } from "@/app/components/NewBand";

export default function FestivalPage(props: {
  params: Promise<{ festivalId: string }>;
}) {
  const { festivalId } = use(props.params);

  return (
    <main>
      <h1>🎪 Festival {festivalId}</h1>
      <Festival id={festivalId} />
      <NewBand id={festivalId} />
    </main>
  );
}
