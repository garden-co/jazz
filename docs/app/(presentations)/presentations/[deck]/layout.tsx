import { PresentationShell } from "@/components/presentations/presentation-shell";
import {
  getPresentationDeckPage,
  getPresentationImageSrcsForPage,
  getPresentationSlidesForPage,
} from "@/lib/presentations";
import { notFound } from "next/navigation";
import { Suspense } from "react";

export default async function PresentationDeckLayout(props: LayoutProps<"/presentations/[deck]">) {
  const params = await props.params;
  const deck = getPresentationDeckPage(params.deck);

  if (!deck) notFound();

  const slides = await getPresentationSlidesForPage(deck);
  const preloadImageSrcs = await getPresentationImageSrcsForPage(deck);

  return (
    <Suspense>
      <PresentationShell
        deckTitle={deck.data.title}
        slides={slides}
        preloadImageSrcs={preloadImageSrcs}
      >
        {props.children}
      </PresentationShell>
    </Suspense>
  );
}
