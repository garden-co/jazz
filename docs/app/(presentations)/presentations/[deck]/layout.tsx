import { PresentationShell } from "@/components/presentations/presentation-shell";
import { getPresentationDeckPage, getPresentationSlidesForPage } from "@/lib/presentations";
import { notFound } from "next/navigation";

export default async function PresentationDeckLayout(props: LayoutProps<"/presentations/[deck]">) {
  const params = await props.params;
  const deck = getPresentationDeckPage(params.deck);

  if (!deck) notFound();

  const slides = await getPresentationSlidesForPage(deck);

  return (
    <PresentationShell deckTitle={deck.data.title} slides={slides}>
      {props.children}
    </PresentationShell>
  );
}
