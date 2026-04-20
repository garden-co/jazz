import {
  getPresentationDeckPage,
  getPresentationDecks,
  getPresentationSlidesForPage,
} from "@/lib/presentations";
import { notFound, redirect } from "next/navigation";

export default async function PresentationDeckRedirectPage(
  props: PageProps<"/presentations/[deck]">,
) {
  const params = await props.params;
  const deck = getPresentationDeckPage(params.deck);

  if (!deck) notFound();

  const slides = await getPresentationSlidesForPage(deck);

  redirect(slides[0].href);
}

export async function generateStaticParams() {
  const decks = await getPresentationDecks();

  return decks.map((deck) => ({ deck: deck.slug }));
}
