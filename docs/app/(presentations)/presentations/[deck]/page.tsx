import { getPresentationDeckSlides, getPresentationDecks } from "@/lib/presentations";
import { notFound, redirect } from "next/navigation";

export default async function PresentationDeckRedirectPage(
  props: PageProps<"/presentations/[deck]">,
) {
  const params = await props.params;
  const slides = getPresentationDeckSlides(params.deck);

  if (slides.length === 0) notFound();

  redirect(slides[0].url);
}

export function generateStaticParams() {
  return getPresentationDecks().map((deck) => ({ deck: deck.slug }));
}
