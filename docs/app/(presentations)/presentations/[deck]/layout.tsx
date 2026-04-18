import { PresentationShell } from "@/components/presentations/presentation-shell";
import { getPresentationDeckSlides, getPresentationNotes } from "@/lib/presentations";
import { notFound } from "next/navigation";

export default async function PresentationDeckLayout(props: LayoutProps<"/presentations/[deck]">) {
  const params = await props.params;
  const slides = getPresentationDeckSlides(params.deck);

  if (slides.length === 0) notFound();

  return (
    <PresentationShell
      deckTitle={slides[0].data.deckTitle}
      slides={slides.map((slide) => ({
        href: slide.url,
        notes: getPresentationNotes(slide),
        title: slide.data.title,
      }))}
    >
      {props.children}
    </PresentationShell>
  );
}
